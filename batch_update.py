"""
Created on Thu Jun 23
@author: lisaliuu

"""
from sys import breakpointhook
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from sqlite3 import OperationalError
from multiprocessing import Queue
from pprint import pprint
from typing import Dict
import pymongo
import queue
import json
import time

class BatchUpdate:
    def __init__(self, config, staleness_threshold=25):
        """
        :param staleness_threshold: Number of new documents read that do not update a time until that
        time is inserted to the transformed collection
        """

        '''
        schema for _cache_data dictionary if MODE == 'RAW':
            {
                time1: [configuration_id,   [id1,           id2,            id3,        ...], 
                                            [(x1,y1),       (x2,y2),        (x3,y3),    ...],
                                            [(l1,w1,h1),    (l2,w2,h2),     (l3,w3,h3), ...]],
                time2: ...
            }
        schema for _cache_data dictionary if MODE == 'RECONCILED':
            {
                time1: [configuration_id,   [id1,           id2,            id3,        ...], 
                                            [(x1,y1),       (x2,y2),        (x3, y3),   ...]
                time2: ...
            }
        '''
        self._cache_data={}
        self._staleness={}
        self.staleness_threshold=staleness_threshold
        self.connect_to_db(config)
    
    def connect_to_db(self, config: str=None,
                            client_username: str=None, 
                            client_password: str=None, 
                            client_host: str=None, 
                            client_port: int=None, 
                            database: str=None, 
                            collection: str=None,):
        """
        Connects to a MongoDB instance

        :param config: Optional config file containing the following params in JSON form.
        :param username: Database authentication username.
        :param password: Database authentication password.
        :param host: Database connection host name.
        :param port: Database connection port number.
        :param database: Name of database to connect to (do not confuse with collection name).
        :param collection: Name of collection to connect to.
        """

        if config:
            with open('config.json') as f:
                config_params = json.load(f)
                client_host=config_params['host']
                client_username=config_params['username']
                client_password=config_params['password']
                client_port=config_params['port']
                database=config_params['write_database_name']
                collection=config_params['write_collection_name']

        self.client=MongoClient(host=client_host,
                    port=client_port,
                    username=client_username,
                    password=client_password,
                    connect=True,
                    connectTimeoutMS=5000)
    
        self._database=self.client[database]
        self._collection=self._database[collection]
        # create timestamp index
        self._collection.create_index('timestamp', unique=True)
        try:
            self.client.admin.command('ping')
        except pymongo.errors.ConnectionFailure:
            raise ConnectionError("Could not connect to MongoDB using pymongo, check connection addresses")
        except pymongo.errors.OperationFailure:
            raise OperationalError("Could not connect to MongoDB using pymongo, check authentications")

    def write_to_mongo(self, staled_timestamps):
        """
        Performs bulk write to write all commands in staled_timestamps to MongoDB
        :params staled_timestamps: a list of MongoDB UpdateOne() commands (upsert = True)
        """
        try:
            self._collection.bulk_write(staled_timestamps,ordered=False)
        except BulkWriteError as bwe:
            pprint(bwe.details)
        print("[BatchUpdate] executed batch to MongoDB at time {}".format(str(time.time())))

    def clear_cache(self, MODE):
        """
        Returns a list of MongoDB commands from the remaining timestamps inside of _staleness dictionary
        :returns batch: a list of MongoDB UpdateOne() commands (upsert = True) that came from what was in cache
        """
        batch=[]
        temp_list = list(self._staleness)
        if MODE.value=="RAW":
            for key in temp_list:
                batch.append(
                    UpdateOne(
                        {'timestamp':key}, 
                        {
                            "$set":
                                {
                                    'timestamp':key,
                                    "configuration_id":self._cache_data[key][0]
                                }, 
                            "$push":
                                {
                                    'id':{'$each':self._cache_data[key][1]},
                                    'position':{'$each':self._cache_data[key][2]},
                                    'dimensions':{'$each':self._cache_data[key][3]}
                                }
                        }, upsert=True)
                    )

                print("[BatchUpdate] clearing from cache the timestamp: {} and inserting write command into batch".format(key))
                self._staleness.pop(key)
                self._cache_data.pop(key)

        elif MODE.value =="RECONCILED":
            for key in temp_list:
                batch.append(
                    UpdateOne(
                        {'timestamp':key},
                        {
                            "$set":
                                {
                                    'timestamp':key,
                                    "configuration_id":self._cache_data[key][0]
                                }, 
                            "$push":
                                {
                                    'id':{'$each':self._cache_data[key][1]},
                                    'position':{'$each':self._cache_data[key][2]}
                                }
                        }, upsert=True)
                    )
                
                print("[BatchUpdate] clearing from cache the timestamp: {} and inserting write command into batch".format(key))
                self._staleness.pop(key)
                self._cache_data.pop(key)

        return batch

    def add_to_cache(self, MODE, timestamp_dict: Dict = None):
        """
        Adds or appends subdocuments onto its respective time oriented document
            If MODE is RAW:
                {
                    'timestamp': 420.0,
                    'configuration_id': ABC,
                    'id': [id1, id2, ...],
                    'position': [(x1,y1), (x2,y2), ...]
                    'dimensions': [(w1,h1), (w2,h2), ...]
                }
            If MODE is RECONCILED:
                {
                    'timestamp': 420.0,
                    'configuration_id': ABC,
                    'id': [id1, id2, ...],
                    'position': [(x1,y1), (x2,y2), ...]
                }
        """
        staled_timestamps=[]
        subdoc_keys = list(timestamp_dict)

        if MODE.value == "RAW":
            for key in list(self._staleness):
                if key in timestamp_dict:
                    # insert & update
                    # timestamp_dict[key][0] is configuration_id -- skip
                    self._cache_data[key][1].append(timestamp_dict[key][1]) #object id
                    self._cache_data[key][2].append(timestamp_dict[key][2]) # (x, y)
                    self._cache_data[key][3].append(timestamp_dict[key][3]) # (l, w, h)
                    self._staleness[key] = 0
                    subdoc_keys.remove(key)
                else:
                    # current key does not exist in subdoc_key, so 
                    # increment its staleness
                    self._staleness[key] += 1
                    if(self._staleness[key]>=self.staleness_threshold and key <= next(iter(self._staleness))):
                        staled_timestamps.append(
                            UpdateOne(
                                {'timestamp':key},
                                {
                                    "$set":
                                        {
                                            'timestamp':key,
                                            'configuration_id':self._cache_data[key][0]
                                        }, 
                                    "$push":
                                        {
                                            'id':{'$each':self._cache_data[key][1]},
                                            'position':{'$each':self._cache_data[key][2]},
                                            'dimensions':{'$each':self._cache_data[key][3]}
                                        }
                                }, upsert=True)
                            )
                        print("[BatchUpdate] Automatically removing staled timestamp: {} and inserting write command into batch".format(key))
                        self._staleness.pop(key)
                        self._cache_data.pop(key)
            # new keys that are in subdoc but not in staleness
            # ... add to cache data
            for key in subdoc_keys:
                self._cache_data[key]=[]
                self._cache_data[key].append(timestamp_dict[key][0]) #config id
                self._cache_data[key].append([timestamp_dict[key][1]]) #object id
                self._cache_data[key].append([timestamp_dict[key][2]]) #x, y
                self._cache_data[key].append([timestamp_dict[key][3]]) #l,w,h
                self._staleness[key]=0
        
        elif MODE.value == "RECONCILED":
            for key in list(self._staleness):
                if key in timestamp_dict:
                    # insert & update
                    # timestamp_dict[key][0] is configuration_id -- skip
                    self._cache_data[key][1].append(timestamp_dict[key][1]) # object id
                    self._cache_data[key][2].append(timestamp_dict[key][2]) #x, y
                    self._staleness[key] = 0
                    subdoc_keys.remove(key)
                else:
                    # current key does not exist in subdoc_key, so 
                    # increment its staleness
                    self._staleness[key] += 1
                    if(self._staleness[key]>=self.staleness_threshold and key <= next(iter(self._staleness))):
                        staled_timestamps.append(
                            UpdateOne(
                                {'timestamp':key},
                                {
                                    "$set":
                                        {
                                            'timestamp':key,
                                            'configuration_id':self._cache_data[key][0]
                                        },
                                    "$push":
                                        {
                                            'id':{'$each':self._cache_data[key][1]},
                                            'position':{'$each':self._cache_data[key][2]}
                                        }
                                },upsert=True)
                            )
                        print("[BatchUpdate] Automatically removing staled timestamp: {} and inserting write command into batch".format(key))
                        self._staleness.pop(key)
                        self._cache_data.pop(key)
            # new keys that are in subdoc but not in staleness
            # ... add to cache data
            for key in subdoc_keys:
                self._cache_data[key]=[]
                self._cache_data[key].append(timestamp_dict[key][0]) #config id
                self._cache_data[key].append([timestamp_dict[key][1]]) #object id
                self._cache_data[key].append([timestamp_dict[key][2]]) #x, y
                self._staleness[key]=0
        else:
            raise ValueError("Invalid MODE, must be either 'RAW' or 'RECONCILED'")
            
        return staled_timestamps

    def main_loop(self, MODE, batch_update_connection: Queue):
        """
        Checks to see if any documents has been not updated for a threshold time
        and arranges the document to be inserted, then inserts them through bulk update
        """

        while (True):
            # print('getting from batch update')
            try:
                obj_from_transformation = batch_update_connection.get(timeout=5)
            except queue.Empty:
                if batch_update_connection.empty() and len(self._cache_data)>0:
                    writes_from_cache=self.clear_cache(MODE)
                    self.write_to_mongo(writes_from_cache)
                    print('emptied cache and executed batch to MongoBD')
                continue
            # print("mode in batch_udpate"+obj_from_transformation)
            
            staled_timestamps = self.add_to_cache(MODE, obj_from_transformation)
            # is_cache_emptied = False
            if staled_timestamps:
                self.write_to_mongo(staled_timestamps)

    def __del__(self):
        """
        Upon DBWriter deletion, close the client/connection.
        :return: None
        """
        try:
            self.client.close()
        except:
            pass

def run(MODE, batch_update_connection):
    batch_update_obj = BatchUpdate("config.json")
    batch_update_obj.main_loop(MODE, batch_update_connection)