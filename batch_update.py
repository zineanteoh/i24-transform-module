"""
Created on Thu Jun 23
@author: lisaliuu

"""
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
    def __init__(self, config, staleness_threshold=2, 
                        wait_time=0):
        """
        :param staleness_threshold: Number of new documents read that do not update a time until that
        time is inserted to the transformed collection
        :param wait_time: Time in seconds between reads from transformation
        """
        self._cache_data={}
        self._staleness={}
        self.staleness_threshold=staleness_threshold
        self.wait_time=wait_time
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
                client_host=config_params['host']
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
        print("[BatchUpdate] inserted batch at time {}".format(str(time.time())))

    def clear_cache(self):
        """
        Returns a list of MongoDB commands from the remaining timestamps inside of _staleness dictionary
        :returns batch: a list of MongoDB UpdateOne() commands (upsert = True)
        """
        batch=[]
        for key in list(self._staleness):
            batch.append(UpdateOne({'timestamp':key},{"$set":{'timestamp':key},"$push":{'id':{'$each':self._cache_data[key][0]},'x_position':{'$each':self._cache_data[key][1]},'y_position':{'$each':self._cache_data[key][2]}}},upsert=True))
            print("[BatchUpdate] clearing from cache the timestamp: {}".format(key))
            self._staleness.pop(key)
            self._cache_data.pop(key)
        return batch

    def add_to_cache(self, subdoc: Dict = None):
        """
        Adds or appends subdocuments onto its respective time oriented document
        """
        staled_timestamps=[]
        subdoc_keys = list(subdoc)
        for key in list(self._staleness):
            if key in subdoc:
                # insert & update
                self._cache_data[key][0].append(subdoc[key][0])
                self._cache_data[key][1].append(subdoc[key][1])
                self._cache_data[key][2].append(subdoc[key][2])
                self._staleness[key] = 0
                subdoc_keys.remove(key)
            else:
                # current key does not exist in subdoc_key, so 
                # increment its staleness
                self._staleness[key] += 1
                if(self._staleness[key]>=self.staleness_threshold and key <= next(iter(self._staleness))):
                    staled_timestamps.append(UpdateOne({'timestamp':key},{"$set":{'timestamp':key},"$push":{'id':{'$each':self._cache_data[key][0]},'x_position':{'$each':self._cache_data[key][1]},'y_position':{'$each':self._cache_data[key][2]}}},upsert=True))
                    print("[BatchUpdate] Automatically removing staled timestamp: {}".format(key))
                    self._staleness.pop(key)
                    self._cache_data.pop(key)
        # new keys that are in subdoc but not in staleness
        # ... add to cache data
        for key in subdoc_keys:
            self._cache_data[key]=[]
            self._cache_data[key].append([subdoc[key][0]]) #id
            self._cache_data[key].append([subdoc[key][1]]) #x
            self._cache_data[key].append([subdoc[key][2]]) #y
            self._staleness[key]=0
        return staled_timestamps

    def send_batch(self, batch_update_connection: Queue):
        """
        Checks to see if any documents has been not updated for a threshold time
        and arranges the document to be inserted, then inserts them through bulk update
        """

        while (True):
            try:
                obj_from_transformation = batch_update_connection.get(timeout=5)
            except queue.Empty:
                self.write_to_mongo(self.clear_cache())
                print('emptied cache')
            staled_timestamps = self.add_to_cache(obj_from_transformation)
            if staled_timestamps:
                self.write_to_mongo(staled_timestamps)
            time.sleep(self.wait_time)

    def __del__(self):
        """
        Upon DBWriter deletion, close the client/connection.
        :return: None
        """
        try:
            self.client.close()
        except:
            pass

def run(batch_update_connection):
    batch_update_obj = BatchUpdate("config.json")
    batch_update_obj.send_batch(batch_update_connection)