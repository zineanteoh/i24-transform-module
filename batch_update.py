"""
Created on Thu Jun 23
@author: lisaliuu

"""
# from logging import exception
# from pydoc import cli
from re import L
from sqlite3 import OperationalError
from typing import Dict
import pymongo
from pymongo import MongoClient, UpdateOne, InsertOne
import json
from pymongo.errors import BulkWriteError
# from time import thread_time
import time
# from collections import OrderedDict
from pprint import pprint
from multiprocessing import Queue
# import math


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

    def add_to_cache(self, subdoc: Dict = None,):
        """
        Adds or appends subdocuments onto its respective
        time oriented document
        """
        # v1
        batch=[]
        subdoc_keys = list(subdoc)
        for key in self._staleness:
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
                if(self._staleness[key]>=self.staleness_threshold):
                    batch.append(UpdateOne({'timestamp':key},{"$set":{'timestamp':key},"$push":{'id':{'$each':self._cache_data[key][0]},'x_position':{'$each':self._cache_data[key][1]},'y_position':{'$each':self._cache_data[key][2]}}},upsert=True))
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
            subdoc_keys.remove(key)

        # v2
        # for key, value in subdoc.items():
        #     if key in self._cache_data.keys(): 
        #         self._cache_data[key][0].append(value[0])
        #         self._cache_data[key][1].append(value[1])
        #         self._cache_data[key][2].append(value[2])
        #     else:
        #         self._cache_data[key]=[]
        #         self._cache_data[key].append([value[0]]) #id
        #         self._cache_data[key].append([value[1]]) #x
        #         self._cache_data[key].append([value[2]]) #y
        #     self._staleness[key]=0
        return batch

    def send_batch(self, batch_update_connection: Queue):
        """
        Checks to see if any documents has been not updated for a threshold time
        and arranges the document to be inserted, then inserts them through bulk update
        """
        
        # def first(od):
        #     """
        #     Return the first element from an ordered collection
        #     or an arbitrary element from an unordered collection.
        #     Raise StopIteration if the collection is empty.
        #     """
        #     return next(iter(od))
        
        tot_ind_time=0
        tot_ind_cache_time=0
        tot_ind_write=0
        st=time.time()
        while (True):

            obj_from_transformation = batch_update_connection.get()
            ind_st_cache=time.time()
            staled_timestamps = self.add_to_cache(obj_from_transformation)

            # current_time=time.time()
            # while(self._cache_data and ((current_time-first(self._staleness.values()))>self.buffer_time)):
            #     stale_key=first(self._staleness)
            #     stale_value=self._cache_data[stale_key]
                # print(stale_key)
                # print(stale_value)
                # batch.append(UpdateOne({'time':stale_key},{"$set":{'time':stale_key},"$push":{'id':{'$each':stale_value[0]},'x_position':{'$each':stale_value[1]},'y_position':{'$each':stale_value[2]}}},upsert=True))
                # # self._collection.update_many
                # # print('appended to batch '+str(stale_key))
                # self._staleness.popitem(last=False)
                # self._cache_data.pop(stale_key)
            ind_et_cache=time.time()
            if staled_timestamps:
                # print(str(len(batch))+" documents in batch to insert")

                try:
                    result=self._collection.bulk_write(staled_timestamps,ordered=False)
                except BulkWriteError as bwe:
                    pprint(bwe.details)
                
                # pprint(result.bulk_api_result)
                print("inserted batch at time "+str(time.time()))
                staled_timestamps.clear()
            ind_et_write=time.time()

            tot_ind_cache_time+=ind_et_cache-ind_st_cache
            tot_ind_write+=ind_et_write-ind_et_cache
            # else:
            #     print('Nothing has passed time threshold')

            # print('checked '+str(count))
            time.sleep(self.wait_time)

            if self._collection.count_documents({})==17995:
                et=time.time()
                print("time to cache, batch, and insert: "+str(tot_ind_time))
                print('time to add to cache: '+str(tot_ind_cache_time)+" time to write to database "+str(tot_ind_write))


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