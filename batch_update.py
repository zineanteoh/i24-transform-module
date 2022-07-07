"""
Created on Thu Jun 23
@author: lisaliuu

"""
from os import times_result
from re import L
from sqlite3 import OperationalError
from typing import Dict
import pymongo
from pymongo import MongoClient, UpdateOne, InsertOne
import json
from pymongo.errors import BulkWriteError
# from time import thread_time
from collections import OrderedDict
import time
from pprint import pprint
from multiprocessing import Queue
import queue
import matplotlib.pyplot as plt
from numpy import diff


class BatchUpdate:
    def __init__(self, config, staleness_threshold=25, 
                        wait_time=0, is_benchmark_on=False):# benchmark_cap=499):
        """
        :param staleness_threshold: Number of new documents read that do not update a time until that
        time is inserted to the transformed collection
        :param wait_time: Time in seconds between reads from transformation
        """
        self._cache_data={}
        self._staleness={}
        self.staleness_threshold=staleness_threshold
        self.wait_time=wait_time
        self._is_benchmark_on = is_benchmark_on

        if self._is_benchmark_on:
            # self._benchmark_cap = benchmark_cap
            self._start_all=time.time()
            self._plt_out_rate=[]
            self._plt_in_rate=[]
            self._plt_time=[]

        self.num_of_updates=0

        self.connect_to_db(config)
    
    def connect_to_db(self, config: str=None,
                            client_username: str=None, 
                            client_password: str=None, 
                            client_host: str=None, 
                            client_port: int=None, 
                            database_write: str=None,
                            database_read: str=None,
                            collection_write: str=None,
                            collection_read: str=None):
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
                database_write=config_params['write_database_name']
                collection_write=config_params['write_collection_name']
                database_read=config_params['read_database_name']
                collection_read=config_params['read_collection_name']

        self.client=MongoClient(host=client_host,
                    port=client_port,
                    username=client_username,
                    password=client_password,
                    connect=True,
                    connectTimeoutMS=5000)
    
        self._write_database=self.client[database_write]
        self._write_collection=self._write_database[collection_write]
        self._read_database=self.client[database_read]
        self._read_collection=self._read_database[collection_read]

        self._write_collection.create_index([('timestamp',1)])
        
        try:
            self.client.admin.command('ping')
        except pymongo.errors.ConnectionFailure:
            raise ConnectionError("Could not connect to MongoDB using pymongo, check connection addresses")
        except pymongo.errors.OperationFailure:
            raise OperationalError("Could not connect to MongoDB using pymongo, check authentications")

    def write_to_mongo(self, staled_timestamps):
        try:
            result=self._write_collection.bulk_write(staled_timestamps,ordered=False)
        except BulkWriteError as bwe:
            pprint(bwe.details)
        # pprint(result.bulk_api_result)
        new_updates=result.bulk_api_result['nModified']
        self.num_of_updates+=new_updates
        if new_updates!=0:
            print("[BatchUpdate] inserted batch at time {}".format(str(time.time()))+" *updates performed")
        else:
            print("[BatchUpdate] inserted batch at time {}".format(str(time.time())))
        print("[BatchUpdate] size of cache between batch_update and writing to mongo when stalled: "+str(len(self._cache_data)))
        # print('number of documents modified '+str(result.bulk_api_result['nModified']))

    def clear_cache(self):
        print('clearing cache')
        batch=[]
        for key in list(self._staleness):
            batch.append(UpdateOne({'timestamp':key},{"$set":{'timestamp':key},"$push":{'id':{'$each':self._cache_data[key][0]},'x_position':{'$each':self._cache_data[key][1]},'y_position':{'$each':self._cache_data[key][2]}}},upsert=True))
            # print("[BatchUpdate] ladies and gentleman we have our first staled timestamp: {}".format(key))
            self._staleness.pop(key)
            self._cache_data.pop(key)
        return batch

    def add_to_cache(self, subdoc: Dict = None,):
        """
        Adds or appends subdocuments onto its respective
        time oriented document
        """
        # v1
        batch=[]
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
                if(self._staleness[key]>=self.staleness_threshold): #and key <= next(iter(self._staleness))):
                    batch.append(UpdateOne({'timestamp':key},{"$set":{'timestamp':key},"$push":{'id':{'$each':self._cache_data[key][0]},'x_position':{'$each':self._cache_data[key][1]},'y_position':{'$each':self._cache_data[key][2]}}},upsert=True))
                    # print("[BatchUpdate] ladies and gentleman we have our first staled timestamp: {}".format(key))
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

        return batch

    def send_batch(self, batch_update_connection: Queue):
        """
        Checks to see if any documents has been not updated for a threshold time
        and arranges the document to be inserted, then inserts them through bulk update
        """

        if self._is_benchmark_on:
            add_to_cache_time=0
            insert_mongo_time=0
            st=time.time()
            count=0
        while (True):
            try:
                obj_from_transformation = batch_update_connection.get(timeout=10)
            except queue.Empty:
                print('connection timed out, emptying cache')
                if self._is_benchmark_on:
                    et=time.time()
                    print("[BatchUpdate] time taken for the entire process including wait time: {}".format(et-st))
                    print('[BatchUpdate] time to add to cache: '+str(add_to_cache_time))
                    print("[BatchUpdate] time to write to database "+str(insert_mongo_time))

                    ccst=time.time()
                    self.write_to_mongo(self.clear_cache())
                    ccet=time.time()
                    print('[BatchUpdate] time to empty cache: '+str(ccet-ccst))
                    print('[BatchUpdate] number of updates: '+str(self.num_of_updates))
                    print('[TOTAL] time to finish entire process: '+str(ccet-self._start_all))
                    break
                else:
                    self.write_to_mongo(self.clear_cache())
                    print('cemptied cache')
                    break

            if self._is_benchmark_on:
                time1=time.time()
            staled_timestamps = self.add_to_cache(obj_from_transformation)
            if self._is_benchmark_on:
                count+=1
                time2=time.time()
            if staled_timestamps:
                self.write_to_mongo(staled_timestamps)
            if self._is_benchmark_on:
                time3=time.time()
                add_to_cache_time+=time2-time1
                insert_mongo_time+=time3-time2
                # if count>=self._benchmark_cap:
                #     # print(obj_from_transformation)
                #     et=time.time()
                #     print("[BatchUpdate] time taken for the entire process including wait time: {}".format(et-st))
                #     print('[BatchUpdate] time to add to cache: '+str(add_to_cache_time))
                #     print("[BatchUpdate] time to write to database "+str(insert_mongo_time))

                #     ccst=time.time()
                #     self.write_to_mongo(self.clear_cache())
                #     ccet=time.time()
                #     print('[BatchUpdate] time to empty cache: '+str(ccet-ccst))
                #     print('[BatchUpdate] number of updates: '+str(self.num_of_updates))
                #     print('[TOTAL] time to finish entire process: '+str(ccet-self._start_all))
                #     break

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

    def benchmark_graphing(self):
        _plt_out_rate=[]
        _plt_in_rate=[]
        _plt_out_docs=[]
        _plt_in_docs=[]
        times=[]
        write_end_docs=0
        read_end_docs=0
        time_step=0
        while True:
            # write_start_docs=write_end_docs
            # read_start_docs=read_end_docs
            dx=1
            time.sleep(dx)
            write_end_docs=self._write_collection.count_documents({})
            read_end_docs=self._read_collection.count_documents({})
            time_step+=dx
            # _plt_out_rate.append(write_end_docs-write_start_docs)
            # _plt_in_rate.append(read_end_docs-read_start_docs)
            _plt_out_docs.append(write_end_docs)
            _plt_in_docs.append(read_end_docs)
            _plt_in_rate=diff(_plt_in_docs)/dx
            _plt_out_rate=diff(_plt_out_docs)/dx
            times.append(time_step)
            if write_end_docs>=18900:
                print(_plt_in_rate)
                times.pop()
                plt.plot(times, _plt_out_rate,label='output rate')
                plt.plot(times, _plt_in_rate,label='input rate')
                plt.title('Insert Rates vs Time')
                plt.xlabel('time (s)')
                plt.ylabel('rate insertion')
                plt.legend(loc="upper left")
                plt.show()
                break


def run_batch_update(batch_update_connection, is_benchmark_on):#, benchmark_cap):
    batch_update_obj = BatchUpdate(config="config.json", is_benchmark_on=is_benchmark_on)#, benchmark_cap=benchmark_cap)
    batch_update_obj.send_batch(batch_update_connection)

def run_benchmark_graphing():
    benchmark_graphing_obj=BatchUpdate(config="config.json")
    benchmark_graphing_obj.benchmark_graphing()