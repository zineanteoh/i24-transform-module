"""
Created on Thu Jun 23
@author: lisaliuu

"""
from pydoc import cli
from re import L
from typing import Dict
import pymongo
from pymongo import MongoClient, UpdateOne, InsertOne
import urllib.parse
from pymongo.errors import BulkWriteError
import time
from collections import OrderedDict
from pprint import pprint

# client=MongoClient()
# col=client['test']['test']
class BatchUpdate:
    def __init__(self, client_username: str=None, client_password: str=None, client_host: str=None, client_port: int=None, database: str=None, collection: str=None, idle_time_before_insert=6):
        self.client=MongoClient(host=client_host,
                                port=client_port,
                                username=client_username,
                                password=client_password,
                                connect=True)
        # if (not self.client):
        #     print('MongoDB connection failed')
        # else:
        #     print("ok")

        try:
            self.client.admin.command('ping')
        except pymongo.errors.ConnectionFailure:
            raise ConnectionError("Could not connect to MongoDB using pymongo.")
        
        self._database=self.client[database]
        self._collection=self._database[collection]
        # self.collection=database[collection]
        self._cache_data={}
        self._staleness=OrderedDict()
        self.idle_time_before_insert=idle_time_before_insert
    
    def connect_to_db():
        """
        define later
        """
    
    def add_to_batch(self,subdoc: Dict = None):
        """
        Adds or appends subdocuments onto its respective
        time oriented document
        """
        for key, value in subdoc.items():
            if key in self._cache_data.keys(): 
                self._cache_data[key][0].append(value[0])
                self._cache_data[key][1].append(value[1])
                self._cache_data[key][2].append(value[2])
                self._staleness[key]=time.time()
            else:
                self._cache_data[key]=[]
                self._cache_data[key].append([value[0]]) #id
                self._cache_data[key].append([value[1]]) #x
                self._cache_data[key].append([value[2]]) #y
                self._staleness[key]=time.time()


    def send_batch(self):
        
        def get_time():
            return time.time()
        def first(od):
            """
            Return the first element from an ordered collection
            or an arbitrary element from an unordered collection.
            Raise StopIteration if the collection is empty.
            """
            return next(iter(od))

        def process_for_insert(key,value):
            """
            Changes the format of the document that is aggregated
            from individual points into a dictionary that can be
            inserted into MongoDB
            """
            final_dict={}
            # final_dict['time']=key
            final_dict['id']=value[0]
            final_dict['x_position']=value[1]
            final_dict['y_position']=value[2]   
            return(final_dict)

        batch=[]
        count=0
        while (True):
            if count==3:
                testDoc12={}
                testDoc12[1]=[10,10,'x']
                self.add_to_batch(testDoc12)
            current_time=time.time()
            # print(current_time-first(self._staleness.values()))
            while(self._cache_data and ((current_time-first(self._staleness.values()))>self.idle_time_before_insert)):
                # print(self._cache_data[1])
                # print("should be 1:"+str(first(self._staleness)))
                stale_key=first(self._staleness)
                stale_value=self._cache_data[stale_key]
                # print(stale_key)
                # print(stale_value)
                insert_doc=process_for_insert(stale_key,stale_value)
                batch.append(UpdateOne({'time':stale_key},{"$set":{'time':stale_key},"$push":{'id':{'$each':stale_value[0]},'x_position':{'$each':stale_value[1]},'y_position':{'$each':stale_value[2]}}},upsert=True))
                self._collection.update_many
                print('appended '+str(stale_key))
                self._staleness.popitem(last=False)
                self._cache_data.pop(stale_key)
            
            if batch:
                print(batch)

                try:
                    result=self._collection.bulk_write(batch,ordered=False)

                except BulkWriteError as bwe:
                    pprint(bwe.details)
                pprint(result.bulk_api_result)
                print("inserted batch at time "+str(time.time()))
                batch.clear()
            else:
                print('nothing has passed time threshold')
            count+=1
            print('checked '+str(count))
            time.sleep(5)
    

    def __del__(self):
        """
        Upon DBWriter deletion, close the client/connection.
        :return: None
        """
        try:
            self.client.close()
        except:
            pass

