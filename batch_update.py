"""
Created on Thu Jun 23
@author: lisaliuu

"""
from typing import Dict
from pymongo import MongoClient
import urllib.parse
from pymongo.errors import BulkWriteError
import time
from collections import OrderedDict

client=MongoClient()
col=client['test']['test']
class BatchUpdate:
    def __init__(self, client_username, client_password, client_ip, database, collection, idle_time_before_insert):
        self.client=MongoClient('mongodb://%s:%s@'+str(client_ip) % (client_username, client_password))
        self.database=client[database]
        self.collection=database[collection]
        self._cache_data=OrderedDict()
        self._staleness=OrderedDict()
        self.idle_time_before_insert=1000
    
    def connect_to_db():
        """
        define later
        """
    
    def add_to_batch(self,subdoc: Dict = None):
        for key, value in subdoc.keys:
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
        batch=OrderedDict()
        
        def first(od):
            """
            Return the first element from an ordered collection
            or an arbitrary element from an unordered collection.
            Raise StopIteration if the collection is empty.
            """
            return next(iter(od))

        def process_for_insert(key,value):
            """
            Restructures the document for insertion
            """

        while (True):
            current_time=time.time()
            first_time_in_stale=first(self._staleness)
            if(current_time-first_time_in_stale>self.idle_time_before_insert):
                batch
            
            time.sleep(10)

