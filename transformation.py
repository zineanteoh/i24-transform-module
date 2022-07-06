"""
Created on Thu Jun 23, 2022

@author: teohz
"""

import multiprocessing
import math
import time

import pymongo
import json
import random

def round_and_truncate(number, digits) -> float:
    # Improve accuracy with floating point operations, to avoid truncate(16.4, 2) = 16.39 or truncate(-1.13, 2) = -1.12
    number = round(number, 7)
    nbDecimals = len(str(number).split('.')[1]) 
    if nbDecimals <= digits:
        return number
    stepper = 10.0 ** digits
    return math.trunc(stepper * number) / stepper

def direct_feed(config, benchmark_cap, is_shuffled):
    client=None
    if config:
         with open('config.json') as f:
                config_params = json.load(f)
                client_host=config_params['host']
                client_username=config_params['username']
                client_password=config_params['password']
                client_port=config_params['port']
                database=config_params['read_database_name']
                collection=config_params['read_collection_name']

                client=pymongo.MongoClient(host=client_host,
                    port=client_port,
                    username=client_username,
                    password=client_password,
                    connect=True,
                    connectTimeoutMS=5000)
        
    else:
        raise Exception('please enter config file')

    _database=client[database]
    _collection=_database[collection]
    cursor= _collection.find({},{"_id":1,"timestamp":1,"x_position":1,"y_position":1}).sort([("first_timestamp",pymongo.ASCENDING),("last_timestamp",pymongo.ASCENDING)]).limit(benchmark_cap)
    if is_shuffled:
        print('starting shuffle')
        cursor=list(cursor)
        random.shuffle(cursor)
        print('ending shuffle')
    return cursor

class Transformation:
    def __init__(self, sample_rate = 30, is_benchmark_on=False, benchmark_cap=499):
        self.SAMPLE_RATE = sample_rate
        self.valid_sample_rate = 30 % self.SAMPLE_RATE == 0 and self.SAMPLE_RATE > 0
        if (not self.valid_sample_rate):
            print("[Transformation] Init ERROR: Sample rate is not valid. Needs to be a positive factor of 30")
        
        self._is_benchmark_on = is_benchmark_on
        if self._is_benchmark_on:
            self._benchmark_cap = benchmark_cap

    def transform_trajectory(self, traj):
        """
        Accepts a trajectory document as parameter and return a dictionary of 
        timestamps with the following schema: 
            {
                t1: [id1, x1, y1],
                t2: [id2, x2, y2],
                ...
            }
        where 
        - id is the vehicle's ObjectID from mongoDB, 
        - x, y are the positions of the vehicle at time t1
        - t1 is the timestamp
        """
        if not self.valid_sample_rate:
            print("[Transformation] Transform ERROR: Sample rate is not valid. Needs to be a positive factor of 30")
            return

        vehicle_id = traj["_id"]
        batch_operations = {}
        for i in range(len(traj["timestamp"])):
            time = round_and_truncate(traj["timestamp"][i], 5)
            x = traj["x_position"][i]
            y = traj["y_position"][i]
            batch_operations[time] = [vehicle_id, x, y]
        # first_key = list(batch_operations.keys())[0]
        # print("transformed doc into: {}".format(batch_operations[first_key]))
        return batch_operations

    def main_loop(self, change_stream_connection: multiprocessing.Queue=None, batch_update_connection: multiprocessing.Queue=None):
        """
        A child process for transformation. 
        1. Listens to change_stream_connection for trajectory documents. 
        2. Transforms the received trajectory into a dictionary of timestamps: 
            {
                t1: [id1, x1, y1],
                t2: [id2, x2, y2],
                ...
            }
        3. Sends the dictionary of timestamp to batch_update
        """
        if not self.valid_sample_rate:
            print("[Transformation] Main Loop ERROR: Sample rate is not valid. Needs to be a positive factor of 30")
            return
        
        if self._is_benchmark_on:
            count = 0
            transform_time = 0
            send_batch_time=0
            st=time.time()
        
        while True:
            if self._is_benchmark_on:
                cursor=direct_feed(config="config.json",benchmark_cap=self._benchmark_cap,is_shuffled=True)
                # print (len(cursor))
                # break
                st_after_cursor=time.time()
                for traj_doc in cursor:
                    # print(traj_doc)
                    # break

                    time1=time.time()

                    batch_operations = self.transform_trajectory(traj_doc)
                    time2=time.time()
                    batch_update_connection.put(batch_operations)

                    time3=time.time()
                    count+=1
                    transform_time+=time2-time1
                    send_batch_time+=time3-time2

                
                et=time.time()
                print('[Transformation] time for entire transformation process: '+str(et-st))
                print('[Transformation] time to generate cursor: '+str(st_after_cursor-st))
                print('[Transformation] time for only transform operation: '+str(transform_time))
                print('[Transformation] time for only adding the batch: '+str(send_batch_time))
                break
            else:
                traj_doc = change_stream_connection.get()
                batch_operations = self.transform_trajectory(traj_doc)
                batch_update_connection.put(batch_operations)
    
def run(change_stream_connection, batch_update_connection, is_benchmark_on, benchmark_cap):
    transformation_obj = Transformation(is_benchmark_on=is_benchmark_on, benchmark_cap=benchmark_cap)
    transformation_obj.main_loop(batch_update_connection=batch_update_connection)