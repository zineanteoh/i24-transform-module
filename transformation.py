"""
Created on Thu Jun 23, 2022

@author: teohz
"""

import multiprocessing
import math
import time

def round_and_truncate(number, digits) -> float:
    # Improve accuracy with floating point operations, to avoid truncate(16.4, 2) = 16.39 or truncate(-1.13, 2) = -1.12
    number = round(number, 7)
    nbDecimals = len(str(number).split('.')[1]) 
    if nbDecimals <= digits:
        return number
    stepper = 10.0 ** digits
    return math.trunc(stepper * number) / stepper

class Transformation:
    def __init__(self, sample_rate = 30):
        # sample rate per second (must be a positive factor of 30)
        self.SAMPLE_RATE = sample_rate
        self.valid_sample_rate = 30 % self.SAMPLE_RATE == 0 and self.SAMPLE_RATE > 0
        if (not self.valid_sample_rate):
            print("[Transformation] Init ERROR: Sample rate is not valid. Needs to be a positive factor of 30")

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
        # count+=1
        vehicle_id = traj["_id"]
        batch_operations = {}
        for i in range(len(traj["timestamp"])):
            time = round_and_truncate(traj["timestamp"][i], 5)
            x = traj["x_position"][i]
            y = traj["y_position"][i]
            batch_operations[time] = [vehicle_id, x, y]
        first_key = list(batch_operations.keys())[0]
        # print("transformed doc into: {}".format(batch_operations[first_key]))
        return batch_operations

    def main_loop(self, change_stream_connection: multiprocessing.Queue, batch_update_connection: multiprocessing.Queue, test_cap):
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
        st=time.time()
        count=0
        tot_ind_time=0
        while True:
            traj_doc = change_stream_connection.get()
            ind_st=time.time()
            # print("[transformation] received doc")
            batch_operations = self.transform_trajectory(traj_doc)
            count+=1
            batch_update_connection.put(batch_operations)
            ind_et=time.time()
            tot_ind_time+=ind_et-ind_st
            
            if count==test_cap:
                et=time.time()
                print('transformation and sending to batch queue took '+str(et-st))
                print('individual operations once gotten data is: '+str(tot_ind_time))
    
def run(change_stream_connection, batch_update_connection, test_cap):
    transformation_obj = Transformation()
    transformation_obj.main_loop(change_stream_connection, batch_update_connection, test_cap)