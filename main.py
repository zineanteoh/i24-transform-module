"""
Created on Thu Jun 23, 2022

@author: teohz
"""

# from i24_database_api.DBReader import DBReader
from multiprocessing import Process, Queue
from transformation import *
from change_stream_reader import *
from batch_update import *
import json

def initialize_transformation():
    return

def test_function():
    arr_length = 30
    test_traj = {
        "_id": 'ObjectId("62a37ac518c10e37c2103c78")',
        "timestamp": [x/30 for x in range(arr_length)], 
        "x_position": [x for x in range(arr_length)],
        "y_position": [10 for x in range(arr_length)],
        "coarse_vehicle_class": 1,
        "first_timestamp": 0,
        "last_timestamp": 0.2,
    }
    testObj = Transformation()
    testObj.transform_trajectory(test_traj)

if __name__=="__main__":
    # (Optional) Uncomment the following two lines if config.json cannot be loaded 
    # path_to_directory = "/isis/home/teohz/Desktop/i24-transform-module"
    # os.chdir(path_to_directory)

    testing = False
    if testing:
        test_function()
    else:
        # initialize Transformation, BatchUpdate, and ChangeStreamReader
        transformation_obj = Transformation()
        batch_update_obj = BatchUpdate("config.json")
        chg_stream_reader_obj = ChangeStreamReader("config.json")

        # initialize Queue for multiprocessing
        # - change_stream_reader pushes trajectories to this queue, which transform would listen from
        # - transform pushes mongoDB operation requests to this queue, which batch_update would listen from
        change_stream_connection = Queue()
        batch_update_connection = Queue()
        
        # start all 3 child processes
        print("[Main] Starting Change Stream process...")
        proc_change_stream = Process(target=chg_stream_reader_obj.listen, args=(change_stream_connection, ))
        proc_change_stream.start()
        
        print("[Main] Starting Transformation process...")
        proc_transform = Process(target=transformation_obj.main_loop, args=(change_stream_connection, batch_update_connection, ))
        proc_transform.start()

        print("[Main] Starting Batch Update process...")
        proc_batch_update = Process(target=batch_update_obj.send_batch, args=(batch_update_connection, ))
        proc_batch_update.start()
        
        proc_change_stream.join()
        proc_transform.join()
        proc_batch_update.join()