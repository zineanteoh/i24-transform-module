"""
Created on Thu Jun 23, 2022

@author: teohz
"""

# from i24_database_api.DBReader import DBReader
from multiprocessing import Process, Queue
import transformation
import json

def initialize_DB(config):
    return 

if __name__=="__main__":
    # (Optional) Uncomment the following two lines if config.json cannot be loaded 
    # path_to_directory = "/isis/home/teohz/Desktop/i24-transform-module"
    # os.chdir(path_to_directory)

    # Load config file from json
    with open('config.json') as f:
        config = json.load(f)
    
    initialize_DB(config)

    # change_stream_reader pushes trajectories to this queue, which transform would listen from
    change_stream_connection = Queue()
    # transform pushes mongoDB operation requests to this queue, which batch_update would listen from
    batch_update_connection = Queue()
    
    print("[Main] Starting Change Stream process...")
    proc_change_stream = Process(target=None, args=(change_stream_connection, ))
    proc_change_stream.start()
    
    print("[Main] Starting Transformation process...")
    proc_transform = Process(target=None, args=(change_stream_connection, batch_update_connection, ))
    proc_transform.start()

    print("[Main] Starting Batch Update process...")
    proc_batch_update = Process(target=None, args=(batch_update_connection, ))
    proc_batch_update.start()
    
    proc_change_stream.join()
    proc_transform.join()
    proc_batch_update.join()