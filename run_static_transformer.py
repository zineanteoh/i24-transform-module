"""
Created on Thu Jun 23, 2022

@author: teohz
"""

# from i24_database_api.DBReader import DBReader
from multiprocessing import Process, Queue
import transformation 
import change_stream_reader 
import batch_update

if __name__=="__main__":
    # (Optional) Uncomment the following two lines if config.json cannot be loaded 
    # path_to_directory = "/isis/home/teohz/Desktop/i24-transform-module"
    # os.chdir(path_to_directory)

    # initialize Queue for multiprocessing
    # - transform pushes mongoDB operation requests to this queue, which batch_update would listen from
    batch_update_connection = Queue()
    
    # start 2 child processes
    print("[Main] Starting Transformation process...")
    proc_transform = Process(target=transformation.run, args=(None, batch_update_connection, ))
    proc_transform.start()
    
    print("[Main] Starting Batch Update process...")
    proc_batch_update = Process(target=batch_update.run, args=(batch_update_connection, ))
    proc_batch_update.start()
    
    proc_transform.join()
    proc_batch_update.join()