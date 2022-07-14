"""
Created on Thu Jun 23, 2022

@author: teohz
"""

import multiprocessing
import pandas as pd
import pymongo
import math
import json

def round_and_truncate(number, digits) -> float:
    '''
    A helper function to round number to 5 digits and truncate the rest
    i.e. round_and_truncate(13921.0333333333) returns 13921.03333
    '''
    # Improve accuracy with floating point operations, to avoid truncate(16.4, 2) = 16.39 or truncate(-1.13, 2) = -1.12
    number = round(number, 7)
    nbDecimals = len(str(number).split('.')[1]) 
    if nbDecimals <= digits:
        return number
    stepper = 10.0 ** digits
    return math.trunc(stepper * number) / stepper

def resample(car, MODE):
    '''
    Original author: yanbing_wang
    resample the original time-series to uniformly sampled time series in 25Hz
    leave nans for missing data
    :param car: car document from MongoDB, containing field 'timestamp', 'x_position', 'y_position'
    '''

    # Select time series only
    if MODE.value == "RAW":
        try:
            time_series_field = ["timestamp", "x_position", "y_position", "length", "width", "height"]
            data = {key: car[key] for key in time_series_field}

            # Read to dataframe and resample
            df = pd.DataFrame(data, columns=data.keys()) 
            index = pd.to_timedelta(df["timestamp"], unit='s')
            df = df.set_index(index)
            df = df.drop(columns = "timestamp")
            # df = df.resample('0.04s').mean() # close to 25Hz
            df=df.groupby(df.index.floor('0.04S')).mean().resample('0.04S').asfreq()
            df.index = df.index.values.astype('datetime64[ns]').astype('int64')*1e-9
            df = df.interpolate(method='linear')

            car['x_position'] = df['x_position'].values
            car['y_position'] = df['y_position'].values
            car['length'] = df['length'].values
            car['width'] = df['width'].values
            car['height'] = df['height'].values
            car['timestamp'] = df.index.values
        except Exception as e:
            print("error resampling: {}".format(e))
        return car

    elif MODE.value == "RECONCILED":
        try:
            time_series_field = ["timestamp", "x_position", "y_position"]
            data = {key: car[key] for key in time_series_field}

            # Read to dataframe and resample
            df = pd.DataFrame(data, columns=data.keys()) 
            index = pd.to_timedelta(df["timestamp"], unit='s')
            df = df.set_index(index)
            df = df.drop(columns = "timestamp")
            # df = df.resample('0.04s').mean() # close to 25Hz
            df=df.groupby(df.index.floor('0.04S')).mean().resample('0.04S').asfreq()
            df.index = df.index.values.astype('datetime64[ns]').astype('int64')*1e-9
            df = df.interpolate(method='linear')

            car['x_position'] = df['x_position'].values
            car['y_position'] = df['y_position'].values
            car['timestamp'] = df.index.values
        except Exception as e:
            print("error resampling: {}".format(e))
        return car
    
    else:
        raise Exception("Unable to determine whether data is RAW or RECONCILED trajectories. Aborting program")

class Transformation:
    def __init__(self, is_collection_dynamic, sample_rate = 25):
        self._is_collection_dynamic = is_collection_dynamic
        # sample rate per second (must be a positive factor of 30)
        self.SAMPLE_RATE = sample_rate
    
    def read_static_collection(self, config, num_of_docs = None):
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
            raise Exception('Unable to load config.json')

        _database=client[database]
        _collection=_database[collection]

        if num_of_docs:
            cursor= _collection.find({},{"_id":1,"timestamp":1,"x_position":1,"y_position":1, "configuration_id":1,"length":1,"width":1,"height":1}).sort([("first_timestamp",pymongo.ASCENDING),("last_timestamp",pymongo.ASCENDING)]).limit(num_of_docs)
        else:
            cursor= _collection.find({},{"_id":1,"timestamp":1,"x_position":1,"y_position":1,"configuration_id":1,"length":1,"width":1,"height":1}).sort([("first_timestamp",pymongo.ASCENDING),("last_timestamp",pymongo.ASCENDING)])
        
        return cursor

    def determine_mode(self, first_doc):
        '''
        Determine the mode of the data by checking the 'length' field of the first document
        '''
        try:
            if isinstance(first_doc['length'], list):
                return "RAW"
            else:
                return "RECONCILED"
        except KeyError:
            raise Exception("Unable to determine whether data is RAW or RECONCILED trajectories. Aborting program")
        
    def transform_trajectory(self, MODE, traj):
        """
        Accepts MODE and trajectory document as parameters
        Returns a dictionary of timestamps with the following schema: 
            If MODE is RAW:
                {
                    time : [config_id, vehicle_ObjectID, (x, y), (l, w, h)],
                    time : [config_id, vehicle_ObjectID, (x, y), (l, w, h)],
                    ...
                }
            If MODE is RECONCILED:
                {
                    time : [config_id, vehicle_ObjectID, (x, y)],
                    time : [config_id, vehicle_ObjectID, (x, y)],
                    ...
                }
        """
        vehicle_id = traj["_id"]
        configuration_id = traj["configuration_id"]
        batch_operations = {}
        if MODE.value == "RAW":
            # transform raw data
            for i in range(len(traj["timestamp"])):
                time = round_and_truncate(traj["timestamp"][i], 5)
                # print(len(traj["timestamp"]), len(traj["x_position"]), len(traj["y_position"]), len(traj["length"]), len(traj["width"]), len(traj["height"]))
                x = traj["x_position"][i]
                y = traj["y_position"][i]
                l = traj["length"][i]
                w = traj["width"][i]
                h = traj["height"][i]
                batch_operations[time] = [configuration_id, vehicle_id, (x, y), (l, w, h)]
        elif MODE.value == "RECONCILED":
            # transform reconciled data
            for i in range(len(traj["timestamp"])):
                time = round_and_truncate(traj["timestamp"][i], 5)
                x = traj["x_position"][i]
                y = traj["y_position"][i]
                batch_operations[time] = [configuration_id, vehicle_id, (x, y)]
        else:
            raise Exception("Unable to determine whether data is RAW or RECONCILED trajectories. Aborting program")
        
        return batch_operations

    def main_loop(self, MODE, change_stream_connection: multiprocessing.Queue, batch_update_connection: multiprocessing.Queue):
        """
        A child process for transformation. 
        1. Listens to change_stream_connection for trajectory documents. 
        2. Transforms the received trajectory into a dictionary of timestamps: 
            If MODE is RAW:
                {
                    time : [config_id, vehicle_ObjectID, (x, y), (l, w, h)],
                    time : [config_id, vehicle_ObjectID, (x, y), (l, w, h)],
                    ...
                }
            If MODE is RECONCILED:
                {
                    time : [config_id, vehicle_ObjectID, (x, y)],
                    time : [config_id, vehicle_ObjectID, (x, y)],
                    ...
                }
        3. Sends the dictionary of timestamp to batch_update
        """
        if self._is_collection_dynamic:
            # Transformer is called from run_dynamic_transformer.py
            # ... collection is dynmaic, so we need to listen to the change stream
            
            # get first doc to determine MODE
            first_doc = change_stream_connection.get()
            MODE.value = self.determine_mode(first_doc)
            batch_operations = self.transform_trajectory(MODE, resample(first_doc, MODE))
            batch_update_connection.put(batch_operations)

            while True:
                traj_doc = change_stream_connection.get()
                # print("[transformation] received doc")
                batch_operations = self.transform_trajectory(MODE, resample(traj_doc, MODE))
                batch_update_connection.put(batch_operations)
        else:
            # Transformer is called from run_static_transformer.py
            # ... collection is static, so we can just read the collection
            traj_doc = self.read_static_collection("config.json")
            for doc in traj_doc:
                if MODE.value == "":
                    MODE.value = self.determine_mode(doc)
                    print('mode in transformation: '+MODE.value)
                print("inserting doc: {}".format(doc["_id"]))
                batch_operations = self.transform_trajectory(MODE, resample(doc, MODE))
                batch_update_connection.put(batch_operations)
                print('put into batch_update')

def run(MODE, change_stream_connection, batch_update_connection):
    if change_stream_connection == None:
        transformation_obj = Transformation(is_collection_dynamic = False)
    else:
        transformation_obj = Transformation(is_collection_dynamic = True)
    transformation_obj.main_loop(MODE, change_stream_connection, batch_update_connection)