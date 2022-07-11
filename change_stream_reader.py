"""
Created on Thu Jun 23
@author: lisaliuu

"""

from sqlite3 import OperationalError
from multiprocessing import Queue
import pandas as pd
import pymongo
import time
import json

class ChangeStreamReader:
    def __init__(self, config):

        """
        :param config: Config file containing MongoDB credentials
        """
        self.connect_to_db(config)

    def connect_to_db(self, config: str=None,
                            client_username: str=None, 
                            client_password: str=None, 
                            client_host: str=None, 
                            client_port: int=None, 
                            database: str=None, 
                            collection: str=None,):
        """
        Connects to a MongoDB instance.
        
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
                client_port=config_params['port']
                database=config_params['read_database_name']
                collection=config_params['read_collection_name']

        self.client=pymongo.MongoClient(host=client_host,
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

    def resample(self, car):
        '''
        resample the original time-series to uniformly sampled time series in 25Hz
        leave nans for missing data
        :param car: car document from MongoDB, containing field 'timestamp', 'x_position', 'y_position'
        '''

        # Select time series only
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

    def listen_stream(self, change_stream_connection : Queue, resume_after=None):
        """
        Listens to MongoDB stream via change stream and resamples document to send to 
        change_stream_connection ready to be read by transformation.py
        :params change_stream_connection: a multiprocessing Queue
        :params resume_after: stream token to resume listening from change stream if cursor failed
        """
        print("change stream being listened")
        try:
            # resume_token = None
            # pipeline = [{'$match': {'operationType': operation_type}}]
            # with self._collection.watch(resume_after=resume_after) as stream:
            pipeline = [{"$project":{"fullDocument._id":1,"fullDocument.timestamp":1,"fullDocument.x_position":1,"fullDocument.y_position":1}}]
            with self._collection.watch(pipeline=pipeline,resume_after=resume_after) as stream:
                for insert_change in stream:
                    print("[ChangeStreamReader] Read document {}".format(insert_change['fullDocument']['_id'])) #SEND
                    # resample every document
                    data_to_insert = self.resample(insert_change['fullDocument'])
                    # interpolate every document
                    # data_to_insert = self.interpolate(data_to_insert)
                    change_stream_connection.put(data_to_insert) 
                    resume_token = stream.resume_token
        except pymongo.errors.PyMongoError:
            print('stream restarting')
        # The ChangeStream encountered an unrecoverable error or the
        # resume attempt failed to recreate the cursor.
        if resume_token is None:
            # There is no usable resume token because there was a
            # failure during ChangeStream initialization.
            raise Exception('change stream cursor failed and is unrecoverable')
        else:
            # Use the interrupted ChangeStream's resume token to create
            # a new ChangeStream. The new stream will continue from the
            # last seen insert change without missing any events.
            print('stream restarted')
            self.listen_stream(change_stream_connection, resume_token)

    def test_write(self):
        while True:
            self._collection.insert_one({'time':time.time()})
            print('inserted 1')
            time.sleep(5)

def run(change_stream_connection):
    chg_stream_reader_obj = ChangeStreamReader("config.json")
    chg_stream_reader_obj.listen_stream(change_stream_connection)
