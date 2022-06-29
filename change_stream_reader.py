"""
Created on Thu Jun 23
@author: lisaliuu

"""

from multiprocessing import Queue
from logging.config import listen
# from logging import exception
from sqlite3 import OperationalError
import pymongo
import time
import json

class ChangeStreamReader:
    def __init__(self, config, is_benchmark_on=False, benchmark_cap=499):

        """
        :param is_benchmark_on: Boolean flag to enable/disable benchmarking.
        """
        self.connect_to_db(config)
        self._is_benchmark_on = is_benchmark_on
        if (self._is_benchmark_on):
            self._benchmark_cap = benchmark_cap

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
                client_host=config_params['host']
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

    
    def listen_stream(self, change_stream_connection : Queue, resume_after=None):
        print("change stream being listened")
        if self._is_benchmark_on:
            st=time.time()
            doc_count = 0
        try:
            # resume_token = None
            pipeline = [{"$project":{"fullDocument._id":1,"fullDocument.timestamp":1,"fullDocument.x_position":1,"fullDocument.y_position":1}}]
            with self._collection.watch(pipeline=pipeline,resume_after=resume_after) as stream:
                for insert_change in stream:
                    print("[CSR] THIS IS OUR DOC FROM CHAGNE STREAM {}".format(insert_change['fullDocument']['_id'])) #SEND
                    change_stream_connection.put(insert_change['fullDocument'])#TODO double check pls
                    resume_token = stream.resume_token

                    if self._is_benchmark_on:
                        doc_count += 1
                        if doc_count == self._benchmark_cap:
                            et=time.time()
                            # etthread=time.thread_time()
                            print('[CSR] time for listening and putting into queue: '+str(et-st))

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
    chg_stream_reader_obj = ChangeStreamReader("config.json", is_benchmark_on=True, benchmark_cap=499)
    chg_stream_reader_obj.listen_stream(change_stream_connection)
