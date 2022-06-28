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
    def __init__(self, config, read_frequency=0):

        """
        :param read_frequency: Time in seconds that listener sleeps between checking for new inserts
        """
        self.read_frequency=read_frequency
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

    def listen(self, change_stream_connection : Queue, resume_after=None):
        count=0
        print("change stream being listened")
        try:
            # resume_token = None
            # pipeline = [{'$match': {'operationType': operation_type}}]
            with self._collection.watch(resume_after=resume_after) as stream:
                for insert_change in stream:
                    print("THIS IS OUR DOC FROM CHAGNE STREAM {}".format(insert_change['fullDocument']['_id'])) #SEND
                    change_stream_connection.put(insert_change['fullDocument'])
                    # count+=1
                    resume_token = stream.resume_token
                    time.sleep(self.read_frequency)
                    # if count==3:
                    #     stream.close()
        except pymongo.errors.PyMongoError:
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
                listen(self, resume_after=resume_token)
                # with col.watch(
                #         pipeline, resume_after=resume_token) as stream:
                #     for insert_change in stream:
                #         print(insert_change)

    def test_write(self):
        while True:
            self._collection.insert_one({'time':time.time()})
            print('inserted 1')
            time.sleep(5)

def run(change_stream_connection):
    chg_stream_reader_obj = ChangeStreamReader("config.json")
    chg_stream_reader_obj.listen(change_stream_connection)
