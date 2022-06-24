"""
Created on Thu Jun 23
@author: lisaliuu

"""

from logging.config import listen
import urllib.parse
import pymongo
from pymongo import MongoClient
import time
from datetime import date
from datetime import datetime


class ChangeStreamReader:
    def __init__(self, client_username: str=None, client_password: str=None, client_host: str=None, client_port: int=None, database: str=None, collection: str=None, read_frequency=1):
        self.client=MongoClient(host=client_host,
                                port=client_port,
                                username=client_username,
                                password=client_password,
                                connect=True)
        # if (not self.client):
        #     print('MongoDB connection failed')
        # else:
        #     print("ok")

        try:
            self.client.admin.command('ping')
        except pymongo.errors.ConnectionFailure:
            raise ConnectionError("Could not connect to MongoDB using pymongo.")
        
        self._database=self.client[database]
        self._collection=self._database[collection]
        self.read_frequency=read_frequency

    def listen(self,operation_type,resume_after=None):
        count=0
        try:
            # resume_token = None
            pipeline = [{'$match': {'operationType': operation_type}}]
            with self._collection.watch(pipeline,resume_after=resume_after) as stream:
                for insert_change in stream:
                    print(insert_change['fullDocument']) #SEND
                    count+=1
                    resume_token = stream.resume_token
                    # time.sleep(self.read_frequency)
                    if count==3:
                        stream.close()
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
                listen(self,operation_type,resume_after=resume_token)
                print('stream restarted')
                # with col.watch(
                #         pipeline, resume_after=resume_token) as stream:
                #     for insert_change in stream:
                #         print(insert_change)

    def test_write(self):
        while True:
            self._collection.insert_one({'time':time.time()})
            print('inserted 1')
            time.sleep(5)