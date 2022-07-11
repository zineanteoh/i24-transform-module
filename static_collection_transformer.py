
from hmac import trans_36
from sqlite3 import OperationalError
import pymongo
import json

"""
Transforms a static (fixed-size) collection by inserting documents one-by-one to a new 
collection to simulate a dynamic collection. The transform module will then listen to
the change stream of this dynamic collection to generate a transformed collection. 

This class will 
1. Rename the original collection from "collection_name" to "collection_name_copy"
2. Insert documents from "collection_name_copy" to a new collection called "collection_name"
    a. `config.json` read_collection_name is set to `collection_name`
3. Once insertion has been completed, "collection_name_copy" is removed.
"""
class StaticCollectionTransformer:
    def __init__(self, config):
        """
        :param config: Config file containing the following params in JSON form.
        """
        if config:
            with open('config.json') as f:
                config_params = json.load(f)
                self.client_host=config_params['host']
                self.client_username=config_params['username']
                self.client_password=config_params['password']
                self.client_port=config_params['port']
                self.read_database_name=config_params['read_database_name']
                self.read_collection_name=config_params['read_collection_name']
        else:
            print("No config file found.")
            return

        self.client=pymongo.MongoClient(host=self.client_host,
                    port=self.client_port,
                    username=self.client_username,
                    password=self.client_password,
                    connect=True,
                    connectTimeoutMS=5000)
        
        self._database=self.client[self.read_database_name]
        self._collection_to_copy_from=self._database[self.read_collection_name]

        try:
            self.client.admin.command('ping')
        except pymongo.errors.ConnectionFailure:
            raise ConnectionError("Could not connect to MongoDB using pymongo, check connection addresses")
        except pymongo.errors.OperationFailure:
            raise OperationalError("Could not connect to MongoDB using pymongo, check authentications")

    def run(self):
        print("Press <Enter> to start inserting documents from collection {} to {}".format(self.read_collection_name, self.read_collection_name + "_copy"))
        print("Make sure that main.py is running and that config.json contains:")
        print("\tread_database_name: {}".format(self.read_database_name))
        print("\tread_collection_name: {}".format(self.read_collection_name + "_copy"))

        while True:
            user = input()
            if user=="":
                break
            else:
                print("Press <Enter> to start inserting documents")

        # Rename original collection from "collection_name" to "collection_name_copy"
        self._collection_to_copy_from.rename(self.read_collection_name + "_copy")
        # Create a new empty collection called "collection_name"
        self._collection_to_copy_to = self._database[self.read_collection_name]

        # Start inserting from "collection_name_copy" to "collection_name"
        cursor = self._collection_to_copy_from.find().sort([("first_timestamp",pymongo.ASCENDING),("last_timestamp",pymongo.ASCENDING)])
        count = 0
        for doc in cursor:
            print("Entering doc {}".format(count))
            count += 1
            self._collection_to_copy_to.insert_one(doc)

        print("Collection has been duplicated")

if __name__=="__main__":
    transformer = StaticCollectionTransformer()
    transformer.run()