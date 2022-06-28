
from pymongo import MongoClient
import pymongo
import urllib.parse
import time

username = urllib.parse.quote_plus('i24-data')
password = urllib.parse.quote_plus('mongodb@i24')
client = MongoClient('mongodb://%s:%s@10.2.218.56' % (username, password))
db=client["trajectories"]

dbwrite=client['lisatest']
read_collection=dbwrite['read_v1']
write_collection=dbwrite['write_v1']

read_collection.drop()
write_collection.drop()
cursor=db['ground_truth_two'].find().sort([("first_timestamp",pymongo.ASCENDING),("last_timestamp",pymongo.ASCENDING)])
user = input()
# if user == "q":
    # break
count = 0
while True:
    doc=cursor.next()
    print('entering doc {}'.format(count))
    count += 1
    if count > 500:
        break
    read_collection.insert_one(doc)
    # time.sleep(2)
print("complete")