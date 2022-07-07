
from pymongo import MongoClient
import pymongo
import urllib.parse
import time

username = urllib.parse.quote_plus('i24-data')
password = urllib.parse.quote_plus('mongodb@i24')
client = MongoClient('mongodb://%s:%s@10.2.218.56' % (username, password))
dbread=client["trajectories"]

dbwrite=client['lisatest']

read_collection=dbwrite['read_live_sim']
write_collection=dbwrite['write_live_sim']
# read_collection.drop()
# write_collection.drop()

# db.create_collection('read_v1')
# db.create_collection('write_v1')
write_collection.create_index([('timestamp',1)]) # reduces write time by ~ 90 sec

cursor=dbread['ground_truth_three'].find({},batch_size=500).limit(500) #sort([("first_timestamp",pymongo.ASCENDING),("last_timestamp",pymongo.ASCENDING)])
user = input()
# if user == "q":
    # break
count = 0
for doc in cursor:
    print('entering doc {}'.format(count))
    count += 1
    # if count > 500:s
    #     break
    read_collection.insert_one(doc)
    time.sleep(.1)
print("complete")