
from pymongo import MongoClient
import keyboard
import urllib.parse

username = urllib.parse.quote_plus('i24-data')
password = urllib.parse.quote_plus('mongodb@i24')
client = MongoClient('mongodb://%s:%s@10.2.218.56' % (username, password))
db=client["trajectories"]
col=db["ground_truth_two"]

dbwrite=client['lisatest']
colwrite=dbwrite['read_v1']

cursor=db['ground_truth_trajectories'].find().limit(100)
tottime=0
while True:
    if keyboard.is_pressed('e'):
        doc=cursor.next()
        print('entering doc')
        colwrite.insert_one(doc)

        
print(tottime/100)