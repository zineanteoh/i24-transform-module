from batch_update import BatchUpdate
import time

batcher=BatchUpdate()
batcher.connect_to_db(client_username="i24-data",
                    client_password="mongodb@i24",
                    client_host="10.2.218.56",
                    client_port=27017,
                    database="lisatest",
                    collection="batch_test")

print('batcher thru')
testDoc1={}
testDoc1[1]=[1,1,'x']
testDoc1[2]=[2,2,'x']
testDoc1[3]=[3,3,'x']
batcher.add_to_cache(testDoc1)
time.sleep(2)

testDoc2={}
testDoc2[1]=[1,1,'y']
testDoc2[2]=[2,2,'y']
testDoc2[3]=[3,3,'y']
batcher.add_to_cache(testDoc2)
time.sleep(7)

testDoc3={}
testDoc3[2]=[2,2,'z']
testDoc3[3]=[3,3,'z']
testDoc3[4]=[4,4,'z']
batcher.add_to_cache(testDoc3)


batcher.send_batch()