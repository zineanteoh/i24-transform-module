from change_stream_reader import ChangeStreamReader

writer=ChangeStreamReader(client_username="i24-data",client_password="mongodb@i24",client_host="10.2.218.56",client_port=27017,database="lisatest",collection="stream_test")

writer.test_write()