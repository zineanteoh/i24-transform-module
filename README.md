# i24-transform-module

I24's Transform module for downstream traffic visualization and analysis (post-post processing)

![transform_module](https://user-images.githubusercontent.com/58854510/177854618-4b249c1d-7bac-4e26-8375-8aadc59d05a3.png)

## High level overview

The Transform module:

1. Subscribes to a post-processed collection via mongoDB's change stream (change_stream_reader.py)
2. Transforms trajectories into a timestamp-based document (transformation.py)
3. Writes and updates the documents into a transformed mongoDB collection (batch_update.py)
4. Parallelizes step 1-3 with multiprocessing (main.py)

## How to transform an existing (static) collection using static_collection_transformer.py

1. Clone the repository
2. Create a `config.json` file using `config.template.json`.
3. Enter the credentials for connecting to MongoDB and the collection info

    > `read_database_name` & `read_collection_name`: The name of the collection and its database to transform (must already exist in db)

    > `write_database_name` & `write_collection_name`: The name of the transformed collection and its database (to be generated by main.py)

    ```
    {
    	"host": "x.x.x.x",
    	"port": 27017,
    	"username": "username",
    	"password": "password",
    	"read_database_name": "db_name",
    	"read_collection_name": "collection_name"
    	"write_database_name": "database_name"
    	"write_collection_name": "collection_name"
    }
    ```

4. Run `static_collection_transformer.py`. Wait for prompt but do not press [Enter] key yet
5. Run `main.py` on a separate terminal. Once all three processes are started, press the [Enter] key on the terminal running `static_collection_transformer.py`.
