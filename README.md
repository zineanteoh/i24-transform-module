# i24-transform-module

I24's Transform module for downstream traffic visualization and analysis (post-post processing)

## High level overview

The Transform module:

1. Subscribes to a post-processed collection via mongoDB's change stream
2. Transforms trajectories into a timestamp-based document
3. Writes and updates the documents into a transformed mongoDB collection

## How to run

1. Clone the repository
2. Create a `config.json` file based on the `config.template.json` and enter the information for connection to a mongoDB collection

```
{
	"host": "x.x.x.x",
	"port": 27017,
	"username": "username",
	"password": "password",
	"database_name": "db_name",
	"collection_name": "collection_name"
}
```

3. Run `main.py`
