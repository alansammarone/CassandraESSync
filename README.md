# CassandraESSync

This is a Python script that synchronizes some set of columns between a set of Cassandra column families and a set of ElasticSearch types.
If there is an id that is present in both places, then the older one is replaced by the most recent one.

It uses Cassandra's batch statement and ES's bulk api for efficiently inserting large amouts of data using a single query.

## Usage

The scripts uses a config json file to set some properties. Here's an example config.js: 

```
{
	"interval": 3600,
	"mappings": [["cf1", "type1"], ["cf2", "type2"]],
	"cassandra": {
		"nodes": ["127.0.0.1"], 
		"keyspace": "test_keyspace",
		"column_families": {
			"cf1": {
				"id": "id_col1",
				"timestamp": "timestamp_col1",
				"columns": ["cf1_column1", "cf1_column2"]
			},
			"cf2": {
				"id": "id_col2",
				"timestamp": "timestamp_col2",
				"columns": ["cf2_column1", "cf2_column2"]
			}, 

		}
	}, 
	"elasticsearch": {
		"nodes": ["127.0.0.1:9200"], 
		"index": "test_index",
		"types": {
			"type1": {
				"id": "_id",
				"timestamp": "timestamp_col3",
				"columns": ["type1_column1", "type1_column2"]
			},
			"type2": {
				"id": "_id",
				"timestamp": "timestamp_col4",
				"columns": ["type2_column1", "type2_column2"]
			}
		}
	}
}
```
```interval``` is an integer representing the amount of seconds between sucessive synchronizations.

```mappings``` indicates what type, in ES, should be synced with each column_family, in Cassandra. In the example, columns from "cf1" will be synced with columns in "type1", and columns in "cf2" will be synced with columns in "type2".

```cassandra``` indicates the node address, the keyspace to be used, the the information about the what are the primary keys (they should be type4 UUID) on each column family, what is the name of the column which contains a timestamp of the last time the record was modified, and which columns should be synced. Note that the order inside the ```columns``` parameter matters: in this example, "cf1_column1" will have its data mapped to "type1_column1", and "cf1_column2" will have its data mapped to "type1_column2". Note also that the data types of these columns (and formats, in case of dates), must be such that data from one can be inserted into the another. 


```elasticsearch``` indicates the node adresses, the index to which the types belong, the id and timestamp columns, and the columns to be synced.


## Known issues 

* The scripts currently doesn't run as a daemon, per say. It just sleeps. This should be fixed soon.
* The script is very type-sensitive. Types should match precisely. 





