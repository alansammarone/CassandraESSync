from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement
from cassandra.cqltypes import UUIDType
import elasticsearch
import json
import calendar
from elasticsearch.helpers import bulk, streaming_bulk
import sys
import datetime
from uuid import UUID
import time
import pdb


class CassandraESSync:
	
	mappings = []

	cassandra_nodes = []
	cassandra_keyspace = None
	cassandra_cfs = []

	es_nodes = []
	es_index = None
	es_types = []

	cassandra_client = None 
	es_client = None



	last_synced = {'cassandra': {}, 'es': {} }  # stores the last time each cf and index were synced, to avoid unnecessary queries


	def __init__(self, config):

		self.mappings = config["mappings"]

		self.cassandra_nodes = config['cassandra']['nodes']
		self.cassandra_keyspace = config['cassandra']['keyspace']
		self.cassandra_cfs = config['cassandra']['column_families']

		self.es_nodes = config['elasticsearch']['nodes']
		self.es_index = config['elasticsearch']['index']
		self.es_types = config['elasticsearch']['types']

		self.cassandra_client = Cluster().connect(self.cassandra_keyspace)
		self.es_client = elasticsearch.Elasticsearch()


	def sync_databases(self):
		for mapping in self.mappings:
			cassandra_cf, es_type = mapping
			self.sync_cf_type(cassandra_cf, es_type)


	def sync_cf_type(self, cassandra_cf, es_type):


		cf_id_column = self.cassandra_cfs[cassandra_cf]['id'] # column storing the document's uid
		cf_timestamp_column = self.cassandra_cfs[cassandra_cf]['timestamp'] # column storing the document's timestamp

		index_id_column = self.es_types[es_type]['id'] # column storing the document's timestamp
		index_timestamp_column = self.es_types[es_type]['timestamp'] # column storing the document's timestamp

		cf_data_fields = self.cassandra_cfs[cassandra_cf]['columns']
		cf_fields = [cf_id_column, cf_timestamp_column] + cf_data_fields

		type_data_fields = self.es_types[es_type]['columns']

		if cassandra_cf in self.last_synced['cassandra']:
			cf_start_time, cf_end_time = self.last_synced['cassandra'][cassandra_cf], time.time()  
		else:
			cf_start_time, cf_end_time = None, None

		if es_type in self.last_synced['es']:
			index_start_time, index_end_time = self.last_synced['es'][es_type], time.time()
		else:
			index_start_time, index_end_time = None, None

		cassandra_data_query = 'SELECT %s, %s FROM %s' % (cf_id_column, cf_timestamp_column, cassandra_cf)
		
		range_filter = {}
		if index_start_time and index_end_time:
			range_filter = self.get_es_range_filter(index_timestamp_column, index_start_time, index_end_time)
		

		self.cassandra_client.set_keyspace(self.cassandra_keyspace)
		cassandra_data = self.cassandra_client.execute(cassandra_data_query)
		self.last_synced['cassandra'][cassandra_cf] = time.time()

		es_data = self.es_client.search(index=self.es_index, doc_type=es_type, fields=[index_id_column, index_timestamp_column], body=range_filter)
		self.last_synced['es'][es_type] = time.time()

		all_data = {}

		ids_to_insert_on_cassandra = []
		ids_to_update_on_cassandra = []

		ids_to_insert_on_es = []
		ids_to_update_on_es = []


		# because we cant make a range query on a non-primary key on cassandra, we have to retrieve it all, and then check for the timestamp by hand.
		for document in cassandra_data:
			doc_id, doc_timestamp = str(document[0]), int(calendar.timegm(document[1].utctimetuple()))
			if not(cf_start_time and cf_end_time):
				all_data[doc_id] = [doc_timestamp, None]
			elif cf_start_time and cf_end_time and doc_timestamp >= cf_start_time and doc_timestamp <= cf_end_time: # 
				all_data[doc_id] = [doc_timestamp, None]

		for document in es_data['hits']['hits']:
			if "fields" in document:
				if index_id_column == '_id': # special case - is not inside fields. there must be a better way to do this ;(
					doc_id, doc_timestamp = document[index_id_column], int(document['fields'][index_timestamp_column][0])
				else:
					doc_id, doc_timestamp = document['fields'][index_id_column], int(document['fields'][index_timestamp_column][0])

				if doc_id in all_data:
					all_data[doc_id][1] = doc_timestamp
				else:
					all_data[doc_id] = [None, doc_timestamp]

		
		for uid in all_data:
			cassandra_ts, es_ts = all_data[uid]
			if cassandra_ts and es_ts:
				if cassandra_ts > es_ts: # same id, cassandra is the most recent. update that data on es. 
					ids_to_update_on_es.append(uid)
				elif es_ts > cassandra_ts: # same id, es is the most recent. update that data on cassandra.
					ids_to_update_on_cassandra.append(uid)
			elif cassandra_ts: # present only on cassandra. add to es.
				ids_to_insert_on_es.append(uid)
			elif es_ts: #present only on es. add to cassandra.

				ids_to_insert_on_cassandra.append(uid)


		if ids_to_insert_on_es or ids_to_update_on_es:
			actions = []
			from_cassandra_to_es = self.get_cassandra_documents_by_id(cassandra_cf, cf_fields, cf_id_column, ids_to_insert_on_es + ids_to_update_on_es)

			for document in from_cassandra_to_es:
				data = {}
				for i in range(len(cf_data_fields)):
					data[type_data_fields[i]] = getattr(document, cf_data_fields[i]) 	

				actions.append(self.get_es_bulk_action(es_type, index_id_column, getattr(document, cf_id_column), index_timestamp_column, getattr(document, cf_timestamp_column), data))
			
			bulk(self.es_client, actions) # send all inserts/updates to es at once

		if ids_to_insert_on_cassandra or ids_to_update_on_cassandra:
			batch = BatchStatement()

			type_fields = type_data_fields + [index_id_column, index_timestamp_column]
			ids_filter = self.get_es_ids_filter(es_type, ids_to_insert_on_cassandra + ids_to_update_on_cassandra)
			from_es_to_cassandra = self.es_client.search(index=self.es_index, doc_type=es_type, fields=type_data_fields + [cf_timestamp_column], body=ids_filter)
			
			for document in from_es_to_cassandra['hits']['hits']:
				
				id_value = document[index_id_column] if index_id_column == '_id' else document["fields"][index_id_column] # this makes me a saaaad panda
				id_value = id_value
				
				es_data = [UUID(id_value), datetime.datetime.utcfromtimestamp(int(document['fields'][index_timestamp_column][0]))]
				
				for field in type_data_fields:
					es_data.append(document['fields'][field][0])

				prepared_insert_statement = self.get_prepared_cassandra_insert_statement(cassandra_cf, cf_fields)
				prepared_update_statement = self.get_prepared_cassandra_update_statement(cassandra_cf, cf_id_column, cf_fields[1:])
				
				if id_value in ids_to_insert_on_cassandra:
					batch.add(prepared_insert_statement, tuple(es_data))
				else: 
					batch.add(prepared_update_statement, tuple(es_data[1:] + [UUID(id_value)]))

			
			self.cassandra_client.execute(batch)


	def get_cassandra_documents_by_id(self, cf, fields, id_column, ids):
		from_cassandra_to_es_query = "SELECT %s FROM %s WHERE %s IN (%s)" % (",".join(fields), cf, id_column, ",".join(ids))
		return self.cassandra_client.execute(from_cassandra_to_es_query)

	def get_prepared_cassandra_insert_statement(self, cf, cols):
		base_insert_statement = "INSERT INTO %s (%s) VALUES (%s)" % (cf, ",".join(cols), ",".join(["?"] * len(cols)))	
		return self.cassandra_client.prepare(base_insert_statement)

	def get_prepared_cassandra_update_statement(self, cf, id_column, fields):
		base_update_statement = "UPDATE %s SET %s WHERE %s = ?" % (cf, ",".join([field + " = ?" for field in fields]), id_column)
		return self.cassandra_client.prepare(base_update_statement)

	def get_es_range_filter(self, col, start, end):
		return {
				"filter": {
					"range" : {
						col: {
			        		"gte" : start,
			        		"lte" : end
			        	}
			    	}
				}
			}

	def get_es_ids_filter(self, es_type, ids):
		return {
				"filter": {
					"ids" : {
			        	"type" : es_type,
			        	"values": ids 
			    	}
				}
			}

	def get_es_bulk_action(self, es_type, id_column, id_value, timestamp_column, timestamp_value, data):

		if isinstance(timestamp_value, datetime.datetime):
			timestamp_value = calendar.timegm(timestamp_value.utctimetuple())

		id_value = str(id_value)
		timestamp_value = str(timestamp_value)

		data[timestamp_column] = timestamp_value

		action = {}
		action['_index'] = self.es_index
		action['_type'] = es_type 
		action[id_column] = id_value
		action['_source'] = data

		return action


	


		


#CE = CassandraESSync(json.load(open('config.json')))
#CE.sync_databases()



