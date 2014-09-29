import json
import os
import time
from cassandraes_sync import CassandraESSync
import logging


config_filename = "config.json"
config = json.load(open(config_filename))
interval = int(config['interval'])


logging.basicConfig(level=logging.DEBUG, filename='/tmp/cassandraes_sync.log')

CESSync = CassandraESSync(config)


while True: 
	try:
		CESSync.sync_databases()
	except:
		logging.exception("Oops, something went wrong:")
		time.sleep(interval)



