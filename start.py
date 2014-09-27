import json
import os
import time
from cassandraes_sync import CassandraESSync


config_filename = "config.json"
config = json.load(open(config_filename))
interval = int(config['interval'])


CESSync = CassandraESSync(config)


while True: 
	CESSync.sync_databases()
	time.sleep(interval)
	


