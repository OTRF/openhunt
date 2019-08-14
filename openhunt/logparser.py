#!/usr/bin/env python

# Author: Jose Rodriguez (@Cyb3rPandaH)
# License: GNU General Public License v3 (GPLv3)

import pandas as pd
from pandas.io.json import json_normalize
import json

class winlogbeat(object):
	
	# Function to get mordor file
	def get_mordorDF(self, path):
		print("[+] Reading Mordor file..")
		mordorDF= pd.read_json(path, lines = True)
		return mordorDF
	
	def clean_fields(self, event_data):
		repeat_good = { 
			'Ipaddress' : 'IpAddress',
			'ProcessID' : 'ProcessId',
			'processId' : 'ProcessId',
			'type' : 'Type'
		} 
		# Iterating over values 
		for repeat, good in repeat_good.items(): 
			if repeat in event_data.columns:
				event_data[good].fillna(event_data[repeat])
				event_data = event_data.drop(columns=[repeat])
		return event_data
	
	# Function to parse winlogbeat data up to version 6
	def winlogbeat_6(self, mordorDF):
		print("[+] Processing Data from Winlogbeat version 6..")
		# Extract event_data nested fields
		event_data_field = mordorDF['event_data'].apply(pd.Series)
		mordorDF = mordorDF.drop('event_data', axis = 1)
		mordorDF_flat = pd.concat([mordorDF, event_data_field], axis = 1)
		mordorDF= mordorDF_flat.dropna(axis = 1,how = 'all')\
			.rename(columns={'log_name':'channel','record_number':'record_id','source_name':'provider_name'})\
			.drop(['process_id','thread_id'], axis = 1)
		mordorDF = self.clean_fields(mordorDF)
		return mordorDF
	
	# Function to parse winlogbeat data since version 7
	def winlogbeat_7(self, mordorDF):
		print("[+] Processing Data from Winlogbeat version 7..")
		winlog_field = mordorDF['winlog'].apply(pd.Series)
		event_data_field = winlog_field['event_data'].apply(pd.Series)
		event_data_field = self.clean_fields(event_data_field)
		mordorDF_flat = pd.concat([mordorDF, winlog_field, event_data_field], axis = 1)
		mordorDF= df.dropna(axis = 1,how = 'all').drop(['winlog','event_data','process'], axis = 1)
		mordorDF['level'] = mordorDF['log'].apply(lambda x : x.get('level'))
		mordorDF = self.clean_fields(mordorDF)
		return mordorDF
	
	# Function to parse winlogbeat data (all versions)
	def extract_nested_fields(self, path):
		print("[+] Processing Pandas DataFrame..")
		mordorDF= self.get_mordorDF(path)
		mordorDF['version'] = mordorDF['@metadata'].apply(lambda x : x.get('version'))
		mordorDF['version'] = mordorDF['version'].astype(str).str[0]
		mordorDF['beat_type'] = mordorDF['@metadata'].apply(lambda x : x.get('beat'))
		mordorDF = mordorDF.drop(columns=['@metadata','user','user_data','beat','host','activity_id'])
		# Initialize Empty Dataframe
		mordorDF_return = pd.DataFrame()
		# Verify what verion of Winlogbeat was used to ship the data
		if ((mordorDF['beat_type'] == 'winlogbeat') & (mordorDF['version'] <= '6')).any():
			version_6_df = self.winlogbeat_6(mordorDF[(mordorDF['beat_type'] == 'winlogbeat') & (mordorDF['version'] <= '6')])
			mordorDF_return = mordorDF_return.append(version_6_df, sort = False)		
		if ((mordorDF['beat_type'] == 'winlogbeat') & (mordorDF['version'] >= '7')).any():
			version_7_df = self.winlogbeat_7(mordorDF[(mordorDF['beat_type'] == 'winlogbeat') & (mordorDF['version'] >= '7')])
			mordorDF_return = mordorDF_return.append(version_7_df, sort = False)			
		if (mordorDF['beat_type'] != 'winlogbeat').any():
			not_winlogbeat = mordorDF[mordorDF['beat_type'] != 'winlogbeat']
			mordorDF_return = mordorDF_return.append(not_winlogbeat, sort = False)		
		mordorDF_return.dropna(axis = 0,how = 'all').reset_index(drop = True)
		
		print("[+] Normalizing dataframe..")
		json_struct = json.loads(mordorDF_return.to_json(orient="records"))
		mordorDF_return = json_normalize(json_struct)
		print("[+] DataFrame Returned !")
		return mordorDF_return
		