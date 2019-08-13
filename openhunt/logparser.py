#!/usr/bin/env python

# Author: Jose Rodriguez (@Cyb3rPandaH)
# License: GNU General Public License v3 (GPLv3)

import pandas as pd

class winlogbeat(object):
	
	# Function to get mordor file
	def get_mordor_file(self, path):
		mordor_file = pd.read_json(path, lines = True)
		return mordor_file
	
	# Function to parse winlogbeat data up to version 6
	def winlogbeat_6(self, mordor_file):
		event_data_field = mordor_file['event_data'].apply(pd.Series)
		mordor_file_wo_event_data = mordor_file.drop('event_data', axis = 1)
		df = pd.concat([mordor_file_wo_event_data, event_data_field], axis = 1)
		mordor_df= df.dropna(axis = 1,how = 'all').rename(columns={'log_name':'channel','record_number':'record_id','source_name':'provider_name'})
		return mordor_df
	
	# Function to parse winlogbeat data since version 7
	def winlogbeat_7(self, mordor_file):
		winlog_field = mordor_file['winlog'].apply(pd.Series)
		event_data_field = winlog_field['event_data'].apply(pd.Series)
		df = pd.concat([mordor_file, winlog_field, event_data_field], axis = 1)
		mordor_df= df.dropna(axis = 1,how = 'all').drop(['winlog','event_data'], axis = 1)
		mordor_df['process_id'] = mordor_df['process'].apply(lambda x : x.get('pid'))
		mordor_df['thread_id'] = mordor_df['process'].apply(lambda x : x.get('thread')).apply(lambda x : x.get('id'))
		mordor_df['level'] = mordor_df['log'].apply(lambda x : x.get('level'))
		return mordor_df
	
	# Function to parse winlogbeat data (all versions)
	def extract_nested_fields(self, path):
		mordor_file = self.get_mordor_file(path)

		mordor_file['agent_version'] = mordor_file['@metadata'].apply(lambda x : x.get('version'))
		mordor_file['agent_version_number'] = mordor_file['agent_version'].astype(str).str[0]
		mordor_file['beat_type'] = mordor_file['@metadata'].apply(lambda x : x.get('beat'))
		
		mordor_df = pd.DataFrame()

		if ((mordor_file['beat_type'] == 'winlogbeat') & (mordor_file['agent_version_number'] <= '6')).any():
			version_6_df = self.winlogbeat_6(mordor_file[(mordor_file['beat_type'] == 'winlogbeat') & (mordor_file['agent_version_number'] <= '6')])
			mordor_df = mordor_df.append(version_6_df, sort = False)
		
		if ((mordor_file['beat_type'] == 'winlogbeat') & (mordor_file['agent_version_number'] >= '7')).any():
			version_7_df = self.winlogbeat_7(mordor_file[(mordor_file['beat_type'] == 'winlogbeat') & (mordor_file['agent_version_number'] >= '7')])
			mordor_df = mordor_df.append(version_7_df, sort = False)
		
		if (mordor_file['beat_type'] != 'winlogbeat').any():
			not_winlogbeat = mordor_file[mordor_file['beat_type'] != 'winlogbeat']
			mordor_df = mordor_df.append(not_winlogbeat, sort = False)
		
		mordor_df.dropna(axis = 0,how = 'all').reset_index(drop = True)
		
		return mordor_df
