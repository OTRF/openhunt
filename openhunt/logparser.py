#!/usr/bin/env python

# Author: Jose Rodriguez (@Cyb3rPandaH)
# License: GNU General Public License v3 (GPLv3)

import pandas as pd

class winlogbeat(object):
	
	def get_mordor_file(self, path):
		mordor_file = pd.read_json(path, lines = True)
		return mordor_file
	
	def extract_nested_fields_winlogbeat_up_to_version_6(mordor_file):
		event_data_field = mordor_file['event_data'].apply(pd.Series)
		mordor_file_wo_event_data = mordor_file.drop('event_data', axis = 1)
		df = pd.concat([mordor_file_wo_event_data, event_data_field], axis = 1)
		mordor_df= df.dropna(axis = 1,how = 'all').rename(columns={'log_name':'channel','record_number':'record_id','source_name':'provider_name'})
		return mordor_df
	
	def extract_nested_fields_winlogbeat_since_version_7(mordor_file):
		winlog_field = mordor_file['winlog'].apply(pd.Series)
		event_data_field = winlog_field['event_data'].apply(pd.Series)
		df = pd.concat([mordor_file, winlog_field, event_data_field], axis = 1)
		mordor_df= df.dropna(axis = 1,how = 'all').drop(['winlog','event_data'], axis = 1)
		mordor_df['process_id'] = mordor_df['process'].apply(lambda x : x.get('pid'))
		mordor_df['thread_id'] = mordor_df['process'].apply(lambda x : x.get('thread')).apply(lambda x : x.get('id'))
		mordor_df['level'] = mordor_df['log'].apply(lambda x : x.get('level'))
		return mordor_df
	
	def extract_nested_fields(mordor_file):
		mordor_file['agent_version'] = mordor_file['@metadata'].apply(lambda x : x.get('version'))
		mordor_file['agent_version_number'] = mordor_file['agent_version'].astype(str).str[0]
		mordor_file['beat_type'] = mordor_file['@metadata'].apply(lambda x : x.get('beat'))
		
		winlogbeat_up_to_version_6 = mordor_file[(mordor_file['beat_type'] == 'winlogbeat') & (mordor_file['agent_version_number'] <= '6')]
		if winlogbeat_up_to_version_6['@metadata'].count() >= 1:
			winlogbeat_up_to_version_6_df = extract_nested_fields_winlogbeat_up_to_version_6(winlogbeat_up_to_version_6)
		
		winlogbeat_since_version_7 = mordor_file[(mordor_file['beat_type'] == 'winlogbeat') & (mordor_file['agent_version_number'] >= '7')]
		if winlogbeat_since_version_7['@metadata'].count() >= 1:
			winlogbeat_since_version_7_df = extract_nested_fields_winlogbeat_since_version_7(winlogbeat_since_version_7)
    		
		mordor_file_not_winlogbeat = mordor_file[mordor_file['beat_type'] != 'winlogbeat']
    		
		mordor_df = winlogbeat_up_to_version_6_df.append([winlogbeat_since_version_7,mordor_file_not_winlogbeat], sort = False).dropna(axis = 0,how = 'all').reset_index(drop = True)
		
		return mordor_df
