#!/usr/bin/env python

# Author: Jose Rodriguez (@Cyb3rPandaH)
# License: GNU General Public License v3 (GPLv3)

import pandas as pd

class winlogbeat(object):
	def get_winlogbeat_version(self,mordor_file):
		meta_data_field = mordor_file['@metadata'].apply(pd.Series).drop_duplicates().reset_index(drop = True)
		if meta_data_field['beat'][0] == 'winlogbeat':
			return int(meta_data_field['version'][0][0])
		else:
			print('The type of beat is not winlogbeat. Use a different function.')
	def extract_nested_fields(self, path):
		mordor_file = pd.read_json(path, lines = True)
		if get_winlogbeat_version(mordor_file) < 7:
			event_data_field = mordor_file['event_data'].apply(pd.Series)
			mordor_file_wo_event_data = mordor_file.drop('event_data', axis = 1)
			df = pd.concat([mordor_file_wo_event_data, event_data_field], axis = 1)
			mordor_df= df.dropna(axis = 1,how = 'all')
			return mordor_df
		else:
			print('For versions equal or greater then 7 use a different function.')
