#!/usr/bin/env python

# Author: Jose Rodriguez (@Cyb3rPandaH)
# License: GNU General Public License v3 (GPLv3)

import pandas as pd

class winlogbeat(object):
	def check_version(self):
		#If beats version field is 7 or 8, do something
		return
	def extract_nested_fields(self, path):
		mordor_file = pd.read_json(path, lines = True)
		event_data_field = mordor_file['event_data'].apply(pd.Series)
		mordor_file_wo_event_data = mordor_file.drop('event_data', axis = 1)
		df = pd.concat([mordor_file_wo_event_data, event_data_field], axis = 1)
		mordor_df= df.dropna(axis = 1,how = 'all')
		return mordor_df
