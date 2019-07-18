#!/usr/bin/env python

# Author: Jose Rodriguez (@Cyb3rPandaH)
# License: GNU General Public License v3 (GPLv3)

import pandas as pd

class winlogbeat(object):
	def check_version(self):
		#If beats version field is 7 or 8, do something
		return
	def extract_nested_fields(self, path):
		mordorFile = pd.read_json(path, lines = True)
		eventDataField = mordorFile['event_data'].apply(pd.Series)
		mordorDf = pd.concat([mordorFile, eventDataField], axis = 1)
		return mordorDf