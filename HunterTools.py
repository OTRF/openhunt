# Created by Jose Luis Rodriguez @Cyb3rPandaH

import pandas as pd

class logparser():
	def winlogbeat(self, path)
		mordorFile = pd.read_json(path, lines = True)
		eventDataField = mordorFile['event_data'].apply(pd.Series)
		mordorDf = pd.concat([mordorFile, eventDataField], axis = 1)
		return mordorDf

