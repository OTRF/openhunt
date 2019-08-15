#!/usr/bin/env python

# Author: Jose Rodriguez (@Cyb3rPandaH)
# License: GNU General Public License v3 (GPLv3)

import pandas as pd

class winlogbeat(object):
	
	# Function to read a json file
	def get_mordorDF(self, path, spark=False):
		print("[+] Reading Mordor file..")
		if (spark):
			mordorDF = spark.read.json(path)
		else:
			mordorDF= pd.read_json(path, lines = True)
		return mordorDF
	
	# Function to parse winlogbeat data up to version 6
	def winlogbeat_6(self, mordorDF, df_type):
		print("[+] Processing Data from Winlogbeat version 6..")
		if df_type == 'Pandas':
			# Extract event_data nested fields
			event_data_field = mordorDF['event_data'].apply(pd.Series)
			mordorDF = mordorDF.drop('event_data', axis = 1)
			mordorDF_flat = pd.concat([mordorDF, event_data_field], axis = 1)
			mordorDF= mordorDF_flat\
				.dropna(axis = 1,how = 'all')\
				.rename(columns={'log_name':'channel','record_number':'record_id','source_name':'provider_name'})
		elif df_type == 'Spark':
			mordorDF = mordorDF.select('event_data.*','source_name','log_name','record_number','event_id','computer_name','@timestamp','message')
			mordorDF = mordorDF\
				.withColumnRenamed("log_name", "channel")\
    			.withColumnRenamed("source_name", "provider_name")\
    			.withColumnRenamed("record_number","record_id")
		else:
			exit
		return mordorDF
	
	# Function to parse winlogbeat data since version 7
	def winlogbeat_7(self, mordorDF, df_type):
		print("[+] Processing Data from Winlogbeat version 7..")
		if df_type == 'Pandas':
			winlog_field = mordorDF['winlog'].apply(pd.Series)
			event_data_field = winlog_field['event_data'].apply(pd.Series)
			event_data_field = self.clean_fields(event_data_field)
			mordorDF_flat = pd.concat([mordorDF, winlog_field, event_data_field], axis = 1)
			mordorDF= df.dropna(axis = 1,how = 'all').drop(['winlog','event_data','process'], axis = 1)
			mordorDF['level'] = mordorDF['log'].apply(lambda x : x.get('level'))
		elif df_type == 'Spark':
			mordorDF = mordorDF.select('winlog.event_data.*','winlog.channel','winlog.provider_name','winlog.record_id','winlog.event_id','winlog.computer_name','@timestamp','message')
			mordorDF = mordorDF\
				.withColumnRenamed("winlog.channel", "channel")\
    			.withColumnRenamed("winlog.provider_name", "provider_name")\
    			.withColumnRenamed("winlog.record_id","record_id")\
				.withColumnRenamed("winlog.event_id","event_id")\
				.withColumnRenamed("winlog.computer_name","computer_name")
		else:
			exit
		return mordorDF
	
	# Function to parse winlogbeat data (all versions)
	def extract_nested_fields(self, path, spark=False):
		if (spark):
			print("[+] Processing a Spark DataFrame..")
			mordorDF= self.get_mordorDF(path, spark)
			mordorDF = mordorDF.withColumn('version', mordorDF["@metadata.version"].substr(1,1))
			mordorDF = mordorDF.withColumn('beat_type', mordorDF["@metadata.beat"])
			# Verify what verion of Winlogbeat was used to ship the data
			if (len(mordorDF.filter((mordorDF.beat_type == 'winlogbeat') & (mordorDF.version <= 6)).limit(1).take(1)) > 0 ):
				mordorDF_return = self.winlogbeat_6(mordorDF.filter((mordorDF.beat_type == 'winlogbeat') & (mordorDF.version <= 6)), 'Spark')
			elif (len(mordorDF.filter((mordorDF.beat_type == 'winlogbeat') & (mordorDF.version >= 7)).limit(1).take(1)) > 0 ):
				mordorDF_return = self.winlogbeat_7(mordorDF.filter((mordorDF.beat_type == 'winlogbeat') & (mordorDF.version >= 7)),'Spark')
			else:
				exit
		else:
			print("[+] Processing Pandas DataFrame..")
			mordorDF= self.get_mordorDF(path)
			mordorDF['version'] = mordorDF['@metadata'].apply(lambda x : x.get('version'))
			mordorDF['version'] = mordorDF['version'].astype(str).str[0]
			mordorDF['beat_type'] = mordorDF['@metadata'].apply(lambda x : x.get('beat'))
			# Initialize Empty Dataframe
			mordorDF_return = pd.DataFrame()
			# Verify what verion of Winlogbeat was used to ship the data
			if ((mordorDF['beat_type'] == 'winlogbeat') & (mordorDF['version'] <= '6')).any():
				version_6_df = self.winlogbeat_6(mordorDF[(mordorDF['beat_type'] == 'winlogbeat') & (mordorDF['version'] <= '6')], 'Pandas')
				mordorDF_return = mordorDF_return.append(version_6_df, sort = False)		
			if ((mordorDF['beat_type'] == 'winlogbeat') & (mordorDF['version'] >= '7')).any():
				version_7_df = self.winlogbeat_7(mordorDF[(mordorDF['beat_type'] == 'winlogbeat') & (mordorDF['version'] >= '7')], 'Pandas')
				mordorDF_return = mordorDF_return.append(version_7_df, sort = False)			
			if (mordorDF['beat_type'] != 'winlogbeat').any():
				not_winlogbeat = mordorDF[mordorDF['beat_type'] != 'winlogbeat']
				mordorDF_return = mordorDF_return.append(not_winlogbeat, sort = False)		
			mordorDF_return = mordorDF_return.dropna(axis = 0,how = 'all').reset_index(drop = True)

		print("[+] DataFrame Returned !")
		return mordorDF_return