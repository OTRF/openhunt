#!/usr/bin/env python

# Author: Jose Rodriguez (@Cyb3rPandaH)
# License: GNU General Public License v3 (GPLv3)

import pandas as pd
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Mordor").getOrCreate()

class winlogbeat(object):
	
	# Function to get mordor file
	def get_mordorDF(self, path, df_type):
		if df_type == 'Pandas':
			mordorDF= pd.read_json(path, lines = True)
		elif df_type == 'Spark':
			mordorDF = spark.read.json(path)
		return mordorDF
	
	# Function to parse winlogbeat data up to version 6
	def winlogbeat_6(self, mordorDF, df_type):
		if df_type == 'Pandas':
			event_data_field = mordorDF['event_data'].apply(pd.Series)
			mordorDF_wo_event_data = mordorDF.drop('event_data', axis = 1)
			df = pd.concat([mordorDF_wo_event_data, event_data_field], axis = 1)
			mordorDF= df.dropna(axis = 1,how = 'all').rename(columns={'log_name':'channel','record_number':'record_id','source_name':'provider_name'})
		elif df_type == 'Spark':
			mordorDF = mordorDF.select('event_data.*','source_name','log_name','record_number','event_id','computer_name','@timestamp')
			mordorDF = mordorDF\
				.withColumnRenamed("log_name", "channel")\
    			.withColumnRenamed("source_name", "provider_name")\
    			.withColumnRenamed("record_number","record_id")
		else:
			exit
		return mordorDF
	
	# Function to parse winlogbeat data since version 7
	def winlogbeat_7(self, mordorDF, df_type):
		if df_type == 'Pandas':
			winlog_field = mordorDF['winlog'].apply(pd.Series)
			event_data_field = winlog_field['event_data'].apply(pd.Series)
			df = pd.concat([mordorDF, winlog_field, event_data_field], axis = 1)
			mordorDF= df.dropna(axis = 1,how = 'all').drop(['winlog','event_data'], axis = 1)
			mordorDF['process_id'] = mordorDF['process'].apply(lambda x : x.get('pid'))
			mordorDF['thread_id'] = mordorDF['process'].apply(lambda x : x.get('thread')).apply(lambda x : x.get('id'))
			mordorDF['level'] = mordorDF['log'].apply(lambda x : x.get('level'))
		elif df_type == 'Spark':
			mordorDF = mordorDF.select('event_data.*','provider_name','channel','record_id','event_id','computer_name','@timestamp')
		else:
			exit
		return mordorDF
	
	# Function to parse winlogbeat data (all versions)
	def extract_nested_fields(self, path, df_type='Pandas'):
		if df_type == 'Pandas':
			mordorDF= self.get_mordorDF(path,df_type)
			mordorDF['version'] = mordorDF['@metadata'].apply(lambda x : x.get('version'))
			mordorDF['version'] = mordorDF['version'].astype(str).str[0]
			mordorDF['beat_type'] = mordorDF['@metadata'].apply(lambda x : x.get('beat'))

			mordorDF_return = pd.DataFrame()

			if ((mordorDF['beat_type'] == 'winlogbeat') & (mordorDF['agent_version_number'] <= '6')).any():
				version_6_df = self.winlogbeat_6(mordorDF[(mordorDF['beat_type'] == 'winlogbeat') & (mordorDF['agent_version_number'] <= '6')], 'Pandas')
				mordorDF_return = mordorDF_return.append(version_6_df, sort = False)
			
			if ((mordorDF['beat_type'] == 'winlogbeat') & (mordorDF['agent_version_number'] >= '7')).any():
				version_7_df = self.winlogbeat_7(mordorDF[(mordorDF['beat_type'] == 'winlogbeat') & (mordorDF['agent_version_number'] >= '7')], 'Pandas')
				mordorDF_return = mordorDF_return.append(version_7_df, sort = False)
			
			if (mordorDF['beat_type'] != 'winlogbeat').any():
				not_winlogbeat = mordorDF[mordorDF['beat_type'] != 'winlogbeat']
				mordorDF_return = mordorDF_return.append(not_winlogbeat, sort = False)
		
			mordor_df.dropna(axis = 0,how = 'all').reset_index(drop = True)
		elif df_type == 'Spark':
			mordorDF= self.get_mordorDF(path,df_type)
			mordorDF = mordorDF.withColumn('version', mordorDF["@metadata.version"].substr(1,1))
			mordorDF = mordorDF.withColumn('beat_type', mordorDF["@metadata.beat"])

			if (len(mordorDF.filter((mordorDF.beat_type == 'winlogbeat') & (mordorDF.version <= 6)).limit(1).take(1)) > 0 ):
				mordorDF_return = self.winlogbeat_6(mordorDF.filter((mordorDF.beat_type == 'winlogbeat') & (mordorDF.version <= 6)), 'Spark')
			elif (len(mordorDF.filter((mordorDF.beat_type == 'winlogbeat') & (mordorDF.version >= 7)).limit(1).take(1)) > 0 ):
				mordorDF_return = self.winlogbeat_7(mordorDF.filter((mordorDF.beat_type == 'winlogbeat') & (mordorDF.version >= 7)),'Spark')
			else:
				exit
		else:
			exit

		return mordorDF_return