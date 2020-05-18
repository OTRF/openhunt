#!/usr/bin/env python

# Author: Jose Rodriguez (@Cyb3rPandaH)
# License: GNU General Public License v3 (GPLv3)

import pandas as pd

class winlogbeat(object):
        # Function to parse winlogbeat data up to version 6
        def winlogbeat_6(self, df, df_type):
                print("[+] Processing Data from Winlogbeat version 6..")
                if df_type == 'Pandas':
                        # Extract event_data nested fields
                        event_data_field = df['event_data'].apply(pd.Series)
                        df = df.drop('event_data', axis = 1)
                        df_flat = pd.concat([df, event_data_field], axis = 1)
                        df= df_flat\
                                .dropna(axis = 1,how = 'all')\
                                .rename(columns={'log_name':'channel','record_number':'record_id','source_name':'provider_name'})
                elif df_type == 'Spark':
                        df = df.select('event_data.*','source_name','log_name','record_number','event_id','computer_name','@timestamp','message')
                        df = df\
                                .withColumnRenamed("log_name", "channel")\
                        .withColumnRenamed("source_name", "provider_name")\
                        .withColumnRenamed("record_number","record_id")
                else:
                        exit
                return df
        # Function to parse winlogbeat data since version 7
        def winlogbeat_7(self, df, df_type):
                print("[+] Processing Data from Winlogbeat version 7..")
                if df_type == 'Pandas':
                        winlog_field = df['winlog'].apply(pd.Series)
                        event_data_field = winlog_field['event_data'].apply(pd.Series)
                        df_flat = pd.concat([df, winlog_field, event_data_field], axis = 1)
                        df= df_flat.dropna(axis = 1,how = 'all').drop(['winlog','event_data','process'], axis = 1)
                        df['level'] = df['log'].apply(lambda x : x.get('level'))
                elif df_type == 'Spark':
                        df = df.select('winlog.event_data.*','winlog.channel','winlog.provider_name','winlog.record_id','winlog.event_id','winlog.computer_name','@timestamp','message')
                        df = df\
                                .withColumnRenamed("winlog.channel", "channel")\
                        .withColumnRenamed("winlog.provider_name", "provider_name")\
                        .withColumnRenamed("winlog.record_id","record_id")\
                                .withColumnRenamed("winlog.event_id","event_id")\
                                .withColumnRenamed("winlog.computer_name","computer_name")
                else:
                        exit
                return df