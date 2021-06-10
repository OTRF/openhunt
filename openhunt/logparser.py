#!/usr/bin/env python

# Author: Jose Rodriguez (@Cyb3rPandaH)
# License: GNU General Public License v3 (GPLv3)

import pandas as pd
import xml.etree.ElementTree as ET

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

# Function to parse event data exported from AzureSentinelToGo: CSV File or Pandas Dataframe
def parse_azure_event_data(data_source):
        # If data_source is a path to .csv file
        if isinstance(data_source,pd.DataFrame) == False:
                data_source = pd.read_csv(data_source)
        # If data_source is a Pandas dataframe
        if data_source['Type'].unique()[0] == 'Event':
                columns = []
                values = []

                for record in data_source.EventData:
                        record_columns = []
                        record_values = []
                        event = ET.fromstring(record)
                        #for x, y in event.attrib.items():
                        #    record_columns.append(x)
                        #    record_values.append(y)
                        for content in event:
                                for data in content:
                                        for r, c in data.attrib.items():
                                                record_columns.append(c)
                                                record_values.append(data.text)
                        columns.append(record_columns)
                        values.append(record_values)

                telemetry_list = []
                for i in range(len(columns)):
                        event_dictionary = {}
                        event_keys = columns[i]
                        event_values = values[i]
                        for key in event_keys:
                                for value in event_values:
                                        event_dictionary[key] = value
                                        event_values.remove(value)
                                        break
                        telemetry_list.append(event_dictionary)

                event_data = pd.DataFrame(telemetry_list)

                data_source['EventDescription'] = data_source['RenderedDescription'].apply(lambda x : x[:x.find(':')])
                data_source = data_source.rename(columns={'Source': 'EventSourceName'})
                data_source = data_source[['TimeGenerated [UTC]','Computer','EventSourceName','EventID','EventDescription']]

                dataframe = pd.concat([data_source, event_data], axis = 1).dropna(how = 'all', axis = 1)

        elif data_source['Type'].unique()[0] == 'SecurityEvent':
                columns = []
                values = []

                for record in data_source.EventData:
                        record_columns = []
                        record_values = []
                        event = ET.fromstring(record)
                        for content in event:
                                for r, c in content.attrib.items():
                                        record_columns.append(c)
                                        record_values.append(content.text)
                        columns.append(record_columns)
                        values.append(record_values)

                telemetry_list = []
                for i in range(len(columns)):
                        event_dictionary = {}
                        event_keys = columns[i]
                        event_values = values[i]
                        for key in event_keys:
                                for value in event_values:
                                        event_dictionary[key] = value
                                        event_values.remove(value)
                                        break
                        telemetry_list.append(event_dictionary)

                event_data = pd.DataFrame(telemetry_list)

                data_source['EventDescription'] = data_source['Activity'].apply(lambda x : x[x.find('-') + 2:])
                data_source = data_source[['TimeGenerated [UTC]','Computer','EventSourceName','EventID','EventDescription']]

                dataframe = pd.concat([data_source, event_data], axis = 1).dropna(how = 'all', axis = 1)
        return(dataframe)

# Function to parse json files (Wireshark) into pandas dataframe
def json_wireshark_dataframe(json_path):
        '''
        Obtain json file using: File -> Export Packet Dissections -> as json
        '''
        df = pd.read_json(json_path)
        df = df['_source'].apply(pd.Series)['layers'].apply(pd.Series)
        columns_names = df.columns

        telemetry = df[columns_names[0]].apply(pd.Series)
        if len(columns_names) > 1:
                for i in range(1,len(columns_names)):
                        df_layers = df[columns_names[i]].apply(pd.Series)
                        telemetry = pd.concat([telemetry,df_layers], axis = 1)
        return telemetry

# Function to parse xml files into pandas dataframe
def xml_dataframe(xml_path):
        '''
        In order to generate xml from EVTX use the following commands:
        Reference: https://www.alishaaneja.com/evtx/
        1) Clone the repository: git clone https://github.com/williballenthin/python-evtx.git
        2) Change directory: cd python-evtx
        3) Install Libraries: python3 setup.py install
        4) Change Directory: cd scripts
        5) Run to generate xml file: python evtx_dump.py ../../Security.evtx > ../../security_test.xml 
        '''
        
        tree = ET.parse(xml_path)
        root = tree.getroot()

        column = []
        values = []
        for event in root:
                record_column = []
                record_values = []
                for block in event:
                        for element in block:
                                if element.tag[-8:] == 'Provider':
                                        for x, y in element.attrib.items():
                                                record_column.append(x)
                                                record_values.append(y)
                                if element.tag[-7:] == 'EventID':
                                        record_column.append('EventID')
                                        record_values.append(element.text)
                                if element.tag[-7:] == 'Version':
                                        record_column.append('Version')
                                        record_values.append(element.text)
                                if element.tag[-5:] == 'Level':
                                        record_column.append('Level')
                                        record_values.append(element.text)
                                if element.tag[-4:] == 'Task':
                                        record_column.append('Task')
                                        record_values.append(element.text)
                                if element.tag[-6:] == 'Opcode':
                                        record_column.append('Opcode')
                                        record_values.append(element.text)
                                if element.tag[-8:] == 'Keywords':
                                        record_column.append('Keywords')
                                        record_values.append(element.text)
                                if element.tag[-11:] == 'TimeCreated':
                                        for x, y in element.attrib.items():
                                                record_column.append(x)
                                                record_values.append(y)
                                if element.tag[-13:] == 'EventRecordID':
                                        record_column.append('EventRecordID')
                                        record_values.append(element.text)
                                if element.tag[-11:] == 'Correlation':
                                        for x, y in element.attrib.items():
                                                record_column.append(x)
                                                record_values.append(y)
                                if element.tag[-9:] == 'Execution':
                                        for x, y in element.attrib.items():
                                                record_column.append(x)
                                                record_values.append(y)
                                if element.tag[-7:] == 'Channel':
                                        record_column.append('Task')
                                        record_values.append(element.text)
                                if element.tag[-8:] == 'Computer':
                                        record_column.append('Computer')
                                        record_values.append(element.text)
                                if element.tag[-8:] == 'Security':
                                        for x, y in element.attrib.items():
                                                record_column.append(x)
                                                record_values.append(y)
                                if element.tag[-4:] == 'Data':
                                        for x, y in element.attrib.items():
                                                record_column.append(y)
                                        record_values.append(element.text)
                column.append(record_column)
                values.append(record_values)

        telemetry_list = []
        for i in range(len(column)):
                event_dictionary = {}
                event_keys = column[i]
                event_values = values[i]
                for key in event_keys:
                        for value in event_values:
                                event_dictionary[key] = value
                                event_values.remove(value)
                                break
                telemetry_list.append(event_dictionary)
        
        df = pd.DataFrame(telemetry_list)
        
        return(df)