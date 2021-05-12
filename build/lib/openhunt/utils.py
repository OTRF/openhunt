#!/usr/bin/env python
# Author: Roberto Rodriguez (@Cyb3rWard0g)
# License: BSD 3-Clause
import json
import requests
import tarfile
from zipfile import ZipFile
from urllib.parse import urlparse
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession
from openhunt.logparser import winlogbeat

def get_spark():
    return (SparkSession.builder.appName("Mordor").config("spark.sql.caseSensitive", "True").getOrCreate())

def decompressJSON(filePath):
    tf = tarfile.open(filePath)
    for member in tf.getmembers():
        tf.extract(member)
        tf.close()
        return member.name

def downloadFile(url, dest="/tmp/"):
    response = requests.get(url, stream=True)
    outFilePath = "{}{}".format(dest, (Path(url).resolve().name))
    if response.status_code == 200:
        with open(outFilePath, 'wb') as f:
            f.write(response.raw.read())
    return outFilePath

# Function to read a json file
def readJSON(filePath, spark=False):
    if (spark):
        df = spark.read.json(filePath)
    else:
        df= pd.read_json(filePath, lines = True)
    return df
    
# Function to parse winlogbeat data (all versions)
def processDataFrame(df, spark=False):
    if (spark):
        print("[+] Processing a Spark DataFrame..")
        if '@metadata' in df.columns:
            df = df.withColumn('version', df["@metadata.version"].substr(1,1))
            df = df.withColumn('beat_type', df["@metadata.beat"])
            win = winlogbeat()
            # Verify what verion of Winlogbeat was used to ship the data
            if (len(df.filter((df.beat_type == 'winlogbeat') & (df.version <= 6)).limit(1).take(1)) > 0 ):
                df = win.winlogbeat_6(df.filter((df.beat_type == 'winlogbeat') & (df.version <= 6)), 'Spark')
            elif (len(df.filter((df.beat_type == 'winlogbeat') & (df.version >= 7)).limit(1).take(1)) > 0 ):
                df = win.winlogbeat_7(df.filter((df.beat_type == 'winlogbeat') & (df.version >= 7)),'Spark')
    else:
        print("[+] Processing Pandas DataFrame..")
        if '@metadata' in df.columns:
            df['version'] = df['@metadata'].apply(lambda x : x.get('version'))
            df['version'] = df['version'].astype(str).str[0]
            df['beat_type'] = df['@metadata'].apply(lambda x : x.get('beat'))
            # Initialize Empty Dataframe
            df = pd.DataFrame()
            win = winlogbeat()
            # Verify what verion of Winlogbeat was used to ship the data
            if ((df['beat_type'] == 'winlogbeat') & (df['version'] <= '6')).any():
                version_6_df = win.winlogbeat_6(df[(df['beat_type'] == 'winlogbeat') & (df['version'] <= '6')], 'Pandas')
                df = df.append(version_6_df, sort = False)              
            if ((df['beat_type'] == 'winlogbeat') & (df['version'] >= '7')).any():
                version_7_df = win.winlogbeat_7(df[(df['beat_type'] == 'winlogbeat') & (df['version'] >= '7')], 'Pandas')
                df = df.append(version_7_df, sort = False)                      
            if (df['beat_type'] != 'winlogbeat').any():
                not_winlogbeat = df[df['beat_type'] != 'winlogbeat']
                df = df.append(not_winlogbeat, sort = False)            
            df = df.dropna(axis = 0,how = 'all').reset_index(drop = True)
    print("[+] DataFrame Returned !")
    return df