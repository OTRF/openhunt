#!/usr/bin/env python
# Author: Roberto Rodriguez (@Cyb3rWard0g)
# License: BSD 3-Clause

from openhunt.utils import *

import requests
from io import BytesIO
from zipfile import ZipFile

def downloadMordorFile(url, dest="/tmp/"):
    if url[-7:] == '.tar.gz':
        mordorFile = downloadFile(url)
        mordorJSONPath = decompressJSON(mordorFile)
    elif url[-4:] == '.zip':
        zipFileRequest = requests.get(url)
        zipFile = ZipFile(BytesIO(zipFileRequest.content))
        mordorJSONPath = zipFile.extract(zipFile.namelist()[0])
    return mordorJSONPath
    
def registerMordorSQLTable(spark, url, tableName):
    mordorJSONPath = downloadMordorFile(url)
    mordorDF = readJSON(mordorJSONPath, spark)
    mordorDF = processDataFrame(mordorDF, spark)
    mordorDF.createOrReplaceTempView(tableName)
    print("[+] Temporary SparkSQL View: {} ".format(tableName))

def getMordorZipFile(url):
    '''
    The initial idea for this function is here: https://stackoverflow.com/questions/5710867/downloading-and-unzipping-a-zip-file-without-writing-to-disk
    '''
    zipFileRequest = requests.get(url)
    zipFile = ZipFile(BytesIO(zipFileRequest.content))
    jsonFilePath = zipFile.extract(zipFile.namelist()[0])
    return jsonFilePath