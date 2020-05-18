#!/usr/bin/env python
# Author: Roberto Rodriguez (@Cyb3rWard0g)
# License: BSD 3-Clause

from openhunt.utils import *

def downloadMordorFile(url, dest="/tmp/"):
    mordorFile = downloadFile(url)
    mordorJSONPath = decompressJSON(mordorFile)
    return mordorJSONPath
    
def registerMordorSQLTable(spark, url, tableName):
    mordorJSONPath = downloadMordorFile(url)
    mordorDF = readJSON(mordorJSONPath, spark)
    mordorDF = processDataFrame(mordorDF, spark)
    mordorDF.createOrReplaceTempView(tableName)
    print("[+] Temporary SparkSQL View: {} ".format(tableName))