#!/usr/bin/env python

# Author: Jose Rodriguez (@Cyb3rPandaH)
# License: GNU General Public License v3 (GPLv3)

import pandas as pd
import requests
import yaml

def getDictionary(platform = '', provider = '', event = '', url = None):
    ''' 
    If platform = 'aws' --> do not change default value of 'provider'
    If provider is an ETW PROVIDER such as 'Microsoft-Windows-Security-Auditing' --> insert platform = 'windows/etw-providers'
    For right format of platform, proviser and event check: https://github.com/hunters-forge/OSSEM/tree/master/source/data_dictionaries
    This function returns a dictionary
    '''
    if provider == 'security':
        provider = 'Microsoft-Windows-Security-Auditing'

    if url == None:
        urlBase = 'https://raw.githubusercontent.com/hunters-forge/OSSEM/master/source/data_dictionaries/'
        url = urlBase + platform + '/' + provider + '/events/'+event+'.yml'
        ossemFile = requests.get(url)
        ossemDict = yaml.safe_load(ossemFile.text)
        return ossemDict
    else:
        ossemFile = requests.get(url)
        ossemDict = yaml.safe_load(ossemFile.text)
        return ossemDict


def getEventDf(platform = '', provider = '', event = '', url = None):
    ''' 
    If platform = 'aws' --> do not change default value of 'provider'
    If provider is an ETW PROVIDER such as 'Microsoft-Windows-Security-Auditing' --> insert platform = 'windows/etw-providers'
    For right format of platform, proviser and event check: https://github.com/hunters-forge/OSSEM/tree/master/source/data_dictionaries
    This function returns a pandas dataframe
    '''
    dictionary = getDictionary(platform, provider, event, url)
    eventFields = dictionary.get('event_fields')
    eventCode = dictionary.get('event_code')
    eventTitle = dictionary.get('title')
    dfBase = pd.DataFrame(columns=['event_code','title','standard_name','standard_type','name','type','description','sample_value'])

    for i in eventFields:
        valuesToAdd = list(i.values())
        valuesToAdd.insert(0,eventCode)
        valuesToAdd.insert(1,eventTitle)
        seriesToAdd = pd.Series(valuesToAdd, index = dfBase.columns)
        dfBase = dfBase.append(seriesToAdd,ignore_index = True)
    return dfBase

def getEventFields(platform = '', provider = '', event = '', url = None):
    '''
    We are using the unique() method because in some OSSEM dictionaries the name of a field is repeated.
    This function returns a List
    '''
    df = getEventDf(platform, provider, event, url)
    eventFields = df['name'].drop_duplicates().to_list()
    return eventFields