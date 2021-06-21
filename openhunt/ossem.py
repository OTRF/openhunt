#!/usr/bin/env python

# Author: Jose Rodriguez (@Cyb3rPandaH)
# License: GNU General Public License v3 (GPLv3)

# Libraries to manipulate data
from numpy import string_
import pandas as pd
from pandas import json_normalize

# Libraries to handle execution errors
import sys

# Libraries to get web content
import requests

# Libraries to handle yaml files
import yaml

# Libraries to handle xml files
import xml.etree.ElementTree as ET

###### OSSEM data dictionaries functions ######

def getDictionariesNames(platform = '', provider = '', url = ''):
    # validating parameters input
    if platform == '' and provider == '' and url == '':
        sys.exit('ERROR: Insert valid OSSEM platform and provider or an OSSEM url, but not all of them.')
    # defining url based on parameters. If url parameter is not empty, the function will prioritize this parameter.
    elif platform != '' and provider != '' and url == '':
        if platform.lower() == 'windows':
            if provider.lower() == 'security':
                url = 'https://github.com/OTRF/OSSEM-DD/tree/main/windows/etw-providers/Microsoft-Windows-Security-Auditing/events'
            elif provider.lower() == 'sysmon':
                url = 'https://github.com/OTRF/OSSEM-DD/tree/main/windows/sysmon/events'
            elif provider.lower() == 'osquery':
                url = 'https://github.com/OTRF/OSSEM-DD/tree/main/windows/osquery/events'
            elif provider.lower() == 'powershell':
                url = 'https://github.com/OTRF/OSSEM-DD/tree/main/windows/powershell/events'
            elif provider.lower() == 'windowsdefenderatp':
                url = 'https://github.com/OTRF/OSSEM-DD/tree/main/windows/windowsdefenderatp/events'
            elif provider.lower() == 'carbonblack':
                url = 'https://github.com/OTRF/OSSEM-DD/tree/main/windows/carbonblack/events'
            elif provider.lower() == 'sohostdata':
                url = 'https://github.com/OTRF/OSSEM-DD/tree/main/windows/so-host-data/events'
            else:
                sys.exit('ERROR: Insert a valid Windows provider.')
        elif platform.lower() == 'linux':
            if provider == 'osquery':
                url = 'https://github.com/OTRF/OSSEM-DD/tree/main/linux/osquery/events'
            else:
                sys.exit('ERROR: Insert a valid Linux provider.')
        elif platform.lower() == 'macos':
            if provider == 'carbonblack':
                url = 'https://github.com/OTRF/OSSEM-DD/tree/main/macos/carbonblack/events'
            elif provider == 'osquery':
                url = 'https://github.com/OTRF/OSSEM-DD/tree/main/macos/osquery/events'
            else:
                sys.exit('ERROR: Insert a valid macOS provider.')
        elif platform.lower() == 'zeek':
            if provider == 'detection':
                url = 'https://github.com/OTRF/OSSEM-DD/tree/main/zeek/detection/events'
            elif provider.lower() == 'files':
                url = 'https://github.com/OTRF/OSSEM-DD/tree/main/zeek/files/events'
            elif provider.lower() == 'miscellaneous':
                url = 'https://github.com/OTRF/OSSEM-DD/tree/main/zeek/miscellaneous/events'
            elif provider.lower() == 'network-observations':
                url = 'https://github.com/OTRF/OSSEM-DD/tree/main/zeek/network-observations/events'
            elif provider.lower() == 'network-protocols':
                url = 'https://github.com/OTRF/OSSEM-DD/tree/main/zeek/network-protocols/events'
            else:
                sys.exit('ERROR: Insert a valid Zeek provider.')
        else:
            sys.exit('ERROR: Insert a valid OSSEM provider.')
    elif platform == '' and provider == '' and url != '':
        pass
    else:
        sys.exit('ERROR: Insert valid OSSEM platform and provider or an OSSEM url, but not all of them.')
    
    # Getting url content
    website_content = requests.get(url)
    event_files_names = []
    
    # Getting yaml file name
    for line in website_content.iter_lines():
        # line is type 'bytes', we are using decode method to get a string
        request_line = line.decode("utf-8")
        # identifying lines that contain names and directory of yaml files
        if request_line.startswith('            <span class="css-truncate css-truncate-target d-block width-fit"'):
            # parsing xml string
            xml_request_line = ET.fromstring(request_line)
            # reading each xml line
            for element in xml_request_line:
                for x, y in element.attrib.items():
                    if x == 'title':
                        event_files_names.append(y)
    
    # Removing past versions
    for event_name in event_files_names:
        # if the name contains _v
        if '_v' in event_name:
            # removing first version
            event_files_names.remove(event_name[:event_name.find('_')] + '.yml')
            # getting count of versions
            count = 0
            for name in event_files_names:
                if event_name[:event_name.find('_')] in name:
                    count += 1
            # removing versions
            if count > 1:
                for version in range(1,count):
                    event_files_names.remove(event_name[:event_name.find('_v') + 2] + str(version) + '.yml')
    return event_files_names

def getDictionary(platform = '', provider = '', event = '', url = ''):
    # validating parameters input
    if platform == '' and provider == '' and event == '' and url == '':
        sys.exit('ERROR: Insert valid OSSEM platform ,provider, and event or an OSSEM url, but not all of them.')
    # defining url based on parameters. If url parameter is not empty, the function will prioritize this parameter.
    elif platform != '' and provider != '' and event != '' and url == '':
        # getting names for platform and provider
        yaml_files_names = getDictionariesNames(platform, provider, url)
        # getting yaml file name (considering version)
        if isinstance(event, str):
            yaml_files_list = []
            for yaml_file in yaml_files_names:
                if event in yaml_file:
                    yaml_files_list.append(yaml_file)
        elif isinstance(event, list):
            yaml_files_list = []
            for i in event:
                for yaml_file in yaml_files_names:
                    if i in yaml_file:
                        yaml_files_list.append(yaml_file)
        else:
            sys.exit('ERROR: Insert a string or list object for event parameter.')
        # getting url reference based on platform and provider
        if platform.lower() == 'windows':
            if provider.lower() == 'security':
                url = 'https://raw.githubusercontent.com/OTRF/OSSEM-DD/main/windows/etw-providers/Microsoft-Windows-Security-Auditing/events'
            elif provider.lower() == 'sysmon':
                url = 'https://raw.githubusercontent.com/OTRF/OSSEM-DD/main/windows/sysmon/events'
            elif provider.lower() == 'osquery':
                url = 'https://raw.githubusercontent.com/OTRF/OSSEM-DD/main/windows/osquery/events'
            elif provider.lower() == 'powershell':
                url = 'https://raw.githubusercontent.com/OTRF/OSSEM-DD/main/windows/powershell/events'
            elif provider.lower() == 'windowsdefenderatp':
                url = 'https://raw.githubusercontent.com/OTRF/OSSEM-DD/main/windows/windowsdefenderatp/events'
            elif provider.lower() == 'carbonblack':
                url = 'https://raw.githubusercontent.com/OTRF/OSSEM-DD/main/windows/carbonblack/events'
            elif provider.lower() == 'sohostdata':
                url = 'https://raw.githubusercontent.com/OTRF/OSSEM-DD/main/windows/so-host-data/events'
            else:
                sys.exit('ERROR: Insert a valid Windows provider.')
        elif platform.lower() == 'linux':
            if provider == 'osquery':
                url = 'https://raw.githubusercontent.com/OTRF/OSSEM-DD/main/linux/osquery/events'
            else:
                sys.exit('ERROR: Insert a valid Linux provider.')
        elif platform.lower() == 'macos':
            if provider == 'carbonblack':
                url = 'https://raw.githubusercontent.com/OTRF/OSSEM-DD/main/macos/carbonblack/events'
            elif provider == 'osquery':
                url = 'https://raw.githubusercontent.com/OTRF/OSSEM-DD/main/macos/osquery/events'
            else:
                sys.exit('ERROR: Insert a valid macOS provider.')
        elif platform.lower() == 'zeek':
            if provider == 'detection':
                url = 'https://raw.githubusercontent.com/OTRF/OSSEM-DD/main/zeek/detection/events'
            elif provider.lower() == 'files':
                url = 'https://raw.githubusercontent.com/OTRF/OSSEM-DD/main/zeek/files/events'
            elif provider.lower() == 'miscellaneous':
                url = 'https://raw.githubusercontent.com/OTRF/OSSEM-DD/main/zeek/miscellaneous/events'
            elif provider.lower() == 'network-observations':
                url = 'https://raw.githubusercontent.com/OTRF/OSSEM-DD/main/zeek/network-observations/events'
            elif provider.lower() == 'network-protocols':
                url = 'https://raw.githubusercontent.com/OTRF/OSSEM-DD/main/zeek/network-protocols/events'
            else:
                sys.exit('ERROR: Insert a valid Zeek provider.')
        else:
            sys.exit('ERROR: Insert a valid OSSEM provider.')
    elif platform == '' and provider == '' and event == '' and url != '':
        pass
    else:
        sys.exit('ERROR: Insert valid OSSEM platform, provider, and event or an OSSEM url, but not all of them.')
    # getiing list with final url's
    final_url_list = []
    for yaml_file in yaml_files_list:
        final_url_list.append(url + '/' + yaml_file)
    # getting data dictionaries
    dict_list = []
    for yaml_url in final_url_list:
        # Getting OSSEM data dictionary content
        ossem_file = requests.get(yaml_url)
        ossem_dict = yaml.safe_load(ossem_file.text)
        # Updating list of dictionaries
        dict_list.append(ossem_dict)
    # creating pandas dataframe
    df_exploded = pd.DataFrame(dict_list).explode('event_fields').reset_index(drop = True)
    df = pd.concat([df_exploded,df_exploded['event_fields'].apply(pd.Series)], axis = 1).drop(['event_fields','references','tags'], axis = 1)
    return df

###### OSSEM detection modeling functions ######

def getRelationships():
    # getting yaml content
    url = 'https://raw.githubusercontent.com/OTRF/OSSEM-DM/main/relationships/_all_ossem_relationships.yml'
    yaml_file = requests.get(url)
    list_of_dict = yaml.safe_load(yaml_file.text)
    relationships_df = pd.DataFrame(list_of_dict).explode('security_events').reset_index(drop = True)
    # Splitting columns with dictionaries
    attack_df = relationships_df['attack'].apply(pd.Series)
    behavior_df = relationships_df['behavior'].apply(pd.Series)
    security_events_df = relationships_df['security_events'].apply(pd.Series)
    all_relationships = pd.concat([relationships_df, attack_df, behavior_df, security_events_df], axis = 1).drop(['attack','behavior','security_events'], axis = 1)
    return all_relationships