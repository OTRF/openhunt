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

# Libraries to create visualizations
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

import networkx as nx

###### OSSEM data dictionaries functions ######

def getDictionaryName(platform = '', provider = '', url = ''):
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

def getDictionaryContent(platform = '', provider = '', event = '', url = '',view = 'all'):
    # validating parameters input
    if platform == '' and provider == '' and event == '' and url == '':
        sys.exit('ERROR: Insert valid OSSEM platform ,provider, and event or an OSSEM url, but not all of them.')
    # defining url based on parameters. If url parameter is not empty, the function will prioritize this parameter.
    elif platform != '' and provider != '' and event != '' and url == '':
        # getting names for platform and provider
        yaml_files_names = getDictionaryName(platform, provider, url)
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
                    if str(i) in yaml_file:
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
    df_exploded = pd.DataFrame(dict_list).rename(columns={'description':'event_description'}).explode('event_fields').reset_index(drop = True)
    df = pd.concat([df_exploded,df_exploded['event_fields'].apply(pd.Series)], axis = 1).drop(['event_fields','references','tags'], axis = 1).rename(columns={'description':'field_description','event_code':'event_id'})
    # filtering columns
    if view == 'summary':
        df = df[['title','event_id','standard_name','name','field_description','sample_value']]
    if view not in ['all','summary']:
        sys.exit('ERROR: Insert a valid option for view: all or summary.')
    return df

###### OSSEM detection modeling functions ######

def getRelationshipContent():
    # getting yaml content
    url = 'https://raw.githubusercontent.com/OTRF/OSSEM-DM/main/relationships/_all_ossem_relationships.yml'
    yaml_file = requests.get(url)
    list_of_dict = yaml.safe_load(yaml_file.text)
    relationships_df = pd.DataFrame(list_of_dict).rename(columns={'name':'title'}).explode('security_events').reset_index(drop = True)
    # Splitting columns with dictionaries
    attack_df = relationships_df['attack'].apply(pd.Series)
    behavior_df = relationships_df['behavior'].apply(pd.Series)
    security_events_df = relationships_df['security_events'].apply(pd.Series).rename(columns={'name':'event_description'})
    all_relationships = pd.concat([relationships_df, attack_df, behavior_df, security_events_df], axis = 1).drop(['attack','behavior','security_events'], axis = 1)
    return all_relationships

###### OSSEM visualizations functions ######

def event_to_field_network(df,type = 'original'):
    ## Code Reference: https://towardsdatascience.com/customizing-networkx-graphs-f80b4e69bedf
    ## defining type of analysis
    if type == 'standard':
        name = 'standard_name'
    else:
        name = 'name'
    ## Creating dataframe for relationships
    ossem = df[['event_id',name]].rename(columns={'event_id':'source',name:'target'}).drop_duplicates()
    ## Creating dataframe nodes characteristics
    event = df[['event_id']].rename(columns={'event_id':'node'})
    event['type'] = 'event'
    field_name = df[[name]].rename(columns={name:'node'})
    field_name['type'] = 'data_field'
    nodes_characteristics = pd.concat([event,field_name]).dropna().drop_duplicates()
    ## Defining size of graph
    fig, ax = plt.subplots(figsize=(20,10))
    ## Creating graph object
    G = nx.from_pandas_edgelist(ossem, 'source', 'target', create_using = nx.Graph())
    ## Making types into categories
    nodes_characteristics = nodes_characteristics.set_index('node')
    # To validate if there are duplicated values before applying reindex: print(nodes_characteristics[nodes_characteristics.index.duplicated()])
    nodes_characteristics = nodes_characteristics.reindex(G.nodes())
    nodes_characteristics['type'] = pd.Categorical(nodes_characteristics['type'])
    nodes_characteristics['type'].cat.codes
    ## Specifying Color
    cmap = matplotlib.colors.ListedColormap(['lime','lightgray'])
    ## Setting nodes sizes
    node_sizes = [4000 if entry == 'event'  else (9000 if entry == 'data_field' else 0) for entry in nodes_characteristics.type]
    ## Drawing graph
    nx.draw(G, with_labels=True, font_size = 14, node_size = node_sizes, node_color = nodes_characteristics['type'].cat.codes,cmap = cmap, node_shape = "o", pos = nx.fruchterman_reingold_layout(G, k=0.4))
    ## Creating legend with color box
    leg_event = mpatches.Patch(color='lightgray', label='Event')
    leg_data_field = mpatches.Patch(color='lime', label='Data Field')
    plt.legend(handles=[leg_event,leg_data_field],loc = 'best', fontsize = 13)
    ## Margins
    plt.margins(0.1)
    plt.show()