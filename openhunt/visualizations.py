#!/usr/bin/env python

# Author: Jose Rodriguez (@Cyb3rPandaH)
# License: GNU General Public License v3 (GPLv3)

# Libraries to manipulate data
import pandas as pd
from pandas import json_normalize

# Libraries to handle execution errors
import sys

# Libraries to create visualizations
import matplotlib.pyplot as plt, seaborn as sns
import matplotlib
import plotly.graph_objects as go
import networkx as nx

from pyspark.sql import DataFrame
from pyspark.sql.types import *
import pyspark.sql.functions as func

    
# Function to get a horizontal bar chart with data labels
def barh_chart(dataframe,xfield,yfield,title,xlabel = '',ylabel = '',figSize = (12,8)):
    ## Code Reference: https://stackoverflow.com/questions/28931224/adding-value-labels-on-a-matplotlib-bar-chart
    # Bring some raw data.
    if isinstance(dataframe,pd.DataFrame) == True:
        frequencies = dataframe[xfield].values[::-1].tolist()
        max_freq = dataframe[xfield].values.max()
        min_freq = dataframe[xfield].values.min()
        y_labels = dataframe[yfield].values[::-1].tolist()
    elif isinstance(dataframe, DataFrame) == True:
        frequencies = dataframe.toPandas()[xfield].values[::-1].tolist()
        max_freq = dataframe.toPandas()[xfield].values.max()
        min_freq = dataframe.toPandas()[xfield].values.min()
        y_labels = dataframe.toPandas()[yfield].values[::-1].tolist()

    freq_series = pd.Series(frequencies)

    # Plot the figure.
    plt.figure(figsize = figSize)
    ax = freq_series.plot(kind='barh')
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_yticklabels(y_labels)
    ax.set_xlim(min_freq, max_freq) # expand xlim to make labels easier to read
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(True)
    ax.spines['left'].set_visible(True)

    rects = ax.patches

    # For each bar: Place a label
    for rect in rects:
        # Get X and Y placement of label from rect.
        x_value = rect.get_width()
        y_value = rect.get_y() + rect.get_height() / 2

        # Number of points between bar and label. Change to your liking.
        space = 5
        # Vertical alignment for positive values
        ha = 'left'

        # If value of bar is negative: Place label left of bar
        if x_value < 0:
            # Invert space to place label to the left
            space *= -1
            # Horizontally align label at right
            ha = 'right'

        # Use X value as label and format number with one decimal place
        label = "{:,.0f}".format(x_value)

        # Create annotation
        plt.annotate(
            label,                      # Use `label` as label
            (x_value, y_value),         # Place label at end of the bar
            xytext=(space, 0),          # Horizontally shift label by `space`
            textcoords="offset points", # Interpret `xytext` as offset in points
            va='center',                # Vertically center label
            ha=ha)                      # Horizontally align label differently for
                                        # positive and negative values.  
    
# Function to get a histogram
def histogram(dataframe,field):
     # Parameters:
        # datadrame: must be a spark dataframe
        # field: String value. It must match a single column name
    # About nulls values: This function does not consider null values on its calculations
    # Selecting column. Dropping null values.
    df=dataframe.select(field).dropna(how='any',subset=field)
    # Converting spark dataframe into pandas dataframe so we can use Seaborn library to create our box plot
    panda_df=df.toPandas()
    return sns.distplot(panda_df[field], kde = False) # This function returns a box plot
    
    
# Function to get a box plot
def box_plot(dataframe,field):
     # Parameters:
        # datadrame: must be a spark dataframe
        # field: String value. It must match a single column name
    # About nulls values: This function does not consider null values on its calculations
    # Selecting column. Dropping null values.
    df=dataframe.select(field).dropna(how='any',subset=field)
    # Converting spark dataframe into pandas dataframe so we can use Seaborn library to create our box plot
    panda_df=df.toPandas()
    return sns.boxplot(x=panda_df[field]) # This function returns a box plot

# Function to get a time series line chart
def time_series_chart(dataframe,field,interval,operation,data=False,fig_width=16,fig_height=4):
     # Parameters:
        # datadrame: must be a pandas dataframe
        # field: String value. It must match a single column name. The function will group the results using the field column
        # interval: String value. Find aliases here: https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
        # operation: String value. Operations defined so far: 'count','sum','mean'
        # data: Boolean value. Default value is False. When True, the function also returns a pandas dataframe with data used in the chart
        # fig_width: Integer value. Default value is 16. This parameter affects the final width of the chart
        # fig_height: Integer value. Default value is 4. This parameter affects the final heigh of the chart
    # About nulls values: This function does not consider null values on its calculations
    # Importing Libraries and Modules
    import pyspark.sql.functions as F
    import matplotlib.pyplot as plt
    from matplotlib import dates as mpl_dates
    from datetime import datetime, timedelta
    # Applying seaborn style for chart
    plt.style.use('seaborn')
    # Using conditional statement based on 'operation' parameter
    if operation=='count':
        # Grouping index (timestamp) and field
        analysis=dataframe.groupby([field,pd.Grouper(freq=interval)])[field].count()
    elif operation=='sum':
        # Grouping index (timestamp) and field
        analysis=dataframe.groupby([field,pd.Grouper(freq=interval)])[field].sum()
    elif operation=='mean':
        # Grouping index (timestamp) and field
        analysis=dataframe.groupby([field,pd.Grouper(freq=interval)])[field].mean()
    else:
        print('Operation has not been defined yet')
        return # Execution ends here

    # Unstacking dataframe by field. All columns will have the same index start
    analysis_plot=analysis.unstack(field)
    # Plotting time series line chart
    analysis_plot.plot(linestyle='-',figsize=(fig_width,fig_height),title=operation+' of '+field+' every '+interval)
    # Auto formatting of dates axis (Inclination of axis labels)
    plt.gcf().autofmt_xdate()
    # Setting format of dates axis
    date_format = mpl_dates.DateFormatter('%D %T')
    # Applying format to dates axis
    plt.gca().xaxis.set_major_formatter(date_format)
    # Setting format of chart legend
    ax=plt.gca()
    plt.legend(bbox_to_anchor=(1.1, 1.1), bbox_transform=ax.transAxes)
    if data==True:
        return analysis_plot.transpose().fillna('').dropna(axis=1,how='all') # This function also shows a pandas dataframe with details by field and time interval

# Function to get a horizontal bar chart with data labels
def time_series_dataframe(dataframe,time_field):
     # Parameters:
        # datadrame: must be a spark dataframe
        # field: String value. It must match a single column name
    # About nulls values: This function does not consider null values on its calculations
    # Importing Libraries and Modules
    import pyspark.sql.functions as F
    # Dropping null values. Creating a new column with timestamp format. Dropping original time_field. Converting spark dataframe into pandas dataframe. Setting 'timestamp' as dataframe index. Sorting new index
    df = dataframe.dropna(how='any',subset = time_field).withColumn('timestamp',F.to_utc_timestamp(time_field,tz = 'UTC')).drop(time_field).toPandas().set_index('timestamp').sort_index()
    return df # This functions returns a pandas dataframe

# Function to get a network graph with techniques, subtechniques, data sources, data components, and security events
def attack_network_graph(mapping):
    ## Code Reference: https://towardsdatascience.com/customizing-networkx-graphs-f80b4e69bedf
    ## Creating dataframe for relationships
    technique_to_data_source = mapping[['technique','data_source']].rename(columns={'technique':'source','data_source':'target'})
    data_source_to_component = mapping[['data_source','data_component']].rename(columns={'data_source':'source','data_component':'target'})
    component_to_event = mapping[['data_component','event_id']].rename(columns={'data_component':'source','event_id':'target'})
    df = pd.concat([technique_to_data_source,data_source_to_component,component_to_event]).drop_duplicates()
    ## Creating dataframe nodes characteristics
    technique = mapping[['technique']].rename(columns={'technique':'node'})
    technique['type'] = 'technique'
    data_source = mapping[['data_source']].rename(columns={'data_source':'node'})
    data_source['type'] = 'data_source'
    component = mapping[['data_component']].rename(columns={'data_component':'node'})
    component['type'] = 'component'
    event = mapping[['event_id']].rename(columns={'event_id':'node'})
    event['type'] = 'event'
    nodes_characteristics = pd.concat([technique,data_source,component,event]).dropna().drop_duplicates()
    ## Defining size of graph
    fig, ax = plt.subplots(figsize=(15,10))
    ## Creating graph object
    G = nx.from_pandas_edgelist(df, 'source', 'target', create_using = nx.Graph())
    ## Making types into categories
    nodes_characteristics = nodes_characteristics.set_index('node')
    # To validate if there are duplicated values before applying reindex: print(nodes_characteristics[nodes_characteristics.index.duplicated()])
    nodes_characteristics = nodes_characteristics.reindex(G.nodes())
    nodes_characteristics['type'] = pd.Categorical(nodes_characteristics['type'])
    nodes_characteristics['type'].cat.codes
    ## Specifying Color
    cmap = matplotlib.colors.ListedColormap(['lime','cyan','orange','gold','lightgray'])
    ## Setting nodes sizes
    node_sizes = [10000 if entry == 'technique'  else (8000 if entry == 'data_source' else (5000 if entry == 'component' else 2000)) for entry in nodes_characteristics.type]
    ## Drawing graph
    nx.draw(G, with_labels=True, font_size = 15, node_size = node_sizes, node_color = nodes_characteristics['type'].cat.codes,cmap = cmap, node_shape = "o", pos = nx.fruchterman_reingold_layout(G, k=0.4))