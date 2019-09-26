#!/usr/bin/env python

# Author: Jose Rodriguez (@Cyb3rPandaH)
# License: GNU General Public License v3 (GPLv3)

import pandas as pd

class summaryStats(object):
    
    # Function to obtain a pandas dataframe with descriptive statistics for a numerical variable stored in a spark dataframe column.
    def desc_stats(self, dataframe, field):
        # Parameters:
            # datadrame: must be a spark dataframe
            # field: String value. It must match a single column name
        # About nulls values: This function does not consider null values on its calculations
        # Importing Libraries and Modules
        from pyspark.sql.functions import col, avg, min, max, stddev, skewness, kurtosis
        # Selecting column. Dropping null values
        df=dataframe.select(field).dropna(how='any')
        # Count of Values
        count=df.count()
        # Central Tendency
        mean_process=df.agg(avg(col(field))) # The result of agg is a spark dataframe
        mean_value=mean_process.collect()[0][mean_process.columns[0]] # The result of collect is a list of row. [0] first element & [mean_process.columns[0] name of the column
        median_value=df.approxQuantile(col=field,probabilities=[0.5],relativeError=0.05)[0] # The result of approxQuantile is a list. [0] first element
        # Min, Max, and Percentiles
        min_process=df.agg(min(col(field))) # The result of agg is a spark dataframe
        min_value=min_process.collect()[0][min_process.columns[0]] # The result of collect is a list of row. [0] first element & [min_process.columns[0] name of the column
        max_process=df.agg(max(col(field))) # The result of agg is a spark dataframe
        max_value=max_process.collect()[0][max_process.columns[0]] # The result of collect is a list of row. [0] first element & [max_process.columns[0] name of the column
        percentile_25=df.approxQuantile(col=field,probabilities=[0.25],relativeError=0.05)[0] # The result of approxQuantile is a list. [0] first element
        percentile_75=df.approxQuantile(col=field,probabilities=[0.75],relativeError=0.05)[0] # The result of approxQuantile is a list. [0] first element
        # Variation
        stddev_process=df.agg(stddev(col(field))) # The result of agg is a spark dataframe
        stddev_value=stddev_process.collect()[0][stddev_process.columns[0]] # The result of collect is a list of row. [0] first element & [stddev_process.columns[0] name of the column
        range_value=max_value-min_value # Calculation of the range of values
        IQR_value=percentile_75-percentile_25 # Calculation of the Interquartile range
        # Shape
        skewness_process=df.agg(skewness(col(field))) # The result of agg is a spark dataframe
        skewness_value=skewness_process.collect()[0][skewness_process.columns[0]] # The result of collect is a list of row. [0] first element & [skewness_process.columns[0] name of the column
        kurtosis_process=df.agg(kurtosis(col(field))) # The result of agg is a spark dataframe
        kurtosis_value=round(kurtosis_process.collect()[0][kurtosis_process.columns[0]],2) # The result of collect is a list of row. [0] first element & [kurtosis_process.columns[0] name of the column
        # Printing summary of statistics
        print('Summary of Descriptive Statistics - ',field)
        print('**********************************************************')
        print('Count of values          : ',count)
        print('Central tendency:-----------------------------------------')
        print('Mean(Average)            : ',round(mean_value,2))
        print('Median(Percentile 25)    : ',round(median_value,2))
        print('Min, Max, and Percentiles:--------------------------------')
        print('Minimum                  : ',round(min_value,2))
        print('Maximum                  : ',round(max_value,2))
        print('Percentile 25 (Q1)       : ',round(percentile_25,2))
        print('Percentile 75 (Q3)       : ',round(percentile_75,2))
        print('Variation:------------------------------------------------')
        print('Standard Deviation       : ',round(stddev_value,2))
        print('Range                    : ',round(range_value,2))
        print('Interquartile Range (IQR): ',round(IQR_value,2))
        print('Shape:----------------------------------------------------')
        print('Skewness                 : ',round(skewness_value,2))
        print('Kurtosis                 : ',round(kurtosis_value,2))
        print('**********************************************************')
        # Creating a dictionary with descriptive statistics
        data = {'Statistic': ['count','mean','median','min','max','percentile25','percentile75','stddev','range','iqr','skewness','kurtosis'],
                'Values': [count,mean_value,median_value,min_value,max_value,percentile_25,percentile_75,stddev_value,range_value,IQR_value,skewness_value,kurtosis_value]}
        # Creating a pandas dataframe
        summary_stats = pd.DataFrame(data)
        return summary_stats # This function returns a pandas dataframe

    # Function to obtain a spark dataframe with descriptive statistics for a numerical variable
    def stack_count(self, dataframe, field):
        # Parameters:
            # datadrame: must be a spark dataframe
            # field: String or List value. It must match column names. When field is a list, the function counts different fields combinations
        # About nulls values: This function does not consider null values on its calculations
        # Importing Libraries and Modules
            # Nothing to import
        # Selecting column. Dropping null values. Grouping by field using count function. The result is a spark dataframe
        df=dataframe.select(field).dropna(how='any',subset=field).groupBy(field).count()
        # Sorting spark dataframe by count column
        df_sorted=df.orderBy('count',ascending=False)
        # Printing message that indicates the number of rows in the spark dataframe. By default, the show() method only shows a maximum of 20 rows
        print('IMPORTANT!! The result contains ',df_sorted.count(),' rows')
        return df_sorted # This function returns a spark dataframe

    # Function to obtain the upper and lower limits for the +/- 1.5(IQR) rule. This values are used to determine if a value within a spark dataframe column can be considered an outlier
    def iqr_outliers_limits(self, dataframe, field):
        # Paramenters:
            # datadrame: must be a spark dataframe
            # field: String value. It must match a single column name
        # About nulls values: This function does not consider null values on its calculations
        # Importing Libraries and Modules
            # Nothing to import
        # Selecting column. Dropping null values
        df=dataframe.select(field).dropna(how='any',subset=field)
        # Applying approxQuatile function to calculate percentiles 25,50, and 75. The result is a list with the three values
        quartiles_list=df.approxQuantile(col=field,probabilities=[0.25,0.5,0.75],relativeError=0.05)
        # Calculating the interquartile range, which is equal to Percentile75 - Percentile25. The interquartile range is a statistic to measure variability in a set of numerical data
        IQR=quartiles_list[2]-quartiles_list[0]
        # The lower level is equal to Percentile25 (first element of quartiles_list: index 0) - 1.5 times IQR
        lower_limit=quartiles_list[0]-1.5*IQR
        # The upper level is equal to Percentile75 (third element of quartiles_list: index 2) + 1.5 times IQR
        upper_limit=quartiles_list[2]+1.5*IQR
        # Creating a list with 2 elements: lower level and upper level
        limits_list = [lower_limit,upper_limit]
        return limits_list # This function returns a list

    # Function to add a column with outlier/normal tag to a spark dataframe based on IQR rule (+/- 1.5 IQR)
    def outlier_tag(self,dataframe,field,filter=False):
        # Paramenters:
            # datadrame: must be a spark dataframe
            # field: String value. It must match a single column name
            # filter: Boolean value. Default value is False. When False, the function returns the original spark dataframe with new column 'outlier_tag'. When True, the function returns the original spark dataframe with new column 'outlier_tag' and filtered by 'outlier_tag' using the value 'outlier'
        # About nulls values: This function does not consider null values on its calculations
        # Importing Libraries and Modules
        import pyspark.sql.functions as F
        # Dropping rows where field column shows null values
        df=dataframe.dropna(how='any',subset=field)
        # Calling iqr_outliers_limits function
        limits_IQR_rule = self.iqr_outliers_limits(df,field)
        # Adding new column to the dataframe with tag 'outlier' or 'normal'
        df_tagged=df.withColumn('outlier_tag',F.when((F.col(field)<limits_IQR_rule[0]) | (F.col(field)>limits_IQR_rule[1]), 'outlier').otherwise('normal'))
        # Filtering dataframe by 'outlier_tag' column using value 'outlier'
        df_filtered=df_tagged.filter(df_tagged.outlier_tag == 'outlier')
        # Using conditional statement based on 'filter' parameter
        if filter==True:
            new_df=df_filtered
        else:
            new_df=df_tagged
        # Counting number of rows in df_filtered ('outlier')
        outliers_qty=df_filtered.count()
        # Printing message that indicates the number of outliers in the dataframe. By default, the show() method only shows a maximum of 20 rows
        print('There are ',outliers_qty,' outliers within ',field,' values')
        return new_df # This function returns a spark dataframe

class visualizations(object):
    
    # Function to get a horizontal bar chart with data labels
    def bar_chart(self,dataframe,order='descending',height=300):
        # Parameters:
            # datadrame: must be a spark dataframe. The dataframe must have only two columns. The second column must contain a numerical variable
            # order: String value. Default value is 'descending'. Other possible value is 'ascending'. This parameter sorts the bars in the chart
            # height: Integer value. Default value is 300. This parameter affects the final height of the chart
        # About nulls values: This function does not consider null values on its calculations
        # Importing Libraries and Modules
        import altair as alt
        # Dropping null values in the second column
        df=dataframe.dropna(how='any',subset=dataframe.columns[1])
        # Converting spark dataframe into pandas dataframe so we can use Altair library to create our bar chart
        pandas_df=df.toPandas()
        # Object to plot the bar chart. In 'y' axis, we are sorting the values by second column values. The operation ('op') used is 'sum'
        bars=alt.Chart(pandas_df).mark_bar().encode(x=pandas_df.columns[1],y=alt.Y(pandas_df.columns[0],sort=alt.EncodingSortField(field=pandas_df.columns[1],op='sum',order=order)))
        # Object to plot data labels in the barchart. dx is the distance between label and bar
        text=bars.mark_text(align='left',baseline='middle',dx=3).encode(text=pandas_df.columns[1])
        return (bars + text).properties(height=height) # This function returns a horizontal bar chart with data labels
    
    # Function to get a box plot
    def box_plot(self,dataframe,field):
         # Parameters:
            # datadrame: must be a spark dataframe
            # field: String value. It must match a single column name
        # About nulls values: This function does not consider null values on its calculations
        # Importing Libraries and Modules
        import seaborn as sns
        # Selecting column. Dropping null values.
        df=dataframe.select(field).dropna(how='any',subset=field)
        # Converting spark dataframe into pandas dataframe so we can use Seaborn library to create our box plot
        panda_df=df.toPandas()
        return sns.boxplot(x=panda_df[field]) # This function returns a box plot
    
    # Function to get a time series line chart
    def time_series_chart(self,dataframe,field,interval,operation,fig_width=16,fig_height=4):
         # Parameters:
            # datadrame: must be a pandas dataframe
            # field: String value. It must match a single column name. The function will group the results using the field column
            # interval: String value. Find aliases here: https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
            # operation: String value. Operations defined so far: 'count','sum','mean'
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
        return analysis_plot.transpose().fillna('').dropna(axis=1,how='all') # This function also shows a pandas dataframe with details by field and time interval
        

class dataManipulation(object):
    
    # Function to get a horizontal bar chart with data labels
    def time_series_dataframe(self,dataframe,time_field):
         # Parameters:
            # datadrame: must be a spark dataframe
            # field: String value. It must match a single column name
        # About nulls values: This function does not consider null values on its calculations
        # Importing Libraries and Modules
        import pyspark.sql.functions as F
        # Dropping null values. Creating a new column with timestamp format. Dropping original time_field. Converting spark dataframe into pandas dataframe. Setting 'timestamp' as dataframe index. Sorting new index
        df=dataframe.dropna(how='any',subset=time_field).withColumn('timestamp',F.to_utc_timestamp(time_field,tz='UTC')).drop(time_field).toPandas().set_index('timestamp').sort_index()
        return df # This functions returns a pandas dataframe