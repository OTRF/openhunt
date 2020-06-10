#!/usr/bin/env python

# Author: Jose Rodriguez (@Cyb3rPandaH)
# License: GNU General Public License v3 (GPLv3)

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.types import *
import pyspark.sql.functions as func
from pyspark.sql.functions import col, avg, min, max, stddev, skewness, kurtosis
   
def summary(dataframe):
    '''
    Identifying dataframe type, count of fields, and count of records.
    This function only identifies pandas and spark dataframes 
    '''
    if isinstance(dataframe, pd.DataFrame) == True: # Valdiating if object is a pandas dataframe
        dataframeType = 'Pandas Dataframe'
        countFields = dataframe.shape[0] # Calculating number of columns
        countRecords = dataframe.shape[1] # Calculating number os rows
    elif isinstance(dataframe, DataFrame) == True:
        dataframeType = 'Spark Dataframe'
        countFields = len(dataframe.columns)
        countRecords = dataframe.count()
    else:
        dataframeType = 'Other'
        countFields = 'Not supported by this function'
        countRecords = 'Not supported by this function'
    
    # Printing summary of dataframe information
    print('Dataframe type             : ', dataframeType)
    print('Number of fields available : ', countFields)
    print('Number of records available: ', countRecords)

def numStats(dataframe, field):
    '''
    This function works with pandas and spark dataframe
    Parameter field must be a String value, and it must make reference to a single column name
    Parameter field must make reference to a numerical variable
    This function does not consider null values on its calculations
    '''
    if isinstance(dataframe, pd.DataFrame) == True:
        df = dataframe[field].dropna() # Selecting column and droping null values
        # Count of Values
        count = df.count()
        countNullValues = dataframe.shape[0] - count
        # Central Tendency
        mean_value = df.sum() / count
        median_value = df.median()
        # Min, max, and Percentiles
        min_value = df.min()
        max_value = df.max()
        percentile_25 = df.quantile(0.25)
        percentile_75 = df.quantile(0.75)
        # Variation
        stddev_value = df.std()
        range_value = max_value - min_value
        IQR_value = percentile_75 - percentile_25
        #Shape
        skewness_value = df.skew()
        kurtosis_value = df.kurt()
    elif isinstance(dataframe, DataFrame) == True:
        df = dataframe.select(field).dropna(how='any')
        # Count of Values
        count=df.count()
        countNullValues = dataframe.select(field).count() - count
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
    print('Count of Null values     : ',countNullValues)
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
    data = {'Statistic': ['count', 'Count Null Values', 'mean', 'median', 'min', 'max', 'percentile25', 'percentile75', 'stddev', 'range', 'IQR', 'skewness', 'kurtosis'],
            'Values': [count,countNullValues,mean_value,median_value,min_value,max_value,percentile_25,percentile_75,stddev_value,range_value,IQR_value,skewness_value,kurtosis_value]}
    # Creating a pandas dataframe
    summary_stats = pd.DataFrame(data)
    return summary_stats # This function returns a pandas dataframe

# Function to obtain a spark dataframe with stack counting operation over a field
def stack_count(dataframe, field, ascending = False):
    # Parameters:
        # datadrame: spark dataframe
        # field: String or List value. It must match column names. When field is a list, the function counts different fields combinations
    # About nulls values: This function does not consider null values on its calculations
    # Importing Libraries and Modules
        # Nothing to import
    
    if isinstance(dataframe,pd.DataFrame) == True:
        print('Pandas dataframe: Still working on it!! Coming Soon!!')
    elif isinstance(dataframe, DataFrame) == True:
        # Selecting column. Dropping null values.
        df = dataframe.select(field).dropna(how='any',subset=field)
        # Grouping by field using count function. The result is a spark dataframe
        df_count = df.groupBy(field).count()
        # Sorting spark dataframe by count column
        df_sorted = df_count.orderBy('count',ascending = ascending)
        # Printing message that indicates the number of rows in the spark dataframe. By default, the show() method only shows a maximum of 20 rows
        # Adding a column with relative frequency
        total_count = df.count()
        df_final = df_sorted.withColumn('%', func.round(df_sorted['count'].cast(IntegerType())*100/total_count,1))
        print('IMPORTANT!! The result contains ',df_final.count(),' rows.')
        return df_final # This function returns a spark dataframe
    else:
        print('This function only supports Spark dataframes')

# Function to obtain the upper and lower limits for the +/- 1.5(IQR) rule. This values are used to determine if a value within a spark dataframe column can be considered an outlier
def iqr_outliers_limits(dataframe, field):
    # Paramenters:
        # datadrame: must be a spark dataframe
        # field: String value. It must match a single column name
    # About nulls values: This function does not consider null values on its calculations
    # Importing Libraries and Modules
        # Nothing to import
    if isinstance(dataframe,pd.DataFrame) == True:    
        # Selecting column. Dropping null values
        df=dataframe[[field]].dropna(how='any',subset=[field])
        # Applying approxQuatile function to calculate percentiles 25,50, and 75. The result is a list with the three values
        quartiles_list=df.quantile([0.25,0.5,0.75])
        # Calculating the interquartile range, which is equal to Percentile75 - Percentile25. The interquartile range is a statistic to measure variability in a set of numerical data
        IQR=quartiles_list[field].values[2] - quartiles_list[field].values[0]
        # The lower level is equal to Percentile25 (first element of quartiles_list: index 0) - 1.5 times IQR
        lower_limit=quartiles_list.values[0]-1.5*IQR
        # The upper level is equal to Percentile75 (third element of quartiles_list: index 2) + 1.5 times IQR
        upper_limit=quartiles_list.values[2]+1.5*IQR
        # Creating a list with 2 elements: lower level and upper level
        limits_list = [lower_limit,upper_limit]
        return limits_list # This function returns a list
    elif isinstance(dataframe, DataFrame) == True:
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
    else:
        print('This function only supports Pandas and Spark dataframes')  

# Function to add a column with outlier/normal tag to a spark dataframe based on IQR rule (+/- 1.5 IQR)
def outlier_tag(dataframe,field,filter=False):
    # Paramenters:
        # datadrame: pandas or spark dataframe
        # field: String value. It must match a single column name
        # filter: Boolean value. Default value is False. When False, the function returns the original dataframe with new column 'outlier_tag'. When True, the function returns the original dataframe with new column 'outlier_tag' and filtered by 'outlier_tag' using the value 'outlier'
    # About nulls values: This function does not consider null values on its calculations
    # Importing Libraries and Modules
    if isinstance(dataframe,pd.DataFrame) == True:
        # Dropping rows where field column shows null values
        df=dataframe.dropna(how='any',subset=[field])
        # Calling iqr_outliers_limits function
        limits_IQR_rule = iqr_outliers_limits(df,field)
        # Adding new column to the dataframe with tag 'outlier' or 'normal'
        df.loc[df[field].values < limits_IQR_rule[0], 'outlier_tag'] = 'outlier'
        df.loc[df[field].values > limits_IQR_rule[1], 'outlier_tag'] = 'outlier'
        df.loc[(df['outlier_tag'].values != 'outlier'), 'outlier_tag'] = 'normal'       
        df_tagged = df
        # Using conditional statement based on 'filter' parameter
        # Filtering dataframe by 'outlier_tag' column using value 'outlier'
        df_filtered=df_tagged[df_tagged['outlier_tag'] == 'outlier']
        if filter==True:
            new_df=df_filtered
        else:
            new_df=df_tagged
        # Counting number of rows in df_filtered ('outlier')
        outliers_qty=len(df_filtered.index)
        # Printing message that indicates the number of outliers in the dataframe. By default, the show() method only shows a maximum of 20 rows
        print('There are ',outliers_qty,' outliers out of ',len(df_tagged.index),' within ',field,' values')
        return new_df
    elif isinstance(dataframe, DataFrame) == True:
        import pyspark.sql.functions as F
        # Dropping rows where field column shows null values
        df=dataframe.dropna(how='any',subset=field)
        # Calling iqr_outliers_limits function
        limits_IQR_rule = iqr_outliers_limits_2(df,field)
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
        return new_df
    else:
        print('This function only supports Pandas and Spark dataframes')



