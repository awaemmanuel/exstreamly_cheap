'''
    Module to batch process the categories
    of deal and merchant topics from hdfs
    @Author: Emmanuel Awa
'''
from pyspark.sql import SQLContext as sqlcon
from pyspark import SparkContext

sc = SparkContext()
sqlContext = sqlcon(sc)

def create_dataframe(json_filepath, is_from_hdfs=True):
    ''' Read in a json file and return a dataframe '''
    return sqlContext.read.json(json_filepath)

def remove_duplicate_deals(df):
    ''' Return new dataframe with distinct records '''
    return df.distinct()

def count_unique_rows(clean_df):
    ''' Return number of unique deals in a category '''
    return clean_df.count()

