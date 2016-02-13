'''
    Module to batch process the categories
    of deal and merchant topics from hdfs
    @Author: Emmanuel Awa
'''
from pyspark.sql import SQLContext as sqlcon
from pyspark import SparkContext
from collections import OrderedDict

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

def evaluate_and_purify(msg):
    ''' Evaluate string as literal, 
        if msg is a stringified python object
        return object else fail gracefully.
        
        :args: msg - Possible stringified python object
        :return: cleaned msg - python dict
    '''
    try:
        msg = eval(msg.encode('utf-8'))
        if isinstance(msg, OrderedDict):
            return dict(msg)
        return msg
    except SyntaxError:
        return msg # already a json
    except Exception:
        raise