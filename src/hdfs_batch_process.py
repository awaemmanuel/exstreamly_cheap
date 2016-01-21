'''
    Module to batch process the categories
    of deal and merchant topics from hdfs
    @Author: Emmanuel Awa
'''
from pyspark.sql import SQLContext as sqlcon
from pyspark import SparkContext
from helper_modules import utility_functions as uf

sc = SparkContext()
sqlContext = sqlcon(sc)

def create_dataframe(json_filepath):
    ''' Read in a json file and return a dataframe '''
    return sqlContext.read.json(json_filepath)

def remove_duplicate_deals(df):
    ''' Return new dataframe with distinct records '''
    return df.distinct()

def count_unique_rows(clean_df):
    ''' Return number of unique deals in a category '''
    return clean_df.count()

#def batch_process_data(data):
#    ''' Call all helper functions to batch process files '''
#    file_name = data[0]

if __name__ == '__main__':
    categories = ['merchants', 'dining-nightlife', 'activities-events', 'product']
    for category in categories:
        uf.print_out('Cleaning {} Table.'.format(category.capitalize()))
        df_category = create_dataframe('/tmp/exstreamly_cheap_files/{}.json'.format(category))
        df_category = remove_duplicate_deals(df_category)
        unique_vals = count_unique_rows(df_category)
        uf.print_out('Number of unique {} serving deals: {}'.format(category, unique_vals))