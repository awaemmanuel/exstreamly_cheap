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

if __name__ == '__main__':
    uf.print_out('Cleaning Merchants Table.')
    df_merchants = create_dataframe('/tmp/exstreamly_cheap_files/merchants.json')
    df_merchants = remove_duplicate_deals(df_merchants)
    unique_merchants = count_unique_rows(df_merchants)
    uf.print_out('Number of unique merchants serving deals: {}'.format(unique_merchants))
    
    uf.print_out('Cleaning Dining and Nightlife Table.')
    df_nitelife = create_dataframe('/tmp/exstreamly_cheap_files/dining-nightlife.json')
    df_nitelife = remove_duplicate_deals(df_nitelife)
    unique_nitelife = count_unique_rows(df_nitelife)
    uf.print_out('Number of unique Dining and Nightlife deals: {}'.format(unique_nitelife))
    
    uf.print_out('Cleaning Activities and Events Table.')
    df_events = create_dataframe('/tmp/exstreamly_cheap_files/activities-events.json')
    df_events = remove_duplicate_deals(df_events)
    unique_events = count_unique_rows(df_events)
    uf.print_out('Number of unique Activities and Events deals: {}'.format(unique_events))
    
    uf.print_out('Cleaning Products Table.')
    df_products = create_dataframe('/tmp/exstreamly_cheap_files/product.json')
    df_products = remove_duplicate_deals(df_products)
    unique_products = count_unique_rows(df_products)
    uf.print_out('Number of unique Product deals: {}'.format(unique_products))

