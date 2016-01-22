'''
    Module that helps insert data from spark to database
    @Author: Emmanuel Awa
'''
from cassandra.cluster import Cluster
from src.helper_modules import utility_functions as uf
from src.batch_process import hdfs_batch_process as hbp
from create_database_schema import *

# Constants definition
INPUT_KEY_CONVERTER = "com.parsely.spark.converters.FromUsersCQLKeyConverter"
INPUT_VALUE_CONVERTER = "com.parsely.spark.converters.FromUsersCQLValueConverter"
OUTPUT_KEY_CONVERTER = "com.parsely.spark.converters.ToCassandraCQLKeyConverter"
OUTPUT_VALUE_CONVERTER = "com.parsely.spark.converters.ToCassandraCQLValueConverter"

if __name__ == '__main__':
    cluster = Cluster(['172.31.2.39'])
    session = cluster.connect(deals)
    categories = ['merchants', 'dining_nightlife', 'activities_events', 'product']
    for category in categories:
        uf.print_out('Cleaning {} Table.'.format(category.capitalize()))
        df_category = hbp.create_dataframe('/tmp/exstreamly_cheap_files/{}.json'.format(category))
        df_category = hbp.remove_duplicate_deals(df_category)
        unique_vals = hbp.count_unique_rows(df_category)
        uf.print_out('Number of unique {} serving deals: {}'.format(category, unique_vals))
        
        
        # Insert dataFrames with all our categories into Cassandra
        if category == 'merchants':
            stmt = session.prepare()
        stmt = session.prepare('''
            INSERT INTO deals.{} (id, merchant_id, provider, title, category, sub_category, description, fine_print, price, percentage_disc, number_sold, created_at, expires_at, updated_at, url, online)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        '''.strip().format(category))
        

        
    
    
