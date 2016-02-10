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
    session = cluster.connect('deals')
    categories = ['merchants', 'dining_nightlife', 'activities_events', 'products']
    for category in categories:
        uf.print_out('Cleaning {} Table.'.format(category.capitalize()))
        file_name = 'hdfs://52.1.154.19:9000/exstreamly_cheap_files/exstreamly_cheap_files/{}.json'.format(category)
        df_category = hbp.create_dataframe(file_name)
        df_category = hbp.remove_duplicate_deals(df_category)
        unique_vals = hbp.count_unique_rows(df_category)
        uf.print_out('Number of unique {} serving deals: {}'.format(category, unique_vals))
        
        #  Insert dataFrames with all our categories into Cassandra
        df_category.registerTempTable('{}'.format(category))
        if category is 'merchants':            
            df_category.select(
                'id', 'name', 'address', 'postal_code', 'country', 
                'phone_number', 'region', 'longitude', 'latitude',  'url').write.format("org.apache.spark.sql.cassandra").options(table='merchants', keyspace='deals').save(mode='append')
        else: # other categories
            df_category.write.format('org.apache.spark.sql.cassandra').options(table='{}'.format(category), keyspace='deals').save(mode='append')
        


        
    
    
