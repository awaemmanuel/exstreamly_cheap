'''
    Module to batch process the categories
    of deal and merchant topics from hdfs
    @Author: Emmanuel Awa
'''
from pyspark.sql import SQLContext as sqlcon
from pyspark import SparkContext

sc = SparkContext()
sqlContext = sqlcon(sc)

df_merchants = sqlContext.read.json('/tmp/exstreamly_cheap_files/merchants.json')
df_merchants.show()

df_events = sqlContext.read.json('/tmp/exstreamly_cheap_files/activities-events.json')
df_events.printSchema()

df_nitelife = sqlContext.read.json('/tmp/exstreamly_cheap_files/dining-nightlife.json')
df_nitelife.printSchema()

df_products = sqlContext.read.json('/tmp/exstreamly_cheap_files/product.json')
df_products.printSchema()

