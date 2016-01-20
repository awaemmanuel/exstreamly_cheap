'''
    Module to batch process the categories
    of deal and merchant topics from hdfs
    @Author: Emmanuel Awa
'''
from pyspark.sql import SQLContext as sqlcon
from pyspark import SparkContext

sc = SparkContext()
sqlContext = sqlcon(sc)

df = sqlContext.read.json('/tmp/exstreamly_cheap_files/merchants.json')
df.printSchema()
