'''
    stream_user_interaction.py
    Module that streams the way users interact and subscribe to types of deals.
    Output will be to display trending deals and live feed on display.
'''
import json
import uuid
import pyspark_cassandra
from pyspark import SparkContext, SparkConf
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, Row

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def retrieve_subscriptions(subscriber_info):
    ''' Retrieve the subscription list to process top 10 '''
    if subscriber_info:
        subscriptions = subscriber_info['subscribed_to']
        return ' '.join(subscriptions).encode('utf-8')
    else:
        return ''

# Convert RDDs of the words DStream to DataFrame and run SQL query
def process(time, rdd):
    print("========= %s =========" % str(time.strftime("%Y%m%d%H%M%S")))
    
    try:
        # Get the singleton instance of SQLContext
        sqlContext = getSqlContextInstance(rdd.context)
    
        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda c: Row(categories=c, ts=str(time)))
        #rowRdd = rdd.map(lambda c: Row(categories=c))
        categories_df = sqlContext.createDataFrame(rowRdd)
        #new_time  = time_uuid.TimeUUID.convert(str(time))
        # Register as table
        categories_df.registerTempTable('trending_categories_by_time')

        # Do word count on table using SQL and print it
        category_counts_df = sqlContext.sql('select categories, first(ts) as ts, count(*) as count from trending_categories_by_time group by categories')
        new_df = sqlContext.sql('select * from trending_categories_by_time')
        #category_counts_df = sqlContext.sql('select *  from trending_categories_by_time')
        category_counts_df.show()
        
        new_df.show()
        category_counts_df.write.format("org.apache.spark.sql.cassandra").options(table="trending_categories_by_time",keyspace="deals_streaming").save(mode="append")
    except:
        pass
    
##########################################################################
#                       MAIN EXECUTION                                   #
##########################################################################
if __name__ == '__main__':
    # Configure spark instance
    sc = SparkContext()

    ssc = StreamingContext(sc, 10)
    
    # Start from beginning and consume all partitions of topic
    start = 0
    partition_1 = 0
    partition_2 = 1
    partition_3 = 2
    partition_4 = 3
    topic = 'user_subscription'
    topicPartition1 = TopicAndPartition(topic, partition_1)
    topicPartition2 = TopicAndPartition(topic, partition_2)
    topicPartition3 = TopicAndPartition(topic, partition_3)
    topicPartition3 = TopicAndPartition(topic, partition_4)
    
    fromOffset = {topicPartition1: long(start), topicPartition2: long(start), topicPartition3: long(start)}

    directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": '52.71.152.72.147.112:9092,52.72.209.156:9092,52.72.105.140:9092,52.72.200.42:9092'}, fromOffsets\
    =fromOffset)
    
    # Read json
    parsed_lines = directKafkaStream.map(lambda (info, json_line): json.loads(json_line))
    
    extracted = parsed_lines.map(lambda msg: retrieve_subscriptions(msg)).filter(lambda x: len(x) > 0)
    
    # Convert the subscriptions into a string of subscriptions.
    categories = extracted.flatMap(lambda line: line.split(' '))
    
    # Count the categories subscripted in the last 10 seconds.
    categories.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()

