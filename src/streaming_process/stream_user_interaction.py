'''
    stream_user_interaction.py
    Module that streams the way users interact and subscribe to types of deals.
    Output will be to display trending deals and live feed on display.
'''
import json
from pyspark import SparkContext, SparkConf
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from pyspark.streaming import StreamingContext

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def retrieve_subscriptions(subscriber_info):
    ''' Retrieve the subscription list to process top 10 '''
    if subscriber_info:
        return subscriber_info['subscribed_to']
    else:
        return []
    
##########################################################################
#                       MAIN EXECUTION                                   #
##########################################################################
if __name__ == '__main__':
    # Configure spark instance
    sc = SparkContext()

    ssc = StreamingContext(sc, 5)
    
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
    
    extracted = parsed_lines.map(lambda msg: retrieve_subscriptions(msg))
    
    print 'Extracted ', extracted.pprint()
    
#    lines = directKafkaStream.map(lambda x: x[1])
#    counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
#    counts.pprint()

    ssc.start()
    ssc.awaitTermination()

