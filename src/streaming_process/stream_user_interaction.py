'''
    stream_user_interaction.py
    Module that streams the way users interact and subscribe to types of deals.
    Output will be to display trending deals and live feed on display.
'''
from pyspark import SparkContext, SparkConf
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from pyspark.streaming import StreamingContext


##########################################################################
#                       MAIN EXECUTION                                   #
##########################################################################
if __name__ == '__main__':
    # Configure spark instance
    conf = (SparkConf().setMaster('spark://ip-172-31-2-36:7077')
           .setAppName('DealsUsersSubscriptions')
           .set('spark.executor.memory', '2g')
           .set('spark.cores.max', '3'))
    sc = SparkContext(conf=conf)
    
    # Retrieve broadcast variaable from cassandra
    count = sc.accumulator(0)
    ssc = StreamingContext(sc, 2)
    start = 0
    partition = 0
    topic = 'user_subscription'
    topicPartion = TopicAndPartition(topic,partition)
    fromOffset = {topicPartion: long(start)}

    directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": '52.71.152.72.147.112:9092,52.72.209.156:9092,52.72.105.140:9092,52.72.200.42:9092'}, fromOffsets\
    =fromOffset)
    lines = directKafkaStream.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()

