'''
    stream_user_interaction.py
    Module that streams the way users interact and subscribe to types of deals.
    Output will be to display trending deals and live feed on display.
'''
import json
import pyspark_cassandra
from pyspark import SparkContext, SparkConf
from datetime import datetime
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, Row

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def retrieve_subscriptions(subscriber_info):
    ''' Retrieve the subscription list to count trends  '''
    if subscriber_info:
        categories = []
        subscriptions = subscriber_info['subscribed_to']
        categories.extend(subscriptions)
        return ' '.join(categories).encode('utf-8') # string of subscriptions.
    else:
        return ''

# Convert RDDs of the words DStream to DataFrame and run SQL query
def process_trends(time, rdd):
    print '========= Trending Categories: {} ========='.format(str(time))
    
    try:
        ''' input: kids 
        personal
        training 
        wine-tasting
        personal
        restaurants
        wine-tasting
        bars-clubs
        '''
        # Get the singleton instance of SQLContext
        sqlContext = getSqlContextInstance(rdd.context)
        
        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda c: Row(category=c, ts=int(datetime.now().strftime('%Y%m%d%H%M%S'))))
        #rowRdd = rdd.map(lambda c: Row(categories=c))
        categories_df = sqlContext.createDataFrame(rowRdd)
        #categories_df.show()

        # Register as table
        categories_df.registerTempTable('trending_categories_by_time')

        # Count category trending in time window
        category_counts_df = sqlContext.sql('select category, first(ts) as ts, count(*) as count from trending_categories_by_time group by category')
        
        #category_counts_df = sqlContext.sql('select *  from trending_categories_by_time')
        category_counts_df.show()
   
        category_counts_df.write.format("org.apache.spark.sql.cassandra").options(table="trending_categories_by_time",keyspace="deals_streaming").save(mode="append")
        print "Appended to table trending_categories_by_time: {}".format(datetime.now().strftime('%Y-%m-%d %H%M%S'))
    except:
        pass 

def process_users_info(time, rdd_user_info):
    ''' Process and insert user information into DB '''
    print '========= USERS PROCESS AT: {} ========='.format(str(time))
    try:
        ''' input: {u'timestamp': 20160216232746, u'name': u'Donald Sarver', u'subscribed_to': [u'kids', u'personal-training', u'wine-tasting', u'bars-clubs']}'''
        # Get the singleton instance of SQLContext
        sqlContext = getSqlContextInstance(rdd_user_info.context)
        
        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd_user_info.map(lambda u: Row(name=u['name'], 
                                                 purchase_time=int(u['timestamp']), 
                                                 purchased=retrieve_subscriptions(u)))
        user_df = sqlContext.createDataFrame(rowRdd)
        user_df.write.format("org.apache.spark.sql.cassandra")\
                     .options(table="users_purchasing_pattern",keyspace="deals_streaming")\
                     .save(mode="append")
        print "Appended to table users_purchasing_pattern: {}".format(datetime.now().strftime('%Y-%m-%d %H%M%S'))
    except:
        pass

##########################################################################
#                       MAIN EXECUTION                                   #
##########################################################################
if __name__ == '__main__':
    # Configure spark instance
    sc = SparkContext()
    ssc = StreamingContext(sc, 3)
    
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

    directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": '172.31.2.36:9092'})
    
    ''' Read json input
        Output - {u'timestamp': 20160216232746, u'name': u'Donald Sarver', u'subscribed_to': [u'kids', u'personal-training', u'wine-tasting', u'bars-clubs', u'womens-clothing', u'travel', u'facial']}
    '''
    json_lines = directKafkaStream.map(lambda (info, json_line): json.loads(json_line))

    ''' Return overall subscriptions in time window
              Output of format - 'facial automotive-services boot-camp pets city-tours yoga'
    '''
    categories = json_lines.map(lambda msg: retrieve_subscriptions(msg)).filter(lambda x: len(x) > 0)
       
    # Send user and subscriptions information to DB using timestamp
    users_info = json_lines.foreachRDD(process_users_info)
    
    # Convert the subscriptions into a string of subscriptions.
    split_categories = categories.flatMap(lambda line: line.split(' '))
    
    # Process Trending categories in time window
    split_categories.foreachRDD(process_trends)

    ssc.start()
    ssc.awaitTermination()

