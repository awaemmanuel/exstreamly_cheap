'''
    kafka_consumer.py
    A module that consumers from kafka topics subscribed to
'''
import logging
from config import settings 
from kafka import SimpleProducer, create_message
from kafka.client import KafkaClient
from kafka.consumer import KafkaConsumer
client = KafkaClient('52.70.92.128')
consumer = KafkaConsumer('price_data_part4',client_id='client_2', group_id='my_group_2', bootstrap_servers=['localhost:9092'])
consumer.set_topic_partitions({'price_data_part4': [0,2]})

'''
logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%\
(process)d:%(message)s',
    level=logging.DEBUG
)
'''

for message in consumer:
     print "{}:{}:{}: key={} value={}".format(message.topic, message.partition, message.offset, message.key, message.value)
    
class Consumer(object):
    ''' A consumer class object '''
    
    def __init__(self, addr, topic, client_id_str, server_list=['localhost:9092']):
        self.topic = topic
        self.client = KafkaClient(addr)
        self.consumer = KafkaConsumer(topic, 
                                      client_id=client_id_str, 
                                      bootstrap_servers=server_list)
        
    def __init__(self, group, config, config_name):
        self.client = KafkaClient(hosts=config.get(config_name, 'kafka_hosts'))     # Create a client
        self.topic = self.client.topics[config.get(config_name, 'topic')]       # create topic if not exists
        self.consumer = self.topic.get_balanced_consumer(       # Zookeeper dynamically assigns partitions
            consumer_group=group,
            auto_commit_enable=True,
            zookeeper_connect=config.get(config_name, 'zookeeper_hosts'))
        self.partitions = set()
            
    def consumer_url(self, partition_list=None):
        ''' Consumer a kafka message and get url to fetch '''
        if partition_list:
            self.consumer.set_topic_partitions(
                {
                    self.topic : partition_list
                })
        for message in self.consumer:
            self.get_category_deals(message)
            
    def get_category_deals(self, msg):
        ''' Fetch all deals from url found in msg '''
        url = 
    def get_consumed_partitions(self):
        ''' Track partitions consumed by consumer instance '''
        return self.partitions
    

if __name__ == '__main__':
    print settings.SQOOT_API_KEY
    print settings.SQOOT_BASE_URL
    
    



