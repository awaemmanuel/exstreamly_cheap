'''
    kafka_consumer.py
    A module that consumers from kafka topics subscribed to
'''
import logging
from config import settings 
from kafka import KafkaConsumer
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
    
    def __init__(self, addr):
        
    

if __name__ == '__main__':
    print settings.SQOOT_API_KEY
    print settings.SQOOT_BASE_URL
    
    



