'''
    kafka_consumer.py
    A module that consumers from kafka topics subscribed to
'''
import json
import os, sys
import datetime
import threading
import Queue
import requests as rq
import logging
from pykafka import KafkaClient
from config import settings 
try:
    import configparser # for Python 3
except ImportError:
    import ConfigParser as configparser # Python 2

class Consumer(object):
    ''' A consumer class object '''
    
    def __init__(self, group, config, config_name):
        self.client = KafkaClient(hosts=config.get(config_name, 'kafka_hosts'))     # Create a client
        self.topic = self.client.topics[config.get(config_name, 'topic')]       # create topic if not exists
        self.consumer = self.topic.get_balanced_consumer(       # Zookeeper dynamically assigns partitions
            consumer_group=group,
            auto_commit_enable=True,
            zookeeper_connect=config.get(config_name, 'zookeeper_hosts'))
        self.partitions = set()
        self.msg_cnt = 0 # Num consumed by instance.
        self.init_time = datetime.now()
        self.start_time = self.init_time
        self.url_queue = Queue(maxsize=0) # infinitely sized
            
    def consumer_url(self):
        ''' Consumer a kafka message and get url to fetch '''
        self.start_time = datetime.now() # For logging
        while True:
            message = self.consumer.consume() # Read one message (url)
            self.partitions.add(message.partition.id)
            self.get_category_deals(message)
            self.msg_cnt += 1
            
    def get_category_deals(self, msg):
        ''' Fetch all deals from url found in msg '''
        url = message.value['fetch'] 
        list_of_pages = message.value['with_pages']
        num_threads = len(list_of_pages)
        for idx in xrange(num_threads):
            worker = Thread(target=fetch_request_data, 
                            name='Thread-{}'.format(idx),
                           args=(self.url_queue,))
            worker.setDaemon(True)
            worker.start()
            
    def fetch_request_data(self, field='deals'):
        ''' Fetch request data from queued up urls '''
        while True:
            url = self.url_queue.get()
            # TODO - Fetch deals from site.
            req = rq.get(url)
            print req.json()[fields]
            q.task_done()
    
    def get_consumed_partitions(self):
        ''' Track partitions consumed by consumer instance '''
        return sorted(self.partitions)

    def get_message_count(self):
        ''' Track total number of messages consumed '''
        return self.msg_cnt
    
    def consumption_start_time(self):
        ''' Track when consumer began consuming '''
        return self.start_time.strftime("%Y-%m-%dT%H:%M:%S%Z")
    
    def get_delta_init_consume(self):
        ''' Track latency between init and consumption '''
        return (self.start_time - self.init_time).seconds
    
if __name__ == '__main__':
#    print settings.SQOOT_API_KEY
#    print settings.SQOOT_BASE_URL
    config = configparser.SafeConfigParser()
    config.read('../../config/general.conf')
    con = Consumer('test_group', config, 'server_settings')
    
    



