'''
    kafka_consumer.py
    A module that consumers from kafka topics subscribed to
'''
import json
import os
import sys
import logging
import requests as rq
from threading import Thread, BoundedSemaphore
from Queue import Queue
from datetime import datetime
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
        self.semaphore = BoundedSemaphore()
            
    def consumer_url(self):
        ''' Consumer a kafka message and get url to fetch '''
        self.start_time = datetime.now() # For logging
        while True:
            message = self.consumer.consume() # Read one message (url)
            print(message.value)
            self.partitions.add(message.partition.id)
            self.get_category_deals(message)
#            print type(self.get_pagenums_msg(message.value))
            self.msg_cnt += 1
            
    def get_category_deals(self, msg):
        ''' Fetch all deals from url found in msg '''
        url = self.get_url_msg(msg)
        list_of_pages = self.get_pagenums_msg(msg)
        num_threads = len(list_of_pages)
        #print "Inside get_category_deals: {} \n{}".format(num_threads, url)
        if self.queue_urls(url, list_of_pages):
            for idx in xrange(num_threads):
                worker = Thread(target=self.fetch_request_data, 
                                name='Thread-{}'.format(idx),
                                args=(self.url_queue,))
                worker.setDaemon(True)
                worker.start()
                #worker.join()
        else:
            raise Queue.Full
            
    def fetch_request_data(self, field='deals'):
        ''' Fetch request data from queued up urls '''
        #print "Inside fetch_request_data"
        while True:
            #print "Trying to dequeue.... Is queue empty? {}".format(self.url_queue.empty())
            url = self.url_queue.get()
            req = rq.get(url)
            self.semaphore.acquire()
            with open('deals.json', 'a') as f:
                f.write(json.dumps(req.json()['deals']))
                f.write('\n')
            self.semaphore.release()
            self.url_queue.task_done()
            
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
    
    def get_url_msg(self, msg):
        ''' Retrieve the url from kafka message '''
        return msg.value.split('=>')[1].strip()
        return self.split_return_by_idx(msg, 1)
    
    def get_pagenums_msg(self, msg):
        ''' Retrieve page chunks to fetch data
            List embedded in kafka msg needs to 
            reconverted to a list
        '''
        pages = self.split_return_by_idx(msg, 2)
        pages = pages.strip('[]') 
        pages = pages.split(', ')
        return map(lambda x: int(x), pages)
        
    def split_return_by_idx(self, msg, idx, by_token='=>'):
        ''' Split msg and return part by idx '''
        return msg.value.split(by_token)[idx].strip()
        
    def queue_urls(self, url, list_of_pages):
        ''' Queue download ready urls '''
        for page in list_of_pages:
            try:
                self.url_queue.put('{};page={}'.format(url, page))
            except Queue.Full:
                return False
        return True

if __name__ == '__main__':
#    print settings.SQOOT_API_KEY
#    print settings.SQOOT_BASE_URL
    config = configparser.SafeConfigParser()
    config.read('../../config/general.conf')
    con = Consumer('test_group', config, 'server_settings')
    con.consumer_url()
    



