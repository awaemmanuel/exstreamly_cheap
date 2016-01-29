''' 
    kafka_producer.py - Module that produces into MQs
        1. Produces urls for consumers to crawl.
        2. Produces the deals to be queued for persistence
           storage and search.
    @Author: Emmanuel Awa
    
'''
import random
import sys
import six
import json
import requests as rq
from datetime import datetime
from kafka.client import KafkaClient
from kafka.producer import KeyedProducer
from config import settings

# settings.SQOOT_API_KEY
# settings.SQOOT_BASE_URL
    
class Producer(object):
    

    def __init__(self, addr, topic):
        self.client = KafkaClient(addr)
        self.producer = KeyedProducer(self.client)
        self._last_update = datetime.utcnow() # For latest deals
        self.topic = topic
        self._more_pages = 20

    def produce_deal_urls(self, url, partition_key, max_deals_per_page=100, initial_visit=True):
        ''' Constantly produce deal urls for consumers to crawl '''
        if not initial_visit: # Get the right URL to crawl
            # Search the UTC time delta since last visit for this category
            checked_last = self._last_update.strftime("%Y-%m-%dT%H:%M:%S%Z")
            url = '{};updated_after={}'.format(url,checked_last)
        req = self.fetch_request(url)
        
        # Calculate number of pages to crawl
        # Max 100 per page, crawl total//100
        total_per_category = req.json()['query']['total']
        num_pages_to_fetch = (total_per_category / max_deals_per_page) + 1
        
        '''
            Produce categories and page range for consumers
            Crawl extra pages to account for changing api.
            Pages with no deals will be filtered out by consumer
            Recommended approaches to partitioning
            1. max(t/p, t/c) partitions.
                - t: Required throughput
                - p: Production speed
                - c: consumption speed
            2. Rule of thumb - 100 * b * r
                - b: # of brokers in cluster
                - r: # of replication factor
            {category_slug; start_page; end_page}
        '''
        for page_num in xrange(1, num_pages_to_fetch + self._more_pages):
            msg = '{}'
            
            
            
    def produce_deal_full_data(self):
        time_stamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%Z")
        pass

    def produce_msgs(self, source_symbol):
        price_field = random.randint(800,1400)
        msg_cnt = 0
        while True:
            time_field = datetime.now().strftime("%Y%m%d %H%M%S")
            price_field += random.randint(-10, 10)/10.0
            volume_field = random.randint(1, 1000)
            str_fmt = "{};{};{};{}"
            message_info = str_fmt.format(source_symbol,
                                          time_field,
                                          price_field,
                                          volume_field)
            print message_info
            self.producer.send_messages('price_data_part4', source_symbol, message_info)
            msg_cnt += 1
            
    def fetch_request(self, url):
        ''' Return url to endpoint '''
        return rq.get(url)
    
    def yield_chunks(self, int_list, num):
        ''' Yield successive chunks of size num from lists '''
        
    

    
if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key) 
