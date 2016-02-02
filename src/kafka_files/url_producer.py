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
    
    _msg_cnt = 0
    def __init__(self, addr):
        print "Trying connection..."
        self.client = KafkaClient(addr)
        self.producer = KeyedProducer(self.client)
        print "Made connection with host: {}".format(addr)
        self._last_update = datetime.utcnow() # For latest deals
        self._more_pages = 20
        self._chunk_size = 10

    def produce_deal_urls(self, url, topic, partition_key, max_deals_per_page=100, initial_visit=True):
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
        total_pages = range(1, num_pages_to_fetch + self._more_pages)
        page_chunks = list(self.yield_chunks(total_pages, self._chunk_size))
        for chunk in page_chunks:
            time_stamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%Z")
            msg = '{} => {} => {}'.format(time_stamp, 
                                          url,
                                          chunk)
            print msg
            self.producer.send_messages(topic, partition_key, msg)
            self.__class__._msg_cnt += 1
            
    def produce_deal_full_data(self):
        time_stamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%Z")
        pass
    
    def get_total_msg_prod(self):
        ''' Returns how many messages all instances of producer sent '''
        return self.__class__._msg_cnt
    
    def fetch_request(self, url):
        ''' Return url to endpoint '''
        return rq.get(url)
    
    def yield_chunks(self, int_list, num):
        ''' Yield successive chunks of size num from lists '''
        for idx in xrange(0, len(int_list), num):
            yield int_list[idx:idx+num]
    

    
if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    #prod.produce_msgs(partition_key) 
    prod.produce_deal_urls('http://api.sqoot.com/v2/deals?api_key=pf3lj0;per_page=100;category_slugs=spa', 'deal_urls', b'1')
    print prod.get_total_msg_prod()
