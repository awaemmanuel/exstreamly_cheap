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
from datetime import datetime
from kafka.client import KafkaClient
from kafka.producer import KeyedProducer

class Producer(object):
    def __init__(self, addr):
        self.client = KafkaClient(addr)
        self.producer = KeyedProducer(self.client)


    def produce_deal_urls(self, api_url=''):
        ''' Constantly produce deal urls for consumers to crawl '''
        # TODO - Find total deals per category
        
        # TODO - Calculate number of pages to crawl
        
        # TODO - Produce categories and page range for consumers
        # {category_slug; start_page; end_page}
        
        

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

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key) 
