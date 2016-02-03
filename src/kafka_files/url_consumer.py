'''
    kafka_consumer.py
    A module that consumers from kafka topics subscribed to
'''
import json
import os
import sys
import logging
import requests as rq
from collections import OrderedDict
from  url_producer import Producer
from threading import Thread, BoundedSemaphore
from Queue import Queue
from datetime import datetime
from pykafka import KafkaClient
from config import settings 
from src.helper_modules import utility_functions as uf
try:
    import configparser # for Python 3
except ImportError:
    import ConfigParser as configparser # Python 2

class Consumer(object):
    
    
    def __init__(self, config, consumer_mode, to_producer=True):
        ''' Init a consumer based on mode activated in input '''
        self.config = config
        self.config_section = consumer_mode
        self.to_producer = to_producer
        config_params = self.get_config_items()
        try:
            self.kafka_hosts = config_params['kafka_hosts']
            self.in_topic = config_params['in_topic']
            self.out_topic = config_params['out_topic']
            self.group = config_params['in_group']
            self.zk_hosts = config_params['zookeeper_hosts']
        except KeyError:
            raise
        uf.print_out("Trying to make connection {}".format(self.in_topic))
        self.client = KafkaClient(hosts=self.kafka_hosts) # Create a client
        self.topic = self.client.topics[self.in_topic] # create topic if not exists
        self.consumer = self.topic.get_balanced_consumer( # Zookeeper dynamically assigns partitions
            consumer_group=self.group,
            auto_commit_enable=True,
            zookeeper_connect=self.zk_hosts)
        uf.print_out("Made connection")
        if self.to_producer: # write into producer
            try:
                self.out_group = config_params['out_group']
                self.out_topic = self.client.topics[config_params['out_topic']]
            except KeyError:
                raise
        else:
            self.output = uf.mkdir_if_not_exist() # write to /tmp/exstreamly_cheap
        uf.print_out("Created output file or producer stage")
        self.partitions = set()
        self.msg_cnt = 0 # Num consumed by instance.
        self.init_time = datetime.now()
        self.start_time = self.init_time
        self.url_queue = Queue(maxsize=0) # infinitely sized
        self.semaphore = BoundedSemaphore()
       
    def consumer_url(self):
        ''' Consumer a kafka message and get url to fetch '''
        self.start_time = datetime.now() # For logging
        uf.print_out("Inside Consumer url")
        while True:
            "Trying to consume message"
            message = self.consumer.consume() # Read one message (url)
            uf.print_out(message.value)
            self.partitions.add(message.partition.id)
            self.get_category_deals(message)
            
    def get_category_deals(self, msg):
        ''' Fetch all deals from url found in msg '''
        url = self.get_url_msg(msg)
        list_of_pages = self.get_pagenums_msg(msg)
        num_threads = len(list_of_pages)
        uf.print_out("Inside get_category_deals: {} \n{}".format(num_threads, url))
        if self.queue_urls(url, list_of_pages):
            for idx in xrange(num_threads):
                worker = Thread(target=self.fetch_request_data, 
                                name='Thread-{}'.format(idx),
                                args=(list_of_pages[idx],))
                worker.setDaemon(True)
                worker.start()
        else:
            raise Queue.Full
            
    def fetch_request_data(self, page_num):
        ''' Fetch request data from queued up urls '''
        uf.print_out("Inside fetch_request_data")
        while not self.url_queue.empty():
            uf.print_out("Trying to dequeue.... Is queue empty? {}".format(self.url_queue.empty()))
            url = self.url_queue.get()
            try:
                req = rq.get(url)
            except rq.exceptions.RequestException:
                continue
            if not req.ok:
                continue
            try:
                data = req.json()['deals']
            except simplejson.scanner.JSONDecodeError:
                continue
            if not data: # JSON Object Ok but no deals.
                uf.print_out("No deals found on page {}. Continuing....".format(page_num))
                continue
        
            # Write deals to output one at a time
            for deal in self._filter_json_fields(data):
                self.msg_cnt += 1
                if self.to_producer: # write to producer
                    with self.out_topic.get_producer() as prod:
                        prod.produce(str(deal))
                        uf.print_out("{} strings written to producer".format(self.msg_cnt))
                else: # write to file
                    uf.print_out("Waiting to acquire lock...")
                    self.semaphore.acquire() # Thread safe I/O write
                    uf.print_out(" ==> Got the lock...")
                    uf.print_out("Trying to write to file")
                    with open('deals.json', 'a') as f:
                        f.write(json.dumps(str(deal)))
                        f.write('\n')
                    uf.print_out("{} strings written to file".format(self.msg_cnt))
                    self.semaphore.release()
            uf.print_out(" ==> Released the lock...")
            self.url_queue.task_done()
    
    def _filter_json_fields(self, all_deals):
        ''' Select only relevant json fields in deals '''
        for idx, deal in enumerate(all_deals):
            uf.print_out('Processing deal: {}'.format(idx))
            if deal:
                output = OrderedDict()
                output['id'] = deal['deal']['id']
                output['category'] = deal['deal']['category_slug']
                output['sub_category'] = deal['deal']['category_slug']
                output['title'] = deal['deal']['short_title']
                output['description'] = deal['deal']['description']
                output['fine_print'] = deal['deal']['fine_print']
                output['number_sold'] = deal['deal']['number_sold']
                output['url'] = deal['deal']['untracked_url']
                output['price'] = deal['deal']['price']
                output['discount_percentage'] = deal['deal']['discount_percentage']
                output['provider_name'] = deal['deal']['provider_name']
                output['online'] = deal['deal']['online']
                output['expires_at'] = deal['deal']['expires_at']
                output['created_at'] = deal['deal']['created_at']
                output['updated_at'] = deal['deal']['updated_at']
                output['merchant_id'] = deal['deal']['merchant']['id']

                # Online merchants have fields null. Change to ''
                # and then flatten merchant info
                merchant_info = deal['deal']['merchant']
                if not all(merchant_info.values()):
                    merchant_info = self._clean_merchant_info(merchant_info) 
                output['merchant_name'] = merchant_info['name']
                output['merchant_address'] = merchant_info['address']
                output['merchant_locality'] = merchant_info['locality']
                output['merchant_region'] = merchant_info['region']
                output['merchant_postal_code'] = merchant_info['postal_code']
                output['merchant_country'] = merchant_info['country']
                output['merchant_latitude'] = merchant_info['latitude']
                output['merchant_longitude'] = merchant_info['longitude']
                output['merchant_phone_number'] = merchant_info['phone_number']
                
                yield output
            else:
                uf.print_out('[EMPTY DEAL] - Could not process: #{}'.format(idx))
                            
    def _clean_merchant_info(self, merchant_info_dict):
        ''' Replace null values in merchant info with empty string '''
        for k, v in merchant_info_dict.iteritems():
            if not v:
                merchant_info_dict[k] = ''
        return merchant_info_dict

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
    
    def get_config_items(self):
        ''' Retrieve relevant config settings for section
            applicable to this type of instance for 
            group, in_topic, out_topic if available
        '''
        try:
            return dict(self.config.items(self.config_section))
        except NoSectionError:
            raise NoSectionError('No section: {} exists in the config file'
                                 .format(self.config_section))

if __name__ == '__main__':
#    uf.print_out(settings.SQOOT_API_KEY)
#    uf.print_out(settings.SQOOT_BASE_URL)

    '''
    settings.SQOOT_API_KEY
    settings.SQOOT_BASE_URL
    settings.CONSUMER_MODE_URL
    settings.CONSUMER_MODE_DATA
    settings.PRODUCER_MODE_URL
    settings.PRODUCER_MODE_DATA
    '''
    config = configparser.SafeConfigParser()
    config.read('../../config/general.conf')
    con = Consumer(config, settings.CONSUMER_MODE_URL)
    con.consumer_url()
