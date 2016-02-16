'''
    purify_elasticsearch_data.py
    Module that cleans out the data in elastic search and redumps it back
    into elasticsearch into another index
'''
#!/usr/bin/env python

import time
import argparse
import hashlib
import pytz
import urllib3
import re
import sys
import os
import warnings
import pdb
import json
from pykafka import KafkaClient
from ast import literal_eval as le
from collections import OrderedDict
from BeautifulSoup import BeautifulSoup
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from datetime import datetime
from config import settings
from src.helper_modules import utility_functions as uf
try:
    import configparser  # Python 3
except ImportError:
    import ConfigParser as configparser  # Python 2

def make_client():
    hostname = '52.72.147.112'
    urllib3.disable_warnings()
    warnings.filterwarnings("ignore",category=UserWarning)
    return Elasticsearch([hostname], timeout=100)

def strip_html_tags(string_with_html):
    ''' Use BeautifulSoup to strip html tags '''
    return ''.join(BeautifulSoup(string_with_html)\
                   .findAll(text=True)) if string_with_html is not None else ''

def clean_data(msg):
    ''' 
        Remove html elements and return just plain text
        
        :args: msg - Dictionary of deal components.
    '''
    clean_description = strip_html_tags(msg['description'])
    clean_fineprint = strip_html_tags(msg['fine_print'])
    msg['description'] = clean_description
    msg['fine_print'] = clean_fineprint
    return msg

def fetch_and_clean_up(index_name):
    ''' Fetch Elastic data and clean it up '''
    # Logstash and HDFS general info
    output_dir = uf.mkdir_if_not_exist('/tmp/exstreamly_cheap_files/elasticsearch_cleanup')
#    logstash_file = os.path.join(output_dir, 'clean_deals.json')
    
    # HDFS Related data
    group = 'deals_data_hdfs'
    topic_id = 'elastic_deals_data'
    timestamp = time.strftime('%Y%m%d%H%M%S')
    hadoop_file = os.path.join(output_dir, 'hdfs_{}.dat'.format(timestamp))
    hadoop_path = '/exstreamly_cheap_main_files/all_deals/history'
    cached_path = '/exstreamly_cheap_main_files/all_deals/cached'
    hadoop_fullpath = '{}/{}_{}_{}.dat'.format(hadoop_path, group, topic_id, timestamp)
    cached_fullpath = '{}/{}_{}_{}.dat'.format(cached_path, group, topic_id, timestamp)
    
    uf.print_out('Writing the logs to {} which will be pushed to hdfs and S3'.format(hadoop_file))
    
    block_cnt = 0
    client = make_client()
    cc = Search(using=client, index=index_name)
    gen = cc.scan()
    
    config = configparser.SafeConfigParser()
    config.read('../../config/general.conf')
    config_params = uf.get_config_items(config, settings.PRODUCER_CLEAN_ES_DATA)
    try:
        kafka_hosts = config_params['kafka_hosts']
        topic = config_params['topic']
        group = config_params['group']
        zk_hosts = config_params['zookeeper_hosts']
    except KeyError:
        raise
    
    kafka_client = KafkaClient(hosts=kafka_hosts)
    kafka_topic = kafka_client.topics[topic] # Create if not exist
    uf.print_out('Producing messages to topic {}. Press Ctrl-C to terminate'.format(kafka_topic.name))
    
    # Produce to kafka for distributed consumption
    hdp_output = open(hadoop_file, 'w')
    with kafka_topic.get_producer() as producer:
        for event in gen:
            new_string = dict(eval(event.message.encode('utf-8')))
            msg = clean_data(new_string)

            # We can decide to have logstash read from file instead
    #        with open(logstash_file, 'a') as log_output:
    #            log_output.write(json.dumps(msg) + '\n')
            # Write to producer.
            producer.produce(json.dumps(msg))
            
            # Back up to file for HDFS and S3
            hdp_output.write(json.dumps(msg) + '\n')
            if hdp_output.tell() > 100000000:
                hdp_output.close()
                
                uf.print_out("Block {}: Flushing 100MB file to HDFS => {}".format(str(block_cnt), hadoop_fullpath))
                
                # place blocked messages into history and cached folders on hdfs
                os.system('hdfs dfs -put {} {}'.format(hadoop_file, hadoop_fullpath))
                os.system('hdfs dfs -put {} {}'.format(hadoop_file, cached_fullpath))

                # Back up in S3
                uf.print_out('Syncing {} to S3 for back up'.format(output_dir))
                os.system('aws s3 sync {} s3://emmanuel-awa/clean_data_from_elastic'.format(output_dir))
                
                # Recreate file handler
                hadoop_file = os.path.join(output_dir, 'hdfs_{}.dat'.format(time.strftime('%Y%m%d%H%M%S')))
                hdp_output = open(hadoop_file, 'w')

            uf.print_out('Cleaned {} blocks. File size: {}KB'.format(block_cnt, hdp_output.tell()/1000))
            block_cnt += 1
            
if __name__ == '__main__':
    # Clean up both indexes in ES now and merge them as one
    for index in ['all_deals_data', 'all_deals_data_index']:
        uf.print_out('Processing {}....'.format(index))
        fetch_and_clean_up(index)
