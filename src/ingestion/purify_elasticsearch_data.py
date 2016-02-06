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
from ast import literal_eval as le
from collections import OrderedDict
from BeautifulSoup import BeautifulSoup
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from datetime import datetime
from src.helper_modules import utility_functions as uf

CSV_FIELDS = ["event",
              "session_id",
              "user_id",
              "timestamp",
              "query",
              "lat",
              "lng",
              "closest_foo_id",
              "closest_bar_id",
              "closest_lat",
              "closest_lng",
              "foo_id",
              "bar_id",
              "reservation_id",
              "source"]

EST_TIMEZONE = pytz.timezone('US/Eastern')
PK_FIELDS = ['event', 'session_id', 'timestamp']
EXPORT_DIR = 'exports'

seen_pks = []

def make_client():
    hostname = '52.72.147.112'
    urllib3.disable_warnings()
    warnings.filterwarnings("ignore",category=UserWarning)
    return Elasticsearch([hostname], timeout=100)

def parse_args():
    parser = argparse.ArgumentParser(description="Export a CSV of demand data from Elasticsearch")
    parser.add_argument('--date', type=str, metavar='date', help="The date as YYYY.MM.DD", required=True)
    args = parser.parse_args()
    validate_args(args)

    return args

def validate_args(args):
    date_regexp = re.compile('^\d{4}\.\d{2}.\d{2}$')
    if len(date_regexp.findall(args.date)) != 1:
        sys.exit('ERROR: date parameter needs to be YYYY.MM.DD')

def parse_event(event):
    pk = make_pkey(event)

    if pk in seen_pks:
        return ''

    seen_pks.append(pk)

    line = '"%s"' % make_pkey(event)
    event_dict = event.to_dict()

    for field in CSV_FIELDS:
        value = event_dict.setdefault(field,'')

        # If there is no source present on the event, assume this event came from res-ui
        if field == 'source' and not value:
            value = 'X'

        if field == 'timestamp':
            dt = datetime.strptime(value[0:16],'%Y-%m-%dT%H:%M')
            value = str(EST_TIMEZONE.localize(dt))

        line += ',"%s"' % value

    line += '\r\n'

    return line

def make_query(client, date):
    search_index = 'logstash-%s' % date
    s = Search(using=client, index=search_index)
    s = s.query('match',type='foo_runtime')
    s = s.query('match',app='reservations-ui')
    s = s.filter('exists',field='session_id')

    return s

def make_pkey(event):
    unhashed = ''
    for field in PK_FIELDS:
        unhashed += event[field]

    hash = hashlib.md5()
    hash.update(unhashed)

    return int(hash.hexdigest(), 16)

def main():
    args = parse_args()
    query = make_query(make_client(), args.date)
    result_generator = query.scan()

    if not os.path.exists(EXPORT_DIR):
        os.makedirs(EXPORT_DIR)

    file_name = '%s/demand_data-%s.csv' % (EXPORT_DIR,args.date)
    f = open(file_name,'w')

    print 'Querying elasticsearch'
    print 'Writing events to file...'

    for event in result_generator:
        sys.stdout.write('.')
        f.write(parse_event(event).encode('utf-8'))
    f.close()

    print '\n\nOutput file created: %s' % file_name
def clean_data(msg):
    ''' Clean data. Convert python dict to json object.
        Remove html elements and return just plain text
        
        :args: msg - Dictionary of deal components.
    '''
    temp_msg = msg
    clean_description = strip_html_tags(temp_msg['description'])
    clean_fineprint = strip_html_tags(temp_msg['fine_print'])
    temp_msg['description'] = clean_description
    temp_msg['fine_print'] = clean_fineprint
    return temp_msg

def fetch_and_clean_up():
    ''' Fetch Elastic data and clean it up '''
    # Logstash and HDFS general info
    output_dir = uf.mkdir_if_not_exist('/tmp/exstreamly_cheap_files/elasticsearch_cleanup')
    logstash_file = os.path.join(output_dir, 'clean_deals.json')
    
    # HDFS Related data
    group = 'deals_data_hdfs'
    topic_id = 'elastic_deals_data'
    timestamp = time.strftime('%Y%m%d%H%M%S')
    hadoop_file = os.path.join(output_dir, 'hdfs_{}.dat'.format(timestamp))
    hadoop_path = '/exstreamly_cheap_main_files/all_deals/history'
    cached_path = '/exstreamly_cheap_main_files/all_deals/cached'
    hadoop_fullpath = '{}/{}_{}_{}.dat'.format(hadoop_path, group, topic_id, timestamp)
    cached_fullpath = '{}/{}_{}_{}.dat'.format(cached_path, group, topic_id, timestamp)
    
    uf.print_out('Writing the logs to {}'.format(hadoop_file))
    uf.print_out('Writing the logs to {}'.format(logstash_file))
    
    block_cnt = 0
    client = make_client()
    cc = Search(using=client, index='all_deals_data_index')
    gen = cc.scan()
    for event in gen:
        new_string = dict(eval(event.message.encode('utf-8')))
        msg = clean_data(new_string)
        with open(logstash_file, 'a') as log_output:
            log_output.write(json.dumps(msg) + '\n')
        with open(hadoop_file, 'a') as hdp_output:
            hdp_output.write(json.dumps(msg) + '\n')
            if hdp_output.tell() > 100000000:
                uf.print_out("Block {}: Flushing 100MB file to HDFS => {}".format(str(block_cnt), hadoop_fullpath))
                # place blocked messages into history and cached folders on hdfs
                os.system('hdfs dfs -put {} {}'.format(temp_file_path, hadoop_fullpath))
                os.system('hdfs dfs -put {} {}'.format(temp_file_path, cached_fullpath))
                
                # Back up in S3
                uf.print_out('Syncing {} to S3 for back up'.format(output_dir))
                os.system('aws s3 sync {} s3://emmanuel-awa/clean_data_from_elastic'.format(output_dir))
                hadoop_file = os.path.join(output_dir, 'hdfs_{}.dat'.format(time.strftime('%Y%m%d%H%M%S')))
        block_cnt += 1
def strip_html_tags(text):
    ''' Use BeautifulSoup to strip html tags '''
    clean_text = ''.join(BeautifulSoup(text).findAll(text=True)) if text is not None else ''
    return clean_text    

if __name__ == '__main__':
    fetch_and_clean_up()