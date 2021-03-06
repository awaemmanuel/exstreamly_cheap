'''
    kafka_to_hdfs.py
    Module that consumes kafka messages and stores them in hdfs.
'''
import os
import time
from pykafka import KafkaClient
from config import settings
from src.helper_modules import utility_functions as uf
try:
    import configparser # for Python 3
except ImportError:
    import ConfigParser as configparser # Python 2

class ConsumerToHDFS(object):
    '''Kafka consumer class with functions to consume messages to HDFS.

    Messages are blocked into 100MB files and transferred to HDFS in two
    folders, /user/Deals/history/ and /user/Deals/cached.
    /history/ will contain all immutable files in the case of needing to
    rebuild the entire batch view. /cached/ will be deleted on a daily basis
    for the incremental batch job.

    Attributes:
        client: string representing IP:port of the kafka broker
        consumer: Consumer object specifying the client group, and topic
        temp_file_path: location of the 100MB file to be appended to before
            transfer to HDFS
        temp_file: File object opened from temp_file_path
        topic: String representing the topic on Kafka
        group: String representing the Kafka consumer group to be associated
            with
        block_cnt: integer representing the block count for print statements
    '''
    def __init__(self, config, consumer_mode):
        ''' Init a consumer based on mode activated in input '''
        self.temp_file_path = None
        self.temp_file = None
        self.block_cnt = 0
        self.msg_cnt = 0
        self.config = config
        self.config_section = consumer_mode
        config_params = self.get_config_items()
        try:
            self.kafka_hosts = config_params['kafka_hosts']
            self.out_topic = str(config_params['out_topic'])
            self.group = str(config_params['hdfs_group'])
            self.hadoop_path = str(config_params['hadoop_path'])
            self.cached_path = str(config_params['cached_path'])
            self.zk_hosts = config_params['zookeeper_hosts']
        except KeyError:
            raise
        uf.print_out("Trying to make connection with params {}".format(config_params))
        self.client = KafkaClient(hosts=self.kafka_hosts) # Create a client
        self.topic = self.client.topics[self.out_topic] # create topic if not exists
        self.consumer = self.topic.get_balanced_consumer( # Zookeeper dynamically assigns partitions
            consumer_group=self.group,
            auto_commit_enable=True,
            zookeeper_connect=self.zk_hosts)
        uf.print_out("Made connection")

    def consume_topic(self, output_dir):
        '''Consumes a stream of messages from the "messages" topic.

        Code template from https://github.com/aouyang1/PuppyPlaydate.git

        Args:
            output_dir: string representing the directory to store the 100MB
                before transferring to HDFS

        Returns:
            None
        '''
        timestamp = time.strftime('%Y%m%d%H%M%S')
        
        # open file for writing
        self.temp_file_path = '{}/kafka_{}_{}_{}.dat'.format(output_dir, self.out_topic, self.group, timestamp)
        self.temp_file = open(self.temp_file_path, 'w')
        uf.print_out('Starting to consume and write to temp')
        while True:
            try:
                # get one consumer message - max_queued = 2000
                message = self.consumer.consume()
                #print('consuming {}....'.format(message.value))
                self.msg_cnt += 1
                #print "Message size {}".format(len(message), type(message))
                #uf.print_out(message.value)
                self.temp_file.write(message.value)
                self.temp_file.write('\n')
                # file size > 100MB
                uf.print_out('Consumed {}: of File size now: {}KB'.format(self.msg_cnt, (self.temp_file.tell()/1000)))
                if self.temp_file.tell() > 100000000:
                    self.flush_to_hdfs(output_dir)
            except:
                print "In the except"
                raise # Balanced consumer restarts automatically.

    def flush_to_hdfs(self, output_dir):
        '''Flushes the File into HDFS.

        Flushes the file into two folders under
        hdfs - History (to rebuild batch view if source of truth is down) and 
        Cache that gets flushed in time intervals

        Args:
            output_dir: temp folder before loading to HDFS

        Returns:
            None
        '''
        uf.print_out('Written {} to {}'.format(self.temp_file.tell(), self.temp_file_path))
        self.temp_file.close()

        timestamp = time.strftime('%Y%m%d%H%M%S')


        hadoop_fullpath = '{}/{}_{}_{}.dat'.format(self.hadoop_path, self.group, self.out_topic, timestamp)
        cached_fullpath = '{}/{}_{}_{}.dat'.format(self.cached_path, self.group, self.out_topic, timestamp)
        print "Block {}: Flushing 100MB file to HDFS => {}".format(str(self.block_cnt), hadoop_fullpath)
        self.block_cnt += 1

        # place blocked messages into history and cached folders on hdfs
        os.system('hdfs dfs -put {} {}'.format(self.temp_file_path, hadoop_fullpath))
        os.system('hdfs dfs -put {} {}'.format(self.temp_file_path, cached_fullpath))
        
        #uf.print_out('Removing temporary file - {}'.format(os.path.basename(self.temp_file_path)))
        # os.remove(self.temp_file_path)

        timestamp = time.strftime('%Y%m%d%H%M%S')
        self.temp_file_path = '{}/kafka_{}_{}_{}.dat'.format(output_dir, self.out_topic, self.group, timestamp)
        self.temp_file = open(self.temp_file_path, "w")
    
    def get_config_items(self):
        ''' Retrieve relevant config settings for section
            applicable to this type of instance for 
            group, in_topic, out_topic if available
        '''
        try:
            return dict(self.config.items(self.config_section))
        except configparser.NoSectionError:
            raise configparser.NoSectionError('No section: {} exists in the config file'
                                 .format(self.config_section))


if __name__ == '__main__':
    tmp_out_dir = '/home/ubuntu/exstreamly_cheap_all_deals/ingestion/kafka_messages'
    tmp_out_dir = uf.mkdir_if_not_exist(tmp_out_dir)
    uf.print_out('Output format: {}'.format(tmp_out_dir))
    config = configparser.SafeConfigParser()
    config.read('../../config/general.conf')
    print '\nConsuming messages...'
    cons = ConsumerToHDFS(config, settings.CONSUMER_MODE_DATA)
    cons.consume_topic(tmp_out_dir)
