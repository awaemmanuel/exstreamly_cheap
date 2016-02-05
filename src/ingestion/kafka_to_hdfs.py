'''
    kafka_to_hdfs.py
    Module that consumes kafka messages and stores them in hdfs.
'''
import os
import time
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from config import settings
try:
    import configparser # for Python 3
except ImportError:
    import ConfigParser as configparser # Python 2

class ConsumerToHDFS(object):
    """Kafka consumer class with functions to consume messages to HDFS.

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
    """
    def __init__(self, addr, group, topic):
        """Initialize Consumer with kafka broker IP, group, and topic."""
        self.client = KafkaClient(addr)
        self.consumer = SimpleConsumer(self.client, group, topic,
                                       max_buffer_size=0) # Infinite size, take all possible.
        self.temp_file_path = None
        self.temp_file = None
        self.hadoop_path = "/user/Deals/history"
        self.cached_path = "/user/Deals/cached"
        self.topic = topic
        self.group = group
        self.block_cnt = 0

    def consume_topic(self, output_dir):
        """Consumes a stream of messages from the "messages" topic.

        Code template from https://github.com/aouyang1/PuppyPlaydate.git

        Args:
            output_dir: string representing the directory to store the 100MB
                before transferring to HDFS

        Returns:
            None
        """
        timestamp = time.strftime('%Y%m%d%H%M%S')
        
        # open file for writing
        self.temp_file_path = "%s/kafka_%s_%s_%s.dat" % (output_dir,
                                                         self.topic,
                                                         self.group,
                                                         timestamp)
        self.temp_file = open(self.temp_file_path,"w")

        while True:
            try:
                # get 5000 messages at a time, non blocking
                messages = self.consumer.get_messages(count=5000, block=False)
                for message in messages:
                    self.temp_file.write(message.message.value + "\n")
                # file size > 100MB
                if self.temp_file.tell() > 100000000:
                    self.flush_to_hdfs(output_dir)
                self.consumer.commit()
            except:
                # Alter the current offset in the consumer - latest known offset
                self.consumer.seek(0, 2)


    def flush_to_hdfs(self, output_dir):
        """Flushes the File into HDFS.

        Code template from https://github.com/ajmssc/bitcoin-inspector.git
        Flushes the file into two folders under
        hdfs://user/Deals/history and
        hdfs://user/Deals/cached

        Args:
            output_dir: temp folder before loading to HDFS

        Returns:
            None
        """
        self.temp_file.close()

        timestamp = time.strftime('%Y%m%d%H%M%S')


        hadoop_fullpath = "%s/%s_%s_%s.dat" % (self.hadoop_path, self.group,
                                               self.topic, timestamp)
        cached_fullpath = "%s/%s_%s_%s.dat" % (self.cached_path, self.group,
                                               self.topic, timestamp)
        print "Block {}: Flushing 100MB file to HDFS => {}".format(str(self.block_cnt),
                                                                  hadoop_fullpath)
        self.block_cnt += 1

        # place blocked messages into history and cached folders on hdfs
        os.system("sudo -u ubuntu hdfs dfs -put %s %s" % (self.temp_file_path,
                                                        hadoop_fullpath))
        os.system("sudo -u ubuntu hdfs dfs -put %s %s" % (self.temp_file_path,
                                                        cached_fullpath))
        os.remove(self.temp_file_path)

        timestamp = time.strftime('%Y%m%d%H%M%S')

        self.temp_file_path = "%s/kafka_%s_%s_%s.dat" % (output_dir,
                                                         self.topic,
                                                         self.group,
                                                         timestamp)
        self.temp_file = open(self.temp_file_path, "w")


if __name__ == '__main__':
    config = configparser.SafeConfigParser()
    config.read('../../config/general.conf')
    config_section = settings.CONSUMER_MODE_DATA
    
    print "\nConsuming messages..."
    cons = ConsumerToHDFS(addr="localhost:9092", group="deals_hdfs", topic="all_deals_data")
    cons.consume_topic("/home/ubuntu/Deals/ingestion/kafka_messages")
