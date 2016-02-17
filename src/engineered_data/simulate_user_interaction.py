'''
    Module: simulate_user_interaction.py
    Module that simulates user interaction
    
    @Author: Emmanuel Awa
'''
import names
import time
import json
from random import randint
from datetime import datetime
from pykafka import KafkaClient
from config import settings 
from generate_user_subscription import SubscribeDeal
from generate_user_profiles import UserProfile
from src.helper_modules import utility_functions as uf
try:
    import configparser # for Python 3
except ImportError:
    import ConfigParser as configparser # Python 2
    

class SimulateInteraction(object):
    
    def __init__(self):
        self._max_num_channels = 10 # User can sub to max 10 channels
        config = configparser.SafeConfigParser()
        config.read('../../config/general.conf')
        try:
            config_params = dict(config.items(settings.PRODUCER_SIMULATE_USERS))
            self.kafka_hosts = config_params['kafka_hosts']
            self.out_topic = config_params['out_topic']
            self.group = config_params['group']
            self.zk_hosts = config_params['zookeeper_hosts']
        except configparser.NoSectionError:
            raise configparser.NoSectionError('No section: {} exists in the config file'
                                 .format(settings.PRODUCER_SIMULATE_USERS)) 
        except KeyError:
            raise
        
        # Create kafka client and produce to topic, if exists
        # else create it.
        
        self.kafka_client = KafkaClient(hosts=self.kafka_hosts)
        
        self.out_topic = self.to_str(self.out_topic) # kafka only handles string bytes
        self.topic = self.kafka_client.topics[self.out_topic]  
        print type(self.out_topic), self.out_topic
        
        uf.print_out('''
            Connected with Client.
            Getting ready to produce messages to topic {}. Press Ctrl-C to interrupt. 
            '''.format(self.topic.name))
        self.msg_cnt = 0
        
        
    def simulate(self, num_of_users=1000000000): 
        ''' Simulate users subscribing to channels '''  
        with self.topic.get_producer() as prod:
            for num in xrange(1, num_of_users + 1):
                full_name = self._generate_random_name()
                num_channels = randint(1, self._max_num_channels)
                sub = SubscribeDeal(full_name)
                subscriptions = sub.subscribe(num_channels)
            
                #   Produce the subscription object to producer
                print "Testing {}".format(subscriptions)
                prod.produce(subscriptions)
                uf.print_out("[SUCCESSFUL] - {} subscribed ==> {}.".format(sub.get_users_name(),
                                                                       sub.get_users_channels()))
                uf.print_out("[SUCCESSFUL] - {} Users written to producer".format(num))
                uf.spinning_cursor(1)
    
    def _generate_random_name(self):
        ''' Generate random full name
            Using open source library from treyhunner
        '''
        return names.get_full_name()
    def to_str(self, unicode_or_str):
        ''' Convert unicode to string to write to output '''
        if isinstance(unicode_or_str, unicode):
            val = unicode_or_str.encode('utf-8')
        else:
            val = unicode_or_str
        return val
if __name__ == '__main__':
    sim = SimulateInteraction()
    uf.print_out('[START] - {}....'.format((datetime.now()).strftime("%Y-%m-%dT%H:%M:%S%Z")))
    sim.simulate()
    uf.print_out('[FINISH] - {}....'.format((datetime.now()).strftime("%Y-%m-%dT%H:%M:%S%Z")))
        
        
        
        
