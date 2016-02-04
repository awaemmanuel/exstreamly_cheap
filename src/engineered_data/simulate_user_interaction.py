'''
    Module: simulate_user_interaction.py
    Module that simulates user interaction
    
    @Author: Emmanuel Awa
'''
import names
from random import randint
from datetime import datetime
from pykafka import KafkaClient
from config import settings 
from generate_user_subscription import SubscribeDeal
    

class SimulateInteraction(object):
    
    def __init__(self, num_users_to_gen):                       
        self._num = num_users_to_gen
        self._max_num_channels = 10 # User can sub to max 10 channels
        config = configparser.SafeConfigParser()
        config.read('../../config/general.conf')
        try:
            config_params = dict(config.items(settings.PRODUCER_SIMULATE_USERS))
            self.kafka_hosts = config_params['kafka_hosts']
            self.out_topic = config_params['out_topic']
            self.group = config_params['in_group']
            self.zk_hosts = config_params['zookeeper_hosts']
        except NoSectionError:
            raise NoSectionError('No section: {} exists in the config file'
                                 .format(settings.PRODUCER_SIMULATE_USERS)) 
        except KeyError:
            raise
        
    def simulate(self, num_of_users=1000000): 
        ''' Simulate users subscribing to channels '''  
        for num in num_of_users:
            full_name = self._generate_random_name()
            num_channels = randint(1, self._max_num_channels)
            sub = SubscribeDeal(full_name)
            subscription = sub.subscribe(num_channels)
            uf.print_out("[SUCCESSFUL] - {} subscribed ==> {}."
                         .format(full_name, sub.get_users_channels()))
#            with self.out_topic.get_producer() as prod:
#                prod.produce(str(subscription))
#                uf.print_out("[SUCCESSFUL] - {} Users written to producer".format(num))
#    
    
    def _generate_random_name(self):
        ''' Generate random full name
            Using open source library from treyhunner
        '''
        return names.get_full_name()
    
if __name__ == '__main__':
    sim = SimulateInteraction()
    sim.simulate()

        
        
        
        