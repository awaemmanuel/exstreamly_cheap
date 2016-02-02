'''
    Module that generates all deals categories from their parent categories.
    @Author: Emmanuel Awa
'''
from config import settings
from fetch_sqoot_data import map_categories, get_request
from kafka_files import url_producer as up
try:
    import configparser # for Python 3
except ImportError:
    import ConfigParser as configparser # Python 2


class ProcessCategories(object):
    
    def __init__(self, url, list_of_categories=None):
        ''' Initialize processing 
            :list_of_categories: List[str] - iterable. if None, then we process
                                everything.
        '''
        self.categories = []
        if list_of_categories:
            self.categories = list_of_categories
        self.url = url
        self.config = configparser.SafeConfigParser()
        self.config.read('../../config/general.conf')
        self.config_section = settings.PRODUCER_MODE_URL
        config_params = self.get_config_items()
        try:
            self.kafka_hosts = config_params['kafka_hosts']
            self.out_topic = config_params['out_topic']
            self.zk_hosts = config_params['zookeeper_hosts']
        except KeyError:
            raise
        self.producer = up.Producer(self.kafka_hosts)
        
    
    def process(self):
        ''' Process all categories in REPL '''
        # TODO
        pass
        
    def get_all_categories(self):
        ''' Retrieve all categories to process '''
        # TODO
        pass
    
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
