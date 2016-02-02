'''
    Module that generates all deals categories from their parent categories.
    @Author: Emmanuel Awa
'''
import itertools
import time
from config import settings
from fetch_sqoot_data import map_categories, get_request
from src.kafka_files import url_producer as up
from src.helper_modules import utility_functions as uf
try:
    import configparser # for Python 3
except ImportError:
    import ConfigParser as configparser # Python 2

class ProcessCategories(object):
    
    def __init__(self, list_of_categories=None):
        ''' Initialize processing 
            :list_of_categories: List[str] - iterable. if None, then we process
                                everything.
        '''
        # Get complete list of categories if none in input.
        self._categories = list_of_categories or self._get_all_categories()
        self._config = configparser.SafeConfigParser()
        self._config.read('../../config/general.conf')
        self._config_section = settings.PRODUCER_MODE_URL
        config_params = self._get_config_items()
        try:
            self._kafka_hosts = config_params['kafka_hosts']
            self._out_topic = config_params['out_topic']
            self._zk_hosts = config_params['zookeeper_hosts']
        except KeyError:
            raise
        self._producer = up.Producer(self._kafka_hosts)
        self.process_cnt = 0
        self.max_deals_per_page = 100
    
    def process(self):
        ''' Process all categories in REPL '''
        base_url = '{}/deals?api_key={}'.format(settings.SQOOT_BASE_URL,
                                           settings.SQOOT_API_KEY)
        first_visit = True
        while True:
            if self.process_cnt == 0:
                print "[PROCESS URLS] - Starting to process first time"
                self._construct_and_produce_urls(base_url, first_visit)
                first_visit = False
            else:
                print "[PROCESS URLS] - Starting to process updates"
                self._construct_and_produce_urls(base_url)
            self.process_cnt += 1
            time.sleep(300) # check for updated deals every 5mins
            
    def _construct_and_produce_urls(self, base_url, initial_visit=False):
        ''' Process all categories. For intial visit, we get everything
            Subsequent visit, used for real time, we use updated_after 
            query param of the api
        '''
        for idx, category in enumerate(self._categories):
            print "Processing: {}".format(category) 
            url = '{};category_slug={}'.format(base_url, category)
            partition_key = idx % 4 # creating 4 partitions by default
            self._producer.produce_deal_urls(url, 
                                              self._out_topic, 
                                              partition_key,
                                              self.max_deals_per_page,
                                              initial_visit)
    def _get_all_categories(self):
        ''' Retrieve all categories to process '''
        all_categories = map_categories(settings.SQOOT_BASE_URL)
        return list(itertools.chain(*all_categories.values()))
    
    def _get_config_items(self):
        ''' Retrieve relevant config settings for section
            applicable to this type of instance for 
            group, in_topic, out_topic if available
        '''
        try:
            return dict(self._config.items(self._config_section))
        except configparser.NoSectionError:
            raise NoSectionError('No section: {} exists in the config file'
                                 .format(self._config_section))
    def categories_in_process(self):
        ''' Retrieve categories that the flow will process '''
        return self._categories
    
if __name__ == '__main__':
    process_flow = ProcessCategories()
    print process_flow.categories_in_process()
    process_flow.process() # Start producing urls
    
