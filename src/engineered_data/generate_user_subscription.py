'''
    generate_user_subscription.py
    Moduel that simulates users and types of deals they are interested in.
    
    @Author: Emmanuel
'''
import random
import json
from collections import OrderedDict
from datetime import datetime
from src.helper_modules import utility_functions as uf

class SubscribeDeal(object):
    
    def __init__(self, user_name):
        ''' Subscribe to channels (deals) for future recommendation '''
        self._all_deals = self._define_all_deals()
        self._channels = []
        self._user = user_name
        
    def _define_all_deals(self):
        ''' All deals offered by sqoot '''
        return ['adult', 'audio', 'automotive', 'beauty_health', 'crafts_hobbies', 'electronics', 'fashion_accessories', 'fitness_product', 'food_alcohol', 'gifts', 'home_goods', 'kitchen', 'luggage', 'mens_fashion', 'mobile', 'movies_music_games', 'office_supplies', 'tools', 'toys', 'women_fashion', 'baby', 'bridal', 'college', 'jewish', 'kids', 'kosher', 'pets', 'travel', 'automotive-services', 'food-grocery', 'home-services', 'mens-clothing', 'photography-services', 'treats', 'womens-clothing', 'chiropractic', 'dental', 'dermatology', 'eye-vision', 'facial', 'hair-removal', 'hair-salon', 'makeup', 'manicure-pedicure', 'massage', 'spa', 'tanning', 'teeth-whitening', 'bars-clubs', 'restaurants', 'boot-camp', 'fitness-classes', 'gym', 'martial-arts', 'personal-training', 'pilates', 'yoga', 'bowling', 'city-tours', 'comedy-clubs', 'concerts', 'dance-classes', 'golf', 'life-skills-classes', 'museums', 'outdoor-adventures', 'skiing', 'skydiving', 'sporting-events', 'theater', 'wine-tasting']
    
    def total_num_deals(self):
        ''' Retrieve total deals we serve '''
        return self._all_deals
    
    def subscribe(self, num_channels):
        ''' Subscribe to certain deals '''
        user_ref = OrderedDict()
        user_ref['name'] = str(self._user)
        res = []
        self._channels = self._random_channels_select(num_channels)
        for deal_type in self._channels:
            res.append(deal_type)
        user_ref['subscribed_to'] = res
        user_ref['timestamp'] = int(datetime.now().strftime('%Y%m%d%H%M%S'))
        return json.dumps(user_ref)
    
    def _random_channels_select(self, num_of_selections):
        ''' Randomly sample entire deals n times '''
        stop_sampling = False
        channels = []
        while not stop_sampling:
            choice = random.choice(self._all_deals)
            channels.append(choice) if choice not in channels else 0
            # change flag when found enough
            stop_sampling = False or len(channels) == num_of_selections
        return channels
    
    def get_users_channels(self):
        """ Retrieve a user's assigned channels.
            Only for the engineered data. 
            Ideally user selects his own channels.
        """
        if self._channels:
            return self._channels
        else:
            uf.print_out("[ERROR] - User: {} needs to subscribe first.".format(self._user))
    
    def get_users_name(self):
        ''' Return subscriber's full name '''
        return str(self._user)
                
####### TESTING ONLY ##############                
if __name__ == '__main__':
    user = SubscribeDeal('Emmanuel')
    print user.subscribe(2)
    
