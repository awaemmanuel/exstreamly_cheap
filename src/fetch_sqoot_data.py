'''
    Module that downloads sqoot data for Insight DE project.
    Author: Emmanuel Awa
'''
import requests as rq

PUBLIC_KEY = 'pf3lj0'
base_url = 'http://api.sqoot.com/v2'
def get_request(base_url, endpoint='categories', extra_params='', public_key=PUBLIC_KEY):
    ''' Return url to endpoint '''
    return rq.get('{}/{}/?api_key={};{}'.format(base_url, endpoint, PUBLIC_KEY, extra_params))
req_categories = get_request(base_url)

main_to_sub_categories = {}
for cat in req_categories.json()['categories']:
    category =  cat['category']
    parent_slug = category['parent_slug']
    slug = category['slug']
    
    # Category is a main category
    if parent_slug is None and slug not in main_to_sub_categories.keys():
            main_to_sub_categories[slug] = []
    else: # Category may be a subcategory 
        if parent_slug not in main_to_sub_categories.keys(): # main category
            main_to_sub_categories[parent_slug] = []
        main_to_sub_categories[parent_slug].append(slug)

print main_to_sub_categories.keys()

# Get deals according to sub-categories
num_of_pages = 10000
req_deals = get_request(base_url, 'deals', 'per_page=100;')
query_params = req_deals.json()['query']
deals = req_deals.json()['deals']
print len(deals)


#{u'product': [u'adult', u'audio', u'automotive', u'beauty_health', u'crafts_hobbies', u'electronics', u'fashion_accessories', u'fitness_product', u'food_alcohol', u'gifts', u'home_goods', u'kitchen', u'luggage', u'mens_fashion', u'mobile', u'movies_music_games', u'office_supplies', u'tools', u'toys', u'women_fashion'], u'special-interest': [u'baby', u'bridal', u'college', u'gay', u'jewish', u'kids', u'kosher', u'pets', u'travel'], u'retail-services': [u'automotive-services', u'food-grocery', u'home-services', u'mens-clothing', u'photography-services', u'treats', u'womens-clothing'], u'health-beauty': [u'chiropractic', u'dental', u'dermatology', u'eye-vision', u'facial', u'hair-removal', u'hair-salon', u'makeup', u'manicure-pedicure', u'massage', u'spa', u'tanning', u'teeth-whitening'], u'dining-nightlife': [u'bars-clubs', u'restaurants'], u'fitness': [u'boot-camp', u'fitness-classes', u'gym', u'martial-arts', u'personal-training', u'pilates', u'yoga'], None: [u'dining-nightlife', u'fitness', u'health-beauty', u'product', u'retail-services', u'special-interest'], u'activities-events': [u'bowling', u'city-tours', u'comedy-clubs', u'concerts', u'dance-classes', u'golf', u'life-skills-classes', u'museums', u'outdoor-adventures', u'skiing', u'skydiving', u'sporting-events', u'theater', u'wine-tasting']}
#{u'product': [u'adult', u'audio', u'automotive', u'beauty_health', u'crafts_hobbies', u'electronics', u'fashion_accessories', u'fitness_product', u'food_alcohol', u'gifts', u'home_goods', u'kitchen', u'luggage', u'mens_fashion', u'mobile', u'movies_music_games', u'office_supplies', u'tools', u'toys', u'women_fashion'], u'special-interest': [u'baby', u'bridal', u'college', u'gay', u'jewish', u'kids', u'kosher', u'pets', u'travel'], u'retail-services': [u'automotive-services', u'food-grocery', u'home-services', u'mens-clothing', u'photography-services', u'treats', u'womens-clothing'], u'health-beauty': [u'chiropractic', u'dental', u'dermatology', u'eye-vision', u'facial', u'hair-removal', u'hair-salon', u'makeup', u'manicure-pedicure', u'massage', u'spa', u'tanning', u'teeth-whitening'], u'dining-nightlife': [u'bars-clubs', u'restaurants'], u'fitness': [u'boot-camp', u'fitness-classes', u'gym', u'martial-arts', u'personal-training', u'pilates', u'yoga'], None: [u'dining-nightlife', u'fitness', u'health-beauty', u'product', u'retail-services', u'special-interest'], u'activities-events': [u'bowling', u'city-tours', u'comedy-clubs', u'concerts', u'dance-classes', u'golf', u'life-skills-classes', u'museums', u'outdoor-adventures', u'skiing', u'skydiving', u'sporting-events', u'theater', u'wine-tasting']}

#[u'product', u'special-interest', u'retail-services', u'health-beauty', u'dining-nightlife', u'fitness', None, u'activities-events']
