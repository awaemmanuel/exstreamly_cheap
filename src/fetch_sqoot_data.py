'''
    Module that downloads sqoot data for Insight DE project.
    Author: Emmanuel Awa
'''
import json
import os
import datetime
import requests as rq
from collections import OrderedDict
from helper_modules import utility_functions as uf

PUBLIC_KEY = 'pf3lj0'
base_url = 'http://api.sqoot.com/v2'
def get_request(base_api_url, endpoint='categories', extra_params='', public_key=PUBLIC_KEY):
    ''' Return url to endpoint '''
    return rq.get('{}/{}/?api_key={};{}'.format(base_api_url, endpoint, PUBLIC_KEY, extra_params))

def reduce_categories_scope(map_of_categories, focus_list):
    ''' Focus on a list of categories '''
    return {k: v for k, v in map_of_categories.iteritems() if k in focus_list}

def category_in_mvp(mvp_focus, category_slug):
    ''' Find category a subcategory falls under '''
    for k, v in mvp_focus.iteritems():
        if category_slug in v:
            return k
    return None

def clean_merchant_info(merchant_info_dict):
    ''' Replace null values in merchant info with empty string '''
    for k, v in merchant_info_dict.iteritems():
        if not v:
            merchant_info_dict[k] = ''
    return merchant_info_dict
    
def map_categories(base_url):
    ''' Map Sqoot API main categories to subcategories '''
    req_categories = get_request(base_url)
    main_to_sub_categories = {}
    for cat in req_categories.json()['categories']:
        category = cat['category']
        parent_slug = category['parent_slug']
        slug = category['slug']

        # Category is a main category
        if parent_slug is None:
            if slug not in main_to_sub_categories.keys():
                main_to_sub_categories[slug] = []
        else: # Category may be a subcategory 
            if parent_slug not in main_to_sub_categories.keys(): # main category
                main_to_sub_categories[parent_slug] = []
            main_to_sub_categories[parent_slug].append(slug)
    return main_to_sub_categories
        
def fetch_sqoot_data(base_url):
    ''' Fetch Sqoot Data and save relevant information to file '''
    files_location = uf.mkdir_if_not_exist() # Folder in /tmp/exstreamly_cheap_files
    merchants_file = os.path.join(files_location, 'merchants.json')
    products_file = os.path.join(files_location, 'products.json')
    events_file = os.path.join(files_location, 'activities_events.json')
    food_nitelife_file = os.path.join(files_location, 'dining_nitelife.json')
    categories_map = map_categories(base_url)
    
    mvp_categories = [u'product', u'dining-nightlife', u'activities-events']
    focus_grp = reduce_categories_scope(categories_map, 
                                        mvp_categories)
    n = True
    start_time = datetime.datetime.now()
    end_time = start_time + datetime.timedelta(hours=3)
    
    while start_time < end_time:
        try:
            # Due to api inconsistencies, to always get the newest ones and page 5
            # Duplicates will be batchly processed in SPARK
            first_100_deals = get_request(base_url, 'deals', 'per_page=100;radius=10000')
            page5_100_deals = get_request(base_url, 'deals', 'page=5;per_page=100;radius=10000')
            
            # Combine both  
            # Flatten JSON, keep online merchant ID in deals file
            # Save Merchant in Merchant Table 
            all_deals = first_100_deals.json()['deals'] + page5_100_deals.json()['deals']           
            for _, deal in enumerate(all_deals):
                # If deal category belongs to mvp, save
                category = category_in_mvp(focus_grp, deal['deal']['category_slug'])
                if category:
                    output = OrderedDict()
                    output['id'] = deal['deal']['id']
                    output['category'] = category
                    output['sub_category'] = deal['deal']['category_slug']
                    output['title'] = deal['deal']['short_title']
                    output['description'] = deal['deal']['description']
                    output['fine_print'] = deal['deal']['fine_print']
                    output['number_sold'] = deal['deal']['number_sold']
                    output['url'] = deal['deal']['untracked_url']
                    output['price'] = deal['deal']['price']
                    output['discount_percentage'] = deal['deal']['discount_percentage']
                    output['provider_name'] = deal['deal']['provider_name']
                    output['online'] = deal['deal']['online']
                    output['expires_at'] = deal['deal']['expires_at']
                    output['created_at'] = deal['deal']['created_at']
                    output['updated_at'] = deal['deal']['updated_at']
                    output['merchant_id'] = deal['deal']['merchant']['id']
                    
                    # Write deal to file
                    with open(os.path.join(files_location, str(category) + '.json'), 'a') as f:
                        f.write(json.dumps(output))
                        f.write('\n')
                    
                    # Write merchant info file
                    merchant_info = deal['deal']['merchant']
                    if not all(merchant_info.values()):
                        merchant_info = clean_merchant_info(merchant_info)        
                    with open(os.path.join(files_location, 'merchants.json'), 'a') as f:
                        f.write(json.dumps(merchant_info))
                        f.write('\n')
            start_time = datetime.datetime.now()
            uf.print_out("Time left: {} minute(s)".format((end_time - start_time).seconds / 60))
            uf.spinning_cursor(5)
        except rq.exceptions.ConnectionError:
            uf.print_out("[ConnectionError] ==> Issue with API server.")
        except rq.exceptions.ConnectTimeout:
            uf.print_out("[ConnectionTimeout] ==> Server connection timing out.")

# Generate data
fetch_sqoot_data(base_url)
        


