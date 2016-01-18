'''
    Module that downloads sqoot data for Insight DE project.
'''
import requests as rq

PUBLIC_KEY = 'pf3lj0'
base_url = 'http://api.sqoot.com/v2'
url = '{}/{}/?api_key={}'.format(base_url, 'categories', PUBLIC_KEY)
req = rq.get(url)

main_to_sub_categories = {}
for cat in req.json()['categories']:
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
print main_to_sub_categories
