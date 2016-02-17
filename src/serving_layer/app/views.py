import re
import random
from datetime import datetime
from app import app
from flask import jsonify, render_template 
from cassandra.cluster import Cluster

# setting up connections to cassandra
cluster = Cluster(['172.31.2.39']) 
session = cluster.connect('deals')

# setting up connections to realtime cluster
cluster_rt = Cluster(['172.31.2.36'])
session_rt = cluster_rt.connect('deals_streaming')

@app.route('/')
@app.route('/index')
def index():
    response_list = []
    stmt = 'SELECT full_name, latitude, longitude from deals.users LIMIT %s'
    response = session.execute(stmt, parameters=[int(10)])
    for val in response:
        response_list.append([val.full_name, float(val.latitude), float(val.longitude)])
    return render_template('index.html', locations=response_list)

@app.route('/api/email/<date>')
def get_email(email, date):
        stmt = "SELECT * FROM email WHERE id=%s and date=%s"
        response = session.execute(stmt, parameters=[email, date])
        response_list = []
        for val in response:
             response_list.append(val)
        jsonresponse = [{"first name": x.fname, "last name": x.lname, "id": x.id, "message": x.message, "time": x.time} for x in response_list]
        return jsonify(emails=jsonresponse)

@app.route('/api/<merchants>')
def get_all_merchants(merchants, full=False):
    if not full:
        stmt = 'SELECT id, name, address, postal_code, country, phone_number from merchants'
        response = session.execute(stmt)
        response_list = []
        for val in response:
            response_list.append(val)
        json_response = [{'merchant id': x.id, 'name': x.name, 'address': x.address,\
                         'postal code': x.postal_code, 'phone': x.phone_number, \
                          'country': x.country} for x in response_list]
        return jsonify(merchants=json_response)
            
@app.route('/category')
def get_categories():
    return render_template('webpage/index.html')

@app.route('/main/page')
def main_index():
    return render_template('index-h5.html')

# Fetch all users location and populate map on view
@app.route('/api/users_locations/<num>')
def get_users_locations(num=100):
    response_list = []
    stmt = 'SELECT full_name, dateOf(time_of_creation) as t_of_c, latitude, longitude from deals.users LIMIT %s'
    response = session.execute(stmt, parameters=[int(num)])
    for val in response:
        response_list.append(val)
    json_response = [{
            'name': x.full_name,
            'joined': x.t_of_c, 
            'lat': x.latitude,
            'long': x.longitude
        } for x in response_list]
    return jsonify(users_loc_info=json_response)



###################### REAL TIME QUERIES ###############################
categories_formal_name = {
 'activities-events': 'Activities & Events',
 'adult': 'Adult Products',
 'audio': 'Audio & Accessories',
 'automotive': 'Automotive',
 'automotive-services': 'Automotive Services',
 'baby': 'Baby',
 'bars-clubs': 'Bars & Clubs',
 'beauty_health': 'Beauty & Health',
 'boot-camp': 'Boot Camp',
 'bowling': 'Bowling',
 'bridal': 'Bridal',
 'chiropractic': 'Chiropractic',
 'city-tours': 'City Tours',
 'college': 'College',
 'comedy-clubs': 'Comedy Clubs',
 'concerts': 'Concerts',
 'crafts_hobbies': 'Crafts & Hobbies',
 'dance-classes': 'Dance Classes',
 'dental': 'Dental',
 'dermatology': 'Dermatology',
 'dining-nightlife': 'Dining & Nightlife',
 'electronics': 'Electronics',
 'eye-vision': 'Eye & Vision',
 'facial': 'Facial',
 'fashion_accessories': 'Fashion Accessories',
 'fitness': 'Fitness',
 'fitness-classes': 'Fitness Classes',
 'fitness_product': 'Fitness',
 'food-grocery': 'Food & Grocery',
 'food_alcohol': 'Food & Alcohol',
 'gay': 'Gay',
 'gifts': 'Gift Ideas',
 'golf': 'Golf',
 'gym': 'Gym',
 'hair-removal': 'Hair Removal',
 'hair-salon': 'Hair Salon',
 'health-beauty': 'Health & Beauty',
 'home-services': 'Home Services',
 'home_goods': 'Home Goods',
 'jewish': 'Jewish',
 'kids': 'Kids',
 'kitchen': 'Kitchen',
 'kosher': 'Kosher',
 'life-skills-classes': 'Life Skills Classes',
 'luggage': 'Luggage & Baggage',
 'makeup': 'Makeup',
 'manicure-pedicure': 'Manicure & Pedicure',
 'martial-arts': 'Martial Arts',
 'massage': 'Massage',
 'mens-clothing': u"Men's Clothing",
 'mens_fashion': u"Men's Fashion",
 'mobile': 'Mobile Devices & Accessories',
 'movies_music_games': 'Movies, Music, & Games',
 'museums': 'Museums',
 'office_supplies': 'Office Supplies',
 'outdoor-adventures': 'Outdoor Adventures',
 'personal-training': 'Personal Training',
 'pets': 'Pets',
 'photography-services': 'Photography Services',
 'pilates': 'Pilates',
 'product': 'Product',
 'restaurants': 'Restaurants',
 'retail-services': 'Retail & Services',
 'skiing': 'Skiing',
 'skydiving': 'Skydiving',
 'spa': 'Spa',
 'special-interest': 'Special Interest',
 'sporting-events': 'Sporting Events',
 'tanning': 'Tanning',
 'teeth-whitening': 'Teeth Whitening',
 'theater': 'Theater',
 'tools': 'Tools & Hardware',
 'toys': 'Toys',
 'travel': 'Travel',
 'treats': 'Treats',
 'wine-tasting': 'Wine Tasting',
 'women_fashion': u"Women's Fashion",
 'womens-clothing': u"Women's Clothing",
 'yoga': 'Yoga'}

comments = [
    'Found amazing deals in ',
    'ExStreamly Cheap gave me the best prices in ',
    'Now I know where to go and get deals in ',
    'Yoh! Dude, check out deals in ',
    'Rush! Rush!! Rush!!! Find really cheap and trending deals in ',
    'No retreat no surrender, I just got deals in ',
    'Wake up and smell the coffee. I got cheap ',
    'I have saved a lot. ExStreamly Cheap offers deals in '
]

def split_and_match_categories(msg):
    ''' Split the string of categories, match them up 
        with their formal names and return new string.
    '''
    msgs = msg.split(' ')
    new_msg = [categories_formal_name[category] for category in msgs]
    return ' '.join(new_msg).encode('utf-8')

def time_formatted(time_int):
    ''' Format time for display '''
    return datetime.strptime(str(time_int), "%Y%m%d%H%M%S").strftime("%H:%M:%S %Y-%m-%d")

@app.route('/api/trending_categories_by_time')
def get_trending_categories_by_time():
    ''' Return the trending categories in real time '''
    response_list = []
    stmt = 'SELECT * FROM trending_categories_by_time LIMIT 10;'
    response = session_rt.execute(stmt)
    for val in response:
        # Format the category : bars-club => Bars & Club
        category = categories_formal_name[val.category]
        ts = time_formatted(val.ts)
        count = random.randint(1, 10) + val.count
        response_list.append([ts, category, count])
    return jsonify(data=response_list)

@app.route('/api/users_purchasing_pattern')
def get_users_purchasing_timeline():
    ''' Retrieve the purchasing pattern of users in real time '''
    response_list = []
    stmt = 'SELECT * FROM users_purchasing_pattern limit 15;'
    response = session_rt.execute(stmt)
    for val in response:
        response_list.append([val.name, 
                              time_formatted(val.purchase_time), 
                              '{}{}'.format(random.choice(comments), split_and_match_categories(val.purchased))])
    return jsonify(data=response_list)
        
    
        
