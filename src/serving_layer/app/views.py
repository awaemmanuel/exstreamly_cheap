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
categories_formal_name = {u'activities-events': u'Activities & Events',
 u'adult': u'Adult Products',
 u'audio': u'Audio & Accessories',
 u'automotive': u'Automotive',
 u'automotive-services': u'Automotive Services',
 u'baby': u'Baby',
 u'bars-clubs': u'Bars & Clubs',
 u'beauty_health': u'Beauty & Health',
 u'boot-camp': u'Boot Camp',
 u'bowling': u'Bowling',
 u'bridal': u'Bridal',
 u'chiropractic': u'Chiropractic',
 u'city-tours': u'City Tours',
 u'college': u'College',
 u'comedy-clubs': u'Comedy Clubs',
 u'concerts': u'Concerts',
 u'crafts_hobbies': u'Crafts & Hobbies',
 u'dance-classes': u'Dance Classes',
 u'dental': u'Dental',
 u'dermatology': u'Dermatology',
 u'dining-nightlife': u'Dining & Nightlife',
 u'electronics': u'Electronics',
 u'eye-vision': u'Eye & Vision',
 u'facial': u'Facial',
 u'fashion_accessories': u'Fashion Accessories',
 u'fitness': u'Fitness',
 u'fitness-classes': u'Fitness Classes',
 u'fitness_product': u'Fitness',
 u'food-grocery': u'Food & Grocery',
 u'food_alcohol': u'Food & Alcohol',
 u'gay': u'Gay',
 u'gifts': u'Gift Ideas',
 u'golf': u'Golf',
 u'gym': u'Gym',
 u'hair-removal': u'Hair Removal',
 u'hair-salon': u'Hair Salon',
 u'health-beauty': u'Health & Beauty',
 u'home-services': u'Home Services',
 u'home_goods': u'Home Goods',
 u'jewish': u'Jewish',
 u'kids': u'Kids',
 u'kitchen': u'Kitchen',
 u'kosher': u'Kosher',
 u'life-skills-classes': u'Life Skills Classes',
 u'luggage': u'Luggage & Baggage',
 u'makeup': u'Makeup',
 u'manicure-pedicure': u'Manicure & Pedicure',
 u'martial-arts': u'Martial Arts',
 u'massage': u'Massage',
 u'mens-clothing': u"Men's Clothing",
 u'mens_fashion': u"Men's Fashion",
 u'mobile': u'Mobile Devices & Accessories',
 u'movies_music_games': u'Movies, Music, & Games',
 u'museums': u'Museums',
 u'office_supplies': u'Office Supplies',
 u'outdoor-adventures': u'Outdoor Adventures',
 u'personal-training': u'Personal Training',
 u'pets': u'Pets',
 u'photography-services': u'Photography Services',
 u'pilates': u'Pilates',
 u'product': u'Product',
 u'restaurants': u'Restaurants',
 u'retail-services': u'Retail & Services',
 u'skiing': u'Skiing',
 u'skydiving': u'Skydiving',
 u'spa': u'Spa',
 u'special-interest': u'Special Interest',
 u'sporting-events': u'Sporting Events',
 u'tanning': u'Tanning',
 u'teeth-whitening': u'Teeth Whitening',
 u'theater': u'Theater',
 u'tools': u'Tools & Hardware',
 u'toys': u'Toys',
 u'travel': u'Travel',
 u'treats': u'Treats',
 u'wine-tasting': u'Wine Tasting',
 u'women_fashion': u"Women's Fashion",
 u'womens-clothing': u"Women's Clothing",
 u'yoga': u'Yoga'}

@app.route('/api/trending_categories_by_time')
def get_trending_categories_by_time():
    ''' Return the trending categories in real time '''
    response_list = []
    stmt = 'SELECT * FROM trending_categories_by_time LIMIT 10;'
    response = session_rt.execute(stmt)
    for val in response:
        # Format the category : bars-club => Bars & Club
        category = categories_formal_name[val.category]
        ts = datetime.strptime(str(val.ts), "%Y%m%d%H%M%S").strftime("%H:%M:%S %Y-%m-%d")
        count = random.randint(1, 10) + val.count
        response_list.append([ts, category, count])
    return jsonify(data=response_list)
        
