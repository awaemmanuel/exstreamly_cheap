from app import app
from flask import jsonify, render_template 
from cassandra.cluster import Cluster

# setting up connections to cassandra
cluster = Cluster(['172.31.2.39']) 
session = cluster.connect('deals') 

@app.route('/')
@app.route('/index')
def index():
#    return '<h1 style="font-size:50px;text-align:center;color:red">ExStreamly Cheap is still under construction</h1>'
    return render_template('index.html')

@app.route('/api/<email>/<date>')
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
@app.route('/api/<users_locations>/<num>')
def get_users_locations(users_locations):
    response_list = []
    stmt = 'SELECT dateOf(time_of_creation) as t_of_c, latitude, longitude from deals.users LIMIT num'
    response = session.execute(stmt)
    for val in response:
        response_list.append(val)
    json_response = [{
            'Joined at': x.t_of_c, 
            'lat': x.latitude,
            'long': x.longitude
        } for x in response_list]
    return jsonify(users_locations=json_response)