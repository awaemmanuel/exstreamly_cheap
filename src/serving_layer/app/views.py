from app import app
from flask import jsonify 
from cassandra.cluster import Cluster

# setting up connections to cassandra
cluster = Cluster(['172.31.2.39']) 
session = cluster.connect('deals') 

@app.route('/')
@app.route('/index')
def index():
    return '<h1 style="font-size:50px;text-align:center;color:red">ExStreamly Cheap is still under construction</h1>'

@app.route('/api/<email>/<date>')
def get_email(email, date):
        stmt = "SELECT * FROM email WHERE id=%s and date=%s"
        response = session.execute(stmt, parameters=[email, date])
        response_list = []
        for val in response:
             response_list.append(val)
        jsonresponse = [{"first name": x.fname, "last name": x.lname, "id": x.id, "message": x.message, "time": x.time} for x in response_list]
        return jsonify(emails=jsonresponse)

