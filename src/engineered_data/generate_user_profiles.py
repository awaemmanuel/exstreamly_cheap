'''
    generate_user_profiles.py
    Module to enginneers user profiles with time of account creation, 
    longitude and latitude information.
    
    @Author: Emmanuel
'''

import names
import csv
import uuid
import random
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from src.helper_modules import utility_functions as uf

class UserProfile(object):
    
    def __init__(self):
        ''' Initialize a user object '''
        self._name = names.get_full_name() 
        self._state = ""
        self._lat = ""
        self._long = ""
        self._active = False
        
    def assign_location(self, loc_info):
        ''' Assign a new user location '''
        self._zipcode = loc_info[0]
        self._lat = loc_info[1]
        self._long = loc_info[2]
        self._state = loc_info[3]
        self._active = True
        
    def get_location(self):
        ''' Return users location '''
        return self._state
    
    def get_name(self):
        ''' Get user name '''
        return self._name
    
    def is_active(self):
        ''' Is users profile active '''
        return self._active
        
    def __repr__(self):
        return "{} created account from {}".format(self._name, self._state)
        
def fetch_all_locations(csv_file='../../ZipCodes_with_LatLong3.csv'):
    ''' Take a csv file, extract location information
        zipcode, longitude and latitude 
    '''    
    f = open(csv_file, 'rU')
    reader = csv.DictReader(f)
    for line in reader:
        yield (line['zipcodes'], line['latitude'], line['longitude'], line['state'])
   
if __name__ == '__main__':
    cluster = Cluster(['172.31.2.39'])
    session = cluster.connect('deals')        
    locations = []
    for line in fetch_all_locations():
        locations.append(line)
    
    # User table prepared table statement
    stmt = session.prepare('INSERT INTO users (id, full_name, latitude, longitude) VALUES (?,?,?,?)')
    batch = BatchedStatement(consistency_level=ConsistencyLevel.QUORUM)
    
    # From the locations lists assign user a get random location
    for num in xrange(1, 11):
        random_location = random.choice(locations)
        # Create user object
        user = UserProfile()
        user.assign_location(random_location)
        print repr(user)
                 
        # Insert into DB
        uf.print_out('Inserting into database...')
        ts = str(uuid.uuid1())
        batch.add(stmt, 
                  (ts, user.get_name(), 
                   random_location[1],
                   random_location[2]))
        
    session.execute(batch)
        
        
        
        response = session.execute(stmt)
        