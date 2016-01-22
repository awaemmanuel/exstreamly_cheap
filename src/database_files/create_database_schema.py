'''
    Module that creates cassandra database schemas
    @Author: Emmanuel Awa
'''
import datetime as dt
import json
import sys

from pyspark import SparkContext
from cassandra.cluster import Cluster
from src.helper_modules import utility_functions as uf

def create_keyspace_if_not_exists(session, keyspace,
                                  replication_opts=None):
    replication_opts = replication_opts or \
                          "{'class': 'SimpleStrategy', 'replication_factor': 3}"
    rows = session.execute("SELECT keyspace_name FROM system.schema_keyspaces;")
    if not any((row.keyspace_name == keyspace for row in rows)):
        cassandra_session.execute("""
            CREATE SCHEMA {}
            WITH REPLICATION={};
        """.format(keyspace, replication_opts))
        return True
    else:
        return False
    
def create_tables(keyspace, table_name, column_descrptn):
    ''' Create table '''
    session.execute(
    '''
    CREATE TABLE 
    IF NOT EXISTS {}.{} ({})
    '''.format(keyspace, table_name, column_descrptn)
    )
    uf.print_out('Table {} in keyspace {} is \
    now created!'.format(table_name, keyspace))

def create_schemas(server_ip, keyspace_name, create_tables=[], column_description=''):
     # setting up connections to cassandra
    cluster = Cluster([str(server_ip)])
    session = cluster.connect(keyspace)
    
    # Create keyspace
    if not create_keyspace_if_not_exists(session, keyspace_name):
        uf.print_out('Keyspace {} already exists!'.format(keyspace_name))
    else:
        uf.print_out('Keyspace {} is now created!'.format(keyspace_name))
    session.set_keyspace(keyspace_name)

    # create table
    create_tables = create_tables or ['products', 'activities_events', 'dining_nightlife']
    column_description = column_description or \
    '''
    (id bigint, merchant_id bigint, provider text, 
    title text, category text, sub_category text, 
    description text, fine_print text, price float, 
    percentage_disc float, number_sold int, 
    created_at timestamp, expires_at timestamp, 
    updated_at timestamp, url text, online boolean, 
    PRIMARY KEY (id,updated_at))
    '''
    for table in create_tables:
        create_tables(keyspace_name, table, column_description)
        

'''
create table activities_events (id bigint, merchant_id bigint, provider text, title text, category text, sub_category text, description text, fine_print text, price float, percentage_disc float, number_sold int, created_at timestamp, expires_at timestamp, updated_at timestamp, url text, online boolean, PRIMARY KEY (id,updated_at));
cqlsh:deals> drop table products;
cqlsh:deals> create table products (id bigint, merchant_id bigint, provider text, title text, category text, sub_category text, description text, fine_print text, price float, percentage_disc float, number_sold int, created_at timestamp, expires_at timestamp, updated_at timestamp, url text, online boolean, PRIMARY KEY (id,updated_at));
cqlsh:deals> create table dining_nightlife (id bigint, merchant_id bigint, provider text, title text, category text, sub_category text,Welcome  description text, fine_print text, price float, percentage_disc float, number_sold int, created_at timestamp, expires_at timestamp, updated_at timestamp, url text, online boolean, PRIMARY KEY (id,updated_at));
cqlsh:deals> .schemas
'''

