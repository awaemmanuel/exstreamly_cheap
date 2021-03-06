'''
    Cleanup_hdfs_files.py
    Module that takes files in history folder in hdfs, 
    performs a literal evaluation of strings, strips html tags
    from deals description, and then resaves it as proper json.
'''
import time
import json
import uuid
import pyspark_cassandra
from cassandra.cluster import Cluster
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from datetime import datetime
from collections import OrderedDict
from BeautifulSoup import BeautifulSoup
from datetime import datetime
#from src.ingestion import purify_elasticsearch_data as purify

def evaluate_and_purify(msg):
    ''' Evaluate string as literal,
        if msg is a stringified python object
        return object else fail gracefully.

        :args: msg - Possible stringified python object
        :return: cleaned msg - python dict
    '''
    print "Inside evaluate and purify"
    msg = to_unicode(msg)
    try:
        msg = eval(msg)
    except SyntaxError:
        raise
    except NameError:
        pass
    if isinstance(msg, OrderedDict):
        msg = dict(msg)
    elif not isinstance(msg, dict) and is_json(msg)
        msg = json.loads(msg)
    elif isinstance(msg, str):
        msg = msg.rstrip('\n')
        msg = eval(msg)
        if isinstance(msg, dict) or isinstance(msg, OrderedDict):
            msg = dict(msg)
        else:
            try: 
                msg = json.dumps(msg)
                print "Might be json already of type {} =>  {}".format(type(msg), msg)
            except:
                pass
    else:
        raise("Unknown Format {} {}".format(type(msg), msg))
    print type(msg)    
        
    cleaned = clean_data(msg) # Remove html tags from deals description
    return cleaned

def strip_html_tags(string_with_html):
    ''' Use BeautifulSoup to strip html tags '''
    return ''.join(BeautifulSoup(string_with_html)\
                   .findAll(text=True)) if string_with_html is not None else ''

def is_json(myjson):
  try:
    print "Inside is_json try"
    json_object = json.loads(myjson)
  except ValueError, e:
    return False
  except TypeError:
      return False
  return True

def clean_data(msg):
    '''
        Remove html elements and return just plain text

        :args: msg - Dictionary of deal components.
    '''
    if isinstance(msg, dict):
        clean_description = strip_html_tags(msg['description'])
        clean_fineprint = strip_html_tags(msg['fine_print'])
        msg['description'] = clean_description
        msg['fine_print'] = clean_fineprint
        msg['price'] = float(msg['price']) if msg['price'] else 0.0
        msg['discount_percentage'] = float(msg['discount_percentage']) if msg['discount_percentage'] else 0.0
        msg['merchant_longitude'] = str(msg['merchant_longitude']) if msg['merchant_longitude'] else '0.0'
        msg['merchant_latitude'] = str(msg['merchant_latitude']) if msg['merchant_latitude'] else '0.0'
        msg['number_sold'] = int(msg['number_sold']) if msg['number_sold'] else  0
        msg['merchant_id'] = int(msg['merchant_id']) if msg['merchant_id'] else 9999999999999
        msg['merchant_postal_code'] = msg['merchant_postal_code'] if msg['merchant_postal_code'] else '00000'
        msg['expires_at'] = msg['expires_at'] or '2040-12-31T00:00:00Z'
        msg['created_at'] = msg['created_at'] or '2015-12-31T00:00:00Z'
        msg['updated_at'] = msg['updated_at'] or '2016-01-15T00:00:00Z'
        msg['category'] = msg['category'] or 'invalid category'
        return msg 
    else:
         print "{} is not a dictionary, moving on...".format(msg)
################ HELPER FUNCTIONS ###############################
def to_unicode(unicode_or_str):
    ''' Convert strings to unicode for internal processes '''
    if isinstance(unicode_or_str, str):
        val = unicode_or_str.decode('utf-8')
    else:
        val = unicode_or_str
    return val

def to_str(unicode_or_str):
    ''' Convert unicode to string to write to output '''
    if isinstance(unicode_or_str, unicode):
        val = unicode_or_str.encode('utf-8')
    else:
        val = unicode_or_str
    return val

def print_stats(rdd):
    ''' Function to print some general stats about the RDD '''
    num_lines      = rdd.count()
    sample_lines   = rdd.sample(False, min(1.0, 5.0 / float(num_lines))).collect()
    num_partitions = rdd.getNumPartitions()
    storage_level  = rdd.getStorageLevel()
    print 'RDD ID: %d' % rdd.id()
    print 'RDD Number of partitions: %d' % num_partitions
    print 'RDD Storage level: %s' % storage_level
    print 'Number of lines: %d' % num_lines
    print 'Sample lines:'
    for line in sample_lines:
        print '    %s' % str(line)
    return

if __name__ == '__main__':
    print "Starting at..... {}".format(time.strftime('%H:%M:%S'))
    cluster = Cluster(['172.31.2.39', '172.31.2.40','172.31.2.41', '172.31.2.42'])
    session = cluster.connect('deals')
    appName = 'CleanUpHDFSFiles'
    master = 'spark://ip-172-31-2-36:7077'
    executor_memory = 'spark.executor.memory'
    driver_memory = 'spark.driver.memory'
    conf = SparkConf().setAppName(appName).setMaster(master).set(executor_memory, '6g').set(driver_memory, '10g')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    base_uri = 'hdfs://52.1.154.19:9000/exstreamly_cheap_main_files/all_deals/cached'
    #base_uri = 'hdfs://52.1.154.19:9000/exstreamly_cheap_main_files/all_deals/history/deals_data_hdfs_all_deals_data_20160210063946.dat'
    
    # Create external dataset
    distFile = sc.textFile(base_uri)
    distFile = distFile.map(evaluate_and_purify)
    new_table = distFile.map(lambda p: Row(id=int(p['id']), title=p['title'], category=p['category'], sub_category=p['sub_category'], description=p['description'], fine_print=p['fine_print'], price=p['price'], discount_percentage=p['discount_percentage'], created_at=p['created_at'], updated_at=p['updated_at'], expires_at=p['expires_at'], number_sold=p['number_sold'], url=p['url'], online=p['online'], provider_name=p['provider_name'], merchant_id=p['merchant_id'], merchant_name=p['merchant_name'], merchant_address=p['merchant_address'], merchant_locality=p['merchant_locality'], merchant_region=p['merchant_region'], merchant_country=p['merchant_country'], merchant_phone_number=p['merchant_phone_number'], merchant_longitude=p['merchant_longitude'], merchant_latitude=p['merchant_latitude']))
    schemaDeals = sqlContext.inferSchema(new_table)#, samplingRatio=None)
    schemaDeals.registerTempTable("Deals")

    #sample = sqlContext.sql("SELECT category, merchant_name, provider_name FROM Deals limit 50")
    sample = sqlContext.sql('SELECT * FROM Deals')
    
    ## Drop Null Values
    sample.dropna()
    time_now = int(datetime.now().strftime('%Y%m%d%H%M'))
    
    ## Write the trending categories by price into database
    sample.write.format('org.apache.spark.sql.cassandra').options(table='trending_categories_with_price', keyspace='deals').save(mode='append')

    ## Write the trending categories by discount into database
    sample.write.format('org.apache.spark.sql.cassandra').options(table='trending_categories_with_disc', keyspace='deals').save(mode='append')

    ## Write to the provider_by_category table
    sqlContext.sql('select category, provider_name, merchant_address, merchant_country, merchant_id, merchant_latitude, merchant_locality,merchant_longitude,merchant_name,merchant_phone_number,merchant_region,number_sold from Deals').write.format('org.apache.spark.sql.cassandra').options(table='provider_by_category', keyspace='deals').save(mode='append')
    
    print "Finished at..... {}".format(time.strftime('%H:%M:%S'))
