'''
    cleanup_hdfs_files.py
    Module that takes files in history folder in hdfs, 
    performs a literal evaluation of strings, strips html tags
    from deals description, and then resaves it as proper json.
'''
from pyspark import SparkContext, SparkConf
from hdfs_batch_process import evaluate_and_purify as eap

appName = 'CleanUpHDFSFiles'
master = '172.31.2.36'
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)
base_uri = 'hdfs://52.1.154.19:9000/exstreamly_cheap_main_files/all_deals/history'

# Create external dataset
distFile = sc.textFile('{}/deals_data_hdfs_all_deals_data_20160205051834.dat'.format(base_uri))

distFile = distFile.map(eap)

print "Done"