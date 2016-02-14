'''
    cleanup_hdfs_files.py
    Module that takes files in history folder in hdfs, 
    performs a literal evaluation of strings, strips html tags
    from deals description, and then resaves it as proper json.
'''
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from collections import OrderedDict

def evaluate_and_purify(msg):
    ''' Evaluate string as literal,
        if msg is a stringified python object
        return object else fail gracefully.

        :args: msg - Possible stringified python object
        :return: cleaned msg - python dict
    '''
    try:
        print "Inside Try block\n"
        msg = eval(msg.encode('utf-8'))
        if isinstance(msg, OrderedDict):
            print "Inside isinstance\n"
            return dict(msg)
        return msg
    except SyntaxError:
        print "Inside Exception\n"
        raise
if __name__ == '__main__':

    appName = 'CleanUpHDFSFiles'
    master = 'spark://ip-172-31-2-36:7077'
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    base_uri = 'hdfs://52.1.154.19:9000/exstreamly_cheap_main_files/all_deals/history'

    # Create external dataset
    distFile = sc.textFile('{}/deals_data_hdfs_all_deals_data_20160205051834.dat'.format(base_uri))
    distFile = distFile.map(evaluate_and_purify).toDF()
    distFile.printSchema()
    distFile.show()
    #distFile.pprint()
    print "Done"
