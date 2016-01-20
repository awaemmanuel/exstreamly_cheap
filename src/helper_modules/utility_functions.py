import sys
import os
import errno

def print_out(str):
    ''' Print to Screen and flush buffer '''
    print str
    sys.stdout.flush()
    
def mkdir_if_not_exist(path='/tmp/exstreamly_cheap_files'):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
    return path

    