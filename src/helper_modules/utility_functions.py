import sys
import os
import errno
import itertools
import time

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

def spinning_cursor(time_to_wait):
    ''' Terminal spinning cursor simulator '''
    spinner = itertools.cycle(['-', '/', '|', '\\'])
    for _ in range(time_to_wait):
        sys.stdout.write(spinner.next())
        sys.stdout.flush()
        time.sleep(1)
        sys.stdout.write('\b')
    