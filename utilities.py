# -*- coding: utf-8 -*-
#!/usr/bin/env python

'''
utilities module

This module contains a few housekeeping functions.
  
@author: Brandon Devine
@contact: brandon.devine@gmail.com
@since: 8:42:57 PM on Dec 15, 2012
'''
import os, operator, signal, errno, time
from functools import wraps

def read_api_key(keyfile):
    '''Reads API key from file. '''
    with open(keyfile, 'r') as f:
        return f.readline().strip()
    
def datafile(name, sep='\t'):
    '''Read key,value pairs from file.'''
    for line in file(name):
        line = line.rstrip('\n')
        yield line.split(sep)    

def read_login_file():
    pathname = os.path.abspath('')
    try:
        if os.path.exists(pathname+'/login.txt'):
            pathname = pathname+'/login.txt'
            f = open(pathname)
            return f.readline().strip(), f.readline().strip()
            f.close()
    except:
        print 'The file login.txt was not found.  Please refer to twitterinfo.py.'
        
class TimeoutError(Exception):
    pass

def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def handletimeout(signum, frame):
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, handletimeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return decorator

def get_timing(func):
    def wrapper(*arg):
        t1 = time.time()
        res = func(*arg)
        t2 = time.time()
        print '%s took %0.3f ms' % (func.func_name, (t2-t1)*1000.0)
        return res
    return wrapper

def product(nums):
    '''Return the product of a sequence of numbers.'''
    return reduce(operator.mul, nums, 1)        #ex: with nums = [2,3,4], (((1x2)x3)x4) = 24