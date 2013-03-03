# -*- coding: utf-8 -*-
#!/usr/bin/env python

'''
**********
The MIT License (MIT)

Copyright (c) 2008-2009 Peter Norvig

Permission is hereby granted, free of charge, to any person obtaining a copy of 
this software and associated documentation files (the "Software"), to deal in 
the Software without restriction, including without limitation the rights to 
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies 
of the Software, and to permit persons to whom the Software is furnished to do 
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all 
copies or substantial portions of the Software.
**********

segbase module

This module implements Peter Norvig's word segmenter and extends it slightly by 
allowing for various input methods. This file is meant to be run from the 
command line.
  
@author: Brandon Devine
@contact: brandon.devine@gmail.com
@since: 8:55:36 PM on Nov 25, 2012
'''

import argparse, operator, sys, os, re, sqlite3, time, logging

log = time.strftime('./logs/'+'%H:%M:%S %d %b %Y', time.localtime())+'.log'

formatter = logging.Formatter('%(asctime)s %(message)s', '%H:%M:%S %d %b %Y')
handler = logging.FileHandler(log)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

class Pdist(dict):
    '''A probability distribution estimated from counts in a datafile.'''
    def __init__(self, data=[], N=None, unkfn=None):
        for key,count in data:
            self[key] = self.get(key, 0) + int(count)   #since this is being populated en masse, all vals are initially 0; hence the + int(count)
        self.N = float(N or sum(self.itervalues()))     #if N is not supplied, back off to the sum of all values in the dict instance. 
        self.unkfn = unkfn or (lambda key, N: 1./N)   #if unkfn is not supplied, back off to a simple estimation of an unknown word
    #an already-created instance of Pdist, when called, executes __call__. 
    #the instance is callable like a function (meaning that Pw below can take args). 
    def __call__(self, key):    
        if key in self: return self[key]/self.N     #returns the simple MLE if the calling key is in the instance's dict  
        else: return self.unkfn(key, self.N)    #if not, back off to whatever we decided the unknown estimation method is

def get_unk_word_prob(key, N):
    '''Estimates the probability of an unknown word.'''
    return 10./(N * 10**len(key))       #seat-of-the-pants heuristic

def get_datafile(name, sep='\t'):
    '''Reads key,value pairs from a file.'''
    for line in file(name):
        line = line.rstrip('\n')
        yield line.split(sep)
        
N = 1024908267229   

Pw  = Pdist(get_datafile('corpora/unigrams.txt'), N, get_unk_word_prob)

def get_Pwords(words): 
    '''Returns the Naive Bayes probability of a sequence of words.'''   #although really, there's not much Bayesian voodoo going on
    return get_product(Pw(w) for w in words)        #Pw can take w as arg because of defined __call__ magic method

def get_product(nums):
    '''Returns the product of a sequence of numbers.'''
    return reduce(operator.mul, nums, 1)        #ex: with nums = [2,3,4], (((1x2)x3)x4) = 24    

def get_splits(text, L=20):
    '''Returns a list of all possible (first, remaining) pairs, \
    len(first)<=L.'''
    return [(text[:i+1], text[i+1:]) 
            for i in range(min(len(text), L))]  #ex: with text = 'spark', [('s', 'park'), ('sp', 'ark'), ('spa', 'rk'), ('spar', 'k'), ('spark', '')]
    
def memoize(f):
    '''Memoizes function f.'''
    table = {}
    def fmemo(*args):
        if args not in table:
            table[args] = f(*args)
        return table[args]
    fmemo.memo = table
    return fmemo

@memoize
def get_segs(text):
    '''Returns one of a list of words that is the best segmentation of text.'''
    if not text: return []
    candidates = ([first]+get_segs(remaining) for \
                  first,remaining in get_splits(text))
    return max(candidates, key=get_Pwords)

def normalize(text):
    '''Normalizes (hashtag) text by removing hashes and setting to lowercase.'''
    text = text.lower()
    text = re.sub('#', '', text)
    return text

def set_segs(data):
    '''Handles data coming in as different formats and outputs as needed.'''
    conn = sqlite3.connect('hashtags.db')
    curs = conn.cursor()
    #handles strings only from -s and -f
    if type(data) is str:
        data = normalize(data)
        segs = get_segs(data)
        segs = ' '.join(segs)
        return segs
    #handles database entries
    else:
        uid = data[0]
        inp = data[1]
        ninp = normalize(inp)
        segs = get_segs(ninp)
        segs = ' '.join(segs)
        curs.execute('UPDATE tblHashtags SET "text.seg.basic" = ? \
        WHERE "UID" = ?', (segs, uid))
        conn.commit()
        return segs

p = argparse.ArgumentParser(description="segbase.py")
p.add_argument("-s", "--string")
p.add_argument("-f", "--infile")

args = p.parse_args()

def read_input(args):
    '''Reads data source from CLI.'''
    if args.string:
        yield args.string
    elif args.infile:
        with open(args.infile,'r') as f:
            for line in f:
                yield line.rstrip("\n")
    else:
        pathname = os.path.abspath('')
        if os.path.exists(pathname+'/hashtags.db') == False:
            logging.info('Please ensure that hashtags.db is in the \
                current directory.')
            sys.exit(1)
        else:
            conn = sqlite3.connect('hashtags.db')
            curs = conn.cursor()
            curs.execute('SELECT * FROM tblHashtags WHERE \
                "text.seg.basic" IS NULL')
            rows = curs.fetchall()
            for r in rows:
                yield (r[0],str(r[2]))

def main():
    logging.info('Started segbase.py at '+\
    time.strftime("%d %b %Y %H:%M:%S", time.localtime()))
    logging.info('Corpus size: %s', N)
    for line in read_input(args):
        inp = line[1]
        logging.info('Input: %s', inp)
        output = set_segs(line)  
        logging.info('Output: %s', output)
    logging.info('Done at '+ time.strftime("%d %b %Y %H:%M:%S", \
                                           time.localtime()))
        
if __name__ == '__main__':
    main()

