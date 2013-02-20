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

segext module

This module implements Peter Norvig's word segmenter and extends it by providing 
a method by which the corpus dependency can be updated so as to allow for 
segmentation of text that may be relevant to the current timeframe.
  
@author: Brandon Devine
@contact: brandon.devine@gmail.com
@since: 8:55:36 PM on Aug 25, 2012
'''

import argparse, operator, re, sqlite3, os, sys, time
from collections import defaultdict

class Pdist(dict):
    ''''A probability distribution estimated from counts in a datafile.'''
    def __init__(self, data=[], N=1024908267229, unkfn=None):
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
    return 10./(N * 10**len(key))       #seat-of-the-pants heuristic
    
def get_datafile(name, sep='\t'):
    '''Reads key,value pairs from file.'''
    for line in file(name):
        line = line.rstrip('\n')
        yield line.split(sep)
    
def get_Pwords(words): 
    '''The Naive Bayes probability of a sequence of words.'''   #although really, there's not much Bayesian voodoo going on
    return get_product(Pw(w) for w in words)        #Pw can take w as arg because of defined __call__ magic method

def get_product(nums):
    '''Returns the product of a sequence of numbers.'''
    return reduce(operator.mul, nums, 1)        #ex: with nums = [2,3,4], (((1x2)x3)x4) = 24    

def get_splits(text, L=20):
    '''Returns a list of all possible (first, remaining) pairs, \
    len(first)<=L.'''
    return [(text[:i+1], text[i+1:]) 
            for i in range(min(len(text), L))]  #ex: with text = 'spark', [('s', 'park'), ('sp', 'ark'), ('spa', 'rk'), ('spar', 'k'), ('spark', '')]
    
def memo(f):
    '''Memoizes function f.'''
    table = {}
    def fmemo(*args):
        if args not in table:
            table[args] = f(*args)
        return table[args]
    fmemo.memo = table
    return fmemo

@memo
def get_segs(text):
    '''Returns one of a list of words that is the best segmentation of text.'''
    if not text: return []
    candidates = ([first]+get_segs(remaining) for \
                  first,remaining in get_splits(text))
    return max(candidates, key=get_Pwords)  #look candidate probs up after all candidates are found

def normalize(text):
    '''Normalizes (hashtag) text by removing hashes and setting to lowercase.'''
    text = text.lower()
    text = re.sub('#', '', text)
    return text

def set_segs(data):
    '''Handles data coming in as different formats and outputs as needed.'''
    try:
        conn = sqlite3.connect('hashtags.db')
        curs = conn.cursor()
        curs.execute('SELECT * FROM tblHashtags WHERE "text.original" = ?',\
                     (unicode(data),))
        for row in curs:
            uid = row[0]
            inp = row[2]
            ninp = normalize(inp)
            segs = get_segs(ninp)
            data = ' '.join(segs)
            curs.execute('UPDATE tblHashtags SET "text.seg.ext" = ? WHERE \
            "UID" = ?', (data, uid))
            conn.commit()
    except:
        inp = data
        ninp = normalize(inp)
        segs = get_segs(ninp)
        data = ' '.join(segs)
    return data

def get_corpus_counts(corpus):
    """Translates the given corpus into a dictionary-based frequency 
    distribution to return the total count of tokens in the corpus"""
    freqdist = defaultdict(int)
    with open(corpus, 'r') as f:
        for l in f.readlines():
            freqdist[l.strip().split('\t')[0]] = \
            int(l.strip().split('\t')[1])
    return sum(freqdist.itervalues())

p = argparse.ArgumentParser(description="segext.py")
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
            print 'Please ensure that hashtags.db is in the current directory.'
            sys.exit(1)
        else:
            conn = sqlite3.connect('hashtags.db')
            curs = conn.cursor()
            curs.execute('SELECT * FROM tblHashtags')
            rows = curs.fetchall()
            for r in rows:  
                yield (r[2])

def main():
    print 'Started segext.py at '+\
    time.strftime("%d %b %Y %H:%M:%S", time.localtime())
    for line in read_input(args):
        try:
            pathname = os.path.abspath('')
            print 'Input: ', line
            N = get_corpus_counts(pathname+'/corpora/tweets/'+str(line)+'.txt')
            print 'Corpus size: ',N
            global Pw
            Pw  = Pdist(get_datafile(pathname+'/corpora/tweets/'+str(line)+'.txt'),\
                        N, get_unk_word_prob)
            output = set_segs(line)    
            print "Output: ", output
        except IOError:
            pass
    print 'Done at '+ time.strftime("%d %b %Y %H:%M:%S", time.localtime())
    print
        
if __name__ == '__main__':
    main()