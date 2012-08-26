# -*- coding: utf-8 -*-
#!/usr/bin/env python

'''
**********
The MIT License (MIT)

Copyright (c) 2008-2009 Peter Norvig

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and 
associated documentation files (the "Software"), to deal in the Software without restriction, including 
without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to 
the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
**********

pyhashseg module        #change this, it sucks

This module implements Peter Norvig's word segmenter and extends it by providing a method by which 
the corpus dependency can be updated so as to allow for segmentation of text that may be relevant to 
the current timeframe.
  
@author: Brandon Devine
@contact: brandon.devine@gmail.com
@since: 8:55:36 PM on Aug 25, 2012
'''

import re, string, random, glob, operator, heapq
from collections import defaultdict
from math import log10

class Pdist(dict):
    ''''A probability distribution estimated from counts in datafile.'''
    def __init__(self, data=[], N=None, missingfn=None):
        for key,count in data:
            self[key] = self.get(key, 0) + int(count)
        self.N = float(N or sum(self.itervalues()))
        self.missingfn = missingfn or (lambda k, N: 1./N)
    def __call__(self, key): 
        if key in self: return self[key]/self.N  
        else: return self.missingfn(key, self.N)

def datafile(name, sep='\t'):
    '''Read key,value pairs from file.'''
    for line in file(name):
        line = line.rstrip('\n')
        yield line.split(sep)
        
def avoid_long_words(key, N):
    '''Estimate the probability of an unknown word.'''
    return 10./(N * 10**len(key))
        
N = 1024908267229 ## Number of tokens

Pw  = Pdist(datafile('unigrams.txt'), N, avoid_long_words)

def splits(text, L=20):
    "Return a list of all possible (first, rem) pairs, len(first)<=L."
    return [(text[:i+1], text[i+1:]) 
            for i in range(min(len(text), L))]

def Pwords(words): 
    "The Naive Bayes probability of a sequence of words."
    return product(Pw(w) for w in words)

def product(nums):
    "Return the product of a sequence of numbers."
    return reduce(operator.mul, nums, 1)

def memo(f):
    '''Memoize function f.'''
    table = {}
    def fmemo(*args):
        if args not in table:
            table[args] = f(*args)
        return table[args]
    fmemo.memo = table
    return fmemo

@memo
def segment(text):
    "Return a list of words that is the best segmentation of text."
    if not text: return []
    candidates = ([first]+segment(rem) for first,rem in splits(text))
    return max(candidates, key=Pwords)
