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

import operator, sys, re

class Pdist(dict):
    ''''A probability distribution estimated from counts in a datafile.'''
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

def unkwordprob(key, N):
    '''Estimate the probability of an unknown word.'''
    return 10./(N * 10**len(key))       #seat-of-the-pants heuristic

def datafile(name, sep='\t'):
    '''Read key,value pairs from file.'''
    for line in file(name):
        line = line.rstrip('\n')
        yield line.split(sep)
        
N = 1024908267229   #number of tokens

Pw  = Pdist(datafile('unigrams.txt'), N, unkwordprob)

def Pwords(words): 
    '''The Naive Bayes probability of a sequence of words.'''   #although really, there's not much Bayesian voodoo going on
    return product(Pw(w) for w in words)        #Pw can take w as arg because of defined __call__ magic method

def product(nums):
    '''Return the product of a sequence of numbers.'''
    return reduce(operator.mul, nums, 1)        #ex: with nums = [2,3,4], (((1x2)x3)x4) = 24    

def splits(text, L=20):
    '''Return a list of all possible (first, remaining) pairs, len(first)<=L.'''
    return [(text[:i+1], text[i+1:]) 
            for i in range(min(len(text), L))]  #ex: with text = 'spark', [('s', 'park'), ('sp', 'ark'), ('spa', 'rk'), ('spar', 'k'), ('spark', '')]
    
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
    '''Return a list of words that is the best segmentation of text.'''
    if not text: return []
    candidates = ([first]+segment(remaining) for first,remaining in splits(text))
    #intercept candidates here and implement a lookup function for viability of candidates
    return max(candidates, key=Pwords)

def normalize(text):
    text = text.lower()
    text = re.sub('#', '', text)
    return text
    
def main():
    try:
        inpstr = normalize(sys.argv[1])
        segs = segment(inpstr)
        segs = ' '.join(segs)
        print segs
    except IndexError:
        print 'Please supply a string as an argument.'
    
if __name__ == "__main__":
    main()

