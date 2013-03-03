# -*- coding: utf-8 -*-
#!/usr/bin/env python

'''
get_text_data module

This module imports the file created by get_hashtags.py and builds a gold 
standard frequency distribution of terms from Peter Norvig's modified 
unigrams.txt. It then filters the Twitter stream for each hashtag, and if a 
sufficient number of tweets containing it are found, those tweets are collected,
cleaned, and compared to the gold standard in order to create a hashtag-specific 
frequency distribution.
 
@author: Brandon Devine
@contact: brandon.devine@gmail.com
@since: 5:10 PM on Jan 8, 2013
'''

import time, re, glob, requests, json, copy, logging
from collections import defaultdict
from utilities import read_api_key

log = time.strftime('./logs/'+'%H:%M:%S %d %b %Y', time.localtime())+'.log'

formatter = logging.Formatter('%(asctime)s %(message)s', '%H:%M:%S %d %b %Y')
handler = logging.FileHandler(log)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

def retrieve_hashtags(path='/home/brandon/code/segmenter/corpora/hashtags'):
    """Pulls hashtags out of the file(s) created by get_hashtags.py."""
    hashtags = []
    for hfile in glob.glob(path+'/*.txt'):
        with open(hfile, 'r') as f:
            for hashtag in f.readlines():
                hashtags.append(hashtag.strip())
    return hashtags

def retrieve_text(hashtag, numtweets=500):      #changing numtweets necessitates changing tweetpayload params below
    """
    Uses Topsy (because they're cool) API to search for tweets containing a 
    given hashtag. If said hashtag occurs often enough, the tweet text is 
    grabbed, sent to the cleaners, and added to a raw corpus that will inform 
    a hashtag-specific frequency distribution.
    """
    payload = {'apikey': read_api_key('./topsyapikey.txt'),'q': hashtag}
    r = requests.get("http://otter.topsy.com/searchcount.json", \
                     params=payload)
    info = json.loads(json.dumps(r.json, sort_keys=True, indent=4))
    ttl = []    #total term list of all text in numtweets tweets with hashtag
    if info['response']['h'] < numtweets:
        return ttl
    else:
        i = 1
        while i <= 5:
            try:
                #print 'Getting page '+str(i)+'...'
                tweetpayload = {'apikey': read_api_key('./topsyapikey.txt'), \
                                'q': hashtag, 'allow_lang':'en', 'window':'h23', \
                                'page':i, 'perpage':10}
                r = requests.get("http://otter.topsy.com/search.json", \
                                 params=tweetpayload)
                tweets = json.loads(json.dumps(r.json, sort_keys=True, indent=4))\
                ['response']['list']  
                for tweet in tweets:
                    text = json.loads(json.dumps(tweet, sort_keys=True, indent=4))\
                    ['content']
                    text = clean_text(text)
                    ttl.extend(text.split())
                    #print 'Tweet added to term list...'
            except KeyError:
                pass
            i += 1
        logging.info('Returning term list...')
        return ttl

def clean_text(text):
    """Normalizes text to lowercase and removes usernames, links, extraneous
    characters, hashtags, and stopwords."""
    #fix links and strip extraneous characters
    text = re.sub(r'http://t.co/[\w]+', ' ', text)     #Twitter converts all links to its t.co domain  
    text = re.sub(r'[\\(){}?!-",;.:/\]\[]', ' ', text)
    text = re.sub('\xe2\x80\x9c|\xe2\x80\x9d', '', text)   #lame left and right double-quotes
    #delete usernames
    text = re.sub('\s*@(\w+)\s*',r' ',text)
    #standardize to lower
    text = text.lower()
    #remove "stopwords"
    p = re.compile('(#(\S*)|rt|\.\.\.)')   #'...' is appended to overlong tweets
    text = p.sub('',text)
    #get rid of crufty whitespace
    text = ' '.join(text.split())
    return text

def get_unigram_corpus():
    """Translates the existing unigrams.txt corpus into dictionary-based
    frequency distribution."""
    freqdist_unigrams = defaultdict(int)
    with open('corpora/unigrams.txt', 'r') as f:
        for l in f.readlines():
            freqdist_unigrams[l.strip().split('\t')[0]] = \
            int(l.strip().split('\t')[1])
    return freqdist_unigrams
    
def make_hashtag_corpus(hashtag,termlist,freqdist_unigrams):
    """Produces a hashtag-centric variant of unigrams.txt that weights the
    terms associated with that hashtag."""
    #make a frequency distribution of terms associated with the supplied hashtag 
    freqdist_hashtag = defaultdict(int)
    for term in termlist: freqdist_hashtag[term] += 1
    
    ratio = \
    sum(freqdist_unigrams.itervalues())/sum(freqdist_hashtag.itervalues())  #normalizing factor
    new = copy.deepcopy(freqdist_unigrams)    #updated dict that combines pertinent k,v from hashtags with the majority of unigrams
    
    for k in freqdist_hashtag.keys():
        #for tokens common to the unigrams and hashtag corpora:
        #if the count of a token from unigrams.txt is lower than the normalized count
        #of that token in the hashtag corpus, update new with normalized hashtag count
        if k in new.keys() and \
        new[k] < int(round(ratio*freqdist_hashtag.get(k))):
            new[k] = int(round(ratio*freqdist_hashtag.get(k)))
        #for tokens in the new hashtag corpus that don't exist in the unigrams corpus,
        #assign the normalized value from the hashtag corpus
        else:
            new[k] = int(round(ratio*freqdist_hashtag.get(k)))
    #tokens found in unigrams but not hashtags already exist in new at their proper ratio. 
    #make the new corpus text file from the accumulated tokens and their counts in new
    myfile = str(hashtag)+'.txt'
    with open('corpora/tweets/'+myfile, 'w+') as f:
        for k,v in sorted(new.items(), key=lambda tup: tup[0]):
            try:
                print >> f, k+'\t'+str(v)
            except UnicodeEncodeError:
                pass
    
def main():
    logging.info('Started get_text_data.py at '+\
    time.strftime("%d %b %Y %H:%M:%S", time.localtime()))
    logging.info('Retrieving hashtags and populating database...')
    hashtaglist = retrieve_hashtags()
    #hashtaglist = ['#SAMPLEHASHTAG']    #use for one-off tests
    logging.info('Building gold standard corpus...')
    unigrams = get_unigram_corpus()
    logging.info('Building hashtag corpora...')
    for hashtag in hashtaglist:
        logging.info('Retrieving text for '+str(hashtag)+' ...')
        termlist = retrieve_text(hashtag)
        if len(termlist) == 0:
            logging.info('Hashtag corpus discarded due to lack of data.')
        else:
            logging.info('Making corpus for '+str(hashtag)+' beginning at '\
            +time.strftime("%d %b %Y %H:%M:%S", time.localtime()))
            make_hashtag_corpus(hashtag,termlist,unigrams)
    logging.info('Done at '+time.strftime("%d %b %Y %H:%M:%S", \
                                          time.localtime()))

if __name__ == '__main__':
    main()
