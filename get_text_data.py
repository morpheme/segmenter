# -*- coding: utf-8 -*-
#!/usr/bin/env python

'''
get_text_data module

This module creates a database populated with hashtags imported from the file 
created by get_hashtags.py and builds a gold standard frequency distribution of
terms from Peter Norvig's modified unigrams.txt. It then filters the Twitter 
stream for each hashtag, and if a sufficient number of tweets containing it are
found, those tweets are collected, cleaned, and compared to the gold standard 
in order to create a hashtag-specific frequency distribution.
 
@author: Brandon Devine
@contact: brandon.devine@gmail.com
@since: 5:10 PM on Jan 8, 2013
'''

import sqlite3, time, os, re, glob, requests, json, copy
from collections import defaultdict

# authenticate with the Topsy API
requests.get('http://otter.topsy.com', auth=('$name', '$key'))

def setup_db(dbname='hashtags.db'):
    """Creates a database for hashtags, their segmentations,
    and their respective scores."""
    pathname = os.path.abspath('')  
    global conn 
    global curs
    #if there is no database set up in script directory yet:
    if os.path.exists(pathname+'/'+dbname) == False:
        conn = sqlite3.connect(str(dbname))
        curs = conn.cursor()    
        curs.execute("""CREATE TABLE tblHashtags (UID INTEGER PRIMARY KEY, \
            "instudy" INTEGER DEFAULT 0, \
            "text_original" VARCHAR(42), \
            "text_seg_basic" VARCHAR (42), \
            "score_seg_basic" INTEGER DEFAULT 0, \
            "text_seg_ext" VARCHAR(42), \
            "score_seg_ext" INTEGER DEFAULT 0)""") 
        conn.commit()
    #else re-initialize the existing database
    else:
        conn = sqlite3.connect(str(dbname))
        curs = conn.cursor()

def retrieve_hashtags(path='/home/brandon/code/segmenter/corpora/hashtags'):
    """Pulls hashtags out of the file created by get_hashtags.py."""
    hashtags = []
    for hfile in glob.glob(path+'/*.txt'):
        with open(hfile, 'r') as f:
            for hashtag in f.readlines():
                print hashtag, type(hashtag)
                hashtags.append(hashtag.strip())
                curs.execute("""INSERT INTO tblHashtags (UID, text_original) \
                VALUES (null, ?)""",(hashtag.strip(),))
    conn.commit()
    return hashtags

def retrieve_text(hashtag, numtweets=500):      #changing numtweets necessitates changing tweetpayload params below
    """
    Uses Topsy (because they're cool) API to search for tweets containing a 
    given hashtag. If said hashtag occurs often enough, the tweet text is 
    grabbed, sent to the cleaners, and added to a raw corpus that will inform 
    a hashtag-specific frequency distribution.
    """
    countpayload = {'q': hashtag}
    r = requests.get("http://otter.topsy.com/searchcount.json", \
                     params=countpayload)
    info = json.loads(json.dumps(r.json, sort_keys=True, indent=4))
    ttl = []    #total term list of all text in numtweets tweets with hashtag
    if info['response']['h'] < numtweets:
        print 'Not enough values!'
        return ttl
    else:
        i = 1
        while i <= 5:
            print 'Getting page '+str(i)+'...'
            tweetpayload = {'q': hashtag, 'allow_lang':'en', 'window':'h23', \
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
                print 'Tweet added to term list...'
            i += 1
        print 'Returning term list...'
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
    with open('corpora/tweets/'+myfile+'.txt', 'w+') as f:
        for k,v in sorted(new.items(), key=lambda tup: tup[0]):
            print >> f, k+'\t'+str(v)
    
def main():
    print 'Started main() at '+\
    time.strftime("%d %b %Y %H:%M:%S", time.localtime())
    print 'Initializing database...'
    setup_db()
    print 'Retrieving hashtags and populating database...'
    hashtaglist = retrieve_hashtags()
    print 'Building gold standard corpus...'
    unigrams = get_unigram_corpus()
    print 'Building hashtag corpora...'
    print
    for hashtag in hashtaglist:
        print 'Retrieving text for '+str(hashtag)+'...'
        termlist = retrieve_text(hashtag)
        if len(termlist) == 0:
            print 'Hashtag corpus discarded due to lack of data.'
        else:
            print 'Making corpus for '+str(hashtag)+' beginning at '\
            +time.strftime("%d %b %Y %H:%M:%S", time.localtime())
            make_hashtag_corpus(hashtag,termlist,unigrams)
        print
    print 'Done at '+time.strftime("%d %b %Y %H:%M:%S", time.localtime())

if __name__ == '__main__':
    main()
