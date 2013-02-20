# -*- coding: utf-8 -*-
#!/usr/bin/env python

'''
get_hashtags module

This module accesses the Twitter REST API and extracts hashtags into a file.

@author: Brandon Devine
@contact: brandon.devine@gmail.com
@since: 3:16:39 PM on Jan 8, 2013
'''

import twitter, time

twitter_api = twitter.Twitter()
myfile = time.strftime("%H:%M:%S %d %b %Y", time.localtime())+'_hashtags.txt'

def get_woeids():
    """Retrieves the set of Yahoo WOEIDs extant to the United States."""
    print 'Retrieving woeids...'
    woeids = []
    try:
        avails = twitter_api.trends.available()
        time.sleep(25)
        for entry in avails:
            if entry['countryCode'] == 'US':
                woeids.append(entry['woeid'])
    except twitter.api.TwitterHTTPError:
        pass
    print str(len(woeids))+' woeids retrieved.'
    return woeids

def get_hashtags(woeids, number):
    """Retrieves up to $number trending hashtags for each location
    represented by a WOEID."""
    print 'Retrieving hashtags...'
    hashtags = []
    hashtagcount = 0
    for woeid in woeids:
        try:
            trends = twitter_api.trends._(woeid)
            time.sleep(60)
        except twitter.api.TwitterHTTPError:
            break
        i=0
        while i< number:
            try:
                trend = trends()[0]['trends'][i]['name']
                time.sleep(25)
                if trend.startswith('#'):
                    hashtagcount += 1
                    print str(hashtagcount)+' of '+str(number*len(woeids))+\
                    ' potential hashtags found...'
                    hashtags.append(trend.lower())
            except twitter.api.TwitterHTTPError or urllib2.URLError:
                break
            i += 1
    print 'Ceasing hashtag retrieval.'
    return hashtags
    
def assemble_hashtags(hashtaglist):
    """Creates a file of unique hashtags based on input."""
    unique_hashtags = set(hashtaglist)
    print 'Creating hashtag set...'
    with open('corpora/hashtags/'+myfile, 'a+') as f:
        for entry in unique_hashtags:
            if entry.startswith('#'):   #double-check 
                print >> f, entry
    print str(len(unique_hashtags))+' unique hashtags retrieved.'

def handle_hashtags(numhashtags=5):
    """Gathers ye functions while ye may."""
    woeidlist = get_woeids()
    hashtags= get_hashtags(woeidlist, numhashtags)
    assemble_hashtags(hashtags)

def main():
    print 'Started get_hashtags.py at '+\
    time.strftime("%d %b %Y %H:%M:%S", time.localtime())
    handle_hashtags()
    print 'Done at '+\
    time.strftime("%d %b %Y %H:%M:%S", time.localtime())
    print

if __name__ == '__main__':
    main()
