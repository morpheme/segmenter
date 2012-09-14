# -*- coding: utf-8 -*-
#!/usr/bin/env python

'''
get_hashtags module

This module crawls the Twitter stream at the default 'Spritzer' access level and extracts hashtags
into a database.

A file named login.txt must be located in the same directory as this module to access the Twitter
stream.  This file holds the user's Twitter login information, in the following format:
    
username
password

  
@author: Brandon Devine
@contact: brandon.devine@gmail.com
@since: 5:10 PM on Sept 4, 2012
'''

# Taken from Matthew Russell's recipes.  Deals with OAuth.
from twitter__login import login  

# A specialized library just for accessing the streaming API
import tweetstream  

# Everything else
import os, sqlite3, time

# t is an authenticated (OAuth) Twitter object once twitter__login.py has had the 
# requisite information entered and login() has been called at least once.  This library
# is used for accessing RTs and anything non-streaming that calls against the API limits.
t = login()
    
def dbsetup(dbname='hashtags.db'):
    pathname = os.path.abspath('')  
    global conn 
    global curs
    #if there is no database set up in script directory yet:
    if os.path.exists(pathname+'/'+dbname) == False:
        #initialize
        conn = sqlite3.connect(str(dbname))
        curs = conn.cursor()    
        #set up empty table.  NB: sqlite places no limits on length of VARCHAR field
        curs.execute("""CREATE TABLE tblHashtags (UID INTEGER PRIMARY KEY, \
			"instudy" INTEGER DEFAULT 0, \
			"text.original" VARCHAR(42), \
			"text.seg.basic" VARCHAR (42), \
			"score.seg.basic.1" INTEGER DEFAULT 0, \
			"score.seg.basic.2" INTEGER DEFAULT 0, \
			"text.seg.ext" VARCHAR(42), \
			"score.seg.ext.1" INTEGER DEFAULT 0, \
			"score.seg.ext.2" INTEGER DEFAULT 0, \
			
			)""") 
        #save
        conn.commit()
    #else re-initialize the existing database
    else:
        conn = sqlite3.connect(str(dbname))
        curs = conn.cursor()    

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
        
(username, password) = read_login_file()

def retrieve_hashtags(numhashtags=100):
    stream = tweetstream.SampleStream(username, password)  
    try:
        count = 0
        while count < numhashtags:       
            for tweet in stream:
                # a check is needed on text as some "tweets" are actually just API operations
                # the language selection doesn't really work but it's better than nothing(?)
                if tweet.get('text') and tweet['user']['lang'] == 'en':
                    if tweet['entities']['hashtags']['text']:
                        ht = tweet['entities']['hashtags']['text']
                        curs.execute("""INSERT INTO tblHashtags VALUES (null,?)""",(ht))
                        conn.commit()
                        count += 1
                        break
    except tweetstream.ConnectionError, e:
        print 'Disconnected from Twitter at '+time.strftime("%d %b %Y %H:%M:%S", time.localtime()) \
        +'.  Reason: ', e.reason

    
def main():
    dbsetup()
    retrieve_hashtags()

if __name__ == '__main__':
    main()
