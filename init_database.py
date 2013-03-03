# -*- coding: utf-8 -*-
#!/usr/bin/env python

'''
get_text_data module

This module creates a database populated with hashtags imported from the files 
created by get_text_data.py.
 
@author: Brandon Devine
@contact: brandon.devine@gmail.com
@since: 5:10 PM on Jan 8, 2013 
'''

import sqlite3, time, os, logging

log = time.strftime('./logs/'+'%H:%M:%S %d %b %Y', time.localtime())+'.log'

formatter = logging.Formatter('%(asctime)s %(message)s', '%H:%M:%S %d %b %Y')
handler = logging.FileHandler(log)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

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
            "text.original" VARCHAR(42), \
            "text.seg.basic" VARCHAR (42), \
            "score.seg.basic" INTEGER DEFAULT 0, \
            "text.seg.ext" VARCHAR(42), \
            "score.seg.ext" INTEGER DEFAULT 0)""") 
        conn.commit()
    #else re-initialize the existing database
    else:
        conn = sqlite3.connect(str(dbname))
        curs = conn.cursor()

def populate_db(path='/home/brandon/code/segmenter/corpora/tweets'):
    """Pulls hashtags out of the files created by get_text_data.py."""
    for f in os.listdir(path):
        if f.endswith('.txt'):
            curs.execute("""INSERT INTO tblHashtags (UID, 'text.original') \
            VALUES (null, ?)""",(f.replace('.txt',''),))
    conn.commit()

def main():
    logging.info('Started init_database.py at '+\
    time.strftime("%d %b %Y %H:%M:%S", time.localtime()))
    logging.info('Initializing database...')
    setup_db()
    logging.info('Populating database...')
    populate_db()
    logging.info('Done at '+time.strftime("%d %b %Y %H:%M:%S", \
                                          time.localtime()))

if __name__ == '__main__':
    main()
