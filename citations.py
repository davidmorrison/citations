#!/usr/bin/python

import psycopg2
import os
import re
import tempfile
import pymarc
import urllib2
import io
import time
import json

# This tries as many as ten times to fetch a URL.  It waits a second between tries. 
def get_page(url):
    for i in range(20):
        try:
            uo = opener.open(url)
            return uo.read()
        except urllib2.URLError as e:
            print "Got " + str(e.code) + " when trying to fetch " + url
            time.sleep(1)
            pass
    return None 

def get_marc(url):
    xml = get_page(url)
    f = tempfile.NamedTemporaryFile(delete=False)
    f.write(xml)
    f.close()
    records = pymarc.parse_xml_to_array(f.name)

    os.unlink(f.name)

    return records

# This retrieves the list of records for a given experiment
def get_records(experiment):
    url = "http://inspirehep.net/search?of=xm&rg=10000&p=find+%28j+phrva+or+j+prlta+or+j+phlta%29+and+exp+" + experiment
    return get_marc(url)

def normalize_date(date):
    if re.search("-.*-", date) is None:
        if re.search("-", date) is None:
            return None
        else:
            date += "-15"
    date += " 12:00:00"
    return date

def get_recid(record):
    recid = record['001'].value()
    return recid

def get_date(record):
    try:
        date1 = record['269']['c']
        if re.search("-", date1) is None:
            date1 = None
    except:
        date1 = None

    try:
        date2 = record['961']['c']
        if re.search("-", date2) is None:
            date2 = None
    except:
        date2 = None

    try:
        date3 = record['961']['x']
        if re.search("-", date3) is None:
            date3 = None
    except:
        date3 = None

    if date1 is not None:
        return normalize_date(date1)
    if date2 is not None:
        return normalize_date(date2)
    if date3 is not None:
        return normalize_date(date3)
        
    return "2100-01-01 12:00:00"

# INSPIRES silently imposes an upper limit of 200 on the number of
# MARCXML records it returns.  Do a recid query first to find out how
# many records we expect and then keep issuing queries for partial
# results and concatenating them.
def get_refersto(record):
    recid = get_recid(record)
    url = "http://inspirehep.net/search?of=id&rg=2000&p=refersto%3Arecid%3A" + str(recid)
    recids_json = get_page(url)
    try:
        recids = json.loads(recids_json)
    except:
        recids = list()
    N = len(recids)
    records = list()
    n = 0
    while n < N:
        url = "http://inspirehep.net/search?of=xm&rg=200&p=refersto%3Arecid%3A" + str(recid) + "&jrec=" + str(n+1)
        tmp_records = get_marc(url)
        n += len(tmp_records)
        records.extend(tmp_records)
        
    return records

def insert_article(recid, date):
    try:
        cursor.execute("INSERT INTO articles(recid, date) VALUES(%s, %s)", (recid, date))
    except psycopg2.IntegrityError:
        conn.rollback()
    else:
        conn.commit()
        
def insert_citation(citer_recid, citee_recid):
    try:
        cursor.execute("INSERT INTO cites(citer, citee) VALUES(%s, %s)", (citer_recid, citee_recid))
    except psycopg2.IntegrityError:
        conn.rollback()
    else:
        conn.commit()

conn_string = "host='localhost' dbname='brahms' user='postgres' password=''"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

#cursor.execute("DROP TABLE articles")
cursor.execute("CREATE TABLE articles (recid int NOT NULL, date date, PRIMARY KEY (recid))")
#cursor.execute("DROP TABLE cites")
cursor.execute("CREATE TABLE cites (citer int, citee int, PRIMARY KEY (citer,citee))")

opener = urllib2.build_opener()

records = get_records("bnl-rhic-brahms")

n = 0
N = len(records)
M = 0
for citee in records:
    n += 1
    print "Working on " + str(n) + " of " + str(N)

    citee_recid = get_recid(citee)
    citee_date = get_date(citee)

    insert_article(citee_recid, citee_date)

    refersto_records = get_refersto(citee)
    m = len(refersto_records)
    M += m
    print "Found " + str(m) + " citers"

    for citer in refersto_records:
        citer_recid = get_recid(citer)
        citer_date = get_date(citer)

        insert_article(citer_recid, citer_date)

        insert_citation(citer_recid, citee_recid)
            
print "Found a total of " + str(M) + " citations."

conn.commit()
conn.close()



        

