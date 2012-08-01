#!/usr/bin/env python

import psycopg2
import os
import re
import tempfile
import pymarc
import urllib2
import io
import time
import json

class Citations(object):
  def __init__(self):
    self.conn_string = "host='localhost' dbname='brahms' user='postgres' password=''"
    self.conn = psycopg2.connect(self.conn_string)
    self.cursor = self.conn.cursor()
    self.opener = urllib2.build_opener()

  def __del__(self):
    self.conn.commit()
    self.conn.close()

  def create_tables(self):
    self.cursor.execute("CREATE TABLE articles (recid int NOT NULL, date date, PRIMARY KEY (recid))")
    self.cursor.execute("CREATE TABLE cites (citer int, citee int, PRIMARY KEY (citer,citee))")

  def drop_tables(self):
    self.cursor.execute("DROP TABLE articles")
    self.cursor.execute("DROP TABLE cites")

  def get_records(self, experiment):
    """ This retrieves the list of records for a given experiment """
    url = "http://inspirehep.net/search?of=xm&rg=10000&p=find+%28j+phrva+or+j+prlta+or+j+phlta%29+and+exp+" + experiment
    return self.get_marc(url)

  def get_marc(self, url):
      xml = self.get_page(url)
      f = tempfile.NamedTemporaryFile(delete=False)
      f.write(xml)
      f.close()
      records = pymarc.parse_xml_to_array(f.name)

      os.unlink(f.name)

      return records

  def get_page(self, url):
      """ This tries as many as ten times to fetch a URL.  It waits a second between tries. """
      for i in range(20):
          try:
              uo = self.opener.open(url)
              return uo.read()
          except urllib2.URLError as e:
              print "Got " + str(e.code) + " when trying to fetch " + url
              time.sleep(1)
              pass
      return None 

  def process(self, records): # FIXME this needs a better name
    n = 0
    N = len(records)
    M = 0
    for citee in records:
        n += 1
        print "Working on " + str(n) + " of " + str(N)

        citee_recid = self.get_recid(citee)
        citee_date = self.get_date(citee)

        self.insert_article(citee_recid, citee_date)

        refersto_records = self.get_refersto(citee)
        m = len(refersto_records)
        M += m
        print "Found " + str(m) + " citers"

        for citer in refersto_records:
            citer_recid = self.get_recid(citer)
            citer_date = self.get_date(citer)

            self.insert_article(citer_recid, citer_date)

            self.insert_citation(citer_recid, citee_recid)
                
    print "Found a total of " + str(M) + " citations."

  def normalize_date(self, date):
      if re.search("-.*-", date) is None:
          if re.search("-", date) is None:
              return None
          else:
              date += "-15"
      date += " 12:00:00"
      return date

  def get_recid(self, record):
      recid = record['001'].value()
      return recid

  def get_date(self, record):
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
          return self.normalize_date(date1)
      if date2 is not None:
          return self.normalize_date(date2)
      if date3 is not None:
          return self.normalize_date(date3)
          
      return "2100-01-01 12:00:00"

  def get_refersto(self, record):
      """
      INSPIRES silently imposes an upper limit of 200 on the number of
      MARCXML records it returns.  Do a recid query first to find out how
      many records we expect and then keep issuing queries for partial
      results and concatenating them.
      """
      recid = self.get_recid(record)
      url = "http://inspirehep.net/search?of=id&rg=2000&p=refersto%3Arecid%3A" + str(recid)
      recids_json = self.get_page(url)
      try:
          recids = json.loads(recids_json)
      except:
          recids = list()
      N = len(recids)
      records = list()
      n = 0
      while n < N:
          url = "http://inspirehep.net/search?of=xm&rg=200&p=refersto%3Arecid%3A" + str(recid) + "&jrec=" + str(n+1)
          tmp_records = self.get_marc(url)
          n += len(tmp_records)
          records.extend(tmp_records)
          
      return records

  def insert_article(self, recid, date):
      try:
          self.cursor.execute("INSERT INTO articles(recid, date) VALUES(%s, %s)", (recid, date))
      except psycopg2.IntegrityError:
          self.conn.rollback()
      else:
          self.conn.commit()
        
  def insert_citation(self, citer_recid, citee_recid):
      try:
          self.cursor.execute("INSERT INTO cites(citer, citee) VALUES(%s, %s)", (citer_recid, citee_recid))
      except psycopg2.IntegrityError:
          self.conn.rollback()
      else:
          self.conn.commit()

cites = Citations()
#cites.drop_tables()
cites.create_tables()

records = cites.get_records("bnl-rhic-brahms")
cites.process(records)
