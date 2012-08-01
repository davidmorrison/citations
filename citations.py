#!/usr/bin/env python

import os
import re
import tempfile
import pymarc
import urllib2
import io
import time
import json
import datetime

### DATABASE ABSTRACTION ###########################
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

from sqlalchemy import Column, Integer, Date

class Article(Base):
    __tablename__ = 'articles'
    id = Column(Integer, primary_key=True)
    recid = Column(Integer)
    date  = Column(Date)

class Cite(Base):
    __tablename__ = 'cites'
    id = Column(Integer, primary_key=True)
    citer = Column(Integer)
    citee = Column(Integer)

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class DB(object):
    def __init__(self, db_uri='sqlite:///:memory:'):
        """
        Create a database interface

        db_uri can be any backend connect string supported by sqlalchemy, see http://docs.sqlalchemy.org/en/rel_0_7/core/engines.html.
        To e.g. connect to a localhost postgres db and work with a db named brahms as user me with password passwd use a uri

            postgresql://me:passwd@localhost/brahms

        A simple solution is to use a file-based sqlite database.

            sqlite:///brahms.db

        The default creates a in-memory sqlite database. Nothing is saved after program exit.
        """

        self.engine = create_engine(db_uri, echo=False)

        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def __del__(self):
        self.session.commit()
    def create_tables(self):
        Base.metadata.create_all(self.engine)
    def drop_tables(self):
        Base.metadata.drop_all(self.engine)
    def insert_article(self, recid, date):
        y, m, d = date.split()[0].split('-')
        self.session.add(Article(recid=recid, date=datetime.date(int(y), int(m), int(d))))
    def insert_citation(self, citer, citee):
        self.session.add(Cite(citer=citer, citee=citee))

####################################################

class Citations(object):
    def __init__(self):
        self.opener = urllib2.build_opener()

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

    def process(self, records, db):  # FIXME this needs a better name
        n = 0
        N = len(records)
        M = 0
        for citee in records:
            n += 1
            print "Working on " + str(n) + " of " + str(N)

            citee_recid = self.get_recid(citee)
            citee_date = self.get_date(citee)

            db.insert_article(citee_recid, citee_date)

            refersto_records = self.get_refersto(citee)
            m = len(refersto_records)
            M += m
            print "Found " + str(m) + " citers"

            for citer in refersto_records:
                citer_recid = self.get_recid(citer)
                citer_date = self.get_date(citer)

                db.insert_article(citer_recid, citer_date)
                db.insert_citation(citer_recid, citee_recid)

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
            url = "http://inspirehep.net/search?of=xm&rg=200&p=refersto%3Arecid%3A" + str(recid) + "&jrec=" + str(n + 1)
            tmp_records = self.get_marc(url)
            n += len(tmp_records)
            records.extend(tmp_records)

        return records

if __name__ == '__main__':
    db = DB('sqlite:///brahms.db')
    #db.drop_tables()
    db.create_tables()

    cites = Citations()
    records = cites.get_records("bnl-rhic-brahms")
    cites.process(records, db)
