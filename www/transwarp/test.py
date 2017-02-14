#!/usr/bin/env python
# -*- coding: utf-8 -*-
import  MySQLdb
import logging
engine = None
def create_engine():
    global engine
    kw = {}
    params = dict(user = 'practice', passwd = 'practice', host = '192.168.101.130', port = 3306, db = 'practiceDB')
    defaults = dict(use_unicode = True, charset = 'utf8', autocommit = False )
    for k, v in defaults.iteritems():
        params[k] = kw.pop(k, v)
    params.update(kw)

    engine = _Engine( MySQLdb.connect(**params))

class _Engine(object):

    def __init__(self, connect):
        self._connection = connect

    def connect(self):
        return self._connection


create_engine()

db = engine.connect()
cursor = db.cursor()

results = cursor.execute('SELECT VERSION()')
print results

