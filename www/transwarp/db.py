#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
db操作模块
'''
import MySQLdb
import threading
import uuid
import logging
import functools
import time

engine = None

def create_engine(user, password, database, host = '192.168.101.130', port = 3306, **kw):

    global engine
    params = dict(user = user, password = password, database = database, host = host, port = port)
    defaults = dict(use_unicode = True, charset = 'utf8', collation = 'utf8_general_ci', autocommit = False)
    for k, v in defaults.iteritems():
        params[k] = kw.pop(k, v)
        params.update(kw)
        engine = _Engine(lambda: MySQLdb.connect(**params))

def next_id(t = None):

    if t is None:
        t = time.time()
    return '%015d%s000' % (int(t*100), uuid.uuid4().hex)

def connection():

    return _ConnectionCtx()

def _profiling(start, sql = ''):

    t = time.time() - start
    if t > 0.1:
        logging.warning('[PROFILING][DB] %s:%s' % (t, sql))
    else:
        logging.info('[PROFILING][DB] %s %s' % (t, sql))

def with_connection(func):

    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with connection():
            return func(*args, **kw)
    return _wrapper

@with_connection
def _update(sql, *args):
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL: %s, ARGS: %s' % (sql, args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        r = cursor.rowcount
        if _db_ctx.transactions == 0:
            logging.info('auto commit')
            _db_ctx.connection.commit()
        return r
    finally:
        if cursor:
            cursor.close()

class _Engine(object):

    def __init__(self, connect):
        self._connect = connect

    def connect(self):
        return self._connect



class _DbCtx(threading.local):

    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return not self.connection is None

    def init(self):
        logging.info('open lazy connection...')
        self.connection = _LasyConnection()
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup()
        self.transactions = 0

    def cursor(self):
        '''
        Return cursor
        '''
        return self.connection.cursor()

class _LasyConnection(object):

    def __init__(self):
        self.connection = None

    def cursor(self):
        if self.connection is None:
            _connection = engine.connect()
            logging.info('[CONNECTION][OPEN] connection <%s> ...' % hex(id(_connection)))
            self.connection = _connection
        return self.connection.cursor()


class _ConnectionCtx(object):

    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self


    def __exit__(self, exctype, excvale, traceback):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()


if __name__ == "__main__":
    _db_ctx = _DbCtx()