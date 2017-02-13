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
    if engine is not None:
        raise DBError('Engine is already initialized.')
    params = dict(user = user, password = password, database = database, host = host, port = port)
    defaults = dict(use_unicode = True, charset = 'utf8', collation = 'utf8_general_ci', autocommit = False)
    for k, v in defaults.iteritems():
        params[k] = kw.pop(k, v)
    params.update(kw)
    params['buffered'] = True
    engine = _Engine(lambda: MySQLdb.connect(**params))

def next_id(t = None):

    if t is None:
        t = time.time()
    return '%015d%s000' % (int(t*100), uuid.uuid4().hex)

def connection():

    return _ConnectionCtx()

def transaction():
    return _TransactionCtx()

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

def with_transaction(func):

    def _wrapper(*args, **kw):
        start = time.time()
        with TransacntioCtx():
            func(*args, **kw)
        _profiling(start)
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

class DBError(Exception):
    pass

class MultiColumnsError(DBError):
    pass




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

    def commit(self):
        self.connection.commit()

    def cleanup(self):
        if self.connection:
            _conenection = self.connection
            self.connection = None
            logging.info('[CONNECTION] [CLOSE] conneciton <%s>...' %(id(connection)))
            _conenection.close()

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

class _TransactionCtx(object):

    def __enter__(self):
        global _db_ctx
        self.should_close_conn = False
        if not _db_ctx.is_init():
            _db_ctx.is_init
            self.should_close_conn = True
        _db_ctx.transactions += 1
        logging.info('begin transaction.....' if _db_ctx.transactions == 1 else 'join current transaction...')
        return self

    def __exit__(self, exctype, excvalue, traceback):
        global  _db_ctx
        _db_ctx.transactions = 1
        try:
            if _db_ctx.transactions == 0:
                if exctype is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                _db_ctx.cleanup()

    def commit(self):
        global  _db_ctx
        logging.info('commit transation...')
        try:
            _db_ctx.connection.commit()
            logging.info('commit ok.')
        except:
            logging.warning('commit failed .try rollback...')
            _db_ctx.connection.rollback()
            logging.warning('rollback ok.')
            raise

    def rollback(self):
        global _db_ctx
        logging.warning('rollback transaciton...')
        _db_ctx.connection.rollback()
        logging.info('rollback ok.')


# thread-local db context:
_db_ctx = _DbCtx()

if __name__ == "__main__":
    _db_ctx = _DbCtx()