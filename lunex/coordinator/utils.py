'''
Created on Aug 13, 2013

@author: KhoaTran
'''
import logging
from sqlalchemy.schema import Column
from lunex.redis_extra.utils import RedisServer

logger = logging.getLogger('utils')

cache_server = RedisServer()

def row2dict(row):
    d = {}
    for key in row._keymap.iterkeys():
        if type(key) is Column:
            d[key.name] = row[key]
        elif type(key) is str:
            if key not in d.keys():
                d[key] = row[key]

    return d

def rows2dictlist(rows):
    result = []
    for item in rows:
        row = row2dict(item)
        result.append(row)
    rows.close()
    return result

def execute_query(engine, s):
    result = []
    try:
        conn = engine.connect()
        result = conn.execute(s)
    except Exception, ex:
        logger.exception(ex)
    
    return result
    
def to_redis_key(key, **kwargs):
    cache_key = key + '?'
    for arg in sorted(kwargs):
        cache_key += '&%s=%s' % (arg, kwargs[arg])
    return cache_key

def convert_querydict_to_dict(querydict):
        return dict([(str(key), value) for key, value in querydict.iteritems()])

class CacheKey(object):
    KEY_DID = 'did:did'
    KEY_NPANXX = 'did:npanxx'
    KEY_NPANXXINLOCAL = 'did:npanxxinlocal'
    KEY_PREFIXES = 'did:prefixes'
    KEY_DID2WAY = 'did:did2way'
    KEY_EXCHANGE = 'did:exchange'
    KEY_COUNTRY = 'vns:country'
    KEY_CITY = 'vns:city'
    KEY_PRODUCT_LIST = 'vns:product_list'
    KEY_COMPANY_INFO = 'vns:company_info'
    
def cache_getter(func, **kwargs):
    module_name = 'did:'
    cache_key = to_redis_key(module_name + func.__name__, **kwargs)
    logger.debug('[cache_getter]cache_key=%s' % (cache_key))
    data = cache_server.get_data(cache_key)
    if not data:
        rows = func(**kwargs)
        cache_server.set_data(cache_key, rows) 
        data = rows
    return data

def get_list_value_from_dict(dict_params, *args):
    rs = []
    for arg in args:
        rs.append(dict_params.get(arg, ''))
    rs.append('')
    return tuple(rs)