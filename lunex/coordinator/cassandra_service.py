'''
Created on May 7, 2014

@author: Duc Le
'''
from datetime import datetime
import logging

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

from lunex.coordinator import settings


CASSANDRA_SESSION_DICT = {}

logger = logging.getLogger('Cassandra services')

class CassandraDbRouter(object):
    DEFAULT = 'default'
    COORDINATOR = 'coordinator'
    
def _create_session(session=None, server_name = None):
    cassandra_dbs = settings.CASSANDRA_DATABASES
    if server_name is None:
        server_name = CassandraDbRouter.DEFAULT
    cass_server = cassandra_dbs[server_name]
    
    if session:
        logger.debug('Shutdown session of server=%s' % server_name)
        session.shutdown()
    
    servers, timeout, auth_dict, keyspace = cass_server['SERVERS'], cass_server['TIMEOUT'], cass_server['AUTH'], cass_server['KEYSPACE']
    cluster = Cluster(servers, control_connection_timeout=timeout, auth_provider=auth_dict)
    session = cluster.connect(keyspace)
    logger.debug('Connected session of server=%s' % server_name)
    return session
    
def _get_session(server_name = None):
    try:
        session = None
        if not CASSANDRA_SESSION_DICT.has_key(server_name):
            CASSANDRA_SESSION_DICT[server_name] = {'Time': datetime.now(),
                                                   'Server': None}
        driver_datetime = CASSANDRA_SESSION_DICT[server_name]['Time']
        server_session = CASSANDRA_SESSION_DICT[server_name]['Server']
        if server_session is not None and (datetime.now() - driver_datetime).seconds < 60*5:
            session = server_session
            CASSANDRA_SESSION_DICT[server_name]['Time'] = datetime.now()
        else:
            session = _create_session(server_session, server_name)
            CASSANDRA_SESSION_DICT[server_name]['Server'] = session
            CASSANDRA_SESSION_DICT[server_name]['Time'] = datetime.now()
        return session
    except Exception, ex:
        logger.exception(ex)
        raise

def execute_cassandra_sql(sql_string, server_name = None, timeout=30):
    try:
        session = _get_session(server_name)
        query = SimpleStatement(sql_string, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        rows = session.execute(query, timeout=timeout)
        if rows or rows is not None:
            rows = [row for row in rows]
        return rows
    except Exception, ex:
        logger.exception(ex)
        raise
    
def execute_cassandra_prepare_sql(sql_string, params, server_name = None, timeout=30):
    try:
        session = _get_session(server_name)            
        prepared = session.prepare(sql_string) 
        prepared.consistency_level = ConsistencyLevel.LOCAL_QUORUM
        sql = prepared.bind(params)
        rows = session.execute(sql, timeout=timeout)
        if rows or rows is not None:
            rows = [row for row in rows]
        return rows
    except Exception, ex:
        logger.exception(ex)
        raise