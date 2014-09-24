'''
Created on Aug 13, 2013

@author: KhoaTran
'''
import logging

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

from lunex.cordinator import settings


logger = logging.getLogger('dao')
DBKEY = 'coordinator' 
CASSANDRA_SERVER = settings.CASSANDRA_DATABASES[DBKEY]['SERVERS']
CASSANDRA_TIMEOUT = settings.CASSANDRA_DATABASES[DBKEY]['TIMEOUT']
CASSANDRA_KEYSPACE = settings.CASSANDRA_DATABASES[DBKEY]['KEYSPACE']
AUTH = settings.CASSANDRA_DATABASES[DBKEY]['AUTH']

def __init__():
    pass

def _get_send(timeuuid, event_name, match_fields):
    cluster = Cluster(CASSANDRA_SERVER, control_connection_timeout=CASSANDRA_TIMEOUT, auth_provider=AUTH)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    
    sql = ' select * from send where timeuuid=? and event_name=? and match_fields=? '
    params = (timeuuid, event_name, match_fields)
    
    prepared = session.prepare(sql) 
    prepared.consistency_level = ConsistencyLevel.LOCAL_QUORUM
    sql = prepared.bind(params)
    rows_send = session.execute(sql, timeout=CASSANDRA_TIMEOUT)
    
    session.shutdown()
    cluster.shutdown()
    return rows_send

def _insert_alert(uuid, event_name, match_fields, alert_name, alert_url, count_threshold, status):
    cluster = Cluster(CASSANDRA_SERVER, control_connection_timeout=CASSANDRA_TIMEOUT, auth_provider=AUTH)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    query = '''
            insert into alert(timeuuid, event_name, match_fields, alert_name, alert_url, count_threshold, status)
            values (?, ?, ?, ?, ?, ?, ?);
            '''
    prepared = session.prepare(query)
    batch = BatchStatement()
    batch.add(prepared, (uuid, event_name, match_fields, alert_name, alert_url, count_threshold, status))
    session.execute(batch)
    session.shutdown()
    cluster.shutdown()
    
def _get_alert(uuid, event_name, match_fields):
    cluster = Cluster(CASSANDRA_SERVER, control_connection_timeout=CASSANDRA_TIMEOUT, auth_provider=AUTH)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    sql = ' select * from alert where timeuuid=? and event_name=? and match_fields=? '
    params = (uuid, event_name, match_fields)
    
    prepared = session.prepare(sql) 
    prepared.consistency_level = ConsistencyLevel.LOCAL_QUORUM
    sql = prepared.bind(params)
    rows_alert = session.execute(sql, timeout=CASSANDRA_TIMEOUT)
    
    session.shutdown()
    cluster.shutdown()
    return rows_alert

def _insert_send(uuid, event_name, match_fields, sender, ttl):
    cluster = Cluster(CASSANDRA_SERVER, control_connection_timeout=CASSANDRA_TIMEOUT, auth_provider=AUTH)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    query = '''
            insert into send(timeuuid, event_name, match_fields, sender, ttl)
            values (?, ?, ?, ?, ?);
            '''
    prepared = session.prepare(query)
    batch = BatchStatement()
    batch.add(prepared, (uuid, event_name, match_fields, sender, ttl))
    session.execute(batch)
    
    session.shutdown()
    cluster.shutdown()

def _update_alert(status, uuid, event_name, match_fields):
    cluster = Cluster(CASSANDRA_SERVER, control_connection_timeout=CASSANDRA_TIMEOUT, auth_provider=AUTH)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    query = ' update alert set status=? where timeuuid=? and event_name=? and match_fields=? '
    prepared = session.prepare(query)
    batch = BatchStatement()
    batch.add(prepared, (status, uuid, event_name, match_fields))
    session.execute(batch)
    
    session.shutdown()
    cluster.shutdown()
    
def _get_list_alert_by_timeuuid(timeuuid):
    cluster = Cluster(CASSANDRA_SERVER, control_connection_timeout=CASSANDRA_TIMEOUT, auth_provider=AUTH)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    sql = ' select * from alert where timeuuid=? '
    params = (timeuuid)
    
    prepared = session.prepare(sql) 
    prepared.consistency_level = ConsistencyLevel.LOCAL_QUORUM
    sql = prepared.bind(params)
    rows_alert = session.execute(sql, timeout=CASSANDRA_TIMEOUT)
    
    session.shutdown()
    cluster.shutdown()
    return rows_alert