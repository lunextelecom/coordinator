'''
Created on Aug 13, 2013

@author: KhoaTran
'''
from datetime import datetime
import logging

from lunex.coordinator import settings
from lunex.coordinator.cassandra_service import execute_cassandra_sql, \
    CassandraDbRouter


logger = logging.getLogger('dao')
DBKEY = 'coordinator' 
CASSANDRA_SERVER = settings.CASSANDRA_DATABASES[DBKEY]['SERVERS']
CASSANDRA_TIMEOUT = settings.CASSANDRA_DATABASES[DBKEY]['TIMEOUT']
CASSANDRA_KEYSPACE = settings.CASSANDRA_DATABASES[DBKEY]['KEYSPACE']
AUTH = settings.CASSANDRA_DATABASES[DBKEY]['AUTH']

def __init__():
    pass

def get_send_by_evtname(event_name):
    pass

def _insert_alert(uuid, event_name, match_fields, alert_name, alert_url, status):
    sql = '''
        insert into alert(id, event_name, match_fields, alert_name, alert_url, status)
        values ({id}, '{event_name}', '{match_fields}', '{alert_name}', '{alert_url}', {status});
        '''
    sql = sql.format(id=uuid.__str__(),
                         event_name=event_name,
                         match_fields=match_fields,
                         alert_name=alert_name,
                         alert_url=alert_url,
                         status=status)
    execute_cassandra_sql(sql, server_name=CassandraDbRouter.COORDINATOR)

def _insert_send(uuid, event_name, match_fields, sender, ttl):
    sql = '''
            insert into send(id, event_name, match_fields, sender, ttl)
            values ({id}, '{event_name}', '{match_fields}', '{sender}', {ttl});
            '''
    sql = sql.format(id=uuid,
                         event_name=event_name,
                         match_fields=match_fields,
                         sender=sender,
                         ttl=ttl)
    execute_cassandra_sql(sql, server_name=CassandraDbRouter.COORDINATOR)

def _update_alert(status, uuid, event_name, match_fields):
    sql = ''' update alert set status={status} where id={id} and event_name='{event_name}' and match_fields='{match_fields}' '''
    sql = sql.format(status=status,id=uuid,
                         event_name=event_name,
                         match_fields=match_fields)
    execute_cassandra_sql(sql, server_name=CassandraDbRouter.COORDINATOR)
    
def _delete_alert_by_id(uuid):
    sql = ' delete from alert where id={id}'
    sql = sql.format(id=uuid)
    execute_cassandra_sql(sql, server_name=CassandraDbRouter.COORDINATOR)
    
def _delete_send_by_id(uuid):
    sql = ' delete from send where id={id}'
    sql = sql.format(id=uuid)
    execute_cassandra_sql(sql, server_name=CassandraDbRouter.COORDINATOR)
    
def _test_insert_alert_name(alert_name, content):
    sql = '''
            insert into alert_name(name, content, create_date)
            values ('{alert_name}', '{content}', '{create_date}');
            '''
    sql = sql.format(alert_name=alert_name,
                         content=content,
                         create_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    execute_cassandra_sql(sql, server_name=CassandraDbRouter.COORDINATOR)