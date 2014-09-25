'''
Created on Aug 18, 2013

@author: KhoaTran
'''
import exceptions
import json
import logging
import re
from uuid import UUID, uuid1

import requests
import simplejson

from lunex.coordinator.common.CacheService import RedisCache
from lunex.coordinator.dao import _insert_alert, _insert_send, \
    _update_alert, _delete_alert_by_id, \
    _delete_send_by_id, _test_insert_alert_name
from lunex.coordinator.models import AlertStatus
from lunex.coordinator.queue.queue_utils import QueueUtils


logger = logging.getLogger('coordinator')
queue_util = QueueUtils()

def __init__():
    pass

def do_make_alert(param):
    try:
        logger.debug("call back make alert")
        uuid = uuid1()
        alert_name = param.get('alert_name', '')
        alert_url = param.get('alert_url', '')
        body_param = param.get('body', '')
        result = {}
        count = 0
        for item in body_param:
            data = {}
            data['id'] = uuid.__str__()
            event_name = item['evtname']
            data['event_name'] = event_name
            result[event_name] = event_name
            del item['evtname']
            data['match_fields'] = item
            match_fields = simplejson.dumps(item)
            
            #check if send come before alert
            #get all send with endwith event_name from cache
            list_key = RedisCache.get_keys(RedisCache.SEND + event_name)
            
            #check to insert into alert: if send existed insert and update status
            if not list_key:
                #insert to cassandra
                _insert_alert(uuid, event_name, match_fields, alert_name, alert_url, AlertStatus.NOT_MATCH)
            else:
                flagAll = False
                for key in list_key:
                    send = RedisCache.get_data(key)
                    send_match_fields = send['match_fields']
                    flagItem = compare_two_match_fields(simplejson.loads(send_match_fields), simplejson.loads(match_fields))
                    if flagItem == True:
                        match_key = key
                        flagAll = True
                        break
                    
                if flagAll == True:
                    #insert to cassandra
                    _insert_alert(uuid, event_name, match_fields, alert_name, alert_url, AlertStatus.MATCHED)
                    count = count + 1
                    
                    send = RedisCache.get_data(match_key)
                    send_id = send['id']
                    #delete in cache
                    RedisCache.delete_by_key(RedisCache.SEND + event_name + ":" + send_id)
                    
                    #delete send in cassandra
                    _delete_send_by_id(UUID(send_id))
                else:
                    #insert to cassandra
                    _insert_alert(uuid, event_name, match_fields, alert_name, alert_url, AlertStatus.NOT_MATCH)
                    
            #add alert to cache
            RedisCache.set_data(RedisCache.ALERT + event_name + ":" + uuid.__str__(), data)
        
        #save cache count event of alert to check notify
        result['alert_name'] = alert_name
        result['alert_url'] = alert_url
        result['total'] = len(body_param)
        result['count'] = count
        RedisCache.set_data(RedisCache.ALERT + uuid.__str__(), result)
        
        if count == len(body_param):
            #fire alert_url
            fire_alert_url(alert_name, alert_url, count, body_param)
            
            #delete alert in redis cache
            keys_delete = RedisCache.get_keys_begin_with(uuid.__str__())
            for key in keys_delete:
                RedisCache.delete_by_key(key)
            
            #delete alert in cassandra
            _delete_alert_by_id(uuid)
            
    except Exception, ex:
        logger.exception(ex)

def make_alert(param):
    param['type'] = 'alert'
    #push into queue
    queue_util.put_into_queue(QueueUtils.ALERT_SERVICE, param)
    
    #return 'success'
    return {"HasError": False, "Code": 0, "Message": ""}

def do_make_send(param):
    try:
        logger.debug("call back make send")
        body_param = param.get('body', '')
        uuid = uuid1()
        event_name = param.get('event_name', '')
        sender = param.get('sender', '')
        ttl = param.get('ttl', '')
        
        #insert into send
        match_fields = simplejson.dumps(body_param)
        #insert to cassandra
        _insert_send(uuid, event_name, match_fields, sender, int(ttl))
        
        #add to cache
        result = {}
        result['id'] = uuid.__str__()
        result['event_name'] = event_name
        result['match_fields'] = match_fields
        data = RedisCache.get_data(RedisCache.SEND + event_name + ":" + uuid.__str__())
        if not data:
            if ttl:
                RedisCache.setex_data(RedisCache.SEND + event_name + ":" + uuid.__str__(), result, int(ttl))
                logger.info('cache send with timeout')
            else:
                RedisCache.set_data(RedisCache.SEND + event_name + ":" + uuid.__str__(), result)
                logger.info('cache send no time out')
        else:
            logger.info('da ton tai')
        
        #get all send with endwith event_name from cache
        list_key = RedisCache.get_keys(RedisCache.ALERT + event_name)
        
        if list_key:
            flagAll = False
            for key in list_key:
                alert = RedisCache.get_data(key)
                alert_match_fields = alert['match_fields']
                flagItem = compare_two_match_fields(simplejson.loads(match_fields), alert_match_fields)
                if flagItem == True:
                    match_key = key
                    flagAll = True
                    break
                
            #update status of alert
            if flagAll == True:
                #add count
                alert_event = RedisCache.get_data(match_key)
                if alert_event:
                    id_alert = alert_event['id']
                    #update status alert cassanra
                    _update_alert(AlertStatus.MATCHED, UUID(id_alert), event_name, simplejson.dumps(alert_event['match_fields']))
                
                    alert = RedisCache.get_data(RedisCache.ALERT + id_alert)
                    if alert:
                        count = int(alert['count']) + 1
                        alert['count'] = count
                        RedisCache.set_data(RedisCache.ALERT + id_alert, alert)
                        if count == int(alert['total']):
                            alert = RedisCache.get_data(RedisCache.ALERT + id_alert)
                            alert_name = alert['alert_name']
                            alert_url = alert['alert_url']
                            RedisCache.delete_by_key(RedisCache.ALERT + id_alert)
                            
                            #id_alert
                            #delete alert in redis cache
                            keys_delete = RedisCache.get_keys_begin_with(id_alert)
                            list_event = []
                            for key in keys_delete:
                                alert = RedisCache.get_data(key)
                                data = alert['match_fields']
                                data['evtname'] = alert['event_name']
                                list_event.append(data)
                                RedisCache.delete_by_key(key)
                            
                            #fire alert_url
                            fire_alert_url(alert_name, alert_url, count, list_event)
                            
                            #delete alert in cassandra
                            _delete_alert_by_id(UUID(id_alert))
                        RedisCache.delete_by_key(RedisCache.SEND + event_name + ":" + uuid.__str__())
                        
                        #delete send in cassandra
                        _delete_send_by_id(uuid)
    except Exception, ex:
        logger.exception(ex)
        
def make_send(param):
    body_param = param.get('body', '')
    if not body_param:
        raise exceptions.ParameterRequired(field_name='match_fields')
    
    event_name = param.get('event_name', '')
    if not event_name:
        raise exceptions.ParameterRequired(field_name='event_name')
    
    #push into queue
    param['type'] = 'send'
    queue_util.put_into_queue(QueueUtils.ALERT_SERVICE, param)
    
    #return 'success'
    return {"HasError": False, "Code": 0, "Message": ""}

#check to fire alert url in Redis
def fire_alert_url (alert_name, alert_url, count, body_param):
    try:
        params = {"alert_name":alert_name, "count": count, "evts": body_param}
        headers = {'Content-Type': 'application/json'}
        res = requests.post(alert_url, data=json.dumps(params), headers=headers)
        return res
    except Exception, ex:
        pass

def compare_two_match_fields(send_match_fields, alert_match_fields):
    flagItem = False
    for key in alert_match_fields:
        if key in send_match_fields.keys():
            alert_value = alert_match_fields[key]
            send_value = send_match_fields[key]
            if isinstance(alert_value, basestring):
                if alert_value.lower().startswith('regex'):
                    matched = re.match(alert_value[6:len(alert_value)-1], send_value)
                    if matched:
                        flagItem = True
                    else:
                        flagItem = False
                        break
                else:
                    if alert_value != send_value:
                        flagItem = False
                        break
                    else:
                        flagItem = True
            else:
                if alert_value != send_value:
                    flagItem = False
                    break
                else:
                    flagItem = True
        else:
            flagItem = False
            break
    return flagItem

def call_back(params):
    try:
        alert_name = params.get('alert_name', '')
        content = params.get('evts', '')
        _test_insert_alert_name(alert_name, simplejson.dumps(content))
        return {"HasError": False, "Code": 0, "Message": ""}
    except Exception, ex:
        logger.exception(ex)
        return {"HasError": True, "Code": 0, "Message": ""}
