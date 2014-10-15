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

from lunex.coordinator.common import CacheService
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
        
#         flag_alert = False
#         for item in body_param:
#             event_name = item['evtname']
#             match_fields = simplejson.dumps(item)
#              
#             #check existed in db
#             list_send_key = RedisCache.get_keys(RedisCache.ALERT + event_name)
#             for key in list_send_key:
#                 alert = RedisCache.get_data(key)
#                 alert_match_field = alert['match_fields']
#                 flag_alert = compare_two_match_fields(simplejson.loads(match_fields), simplejson.loads(alert_match_field))
#                 if flag_alert == True:
#                     break
        
        for n,i in enumerate(body_param):
            body_param[n] = simplejson.dumps(body_param[n])
         
        duplicate_list = set([x for x in body_param if body_param.count(x) > 1])
        for item in duplicate_list:
            body_param.remove(item)
        
        #if flag_alert == False:
        logger.debug('alert not existed in sytem.')
        index = 0
        for item in body_param:
            item = simplejson.loads(item)
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
            #_update_alert(AlertStatus.MATCHED, UUID(id_alert), event_name, simplejson.dumps(alert_event['match_fields']))
            

            #insert to cassandra
            _insert_alert(uuid, event_name, match_fields, alert_name, alert_url, AlertStatus.NOT_MATCH)
            data['status'] = AlertStatus.NOT_MATCH
            logger.debug("insert alert")
            
            flagAll = False
            for key in list_key:
                send = RedisCache.get_data(key)
                send_match_fields = send['match_fields']
                flagItem = compare_two_match_fields(simplejson.loads(send_match_fields), simplejson.loads(match_fields))
                if flagItem == True:
                    match_key = key
                    flagAll = True
                    
                    #insert to cassandra
                    _update_alert(AlertStatus.MATCHED, uuid, event_name, match_fields)
                    data['status'] = AlertStatus.MATCHED
                    
                    logger.debug("insert alert matched")
                    
                    
                    send = RedisCache.get_data(match_key)
                    send_id = send['id']
                    #delete in cache
                    RedisCache.delete_by_key(RedisCache.SEND + event_name + ":" + send_id)
                    
                    #delete send in cassandra
                    _delete_send_by_id(UUID(send_id))
                    
            if flagAll == True:
                count = count + 1
            
            #add alert to cache
            RedisCache.set_data(RedisCache.ALERT + event_name + ":" + str(index) + ":" + uuid.__str__(), data)
            index = index + 1
        
        #save cache count event of alert to check notify
        result['alert_name'] = alert_name
        result['alert_url'] = alert_url
        result['total'] = len(body_param)
        result['count'] = count
        RedisCache.set_data(RedisCache.ALERT + uuid.__str__(), result)
        
        if count == len(body_param):
            logger.debug("fire url when recived alert")
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
    logger.debug("into make alert")
    body_param = param.get('body', '')
    if not body_param:
        raise exceptions.ParameterRequired(field_name='match_fields')
    
    alert_name = param.get('alert_name', '')
    if not alert_name:
        raise exceptions.ParameterRequired(field_name='alert_name')
    
    alert_url = param.get('alert_url', '')
    if not alert_url:
        raise exceptions.ParameterRequired(field_name='alert_url')
    
    param['type'] = 'alert'
    #push into queue
    queue_util.put_into_queue(QueueUtils.ALERT_SERVICE, param)
    logger.debug("put new alert to queue")
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
        
        #check existed in db
#         flag_send = False
#         list_send_key = RedisCache.get_keys(RedisCache.SEND + event_name)
#         for key in list_send_key:
#             send = RedisCache.get_data(key)
#             send_match_field = send['match_fields']
#             flag_send = compare_two_match_fields(simplejson.loads(match_fields), simplejson.loads(send_match_field))
#             if flag_send == True:
#                 break;
        
        #if flag_send == False:
        logger.debug('send not existed in sytem.')
        
        #insert to cassandra
        _insert_send(uuid, event_name, match_fields, sender, int(ttl))
        logger.debug("insert send complete")
        
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
            
        #get all send with endwith event_name from cache
        list_key = RedisCache.get_keys(RedisCache.ALERT + event_name)
        
        if list_key:
            #flagAll = False
            #flagSendExistedMacth = False
            for key in list_key:
                alert = RedisCache.get_data(key)
                alert_match_fields = alert['match_fields']
                flagItem = compare_two_match_fields(simplejson.loads(match_fields), alert_match_fields)
                if flagItem == True:
                    if int(alert['status']) == AlertStatus.NOT_MATCH:
                        alert['status'] = AlertStatus.MATCHED
                        RedisCache.set_data(key, alert)
                        match_key = key
                        #flagAll = True
                        
                        #update status of alert
                        #if flagAll == True:
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
                                    logger.debug("fire alert url when recieved send")
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
                                    logger.debug('matched delete alert')
                                    _delete_alert_by_id(UUID(id_alert))
                                RedisCache.delete_by_key(RedisCache.SEND + event_name + ":" + uuid.__str__())
                                
                                #delete send in cassandra
                                _delete_send_by_id(uuid)
                    else:
                        logger.debug('existed matched will removed')
                        RedisCache.delete_by_key(RedisCache.SEND + event_name + ":" + uuid.__str__())
                        _delete_send_by_id(uuid)
                        #else:
                            #flagAll = False
                            #flagSendExistedMacth = True
                        #break
                #if flagSendExistedMacth == False:
        #else:
            #logger.debug('send already existed system')
            
    except Exception, ex:
        logger.exception(ex)
        
def make_send(param):
    logger.debug("into make send")
    body_param = param.get('body', '')
    if not body_param:
        raise exceptions.ParameterRequired(field_name='match_fields')
    
    event_name = param.get('event_name', '')
    if not event_name:
        raise exceptions.ParameterRequired(field_name='event_name')
    
    #push into queue
    param['type'] = 'send'
    queue_util.put_into_queue(QueueUtils.ALERT_SERVICE, param)
    logger.debug("put new send to queue")
    #return 'success'
    return {"HasError": False, "Code": 0, "Message": ""}

#check to fire alert url in Redis
def fire_alert_url (alert_name, alert_url, count, body_param):
    try:
        params = {"alert_name":alert_name, "count": count, "evts": body_param}
        headers = {'Content-Type': 'application/json'}
        res = requests.post(alert_url, data=json.dumps(params), headers=headers)
        return res
    except Exception:
        logger.error('fire alert_url error')

def compare_two_match_fields(send_match_fields, alert_match_fields):
    flagItem = False
    if len(alert_match_fields.keys()) != len(send_match_fields.keys()):
        return False
    
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
    
if __name__ == "__main__":
#     l = [{
#             "evtname": "pap1",
#             "txid": 1,
#             "status": "SUCCESS"
#             }, {
#             "evtname": "pap1",
#             "txid": 1,
#             "status": "SUCCESS"
#         }, {
#             "evtname": "pap3",
#             "txid": 4,
#             "status": "SUCCESS"
#         }, {
#             "evtname": "pap3",
#             "txid": 4,
#             "status": "SUCCESS"
#         }, {
#             "evtname": "pap6",
#             "txid": 4,
#             "status": "SUCCESS"
#         }]
#     
#     for n,i in enumerate(l):
#         l[n] = simplejson.dumps(l[n])
#      
#     s = set([x for x in l if l.count(x) > 1])
#     
#     
#     print s
#     for item in s:
#         l.remove(item)
#     
#     print l
#
    from lunex.coordinator import settings
    CacheService.__init__(settings.CACHE_SERVER['Host'], settings.CACHE_SERVER['Port'])
    list_send_key = RedisCache.get_keys('coor')
    for key in list_send_key:
        RedisCache.delete_by_key(key)
