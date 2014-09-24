'''
Created on Aug 18, 2013

@author: KhoaTran
'''
import exceptions
import logging
from uuid import UUID

import simplejson

from lunex.cordinator.dao import _get_send, _insert_alert, _insert_send, \
    _get_alert, _update_alert, _get_list_alert_by_timeuuid
from lunex.cordinator.models import AlertStatus
from lunex.cordinator.queue.queue_utils import QueueUtils


logger = logging.getLogger('services')
queue_util = QueueUtils()

def __init__(pos_service, ats_service, report_service, _intl_sms_message, **kwargs):
    pass

def do_make_alert(param):
    try:
        logger.debug("call back make alert")
        timeuuid = param.get('timeuuid', '')
        if not timeuuid:
            raise exceptions.ParameterRequired(field_name='timeuuid')
        uuid = UUID(timeuuid)
        alert_name = param.get('alert_name', '')
        alert_url = param.get('alert_url', '')
        count_threshold = param.get('count_threshold', '')
        
        body_param = param.get('body', '')
        for item in body_param:
            event_name = item['evtname']
            match_fields = simplejson.dumps(item)
            #select in send to compare with current alert
            rows_send = _get_send(uuid, event_name, match_fields)
            
            #insert into alert
            if rows_send:
                _insert_alert(uuid, event_name, match_fields, alert_name, alert_url, int(count_threshold), AlertStatus.MATCHED)
            else:
                _insert_alert(uuid, event_name, match_fields, alert_name, alert_url, int(count_threshold), AlertStatus.NOT_MATCH)
        
        #check notify this alert with timeuuid
        if check_notify(uuid) == True:
            #send notify
            pass
        
    except Exception, ex:
        logger.exception(ex)

def make_alert(param):
    timeuuid = param.get('timeuuid', '')
    if not timeuuid:
        raise exceptions.ParameterRequired(field_name='timeuuid')
    
    param['type'] = 'alert'
    #push into queue
    queue_util.put_into_queue(QueueUtils.ALERT_SERVICE, param)
    
    #return 'success'
    return {"HasError": False, "Code": 0, "Message": ""}

def do_make_send(param):
    try:
        logger.debug("call back make send")
        body_param = param.get('body', '')
        if not body_param:
            raise exceptions.ParameterRequired(field_name='match_fields')
        
        timeuuid = param.get('timeuuid', '')
        if not timeuuid:
            raise exceptions.ParameterRequired(field_name='timeuuid')
        uuid = UUID(timeuuid)
        
        event_name = param.get('event_name', '')
        if not event_name:
            raise exceptions.ParameterRequired(field_name='event_name')
        
        sender = param.get('sender', '')
        ttl = param.get('ttl', '')
        
        #insert into send
        match_fields = simplejson.dumps(body_param)
        _insert_send(uuid, event_name, match_fields, sender, int(ttl))
        
        #select in alert to compare with current send
        rows_alert = _get_alert(uuid, event_name, match_fields)
        
        #update status of alert
        if rows_alert:
            _update_alert(AlertStatus.MATCHED, uuid, event_name, match_fields)
        
        #check notify this alert with timeuuid
        if check_notify(uuid) == True:
            #send notify
            pass
        
    except Exception, ex:
        logger.exception(ex)
        
def make_send(param):
    body_param = param.get('body', '')
    if not body_param:
        raise exceptions.ParameterRequired(field_name='match_fields')
    
    timeuuid = param.get('timeuuid', '')
    if not timeuuid:
        raise exceptions.ParameterRequired(field_name='timeuuid')
    
    event_name = param.get('event_name', '')
    if not event_name:
        raise exceptions.ParameterRequired(field_name='event_name')
    
    #push into queue
    param['type'] = 'send'
    queue_util.put_into_queue(QueueUtils.ALERT_SERVICE, param)
    
    #return 'success'
    return {"HasError": False, "Code": 0, "Message": ""}

def check_notify(timeuuid):
    list_alert = _get_list_alert_by_timeuuid(timeuuid)
    if list_alert:
        count = 0
        for alert in list_alert:
            if alert[6] == AlertStatus.MATCHED:
                count = count + 1
        if count == len(list_alert):
            return True
    return False