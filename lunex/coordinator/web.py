'''
Created on Aug 13, 2013

@author: KhoaTran
'''

import logging

import bottle
from gevent.pywsgi import WSGIServer

from lunex.bottle_extra.plugins import ExceptionPlugin
from lunex.bottle_extra.plugins import RequestLoggingPlugin
from lunex.bottle_extra.plugins import ResponsePlugin
from lunex.coordinator.service import make_alert, make_send, call_back
from lunex.coordinator.utils import convert_querydict_to_dict


app = bottle.Bottle()
app.install(ResponsePlugin())
app.install(ExceptionPlugin())
app.install(RequestLoggingPlugin())
#app.install(StatPlugin(settings.url_event))

logger = logging.getLogger('coordinator')

@app.route('/')
def index():
    return {'Message': 'Coordinator (Cassandra) worked.'}

#/alert/?alert_name={alert_name}&alert_url={alert_url}
@app.route('/alert/', method='POST')
def alert():
    '''
    Make alert of mainservice
    
    :query string alert_name: Alert Name
    :query string alert_url: Alert Url

    **Example response**:
    
    .. sourcecode:: http
    
        {
            "HasError": false,
            "Code": 0,
            "Message": ""
        }
    '''    
    logger.debug("a new alert come")
    params = convert_querydict_to_dict(bottle.request.GET)
    body_param = bottle.request.json
    params['body'] = body_param
    res = make_alert(params)
    return res

#/send/?event_name={event_name}&sender={sender}&ttl={ttl}
@app.route('/send/', method='POST')
def send():
    '''
    Make send of subsystem
    
    :query string event_name: Event name
    :query string sender: Sender
    :query int ttl: Time to live

    **Example response**:
    
    .. sourcecode:: http
    
        {
            "HasError": false,
            "Code": 0,
            "Message": ""
        }
    '''    
    logger.debug("a new send come")
    params = convert_querydict_to_dict(bottle.request.GET)
    body_param = bottle.request.json
    params['body'] = body_param
    res = make_send(params)
    return res

@app.route('/alert-callback/', method='POST')
def call_back_alert():
    data = bottle.request.json
    res = call_back(data)
    return res

def init_server():
    """
    All initialization goes here
    """
    """
    for this project
    1) redis
    """
    logger.info('init_server()')

init_server()
    
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", help="ip address")
    parser.add_argument("-p", help="port")
    args = parser.parse_args()    
    ip_addr = '0.0.0.0'
    port = 8080
    if args.i:
        ip_addr = args.i
    if args.p:
        port = int(args.p)
    logger.info('Listening on %s:%s', ip_addr, port)
    WSGIServer((ip_addr, port), app).serve_forever()