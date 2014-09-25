'''
Created on Aug 13, 2013

@author: KhoaTran
'''
CASSANDRA_DATABASES = {
#    'default': {
#         'KEYSPACE': 'coordinator' ,        
#         'SERVERS': ['127.0.0.1'],
#         'AUTH': None, #{'username':'', 'password':''}
#         'TIMEOUT': 10
#     },
#     'coordinator': {
#         'KEYSPACE': 'coordinator' ,        
#         'SERVERS': ['127.0.0.1'],
#         'AUTH': None, #{'username':'', 'password':''}
#         'TIMEOUT': 10
#     },
    'default': {
        'KEYSPACE': 'coordinator' ,        
        'SERVERS': ['192.168.93.38'],
        'AUTH': None, #{'username':'', 'password':''}
        'TIMEOUT': 10
    },
    'coordinator': {
        'KEYSPACE': 'coordinator' ,        
        'SERVERS': ['192.168.93.38'],
        'AUTH': None, #{'username':'', 'password':''}
        'TIMEOUT': 10
    },
}

CACHE_SERVER = {'Host': '10.9.9.61',
                'Port': 6379
                }

import logging
LOGGING_LEVEL = logging.DEBUG
LOGGING_FILE = '/tmp/coordinator.log'
