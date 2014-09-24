'''
Created on Aug 13, 2013

@author: KhoaTran
'''
CASSANDRA_DATABASES = {
   'default': {
        'KEYSPACE': 'coordinator' ,        
        'SERVERS': ['127.0.0.1'],
        'AUTH': None, #{'username':'', 'password':''}
        'TIMEOUT': 10
    },
    'coordinator': {
        'KEYSPACE': 'coordinator' ,        
        'SERVERS': ['127.0.0.1'],
        'AUTH': None, #{'username':'', 'password':''}
        'TIMEOUT': 10
    },
}

import logging
LOGGING_LEVEL = logging.DEBUG
LOGGING_FILE = '/tmp/coordinator.log'
