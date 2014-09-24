'''
Created on Aug 17, 2013

@author: KhoaTran
'''

class AlertStatus(object):
    (NOT_MATCH, MATCHED) = range(2)
    choices = [(NOT_MATCH, 'Not yet'),
               (MATCHED, 'Matched'),]
