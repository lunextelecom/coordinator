'''
Created on Mar 18, 2013

@author: KhoaTran
'''
import pika
import simplejson
from decimal import Decimal
from datetime import datetime


class QueueUtils():
    ALERT_SERVICE = 'ALERT_SERVICE'
    SEND_SERVICE = 'SEND_SERVICE'
    
    def __init__(self):
        self.connection = None
    
    def declare_queue(self, queue_name):
        if self.connection is None:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = self.connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        return channel
     
    def put_into_queue(self, queue_name, params):
        message = simplejson.dumps(params, cls = EncoderJSON)
        channel = self.declare_queue(queue_name)
        channel.basic_publish(exchange='',
                              routing_key=queue_name,
                              body=message,
                              properties=pika.BasicProperties(delivery_mode = 2)
                             )
    def stop_put_into_queue(self):
        QueueUtils.connection.close()
            
            
class EncoderJSON(simplejson.JSONEncoder):
    def default(self, obj):
        """
        default method is used if there is an unexpected object type
        in our example obj argument will be Decimal('120.50') and datetime
        in this encoder we are converting all Decimal to float and datetime to str
        """
        if isinstance(obj, datetime):
            obj = str(obj)
        elif isinstance(obj, Decimal):
            obj = float(obj)
        else:
            obj = super(EncoderJSON, self).default(obj)
        return obj