from decimal import Decimal
import logging
import simplejson

from lunex.cordinator.queue.queue_utils import QueueUtils
from lunex.cordinator import service

logger = logging.getLogger('coordinator')
hdlr = logging.FileHandler('D:/file_log.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.DEBUG)

def start_alert():
    #queue for order post
    q = QueueUtils()
    channel = q.declare_queue(QueueUtils.ALERT_SERVICE)
    
    def callback_func(ch, method, properties, body):
        params =simplejson.loads(body, parse_float=Decimal)
        if params['type'] == 'alert':
            logger.info(" do make alert")
            service.do_make_alert(params)
        else:
            logger.info(" do make send")
            service.do_make_send(params)
            
        ch.basic_ack(delivery_tag = method.delivery_tag)
    
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback_func, queue=QueueUtils.ALERT_SERVICE)
    channel.start_consuming()

# def start_send():
#     #queue for order post
#     q = QueueUtils()
#     channel = q.declare_queue(QueueUtils.SEND_SERVICE)
#     
#     def callback_func(ch, method, properties, body):
#         logger.error('call back make send')
#         logger.info("do make send")
#         params =simplejson.loads(body, parse_float=Decimal)
#         service.do_make_send(params)
#         ch.basic_ack(delivery_tag = method.delivery_tag)
#     
#     channel.basic_qos(prefetch_count=1)
#     channel.basic_consume(callback_func, queue=QueueUtils.SEND_SERVICE)
#     channel.start_consuming()

def main():
    start_alert()
    #start_send()
    
if __name__ == "__main__":
    main()