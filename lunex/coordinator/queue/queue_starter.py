from decimal import Decimal
import logging
import djangoenv
import simplejson

from lunex.coordinator import service, settings
from lunex.coordinator.common import CacheService
from lunex.coordinator.queue.queue_utils import QueueUtils


logger = logging.getLogger('coordinator')
# hdlr = logging.FileHandler('D:/file_log.log')
hdlr = logging.FileHandler('/usr/local/lib/lunex-apps/coordinator/file_log.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.DEBUG)

def start_alert():
    #queue for alert and send
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

def main():
    CacheService.__init__(settings.CACHE_SERVER['Host'], settings.CACHE_SERVER['Port'])
    start_alert()
    
if __name__ == "__main__":
    main()