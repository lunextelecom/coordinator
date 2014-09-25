import redis
import simplejson
from datetime import datetime
from decimal import Decimal

caching_objects = {}
CACHE_TIME_TO_EXPIRE = 600; # 600 seconds = 5 minutes 

cache_server = None

def __init__(host, port):
    global cache_server
    cache_server = redis.StrictRedis(host=host, port=port, db=0)
    
#TODO: Need a CacheUtil to centralize caching object for all pages

class CacheObject:
    def __init__(self,key,populate,arg = None, expire = CACHE_TIME_TO_EXPIRE,changed = True):
        self.key = key;
        self.populate = populate;
        self.expire = expire;
        self.changed = changed;
        self.arg = arg;    
     
    
def add(objs_to_cache):    
    for obj in objs_to_cache:
        caching_objects[obj.key] =obj;

def add_if_not_exists(objs_to_cache):    
    for obj in objs_to_cache:
        if not caching_objects.has_key(obj.key):
            caching_objects[obj.key] =obj;
        
def notify_data_changed(*keys):
    for k in keys:
        if caching_objects.has_key(k):
            caching_objects[k].changed = True;   

def setData(key,newData):
    #obj = caching_objects[key];
    caching_objects[key].changed = True;
    caching_objects[key].arg = newData;

def setDataIfNull(key,newData):
    if None == caching_objects[key].arg:
        caching_objects[key].changed = True;
        caching_objects[key].arg = newData;
                
    #obj = caching_objects[key];

def updateData(key,newData,changed):
    #global caching_objects;
    if None == caching_objects[key].arg or changed:
        caching_objects[key].changed = True;
        caching_objects[key].arg = newData;
    else:
        obj = caching_objects[key].arg ;
        if not obj == newData:
            caching_objects[key].changed = True;
            caching_objects[key].arg = newData;
    
def getCacheObject(key):
    return caching_objects[key];

def setCacheObject(key,obj):
    caching_objects[key] = obj;

def hasKey(key):
    return caching_objects.has_key(key); 

class RedisCache(object):
    ALERT = 'coordinator:alert:'
    SEND = 'coordinator:send:'
    
    @staticmethod
    def get_cache_key(cache_key, *args, **kwarg):
        return 'coordinator:' + cache_key.format(*args, **kwarg)
    
    @staticmethod
    def set_data(key, data):
        cache_server.set(key, simplejson.dumps(data, cls = EncoderJSON))
        
    @staticmethod
    def setex_data(key, data, timeout, api_cache = False):
        cache_server.setex(key, timeout, simplejson.dumps(data, cls = EncoderJSON))
        
    @staticmethod
    def get_data(key):
        data = cache_server.get(key)
        return None if data is None else simplejson.loads(data, parse_float=Decimal)
    
    @staticmethod
    def get_data_by_key(key):
        return cache_server.get(key)
    
    @staticmethod
    def exists(key):
        return cache_server.exists(key)
    
    @staticmethod
    def get_keys(prefix):
        return cache_server.keys(prefix + '*')
    
    @staticmethod
    def get_keys_begin_with(suffix):
        return cache_server.keys('*' + suffix)
    
    @staticmethod
    def delete_by_key(key):
        cache_server.delete(key)
        
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
        