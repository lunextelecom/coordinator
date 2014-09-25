'''
Created on Aug 18, 2013

@author: KhoaTran
'''
from lunex.bottle_extra.plugins.exception import CodeMessageException

ERR_DEFAULT = (-1, 'Unknown error.')
ERR_NOT_IMPLEMENT = (-2, 'Not implement.')
ERR_OPERATION_INVALID = (-3, 'Invalid operation.')

#Business
ERR_PARAM_REQUIRED = (-201, 'Missing required parameter.')
ERR_PARAM_INVALID = (-202, 'Parameter is invalid.')
ERR_TX_FAILED = (-202, 'Transaction failed.')

ERR_TIMEUUID_NOT_SUPPORT = (-201, 'Missing required parameter.')

class BaseVNumberException(CodeMessageException):
    default_value = ERR_DEFAULT
    def __init__(self, code=None, message=None):
        self.Code = code if code else self.default_value[0]
        self.Message = message if message else self.default_value[1]

class ParameterRequired(CodeMessageException):
    def __init__(self, code=None, message=None, field_name=None):
        self.Code = code if code else ERR_PARAM_REQUIRED[0]
        self.Message = message if message else (ERR_PARAM_REQUIRED[1] + ' Field=' + str(field_name) + '.')
        self.FieldName = field_name
        CodeMessageException.__init__(self, self.Code, self.Message)
        
class ParameterInvalid(CodeMessageException):
    def __init__(self, code=None, message=None, field_name=None):
        self.Code = code if code else ERR_PARAM_INVALID[0]
        self.Message = message if message else (ERR_PARAM_INVALID[1] + ' Field=' + str(field_name) + '.')
        self.FieldName = field_name
        CodeMessageException.__init__(self, self.Code, self.Message)

class TxFailed(BaseVNumberException):
    default_value = ERR_TX_FAILED
    
class VenueNotSupport(BaseVNumberException):
    default_value = ERR_TIMEUUID_NOT_SUPPORT
        
    