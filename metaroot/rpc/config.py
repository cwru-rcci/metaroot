import yaml
from enum import Enum


class ConfigParams(Enum):
    MQUSER = 'mquser'
    MQPASS = 'mqpass'
    MQHOST = 'mqhost'
    MQPORT = 'mqport'
    MQNAME = 'mqname'
    MQHDLR = 'mqhdlr'


class Config:
    _data = dict()

    def load(self, file):
        stream = open(file, 'r')
        self._data = yaml.load(stream)
        stream.close()

    def get(self, key):
        return self._data[key]

    def get_mq_user(self):
        return self._data[ConfigParams.MQUSER.value]

    def get_mq_pass(self):
        return self._data[ConfigParams.MQPASS.value]

    def get_mq_host(self):
        return self._data[ConfigParams.MQHOST.value]

    def get_mq_port(self):
        return int(self._data[ConfigParams.MQPORT.value])

    def get_mq_queue_names(self):
        return self._data[ConfigParams.MQNAME.value]

    def get_mq_queue_name(self):
        if isinstance(self._data[ConfigParams.MQNAME.value], list):
            return self._data[ConfigParams.MQNAME.value][0]
        else:
            return self._data[ConfigParams.MQNAME.value]

    def get_mq_handler_class(self):
        return self._data[ConfigParams.MQHDLR.value]
