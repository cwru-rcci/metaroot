import yaml
import os
from enum import Enum


class ConfigParams(Enum):
    MQUSER = 'METAROOT_MQUSER'
    MQPASS = 'METAROOT_MQPASS'
    MQHOST = 'METAROOT_MQHOST'
    MQPORT = 'METAROOT_MQPORT'
    MQNAME = 'METAROOT_MQNAME'
    MQHDLR = 'METAROOT_MQHDLR'
    MQSVER = 'METAROOT_MQSVER'
    MQFVER = 'METAROOT_MQFVER'


class Config:

    def __init__(self):
        self._data = dict()
        self._data[ConfigParams.MQSVER.value] = "INFO"
        self._data[ConfigParams.MQFVER.value] = "INFO"

    def populate(self, atts: dict):
        valid_properties = set(item.value for item in ConfigParams)
        for prop in atts:
            if prop in valid_properties:
                self._data[prop] = atts[prop]

    def load(self, file):
        stream = open(file, 'r')
        cfg_obj = yaml.load(stream)
        stream.close()
        self.populate(cfg_obj)

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

    def get_mq_screen_verbosity(self):
        return self._data[ConfigParams.MQSVER.value]

    def get_mq_file_verbosity(self):
        return self._data[ConfigParams.MQFVER.value]


def locate_config(clazz: str):
    """
    The classes that are run via RPC are meant to be interchangeable by simply changing the import statement. To
    accomplish that we provide this utility method that subclasses of RPC Client call in their constructor to locate
    the class level configuration. E.g., this would tell a RPC client subclass where the RPC server is running,
    credentials to connect and what "channel" to publish RPC requests to.

    Parameters
    ----------
    clazz: str
        The class as a dot delimited string

    Returns
    ----------
    Config
        The configuration for this class

    Raises
    ----------
    Exception
        If no configuration could be found, or the configuration information was invalid
    """
    key = "METAROOT_"+clazz.upper()
    config = Config()

    # #
    #
    # Check for in-memory settings first
    #
    # #
    ready = False

    #

    try:
        from django.conf import settings
        config.populate(getattr(settings, key))
        ready = True
    except Exception:
        pass

    # #
    #
    # If no in-memory settings found, check on the disk. If we end up here, we will print some diagnostic information if
    # we did not find the file
    #
    # #
    if not ready:
        file_name = clazz + "RPC.yml"
        try:
            config.load(file_name)
            ready = True
        except FileNotFoundError:
            print("Could not locate RPC server config {0}/{1}".format(os.getcwd(), file_name))
        except yaml.YAMLError:
            print("Could not parse {0}/{1} as YAML".format(os.getcwd(), file_name))

    if ready:
        return config
    else:
        raise Exception("Could not locate configuration in any standard locations")
