import yaml
import os
from enum import Enum


class ConfigParams(Enum):
    """
    Named parameters used for mapping configuration file content to getters
    """
    METAROOT_MQUSER = 'METAROOT_MQUSER'
    METAROOT_MQPASS = 'METAROOT_MQPASS'
    METAROOT_MQHOST = 'METAROOT_MQHOST'
    METAROOT_MQPORT = 'METAROOT_MQPORT'
    METAROOT_MQNAME = 'METAROOT_MQNAME'
    METAROOT_MQHDLR = 'METAROOT_MQHDLR'
    METAROOT_SCREEN_VERBOSITY = 'METAROOT_SCREEN_VERBOSITY'
    METAROOT_FILE_VERBOSITY = 'METAROOT_FILE_VERBOSITY'
    METAROOT_LOG_FILE = 'METAROOT_LOG_FILE'
    METAROOT_HOOKS = 'METAROOT_HOOKS'
    METAROOT_ACTIVITY_STREAM = 'METAROOT_ACTIVITY_STREAM'
    METAROOT_DATABASE = 'METAROOT_DATABASE'


class Config:
    """
    A standardized source of configuration information that maps a dict of key=value pairs to getters
    """

    def __init__(self):
        self._data = dict()
        self._data[ConfigParams.METAROOT_LOG_FILE.value] = "metaroot.log"
        self._data[ConfigParams.METAROOT_SCREEN_VERBOSITY.value] = "INFO"
        self._data[ConfigParams.METAROOT_FILE_VERBOSITY.value] = "INFO"

    def get(self, key):
        return self._data[key]

    def has(self, key):
        return key in self._data

    def populate(self, atts: dict):
        valid_properties = set(item.value for item in ConfigParams)
        for prop in atts:
            if prop in valid_properties:
                self._data[prop] = atts[prop]

    def get_mq_user(self):
        return self._data[ConfigParams.METAROOT_MQUSER.value]

    def get_mq_pass(self):
        return self._data[ConfigParams.METAROOT_MQPASS.value]

    def get_mq_host(self):
        return self._data[ConfigParams.METAROOT_MQHOST.value]

    def get_mq_port(self):
        return int(self._data[ConfigParams.METAROOT_MQPORT.value])

    def get_mq_queue_names(self):
        return self._data[ConfigParams.METAROOT_MQNAME.value]

    def get_mq_queue_name(self):
        if isinstance(self._data[ConfigParams.METAROOT_MQNAME.value], list):
            return self._data[ConfigParams.METAROOT_MQNAME.value][0]
        else:
            return self._data[ConfigParams.METAROOT_MQNAME.value]

    def get_mq_handler_class(self):
        return self._data[ConfigParams.METAROOT_MQHDLR.value]

    def get_mq_screen_verbosity(self):
        return self._data[ConfigParams.METAROOT_SCREEN_VERBOSITY.value]

    def get_mq_file_verbosity(self):
        return self._data[ConfigParams.METAROOT_FILE_VERBOSITY.value]

    def get_log_file(self):
        return self._data[ConfigParams.METAROOT_LOG_FILE.value]

    def get_hooks(self):
        return self._data[ConfigParams.METAROOT_HOOKS.value]

    def get_activity_stream(self):
        return self._data[ConfigParams.METAROOT_ACTIVITY_STREAM.value]

    def get_activity_stream_db(self):
        return self._data[ConfigParams.METAROOT_DATABASE.value]


def get_config(key: str):
    """
    Returns environment specific information by searching a set of locations. The current implementation searches for
    METAROOT_'key' in the following:
        - The top-level keys of the global 'auto' which is loaded from local file 'metaroot[-test].yaml' if it exists
        - The attributes of django.conf.settings

    The returned configuration is created by augmenting/overwriting the parameters defined for key METAROOT_GLOBAL
    with all parameters defined for METAROOT_'key'.

    The convention used throughout the project is that class pass their class name as 'key'. E.g.
    metaroot.slurm.manager_rpc.SlurmManager calls locate_config("SlurmManager") which searches for the key
    METAROOT_SLURMMANAGER.

    Parameters
    ----------
    key: str
        The key that identifies the configuration information of interest

    Returns
    ----------
    Config
        The configuration if it is found

    Raises
    ----------
    Exception
        If no configuration could be found, or the configuration information was invalid
    """
    key = "METAROOT_" + key.upper()
    config = Config()

    # #
    #
    # Check for in-memory settings first
    #
    # #
    ready = False

    # #
    #
    # Check for config file loaded by
    #
    # #
    try:
        config.populate(auto["METAROOT_GLOBAL"])
        config.populate(auto[key])
        ready = True
        print("{0} using auto-configuration from global".format(key))
    except Exception:
        pass

    # #
    #
    # Check if running in Django
    #
    # #
    if not ready:
        try:
            from django.conf import settings
            config.populate(getattr(settings, "METAROOT_GLOBAL"))
            config.populate(getattr(settings, key))
            ready = True
            print("{0} using configuration from DJango settings".format(key))
        except Exception:
            pass

    if ready:
        return config
    else:
        raise Exception("Could not locate configuration in any standard locations")


def load_file_based_config():
    """
    Locates environment specific information by searching for file 'metaroot[-test].yaml' starting in the current
    working directory and traversing upward no more than four levels. metaroot-test.yaml takes precedence over
    metaroot.yaml if they are both located at the same level.

    Returns
    ----------
    Config
        The contents of the YAML file as an object if it was located, and None otherwise
    """
    path = ""
    ready = False
    attempts = 0

    while not ready and attempts < 4:
        try:
            # Test config takes precedence over production config
            config_file="metaroot.yaml"
            if os.path.exists(path+"metaroot-test.yaml"):
                print("Using configuration, metaroot-test.yaml. You should rename or remove this file in production")
                config_file = "metaroot-test.yaml"

            stream = open(path+config_file, 'r')
            config = yaml.safe_load(stream)
            stream.close()
            print("Loaded first configuration file found at {0}\n".format(path+config_file))
            return config
        except Exception:
            if os.path.exists(path + "../"):
                path = path + "../"
                attempts = attempts + 1
            else:
                break
    return None


def get_global_config():
    """
    Retrieves only the global parameters.

    Returns
    ----------
    Config
        The configuration if it is found

    Raises
    ----------
    Exception
        If no configuration could be found, or the configuration information was invalid
    """
    config = Config()
    config.populate(auto["METAROOT_GLOBAL"])
    return config


auto = load_file_based_config()
