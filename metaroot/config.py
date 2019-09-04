import yaml
import os
from enum import Enum
from metaroot.utils import get_logger


class ConfigParams(Enum):
    """
    Named parameters used for mapping configuration file content to getters
    """
    MQUSER = 'MQUSER'
    MQPASS = 'MQPASS'
    MQHOST = 'MQHOST'
    MQPORT = 'MQPORT'
    MQNAME = 'MQNAME'
    MQHDLR = 'MQHDLR'
    SCREEN_VERBOSITY = 'SCREEN_VERBOSITY'
    FILE_VERBOSITY = 'FILE_VERBOSITY'
    LOG_FILE = 'LOG_FILE'
    HOOKS = 'HOOKS'
    ACTIVITY_STREAM_CLASS = 'ACTIVITY_STREAM_CLASS'
    ACTIVITY_STREAM_DATABASE = 'ACTIVITY_STREAM_DATABASE'
    READ_ONLY_ENABLED = 'READ_ONLY_ENABLED'
    SSL = 'SSL'
    SSL_VERIFY_MODE = 'SSL_VERIFY_MODE'
    SSL_NOCHECK_HOSTNAME = 'SSL_NOCHECK_HOSTNAME'


config_logger = None


class Config:
    """
    A standardized source of configuration information that maps a dict of key=value pairs to getters
    """

    def __init__(self):
        self._data = dict()
        self._data[ConfigParams.LOG_FILE.value] = "metaroot.log"
        self._data[ConfigParams.SCREEN_VERBOSITY.value] = "INFO"
        self._data[ConfigParams.FILE_VERBOSITY.value] = "INFO"
        self._data[ConfigParams.ACTIVITY_STREAM_CLASS.value] = "$NONE"

    def get(self, key):
        return self._data[key]

    def has(self, key):
        return key in self._data

    def populate(self, atts: dict):
        for prop in atts:
            self._data[prop] = atts[prop]

    def data(self):
        return self._data

    def get_mq_user(self):
        return self._data[ConfigParams.MQUSER.value]

    def get_mq_pass(self):
        return self._data[ConfigParams.MQPASS.value]

    def get_mq_host(self):
        return self._data[ConfigParams.MQHOST.value]

    def get_mq_port(self):
        return int(self._data[ConfigParams.MQPORT.value])

    def get_mq_queue_name(self):
        return self._data[ConfigParams.MQNAME.value]

    def get_mq_handler_class(self):
        return self._data[ConfigParams.MQHDLR.value]

    def get_screen_verbosity(self):
        return self._data[ConfigParams.SCREEN_VERBOSITY.value]

    def get_file_verbosity(self):
        return self._data[ConfigParams.FILE_VERBOSITY.value]

    def get_log_file(self):
        return self._data[ConfigParams.LOG_FILE.value]

    def get_hooks(self):
        return self._data[ConfigParams.HOOKS.value]

    def get_activity_stream(self):
        return self._data[ConfigParams.ACTIVITY_STREAM_CLASS.value]

    def get_activity_stream_db(self):
        return self._data[ConfigParams.ACTIVITY_STREAM_DATABASE.value]

    def get_read_only_enabled(self):
        return ConfigParams.READ_ONLY_ENABLED.value in self._data

    def get_ssl(self):
        return ConfigParams.SSL.value in self._data

    def get_ssl_verify_mode(self):
        return self._data[ConfigParams.SSL_VERIFY_MODE.value]

    def get_ssl_nocheck_hostname(self):
        return ConfigParams.SSL_NOCHECK_HOSTNAME.value in self._data


def debug_config(config: Config):
    for key in config.data():
        config_logger.debug("%s = %s", key, config.get(key))


def get_config(key: str):
    """
    Returns environment specific information by searching a set of locations. The current implementation searches for
    'key' in the top-level keys of the global 'auto' which is loaded from local file 'metaroot[-test].yaml'

    The returned configuration is created by augmenting/overwriting the parameters defined for key 'GLOBAL'
    with all parameters defined for 'key'.

    The convention used throughout the project is that classes pass their class name as 'key'. E.g.
    metaroot.slurm.manager_rpc.SlurmManager calls locate_config("SlurmManager") which searches for the key
    'SLURMMANAGER'.

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
    key = key.upper()
    config = Config()

    try:
        config.populate(auto["GLOBAL"])
        config.populate(auto[key])
        config_logger.info("%s using auto-configuration from global", key)
        return config
    except Exception:
        config_logger.error("Could not locate configuration for %s in any standard locations", key)
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

    Raises
    ----------
        Exception if an IO or parsing error occurs while loading the config file, or if no config file could not be found
    """
    path = ""
    envfile = os.getenv('METAROOT_CONFIG_FILE')
    config_file = None
    attempts = 0
    global config_logger

    # Precedence of test file discovery is 1) environment variable specified, 2) metaroot-test.yaml, 3) metaroot.yaml
    while attempts < 4:
        if envfile is not None and os.path.exists(envfile):
            config_file = envfile
            break
        elif os.path.exists(path+"metaroot-test.yaml"):
            config_file = path+"metaroot-test.yaml"
            break
        elif os.path.exists(path+"metaroot.yaml"):
            config_file = path+"metaroot.yaml"
            break
        elif os.path.exists(path + "../"):
            path = path + "../"
            attempts = attempts + 1

    # If no configuration file could be located
    if config_file is None:
        raise Exception("Could not locate a configuration file")

    # Load config, raises exception on error
    # print("Loading configuration file {0}".format(config_file))
    try:
        stream = open(config_file, 'r')
        config = yaml.safe_load(stream)
        stream.close()
        config = config["METAROOT"]

    except Exception as e:
        print("An IO or parsing error was encountered while loading the config file {0}".format(path+config_file))
        raise e

    # If we found a configuration file, we want to output logging statements in the requested way, so configure a
    # logger for the config class now
    wrapper = Config()
    wrapper.populate(config["GLOBAL"])
    config_logger = get_logger("CONFIG",
                               wrapper.get_log_file(),
                               wrapper.get_file_verbosity(),
                               wrapper.get_screen_verbosity())
    config_logger.info("Loaded configuration file %s", config_file)

    # Overload message queue connection parameter values with environment variable values if defined
    params = {'METAROOT_MQUSER': 'MQUSER',
              'METAROOT_MQPASS': 'MQPASS',
              'METAROOT_MQHOST': 'MQHOST',
              'METAROOT_MQPORT': 'MQPORT'}
    for key in params:
        if os.getenv(key) is not None:
            config['GLOBAL'][params[key]] = os.getenv(key)

    return config


def get_global_config():
    """
    Retrieves only the global parameters.

    Returns
    ----------
    Config
        The global configuration as a Config object

    Raises
    ----------
    Exception
        If no configuration could be found, or the configuration information was invalid
    """
    config = Config()
    config.populate(auto["GLOBAL"])
    return config


auto = load_file_based_config()
