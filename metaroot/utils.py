import sys
import logging
import inspect
import datetime
import pika
from importlib import import_module
import metaroot.config

loggers = {}


def get_log_level(level: str):
    """
    Maps a string log level to one of the logging.LEVEL constants so that log levels can be specified by string tokens
    in config files or variables

    Parameters
    ----------
    level: str
        Log level string corresponding to a logging.LEVEL constant, E.g., level=INFO maps to logging.INFO

    Returns
    ----------
    int
        Numeric value of logging level

    Raises
    ----------
    KeyError
        If the argument level string does not correspond to a logging level
    """
    levels = {"DEBUG": logging.DEBUG,
              "INFO": logging.INFO,
              "WARN": logging.WARN,
              "ERROR": logging.ERROR,
              "CRITICAL": logging.CRITICAL,
              "FATAL": logging.FATAL}
    return levels[level]


def get_logger(name: str, file_path: str, file_level="INFO", screen_level="INFO"):
    """
    Creates a singleton logger for each unique name that is passed

    Parameters
    ----------
    name: str
        The name of the requested logger. Usually the name of the class.
    file_path: str
        The path to the log file that will be written
    file_level: str
        The logging verbosity level for messages written to the log file
    screen_level: str
        The logging verbosity level for messages written to the terminal

    Returns
    ----------
    logger
        A logger configured to log messages with the argument name
    """
    if name in loggers:
        return loggers[name]
    else:
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        ch = logging.StreamHandler()
        ch.setLevel(get_log_level(screen_level))
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        if file_path != "$NONE":
            fh = logging.FileHandler(file_path)
            fh.setLevel(get_log_level(file_level))
            fh.setFormatter(formatter)
            logger.addHandler(fh)

        loggers[name] = logger
        return logger


def delete_queue(queue_name: str):
    """
    Deletes a queue from the message queue server

    Parameters
    ----------
    queue_name: str
        The name of the queue to delete

    Returns
    ----------
    int
        Returns 0 on success

    Raises
    ----------
    Exception
        If the underlying operations raise an exception
    """
    config = metaroot.config.get_global_config()

    # Pretty standard connection stuff (user, password, etc)
    credentials = pika.PlainCredentials(config.get_mq_user(), config.get_mq_pass())
    parameters = pika.ConnectionParameters(host=config.get_mq_host(),
                                           port=config.get_mq_port(),
                                           virtual_host='/',
                                           credentials=credentials,
                                           heartbeat=30)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_delete(queue=queue_name)
    connection.close()
    return 0


def create_queue(queue_name: str):
    """
    Creates a durable queue on the message queue server

    Parameters
    ----------
    queue_name: str
        The name of the queue to delete

    Returns
    ----------
    int
        Returns 0 on success

    Raises
    ----------
    Exception
        If the underlying operations raise an exception
    """
    config = metaroot.config.get_global_config()

    # Pretty standard connection stuff
    credentials = pika.PlainCredentials(config.get_mq_user(), config.get_mq_pass())
    parameters = pika.ConnectionParameters(host=config.get_mq_host(),
                                           port=config.get_mq_port(),
                                           virtual_host='/',
                                           credentials=credentials,
                                           heartbeat=30)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue_name,
                          durable=True)  # request that the queue be persisted to disk
    connection.close()
    return 0


def instantiate_object_from_class_path(path: str):
    """
    Instantiates an instance of a class specified as a string

    Parameters
    ----------
    path: str
        The path/name of the class as a dot delimited string, e.g. io.stream.Decoder

    Returns
    ----------
    object
         An instance of the specified class

    Raises
    ----------
    Exception
        Will raise an exception if the class is invalid, cannot be imported or cannot be instantiated
    """
    module_path, _, class_name = path.rpartition(".")
    mod = import_module(module_path)
    clazz = getattr(mod, class_name)
    return clazz()


def create_rpc_wrapper(clazz):
    """
    Uses reflection to enumerate public methods of an object and writes to STDOUT an version of the code that will
    run in our RPC infrastructure.

    Parameters
    ----------
    clazz: str
        The class name/path that will be processed
    """
    # Import the class
    obj = instantiate_object_from_class_path(clazz)

    # Get class name
    _, _, class_name = clazz.rpartition(".")

    # Output headers
    dt = datetime.datetime.now()
    print("from metaroot.rpc.client import RPCClient")
    print("from metaroot.config import get_config")
    print("from metaroot.common import Result")
    print("")
    print("")
    print("class {0}(RPCClient):".format(class_name))
    print("    \"\"\"")
    print("    RPC wrapper for {0}".format(clazz))
    print("    Auto-generated {0}".format(dt.isoformat()))
    print("    \"\"\"")
    print("    def __init__(self):")
    print("        super().__init__(get_config(self.__class__.__name__))")
    print("")

    methods = inspect.getmembers(obj, inspect.ismethod)
    for method_spec in methods:
        method_name = method_spec[0]

        # Skip private methods
        if method_name.startswith("__"):
            continue

        print("    \"\"\"")
        print("    An RPC wrapper to the method {0}.{1}".format(clazz, method_name))
        print("")
        print("    See Also")
        print("    --------")
        print("    {0}.{1}".format(clazz, method_name))
        print("    \"\"\"")
        print("    def {0}(self".format(method_name), end='')

        # Lookup the requested method in the handler object
        try:
            method = getattr(obj, method_name)
        except AttributeError:
            print("The method {0} is not defined on the argument class {1}".format(method_name, type(obj).__name__))
            exit(1)

        # Validate arguments match the method signature
        arguments = inspect.signature(method).parameters
        for argument_name in arguments:
            print(", {0}".format(argument_name), end='')

        print(") -> Result:")
        print("        request = {{'action': '{0}',".format(method_name))
        for argument_name in arguments:
            print("                   '{0}': {1},".format(argument_name, argument_name))
        print("                   }")
        print("        return self.send(request)")
        print("")
    print("")
    print("")


def create_producer_wrapper(clazz):
    """
    Uses reflection to enumerate public methods of an object and writes to STDOUT an version of the code that will
    run in our Producer/Consumer infrastructure.

    Parameters
    ----------
    clazz: str
        The class name/path that will be processed
    """
    # Import the class
    obj = instantiate_object_from_class_path(clazz)

    # Get class name
    _, _, class_name = clazz.rpartition(".")

    # Output headers
    dt = datetime.datetime.now()
    print("from metaroot.event.producer import Producer")
    print("from metaroot.config import get_config")
    print("from metaroot.common import Result")
    print("")
    print("")
    print("class {0}(Producer):".format(class_name))
    print("    \"\"\"")
    print("    Producer wrapper for {0}".format(clazz))
    print("    Auto-generated {0}".format(dt.isoformat()))
    print("    \"\"\"")
    print("    def __init__(self):")
    print("        super().__init__(get_config(self.__class__.__name__))")
    print("")

    methods = inspect.getmembers(obj, inspect.ismethod)
    for method_spec in methods:
        method_name = method_spec[0]

        # Skip private methods
        if method_name.startswith("__"):
            continue

        print("    \"\"\"")
        print("    A Producer wrapper to the method {0}.{1}".format(clazz, method_name))
        print("")
        print("    See Also")
        print("    --------")
        print("    {0}.{1}".format(clazz, method_name))
        print("    \"\"\"")
        print("    def {0}(self".format(method_name), end='')

        # Lookup the requested method in the handler object
        try:
            method = getattr(obj, method_name)
        except AttributeError:
            print("The method {0} is not defined on the argument class {1}".format(method_name, type(obj).__name__))
            exit(1)

        # Validate arguments match the method signature
        arguments = inspect.signature(method).parameters
        for argument_name in arguments:
            print(", {0}".format(argument_name), end='')

        print(") -> Result:")
        print("        request = {{'action': '{0}',".format(method_name))
        for argument_name in arguments:
            print("                   '{0}': {1},".format(argument_name, argument_name))
        print("                   }")
        print("        return self.send(request)")
        print("")
    print("")
    print("")


if __name__ == "__main__":
    if sys.argv[1] == "RPC":
        create_rpc_wrapper(sys.argv[2])
    elif sys.argv[1] == "EVENT":
        create_producer_wrapper(sys.argv[2])
    else:
        print("USAGE: utils.py <RPC|EVENT> <class name>")
