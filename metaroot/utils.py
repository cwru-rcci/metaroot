import sys
import logging
import inspect
from importlib import import_module


loggers = {}


def get_log_level(level: str):
    levels = {"DEBUG": logging.DEBUG,
              "INFO": logging.INFO,
              "WARN": logging.WARN,
              "ERROR": logging.ERROR,
              "CRITICAL": logging.CRITICAL,
              "FATAL": logging.FATAL}
    return levels[level]


def get_logger(name: str, file_level="INFO", screen_level="INFO"):
    """
    Creates/caches loggers based on argument name

    Parameters
    ----------
    name: str
        The name of the requested logger. Usually the name of the class.
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

        fh = logging.FileHandler("metaroot.log")
        fh.setLevel(get_log_level(file_level))
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        loggers[name] = logger
        return logger


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
    print("from metaroot.rpc.client import RPCCLient")
    print("from metaroot.rpc.config import locate_config")
    print("from metaroot.common import Result\n\n\nclass {0}(RPCClient):".format(class_name))
    print("    def __init__(self, config_file='client-config.yml'):")
    print("        super().__init__(locate_config(self.__class__.__name__))")
    print("")

    methods = inspect.getmembers(obj, inspect.ismethod)
    for method_spec in methods:
        method_name = method_spec[0]

        # Skip private methods
        if method_name.startswith("__"):
            continue

        print("    def {0}(self".format(method_name), end='')

        # Lookup the requested method in the handler object
        try:
            method = getattr(obj, method_name)
        except AttributeError:
            print("The method {0} is not defined on the argument object {1}".format(method_name, type(obj).__name__))
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
        print("        return self.call(request)")
        print("")
    print("")
    print("")


if __name__ == "__main__":
    create_rpc_wrapper(sys.argv[1])
