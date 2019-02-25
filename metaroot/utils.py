import sys
import logging
import inspect
from importlib import import_module


loggers = {}


def get_logger(name: str, logfile="messages.log"):
    """
    Creates/caches loggers based on argument name

    Parameters
    ----------
    name: str
        The name of the requested logger. Usually the name of the class.
    logfile: str
        The name of the file to log to

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
        ch.setLevel(logging.CRITICAL)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        fh = logging.FileHandler(logfile)
        fh.setLevel(logging.DEBUG)
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
    print("import metaroot.rpc.client\nfrom metaroot.common import Result\n\n\nclass {0}(metaroot.rpc.client.RPCClient):".format(class_name))
    print("    def __init__(self, config_file='client-config.yml'):")
    print("        super().__init__(config_file)")
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
