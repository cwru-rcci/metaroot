import pika
import yaml
import sys
import inspect
import signal
import metaroot.config
import metaroot.utils


class Consumer:
    """
    Consumes AMQP messages and maps them to method calls of a configured "handler" class. This is for event based
    workflows where no response is expected by the sender.
    """

    def __init__(self):
        self._handler = None
        self._logger = None
        self._connection = None
        self._channel = None

    @staticmethod
    def get_error_response(status: int):
        return {"status": status, "response": None}

    def call_method(self, obj: object, message: dict):
        """
        Calls a method of an object

        Parameters
        ----------
        obj: object
            An object to invoke a method of
        message: dict
            Parameters specifying the method to invoke and arguments to pass

        Returns
        ----------
        dict
            key status is 0 for success, and >0 on error
            key response is response from method call, or None is server is returning internal error
        """
        # Validate that the message defines an 'action' attribute which maps to a method name
        if 'action' not in message:
            self._logger.error("The message does not define an 'action' -> %s", message)
            return self.get_error_response(450)

        # Lookup the requested method in the handler object
        try:
            method = getattr(obj, message['action'])
        except AttributeError:
            self._logger.error("The method %q is not defined on the argument object %s",
                               message['action'], type(obj).__name__)
            return self.get_error_response(451)

        # Validate arguments match the method signature
        arguments = inspect.signature(method).parameters
        args = []
        for argument in arguments:
            if argument not in message:
                self._logger.error("Call to method %s.%s%s, no parameter %r in message", type(obj).__name__,
                                   message['action'], inspect.signature(method), argument)
                return self.get_error_response(452)
            args.append(message[argument])

        # Call the method, returning its Result. This is wrapped by a try/except so that exception raise by method
        # calls do not cause the server to stop
        try:
            return method(*args).to_transport_format()
        except Exception as e:
            self._logger.exception(e)
            return self.get_error_response(455)

    def consume_callback(self, ch, method, props, body):
        """
        Method called when a response is received to a previous request

        Parameters
        ----------
        ch:
            Connection/Channel to send response
        method:
            Unused
        props:
            Unused
        body: bytearray
            Response to request
        """
        # If debugging, helpful to print each message consumed
        self._logger.debug('Consumed message')
        self._logger.debug("Body: %r", body.decode())

        # Parse message body to object, setting result to fail if the body cannot be decoded as YAML
        try:
            message = yaml.safe_load(body)
        except yaml.YAMLError as exc:
            self._logger.error("YAML parsing error: %s", exc)
            self._logger.error(body.decode())
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Handle special case of the CLOSE_IMMEDIATELY message that shuts down the Consumer
        if message == "CLOSE_IMMEDIATELY":
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self._channel.stop_consuming()
            return

        # Apply the handler method that maps to the request if message parsing succeeded
        self.call_method(self._handler, message)

        # Acknowledge message consumed
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def shutdown(self, signum, frame):
        self._logger.warning("Shutting down...")
        try:
            self._channel.stop_consuming()
        except Exception:
            self._logger.info("channel.stop_consuming() raised an exception")

        try:
            self._connection.close()
        except Exception:
            self._logger.info("connection.close() raised an exception")

    def start(self, config_key: str):
        """
        Calls a method of an object

        Parameters
        ----------
        config_key: str
            Key identifying where to find options in the global configuration dict for this consumer

        Returns
        ----------
        int
            Returns 1 on exit
        """
        config = metaroot.config.get_config(config_key)

        # Setup our custom logging to use the class name processing the messages as its tag
        self._logger = metaroot.utils.get_logger(config.get_mq_handler_class(),
                                                 config.get_log_file(),
                                                 config.get_mq_file_verbosity(),
                                                 config.get_mq_screen_verbosity())

        # Pretty standard connection stuff (user, password, etc)
        credentials = pika.PlainCredentials(config.get_mq_user(), config.get_mq_pass())
        parameters = pika.ConnectionParameters(host=config.get_mq_host(),
                                               port=config.get_mq_port(),
                                               virtual_host='/',
                                               credentials=credentials,
                                               heartbeat=30)
        self._connection = pika.BlockingConnection(parameters)
        self._channel = self._connection.channel()

        # Consumers declare the channel on startup.
        self._channel.queue_declare(config.get_mq_queue_name(),
                                    durable=True)  # request that the queue be persisted to disk
        self._logger.info("declared queue %s, Durable=True", config.get_mq_queue_name())

        # Only receive messages if idle
        self._channel.basic_qos(prefetch_count=1)

        # Instantiate an instance of the class specified in the config file that will process messages
        self._handler = metaroot.utils.instantiate_object_from_class_path(config.get_mq_handler_class())
        self._logger.info("instantiated handler %s", config.get_mq_handler_class())

        # Attach the callback to handle messages
        self._channel.basic_consume(self.consume_callback,
                                    queue=config.get_mq_queue_name())

        # We want to exit gracefully if a SIGTERM is sent, so configure a handler
        signal.signal(signal.SIGTERM, self.shutdown)

        # Consume messages, attempting to exit gracefully
        try:
            self._logger.info('starting consume loop for messages of type "%s"...', config.get_mq_queue_name())
            self._channel.start_consuming()
        except KeyboardInterrupt as e:
            self._logger.warning("Interrupted by keyboard input.")
        except IOError as e:
            if e.errno == 9:
                self._logger.warning("Bad file descriptor. This is expected if the process was sent a SIGTERM, but " +
                                     "could otherwise be indicative of a network problem")
            else:
                self._logger.exception(e)
        except Exception as e:
            self._logger.exception(e)
        self.shutdown(1, None)

        return 1


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("USAGE: python3 -m metaroot.event.consumer <path to config file>")
        exit(1)

    server = Consumer()
    server.start(sys.argv[1])
