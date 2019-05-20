import pika
import pika.exceptions
import yaml
import sys
import inspect
import time
import metaroot.config
import metaroot.utils


class RPCServer:
    """
    An RPC server based on pika that maps requests to methods of an object hosted by the server.
    """

    def __init__(self):
        """
        Instantiate a new RPCServer
        """
        self._handler = None
        self._logger = None
        self._connection = None
        self._channel = None
        self._config = None
        self._exit_requested = False

    @staticmethod
    def get_error_response(status: int):
        """
        Convenience method to construct an error response that contains only an integer status.

        Parameters
        ----------
        status: int
            The error status

        Returns
        -------
        dict

        """
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
            self._logger.error("The method %s is not defined on the argument object %s",
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

        Returns
        ----------
        int
            Status of the operation. If transport/encoding issues, this will be integer status of the error. Otherwise,
            it is the return status of the underlying method invocation.
        """
        # If debugging, helpful to print each message consumed
        self._logger.debug('Consumed message')
        self._logger.debug("Body: %r", body.decode())

        # result is initially success, and will either be 453 for message parsing exception, or the return result of
        # the operation that is requested by the message
        result = {"status": 0, "response": None}

        # Parse message body as YAML
        try:
            message = yaml.safe_load(body)
        except yaml.YAMLError as exc:
            self._logger.error("YAML parsing error: %s", exc)
            self._logger.error(body.decode())
            result = self.get_error_response(450)
            message = None

        # Handle special case of the CLOSE_IMMEDIATELY message that shuts down the Server
        if message == "CLOSE_IMMEDIATELY":
            ch.basic_publish(exchange='',
                             routing_key=props.reply_to,
                             properties=pika.BasicProperties(correlation_id=props.correlation_id),
                             body=yaml.safe_dump({"status": 0, "response": "SHUTDOWN_INIT"}))
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self._exit_requested = True
            self._channel.stop_consuming()
            return 0

        # Apply the handler method that maps to the request if message parsing succeeded
        if result["status"] == 0:
            result = self.call_method(self._handler, message)

        # RPC response sent to callers private queue as YAML document
        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id=props.correlation_id),
                         body=yaml.safe_dump(result))

        # Acknowledge message consumed
        ch.basic_ack(delivery_tag=method.delivery_tag)

        return result["status"]

    def shutdown(self, signum, frame):
        self._logger.warning("Shutting down...")
        try:
            self._handler.finalize()
        except Exception as e:
            self._logger.info("handler.finalize() raised an exception")
            self._logger.exception(e)

        try:
            self._channel.stop_consuming()
        except Exception as e:
            self._logger.info("channel.stop_consuming() raised an exception")
            self._logger.exception(e)

        try:
            self._connection.close()
        except Exception as e:
            self._logger.info("connection.close() raised an exception")
            self._logger.exception(e)

    def connect(self):
        try:
            # Pretty standard connection stuff (user, password, etc)
            credentials = pika.PlainCredentials(self._config.get_mq_user(), self._config.get_mq_pass())
            parameters = pika.ConnectionParameters(host=self._config.get_mq_host(),
                                                   port=self._config.get_mq_port(),
                                                   virtual_host='/',
                                                   credentials=credentials,
                                                   heartbeat=30)
            self._connection = pika.BlockingConnection(parameters)
            self._channel = self._connection.channel()

            # Only servers declare queues (not the clients)
            self._channel.queue_declare(self._config.get_mq_queue_name(),
                                        durable=True)  # request that the queue be persisted to disk

            # Only receive messages if idle
            self._channel.basic_qos(prefetch_count=1)

            # Attach the callback to handle messages
            self._channel.basic_consume(queue=self._config.get_mq_queue_name(),
                                        on_message_callback=self.consume_callback)

            return True
        except Exception as e:
            self._logger.exception(e)
            return False

    def start_consuming(self):
        """
        Start the blocking wait for messages
        """
        self._channel.start_consuming()

    def start(self, config_key):
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
        self._config = metaroot.config.get_config(config_key)
        metaroot.config.debug_config(self._config)

        # Setup our custom logging to use the class name processing the messages as its tag
        self._logger = metaroot.utils.get_logger(self.__class__.__name__,
                                                 self._config.get_log_file(),
                                                 self._config.get_file_verbosity(),
                                                 self._config.get_screen_verbosity())

        # Instantiate an instance of the class specified in the config file that will process messages
        self._handler = metaroot.utils.instantiate_object_from_class_path(self._config.get_mq_handler_class())
        self._handler.initialize()

        # We want to exit gracefully if a SIGTERM is sent, so configure a handler
        # signal.signal(signal.SIGTERM, self.shutdown)

        # Consume messages, attempting to recover from network dropout
        self._logger.info('starting consume loop for messages of type "%s"...', self._config.get_mq_queue_name())
        connect_attempts = 1
        while connect_attempts < 30 and not self._exit_requested:
            if not self.connect():
                self._logger.info("Failed to connect on attempt %d. Will try again after sleeping %d seconds",
                                  connect_attempts,
                                  connect_attempts*5)
                time.sleep(connect_attempts * 5)
                connect_attempts = connect_attempts + 1
            else:
                self._logger.info("Connected to message host %s:%d after %d attempts",
                                  self._config.get_mq_host(),
                                  self._config.get_mq_port(),
                                  connect_attempts)
                connect_attempts = 1

                try:
                    self.start_consuming()
                except KeyboardInterrupt as e:
                    self._logger.warning("Interrupted by keyboard input.")
                    break
                except (pika.exceptions.ConnectionClosed, pika.exceptions.ChannelClosed) as e:
                    self._logger.exception(e)
                    self._logger.error("Will attempt to reconnect")
                except Exception as e:
                    self._logger.exception(e)
                    self._logger.error("Will not attempt to reconnect")
                    break

        return 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.shutdown(0, None)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("USAGE: python3 -m metaroot.rpc.server <config key>")
        exit(1)

    server = RPCServer()
    server.start(sys.argv[1])
