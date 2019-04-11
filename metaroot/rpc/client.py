#!/usr/bin/env python
import pika
import pika.exceptions
import uuid
import yaml
import time
from metaroot.api.result import Result
from metaroot.config import Config
from metaroot.utils import get_logger


class RPCClient:
    """
    An RPC client based on pika that passes YAML messages
    """

    def __init__(self, config: Config):
        """
        Initialize a new RPC Client for use.

        Parameters
        ----------
        config: Config
            Connection properties for the RPC server

        Raises
        ----------
        Exception
            If any underlying operations fail by raising an exception

        """
        self.config = config
        self.connection = None
        self.channel = None
        self.callback_queue = None
        self.corr_id = None
        self.response = None
        self.queue = self.config.get_mq_queue_name()
        self.logger = get_logger(RPCClient.__name__,
                                 config.get_log_file(),
                                 config.get_mq_file_verbosity(),
                                 config.get_mq_screen_verbosity())
        self.connect()

    def __del__(self):
        """
        Attempt to shutdown cleanly by closing pika connection
        """
        self.close()

    def _connection_blocked_cb(self, frame):
        """
        Callback to log cases where the server began rejecting messages
        """
        self.logger.WARN("The connection has been blocked by the server")

    def _connection_unblocked_cb(self, frame):
        """
        Callback to log cases where the server resumed accepting messages after a period of rejecting them
        """
        self.logger.WARN("The connection has been unblocked")

    def connect(self) -> bool:
        """
        Connect the RPC client to the message queue

        Returns
        ----------
        bool
            True if connection was successful, False otherwise
        """
        try:
            # Pretty standard connection stuff
            credentials = pika.PlainCredentials(self.config.get_mq_user(), self.config.get_mq_pass())
            parameters = pika.ConnectionParameters(host=self.config.get_mq_host(),
                                                   port=self.config.get_mq_port(),
                                                   virtual_host='/',
                                                   credentials=credentials,
                                                   heartbeat=30)
            self.connection = pika.BlockingConnection(parameters)
            self.connection.add_on_connection_blocked_callback(self._connection_blocked_cb)
            self.connection.add_on_connection_unblocked_callback(self._connection_unblocked_cb)
            self.channel = self.connection.channel()

            # Declare a delete-on-exit queue for this client to receive RPC callback message
            qd_result = self.channel.queue_declare("", exclusive=True)
            self.callback_queue = qd_result.method.queue

            # Specify the function to process the RPC callback responses
            self.channel.basic_consume(queue=self.callback_queue,
                                       on_message_callback=self.on_response,
                                       auto_ack=True)

            # Set properties to track call/response to
            self.corr_id = None
            self.response = None

            # Success
            return True

        except Exception as e:
            self.logger.error("connection attempt threw exception")
            self.logger.exception(e)

            # Failure
            return False

    def on_response(self, ch, method, props, body):
        """
        Method called when a response is received to a previous request

        Parameters
        ----------
        ch:
            Unused
        method:
            Unused
        props:
            Properties of the response
        body: str
            Response to request

        Raises
        ----------
        Exception
            If any underlying operations fail by raising an exception
        """
        if self.corr_id == props.correlation_id:
            self.response = body

    def close(self):
        """
        Shutdown the RPC Client
        """
        try:
            if not self.connection.is_closed:
                self.connection.close()
        except Exception as e:
            self.logger.warn("closing connection raised an exception")

    def send(self, obj) -> Result:
        """
        Method to initiate an RPC request

        Parameters
        ----------
        obj: dict
            A dictionary specifying a remote method name and arguments to invoke

        Returns
        ----------
        Result
            Result.status is 0 for success, >0 on error
            Result.response is any object returned by the remote method invocation or None
        """
        # Encode the request dict as YAML
        try:
            message = yaml.safe_dump(obj)
        except yaml.YAMLError as exc:
            self.logger.error("YAML serialization error: %s", exc)
            self.logger.error("{0}".format(obj))
            return Result(453, None)

        self.response = None
        self.corr_id = str(uuid.uuid4())

        # Send RPC request to server
        not_sent = True
        attempts = 1
        while not_sent and attempts < 10:
            try:
                self.channel.basic_publish(exchange='',
                                           routing_key=self.queue,
                                           body=message,
                                           properties=pika.BasicProperties(
                                               reply_to=self.callback_queue,
                                               correlation_id=self.corr_id))
                not_sent = False
            except Exception as e:
                self.logger.info("Failed to send on attempt %d because connection closed. Reconnecting...", attempts)
                time.sleep((attempts-1)*5)
                if self.connection.is_closed:
                    self.connect()

            attempts = attempts + 1
        if not_sent:
            self.logger.error("Failed to deliver message %s:%s", self.queue, message.rstrip())
            return Result(470, "Message could not be delivered")

        # Wait for response
        while self.response is None:
            self.connection.process_data_events()
        self.corr_id = None

        # Decode the response dict as YAML
        try:
            res_obj = yaml.safe_load(self.response)
            return Result.from_transport_format(res_obj)
        except yaml.YAMLError as exc:
            self.logger.error("YAML serialization error: %s", exc)
            self.logger.error("{0}".format(obj))
            return Result(454, None)


