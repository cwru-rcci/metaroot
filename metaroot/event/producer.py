#!/usr/bin/env python
import pika
import pika.exceptions
import yaml
from metaroot.common import Result
from metaroot.config import Config
from metaroot.utils import get_logger


class Producer:
    """
    An AMQP message producer based on pika that sends messages with a YAML payload to the message queue server.
    """

    def __init__(self, config: Config):
        """
        Initialize a new Producer for use.

        Parameters
        ----------
        config: Config
            Connection properties for the message queue server

        Raises
        ----------
        Exception
            If any underlying operations fail by raising an exception.
        """
        # Pretty standard connection stuff
        self.connection = None
        self.channel = None
        self.config = config
        self.queue = config.get_mq_queue_name()
        self._logger = get_logger(Producer.__name__,
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
        self._logger.WARN("The connection has been blocked by the server")

    def _connection_unblocked_cb(self, frame):
        """
        Callback to log cases where the server resumed accepting messages after a period of rejecting them
        """
        self._logger.WARN("The connection has been unblocked")

    def connect(self):
        """
        Connect the producer to the message queue server

        Raises
        ----------
        Exception
            If any underlying operations fail by raising an exception.
        """
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

        # Turn on delivery confirmation
        self.channel.confirm_delivery()

    def close(self):
        """
        Close pika connection
        """
        try:
            self.connection.close()
        except Exception as e:
            self._logger.warning("closing connection raised an exception")

    def send(self, obj) -> Result:
        """
        Publish a message to server

        Parameters
        ----------
        obj: dict
            The message to send

        Returns
        ----------
        Result
            Result.status is 0 for success, >0 on error
            Result.response is None on success, and informational message on error
        """
        # Encode the request dict as YAML
        try:
            message = yaml.safe_dump(obj)
        except yaml.YAMLError as exc:
            self._logger.error("YAML serialization error: %s", exc)
            self._logger.error("{0}".format(obj))
            return Result(453, "Could not serialize the message as YAML")

        # Send RPC request to server
        self._logger.debug("Sending %s:%s", self.queue, message.rstrip())

        # Send message to server
        not_sent = True
        attempts = 1
        while not_sent and attempts < 10:
            try:
                delivered = self.channel.basic_publish(exchange='',
                                                       routing_key=self.queue,
                                                       body=message,
                                                       properties=pika.BasicProperties(
                                                           delivery_mode=2,
                                                           # Indicates message should be persisted on disk
                                                       ))
                not_sent = not delivered
            except pika.exceptions.ConnectionClosed as e:
                self._logger.error("Failed to send on attempt %d because connection closed. Reconnecting...", attempts)
                self.connect()
            except Exception as e:
                self._logger.error("Failed to send on attempt %d. Retrying...", attempts)

            attempts = attempts + 1

        if not_sent:
            self._logger.error("Failed to deliver message %s:%s", self.queue, message.rstrip())
            return Result(470, "Message could not be delivered")
        else:
            self._logger.debug("Success")
            return Result(0, None)



