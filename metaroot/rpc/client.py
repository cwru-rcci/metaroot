#!/usr/bin/env python
import pika
import uuid
import yaml
import metaroot.rpc.config
import metaroot.utils
from metaroot.common import Result


class RPCClient:
    """
    A lightweight RPC client based on pika that passes YAML messages
    """

    def __init__(self, config_file='client-config.yml'):
        """
        Initialize a new RPC Client for use.

        Parameters
        ----------
        config_file: str (default "client-config.yml")
            The YAML file specifying configuration parameters. By defaul the client looks for a file "client-config.yml"
            in the current working directory

        Raises
        ----------
        Exception
            If any underlying operations fail by raising an exception

        """
        # Connection properties and credentials are in the config file
        config = metaroot.rpc.config.Config()
        config.load(config_file)

        # Pretty standard connection stuff
        credentials = pika.PlainCredentials(config.get_mq_user(), config.get_mq_pass())
        parameters = pika.ConnectionParameters(host=config.get_mq_host(),
                                               port=config.get_mq_port(),
                                               virtual_host='/',
                                               credentials=credentials,
                                               heartbeat=30)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

        # Declare a delete-on-exit queue for this client to receive RPC callback message
        qd_result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = qd_result.method.queue

        # Specify the function to process the RPC callback responses
        self.channel.basic_consume(self.on_response, no_ack=True, queue=self.callback_queue)

        # Initialize remaining attributes
        self.corr_id = None
        self.response = None
        self.queue = config.get_mq_queue_name()
        self._logger = metaroot.utils.get_logger(RPCClient.__name__)

    def __del__(self):
        """
        Attempt to close the pika connection in the destructor if the connection is still open
        """
        if self.connection.is_open:
            self.connection.close()

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

    def finish(self):
        """
        Shutdown the RPC Client
        """
        self.connection.close()

    def call(self, obj) -> Result:
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
            self._logger.error("YAML serialization error: %s", exc)
            self._logger.error("{0}".format(obj))
            return Result(453, None)

        self.response = None
        self.corr_id = str(uuid.uuid4())

        # Send RPC request to server
        self.channel.basic_publish(exchange='',
                                   routing_key=self.queue,
                                   body=message,
                                   properties=pika.BasicProperties(
                                       reply_to=self.callback_queue,
                                       correlation_id=self.corr_id))

        # Wait for response
        while self.response is None:
            self.connection.process_data_events()
        self.corr_id = None

        # Decode the response dict as YAML
        try:
            res_obj = yaml.safe_load(self.response)
            return Result.from_transport_format(res_obj)
        except yaml.YAMLError as exc:
            self._logger.error("YAML serialization error: %s", exc)
            self._logger.error("{0}".format(obj))
            return Result(454, None)


