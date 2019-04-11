import pika
from metaroot.config import get_global_config


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
    config = get_global_config()

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
    config = get_global_config()

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