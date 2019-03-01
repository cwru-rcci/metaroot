import pika
from metaroot.rpc.config import Config


def declare_queue(config_file: str, queue_name: str):
    config = Config()
    config.load(config_file)

    # Pretty standard connection stuff
    credentials = pika.PlainCredentials(config.get_mq_user(), config.get_mq_pass())
    parameters = pika.ConnectionParameters(host=config.get_mq_host(),
                                           port=config.get_mq_port(),
                                           virtual_host='/',
                                           credentials=credentials,
                                           heartbeat=30)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.queue_declare(queue_name)

    connection.close()

