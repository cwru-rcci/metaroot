from metaroot.api.base_client import BaseClient
from metaroot.config import get_config
from metaroot.event.producer import Producer


class EventClientAPI(BaseClient):
    """
    An event based API client where only the delivery status to the message queue is returned.

    This client is intended for use when a long running operations needs to be initiated from an application that
    requires a short time from call to return. This style precludes use of methods that fetch/get information from
    the backend.
    """
    def __init__(self):
        super().__init__(Producer(get_config(self.__class__.__name__)))
