from metaroot.api.abstract_client import ClientAPI
from metaroot.api.result import Result
from metaroot.config import get_config
from metaroot.rpc.client import RPCClient


class MethodClientAPI(ClientAPI):
    """
    An RPC based API client where requests are sent, and the result of the background operation is returned. This client
    is intended for use when the calling application can wait sufficiently long for a response. Methods to fetch/get
    information from the backend are available (versus the EventClient)
    """

    def __init__(self):
        super().__init__(RPCClient(get_config(self.__class__.__name__)))

    def exists_group(self, name) -> Result:
        """
        Test if a group exists

        Parameters
        ----------
        name: str
            Group name

        Returns
        -------
        Result
            Depending on the underlying client this will be the status of message delivery (EventAPI) or the status
            of the backend operations (MethodAPI)
        """
        request = {'action': 'exists_group',
                   'name': name,
                   }
        return self._call(request)

    def exists_user(self, name) -> Result:
        """
        Test if a user exists

        Parameters
        ----------
        name: str
            User name

        Returns
        -------
        Result
            Depending on the underlying client this will be the status of message delivery (EventAPI) or the status
            of the backend operations (MethodAPI)
        """
        request = {'action': 'exists_user',
                   'name': name,
                   }
        return self._call(request)

    def get_group(self, name) -> Result:
        """
        Retrieve all information about the group from the backend

        Parameters
        ----------
        name: str
            Group name

        Returns
        -------
        Result
            The group data
        """
        request = {'action': 'get_group',
                   'name': name,
                   }
        return self._call(request)

    def get_members(self, name) -> Result:
        """
        Retrieve a list of users associated with a group

        Parameters
        ----------
        name: str
            Group name

        Returns
        -------
        Result
            The list of user names associate with the group
        """
        request = {'action': 'get_members',
                   'name': name,
                   }
        return self._call(request)

    def get_user(self, name) -> Result:
        """
        Retrieve all information about the user from the backend

        Parameters
        ----------
        name: str
            User name

        Returns
        -------
        Result
            The user data
        """
        request = {'action': 'get_user',
                   'name': name,
                   }
        return self._call(request)
