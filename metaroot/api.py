from metaroot.rpc.client import RPCClient
from metaroot.event.producer import Producer
from metaroot.config import get_config
from metaroot.common import Result


class API:
    """
    A collection of methods for performing administrative tasks in backend infrastructure
    """

    def __init__(self, client):
        """
        Initialize the API to use either a Producer (Events) or RPCClient (RPC) for communication

        Parameters
        ----------
        client
            An instance of an object with a "send" method
        """
        self._client = client

    def __del__(self):
        """
        Destructor attempts a clean shutdown by closing the underlying client used for communication
        :return:
        """
        self.close()

    def _call(self, request: dict) -> Result:
        """
        Wrapper around the client send method (plan to add debug logging here)

        Parameters
        ----------
        request: dict
            A dictionary the defines the request

        Returns
        ---------
        Result
            Depending on the underlying client this will be the status of message delivery (EventAPI) or the status
            of the backend operations (MethodAPI)
        """
        return self._client.send(request)

    def close(self):
        """
        Close the API instance
        """
        self._client.close()

    def add_group(self, group_atts) -> Result:
        """
        Request that a group with name group_atts['name'] be created

        Parameters
        ----------
        group_atts: dict
            A dictionary of free form parameters that will be passed to backend systems for group creation. group_atts
            must minimally contain a key 'name'

        Returns
        -------
        Result
            Depending on the underlying client this will be the status of message delivery (EventAPI) or the status
            of the backend operations (MethodAPI)

        Raises
        ------
        Exception
            If group_atts does not contain a name attribute
        """
        if 'name' not in group_atts:
            raise Exception("group_atts must contain a key 'name'")

        request = {'action': 'add_group',
                   'group_atts': group_atts,
                   }
        return self._call(request)

    def add_user(self, user_atts) -> Result:
        """
        Request that a user with name user_atts['name'] be created

        Parameters
        ----------
        user_atts: dict
            A dictionary of free form parameters that will be passed to backend systems for user creation. user_atts
            must minimally contain a key 'name'

        Returns
        -------
        Result
            Depending on the underlying client this will be the status of message delivery (EventAPI) or the status
            of the backend operations (MethodAPI)

        Raises
        ------
        Exception
            If user_atts does not contain a name attribute
        """
        if 'name' not in user_atts:
            raise Exception("group_atts must contain a key 'name'")

        request = {'action': 'add_user',
                   'user_atts': user_atts,
                   }
        return self._call(request)

    def associate_user_to_group(self, user_name, group_name) -> Result:
        """
        Request that user is added to group

        Parameters
        ----------
        user_name: str
            Name of user
        group_name: str
            Group name

        Returns
        -------
        Result
            Depending on the underlying client this will be the status of message delivery (EventAPI) or the status
            of the backend operations (MethodAPI)
        """
        request = {'action': 'associate_user_to_group',
                   'user_name': user_name,
                   'group_name': group_name,
                   }
        return self._call(request)

    def delete_group(self, name) -> Result:
        """
        Request to delete a group

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
        request = {'action': 'delete_group',
                   'name': name,
                   }
        return self._call(request)

    def delete_user(self, name) -> Result:
        """
        Request to delete a user

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
        request = {'action': 'delete_user',
                   'name': name,
                   }
        return self._call(request)

    def disassociate_user_from_group(self, user_name, group_name) -> Result:
        """
        Request to remove a user from a group

        Parameters
        ----------
        user_name: str
            Name of user
        group_name: str
            Group name

        Returns
        -------
        Result
            Depending on the underlying client this will be the status of message delivery (EventAPI) or the status
            of the backend operations (MethodAPI)
        """
        request = {'action': 'disassociate_user_from_group',
                   'user_name': user_name,
                   'group_name': group_name,
                   }
        return self._call(request)

    def disassociate_users_from_group(self, user_names, group_name) -> Result:
        """
        Request to remove a list of users from a group

        Parameters
        ----------
        user_names: list
            Names of users
        group_name: str
            Group name

        Returns
        -------
        Result
            Depending on the underlying client this will be the status of message delivery (EventAPI) or the status
            of the backend operations (MethodAPI)
        """
        request = {'action': 'disassociate_users_from_group',
                   'user_names': user_names,
                   'group_name': group_name,
                   }
        return self._call(request)

    def set_user_default_group(self, user_name, group_name) -> Result:
        """
        Request to set the default group for a user

        Parameters
        ----------
        user_name: str
            Name of user
        group_name: str
            Group name

        Returns
        -------
        Result
            Depending on the underlying client this will be the status of message delivery (EventAPI) or the status
            of the backend operations (MethodAPI)
        """
        request = {'action': 'set_user_default_group',
                   'user_name': user_name,
                   'group_name': group_name,
                   }
        return self._call(request)

    def update_group(self, group_atts) -> Result:
        request = {'action': 'update_group',
                   'group_atts': group_atts,
                   }
        return self._call(request)

    def update_user(self, user_atts) -> Result:
        request = {'action': 'update_user',
                   'user_atts': user_atts,
                   }
        return self._call(request)


class EventAPI(API):
    """
    An event based API where requests are dispatched, and only the delivery status to the message queue is returned.
    This API is intended for use when an operation is long running and needs to be initiated from an application that
    requires a short time from call ro return. This API style precludes use of methods the fetch/get information from
    the backend.
    """
    def __init__(self):
        super().__init__(Producer(get_config(self.__class__.__name__)))


class MethodAPI(API):
    """
    An RPC based API where requests are sent, and the result of the background operation is returned. This API is
    intended for use when the calling application can wait sufficiently long for a response. Methods to fetch/get
    information from the backend are available in this API (versus the EventAPI)
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
