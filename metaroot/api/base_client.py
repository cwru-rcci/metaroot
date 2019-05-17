from metaroot.api.result import Result


class BaseClient:
    """
    Client for performing administrative tasks in metaroot backend infrastructure.
    """

    def __init__(self, client):
        """
        Initialize the API to use a Producer (Events) or RPCClient (RPC) for communication

        Parameters
        ----------
        client
            An instance of an object with a "send" method
        """
        self.client = client

    def __enter__(self):
        """
        Connect the RPC client to the message queue if manager is instantiated by a with statement
        """
        self.client.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Disconnects the RPC client from the message queue when with statement block is exited
        """
        self.client.close()

    def _call(self, request: object) -> Result:
        """
        Wrapper around the client send method (plan to add debug logging here)

        Parameters
        ----------
        request: object
            A dictionary the defines the request, or a simple string

        Returns
        ---------
        Result
            Depending on the underlying client this will be the status of message delivery (EventAPI) or the status
            of the backend operations (MethodAPI)
        """
        return self.client.send(request)

    def initialize(self):
        """
        Connects the RPC client to the message queue
        """
        self.client.connect()

    def finalize(self):
        """
        Disconnects the RPC client fomr the message queue
        """
        self.client.close()

    def add_group(self, group_atts, managers="any") -> Result:
        """
        Request that a group with name group_atts['name'] be created

        Parameters
        ----------
        group_atts: dict
            A dictionary of free form parameters that will be passed to backend systems for group creation. group_atts
            must minimally contain a key 'name'
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

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
                   'managers': managers
                   }
        return self._call(request)

    def add_user(self, user_atts, managers="any") -> Result:
        """
        Request that a user with name user_atts['name'] be created

        Parameters
        ----------
        user_atts: dict
            A dictionary of free form parameters that will be passed to backend systems for user creation. user_atts
            must minimally contain a key 'name'
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

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
                   'managers': managers
                   }
        return self._call(request)

    def associate_user_to_group(self, user_name, group_name, managers="any") -> Result:
        """
        Request that user is added to group

        Parameters
        ----------
        user_name: str
            Name of user
        group_name: str
            Group name
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        -------
        Result
            Depending on the underlying client this will be the status of message delivery (EventAPI) or the status
            of the backend operations (MethodAPI)
        """
        request = {'action': 'associate_user_to_group',
                   'user_name': user_name,
                   'group_name': group_name,
                   'managers': managers
                   }
        return self._call(request)

    def delete_group(self, name, managers="any") -> Result:
        """
        Request to delete a group

        Parameters
        ----------
        name: str
            Group name
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        -------
        Result
            Depending on the underlying client this will be the status of message delivery (EventAPI) or the status
            of the backend operations (MethodAPI)
        """
        request = {'action': 'delete_group',
                   'name': name,
                   'managers': managers
                   }
        return self._call(request)

    def delete_user(self, name, managers="any") -> Result:
        """
        Request to delete a user

        Parameters
        ----------
        name: str
            User name
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        -------
        Result
            Depending on the underlying client this will be the status of message delivery (EventAPI) or the status
            of the backend operations (MethodAPI)
        """
        request = {'action': 'delete_user',
                   'name': name,
                   'managers': managers
                   }
        return self._call(request)

    def disassociate_user_from_group(self, user_name, group_name, managers="any") -> Result:
        """
        Request to remove a user from a group

        Parameters
        ----------
        user_name: str
            Name of user
        group_name: str
            Group name
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        -------
        Result
            Depending on the underlying client this will be the status of message delivery (EventAPI) or the status
            of the backend operations (MethodAPI)
        """
        request = {'action': 'disassociate_user_from_group',
                   'user_name': user_name,
                   'group_name': group_name,
                   'managers': managers
                   }
        return self._call(request)

    def disassociate_users_from_group(self, user_names, group_name, managers="any") -> Result:
        """
        Request to remove a list of users from a group

        Parameters
        ----------
        user_names: list
            Names of users
        group_name: str
            Group name
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        -------
        Result
            Depending on the underlying client this will be the status of message delivery (EventAPI) or the status
            of the backend operations (MethodAPI)
        """
        request = {'action': 'disassociate_users_from_group',
                   'user_names': user_names,
                   'group_name': group_name,
                   'managers': managers
                   }
        return self._call(request)

    def set_user_default_group(self, user_name, group_name, managers="any") -> Result:
        """
        Request to set the default group for a user

        Parameters
        ----------
        user_name: str
            Name of user
        group_name: str
            Group name
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called
        Returns
        -------
        Result
            Depending on the underlying client this will be the status of message delivery (EventAPI) or the status
            of the backend operations (MethodAPI)
        """
        request = {'action': 'set_user_default_group',
                   'user_name': user_name,
                   'group_name': group_name,
                   'managers': managers
                   }
        return self._call(request)

    def update_group(self, group_atts, managers="any") -> Result:
        """
        Change the attributes of a group with name group_atts['name'] to those in group_atts

        Parameters
        ----------
        group_atts:
            Attributes to update. Must contain a key 'name' that specifies the group name.
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called
        Returns
        -------
        Result
            Depending on the underlying client this will be the status of message delivery (EventAPI) or the status
            of the backend operations (MethodAPI)
        """
        request = {'action': 'update_group',
                   'group_atts': group_atts,
                   'managers': managers
                   }
        return self._call(request)

    def update_user(self, user_atts, managers="any") -> Result:
        """
        Change the attributes of a user with name user_atts['name'] to those in user_atts

        Parameters
        ----------
        user_atts:
            Attributes to update. Must contain a key 'name' that specifies the user name.
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called
        Returns
        -------
        Result
            Depending on the underlying client this will be the status of message delivery (EventAPI) or the status
            of the backend operations (MethodAPI)
        """
        request = {'action': 'update_user',
                   'user_atts': user_atts,
                   'managers': managers
                   }
        return self._call(request)
