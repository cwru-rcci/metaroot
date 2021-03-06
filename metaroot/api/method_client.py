from metaroot.api.base_client import BaseClient
from metaroot.api.result import Result
from metaroot.config import get_config
from metaroot.rpc.client import RPCClient


class MethodClientAPI(BaseClient):
    """
    An RPC based API client where requests are sent, and the result of the background operation is returned. This client
    is intended for use when the calling application can wait sufficiently long for a response. Methods to fetch/get
    information from the backend are available (versus the EventClient)
    """

    def __init__(self):
        super().__init__(RPCClient(get_config(self.__class__.__name__)))

    def exists_group(self, name, managers="any") -> Result:
        """
        Test if a group exists

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
        request = {'action': 'exists_group',
                   'name': name,
                   'managers': managers
                   }
        return self._call(request)

    def exists_user(self, name, managers="any") -> Result:
        """
        Test if a user exists

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
        request = {'action': 'exists_user',
                   'name': name,
                   'managers': managers
                   }
        return self._call(request)

    def get_group(self, name, managers="any") -> Result:
        """
        Retrieve all information about the group from the backend

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
            The group data
        """
        request = {'action': 'get_group',
                   'name': name,
                   'managers': managers
                   }
        return self._call(request)

    def get_members(self, name, managers="any") -> Result:
        """
        Retrieve a list of users associated with a group

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
            The list of user names associate with the group
        """
        request = {'action': 'get_members',
                   'name': name,
                   'managers': managers
                   }
        return self._call(request)

    def get_user(self, name, managers="any") -> Result:
        """
        Retrieve all information about the user from the backend

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
            The user data
        """
        request = {'action': 'get_user',
                   'name': name,
                   'managers': managers
                   }
        return self._call(request)

    def list_users(self, with_default_group="any", managers="any") -> Result:
        """
        Enumerate all user names that are defined in the backend

        Parameters
        ----------
        with_default_group: str
            Either the string "any" meaning any group, or a string id of a group that will restrict the result to only
            users with the specified group set as their default
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        -------
        Result
            Lists of user names generated by all backend managers that implement the method
        """
        request = {'action': 'list_users',
                   'with_default_group': with_default_group,
                   'managers': managers
                   }
        return self._call(request)

    def validate_users(self, names: list, managers="any"):
        """
        Removes user names from the argument list that are invalid, returning the list containing only valid
        user names. If this method is implemented, it usually means that validation requires lookup in a backend
        database.

        Parameters
        ----------
        names: list
            User names to validate
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        -------
        Result
            Result.status is 0 for success, > 0 on error.
            Result.response is the list of names that were valid
        """
        request = {'action': 'validate_users',
                   'names': names,
                   'managers': managers
                   }
        return self._call(request)

    def roles_user(self, name: str, managers="any"):
        """
        Report what roles this user is authorized to take in system interaction. This does not imply that the user
        has actually been provisioned in the system.

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
            Result.status is 0 for success, > 0 on error.
            Result.response is the list of names that were valid
        """
        request = {'action': 'roles_user',
                   'name': name,
                   'managers': managers}
        return self._call(request)

    def list_groups(self, managers="any") -> Result:
        """
        Enumerate all group names that are defined in the backend

        Parameters
        ----------
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        -------
        Result
            Lists of group names generated by all backend managers that implement the method
        """
        request = {'action': 'list_groups',
                   'managers': managers
                   }
        return self._call(request)
