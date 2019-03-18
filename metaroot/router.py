from metaroot.config import get_config
from metaroot.utils import instantiate_object_from_class_path, get_logger
from metaroot.api.result import Result


class NullActivityStream:
    """
    Built-in activity stream to ignore all records (used if config specifies ACTIVITY_STREAM = "$NONE")
    """
    def record(self, id: str, params: object, result: Result):
        pass


class Router:
    """
    Manages the sequence of calls/responses required to distribute a single API requests to one or more backend RPC,
    servers, collecting the overall result, and logging actions to make failures recoverable.
    """

    def __init__(self):
        """
        Initializes the ActivityStream and list of Managers that will be contacted when API requests arrive
        """
        config = get_config(self.__class__.__name__)
        self._logger = get_logger(self.__class__.__name__,
                                  config.get_log_file(),
                                  config.get_mq_file_verbosity(),
                                  config.get_mq_screen_verbosity())

        if config.get_activity_stream() != "$NONE":
            self._logger.info("Logging activity using %s", config.get_activity_stream())
            self.__activity_stream = instantiate_object_from_class_path(config.get_activity_stream())
        else:
            self._logger.warning("***Not recording an activity stream***")
            self.__activity_stream = NullActivityStream()

        hooks = config.get_hooks()
        self._managers = []
        for hook in hooks:
            self._managers.append(instantiate_object_from_class_path(hook))
            self._logger.info("Loaded manager for %s", hook)

    def _safe_call(self, method_name: str, args: list) -> Result:
        """
        Iterates over the list of Managers, calling manager methods that implement the API request and returning the
        individual and overall result.

        Parameters
        ----------
        method_name: str
            The name of the method that should be called on each Manager
        args: list
            An ordered list of arguments that match the method signature

        Returns
        -------
        Result
            Result.status is the overall status: 0 for success, >0 for error
            Result.response is a dictionary with keys that are the class names of each Manager that implements the
            requested method and the value of each key is the specific Result returned by the call to that Mangers's
            class method
        """
        status = 0
        all_results = {}

        for manager in self._managers:
            # Find the method on the manager (skip if not defined)
            try:
                method = getattr(manager, method_name)
            except AttributeError as e:
                self._logger.debug("Method %s is not defined for manager/hook %s",
                                   method_name, manager.__class__.__name__)
                continue

            result = method(*args)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
            self.__activity_stream.record(method_name + ":" + manager.__class__.__name__,
                                          args,
                                          result)
        return Result(status, all_results)

    def add_group(self, group_atts: dict) -> Result:
        """
        Adds a group through each configured Manager

        Parameters
        ----------
        group_atts : dict
            Properties defining the group. These are case insensitive and optional, except for a key "name", whose
            existence is enforced by the API client.

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("add_group", [group_atts])

    def get_group(self, name: str) -> Result:
        """
        Retrieves the group information from each configured Manager

        Parameters
        ----------
        name: str
            The group name

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("get_group", [name])

    def get_members(self, name: str) -> Result:
        """
        Retrieves the members of a group from each configured Manager

        Parameters
        ----------
        name: str
            The group name

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("get_members", [name])

    def update_group(self, group_atts: dict) -> Result:
        """
        Updates a group through each configured Manager

        Parameters
        ----------
        group_atts : dict
            Properties defining the group. These are case insensitive and optional, except for a key "name", whose
            existence is enforced by the API client.

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("update_group", [group_atts])

    def delete_group(self, name: str) -> Result:
        """
        Deletes a group from each configured Manager

        Parameters
        ----------
        name: str
            The group name

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("delete_group", [name])

    def exists_group(self, name: str) -> Result:
        """
        Tests for the existence of a group through each configured Manager

        Parameters
        ----------
        name: str
            The group name

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("exists_group", [name])

    def add_user(self, user_atts: dict) -> Result:
        """
        Add a user through each configured Manager

        Parameters
        ----------
        user_atts : dict
            Properties defining the user. These are case insensitive and optional, except for a key "name", whose
            existence is enforced by the API client.

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("add_user", [user_atts])

    def update_user(self, user_atts: dict) -> Result:
        """
        Update a user through each configured Manager

        Parameters
        ----------
        user_atts : dict
            New properties for the user. These are case insensitive and optional, except for a key "name", whose
            existence is enforced by the API client.

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("update_user", [user_atts])

    def get_user(self, name: str) -> Result:
        """
        Retrieve all information about a user from each configured Manager

        Parameters
        ----------
        name: str
            The name of the user

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("get_user", [name])

    def delete_user(self, name: str) -> Result:
        """
        Delete a user from each configured Manager

        Parameters
        ----------
        name: str
            The name of the user

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("delete_user", [name])

    def exists_user(self, name: str) -> Result:
        """
        Test for existence of a user through each configured Manager

        Parameters
        ----------
        name: str
            The name of the user

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("exists_user", [name])

    def set_user_default_group(self, user_name: str, group_name: str) -> Result:
        """
        Set the user's default group through each configured Manager

        Parameters
        ----------
        user_name: str
            The name of the user
        group_name:
            The name of the group

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("set_user_default_group", [user_name, group_name])

    def associate_user_to_group(self, user_name: str, group_name: str) -> Result:
        """
        Make a user a member of a group through each configured Manager

        Parameters
        ----------
        user_name: str
            The name of the user
        group_name:
            The name of the group

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("associate_user_to_group", [user_name, group_name])

    def disassociate_user_from_group(self, user_name: str, group_name: str) -> Result:
        """
        Remove a user from a group through each configured Manager

        Parameters
        ----------
        user_name: str
            The name of the user
        group_name:
            The name of the group

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("disassociate_user_from_group", [user_name, group_name])

    def disassociate_users_from_group(self, user_names: list, group_name: str) -> Result:
        """
        Remove a list of users from a group through each configured Manager

        Parameters
        ----------
        user_names: str
            The names of the users
        group_name:
            The name of the group

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("disassociate_users_from_group", [user_names, group_name])
