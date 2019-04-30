from metaroot.config import get_config
from metaroot.utils import instantiate_object_from_class_path, get_logger
from metaroot.api.result import Result
from metaroot.api.reactions import DefaultReactions


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
            try:
                self._managers.append(instantiate_object_from_class_path(hook))
                self._logger.info("Loaded manager for %s", hook)
            except Exception as e:
                self._logger.exception(e)
                self._logger.error("Exception while instantiating hook %s", hook)

        self._reactions = None
        if config.has("METAROOT_REACTION_HANDLER"):
            self._reactions = instantiate_object_from_class_path(config.get("METAROOT_REACTION_HANDLER"))
        else:
            self._reactions = DefaultReactions()

    def _safe_call(self, method_name: str, args: list, target_managers="any") -> Result:
        """
        Iterates over the list of Managers, calling manager methods that implement the API request and returning the
        individual and overall result.

        Parameters
        ----------
        method_name: str
            The name of the method that should be called on each Manager
        args: list
            An ordered list of arguments that match the method signature
        target_managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        -------
        Result
            Result.status is the overall status: 0 for success, >0 for error
            Result.response is a dictionary with keys that are the class names of each Manager that implements the
            requested method and the value of each key is the specific Result returned by the call to that Manger
            class method
        """
        status = 0
        all_results = {}

        for manager in self._managers:
            # Filter which mangers to target (by default all will be targeted)
            if target_managers == "any" or manager.__class__.__name__ in target_managers:

                # Find the method on the manager (skip if not defined)
                try:
                    method = getattr(manager, method_name)
                except AttributeError as e:
                    self._logger.debug("Method %s is not defined for manager/hook %s",
                                       method_name, manager.__class__.__name__)
                    continue

                result = method(*args)
                status = status + result.status
                all_results[manager.__class__.__name__] = result.to_transport_format()
                self.__activity_stream.record(method_name + ":" + manager.__class__.__name__,
                                              args,
                                              result)

                # Allow reactions to occur in response to result of last action
                self._reactions.occur_in_response_to(manager.__class__.__name__, method_name, args, result)

        return Result(status, all_results)

    def add_group(self, group_atts: dict, managers: object) -> Result:
        """
        Adds a group through each configured Manager

        Parameters
        ----------
        group_atts : dict
            Properties defining the group. These are case insensitive and optional, except for a key "name", whose
            existence is enforced by the API client.
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("add_group", [group_atts], managers)

    def get_group(self, name: str, managers: object) -> Result:
        """
        Retrieves the group information from each configured Manager

        Parameters
        ----------
        name: str
            The group name
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("get_group", [name], managers)

    def list_groups(self, managers: object) -> Result:
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
        return self._safe_call("list_groups", [], managers)

    def get_members(self, name: str, managers: object) -> Result:
        """
        Retrieves the members of a group from each configured Manager

        Parameters
        ----------
        name: str
            The group name
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("get_members", [name], managers)

    def update_group(self, group_atts: dict, managers: object) -> Result:
        """
        Updates a group through each configured Manager

        Parameters
        ----------
        group_atts : dict
            Properties defining the group. These are case insensitive and optional, except for a key "name", whose
            existence is enforced by the API client.
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("update_group", [group_atts], managers)

    def delete_group(self, name: str, managers: object) -> Result:
        """
        Deletes a group from each configured Manager

        Parameters
        ----------
        name: str
            The group name
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("delete_group", [name], managers)

    def exists_group(self, name: str, managers: object) -> Result:
        """
        Tests for the existence of a group through each configured Manager

        Parameters
        ----------
        name: str
            The group name
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("exists_group", [name], managers)

    def add_user(self, user_atts: dict, managers: object) -> Result:
        """
        Add a user through each configured Manager

        Parameters
        ----------
        user_atts : dict
            Properties defining the user. These are case insensitive and optional, except for a key "name", whose
            existence is enforced by the API client.
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("add_user", [user_atts], managers)

    def update_user(self, user_atts: dict, managers: object) -> Result:
        """
        Update a user through each configured Manager

        Parameters
        ----------
        user_atts : dict
            New properties for the user. These are case insensitive and optional, except for a key "name", whose
            existence is enforced by the API client.
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("update_user", [user_atts], managers)

    def get_user(self, name: str, managers: object) -> Result:
        """
        Retrieve all information about a user from each configured Manager

        Parameters
        ----------
        name: str
            The name of the user
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("get_user", [name], managers)

    def list_users(self, managers: object) -> Result:
        """
        Enumerate all user names that are defined in the backend

        Parameters
        ----------
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        -------
        Result
            Lists of user names generated by all backend managers that implement the method
        """
        return self._safe_call("list_users", [], managers)

    def delete_user(self, name: str, managers: object) -> Result:
        """
        Delete a user from each configured Manager

        Parameters
        ----------
        name: str
            The name of the user
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("delete_user", [name], managers)

    def exists_user(self, name: str, managers: object) -> Result:
        """
        Test for existence of a user through each configured Manager

        Parameters
        ----------
        name: str
            The name of the user
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("exists_user", [name], managers)

    def set_user_default_group(self, user_name: str, group_name: str, managers: object) -> Result:
        """
        Set the user's default group through each configured Manager

        Parameters
        ----------
        user_name: str
            The name of the user
        group_name:
            The name of the group
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("set_user_default_group", [user_name, group_name], managers)

    def associate_user_to_group(self, user_name: str, group_name: str, managers: object) -> Result:
        """
        Make a user a member of a group through each configured Manager

        Parameters
        ----------
        user_name: str
            The name of the user
        group_name:
            The name of the group
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("associate_user_to_group", [user_name, group_name], managers)

    def disassociate_user_from_group(self, user_name: str, group_name: str, managers: object) -> Result:
        """
        Remove a user from a group through each configured Manager

        Parameters
        ----------
        user_name: str
            The name of the user
        group_name:
            The name of the group
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("disassociate_user_from_group", [user_name, group_name], managers)

    def disassociate_users_from_group(self, user_names: list, group_name: str, managers: object) -> Result:
        """
        Remove a list of users from a group through each configured Manager

        Parameters
        ----------
        user_names: str
            The names of the users
        group_name:
            The name of the group
        managers: object
            Either the string "any" meaning all managers that implement the method will be called, or a list of
            manager class names that should be called

        Returns
        ---------
        Result
            The result of the operation

        See Also
        ---------
        #_safe_call
        """
        return self._safe_call("disassociate_users_from_group", [user_names, group_name], managers)
