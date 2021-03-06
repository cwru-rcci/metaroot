from metaroot.config import get_config, debug_config
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
                                  config.get_file_verbosity(),
                                  config.get_screen_verbosity())

        # Output all config parameters when debugging
        self._logger.debug("VVVVVV Router Config VVVVVV")
        debug_config(config)

        # Instantiate an activity stream to store a record of requests/responses
        if config.get_activity_stream() != "$NONE":
            self._logger.info("Logging activity using %s", config.get_activity_stream())
            self.__activity_stream = instantiate_object_from_class_path(config.get_activity_stream())
        else:
            self._logger.info("***Not recording an activity stream***")
            self.__activity_stream = NullActivityStream()

        # Initialize managers to receive requests
        hooks = config.get_hooks()
        self._managers = []
        for hook in hooks:
            try:
                manager = instantiate_object_from_class_path(hook)
                try:
                    # Managers must implement methods "initialize()" and "finalize()" to ensure clean statup/shutdown
                    method = getattr(manager, "initialize")
                    method()
                    getattr(manager, "finalize")
                    self._managers.append(manager)
                    self._logger.info("Loaded manager for %s", hook)
                except AttributeError as e:
                    self._logger.error("Method 'initialize' or 'finalize' is not defined for manager/hook %s",
                                       manager.__class__.__name__)

            except Exception as e:
                self._logger.exception(e)
                self._logger.error("Exception while instantiating hook %s", hook)

        if len(self._managers) < len(hooks):
            self._logger.error("%d of %d hooks were initialized. refusing to run with reduced set.",
                               len(self._managers), len(hooks))
            exit(1)

        # Initialize reactions to take actions relative to requests outcomes
        self._reactions = None
        if config.has("REACTION_HANDLER"):
            self._reactions = instantiate_object_from_class_path(config.get("REACTION_HANDLER"))
        else:
            self._logger.info("No reaction handle specified so using DefaultReactions")
            from metaroot.api.reactions import DefaultReactions
            self._reactions = DefaultReactions()

        # Check for read-only operation to block write requests
        self._read_only = config.get_read_only_enabled()

    def __enter__(self):
        """
        Stub for instantiation in context manager. The router is meant to run in a consumer or RPC server so it needs
        to behave like a manager object, but no actions are required for initialize()
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Finalize all the managers before exiting the context block to ensure clean shutdown of message queue connections
        """
        self.finalize()

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

        # If operating in read-only mode, refuse all write requests
        if self._read_only:
            if "add" in method_name or "delete" in method_name or "associate" in method_name or \
               "update" in method_name or "set" in method_name:
                result = Result(470, "Read-only operation is enabled, but write operation requested")
                self.__activity_stream.record(method_name + ":any",
                                              args,
                                              result)
                return result

        n_priors = 0
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
                n_priors = n_priors + self._reactions.occur_in_response_to(manager.__class__.__name__, method_name, args, result, n_priors)

        return Result(status, all_results)

    def initialize(self):
        """
        Stub to adhere to general contract. The router is running in a consumer or RPC server so it needs to behave
        like a manager object, but it doesn't need to take an special actions on initialize.
        """
        pass

    def finalize(self):
        """
        Explicitly finalize all managers for clean shutdown
        """
        for manager in self._managers:
            manager.finalize()

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

    def list_users(self, with_default_group: str, managers: object) -> Result:
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
        return self._safe_call("list_users", [with_default_group], managers)

    def validate_users(self, names: list, managers: object):
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
        return self._safe_call("validate_users", [names], managers)

    def roles_user(self, name: str, managers: object):
        """
        Determine what roles this user is authorized to take in system interaction. This does not imply that the user
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
        return self._safe_call("roles_user", [name], managers)

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
