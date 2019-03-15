from metaroot.config import get_config
from metaroot.utils import instantiate_object_from_class_path
from metaroot.common import Result


class Router:
    def __init__(self):
        config = get_config(self.__class__.__name__)
        hooks = config.get_hooks()
        self.__activity_stream = instantiate_object_from_class_path(config.get_activity_stream())
        self._managers = []
        for hook in hooks:
            self._managers.append(instantiate_object_from_class_path(hook))

    def add_group(self, group_atts: dict) -> Result:
        """
        Calls the add_group(group_atts: dict) method of all configured managers. All actions are recoded by the
        configured ActivityStream

        Parameters
        ----------
        group_atts : dict
            Properties defining the account. These are case insensitive, and minimally require a key "name"

        Returns
        ---------
        Result
            Result.status is 0 for success, >0 if the operation failed (e.g., if the account already exists)
        """
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.add_group(group_atts)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
            self.__activity_stream.record("add_group:"+manager.__class__.__name__, group_atts, result)
        return Result(status, all_results)

    def get_group(self, name: str) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.get_group(name)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
            self.__activity_stream.record("get_group:" + manager.__class__.__name__, name, result)
        return Result(status, all_results)

    def get_members(self, name: str) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.get_members(name)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
            self.__activity_stream.record("get_group:" + manager.__class__.__name__, name, result)
        return Result(status, all_results)

    def update_group(self, group_atts: dict) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.update_group(group_atts)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
            self.__activity_stream.record("update_group:" + manager.__class__.__name__, group_atts, result)
        return Result(status, all_results)

    def delete_group(self, name: str) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.delete_group(name)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
            self.__activity_stream.record("delete_group:" + manager.__class__.__name__, name, result)
        return Result(status, all_results)

    def exists_group(self, name: str) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.exists_group(name)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
            self.__activity_stream.record("exists_group:" + manager.__class__.__name__, name, result)
        return Result(status, all_results)

    def add_user(self, user_atts: dict) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.add_user(user_atts)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
            self.__activity_stream.record("add_user:" + manager.__class__.__name__, user_atts, result)
        return Result(status, all_results)

    def update_user(self, user_atts: dict) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.update_user(user_atts)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
            self.__activity_stream.record("update_user:" + manager.__class__.__name__, user_atts, result)
        return Result(status, all_results)

    def get_user(self, name: str) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.get_user(name)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
            self.__activity_stream.record("get_user:" + manager.__class__.__name__, name, result)
        return Result(status, all_results)

    def delete_user(self, name: str) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.delete_user(name)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
            self.__activity_stream.record("delete_user:" + manager.__class__.__name__, name, result)
        return Result(status, all_results)

    def exists_user(self, name: str) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.exists_user(name)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
            self.__activity_stream.record("exists_user:" + manager.__class__.__name__, name, result)
        return Result(status, all_results)

    def set_user_default_group(self, user_name: str, group_name: str) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.set_user_default_group(user_name, group_name)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
            self.__activity_stream.record("set_user_default_group:" + manager.__class__.__name__, [user_name, group_name], result)
        return Result(status, all_results)

    def associate_user_to_group(self, user_name: str, group_name: str) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.associate_user_to_group(user_name, group_name)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
            self.__activity_stream.record("associate_user_to_group:" + manager.__class__.__name__,
                                          [user_name, group_name], result)
        return Result(status, all_results)

    def disassociate_user_from_group(self, user_name: str, group_name: str) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.disassociate_user_from_group(user_name, group_name)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
            self.__activity_stream.record("disassociate_user_from_group:" + manager.__class__.__name__,
                                          [user_name, group_name], result)
        return Result(status, all_results)

    def disassociate_users_from_group(self, user_names: list, group_name: str) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.disassociate_users_from_group(user_names, group_name)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
            self.__activity_stream.record("disassociate_users_from_group:" + manager.__class__.__name__,
                                          [user_names, group_name], result)
        return Result(status, all_results)
