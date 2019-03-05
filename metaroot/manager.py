from metaroot.config import locate_config
from metaroot.utils import instantiate_object_from_class_path, get_logger
from metaroot.common import Result


class Manager:
    def __init__(self):
        config = locate_config(self.__class__.__name__)
        hooks = config.get_hooks()
        self._managers = []
        for hook in hooks:
            self._managers.append(instantiate_object_from_class_path(hook))

    def add_group(self, group_atts: dict) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.add_group(group_atts)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
        return Result(status, all_results)

    def get_group(self, name: str) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.get_group(name)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
        return Result(status, all_results)

    def get_members(self, name: str) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.get_members(name)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
        return Result(status, all_results)

    def update_group(self, group_atts: dict) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.update_group(group_atts)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
        return Result(status, all_results)

    def delete_group(self, name: str) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.delete_group(name)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
        return Result(status, all_results)

    def exists_group(self, name: str) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.exists_group(name)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
        return Result(status, all_results)

    def add_user(self, user_atts: dict) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.add_user(user_atts)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
        return Result(status, all_results)

    def update_user(self, user_atts: dict) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.update_user(user_atts)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
        return Result(status, all_results)

    def get_user(self, name: str) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.get_user(name)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
        return Result(status, all_results)

    def delete_user(self, name: str) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.delete_user(name)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
        return Result(status, all_results)

    def exists_user(self, name: str) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.exists_user(name)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
        return Result(status, all_results)

    def set_user_default_group(self, user_name: str, group_name: str) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.set_user_default_group(user_name, group_name)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
        return Result(status, all_results)

    def associate_user_to_group(self, user_name: str, group_name: str) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.associate_user_to_group(user_name, group_name)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
        return Result(status, all_results)

    def disassociate_user_from_group(self, user_name: str, group_name: str) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.disassociate_user_from_group(user_name, group_name)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
        return Result(status, all_results)

    def disassociate_users_from_group(self, user_names: list, group_name: str) -> Result:
        status = 0
        all_results = {}
        for manager in self._managers:
            result = manager.disassociate_users_from_group(user_names, group_name)
            status = status + result.status
            all_results[manager.__class__.__name__] = result
        return Result(status, all_results)

    

