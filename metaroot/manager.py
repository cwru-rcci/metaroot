from metaroot.config import locate_config
from metaroot.utils import instantiate_object_from_class_path
from metaroot.common import Result


class Manager:
    def __init__(self):
        config = locate_config(self.__class__.__name__)
        hooks = config.get_hooks()
        self._managers = []
        for hook in hooks:
            self._managers.append(instantiate_object_from_class_path(hook))

    def add_account(self, account_atts) -> Result:
        status = 0
        response = ""
        for manager in self._managers:
            result = manager.add_account(account_atts)
            status = status + result.status
            response = response + "{0}={1}".format(manager.__class__.__name__, result.status)
        return Result(status, response)

    def add_user(self, user_atts) -> Result:
        status = 0
        response = ""
        for manager in self._managers:
            result = manager.add_user(user_atts)
            status = status + result.status
            response = response + "{0}={1}".format(manager.__class__.__name__, result.status)
        return Result(status, response)

    def set_user_default_account(self, user_name, account_name) -> Result:
        status = 0
        response = ""
        for manager in self._managers:
            result = manager.set_user_default_account(user_name, account_name)
            status = status + result.status
            response = response + "{0}={1}".format(manager.__class__.__name__, result.status)
        return Result(status, response)

    def delete_account(self, name) -> Result:
        status = 0
        response = ""
        for manager in self._managers:
            result = manager.delete_account(name)
            status = status + result.status
            response = response + "{0}={1}".format(manager.__class__.__name__, result.status)
        return Result(status, response)

    def delete_user(self, name) -> Result:
        status = 0
        response = ""
        for manager in self._managers:
            result = manager.delete_user(name)
            status = status + result.status
            response = response + "{0}={1}".format(manager.__class__.__name__, result.status)
        return Result(status, response)

    def associate_user_to_account(self, user_name, account_name) -> Result:
        status = 0
        response = ""
        for manager in self._managers:
            result = manager.associate_user_to_account(user_name, account_name)
            status = status + result.status
            response = response + "{0}={1}".format(manager.__class__.__name__, result.status)
        return Result(status, response)

    def disassociate_user_from_account(self, user_name, account_name) -> Result:
        status = 0
        response = ""
        for manager in self._managers:
            result = manager.disassociate_user_from_account(user_name, account_name)
            status = status + result.status
            response = response + "{0}={1}".format(manager.__class__.__name__, result.status)
        return Result(status, response)

