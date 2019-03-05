from metaroot.rpc.client import RPCClient
from metaroot.config import locate_config
from metaroot.common import Result


class SlurmManager(RPCClient):
    """
    RPC wrapper for metaroot.slurm.manager.SlurmManager
    Auto-generated 2019-03-05T10:07:07.183258
    """
    def __init__(self):
        super().__init__(locate_config(self.__class__.__name__))

    def add_group(self, account_atts) -> Result:
        request = {'action': 'add_group',
                   'account_atts': account_atts,
                   }
        return self.call(request)

    def add_user(self, user_atts) -> Result:
        request = {'action': 'add_user',
                   'user_atts': user_atts,
                   }
        return self.call(request)

    def associate_user_to_group(self, user_name, account_name) -> Result:
        request = {'action': 'associate_user_to_group',
                   'user_name': user_name,
                   'account_name': account_name,
                   }
        return self.call(request)

    def delete_group(self, name) -> Result:
        request = {'action': 'delete_group',
                   'name': name,
                   }
        return self.call(request)

    def delete_user(self, name) -> Result:
        request = {'action': 'delete_user',
                   'name': name,
                   }
        return self.call(request)

    def disassociate_user_from_group(self, user_name, account_name) -> Result:
        request = {'action': 'disassociate_user_from_group',
                   'user_name': user_name,
                   'account_name': account_name,
                   }
        return self.call(request)

    def disassociate_users_from_group(self, user_names, account_name) -> Result:
        request = {'action': 'disassociate_users_from_group',
                   'user_names': user_names,
                   'account_name': account_name,
                   }
        return self.call(request)

    def exists_group(self, name) -> Result:
        request = {'action': 'exists_group',
                   'name': name,
                   }
        return self.call(request)

    def exists_user(self, name) -> Result:
        request = {'action': 'exists_user',
                   'name': name,
                   }
        return self.call(request)

    def get_group(self, name) -> Result:
        request = {'action': 'get_group',
                   'name': name,
                   }
        return self.call(request)

    def get_members(self, name) -> Result:
        request = {'action': 'get_members',
                   'name': name,
                   }
        return self.call(request)

    def get_user(self, name) -> Result:
        request = {'action': 'get_user',
                   'name': name,
                   }
        return self.call(request)

    def set_user_default_group(self, user_name, account_name) -> Result:
        request = {'action': 'set_user_default_group',
                   'user_name': user_name,
                   'account_name': account_name,
                   }
        return self.call(request)

    def update_group(self, account_atts) -> Result:
        request = {'action': 'update_group',
                   'account_atts': account_atts,
                   }
        return self.call(request)

    def update_user(self, user_atts) -> Result:
        request = {'action': 'update_user',
                   'user_atts': user_atts,
                   }
        return self.call(request)

