from metaroot.rpc.client import RPCClient
from metaroot.config import get_config
from metaroot.api.result import Result


class SlurmManager(RPCClient):
    """
    RPC wrapper for metaroot.slurm.manager.SlurmManager
    Auto-generated 2019-03-18T10:09:36.029748
    """
    def __init__(self):
        super().__init__(get_config(self.__class__.__name__))

    """
    An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.add_group

    See Also
    --------
    metaroot.slurm.manager.SlurmManager.add_group
    """
    def add_group(self, group_atts) -> Result:
        request = {'action': 'add_group',
                   'group_atts': group_atts,
                   }
        return self.send(request)

    """
    An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.add_user

    See Also
    --------
    metaroot.slurm.manager.SlurmManager.add_user
    """
    def add_user(self, user_atts) -> Result:
        request = {'action': 'add_user',
                   'user_atts': user_atts,
                   }
        return self.send(request)

    """
    An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.associate_user_to_group

    See Also
    --------
    metaroot.slurm.manager.SlurmManager.associate_user_to_group
    """
    def associate_user_to_group(self, user_name, group_name) -> Result:
        request = {'action': 'associate_user_to_group',
                   'user_name': user_name,
                   'group_name': group_name,
                   }
        return self.send(request)

    """
    An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.delete_group

    See Also
    --------
    metaroot.slurm.manager.SlurmManager.delete_group
    """
    def delete_group(self, name) -> Result:
        request = {'action': 'delete_group',
                   'name': name,
                   }
        return self.send(request)

    """
    An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.delete_user

    See Also
    --------
    metaroot.slurm.manager.SlurmManager.delete_user
    """
    def delete_user(self, name) -> Result:
        request = {'action': 'delete_user',
                   'name': name,
                   }
        return self.send(request)

    """
    An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.disassociate_user_from_group

    See Also
    --------
    metaroot.slurm.manager.SlurmManager.disassociate_user_from_group
    """
    def disassociate_user_from_group(self, user_name, group_name) -> Result:
        request = {'action': 'disassociate_user_from_group',
                   'user_name': user_name,
                   'group_name': group_name,
                   }
        return self.send(request)

    """
    An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.disassociate_users_from_group

    See Also
    --------
    metaroot.slurm.manager.SlurmManager.disassociate_users_from_group
    """
    def disassociate_users_from_group(self, user_names, group_name) -> Result:
        request = {'action': 'disassociate_users_from_group',
                   'user_names': user_names,
                   'group_name': group_name,
                   }
        return self.send(request)

    """
    An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.exists_group

    See Also
    --------
    metaroot.slurm.manager.SlurmManager.exists_group
    """
    def exists_group(self, name) -> Result:
        request = {'action': 'exists_group',
                   'name': name,
                   }
        return self.send(request)

    """
    An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.exists_user

    See Also
    --------
    metaroot.slurm.manager.SlurmManager.exists_user
    """
    def exists_user(self, name) -> Result:
        request = {'action': 'exists_user',
                   'name': name,
                   }
        return self.send(request)

    """
    An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.get_group

    See Also
    --------
    metaroot.slurm.manager.SlurmManager.get_group
    """
    def get_group(self, name) -> Result:
        request = {'action': 'get_group',
                   'name': name,
                   }
        return self.send(request)

    """
    An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.get_members

    See Also
    --------
    metaroot.slurm.manager.SlurmManager.get_members
    """
    def get_members(self, name) -> Result:
        request = {'action': 'get_members',
                   'name': name,
                   }
        return self.send(request)

    """
    An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.get_user

    See Also
    --------
    metaroot.slurm.manager.SlurmManager.get_user
    """
    def get_user(self, name) -> Result:
        request = {'action': 'get_user',
                   'name': name,
                   }
        return self.send(request)

    """
    An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.set_user_default_group

    See Also
    --------
    metaroot.slurm.manager.SlurmManager.set_user_default_group
    """
    def set_user_default_group(self, user_name, group_name) -> Result:
        request = {'action': 'set_user_default_group',
                   'user_name': user_name,
                   'group_name': group_name,
                   }
        return self.send(request)

    """
    An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.update_group

    See Also
    --------
    metaroot.slurm.manager.SlurmManager.update_group
    """
    def update_group(self, group_atts) -> Result:
        request = {'action': 'update_group',
                   'group_atts': group_atts,
                   }
        return self.send(request)

    """
    An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.update_user

    See Also
    --------
    metaroot.slurm.manager.SlurmManager.update_user
    """
    def update_user(self, user_atts) -> Result:
        request = {'action': 'update_user',
                   'user_atts': user_atts,
                   }
        return self.send(request)

