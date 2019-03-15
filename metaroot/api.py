from metaroot.rpc.client import RPCClient
from metaroot.event.producer import Producer
from metaroot.config import get_config
from metaroot.common import Result


class API:
    def __init__(self, client):
        self._client = client

    def __del__(self):
        self.close()

    def close(self):
        self._client.close()

    def call(self, request: dict) -> Result:
        return self._client.send(request)

    def add_group(self, group_atts) -> Result:
        request = {'action': 'add_group',
                   'group_atts': group_atts,
                   }
        return self.call(request)

    def add_user(self, user_atts) -> Result:
        request = {'action': 'add_user',
                   'user_atts': user_atts,
                   }
        return self.call(request)

    def associate_user_to_group(self, user_name, group_name) -> Result:
        request = {'action': 'associate_user_to_group',
                   'user_name': user_name,
                   'group_name': group_name,
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

    def disassociate_user_from_group(self, user_name, group_name) -> Result:
        request = {'action': 'disassociate_user_from_group',
                   'user_name': user_name,
                   'group_name': group_name,
                   }
        return self.call(request)

    def disassociate_users_from_group(self, user_names, group_name) -> Result:
        request = {'action': 'disassociate_users_from_group',
                   'user_names': user_names,
                   'group_name': group_name,
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

    def set_user_default_group(self, user_name, group_name) -> Result:
        request = {'action': 'set_user_default_group',
                   'user_name': user_name,
                   'group_name': group_name,
                   }
        return self.call(request)

    def update_group(self, group_atts) -> Result:
        request = {'action': 'update_group',
                   'group_atts': group_atts,
                   }
        return self.call(request)

    def update_user(self, user_atts) -> Result:
        request = {'action': 'update_user',
                   'user_atts': user_atts,
                   }
        return self.call(request)


class EventAPI(API):
    def __init__(self):
        super().__init__(Producer(get_config(self.__class__.__name__)))


class MethodAPI(API):
    def __init__(self):
        super().__init__(RPCClient(get_config(self.__class__.__name__)))

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
