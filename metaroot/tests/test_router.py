import unittest
from metaroot.api.result import Result
from metaroot.router import Router


class Handler1:
    def __init__(self):
        self.name = "handler1"

    def add_group(self, group_atts: dict):
        return Result(0, "add_group:"+self.name)

    def get_group(self, name: str):
        return Result(0, "get_group:"+self.name)

    def get_members(self, name: str):
        return Result(0, "get_members:"+self.name)

    def update_group(self, group_atts: dict):
        return Result(0, "update_group:"+self.name)

    def delete_group(self, name: str):
        return Result(0, "delete_group:"+self.name)

    def exists_group(self, name: str):
        return Result(0, "exists_group:"+self.name)

    def add_user(self, user_atts: dict):
        return Result(0, "add_user:"+self.name)

    def update_user(self, user_atts: dict):
        return Result(0, "update_user:"+self.name)

    def get_user(self, name: str):
        return Result(0, "get_user:"+self.name)

    def delete_user(self, name: str):
        return Result(0, "delete_user:"+self.name)

    def exists_user(self, name: str):
        return Result(0, "exists_user:"+self.name)

    def set_user_default_group(self, user_name: str, group_name: str):
        return Result(0, "set_user_default_group:"+self.name)

    def associate_user_to_group(self, user_name: str, group_name: str):
        return Result(0, "associate_user_to_group:"+self.name)

    def disassociate_user_from_group(self, user_name: str, group_name: str):
        return Result(0, "disassociate_user_from_group:"+self.name)

    def disassociate_users_from_group(self, user_names: list, group_name: str):
        return Result(0, "disassociate_users_from_group:"+self.name)


class Handler2:
    def __init__(self):
        self.name = "handler2"

    def add_group(self, group_atts: dict):
        return Result(0, "add_group:" + self.name)

    def get_group(self, name: str):
        return Result(0, "get_group:" + self.name)

    def get_members(self, name: str):
        return Result(0, "get_members:" + self.name)

    def update_group(self, group_atts: dict):
        return Result(0, "update_group:" + self.name)

    def delete_group(self, name: str):
        return Result(0, "delete_group:" + self.name)

    def exists_group(self, name: str):
        return Result(0, "exists_group:" + self.name)

    def add_user(self, user_atts: dict):
        return Result(0, "add_user:" + self.name)

    def update_user(self, user_atts: dict):
        return Result(0, "update_user:" + self.name)

    def get_user(self, name: str):
        return Result(0, "get_user:" + self.name)

    def delete_user(self, name: str):
        return Result(0, "delete_user:" + self.name)

    def exists_user(self, name: str):
        return Result(0, "exists_user:" + self.name)

    def set_user_default_group(self, user_name: str, group_name: str):
        return Result(0, "set_user_default_group:" + self.name)

    def associate_user_to_group(self, user_name: str, group_name: str):
        return Result(0, "associate_user_to_group:" + self.name)

    def disassociate_user_from_group(self, user_name: str, group_name: str):
        return Result(0, "disassociate_user_from_group:" + self.name)

    def disassociate_users_from_group(self, user_names: list, group_name: str):
        return Result(0, "disassociate_users_from_group:" + self.name)


class RouterTest(unittest.TestCase):
    def test_add_group(self):
        router = Router()
        result = router.add_group({}, "any")
        self.assertEqual(0, result.status)
        self.assertEqual(0, result.response["Handler1"]["status"])
        self.assertEqual("add_group:handler1", result.response["Handler1"]["response"])
        self.assertEqual(0, result.response["Handler2"]["status"])
        self.assertEqual("add_group:handler2", result.response["Handler2"]["response"])

    def test_get_group(self):
        router = Router()
        result = router.get_group("", "any")
        self.assertEqual(0, result.status)
        self.assertEqual(0, result.response["Handler1"]["status"])
        self.assertEqual("get_group:handler1", result.response["Handler1"]["response"])
        self.assertEqual(0, result.response["Handler2"]["status"])
        self.assertEqual("get_group:handler2", result.response["Handler2"]["response"])

    def test_get_members(self):
        router = Router()
        result = router.get_members("", "any")
        self.assertEqual(0, result.status)
        self.assertEqual(0, result.response["Handler1"]["status"])
        self.assertEqual("get_members:handler1", result.response["Handler1"]["response"])
        self.assertEqual(0, result.response["Handler2"]["status"])
        self.assertEqual("get_members:handler2", result.response["Handler2"]["response"])

    def test_update_group(self):
        router = Router()
        result = router.update_group({}, "any")
        self.assertEqual(0, result.status)
        self.assertEqual(0, result.response["Handler1"]["status"])
        self.assertEqual("update_group:handler1", result.response["Handler1"]["response"])
        self.assertEqual(0, result.response["Handler2"]["status"])
        self.assertEqual("update_group:handler2", result.response["Handler2"]["response"])

    def test_delete_group(self):
        router = Router()
        result = router.delete_group("", "any")
        self.assertEqual(0, result.status)
        self.assertEqual(0, result.response["Handler1"]["status"])
        self.assertEqual("delete_group:handler1", result.response["Handler1"]["response"])
        self.assertEqual(0, result.response["Handler2"]["status"])
        self.assertEqual("delete_group:handler2", result.response["Handler2"]["response"])

    def test_exists_group(self):
        router = Router()
        result = router.exists_group("", "any")
        self.assertEqual(0, result.status)
        self.assertEqual(0, result.response["Handler1"]["status"])
        self.assertEqual("exists_group:handler1", result.response["Handler1"]["response"])
        self.assertEqual(0, result.response["Handler2"]["status"])
        self.assertEqual("exists_group:handler2", result.response["Handler2"]["response"])

    def test_add_user(self):
        router = Router()
        result = router.add_user({}, "any")
        self.assertEqual(0, result.status)
        self.assertEqual(0, result.response["Handler1"]["status"])
        self.assertEqual("add_user:handler1", result.response["Handler1"]["response"])
        self.assertEqual(0, result.response["Handler2"]["status"])
        self.assertEqual("add_user:handler2", result.response["Handler2"]["response"])

    def test_update_user(self):
        router = Router()
        result = router.update_user({}, "any")
        self.assertEqual(0, result.status)
        self.assertEqual(0, result.response["Handler1"]["status"])
        self.assertEqual("update_user:handler1", result.response["Handler1"]["response"])
        self.assertEqual(0, result.response["Handler2"]["status"])
        self.assertEqual("update_user:handler2", result.response["Handler2"]["response"])

    def test_get_user(self):
        router = Router()
        result = router.get_user("", "any")
        self.assertEqual(0, result.status)
        self.assertEqual(0, result.response["Handler1"]["status"])
        self.assertEqual("get_user:handler1", result.response["Handler1"]["response"])
        self.assertEqual(0, result.response["Handler2"]["status"])
        self.assertEqual("get_user:handler2", result.response["Handler2"]["response"])

    def test_delete_user(self):
        router = Router()
        result = router.delete_user("", "any")
        self.assertEqual(0, result.status)
        self.assertEqual(0, result.response["Handler1"]["status"])
        self.assertEqual("delete_user:handler1", result.response["Handler1"]["response"])
        self.assertEqual(0, result.response["Handler2"]["status"])
        self.assertEqual("delete_user:handler2", result.response["Handler2"]["response"])

    def test_exists_user(self):
        router = Router()
        result = router.exists_user("", "any")
        self.assertEqual(0, result.status)
        self.assertEqual(0, result.response["Handler1"]["status"])
        self.assertEqual("exists_user:handler1", result.response["Handler1"]["response"])
        self.assertEqual(0, result.response["Handler2"]["status"])
        self.assertEqual("exists_user:handler2", result.response["Handler2"]["response"])

    def test_set_user_default_group(self):
        router = Router()
        result = router.set_user_default_group("", "", "any")
        self.assertEqual(0, result.status)
        self.assertEqual(0, result.response["Handler1"]["status"])
        self.assertEqual("set_user_default_group:handler1", result.response["Handler1"]["response"])
        self.assertEqual(0, result.response["Handler2"]["status"])
        self.assertEqual("set_user_default_group:handler2", result.response["Handler2"]["response"])

    def test_associate_user_to_group(self):
        router = Router()
        result = router.associate_user_to_group("", "", "any")
        self.assertEqual(0, result.status)
        self.assertEqual(0, result.response["Handler1"]["status"])
        self.assertEqual("associate_user_to_group:handler1", result.response["Handler1"]["response"])
        self.assertEqual(0, result.response["Handler2"]["status"])
        self.assertEqual("associate_user_to_group:handler2", result.response["Handler2"]["response"])

    def test_disassociate_user_from_group(self):
        router = Router()
        result = router.disassociate_user_from_group("", "", "any")
        self.assertEqual(0, result.status)
        self.assertEqual(0, result.response["Handler1"]["status"])
        self.assertEqual("disassociate_user_from_group:handler1", result.response["Handler1"]["response"])
        self.assertEqual(0, result.response["Handler2"]["status"])
        self.assertEqual("disassociate_user_from_group:handler2", result.response["Handler2"]["response"])

    def test_disassociate_users_from_group(self):
        router = Router()
        result = router.disassociate_users_from_group([], "", "any")
        self.assertEqual(0, result.status)
        self.assertEqual(0, result.response["Handler1"]["status"])
        self.assertEqual("disassociate_users_from_group:handler1", result.response["Handler1"]["response"])
        self.assertEqual(0, result.response["Handler2"]["status"])
        self.assertEqual("disassociate_users_from_group:handler2", result.response["Handler2"]["response"])


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(RouterTest)
    unittest.TextTestRunner(verbosity=2).run(suite)

