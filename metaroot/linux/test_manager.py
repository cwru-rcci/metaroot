import unittest
from metaroot.linux.manager import LinuxManager


class LinuxManagerTest(unittest.TestCase):
    def test_add_group(self):
        lm = LinuxManager()

        # Test expected success
        result = lm.add_group("test_grp", {})
        self.assertEqual(True, result.is_success())

        # Test expected failure
        result = lm.add_group("test_grp", {})
        self.assertEqual(False, result.is_success())

        # Cleanup
        lm.delete_group("test_grp")

    def test_exists_group(self):
        pass

    def test_delete_group(self):
        pass

    def test_add_user(self):
        pass

    def test_add_user_to_group(self):
        pass

    def test_remove_user_from_group(self):
        pass

    def test_change_user_primary_group(self):
        pass

    def test_exists_user(self):
        pass

    def test_delete_user(self):
        pass
