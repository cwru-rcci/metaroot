import unittest
from metaroot.slurm.manager_rpc import SlurmManager

class SlurmManagerTest(unittest.TestCase):
    """
    Integration tests for the SlurmManager class. Method names are self explanatory. Most methods run multiple tests
    to check for expected success, expected failure, and roll-back modifications to the test database
    """
    @staticmethod
    def get_test_account():
        ta_attr = {"name": "test_acct",
                   "Organization": 'org1',
                   "Fairshare": 5,
                   "GrpCPUS": 4000,
                   "MaxWall": "13-12",
                   "MaxSubmitJobs": 2000,
                   "MaxJobs": 1000}

        return ta_attr

    @staticmethod
    def get_test_account2():
        ta_attr = {"name": "test_acct2",
                   "Organization": 'org2',
                   "Fairshare": 5,
                   "GrpCPUS": 4000,
                   "MaxWall": "13-12",
                   "MaxSubmitJobs": 2000}

        return ta_attr

    @staticmethod
    def get_test_user():
        ta_name = "test_acct"
        tu_name = "test_user"
        tu_attr = {"name": tu_name, "DefaultAccount": ta_name}

        return tu_attr

    @staticmethod
    def get_test_user2():
        ta_name = "test_acct"
        tu_name = "test_user"
        tu_attr = {"name": tu_name,
                   "MaxJobs": 300,
                   "Account": ta_name}

        return tu_attr

    @staticmethod
    def get_named_user(user_name: str, account_name: str):
        tu_attr = {"name": user_name,
                   "MaxJobs": 300,
                   "Account": account_name}
        return tu_attr

    @staticmethod
    def get_test_update_account():
        ta_name = "test_acct"
        ta_attr = {"name": ta_name,
                   "MaxSubmitJobs": 4000}

        return ta_attr

    def test_add_account(self):
        sam = SlurmManager()
        test_account = SlurmManagerTest.get_test_account()

        # Assert success when account is added
        result = sam.add_account(test_account)
        self.assertEqual(True, result.is_success())

        # Assert fail when account already exists
        result = sam.add_account(test_account)
        self.assertEqual(False, result.is_success())

        # Cleanup
        sam.delete_account(test_account["name"])

    def test_update_account(self):
        sam = SlurmManager()
        test_account = SlurmManagerTest.get_test_account()
        test_update = SlurmManagerTest.get_test_update_account()

        # Assert fail if account does not exist
        result = sam.update_account(test_update)
        self.assertEqual(False, result.is_success())

        # Add account
        sam.add_account(test_account)

        # Assert success when account exists
        result = sam.update_account(test_update)
        self.assertEqual(True, result.is_success())

        # Assert attribute was updated
        result = sam.get_account(test_account["name"])
        self.assertEqual("4000", result.response["MaxSubmit"])

        # Cleanup
        sam.delete_account(test_account["name"])

    def test_get_account(self):
        sam = SlurmManager()
        test_account = SlurmManagerTest.get_test_account()

        # Assert empty when account does not exist
        result = sam.get_account(test_account["name"])
        self.assertEqual(0, len(result.response))

        # Add account
        sam.add_account(test_account)

        # Assert success when account exists
        result = sam.get_account(test_account["name"])
        self.assertEqual("2000", result.response["MaxSubmit"])

        # Cleanup
        sam.delete_account(test_account["name"])

    def test_get_members(self):
        sam = SlurmManager()
        test_account = SlurmManagerTest.get_test_account()
        test_user = SlurmManagerTest.get_test_user()

        # Assert empty when account does not exist
        result = sam.get_members(test_account["name"])
        self.assertEqual(0, len(result.response))

        # Add account
        sam.add_account(test_account)

        # Assert empty when no users affiliated
        result = sam.get_members(test_account["name"])
        self.assertEqual(0, len(result.response))

        # Add a user affiliation
        sam.add_user(test_user)

        # Assert 1 member
        result = sam.get_members(test_account["name"])
        self.assertEqual(1, len(result.response))
        self.assertEqual(test_user["name"], result.response[0])

        # Cleanup
        sam.delete_user(test_user["name"])
        sam.delete_account(test_account["name"])

    def test_delete_account(self):
        sam = SlurmManager()
        test_account = SlurmManagerTest.get_test_account()
        sam.add_account(test_account)

        # Assert success when account exists
        result = sam.delete_account(test_account["name"])
        self.assertEqual(True, result.is_success())

        # Assert fail when account does not exist
        result = sam.delete_account(test_account["name"])
        self.assertEqual(False, result.is_success())

    def test_exists_account(self):
        sam = SlurmManager()
        test_account = SlurmManagerTest.get_test_account()

        # Assert success when account exists
        sam.add_account(test_account)
        result = sam.exists_account(test_account["name"])
        self.assertEqual(True, result.is_success())

        sam.delete_account(test_account["name"])

        # Assert fail when account does not exist
        result = sam.delete_account(test_account["name"])
        self.assertEqual(False, result.is_success())

    def test_add_user(self):
        sam = SlurmManager()
        test_account = SlurmManagerTest.get_test_account()
        test_user = SlurmManagerTest.get_test_user()

        # Assert success when user is added
        sam.add_account(test_account)
        result = sam.add_user(test_user)
        self.assertEqual(True, result.is_success())

        # Assert success when user already exists (nothing to do)
        result = sam.add_user(test_user)
        self.assertEqual(True, result.is_success())

        # Cleanup
        sam.delete_user(test_user["name"])
        sam.delete_account(test_account["name"])

    def test_associate_user_to_account(self):
        sam = SlurmManager()
        test_account = SlurmManagerTest.get_test_account()
        test_account2 = SlurmManagerTest.get_test_account2()
        test_user = SlurmManagerTest.get_test_user()

        # Assert failure when neither exists
        result = sam.associate_user_to_account(test_user["name"], test_account["name"])
        self.assertEqual(False, result.is_success())

        # Assert success when creating user and assigning association
        sam.add_account(test_account)
        result = sam.associate_user_to_account(test_user["name"], test_account["name"])
        self.assertEqual(True, result.is_success())

        # Assert success when creating an secondary association
        sam.add_account(test_account2)
        result = sam.associate_user_to_account(test_user["name"], test_account2["name"])
        self.assertEqual(True, result.is_success())

        # Assert failure when association already exists
        result = sam.associate_user_to_account(test_user["name"], test_account["name"])
        self.assertEqual(False, result.is_success())
        result = sam.associate_user_to_account(test_user["name"], test_account2["name"])
        self.assertEqual(False, result.is_success())

        # Cleanup (remove secondary association first)
        sam.delete_user(test_user["name"])
        sam.delete_account(test_account2["name"])
        sam.delete_account(test_account["name"])

    def test_disassociate_user_from_account(self):
        sam = SlurmManager()
        test_account = SlurmManagerTest.get_test_account()
        test_account2 = SlurmManagerTest.get_test_account2()
        test_user = SlurmManagerTest.get_test_user()

        # Create user, accounts and affiliations
        sam.add_account(test_account)
        sam.associate_user_to_account(test_user["name"], test_account["name"])
        sam.add_account(test_account2)
        sam.associate_user_to_account(test_user["name"], test_account2["name"])

        # disassociate from primary (assert success)
        result = sam.disassociate_user_from_account(test_user["name"], test_account["name"])
        self.assertEqual(True, result.is_success())
        self.assertEqual(True, result.response)

        # Check primary affiliation pivoted to 'bench'
        result = sam.get_user(test_user["name"])
        self.assertEqual('bench', result.response["default"])

        # Cleanup
        sam.delete_user(test_user["name"])
        sam.delete_account(test_account2["name"])
        sam.delete_account(test_account["name"])

    # This test checks a fairly complete use case involving 3 users and 2 accounts with the following member
    # hierarchy (* indicates primary account, - indicates secondary affiliation):
    #
    #        group1  group2
    #        ------  ------
    # user1 |  *
    # user2 |  -       *
    # user3 |  *
    #
    # We remove the members of group1 and expect that user1 and user3 will be benched, but user2 will not
    def test_disassociate_users_from_account(self):
        sam = SlurmManager()
        test_account = SlurmManagerTest.get_test_account()
        test_account2 = SlurmManagerTest.get_test_account2()
        test_user1 = SlurmManagerTest.get_named_user("user1", test_account["name"])
        test_user2 = SlurmManagerTest.get_named_user("user2", test_account2["name"])
        test_user3 = SlurmManagerTest.get_named_user("user3", test_account["name"])

        # Create user, accounts and affiliations
        sam.add_account(test_account)
        sam.add_account(test_account2)
        sam.add_user(test_user1)
        sam.add_user(test_user2)
        sam.add_user(test_user3)

        # Create a "secondary" affiliation of user2 to the first account
        sam.associate_user_to_account(test_user2["name"], test_account["name"])

        # disassociate from primary (assert success)
        result = sam.disassociate_users_from_account([test_user1["name"], test_user2["name"], test_user3["name"]],
                                                     test_account["name"])
        self.assertEqual(True, result.is_success())

        # Check that benched is of length 2 and contains the user names that were expected to be benched
        self.assertEqual(2, len(result.response))
        self.assertEqual(test_user1["name"], result.response[0])
        self.assertEqual(test_user3["name"], result.response[1])

        # Check members is empty
        result = sam.get_members(test_account["name"])
        self.assertEqual(0, len(result.response))

        # Cleanup
        sam.delete_user(test_user1["name"])
        sam.delete_user(test_user2["name"])
        sam.delete_user(test_user3["name"])
        sam.delete_account(test_account["name"])
        sam.delete_account(test_account2["name"])

    def test_update_user(self):
        sam = SlurmManager()
        test_account = SlurmManagerTest.get_test_account()
        test_user = SlurmManagerTest.get_test_user()
        update_user = SlurmManagerTest.get_test_user2()

        sam.add_account(test_account)

        # Assert fail if user does not exist
        result = sam.update_user(test_user)
        self.assertEqual(False, result.is_success())

        # Assert success when user exists
        sam.add_user(test_user)
        result = sam.update_user(update_user)
        self.assertEqual(True, result.is_success())

        # Assert attribute was updated
        result = sam.get_user(update_user["name"])
        self.assertEqual("300", result.response[test_account["name"]]["MaxJobs"])

        # Cleanup
        sam.delete_user(test_user["name"])
        sam.delete_account(test_account["name"])

    def test_get_user(self):
        sam = SlurmManager()
        test_account = SlurmManagerTest.get_test_account()
        test_account2 = SlurmManagerTest.get_test_account2()
        test_user = SlurmManagerTest.get_test_user()

        # Assert empty if user does not exist
        result = sam.get_user(test_user["name"])
        self.assertEqual(0, len(result.response))

        # Create user, accounts and affiliations
        sam.add_account(test_account)
        sam.associate_user_to_account(test_user["name"], test_account["name"])
        sam.add_account(test_account2)
        sam.associate_user_to_account(test_user["name"], test_account2["name"])

        # Check some expected user attributes
        result = sam.get_user(test_user["name"])
        self.assertEqual(3, len(result.response))
        self.assertEqual(test_account["name"], result.response['default'])

        # Cleanup (remove secondary association first)
        sam.delete_user(test_user["name"])
        sam.delete_account(test_account2["name"])
        sam.delete_account(test_account["name"])

    def test_delete_user(self):
        sam = SlurmManager()
        test_account = SlurmManagerTest.get_test_account()
        test_user = SlurmManagerTest.get_test_user()
        sam.add_account(test_account)
        sam.add_user(test_user)

        # Assert success when user exists
        result = sam.delete_user(test_user["name"])
        self.assertEqual(True, result.is_success())

        # Assert fail when user does not exist
        result = sam.delete_user(test_user["name"])
        self.assertEqual(False, result.is_success())

        # Cleanup
        sam.delete_account(test_account["name"])

    def test_exists_user(self):
        sam = SlurmManager()
        test_account = SlurmManagerTest.get_test_account()
        test_user = SlurmManagerTest.get_test_user()
        sam.add_account(test_account)
        sam.add_user(test_user)

        # Assert success when user exists
        result = sam.exists_user(test_user["name"])
        self.assertEqual(True, result.is_success())

        # Assert fail when user does not exist
        sam.delete_user(test_user["name"])
        result = sam.exists_user(test_user["name"])
        self.assertEqual(False, result.is_success())

        # Cleanup
        sam.delete_account(test_account["name"])

    def test_set_user_default_account(self):
        sam = SlurmManager()
        test_account = SlurmManagerTest.get_test_account()
        test_account2 = SlurmManagerTest.get_test_account2()
        test_user = SlurmManagerTest.get_test_user()

        sam.add_account(test_account)
        sam.add_account(test_account2)
        sam.add_user(test_user)

        # Assert default account is test_account
        result = sam.get_user(test_user["name"])
        self.assertEqual(test_account["name"], result.response['default'])

        # Change default to test_acct2
        sam.set_user_default_account(test_user["name"], test_account2["name"])

        # Assert default account is test_account2
        result = sam.get_user(test_user["name"])
        self.assertEqual(test_account["name"], result.response['default'])

        # Cleanup (remove secondary association first)
        sam.delete_user(test_user["name"])
        sam.delete_account(test_account["name"])
        sam.delete_account(test_account2["name"])


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(SlurmManagerTest)
    unittest.TextTestRunner(verbosity=2).run(suite)
