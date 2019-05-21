import unittest
from metaroot.api.notifications import send_email


class NotificationsTest(unittest.TestCase):
    def test_send_email_fail_address_unresolveable(self):
        self.assertEqual(False, send_email("foo", "test email", "<i>Test Content</i>"))


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(NotificationsTest)
    unittest.TextTestRunner(verbosity=2).run(suite)