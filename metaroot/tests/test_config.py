import unittest
import metaroot.config


class MetarootTests(unittest.TestCase):

    def test_auto_config(self):
        self.assertEqual(True, "METAROOT_GLOBAL" in metaroot.config.auto)
        self.assertEqual(True, "METAROOT_MQUSER" in metaroot.config.auto["METAROOT_GLOBAL"])
