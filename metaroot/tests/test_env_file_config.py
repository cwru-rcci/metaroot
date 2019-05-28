import unittest
import metaroot.config


class MetarootEnvfileConfigTests(unittest.TestCase):

    def test_envfile_global_config(self):
        config = metaroot.config.get_global_config()
        self.assertEqual("$NONE", config.get_log_file())

    def test_envfile_class_config(self):
        config = metaroot.config.get_config("ROUTER")
        self.assertEqual(["metaroot.tests.test_router_rw.Handler1", "metaroot.tests.test_router_rw.Handler2"],
                         config.get_hooks())


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(MetarootEnvfileConfigTests)
    unittest.TextTestRunner(verbosity=2).run(suite)