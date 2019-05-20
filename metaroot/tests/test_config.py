import unittest
import metaroot.config


class MetarootTests(unittest.TestCase):

    def test_global_config(self):
        config = metaroot.config.get_global_config()
        self.assertEqual("user", config.get_mq_user())
        self.assertEqual("pass", config.get_mq_pass())
        self.assertEqual("host", config.get_mq_host())
        self.assertEqual(1234, config.get_mq_port())
        self.assertEqual("name", config.get_mq_queue_name())
        self.assertEqual("handlerClassName", config.get_mq_handler_class())
        self.assertEqual("INFO", config.get_screen_verbosity())
        self.assertEqual("ERROR", config.get_file_verbosity())
        self.assertEqual("$NONE", config.get_log_file())
        self.assertEqual(['HOOK1', 'HOOK2'], config.get_hooks())
        self.assertEqual("asClassName", config.get_activity_stream())
        self.assertEqual("dataBaseName", config.get_activity_stream_db())

    def test_class_config(self):
        config = metaroot.config.get_config("CLAZZ")

        # Inherited from GLOBAL
        self.assertEqual("user", config.get_mq_user())
        self.assertEqual("pass", config.get_mq_pass())
        self.assertEqual("host", config.get_mq_host())
        self.assertEqual(1234, config.get_mq_port())

        # Defined by class
        self.assertEqual("classQueueName", config.get_mq_queue_name())
        self.assertEqual("handlerClassName2", config.get_mq_handler_class())
        self.assertEqual("DEBUG", config.get_screen_verbosity())
        self.assertEqual("INFO", config.get_file_verbosity())
        self.assertEqual("$NONE", config.get_log_file())
        self.assertEqual(['HOOK3', 'HOOK4'], config.get_hooks())
        self.assertEqual("asClassName2", config.get_activity_stream())
        self.assertEqual("dataBaseName2", config.get_activity_stream_db())

        # Custom key/value without a builtin getter
        self.assertEqual("Value1", config.get("CUSTOM1"))
