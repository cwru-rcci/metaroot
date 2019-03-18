import unittest
from metaroot.api.event_client import EventClient
from metaroot.api.result import Result
import metaroot.daemon

sequence = 0


class OrderedHandlerTest:
    def echo(self, message: str) -> Result:
        global sequence
        if message != "hello {0}".format(sequence):
            raise Exception("Expecting 'hello {0}' but consumed '{1}'".format(sequence, message))
        sequence = sequence + 1
        return Result(0, None)


class AEventAPITest(unittest.TestCase):
    def test_api_add_group(self):
        api = EventClient()
        result = api.add_group({})
        self.assertEqual(0, result.status)


class BEventDaemonTest(unittest.TestCase):
    def test_daemon_add_group(self):
        metaroot.daemon.run("DAEMON")


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(AEventAPITest)
    unittest.TextTestRunner(verbosity=2).run(suite)

    suite = unittest.TestLoader().loadTestsFromTestCase(BEventDaemonTest)
    unittest.TextTestRunner(verbosity=2).run(suite)
