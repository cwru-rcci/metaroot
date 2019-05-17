import unittest
from threading import Thread
from metaroot.rpc.server import RPCServer
from metaroot.rpc.client import RPCClient
from metaroot.config import get_config
from metaroot.api.result import Result

sequence = 0


class OrderedHandler:
    def echo(self, message: str) -> Result:
        global sequence
        if message != "hello {0}".format(sequence):
            raise Exception("Expecting 'hello {0}' but consumed '{1}'".format(sequence, message))
        sequence = sequence + 1
        return Result(0, message)

    def initialize(self):
        pass

    def finalize(self):
        pass


def run_server():
    server = RPCServer()
    with server as s:
        s.start("RPC_TEST")


class IntegrationTest(unittest.TestCase):
    def test_integration(self):
        st = Thread(target=run_server)
        st.start()

        config = get_config("RPC_TEST")
        with RPCClient(config) as c:
            for i in range(10):
                message = "hello {0}".format(i)
                result = c.send({"action": "echo", "message": message})
                self.assertEqual(0, result.status)
                self.assertEqual(message, result.response)
            result = c.send("CLOSE_IMMEDIATELY")
            self.assertEqual(0, result.status)
            self.assertEqual("SHUTDOWN_INIT", result.response)

        st.join()


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(IntegrationTest)
    unittest.TextTestRunner(verbosity=2).run(suite)

