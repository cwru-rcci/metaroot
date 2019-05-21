import unittest
from threading import Thread
from metaroot.api.method_client import MethodClientAPI
from metaroot.api.result import Result
from metaroot.rpc.server import RPCServer


class MethodAPIRequestHandler:
    def add_group(self, group_atts: dict) -> Result:
        return Result(0, group_atts)

    def initialize(self):
        pass

    def finalize(self):
        pass


def run_server():
    server = RPCServer()
    with server as s:
        s.start("METHODCLIENTAPI")


class IntegrationTest(unittest.TestCase):
    def test_method_api_integration(self):
        st = Thread(target=run_server)
        st.start()

        with MethodClientAPI() as api:
            message = {"name": "value"}
            result = api.add_group(message)
            self.assertEqual(0, result.status)
            self.assertEqual(message, result.response)
            api._call("CLOSE_IMMEDIATELY")
        st.join()


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(IntegrationTest)
    unittest.TextTestRunner(verbosity=2).run(suite)

