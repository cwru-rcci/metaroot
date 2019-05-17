import unittest
from threading import Thread
from metaroot.api.event_client import EventClientAPI
from metaroot.api.result import Result
from metaroot.event.consumer import Consumer


class EventAPIRequestHandler:
    def add_group(self, group_atts: dict) -> Result:
        return Result(0, group_atts)

    def initialize(self):
        pass

    def finalize(self):
        pass


def run_server():
    consumer = Consumer()
    with consumer as c:
        c.start("EVENTCLIENTAPI")


class IntegrationTest(unittest.TestCase):
    def test_api_add_group(self):
        st = Thread(target=run_server)
        st.start()

        with EventClientAPI() as api:
            message = {"name": "value"}
            result = api.add_group(message)
            self.assertEqual(0, result.status)
            api._call("CLOSE_IMMEDIATELY")
        st.join()


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(IntegrationTest)
    unittest.TextTestRunner(verbosity=2).run(suite)

