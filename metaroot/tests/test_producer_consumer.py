import unittest
from threading import Thread
from metaroot.event.consumer import Consumer
from metaroot.event.producer import Producer
from metaroot.config import get_config
from metaroot.api.result import Result

sequence = 0


class OrderedHandler:
    def echo(self, message: str) -> Result:
        global sequence
        if message != "hello {0}".format(sequence):
            raise Exception("Expecting 'hello {0}' but consumed '{1}'".format(sequence, message))
        sequence = sequence + 1
        return Result(0, None)

    def initialize(self):
        pass

    def finalize(self):
        pass


def run_server():
    consumer = Consumer()
    with consumer as c:
        c.start("EVENT_TEST")


class IntegrationTest(unittest.TestCase):
    def test_integration(self):
        st = Thread(target=run_server)
        st.start()

        config = get_config("EVENT_TEST")
        with Producer(config) as p:
            for i in range(10):
                result = p.send({"action": "echo", "message": "hello {0}".format(i)})
                self.assertEqual(0, result.status)
            result = p.send("CLOSE_IMMEDIATELY")
            self.assertEqual(0, result.status)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(IntegrationTest)
    unittest.TextTestRunner(verbosity=2).run(suite)

