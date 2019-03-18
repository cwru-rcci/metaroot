import unittest
from metaroot.event.consumer import Consumer
from metaroot.event.producer import Producer
from metaroot.config import get_config
from metaroot.api.result import Result

sequence = 0


class OrderedHandlerTest:
    def echo(self, message: str) -> Result:
        global sequence
        if message != "hello {0}".format(sequence):
            raise Exception("Expecting 'hello {0}' but consumed '{1}'".format(sequence, message))
        sequence = sequence + 1
        return Result(0, None)


class AProducerTest(unittest.TestCase):
    def test_send(self):
        config = get_config("EVENT_TEST")
        producer = Producer(config)
        for i in range(1000):
            result = producer.send({"action": "echo", "message": "hello {0}".format(i)})
            self.assertEqual(0, result.status)
        producer.send("CLOSE_IMMEDIATELY")
        producer.close()


class BConsumerTest(unittest.TestCase):
    def test_consume(self):
        consumer = Consumer()
        consumer.start("EVENT_TEST")


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(AProducerTest)
    unittest.TextTestRunner(verbosity=2).run(suite)

    suite = unittest.TestLoader().loadTestsFromTestCase(BConsumerTest)
    unittest.TextTestRunner(verbosity=2).run(suite)
