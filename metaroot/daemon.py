import sys
from metaroot.event.consumer import Consumer


def run(key: str):
    consumer = Consumer()
    consumer.start(key)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("USAGE: python3 -m metaroot.daemon <CONFIG_KEY>")
        exit(1)
    run(sys.argv[1])
