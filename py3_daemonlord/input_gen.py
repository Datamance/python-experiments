import random
import struct
import functools
from daemonizer import Daemonizer


def main():
    yield random.randint(1, 30)


serialize = functools.partial(struct.pack, ">I")


daemonized = Daemonizer(main, output_serializer=serialize)

daemonized.run()
