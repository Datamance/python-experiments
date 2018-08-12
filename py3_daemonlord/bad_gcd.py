from daemonizer import Daemonizer
from functools import partial
from struct import pack, unpack


def main(first, second):
    """Crappy greatest common divisor"""
    for x in reversed(range(min(first, second))):
        if first % x == 0 and second % x == 0:
            return x  # guaranteed to return at least 1


def input_deserializer(bytes_obj):
    return unpack(">II", bytes_obj)


output_serializer = partial(pack, ">I")


daemonized = Daemonizer(
    main, input_deserializer=input_deserializer,
    output_serializer=output_serializer)

daemonized.run()
