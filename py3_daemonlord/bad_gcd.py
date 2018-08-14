# -*- coding: utf-8 -*-
from daemonizer import Daemonizer
from functools import partial
from struct import pack, unpack
# import random
# import time


def main(first, second):
    """Crappy greatest common divisor"""
    # time.sleep(1)

    for x in range(min(first, second), 0, -1):
        if first % x == 0 and second % x == 0:
            return x  # guaranteed to return at least 1


def input_deserializer(bytes_obj):
    return unpack(">II", bytes_obj)


output_serializer = partial(pack, ">I")


daemonized = Daemonizer(
    main, input_deserializer=input_deserializer,
    output_serializer=output_serializer)


if __name__ == "__main__":
    daemonized.run()
