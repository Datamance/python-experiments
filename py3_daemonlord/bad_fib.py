# -*- coding: utf-8 -*-
from daemonizer import Daemonizer
from functools import partial
from struct import pack, unpack
import time


def main(foo):
    """Crappy fibonacci - we want big compute times to prove a point here."""
    time.sleep(1)
    if foo <= 2:
        return 1
    else:
        return main(foo - 1) + main(foo - 2)


def input_deserializer(bytes_obj):
    return unpack(">I", bytes_obj)


output_serializer = partial(pack, ">I")


daemonized = Daemonizer(
    main, input_deserializer=input_deserializer,
    output_serializer=output_serializer)

if __name__ == "__main__":
    daemonized.run()
