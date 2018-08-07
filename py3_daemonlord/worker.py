from daemonizer import Daemonizer
from functools import partial
from struct import pack, unpack


def main(foo):
    """Crappy fibonacci - we want big compute times to prove a point here."""
    if foo <= 2:
        return 1
    else:
        return main(foo - 1) + main(foo - 2)


def input_deserializer(bytes_obj):
    return unpack(">I", bytes_obj)


output_serializer = partial(pack, ">I")


daemonized = Daemonizer(main, input_deserializer, output_serializer)
daemonized.run()
