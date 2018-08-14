# -*- coding: utf-8 -*-
import random
import struct
import functools
from daemonizer import Daemonizer
import time


def main():
    # time.sleep(1)
    return random.randint(1, 5)


serialize = functools.partial(struct.pack, ">I")


daemonized = Daemonizer(main, output_serializer=serialize)

if __name__ == "__main__":
    daemonized.run()
