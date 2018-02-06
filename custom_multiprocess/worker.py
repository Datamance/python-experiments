"""Doing big heavy important things"""
import sys
from time import sleep
import random

while True:
    line = sys.stdin.readline()
    experiments = line.split()
    # UF AH BIG BOI DO HEVY COMPUTE
    sleep(random.uniform(0.5,1.5))
    # wauww such compute much amaze
    scores = ' '.join(['%s:%s' % (x, str(random.uniform(0.5, 0.95))) for x in experiments])
    sys.stdout.write(scores + '\n')
