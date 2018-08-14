# -*- coding: utf-8 -*-
"""
An example master file.

So theoretically, we should be able to load task graph configs from a TOML or
YAML file. I can probably do this with tablib pretty easily.

The constructs involved are:
- master process (DaemonLord)
- workers
- routers: take a single input and distribute it to multiple targets
- reducers:

We should have an imperative way of putting components together. This will
serve as the low-level API.

Upon that low-level API, we build the first step of a declarative interface,
which is constructor-based creation. That's what I'll illustrate below.
Eventually, we will build a configured_master.py that takes the TASK_CONFIG.


You should SUBSCRIBE to a reducer. That reducer will then give you its outputs.

A router will take A SINGLE INPUT and distribute it to multiple targets.

Both of these have the same api - from, to, and optional how and with.
+ "how" takes a function.
+ "with" takes a full-blown reducer or router object.
only one of these should be passed.

task_two, task_three = master.route(
    from=task_one, to=[task_two, task_three], how=None, with=None)

master.reduce(
    from=[task_two, task_three, task_four], to=task_five, how=None, with=None)
"""

from daemonizer import Manasa

####
# Example static initialization
####
# TASK_CONFIG = {}


####
# Example imperative initialization
####
manasa = Manasa()

# manasa.register()

manasa.worker("input_gen", "input_gen.py")
manasa.worker("bad_fib", "bad_fib.py")
manasa.worker("bad_fib_2", "bad_fib.py")
manasa.worker("bad_gcd", "bad_gcd.py")

manasa.route("input_gen", ("bad_fib", "bad_fib_2"))

manasa.reduce(("bad_fib", "bad_fib_2"), "bad_gcd")

# manasa.output("bad_gcd")

manasa.run()
