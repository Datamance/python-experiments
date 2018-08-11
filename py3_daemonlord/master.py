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

from daemonizer import DaemonLord, Worker, Reducer, Router
import random


def input_generator():
    yield random.randint(1, 30)


####
# Example static initialization
####
TASK_CONFIG = {  # Optional - 1:1 mapping with constructors!
    "input_generator": input_generator
    # "output_queue":
}  # Will need a "recursive initializer" for this!


###
# Example declarative initialization
###

# master = DaemonLord(TASK_CONFIG)
master = DaemonLord()

# Should be able to take actual imported module as well?
first_fib = Worker("bad_fib.py")
second_fib = Worker("bad_fib.py")

adder = Worker("bad_gcd.py")

# A reducer must:
# 1) establish ONE QUEUE PER INPUT!
# 2) Take those inputs and concatenate them somehow.
reducer = Reducer(inputs=(first_fib, second_fib))

router = Router(input=reducer, )


# MUST HAVES FOR API
# Registry - for when you want to be able to just use strings when you do the
# declarative API later.
master.register(
    input_source=input_generator,
    workers=[("first_fib", Worker("bad_fib.py")),
             ("second_fib", Worker("bad_fib.py"))],
    reducers=[("first_and_second_fib", Reducer())],
    routers=[("")]
)

master.start()
