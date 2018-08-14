
# -*- coding: utf-8 -*-
"""
Things a coroutine can do:

result = await future or result = yield from future – suspends the coroutine
    until the future is done, then returns the future’s result, or raises an
    exception, which will be propagated. (If the future is cancelled, it will
    raise a CancelledError exception.) Note that tasks are futures, and
    everything said about futures also applies to tasks.

result = await coroutine or result = yield from coroutine – wait for another
    coroutine to produce a result (or raise an exception, which will be
    propagated). The coroutine expression must be a call to another coroutine.

return expression – produce a result to the coroutine that is waiting for this
    one using await or yield from.
raise exception – raise an exception in the coroutine that is waiting for this
    one using await or yield from.

Calling a coroutine does not start its code running – the coroutine object
    returned by the call doesn’t do anything until you schedule its execution.
    There are two basic ways to start it running: call await coroutine or yield
    from coroutine from another coroutine (assuming the other coroutine is
    already running!), or schedule its execution using the ensure_future()
    function or the AbstractEventLoop.create_task() method.

Coroutines (and tasks) can only run when the event loop is running.


asyncio.create_task(coro)
Wrap a coroutine coro into a task and schedule its execution. Return the task
object.

The task is executed in get_running_loop() context, RuntimeError is raised if
there is no running loop in current thread.


PHILOSOPHY:

- All async anything, meaning all IO bound things, should be done in the main
  thread.

- All CPU-bound things should be delegated to the workers.


ARCHITECTURE:

- This is going to be something like a task graph - a series of nodes that
have inputs and outputs.

- Tasks output to routers (queues). Those routers copy that output and send it
  to the reducers of the functions that need those outputs as inputs.

- Tasks can have many inputs, but only one output. As previously mentioned,
  it's up to the router to copy that output and route it to a reducer.

- Tasks output to routers (queues). Those routers output to reducers.

Routers:
- Have queues.
- await put() into those queues
- await get() from those queues
- get() results are tuple of (worker_name, result) --> worker name keys a dict
  where the values are the list of reducers, the value is copied and put into
  the reducer queue for each.

Reducers:
- have Queues
- await put() into those queues
- immediately get() from the queue
- get the result and cache it according to the passed worker name (key it)
- keep waiting until you have all the necessary inputs
- finally, when all keys have values, pop() them all and have your serializer
  run.


these things ARE special queues. They are queues but doing slightly different
strategies for input and output.
"""


import sys
import asyncio
from typing import Iterable
import struct


AsyncQueue = asyncio.Queue
subprocess = asyncio.subprocess


DEFAULT_REDUCER = b''.join


# IMPORTED INTO MASTER

class Manasa:
    """The goddess of the serpents!"""

    def __init__(self):
        """Constructor."""
        self._loop = asyncio.get_event_loop()
        # asyncio.get_child_watcher().attach_loop(self._loop)
        # asyncio.set_event_loop(self._loop)
        self._workers = {}
        self._reducers = {}
        self._routers = {}

    def run(self):
        """Run until complete."""

        self.spawn()

        try:
            self._loop.run_forever()
        finally:
            self.die()

    def die(self):
        """Stop the loop."""
        self._loop.run_until_complete(self._loop.shutdown_asyncgens())
        self._loop.stop()
        self._loop.close()

    def worker(self, worker_name: str, worker_path: str):
        """Registers a worker"""
        self._workers[worker_name] = Worker(worker_path)

    def feed(self, source_name, target_name, buffer_size=0):
        """Feeds one process to another via regular async queue."""
        queue = AsyncQueue(max_size=buffer_size, loop=self._loop)
        self._workers[source_name].set_output(queue)
        self._workers[source_name].set_input(queue)


    def route(self, source_name : str, target_list: Iterable[str]):
        """Route things together."""
        print("INSIDE ROUTE")
        router = self._routers.setdefault(
            source_name, Router(master_loop=self._loop))

        source = self._workers[source_name]

        source.set_output(router)

        for target_name in target_list:
            target = self._workers[target_name]
            queue = router.get_queue(target_name)
            target.set_input(queue)



    def reduce(self, source_list, target_worker):
        """Reduce."""
        reducer = self._reducers.setdefault(
            target_worker, Reducer(master_loop=self._loop))

        target = self._workers[target_worker]

        target.set_input(reducer)

        for source_name in source_list:
            source = self._workers[source_name]
            queue = reducer.get_queue(source_name)
            source.set_output(queue)


    def spawn(self):
        """Creates a daemon and adds it to the loop."""
        for worker in self._workers.values():
            self._loop.create_task(worker.run())


class Node:
    """Will build this out eventually for task graph purposes."""



class Worker(Node):
    """The Worker class."""
    def __init__(self, worker_path, in_q=None, out_q=None):
        """Class is a really great way to maintain state, go figure :)"""
        self._proc = None
        self._worker_path = worker_path
        print(f"initializing {worker_path}")
        self._input = in_q
        self._output = out_q

    def set_input(self, in_q):
        self._input = in_q

    def set_output(self, out_q):
        self._output = out_q

    async def init(self):
        """Actual initialization routine."""
        await self._mount_process()
        return self

    async def _mount_process(self):
        # Create the subprocess, redirect the standard output into a pipe
        self._proc = await asyncio.create_subprocess_exec(
            sys.executable, '-u', self._worker_path,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)

    async def run(self):
        if not self._proc:
            await self.init()
        # Read one line of output
        while True:
            if self._input:
                in_bytes = await self._input.get()
                print(f"PUTTING BYTES INTO {self._worker_path}")
                print(in_bytes)
                self._proc.stdin.write(in_bytes)
            # PROCESSING HAPPENS HERE!
            if self._output:
                out_bytes = await self._proc.stdout.readline()
                await self._output.put(out_bytes)
                # above, "await" only matters if there is a full queue.


class Reducer(Node):
    """Takes multiple inputs and concatenates them to one."""

    def __init__(
            self, queue_defs=(), master_loop=None, reducer=DEFAULT_REDUCER):
        self._loop = master_loop or asyncio.get_running_loop()
        self._reduce = reducer
        self._queues = {  # 0 is name, 1 is size
            queue_spec[0]: AsyncQueue(maxsize=queue_spec[1], loop=self._loop)
            for queue_spec in queue_defs
        }

    def get_queue(self, source_name, size=0):
        """Add a queue."""
        return self._queues.setdefault(
            source_name, AsyncQueue(maxsize=size, loop=self._loop))


    async def get(self):
        results = await asyncio.gather(
            *[subqueue.get() for subqueue in self._queues.values()]
        )

        print("RESULTS: ", results)
        return self._reduce(results)


class Router(Node):
    """Ensures that multiple subscribers will recieve the same input."""
    def __init__(self, queue_defs=(), master_loop=None):
        self._loop = master_loop or asyncio.get_running_loop()
        self._queues = {
            queue_spec[0]: AsyncQueue(maxsize=queue_spec[1], loop=self._loop)
            for queue_spec in queue_defs
        }

    def get_queue(self, target_name, size=0):
        """Add a queue."""
        return self._queues.setdefault(target_name, AsyncQueue(
            maxsize=size, loop=self._loop))


    async def put(self, value):
        await asyncio.gather(
            *[subqueue.put(value) for subqueue in self._queues.values()]
        )


# IMPORTED INTO WORKER

class Daemonizer:
    """Import this, instantiate it, and run it in your worker file."""

    def __init__(
        self, main_fn, input_deserializer=None, output_serializer=None,
            batch_size=None):
        """Constructor."""
        self._main_fn = main_fn
        self._deserialize = input_deserializer
        self._serialize = output_serializer
        self._batch_size = batch_size or 1
        self._arg_collector = []

    def run(self):
        """Create a daemon from a simple worker function."""
        main_fn = self._main_fn
        deserialize = self._deserialize
        serialize = self._serialize
        batch_size = self._batch_size
        arg_collector = self._arg_collector
        # In the future, somehow limit this to batch size or whatever

        while True:
            if deserialize:
                # 1) Get inputs from stdin.
                input_bytes = sys.stdin.buffer.readline().rstrip(b"\n")
                # the below could be: functools.partial(struct.pack, 'hhl')
                input_args = deserialize(input_bytes)[0]
                arg_collector.append(input_args)

                if len(arg_collector) < batch_size:
                    continue  # jump to next loop!
                else:
                    result = main_fn(*arg_collector)
                    arg_collector = []  # Reset
            else:
                result = main_fn()

            if serialize:
                output_bytes = serialize(result)
                sys.stdout.buffer.write(output_bytes + b"\n")
