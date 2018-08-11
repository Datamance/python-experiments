
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

Reducers in specific must have a BUFFER for output.



TODO(Datamance): Reducers and Routers are done. Now, we need to know how to
create them dynamically. We may need a registry for this.
"""


import sys
import asyncio


AsyncQueue = asyncio.Queue
subprocess = asyncio.subprocess


# IMPORTED INTO MASTER

class DaemonLord:
    """The Lord of the daemons!

    This guy should tie processes together with queues.
    """

    def __init__(self):
        """Constructor."""
        self._loop = asyncio.get_event_loop()

    def run(self):
        """Run until complete."""
        try:
            self._loop.run_forever()
        finally:
            self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            self._loop.close()

    def die(self):
        """Stop the loop."""
        self._loop.stop()
        self._loop.close()

    def spawn(self, worker_file, input_queue, output_queue):
        """Creates a daemon and adds it to the loop."""
        task = self._loop.create_task(
            daemon_coro(worker_file, input_queue, output_queue))


async def daemon_coro(worker_file, input_queue, output_queue):
    # Create the subprocess, redirect the standard output into a pipe
    proc = await asyncio.create_subprocess_exec(
        sys.executable, '-u', worker_file,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)

    # Read one line of output
    while True:
        in_bytes = await input_queue.get()
        proc.stdin.write(in_bytes)
        # PROCESSING HAPPENS HERE!
        out_bytes = await proc.stdout.readline()
        await output_queue.put(out_bytes)
        # above, "await" only matters if there is a full queue.


class Worker:
    """Worky work busy bee"""
    pass


DEFAULT_REDUCER = b''.join


class Reducer():
    """Takes multiple inputs and concatenates them to one."""

    def __init__(self, queue_defs, master_loop, reducer=DEFAULT_REDUCER):
        self._loop = master_loop
        self._reduce = reducer
        self._queues = {
            queue_spec[0]: AsyncQueue(maxsize=queue_spec[1], loop=self._loop)
            for queue_spec in queue_defs
        }

    def add_queue(self, queue_spec):
        """Add a queue."""
        self._queues[queue_spec[0]] = AsyncQueue(
            maxsize=queue_spec[1], loop=self._loop)


    async def get(self):
        results = await self._loop.run_until_complete(
            self._loop.gather(
                *[subqueue.get() for subqueue in self._queues.values()]
            )
        )

        return self._reduce(results)

    async def put(self, subqueue, item):
        await self._queues[subqueue].put(item)


class Router():
    """Ensures that multiple subscribers will recieve the same input."""
    def _init(self, queue_defs, master_loop):
        self._loop = master_loop
        self._queues = {
            queue_spec[0]: AsyncQueue(maxsize=queue_spec[1], loop=self._loop)
            for queue_spec in queue_defs
        }

    async def put(self, value):
        await self._loop.run_until_complete(
            self._loop.gather(
                *[subqueue.put(value) for subqueue in self._queues.values()]
            )
        )

    async def get(self, subqueue):
        value = await self._output_queues[subqueue].get()
        return value


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
            # 1) Get inputs from stdin.
            input_bytes = sys.stdin.readline()
            # the below could be: functools.partial(struct.pack, 'hhl')
            input_args = deserialize(input_bytes)

            arg_collector.append(input_args)

            if len(arg_collector) < batch_size:
                continue  # jump to next loop!
            else:
                result = main_fn(arg_collector)
                arg_collector = []  # Reset
                # Assumes returning a tuple of outputs.
                output_bytes = serialize(result)
                sys.stdout.write(output_bytes + b"\n")
