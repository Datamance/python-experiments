
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
"""


import sys
import asyncio


AsyncQueue = asyncio.Queue
subprocess = asyncio.subprocess


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

    def create_daemon(self, worker_file, input_queue, output_queue):
        """Creates a daemon and adds it to the loop."""
        self._tasks.append(
            self._loop.create_task(
                daemon(worker_file, input_queue, output_queue))
        )



async def daemon(worker_file, input_queue, output_queue):
    # Create the subprocess, redirect the standard output into a pipe
    proc = await asyncio.create_subprocess_exec(
        sys.executable, '-u', worker_file,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)

    # Read one line of output
    while True:
        in_bytes = await input_queue.get()
        proc.stdin.write(in_bytes)
        out_bytes = await proc.stdout.readline()
        await output_queue.put()  # Only care if there is a full queue.


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
                continue
            else:
                result = main_fn(arg_collector)
                arg_collector = []  # Reset
                # Assumes returning a tuple of outputs.
                output_bytes = serialize(result)
                sys.stdout.write(output_bytes + b"\n")
