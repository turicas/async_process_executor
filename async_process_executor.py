import datetime
import multiprocessing
from asyncio import get_event_loop, sleep
from collections import namedtuple
from multiprocessing import Pool, cpu_count


class Task:

    def __init__(self, function, args=None, kwargs=None):
        self.function = function
        self.args = args or []
        self.kwargs = kwargs or {}
        self._manager = None
        self._process = None
        self._result_dict = None
        self._start_time = None

    def start(self):
        self._manager = multiprocessing.Manager()
        self._result_dict = self._manager.dict()
        self._process = multiprocessing.Process(
            target=worker,
            args=(self._result_dict, self.function, self.args, self.kwargs),
        )
        self._process.start()
        self._start_time = datetime.datetime.now()

    @property
    def started(self):
        return self._start_time is not None

    @property
    def finished(self):
        return not self._process.is_alive()

    def kill(self):
        if not self.started:
            raise RuntimeError("Task not started")
        else:
            self._process.kill()

    @property
    def working_time(self):
        if not self.started:
            raise RuntimeError("Task not started")
        else:
            return datetime.datetime.now() - self._start_time

    @property
    def status(self):
        if not self.finished:
            raise RuntimeError("Task not finished")
        else:
            return self._result_dict["status"]

    @property
    def exception(self):
        if not self.finished:
            raise RuntimeError("Task not finished")
        else:
            try:
                return self._result_dict["exception"]
            except KeyError:
                raise RuntimeError("Task has no exception")

    @property
    def result(self):
        if not self.finished:
            raise RuntimeError("Task not finished")
        else:
            try:
                return self._result_dict["result"]
            except KeyError:
                raise RuntimeError("Task has no result")


class AsyncProcessExecutor:
    """
    Execute tasks in processes and return its results asynchonously
    """

    def __init__(self, workers=cpu_count(), initializer=None, sleep_time=0.01):
        self.workers = workers
        self.initializer = initializer
        self.sleep_time = sleep_time

    def run(self):
        loop = get_event_loop()
        loop.run_until_complete(self.execute())

    async def tasks(self):
        raise NotImplementedError(
            "Method tasks of AsyncProcessExecutor must be overwritten"
        )

    async def process(self, result):
        raise NotImplementedError(
            "Method tasks of AsyncProcessExecutor must be overwritten"
        )

    async def close(self):
        pass

    async def execute(self):
        finished, has_more_tasks = False, True
        tasks = self.tasks()
        tasks_async_generator = tasks.__aiter__()

        with Pool(self.workers, initializer=self.initializer) as pool:
            results = []
            while not finished:
                for result in results:
                    if result.ready():
                        await self.process(result.get())
                        results.remove(result)
                for _ in range(self.workers - len(results)):
                    try:
                        task = await tasks_async_generator.__anext__()
                    except StopAsyncIteration:
                        has_more_tasks = False
                    else:
                        results.append(pool.apply_async(task.function, task.args))
                if not has_more_tasks and not results:
                    finished = True
                await sleep(self.sleep_time)
        await self.close()
