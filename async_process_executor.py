import datetime
import multiprocessing
from asyncio import get_event_loop, sleep
from collections import namedtuple


def worker(return_dict, target, args, kwargs):
    try:
        result = target(*args, **kwargs)
    except Exception as exp:
        return_dict["status"] = "exception"
        return_dict["exception"] = exp
    else:
        return_dict["status"] = "ok"
        return_dict["result"] = result


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

    def __init__(
        self,
        workers=multiprocessing.cpu_count(),
        initializer=None,
        sleep_time=0.01,
        max_working_time=None,
    ):
        self.workers = workers
        self.initializer = initializer
        self.sleep_time = sleep_time
        self.max_working_time = (
            datetime.timedelta(seconds=max_working_time)
            if max_working_time is not None
            else None
        )

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
        tasks_async_generator = self.tasks().__aiter__()
        tasks = []
        while not finished:

            # Check if some of the tasks finished or timed out
            for task in tasks:
                if task.finished:
                    if task.status == "exception":
                        # TODO: log
                        task.start()  # Try again
                        # TODO: should add this task to the end of task list?
                        # TODO: implement max number of retries

                    elif task.status == "ok":
                        # TODO: log
                        await self.process(task.result)
                        tasks.remove(task)

                    else:
                        raise RuntimeError("Task status not implemented")

                elif (
                    self.max_working_time is not None
                    and task.working_time >= self.max_working_time
                ):
                    task.kill()
                    task.start()
                    # TODO: add max_working_time per task also
                    # TODO: implement max number of retries
                    # TODO: should add this task to the end of task list?
                    # TODO: log

            for _ in range(self.workers - len(tasks)):
                try:
                    task = await tasks_async_generator.__anext__()
                    # TODO: log
                except StopAsyncIteration:
                    has_more_tasks = False
                    # TODO: log
                else:
                    tasks.append(task)
                    task.start()
                    # TODO: log

            finished = not has_more_tasks and len(tasks) == 0
            await sleep(self.sleep_time)

        await self.close()
