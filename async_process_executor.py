import datetime
import inspect
import logging
import multiprocessing
import queue
from asyncio import get_event_loop, sleep

DEFAULT_LOG_LEVEL = "ERROR"
logging.basicConfig(level=getattr(logging, DEFAULT_LOG_LEVEL))
logger = logging.getLogger(__name__)
# TODO: add details about the task instance in all logs


def minify(text, max_size):
    """
    >>> minify("this is a test", 99)
    "this is a test"
    >>> minify("this is a test", 13)
    "this is[...]"
    """

    if len(text) <= max_size:
        return text
    else:
        return text[: max_size - 5].strip() + "[...]"


def worker(return_dict, target, args, kwargs):
    try:
        result = target(*args, **kwargs)
    except Exception as exp:
        return_dict["status"] = "exception"
        return_dict["exception"] = exp
    else:
        return_dict["status"] = "ok"
        return_dict["result"] = result


def worker_generator(result_queue, target, args, kwargs):
    try:
        result = target(*args, **kwargs)
        for item in result:
            result_queue.put_nowait((False, item))
    except Exception as exp:
        result_queue.put(("error", exp))
    finally:
        result_queue.put((True, None))


class Task:
    def __init__(self, function, args=None, kwargs=None):
        self.function = function
        self.args = args or []
        self.kwargs = kwargs or {}
        self._manager = None
        self._process = None
        self._result_dict = None
        self._start_time = None
        self._result_container = None
        self._generator_finished = False

    def __repr__(self):
        args = minify(str(self.args), 50)
        kwargs = minify(str(self.kwargs), 50)
        return f"Task(function={self.function}, args={args}, kwargs={kwargs})"

    @property
    def is_generator(self):
        return inspect.isgeneratorfunction(self.function)

    @property
    def worker_function(self):
        if self.is_generator:
            return worker_generator
        else:
            return worker

    @property
    def _result_data(self):
        if self._result_container is None:
            if self.is_generator:
                self._result_container = self._manager.Queue()
            else:
                self._result_container = self._manager.dict()
        return self._result_container

    def start(self):
        self._manager = multiprocessing.Manager()
        self._process = multiprocessing.Process(
            target=self.worker_function,
            args=(self._result_data, self.function, self.args, self.kwargs),
        )
        self._process.start()
        self._start_time = datetime.datetime.now()

    def _get_item(self):
        try:
            finished, item = self._result_container.get_nowait()
        except queue.Empty:
            return (None, None)
        else:
            self._generator_finished = finished in ("error", True)
            if finished == "error":
                self._error = item
            return (finished, item)
            # TODO: move this method to self.result?

    @property
    def started(self):
        return self._start_time is not None

    @property
    def has_finished(self):
        if self.is_generator:
            return self._generator_finished
        else:
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
        if not self.has_finished:
            raise RuntimeError("Task not finished")
        else:
            # TODO: what if is generator?
            return self._result_data["status"]

    @property
    def exception(self):
        if not self.has_finished:
            raise RuntimeError("Task not finished")
        else:
            try:
                # TODO: what if is generator?
                return self._result_data["exception"]
            except KeyError:
                raise RuntimeError("Task has no exception")

    @property
    def result(self):
        if not self.has_finished:
            raise RuntimeError("Task not finished")
        else:
            try:
                # TODO: what if is generator?
                return self._result_data["result"]
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

    async def finished(self, task):
        """Method called when task finishes"""
        pass

    async def close(self):
        pass

    async def execute(self):
        finished, has_more_tasks = False, True
        tasks_async_generator = self.tasks().__aiter__()
        tasks = []
        while not finished:

            # Check if some of the tasks finished or timed out
            for task in tasks:
                if task.is_generator:
                    # TODO: implement max working time for generator tasks
                    finished, item = task._get_item()  # TODO: move to next(task)?
                    if finished is None:  # Nothing yielded by worker function
                        continue
                    elif finished is False:
                        logger.info(f"Task {task} got new item")
                        logger.debug(f"  Item: {minify(str(item), 100)}")
                        await self.process(item)
                    elif finished == "error":
                        logger.error(f"Exception on Task: {item}")
                    elif finished is True:
                        logger.info(f"Task finished: {task}")
                        tasks.remove(task)
                        await self.finished(task)

                else:  # Process regular tasks
                    if task.has_finished:
                        if task.status == "exception":
                            logger.error(f"Exception on Task: {task.exception}")
                            task.kill()
                            task.start()  # Try again
                            # TODO: implement max number of retries
                            # TODO: implement way to call error callback
                            # XXX: should add this task to the end of task list?

                        elif task.status == "ok":
                            logger.info(f"Task finished: {task}")
                            logger.debug(f"  Result: {minify(str(task.result), 100)}")
                            await self.process(task.result)
                            tasks.remove(task)
                            await self.finished(task)

                        else:
                            raise RuntimeError("Task status not implemented")

                    elif (
                        self.max_working_time is not None
                        and task.working_time >= self.max_working_time
                    ):
                        logger.error(f"Killing task running for {task.working_time}")
                        task.kill()
                        task.start()
                        # TODO: add max_working_time per task also
                        # TODO: implement max number of retries
                        # XXX: should add this task to the end of task list?

            for _ in range(self.workers - len(tasks)):
                try:
                    task = await tasks_async_generator.__anext__()
                except StopAsyncIteration:
                    logger.debug(f"All tasks finished")
                    has_more_tasks = False
                else:
                    logger.debug(f"Starting new task: {task}")
                    tasks.append(task)
                    task.start()

            finished = not has_more_tasks and len(tasks) == 0
            await sleep(self.sleep_time)

        await self.close()
