# async_process_executor

Provides a class to execute tasks in processes with asynchronous processing of
results. It consumes the tasks lazily (you can virtually have infinite tasks)
and calls the processing method as the results are done.

## Installation

Install directly from [Python Package Index](https://pypi.org/project/async-process-executor/):

```bash
pip install async_process_executor
```

## Usage

### Regular functions

You must inherit from `async_process_executor.AsyncProcessExecutor` and
implement `tasks` and `process` async methods, then call `YourClass().run()`,
like in `example.py`:

```python
from async_process_executor import AsyncProcessExecutor, Task


def my_task(task_number):
    import random
    import time

    sleep_time = random.randint(1, 100) / 10
    time.sleep(sleep_time)
    return task_number, sleep_time


class MyTaskExecutor(AsyncProcessExecutor):
    async def tasks(self):
        for task_number in range(20):
            yield Task(function=my_task, args=(task_number,))
            print(f"Task #{task_number} consumed")

    async def process(self, result):
        task_number, sleep_time = result
        print(f"Task #{task_number} completed (sleep_time={sleep_time})")


if __name__ == "__main__":
    workers = 4
    print(f"Starting async task executor with {workers} workers.")
    MyTaskExecutor(workers=workers).run()
```

You can also limit the amount of time a worker will work on the task (the
process will be killed and the task will be started again) by passing
`max_working_time`, in seconds:

```python
    MyTaskExecutor(workers=workers, max_working_time=7).run()
```


### Generators

Your worker function can be a generator as well. The data will be consumed from
the child processes and `YourClass.process` will be called for each yielded
item.

Check `example_generator.py`:

```python
from async_process_executor import AsyncProcessExecutor, Task


def my_task(task_number):
    import random
    import time

    for sleep_cycle in range(1, random.randint(1, 10)):
        sleep_time = random.randint(1, 100) / 100
        time.sleep(sleep_time)
        yield task_number, sleep_cycle, sleep_time


class MyTaskExecutor(AsyncProcessExecutor):
    async def tasks(self):
        for task_number in range(20):
            yield Task(function=my_task, args=(task_number,))
            print(f"Task #{task_number} consumed")

    async def process(self, result):
        task_number, sleep_cycle, sleep_time = result
        print(f"Task #{task_number} got item: sleep_cycle={sleep_cycle}, sleep_time={sleep_time}")

    async def finished(self, task):
        print(f"{task} finished")


if __name__ == "__main__":
    workers = 4
    print(f"Starting async task executor with {workers} workers.")
    MyTaskExecutor(workers=workers, max_working_time=5).run()
```

> Note: `max_working_time` is currently not implemented for generator tasks.


## To Do

- Create a CLI to execute tasks from CSV. Example:
  `asyncexec 'module.function' 'file.csv'` (will read rows and create tasks
  automatically).
- Create an way to save/continue task execution (identify which tasks were
  executed by uuid?).
