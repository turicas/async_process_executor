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

    async def finished(self, task):
        # You don't need to implement this method - it's called just after
        # `self.process` is called. You must implement `self.process` to
        # process task's result.
        print(f"#{task} finished")


if __name__ == "__main__":
    workers = 4
    print(f"Starting async task executor with {workers} workers.")
    MyTaskExecutor(workers=workers, max_working_time=5).run()
