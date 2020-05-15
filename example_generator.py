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
    MyTaskExecutor(workers=workers).run()
