# event_loop_01_no_io.py

from collections import deque


class EventLoopNoIO:
    def __init__(self):
        self.tasks_to_run = deque([])

    def create_task(self, coro):
        self.tasks_to_run.append(coro)

    def run(self):
        while self.tasks_to_run:
            task = self.tasks_to_run.popleft()
            try:
                next(task)
            except StopIteration:
                continue
            self.create_task(task)