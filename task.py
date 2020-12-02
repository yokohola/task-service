import threading
from uuid import uuid4
from datetime import datetime
from abc import ABC, abstractmethod
from collections import OrderedDict
from apscheduler.schedulers.background import BackgroundScheduler


FREE = 'free'
PROCESSING = 'processing'
COMPLETE = 'complete'
ERROR = 'error'

mutex = threading.Lock()


class Task:
    __statuses__ = (
        FREE,
        PROCESSING,
        COMPLETE,
        ERROR
    )

    def __init__(self, url):
        self.id = str(uuid4())
        self.url = url
        self.__status = FREE
        self.__result = None

    @property
    def status(self):
        return self.__status

    @status.setter
    def status(self, status):
        if status not in self.__statuses__:
            raise ValueError('Status is not allowed')
        if status == self.__status:
            raise ValueError('Status already set')
        self.__status = status

    @property
    def result(self):
        return self.__result

    def process(self):
        if self.__status != FREE and self.__status != ERROR:
            raise ValueError(f'Task `{self.id}` can not be processing again {self.__status}')
        self.__status = PROCESSING
        try:
            ...
            # write your code here for processing task.
        except Exception:  # noqa
            self.__status = ERROR
        else:
            self.__status = COMPLETE

    def json(self):
        return {
            'id': self.id,
            'url': self.url,
            'status': self.__status,
            'result': self.__result
        }


class ABCTaskManager(ABC):
    @abstractmethod
    def manage_tasks(self):
        raise NotImplementedError('Method must be implemented.')


class BaseTaskManager(ABCTaskManager):
    def __init__(self, delay_time=1):
        self.delay_time = delay_time
        self.chunk_size = 5  # default chunk size per thread
        self.job_id = 'base'
        self.tasks = OrderedDict()  # uuid: Task

    def manage_tasks(self):
        mutex.acquire()
        free_tasks = self.pop_chunk(self.chunk_size)
        if free_tasks:
            print(f'Start round at local: {datetime.now().strftime("%H:%M:%S")}')
        mutex.release()

        for task in free_tasks:  # `reversed` are too broad
            task.process()
            self.add_task(task, last=True)  # to the end
        self.after_hook()

    def pop_chunk(self, amount):
        assert amount > 0, 'Amount must be a positive integer.'

        if not len(self.tasks):
            return []

        values = iter(tuple(self.tasks.values()))
        free_tasks = []
        while amount:
            try:
                task = next(values)
                if task.status == FREE:
                    free_tasks.append(task)
                    self.tasks.pop(task.id)
                    amount -= 1
            except StopIteration:
                break
        return free_tasks

    def add_task(self, task, last=False):
        assert isinstance(task, Task), f'Expected type `Task` got `{type(task)}`'

        self.tasks[task.id] = task
        self.tasks.move_to_end(task.id, last=last)
        print(f'Task `{task.id}`has been created.')
        return task.id

    def get_task(self, uuid):
        return self.tasks.get(uuid)

    def pop_task(self, uuid, default=None):
        return self.tasks.pop(uuid, default)

    def check_status(self, uuid, safe=True):
        task = self.tasks.get(uuid)
        if not task and not safe:
            raise ValueError(f'No task with uuid: {uuid}')
        elif not task and safe:
            return None
        return task.status

    def add_scheduler_job(self, aps_scheduler: BackgroundScheduler):
        aps_scheduler.add_job(
            id=self.job_id,
            func=self.manage_tasks,
            trigger='interval',
            seconds=self.delay_time,
            max_instances=6,
            misfire_grace_time=3
        )

    def after_hook(self):
        pass


class TaskManager(BaseTaskManager):
    ...
