import os
from subprocess import Popen
from threading import Thread, Event, Lock, Condition
from queue import Queue
from time import time, sleep
from datetime import datetime
from math import ceil
import random
import socket
import traceback
from abc import ABC
from . import common as cm


MAIN_PROCESS = f'PID {os.getpid()}'
INSTANTIATED = 'INSTANTIATED'
CONNECTED = 'CONNECTED'
READY_FOR_TASKS = 'READY_FOR_TASKS'
KILLED = 'KILLED'
MIN_WORKERS = 5
SANE_WORKERS = os.cpu_count() or 12
MAX_WORKERS = max(25, (3 * SANE_WORKERS) // 2)
SPEED_ADJ = 100
MIN_TIME_STEP = 5
# True is for the old version
SINGLETON_TASK = True
TASK, INIT = cm.TASK, cm.INIT


min_sec = lambda sec: f'{sec // 60 :.0f} min {sec % 60 :.0f} s'


def get_socket(socket_file):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(socket_file)
    sf = s.makefile('rwb')
    return s, sf


class RemoteException(Exception): ...


class AbstractWorker(ABC):
    def connect(self): ...

    def initialize(self, init_data): ...
    
    def do_task(self, task): ...

    def has_quit(self) -> bool: ...


class HouseWorker(AbstractWorker):
    '''
    Runs in the main client process.
    '''
    def __init__(self, calls, name='House Worker'):
        '''
        :param calls: {INIT: callable, TASK: callable, ...}
        '''
        self.calls = calls
        self.name = name

    def initialize(self, init_data):
        return self.calls[INIT](*init_data)
        
    def do_task(self, task, target=TASK):
        if SINGLETON_TASK:
            return task[0], self.calls[target](task[1])
        else:
            return task[0], self.calls[target](*task[1])

    def has_quit(self):
        return False


class Socket(AbstractWorker):
    def __init__(self, pid, suffix=''):
        '''
        A client socket for connecting to an existing worker
        :param pid: pid of the worker
        '''
        self.pid = pid
        self.name = f'pid {self.pid}'
        self.in_stream = None
        self.out_stream = None
        self.socket_file = cm.SOCKET_FILE.format(pid=self.pid, suffix=suffix)
        self.status = INSTANTIATED

    def attempt_connect(self):
        self.socket, io_stream = get_socket(self.socket_file)
        self.in_stream = io_stream
        self.out_stream = io_stream

    def connect(self):
        while True:
            try:
                self.attempt_connect()
                break
            except:
                sleep(1)
        self.status = CONNECTED

    def send_msg(self, raw_msg: bytes):
        return cm.send_msg(raw_msg, self.out_stream)
    
    def read_msg(self) -> bytes:
        return cm.read_msg(self.in_stream)
    
    def initialize(self, init_data):
        raw_idx = cm.encode_int(0)
        msg = cm.compose_msg(raw_idx, cm.CALL, INIT, map(cm.dumpb, init_data))
        self.send_msg(msg)
        logger.debug(f'Sent init data')
        msg, success = self.read_msg()
        logger.debug('Received init response')
        if success:
            ret_raw_idx, _, target, _ = cm.decompose_msg(msg)
            if target == INIT and ret_raw_idx == raw_idx:
                self.status = READY_FOR_TASKS
                logger.info(f'{self.name} is ready for tasks')
                return True
            else:
                logger.error(f'{self.name} failed to set up')
                logger.error(f'{self.name} init response:  {msg}')
                return False
        else:
            logger.info(f'{self.name} is dead')
            return
    
    def send_task(self, idx_args, target=TASK):
        '''
        Sumbits task to the worker
        :param idx_args: (index, args)
        '''
        if SINGLETON_TASK:
            self.send_msg(cm.compose_msg(cm.encode_int(idx_args[0]), cm.CALL, target, (cm.dumpb(idx_args[1]),)))
        else: 
            self.send_msg(cm.compose_msg(cm.encode_int(idx_args[0]), cm.CALL, map(cm.dumpb, idx_args[1])))

    def get_task_result(self):
        msg, success = self.read_msg()
        assert success
        raw_idx, outcome, _, res = cm.decompose_msg(msg)
        if outcome == cm.RETURN:
            return cm.decode_int(raw_idx, 0), cm.loadb(res[0])
        elif outcome == cm.RAISE:
            error = cm.deserialize_exception(res[0])
            error.idx = cm.decode_int(raw_idx, 0)
            raise error
        
    def do_task(self, idx_args, target=TASK):
        '''
        Submits task to the worker and returns the result.
        :param idx_args: (index, args)
        '''
        self.send_task(idx_args, target=target)
        return self.get_task_result()

    def has_quit(self):
        try:
            os.kill(int(self.pid), 0)
            return True
        except OSError:
            return False


class Worker(Socket):
    '''
    A socket which results after starting a process by a given command.
    '''
    def __init__(self, command, suffix=''):
        self.command = command
        self.process = Popen(command)
        super().__init__(self.process.pid, suffix=suffix)
        logger.info(f'Worker launched, command: {command}, '
            f"id = {self.process.pid}, socket file: {self.socket_file}")

    def initialize(self, init_data):
        ret_val = super().initialize(init_data)
        if not ret_val:
            self.status = KILLED
            self.kill()
        return ret_val

    def has_quit(self):
        return self.process.poll() is not None
    
    def kill(self):
        logger.info(f'Removing socket file {self.socket_file}')
        try:
            os.unlink(self.socket_file)
            logger.info(f'Removed {self.socket_file}')
        except:
            pass
        logger.info(f'Killing process {self.process.pid}')
        self.process.kill()
        logger.info(f'Killed {self.process.pid}.')


class Empty(Exception):
    pass


class DLLNode:
    def __init__(self, data):
        self.pred = None
        self.succ = None
        self.data = data


class DoublyLinkedList:
    def __init__(self):
        self.head = None
        self.tail = None

    def append(self, new_node):
        if self.head is None:
            self.head = new_node
        else:     
            self.tail.succ = new_node
            new_node.pred = self.tail
        self.tail = new_node

    def discard(self, node):
        if node.succ is None:
            if node.pred is None:
                if self.head is node:
                    self.head = self.tail = None
                else:
                    return 
            else:
                assert node.pred.succ is node
                node.pred.succ = None
                self.tail = node.pred
        else:
            assert node.succ.pred is node
            if node.pred is None:
                node.succ.pred = None
                self.head = node.succ
            else:
                assert node.pred.succ is node
                node.pred.succ = node.succ
                node.succ.pred = node.pred
        node.succ = node.pred = None
        return 1

    def __contains__(self, node):
        return self.head is node if node.pred is None else node.pred.succ is node


class Node:
    def __init__(self, task, requestor='', labels=None):
        self.task = task
        labels = (requestor, '*') if labels is None else labels
        self.structure = tuple((label, DLLNode(self)) for label in labels)


class Multiqueue:
    '''
    Multiple queues with same elements.
    Delete elements in O(1) time.
    '''
    def __init__(self):
        self.queue_dict = {}
        self.qsize = 0

    def __len__(self):
        return self.qsize

    def append(self, new_node):
        for label, dll_node in new_node.structure:
            self.queue_dict.setdefault(label, DoublyLinkedList()).append(dll_node)
        self.qsize += 1

    def __contains__(self, node):
        return (node.structure and node.structure[0][0] in self.queue_dict 
                and node.structure[0][1] in self.queue_dict[node.structure[0][0]])

    def discard(self, node):
        popped = False
        for label, dll_node in node.structure:
            if label in self.queue_dict:
                popped = self.queue_dict.discard(dll_node)
        self.qsize -= popped
            
    def peekleft(self, requestor='', labels=None):
        for label in (requestor, '*') if labels is None else labels:
            if label in self.queue_dict and self.queue_dict[label].head is not None:
                return self.queue_dict[label].head.data
        raise Empty()

    def popleft(self, requestor=''):
        node = self.peekleft(requestor=requestor)
        self.discard(node)
        return node


class Sprint:
    '''
    Like a jira sprint
    '''
    def __init__(self, init_data=None, with_inds=False):
        self.mutex = Lock()
        self.not_empty = Condition(self.mutex)
        self.init_data = init_data
        self.tasks = Multiqueue()
        self.num_submitted_tasks = 0
        self.nums_submitted_tasks = {}
        self.nums_picked_res = {}
        self.in_progress = dict()
        self.out_queues = {}
        self.open = True
        self.is_done_event = Event() # the batch is closed and all tasks are completed
        self.result_count = 0
        self.ind_lookup = {} if with_inds else None
        self.requestors = {}
        self.log = Queue()
        self.recycle = {} # {requestor: True|False}

    def submit_tasks(self, tasks, requestor='', labels=None) -> int:
        '''
        Submit an ask for execution
        :param tasks:
        :return: submission sequence number
        '''
        with self.mutex:
            for task in tasks:
                seq_num = self.num_submitted_tasks
                if self.ind_lookup is not None:
                    name, task = task
                    self.ind_lookup[seq_num] = name
                self.requestors[seq_num] = requestor
                task = (seq_num, task)
                self.num_submitted_tasks += 1
                self.nums_submitted_tasks[requestor] = self.nums_submitted_tasks.setdefault(requestor, 0) + 1 
                self.tasks.append(Node(task, requestor=requestor, labels=labels))
            self.not_empty.notify(len(tasks))
            return self.num_submitted_tasks
    
    def submit_task(self, task, requestor='', labels=None) -> int:
        return self.submit_tasks([task], requestor=requestor, labels=labels)
    
    def get_task(self, requestor='', labels=None):
        '''
        :return: the next task in the queue so as to execute it
        '''
        if labels is None:
            labels = (requestor, '*')
        with self.mutex:
            while True:
                try:
                    node = self.tasks.popleft(labels=labels)
                except Empty:
                    if self.open:
                        self.not_empty.wait()
                    else:
                        raise Empty
                else:
                    task_to_grab = node.task
                    idx = task_to_grab[0]
                    # idx might already be there...
                    self.in_progress[idx] = node
                    # A batch starts out with recycle set to True 
                    if self.recycle.setdefault(self.requestors[idx], True):
                        self.tasks.append(node) # let another worker pick it up...
                        self.not_empty.notify()
                    return task_to_grab
 
    def submit_result(self, ind: int, result, worker_name):
        '''
        :param ind: the sequence number of the task whose result is being submitted
        :param result: the result of execution of the task
        '''
        time_now = time()
        with self.mutex:
            if ind in self.in_progress:
                self.tasks.discard(self.in_progress[ind])
                del self.in_progress[ind]
                requestor = self.requestors[ind]
                self.out_queues.setdefault(requestor, Queue()).put((ind, result))
                self.result_count += 1
            # To log arrival stats...
            self.log.put([ind, time_now, worker_name])
            if not self.open and self.num_submitted_tasks == self.result_count:
                self.is_done_event.set()

    def yield_results(self, requestor=''):
        '''
        Not to be called by multiple threads
        :param requestor: the  name of the requestor
        '''
        if requestor not in self.nums_picked_res:
            self.nums_picked_res[requestor] = 0
        out_queue = self.out_queues.setdefault(requestor, Queue())
        logger.debug(f'started iterating over results submitted by "{requestor}"...')
        while self.nums_picked_res[requestor] < self.nums_submitted_tasks[requestor]:
            resp = out_queue.get()
            idx = resp[0]
            if self.ind_lookup is not None:
                resp = (self.ind_lookup.pop(idx), resp[1])
            self.requestors.pop(idx)
            self.nums_picked_res[requestor] += 1
            yield resp

    def is_done(self):
        return self.is_done_event.is_set()
    
    def close(self):
        '''
        Once the batch is closed, it is not to accept any more asks but can continue to
        work on already submitted ones.
        '''
        if self.open:
            self.open = False
            self.set_recycle(True, all=True)
            with self.mutex:
                self.not_empty.notify_all()

    def set_recycle(self, val: bool, all=False, requestor=''):
        with self.mutex:
            if all:
                for r, v in self.recycle:
                    self.recycle[r] = val
            else:
                self.recycle[requestor] = val
            if val:
                count = 0
                for node in self.in_progress.values():
                    if node not in self.tasks and (all or 
                        node.task[0] in self.requestors and self.requestors[node.task[0]] == requestor):
                        self.tasks.append(node)
                        count += 1
                self.not_empty.notify(count)

            
class SprintMan:
    '''
    A worker equipped with a queue for sprints to work on and a thread for working on them.
    '''
    def __init__(self, worker, requestor='', labels=None):
        self.sprints = Queue()
        self.actual = worker
        self.thread = Thread(target=self.worker_loop, daemon=True)
        self.sprint_status = (None, INSTANTIATED)
        self.stop_order = False
        self.labels = (requestor, '*') if labels is None else labels

    def work_on_sprint(self):
        sprint = self.sprint_status[0]
        if sprint.init_data is not None:
            self.actual.initialize(sprint.init_data)
        self.sprint_status = (sprint, READY_FOR_TASKS)
        while True:
            if self.stop_order:
                self.stop_order = False
                break
            try:
                task = sprint.get_task(labels=self.labels)
            except Empty:
                break
            try:
                idx = task[0]
                ret_idx, res = self.actual.do_task(task)
                assert ret_idx == idx
            except Exception as error:
                logger.warning(f'{self.actual.name} failed to process task # {idx}')
                logger.warning(f'with error: {error}')
                if self.actual.has_quit():
                    logger.info(f'{self.actual.name} has quit')
                    return                  
            else:
                sprint.submit_result(idx, res, self.actual.name)

    def worker_loop(self):
        self.actual.connect()
        while True:
            self.sprint_status = (None, CONNECTED)
            sprint = self.sprints.get()
            if sprint == 'return':
                break
            self.sprint_status = (sprint, CONNECTED)
            try:
                self.work_on_sprint()
            except Exception:
                logger.error(traceback.format_exc())
                return
    

class Batch(Sprint):
    def __init__(self, tasks, init_data=None, shuffle=False):
        super().__init__(init_data=init_data, with_inds=shuffle)
        if shuffle:
            # Randomize the order of tasks to better estimate speed of processing...
            tasks = random.sample(list(enumerate(tasks)), len(tasks))
        self.submit_tasks(tasks)
        self.close()

    def list_results(self, requestor=''):
        res_pairs = list(self.yield_results(requestor))
        results = [None] * len(res_pairs)
        for idx, res in res_pairs:
            results[idx] = res
        return results


class Pool:
    '''
    A pool of workers
    '''
    def __init__(self, command, num_workers=None, calls=None, task_processor=None, initializer=None, suffix=''):
        self.suffix = suffix
        if num_workers is None:
            num_workers = SANE_WORKERS
        self.command = command
        logger.info(f'Worker command: {self.command}')
        self.workers = [] # it is useful to retain the order in which workers were created...
        # for backward compatibility as well as convenience
        if calls is None:
            calls = {}
        if task_processor is not None:
            calls[TASK] = task_processor
        if initializer is not None:
            calls[INIT] = initializer
        if calls:
            self.house_worker = SprintMan(HouseWorker(calls))
            self.house_worker.thread.start()
        else:
            self.house_worker = None
        self.add_workers(num_workers)
        self.sprint_to_workers = dict()
        
    def assign_worker_to_sprint(self, worker, batch):
        # TODO 
        self.sprint_to_workers.setdefault(batch, set()).add(worker)
        worker.sprints.put(batch)

    def add_workers(self, num_workers):
        added_workers = set()
        while len(added_workers) < num_workers:
            command = self.command
            try:
                worker = SprintMan(Worker(command, suffix=self.suffix))
            except Exception:
                logger.warning(f'Failed to launch a worker.  Command: {command}')
                logger.error(traceback.format_exc())
                continue      
            worker.thread.start()        
            self.workers.append(worker)
            logger.info(f'Appended worker {worker.actual.name} to the worker list')
            added_workers.add(worker)      
        return added_workers
    
    def add_workers_to_sprint(self, num_workers, batch):
        '''
        Adds existent workers to the batch, and if that is not enough, spin up some new workers
        '''
        workers_to_add = set()
        batch_workers = self.sprint_to_workers.setdefault(batch, set())
        for worker in self.workers:
            if len(workers_to_add) == num_workers:
                break
            if worker not in batch_workers:
                workers_to_add.add(worker)
        workers_to_add.update(self.add_workers(num_workers - len(workers_to_add)))
        for worker in workers_to_add:
            self.assign_worker_to_sprint(worker, batch)

    def submit_sprint(self, batch, num_workers=SANE_WORKERS):
        '''
        '''
        self.add_workers_to_sprint(num_workers, batch)
        if self.house_worker:
            self.house_worker.sprints.put(batch)

    def submit_batch(self, batch, num_workers=SANE_WORKERS):
        '''
        Synonym to submit_sprint, for back-compatibility
        '''
        return self.submit_sprint(batch, num_workers)

    def remove_dead_workers(self, batch):
        alive_workers = []
        for worker in self.workers:
            if worker.actual.has_quit():
                self.sprint_to_workers[batch].discard(worker)
            else:
                alive_workers.append(worker)
        self.workers = alive_workers
            
    def close(self):
        for w in self.workers:
            w.actual.kill()
        logger.info('Worker Pool is closed')          

    def __enter__(self):
        return self
    
    def __exit__(self, type, value, tb):
        self.close()


def print_contributions(pool, batch, contributions):
    is_done = batch.is_done()
    while not batch.log.empty():
        worker_name = batch.log.get()[2]
        contributions[worker_name] = contributions.get(worker_name, 0) + 1
    logger.info('Worker Contributions by task counts and percent:')
    worker_stati = {pool.house_worker.actual.name: None} if pool.house_worker else {}
    for worker in pool.workers:
        worker_stati[worker.actual.name] = worker.actual.status

    for worker_name, status in worker_stati.items():
        contribution = contributions.get(worker_name, 0)
        if is_done:
            logger.info(f'Worker: {worker_name}, status: {status}, contrib: {contribution}, {round(contribution / batch.num_submitted_tasks * 100, 1)}%')
        else:
            logger.info(f'Worker: {worker_name}, status: {status}, contrib: {contribution}')
    if is_done:
        logger.info(f'Total contributions = {sum(contributions.values())}, total tasks = {batch.num_submitted_tasks}')


class BatchManager:
    '''
    This class deploys new workers if and when calculation runs late.
    '''
    @staticmethod
    def trim_workers(num_workers):
        num_workers = min(num_workers, MAX_WORKERS)
        num_workers = max(num_workers, MIN_WORKERS)
        return num_workers
    
    @staticmethod
    def send_progress_update(speed, avg_speed, time_remains, tasks_remain, num_submitted_tasks):
        desired_speed = tasks_remain / time_remains
        speed_ = speed * 60
        avg_speed_ = avg_speed * 60
        desired_speed_ = desired_speed * 60
        # Based on current speed...
        exp_delay = tasks_remain / speed - time_remains
        # Based on average speed...
        overall_exp_delay = tasks_remain / avg_speed - time_remains
        delays_str = f'{exp_delay / 60 :.2f} / {overall_exp_delay / 60:.2f} min'
        eta_str = min_sec(tasks_remain / speed)
        arrival_update = f'(Current/Avg/Desired) speeds: {speed_} / {avg_speed_} / {desired_speed_}, delays: {delays_str}, remains {tasks_remain} / {num_submitted_tasks}, should be finished in {eta_str}'
        logger.info(arrival_update)

    def __init__(self, pool, asks, init_data=None):
        self.active_workers = set()
        self.pending_workers = set()
        self.contributions = {}
        self.start_time = None
        self.tta = None
        self.main_thread = None
        self.batch = Batch(asks, init_data=init_data, shuffle=True)
        self.pool = pool

    def survey_workers(self):
        active_workers = set()
        pending_workers = set()
        self.pool.remove_dead_workers(self.batch)
        for worker in self.pool.sprint_to_workers[self.batch]:
            if worker.sprint_status == (self.batch, READY_FOR_TASKS):
                active_workers.add(worker)
            else:
                pending_workers.add(worker)
        self.active_workers = active_workers
        self.pending_workers = pending_workers
 
    def set_workers(self, num_workers):
        self.survey_workers()
        logger.info(f'Workers: current total: {len(self.pool.workers)}, active: {len(self.active_workers)}, pending: {len(self.pending_workers)}, '
            f'desired: {num_workers}')
        num_workers_to_add = max(0, num_workers - len(self.active_workers) - len(self.pending_workers))
        logger.info(f'Adding {num_workers_to_add} workers')
        self.pool.add_workers_to_sprint(num_workers_to_add, self.batch)
        self.survey_workers() # to update the sets accessed in the next line...  
        logger.info(f'Workers: current total: {len(self.pool.workers)}, active: {len(self.active_workers)}, pending: {len(self.pending_workers)}')

    def batch_manager(self, num_workers=None, tasks_per_worker=None, tta=None, speed_adj=SPEED_ADJ):
        self.start_time, self.tta = time(), tta
        batch = self.batch
        try:            
            if not batch.num_submitted_tasks:
                batch.is_done_event.set()
                return
            num_workers = num_workers or (batch.num_submitted_tasks // tasks_per_worker if tasks_per_worker else SANE_WORKERS)
            num_workers = BatchManager.trim_workers(num_workers)
            logger.info(f'Starting with  {batch.num_submitted_tasks} tasks and {num_workers} workers.')
            self.pool.submit_batch(batch, num_workers)
            if speed_adj and tta is not None:
                logger.info(f'Target time of completion = {datetime.fromtimestamp(tta)}')
                speed_adj = min(speed_adj, len(batch.tasks))
                self.time_step = (self.tta - self.start_time) // (speed_adj or 1)
                self.time_step = max(self.time_step, MIN_TIME_STEP)
                # Start a progress-monitoring thread...
                self.speed_adjustment(gradual=False)
            batch.is_done_event.wait()
            logger.info(f'Done.  Target time of completion: {datetime.fromtimestamp(self.tta) if self.tta else None}.')
            print_contributions(self.pool, self.batch, self.contributions)
        finally:
            logger.info('batch_manager thread exited')

    def start(self, *args, **kwargs):
        self.main_thread = Thread(target=self.batch_manager, args=args, kwargs=kwargs, daemon=True)
        self.main_thread.start()

    def speed_adjustment(self, gradual=True):
        full_throttle = False
        prev_time = time()
        batch = self.batch
        prev_result_count = batch.result_count
        while True:
            # Wait for some new results to come in so we could estimate speed
            # while frequently checking for the done event...
            sleep(1)
            if batch.is_done():
                return
            if batch.result_count == prev_result_count or time() - prev_time <= self.time_step:
                continue
            self.survey_workers()
            num_active_workers = len(self.active_workers)
            time_now = time()
            result_count = batch.result_count
            elapsed_time = time_now - self.start_time
            time_remains = self.tta - time_now
            time_delta = time_now - prev_time
            tasks_remain = batch.num_submitted_tasks - result_count
            task_delta = result_count - prev_result_count
            speed = task_delta / time_delta
            avg_speed = result_count / elapsed_time
            desired_speed = tasks_remain / time_remains
            BatchManager.send_progress_update(speed, avg_speed, time_remains, tasks_remain, batch.num_submitted_tasks)
            desired_num_workers = num_active_workers
            if not full_throttle:
                full_throttle = time_remains <= 0
                if full_throttle:
                    desired_num_workers = BatchManager.trim_workers(num_active_workers + len(batch.tasks))
                    logger.info(f'Missed the target, blasting full throttle...')
                    self.set_workers(desired_num_workers)
                else:
                    if speed < desired_speed and avg_speed < desired_speed:
                        # it is time to get aggressive...
                        logger.info('We need to speed up...')
                        if gradual:
                            desired_num_workers = num_active_workers + 1 
                        else:
                            desired_num_workers = ceil((num_active_workers + 1) * desired_speed / speed) - 1
                            # ...to account for the house worker
                        desired_num_workers = BatchManager.trim_workers(desired_num_workers)
                        self.set_workers(desired_num_workers)
            logger.info(f'Numbers of workers: currently active: {num_active_workers}, desired: {desired_num_workers}')
            prev_time = time_now
            prev_result_count = result_count

    def close(self):
        if self.main_thread:
            self.main_thread.join()


logger = cm.Logger(__file__)
