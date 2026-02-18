import os
import socket
from time import time
import traceback
from . import common as cm


TASK, INIT = cm.TASK, cm.INIT
LOGFREQ = 0


# This loop is executed by the worker process...
def worker_loop(input, output, calls):
    counter = total_time = read_time = calc_time = send_time = msg_size = 0
    while True:
        try:
            when_start = time()
            msg, success = cm.read_msg(input)
            when_read = time()
            if not success:
                return
            raw_idx, _, target, args = cm.decompose_msg(msg)
            func = calls[target]
            try:
                res = func(*map(cm.loadb, args))
            except Exception as e:
                res.cm.compose_msg(raw_idx, cm.RAISE, target, (cm.serialize_exc(e),))
            else:
                res = cm.compose_msg(raw_idx, cm.RETURN, target, (cm.dumpb(res),))
            when_calced = time()
            cm.send_msg(res, output)
            when_done = time()
            total_time += when_done - when_start
            read_time += when_read - when_start
            calc_time += when_calced - when_read
            send_time += when_done - when_calced
            msg_size += success
            counter += 1
            if LOGFREQ and counter % LOGFREQ == 0:
                logger.info(f'size total read calc send (per call): '
                    f'{msg_size / LOGFREQ} '
                    f'{total_time / LOGFREQ * 1000} ms '
                    f'{round(read_time / total_time * 100, 2)}% '
                    f'{round(calc_time / total_time * 100, 2)}% '
                    f'{round(send_time / total_time * 100, 2)}% '
                    )
                total_time = read_time = calc_time = send_time = msg_size = 0
        except Exception:
            logger.error(traceback.format_exc())


def run_socket(loop, suffix=''):
    '''
    :param loop: a function taking two arguments: input: readable binary stream, output, writable binary stream
    :param suffix: suffix for forming socket name
    '''
    socket_file = cm.SOCKET_FILE.format(pid=os.getpid(), suffix=suffix)
    try:
        os.unlink(socket_file)
    except OSError:
        if os.path.exists(socket_file):
            raise
    os.makedirs(os.path.dirname(socket_file), exist_ok=True)
    
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        try:
            s.bind(socket_file)
            s.listen(1)
            logger.info(f'Listening on file {socket_file}')
            while True:
                conn, addr = s.accept()
                with conn:
                    logger.info(f'Connected by {addr}')
                    input = output = conn.makefile('rwb')
                    loop(input, output)
        except Exception as error:
            logger.error(error)
        finally:
            os.unlink(socket_file)


# Use this to create a task-processing server to be used as a worker...
def run(task_processor=None, initializer=None, calls=None, suffix=''):
    if calls is None:
        calls = {}
    if task_processor:
        calls[TASK] = task_processor
    if initializer:
        calls[INIT] = initializer
    loop = lambda i, o: worker_loop(i, o, calls)
    run_socket(loop, suffix=suffix)


logger = cm.Logger(__file__)
