import os
from pickle import loads as loadb, dumps as dumpb
from datetime import datetime
import __main__


SOCKET_FILE = '/tmp/socket_{pid}_{suffix}'
BYTE_ORDER = 'little'
INTSIZE = 8
INIT = b'initialize'
TASK = b'do_task'

encode_int = lambda x: int.to_bytes(x, INTSIZE, BYTE_ORDER)
decode_int = lambda y, i: int.from_bytes(y[i: i + INTSIZE], BYTE_ORDER)

CALL = encode_int(0)
RETURN = encode_int(1)
RAISE = encode_int(2)
serialize_exc = lambda e: str(e).encode()
deserialize_exc = lambda b: Exception(b.decode())


def send_msg(raw_msg: bytes, out_stream):
    out_stream.write(encode_int(len(raw_msg)) + raw_msg)
    out_stream.flush()


def read_msg(in_stream):
    header = in_stream.read(INTSIZE)
    if not header:
        logger.info(f'{in_stream} is closed')
        return None, None
    elif len(header) < INTSIZE:
        raise Exception(f'Reading: header is too small: size {len(header)}')
    msg_size = decode_int(header, 0)
    raw_msg = in_stream.read(msg_size)
    return raw_msg, INTSIZE + msg_size  # or just anything that has a boolean value of True


def encode_iterable(args, prefix=b''):
    for arg in args:
        prefix += encode_int(len(arg)) + arg
    return prefix


def decode_iterable(msg, i=0):
    args = []
    while i < len(msg):
        j = i + INTSIZE + decode_int(msg, i)
        args.append(msg[i + INTSIZE: j])
        i = j
    return tuple(args)


def decompose_msg(msg: bytes):
    target_len = decode_int(msg, 2 * INTSIZE)
    # target, raw_idx, args
    return msg[:INTSIZE], msg[INTSIZE: 2 * INTSIZE], msg[3 * INTSIZE: 3 * INTSIZE + target_len], \
        decode_iterable(msg, 3 * INTSIZE + target_len)
    

def compose_msg(raw_idx: bytes, kind: bytes, target: bytes, args: list):
    return encode_iterable(args, raw_idx + kind + encode_int(len(target)) + target)


class Logger:
    try:
        root_name = os.path.splitext(os.path.basename(__main__.__file__))[0]
    except:
        root_name = ''
    
    def __init__(self, module_path):
        self.module_name = os.path.splitext(os.path.basename(module_path))[0]
        self.error = self._vocal('ERROR')
        self.warning = self._vocal('WARNING')
        self.info = self._vocal('INFO')
        self.debug = self._silent

    def _silent(self, *args, **kwargs): 
        return
    
    def _vocal(self, mode):
        def f(*args, **kwargs):
            return print(f'{self.root_name} {self.module_name} {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} {os.getpid()} {mode}:',  *args, **kwargs)
        return f
    
    
logger = Logger(__file__)
