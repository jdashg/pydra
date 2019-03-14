#!/usr/bin/env python3
assert __name__ != '__main__'

import net_utils as nu

import logging
import os
import pathlib
import platform
import socket
import struct
import subprocess
import sys
import threading
import time
import traceback

# --

SEMVER_MAJOR = 3
nu.PacketConn.MAGIC += nu.pack_t(nu.U32_T, SEMVER_MAJOR)

PYDRA_HOME = pathlib.Path.home() / '.pydra'
CONFIG_PATH = PYDRA_HOME / 'config.py'

# --

print_lock = threading.Lock()
def locked_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)
        sys.stdout.flush()

# --

class LockingLogHandler(logging.Handler):
    def emit(self, record):
        text = self.format(record)
        locked_print(text)

    @staticmethod
    def install():
        logger = logging.getLogger()
        logger.addHandler(LockingLogHandler())

# --

DEFAULT_CONFIG = {
    'JOB_SERVER_ADDR': ('', 38520),
    'LOG_ADDR': ('localhost', 38521),
    'WORKER_BASE_ADDR': (socket.gethostname(), 38522),
    'WORKERS': os.cpu_count(),
    'HOSTNAME': socket.gethostname(),
    'TIMEOUT_CLIENT_TO_SERVER': 0.300,
    'TIMEOUT_WORKER_TO_SERVER': 3.000,
    'TIMEOUT_TO_WORKER': 0.300,
    'TIMEOUT_TO_LOG': 0.300,
    'KEEPALIVE_TIMEOUT': 1.000,
    'LOG_LEVEL': logging.WARNING,
    'CC_LIST': [],
}

# --

U8_T = struct.Struct('<B')
U16_T = struct.Struct('<H')
I32_T = struct.Struct('<i')
U32_T = struct.Struct('<I')
U64_T = struct.Struct('<Q')
BOOL_T = struct.Struct('<?')
F64_T = struct.Struct('<d')

# --

def make_key(mod_name, subkey):
    return mod_name.encode() + b'|' + subkey


def key_from_call(args, **kwargs):
    p = subprocess.run(args, check=True, capture_output=True, **kwargs)
    return p.stderr + p.stdout

# Offer some helpers:
CONFIG_GLOBALS = {
    key_from_call: key_from_call,
}

# --

CONFIG = dict(DEFAULT_CONFIG)
if CONFIG_PATH.exists():
    code = CONFIG_PATH.read_bytes()
    code = compile(code, CONFIG_PATH.as_posix(), 'exec', optimize=0)
    exec(code, CONFIG_GLOBALS, CONFIG)
    # For example, CONFIG['CC_LIST'] can be modified in ~/.pydra/config.py as `CC_LIST +=`.

# --

log_level = CONFIG['LOG_LEVEL']
for x in sys.argv:
    if x.startswith('-v'):
        v_count = len(x) - 1
        if v_count == 1:
            v_level = logging.INFO
        else:
            v_level = logging.DEBUG
        log_level = min(log_level, v_level)

logging.getLogger().setLevel(log_level)

# --

class ByteReader(object):
    def __init__(self, b):
        self._view = memoryview(b)

    def unpack_t(self, type_struct):
        v_view = self._view[:type_struct.size]
        self._view = self._view[type_struct.size:]
        (v,) = type_struct.unpack(v_view)
        return v

    def unpack_bytes(self):
        b_len = self.unpack_t(U8_T)
        if b_len == 0xff:
            b_len = self.unpack_t(U64_T)
        b = self._view[:b_len]
        self._view = self._view[b_len:]
        return bytes(b)


class ByteWriter(object):
    def __init__(self):
        self._parts = []

    def pack_t(self, type_struct, v):
        self._parts.append(type_struct.pack(v))

    def pack_bytes(self, b):
        if len(b) < 0xff:
            self.pack_t(U8_T, len(b))
        else:
            self.pack_t(U8_T, 0xff)
            self.pack_t(U64_T, len(b))
        self._parts.append(b)

    def data(self):
        b = b''.join(self._parts)
        self._parts = [b]
        return self._parts[0]

# --

def exit_after_keyboard():
    try:
        while True:
            time.sleep(1000 * 1000)
    except KeyboardInterrupt:
        exit(0)

# --

def walk_path(root):
    pending_dirs = [root]
    while pending_dirs:
        cur_dir = pending_dirs.pop(0)
        for x in cur_dir.iterdir():
            x.skip = False
            yield x
            if not x.skip and x.is_dir():
                pending_dirs.append(x)

# --

def nice_down():
    if sys.platform == 'win32':
        try:
            import psutil
            p = psutil.Process()
            p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
            return
        except ModuleNotFoundError:
            logging.warning('nice_down() on Windows require psutil. (py -3 -m pip install psutil)')
            pass
    else:
        try:
            os.nice(10)
            return
        except PermissionError:
            pass
    logging.warning('Warning: nice_down failed.')

# --

class Packetable(object):
    def encode(self):
        bw = ByteWriter()
        self.encode_into(bw)
        return bw.data()

    @classmethod
    def decode(class_, data):
        ret = class_()
        br = ByteReader(data)
        ret.decode_from(br)
        return ret


    def send_encode(self, pc):
        pc.send(self.encode())


    @classmethod
    def recv_decode(class_, pc):
        return class_.decode(pc.recv())

# --

class WorkerAssignmentPacket(Packetable):
    def encode_into(self, bw):
        bw.pack_bytes(self.hostname.encode())

        bw.pack_t(U64_T, len(self.addrs))
        [bw.pack_bytes(x.encode()) for x in self.addrs]


    def decode_from(self, br):
        self.hostname = br.unpack_bytes().decode()

        num_addr = br.unpack_t(U64_T)
        self.addrs = [Address.decode(br.unpack_bytes()) for _ in range(num_addr)]

# --

class Address(Packetable):
    def __init__(self, addr=None):
        self.addr = addr


    def encode_into(self, bw):
        bw.pack_bytes(self.addr[0].encode())
        bw.pack_t(U16_T, self.addr[1])


    def decode_from(self, br):
        host = br.unpack_bytes().decode()
        port = br.unpack_t(U16_T)
        self.addr = (host, port)

# --

class WorkerAdvertPacket(Packetable):
    def encode_into(self, bw):
        bw.pack_bytes(self.hostname.encode())

        bw.pack_t(U64_T, len(self.keys))
        [bw.pack_bytes(x) for x in self.keys]

        bw.pack_t(U64_T, len(self.addrs))
        [bw.pack_bytes(x.encode()) for x in self.addrs]


    def decode_from(self, br):
        self.hostname = br.unpack_bytes().decode()

        num_keys = br.unpack_t(U64_T)
        self.keys = [br.unpack_bytes() for _ in range(num_keys)]

        num_addr = br.unpack_t(U64_T)
        self.addrs = [Address.decode(br.unpack_bytes()) for _ in range(num_addr)]

# --

def dump_thread_stacks():
    for (k,v) in sys._current_frames().items():
        stack = traceback.format_stack(f=v)
        text = '\nThread {}:\n{}'.format(hex(k), '\n'.join(stack))
        logging.debug(text)

# --

class MsTimer(object):
    def __init__(self):
        self.start = time.time()
        self.lap_start = self.start

    class Res(object):
        def __init__(self, val):
            self.val = val

        def __float__(self):
            return self.val

        def __str__(self):
            return '{:.3f}ms'.format(self.val)

    def time(self):
        now = time.time()
        x = now - self.start
        return self.Res(x * 1000.0)

    def lap(self):
        now = time.time()
        x = now - self.lap_start
        self.lap_start = now
        return self.Res(x * 1000.0)

# --

JOB_SERVER_MDNS_SERVICE = 'job_server._pydra._tcp.local.'

def job_server_addr(timeout):
    addr = CONFIG['JOB_SERVER_ADDR']
    if addr[0]:
        return addr

    try:
        import zeroconf
    except ImportError:
        logging.error('JOB_SERVER_ADDR[0]='' requires `pip install zeroconf`.')
        return None
    zc = zeroconf.Zeroconf()

    logging.info('Querying mDNS...')
    info = zc.get_service_info(JOB_SERVER_MDNS_SERVICE, JOB_SERVER_MDNS_SERVICE,
            timeout=timeout*1000)
    if not info:
        return None
    host = socket.inet_ntop(socket.AF_INET, info.address)
    return (host, info.port)
