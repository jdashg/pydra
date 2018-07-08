#!/usr/bin/env python3
assert __name__ != '__main__'

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

PYDRA_HOME = pathlib.PosixPath.home() / '.pydra'
CONFIG_PATH = PYDRA_HOME / 'config.py'

# --

def basic_log(msg):
    Globals.PRINT_FUNC(msg)

def v_log(v_level, fmt_str, *fmt_args):
    if Globals.VERBOSE < v_level:
        return
    msg = fmt_str.format(*fmt_args)
    Globals.LOG_FUNC(msg)

# --

print_lock = threading.Lock()
def locked_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)
        sys.stdout.flush()

# --

class Globals:
    # Otherwise it's easy to forget `global FOO`.
    PRINT_FUNC = locked_print
    LOG_FUNC = basic_log
    VERBOSE = 3

# --

DEFAULT_CONFIG = {
    'JOB_SERVER_ADDR': ('localhost', 38520),
    'WORKER_ADDR': (socket.gethostname(), 38521),
    'WORKER_LOG_ADDR': ('localhost', 38522),
    'HOSTNAME': socket.gethostname(),
    'TIMEOUT_CLIENT_TO_SERVER': 0.300,
    'TIMEOUT_WORKER_TO_SERVER': 0.300,
    'TIMEOUT_TO_WORKER': 0.300,
    'TIMEOUT_TO_LOG': 0.300,
}

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

# --

def recv_n(conn, n):
    ret = bytearray(n)
    if n:
        view = memoryview(ret)
        while view:
            got = conn.recv_into(view)
            if not got:
                raise ExSocketClosed()
            view = view[got:]
    return bytes(ret)

# --

U8_T = struct.Struct('<B')
U16_T = struct.Struct('<H')
I32_T = struct.Struct('<i')
U32_T = struct.Struct('<I')
U64_T = struct.Struct('<Q')
BOOL_T = struct.Struct('<?')
F64_T = struct.Struct('<d')

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

class ExSocketClosed(Exception):
    pass

# --

def send_t(conn, type_struct, v):
    b = type_struct.pack(v)
    conn.sendall(b)

def recv_t(conn, type_struct):
    b = recv_n(conn, type_struct.size)
    (v,) = type_struct.unpack(b)
    return v

# --

def send_bytes(conn, b):
    if len(b) < 0xff:
        send_t(conn, U8_T, len(b))
    else:
        send_t(conn, U8_T, 0xff)
        send_t(conn, U64_T, len(b))
    conn.sendall(b)


def recv_bytes(conn):
    b_len = recv_t(conn, U8_T)
    if b_len == 0xff:
        b_len = recv_t(conn, U64_T)
    return recv_n(conn, b_len)

# --

def set_keepalive(conn, after_idle_sec=1, interval_sec=1, max_fails=10):
    conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    if platform.system() == 'Darwin':
        # scraped from /usr/include, not exported by python's socket module
        TCP_KEEPALIVE = 0x10
        sock.setsockopt(socket.IPPROTO_TCP, TCP_KEEPALIVE, interval_sec)
        return

    # Linux and Windows
    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, after_idle_sec)
    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval_sec)

    if platform.system() == 'Linux':
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, max_fails)
        return

    if platform.system() == 'Windows':
        # On Windows Vista and later, the number of keep-alive probes (data
        # retransmissions) is set to 10 and cannot be changed.
        # On Windows Server 2003, Windows XP, and Windows 2000, the default
        # setting for number of keep-alive probes is 5.
        '''
        onoff = 1
        tcp_keepalive = (onoff, after_idle_sec*1000, interval_sec*1000)
        sock.ioctl(socket.SIO_KEEPALIVE_VALS, tcp_keepalive)
        '''
        return

    assert False

# --

def nuke_socket(conn):
    try:
        conn.shutdown(socket.SHUT_RDWR)
    except socket.error:
        pass
    try:
        conn.close()
    except socket.error:
        pass

# --

def wait_for_keyboard():
    try:
        while True:
            time.sleep(1000 * 1000)
    except KeyboardInterrupt:
        pass

# --

class Server(object):
    def __init__(self, addrs, target, on_accept_args=()):
        self.addrs = set(addrs)
        self.fn_on_accept = target
        self.on_accept_args = tuple(on_accept_args)

        self.lock = threading.Lock()
        self.alive = True
        self.s_by_gai = {}
        return


    def listen_until_shutdown(self, daemon=True):
        threading.Thread(target=self.listen_loop, daemon=daemon).start()


    def listen_loop(self):
        while True:
            gais = set()
            for (host,port) in self.addrs:
                gais |= set(socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP))

            with self.lock:
                if not self.alive:
                    break

                new_gais = gais.difference(self.s_by_gai.keys())
                for gai in new_gais:
                    s = socket.socket(gai[0])
                    try:
                        s.bind(gai[4])
                        s.listen()
                    except socket.error:
                        continue
                    self.s_by_gai[gai] = s
                    threading.Thread(target=self._th_accept_loop, args=(gai, s)).start()

            time.sleep(1.0)
            continue

        return


    def shutdown(self):
        with self.lock:
            self.alive = False
            for x in self.s_by_gai.values():
                nuke_socket(x)


    def get_gais(self):
        with self.lock:
            return set(self.s_by_gai.keys())


    def _th_accept_loop(self, gai, s):
        while True:
            try:
                (conn, addr) = s.accept()
            except socket.error:
                break

            threading.Thread(target=self._th_on_accept, args=(conn, addr)).start()
            continue

        nuke_socket(s)
        with self.lock:
            del self.s_by_gai[gai]
        return


    def _th_on_accept(self, conn, addr):
        try:
            self.fn_on_accept(conn=conn, addr=addr, *(self.on_accept_args))
        except ExSocketClosed as _:
            pass
        except Exception as e:
            traceback.print_exc()
        nuke_socket(conn)
        return

# --

def connect_any(addrs, timeout=socket.getdefaulttimeout()):
    gais = set()
    for (host,port) in addrs:
        gai = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
        gais |= set(gai)

    s_addr_list = [(socket.socket(gai[0]), gai[4]) for gai in gais]

    completion_sem = threading.Semaphore(0)
    winner_lock = threading.Lock()
    winner = [None] # box

    outer_stack = traceback.extract_stack()

    def fn_thread(s, remote_addr):
        try:
            try:
                s.settimeout(timeout)
                s.connect(remote_addr)
            except socket.timeout:
                return
            except socket.error:
                outer_stack_str = traceback.format_list(outer_stack)
                outer_stack_str = '\n'.join(outer_stack_str)
                v_log(2, '{}\nWithin:\n{}', traceback.format_exc(), outer_stack_str)
                return

            if winner_lock.acquire(blocking=False):
                winner[0] = s
                for (s2,_) in s_addr_list:
                    if s2 != s:
                        nuke_socket(s2)

                for _ in s_addr_list:
                    completion_sem.release()
                return
        finally:
            completion_sem.release()
        return

    for s_addr in s_addr_list:
        threading.Thread(target=fn_thread, args=s_addr).start()

    for _ in s_addr_list:
        completion_sem.acquire()
    return winner[0]

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
            pass
    else:
        try:
            os.nice(10)
        except PermissionError:
            pass
    v_log(0, 'Warning: nice_down failed.')

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


    def send_encode(self, conn):
        send_bytes(conn, self.encode())


    @classmethod
    def recv_decode(class_, conn):
        return class_.decode(recv_bytes(conn))

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
'''
class Address(Packetable):
    def __init__(self, af=None, addr=None, gai=None):
        if gai:
            (af, _, _, _, addr) = gai
        self.af = af
        self.addr = addr


    def encode_into(self, bw):
        bw.pack_t(U8_T, int(self.af))

        bw.pack_bytes(self.addr[0].encode())
        bw.pack_t(U16_T, self.addr[1])
        if self.af == socket.AF_INET6:
            bw.pack_t(U32_T, self.addr[2]) # flow info
            bw.pack_t(U32_T, self.addr[3]) # scope id
        else:
            assert self.af == socket.AF_INET, self.af


    def decode_from(self, br):
        self.af = socket.AddressFamily(br.unpack_t(U8_T))

        host = br.unpack_bytes().decode()
        port = br.unpack_t(U16_T)
        if self.af == socket.AF_INET:
            self.addr = (host, port)
        elif self.af == socket.AF_INET6:
            flow_info = br.unpack_t(U32_T)
            scope_id = br.unpack_t(U32_T)
            self.addr = (host, port, flow_info, scope_id)
        else:
            assert False, self.af


    @staticmethod
    def test():
        gp = Address(gai=(socket.AF_INET, 0, 0, '', ('localhost', 49392)))
        b = gp.encode()
        gp2 = Address.decode(b)
        assert (gp.af, gp.addr) == (gp2.af, gp2.addr), ((gp.af, gp.addr), (gp2.af, gp2.addr))
        b2 = gp2.encode()
        assert b == b2, (b.hex(), b2.hex())

Address.test()
'''

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
