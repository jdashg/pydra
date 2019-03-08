#!/usr/bin/env python3
assert __name__ != '__main__'

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

# Subclass OSError, because it's not usually useful to rely on detecting a
# 'clean' shutdown like this.
class recv_n_eof(OSError):
    pass

# --

def recv_n(conn, n):
    ret = bytearray(n)
    if n:
        view = memoryview(ret)
        while view:
            got = conn.recv_into(view)
            if not got:
                raise recv_n_eof()
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

def pack_t(type_struct, v):
    b = type_struct.pack(v)
    return b


def unpack_t(type_struct, b):
    (v,) = type_struct.unpack(b)
    return v

# --

def send_t(conn, type_struct, v):
    conn.sendall(type_struct.pack(v))

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

#def set_keepalive(conn, approx=None, after_idle_sec=1, interval_sec=1, max_fails=10):
#    if approx:
#        after_idle_sec = approx / 2
#        interval_sec = approx / 2
#        interval_sec /= max_fails
#
#    conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
#
#    if platform.system() == 'Darwin':
#        # scraped from /usr/include, not exported by python's socket module
#        TCP_KEEPALIVE = 0x10
#        total_sec = after_idle_sec + interval_sec * max_fails
#        sock.setsockopt(socket.IPPROTO_TCP, TCP_KEEPALIVE, total_sec)
#        return
#
#    if platform.system() == 'Linux':
#        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, max_fails)
#    elif platform.system() == 'Windows':
#        # On Windows Vista and later, the number of keep-alive probes (data
#        # retransmissions) is set to 10 and cannot be changed.
#        # On Windows Server 2003, Windows XP, and Windows 2000, the default
#        # setting for number of keep-alive probes is 5.
#        interval_sec *= max_fails
#        max_fails = 10
#        interval_sec /= max_fails
#        '''
#        # Not needed anymore?
#        onoff = 1
#        tcp_keepalive = (onoff, after_idle_sec*1000, interval_sec*1000)
#        sock.ioctl(socket.SIO_KEEPALIVE_VALS, tcp_keepalive)
#        '''
#    else:
#        assert False, platform.system()
#
#    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, after_idle_sec)
#    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval_sec)

# --

def nuke_socket(conn):
    try:
        conn.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass

    try:
        conn.close()
    except OSError:
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
                    except OSError:
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
            except OSError:
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
            except OSError:
                if not winner[0]:
                    outer_stack_str = traceback.format_list(outer_stack)
                    outer_stack_str = '\n'.join(outer_stack_str)
                    logging.warning('%s\nWithin:\n%s', traceback.format_exc(), outer_stack_str)
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
        threading.Thread(target=fn_thread, args=tuple(s_addr)).start()

    for _ in s_addr_list:
        completion_sem.acquire()
    return winner[0]

# --

class PacketConn(object):
    def __init__(self, conn, timeout=False, keepalive=False):
        self.alive = True
        self.conn = conn
        self.slock = threading.RLock()
        self.rlock = threading.RLock()
        self.keepalive_cond = threading.Condition()
        self.keepalive_ratio = 2.5
        self.keepalive_thread = None

        if timeout != False:
            self.set_timeout(timeout, keepalive=keepalive)

    LONG_LEN_THRESHOLD = 0xfe
    KEEP_ALIVE_VAL = 0xff

    def send(self, b):
        with self.slock:
            if len(b) < self.LONG_LEN_THRESHOLD:
                send_t(self.conn, U8_T, len(b))
            else:
                send_t(self.conn, U8_T, self.LONG_LEN_THRESHOLD)
                send_t(self.conn, U64_T, len(b))
            if len(b):
                self.conn.sendall(b)


    def recv(self):
        try:
            with self.rlock:
                while True:
                    b_len = recv_t(self.conn, U8_T)
                    if b_len == self.KEEP_ALIVE_VAL:
                        continue
                    if b_len == self.LONG_LEN_THRESHOLD:
                        b_len = recv_t(self.conn, U64_T)

                    b = b''
                    if b_len:
                        b = recv_n(self.conn, b_len)
                    return b
        except (socket.timeout, recv_n_eof) as e:
            # Treat timeout and EOF as errors.
            self.nuke()
            raise OSError(e)


    def send_t(self, t, v):
        b = pack_t(t, v)
        self.send(b)


    def recv_t(self, t):
        b = self.recv()
        return unpack_t(t, b)


    def set_keepalive(self, on):
        with self.keepalive_cond:
            if on == bool(self.keepalive_thread):
                return
            if on:
                self.keepalive_thread = threading.Thread(target=self._th_keepalive, daemon=True)
                self.keepalive_thread.start()
            else:
                self.keepalive_thread = None
            self.keepalive_cond.notify()


    def set_timeout(self, secs, keepalive=None):
        with self.keepalive_cond:
            self.conn.settimeout(secs)
            self.keepalive_cond.notify()

        if keepalive != None:
            self.set_keepalive(keepalive)


    # Useful info:
    # * https://stackoverflow.com/questions/3757289/tcp-option-so-linger-zero-when-its-required)
    # * https://docs.microsoft.com/en-us/windows/desktop/WinSock/graceful-shutdown-linger-options-and-socket-closure-2

    # Sender should send_shutdown instead of nuking, when done.
    def send_shutdown(self):
        self.set_keepalive(False)
        try:
            self.conn.shutdown(socket.SHUT_WR)
        except socket.error:
            pass
        self.wait_for_disconnect()


    # Allows sending on other threads still.
    def wait_for_disconnect(self):
        try:
            self.recv()
            assert False
        except OSError:
            pass
        self.nuke()


    def nuke(self):
        self.alive = False
        self.set_keepalive(False)
        nuke_socket(self.conn)


    def _th_keepalive(self):
        with self.keepalive_cond:
            try:
                while True:
                    timeout = self.conn.gettimeout()
                    if timeout:
                        timeout /= self.keepalive_ratio
                    else:
                        # Wait forever if forever-blocking *or* non-blocking.
                        timeout = None
                    self.keepalive_cond.wait(timeout)
                    if not self.keepalive_thread:
                        return
                    with self.slock:
                        send_t(self.conn, U8_T, self.KEEP_ALIVE_VAL)

            except OSError: # Remote socket shutdown?
                pass

            finally:
                self.keepalive_thread = None
        self.nuke()

