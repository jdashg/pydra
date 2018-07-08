#!/usr/bin/env python3
assert __name__ != '__main__'

import common
import job_client
import pydra_mod

import time

# --

def pydra_get_subkeys():
    return [b'']


def pydra_shim(fn_dispatch, delay):
    delay = float(delay)
    return fn_dispatch(b'', delay)


def pydra_job_client(conn, subkey, delay):
    common.send_t(conn, common.F64_T, delay)
    common.recv_bytes(conn)
    return True


def pydra_job_worker(conn, subkey):
    delay = common.recv_t(conn, common.F64_T)
    time.sleep(delay)
    common.send_bytes(conn, b'')
