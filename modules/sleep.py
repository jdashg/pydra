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


def pydra_job_client(pconn, subkey, delay):
    pconn.send_t(common.F64_T, delay)
    pconn.recv() # To know when we are done.
    return True


def pydra_job_worker(pconn, worker_hostname, subkey):
    delay = pconn.recv_t(common.F64_T)
    time.sleep(delay)
    pconn.send(b'\0')
