#!/usr/bin/env python3
assert __name__ == '__main__'

from common import *
import net_utils as nu

import itertools

g_cvar = threading.Condition()
job_queue_by_key = {}
worker_queue_by_key = {}

# --

LockingLogHandler.install()

# --

class Job(object):
    next_id = itertools.count()

    def __init__(self, pconn, hostname, key):
        self.pconn = pconn
        self.hostname = hostname
        self.key = key

        self.id = next(self.next_id)
        self._active = False
        return


    def __lt__(a, b):
        return a.id < b.id


    def __str__(self):
        return 'Job{}@{}'.format(self.id, self.hostname)


    def set_active(self, new_val):
        logging.warning('{}.set_active({})'.format(self, new_val))
        if self._active == new_val:
            return
        self._active = new_val

        if new_val:
            job_queue = job_queue_by_key.setdefault(self.key, [])
            job_queue.append(self)
            job_queue.sort()
            g_cvar.notify()
        else:
            job_queue = job_queue_by_key[self.key]
            job_queue.remove(self)
            if not job_queue:
                del job_queue_by_key[self.key]

# --

class Worker(object):
    next_id = itertools.count()

    def __init__(self, pconn, hostname, keys, addrs):
        self.pconn = pconn
        self.hostname = hostname
        self.keys = keys
        self.addrs = addrs
        self.id = next(self.next_id) # Purely informational.
        self._active = False
        return


    def __str__(self):
        return 'Worker{}@{}'.format(self.id, self.hostname)


    def set_active(self, new_val):
        logging.warning('{}.set_active({})'.format(self, new_val))
        if self._active == new_val:
            return
        self._active = new_val

        for key in self.keys:
            if new_val:
                worker_queue = worker_queue_by_key.setdefault(key, [])
                worker_queue.append(self)
                g_cvar.notify()
            else:
                worker_queue = worker_queue_by_key[key]
                worker_queue.remove(self)
                if not worker_queue:
                    del worker_queue_by_key[key]

# --

def job_accept(pconn):
    job = None
    try:
        hostname = pconn.recv().decode()
        key = pconn.recv()

        job = Job(pconn, hostname, key)
        while True:
            with g_cvar:
                job.set_active(True)

            pconn.recv() # Remote will kill socket if its done.
            continue # Recv dummy val means repeat.

    except OSError:
        pass
    finally:
        if job:
            with g_cvar:
                job.set_active(False)

# --

def worker_accept(pconn):
    worker = None
    try:
        wap = WorkerAdvertPacket.decode(pconn.recv())
        worker = Worker(pconn, wap.hostname, wap.keys, wap.addrs)

        with g_cvar:
            worker.set_active(True)

        pconn.wait_for_shutdown()
    except OSError:
        pass
    finally:
        if worker:
            with g_cvar:
                worker.set_active(False)

# --

def matchmake():
    next_jobs = [x[0] for x in job_queue_by_key.values()]
    next_jobs = sorted(next_jobs, key=lambda x: x.id)
    for job in next_jobs:
        try:
            worker_queue = worker_queue_by_key[job.key]
        except KeyError:
            continue

        worker = worker_queue.pop(0) # Always non-empty.
        worker_queue.append(worker)

        job.set_active(False)

        return (job, worker)

    return (None, None)


def matchmake_loop():
    with g_cvar:
        while True:
            (job, worker) = matchmake()
            if not job:
                g_cvar.wait()
                continue

            logging.warning('Matched ({}, {})'.format(job, worker))

            wap = WorkerAssignmentPacket()
            wap.hostname = worker.hostname
            wap.addrs = worker.addrs
            job.pconn.send(wap.encode())


threading.Thread(target=matchmake_loop, daemon=True).start()

# --

def th_on_accept(conn, addr):
    try:
        pconn = nu.PacketConn(conn, CONFIG['KEEPALIVE_TIMEOUT'], True)
        conn_type = pconn.recv()
    except OSError:
        return

    if conn_type == b'job':
        return job_accept(pconn)
    if conn_type == b'worker':
        return worker_accept(pconn)
    assert False, conn_type

# --

addr = CONFIG['JOB_SERVER_ADDR']
if addr[0] == 'localhost':
    logging.error('Hosting job_server on localhost, which excludes remote hosts.')

logging.warning('Hosting job_server at: {}'.format(addr))
server = nu.Server([addr], target=th_on_accept)

server.listen_until_shutdown()
wait_for_keyboard()
server.shutdown()

#dump_thread_stacks()
exit(0)
