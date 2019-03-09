#!/usr/bin/env python3
assert __name__ == '__main__'

from common import *
import net_utils as nu

import itertools
import random

g_cvar = threading.Condition(threading.Lock())
job_queue_by_key = {}
workers_by_key = {}

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
        plusminus = ('-', '+')
        logging.debug('{}{}'.format(plusminus[int(new_val)], self))
        assert not g_cvar.acquire(False)
        if self._active == new_val:
            return
        logging.info('{}{}'.format(plusminus[int(new_val)], self))
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
        self.avail_slots = 0.0
        self.id = next(self.next_id) # Purely informational.
        self._active = False
        return


    def __str__(self):
        return 'Worker{}@{}'.format(self.id, self.hostname)


    def set_active(self, new_val):
        plusminus = ('-', '+')
        logging.debug('{}{}'.format(plusminus[int(new_val)], self))
        assert not g_cvar.acquire(False)
        if self._active == new_val:
            return
        logging.info('{}{}'.format(plusminus[int(new_val)], self))
        self._active = new_val

        for key in self.keys:
            if new_val:
                workers = workers_by_key.setdefault(key, [])
                workers.append(self)
                g_cvar.notify()
            else:
                workers = workers_by_key[key]
                workers.remove(self)
                if not workers:
                    del workers_by_key[key]

# --

def job_accept(pconn):
    job = None
    try:
        hostname = pconn.recv().decode()
        key = pconn.recv()

        job = Job(pconn, hostname, key)
        while True:
            # On recv, request new worker.
            # Remote will kill socket if its done.
            pconn.recv_t(BOOL_T)
            with g_cvar:
                job.set_active(True)
            continue

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

        while pconn.alive:
            avail_slots = pconn.recv_t(F64_T)
            with g_cvar:
                worker.avail_slots = avail_slots
                logging.warning('{}.avail_slots = {:.2f}'.format(worker, worker.avail_slots))
                worker.set_active(bool(worker.avail_slots))
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
            workers = workers_by_key[job.key]
        except KeyError:
            continue
        assert len(workers)
        weigts = (x.avail_slots for x in workers)
        cum_weights = list(itertools.accumulate(weigts))
        (worker,) = random.choices(workers, cum_weights=cum_weights, k=1)

        job.set_active(False)
        worker.set_active(False)

        return (job, worker)

    return (None, None)


def matchmake_loop():
    with g_cvar:
        while True:
            (job, worker) = matchmake()
            if not job:
                info = 'Outstanding jobs:'
                if job_queue_by_key:
                    info = '\n'.join([info] +
                        ['  {}: {}'.format(k, len(v)) for (k,v) in job_queue_by_key.items()])
                else:
                    info += ' None'
                logging.warning(info)
                g_cvar.wait()
                continue

            logging.warning('Matched ({}, {})'.format(job, worker))

            wap = WorkerAssignmentPacket()
            wap.hostname = worker.hostname
            wap.addrs = worker.addrs
            try:
                job.pconn.send(wap.encode())
            except OSError:
                logging.warning('Disconnect during matchmaking.')
                job.pconn.nuke()
                continue


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
