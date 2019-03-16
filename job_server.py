#!/usr/bin/env python3
assert __name__ == '__main__'

from common import *
import net_utils as nu

import itertools
import os
import random
import signal

g_cvar = threading.Condition(threading.Lock())
job_queue_by_key = {}
available_workers_by_key = {}
connected_workers = set()
connected_workers_by_key = {}
karma_by_hostname = {}

# --

def add_karma_by_hostname(hostname, karma):
    try:
        karma_by_hostname[hostname] += karma
    except KeyError:
        karma_by_hostname[hostname] = karma

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

        logging.debug('%s connected.', self)
        return


    def close(self):
        logging.debug('%s disconnected.', self)
        with g_cvar:
            self.set_active(False)
        self.pconn.nuke()


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

        job_queue = job_queue_by_key.setdefault(self.key, [])
        if new_val:
            job_queue.append(self)
            job_queue.sort()
            g_cvar.notify()
        else:
            job_queue.remove(self)
            if not job_queue:
                del job_queue_by_key[self.key]

# --

class Worker(object):
    next_id = itertools.count()

    def __init__(self, pconn, desc):
        self.pconn = pconn
        self.desc = desc
        self.avail_slots = 0.0
        self.id = next(self.next_id) # Purely informational.
        self._active = False

        logging.warning('%s connected', self)

        with g_cvar:
            connected_workers.add(self)

            for key in self.desc.keys:
                workers = connected_workers_by_key.setdefault(key, set())
                workers.add(self)


    def close(self):
        logging.warning('%s disconnected.', self)

        with g_cvar:
            self.set_active(False)

            connected_workers.remove(self)

            for key in self.desc.keys:
                workers = connected_workers_by_key[key]
                workers.remove(self)
                if not workers:
                    del connected_workers_by_key[key]

                    # Purge outstanding jobs for the now-workerless key.
                    try:
                        for queue in job_queue_by_key[key]:
                            for j in queue:
                                j.pconn.nuke()
                    except KeyError:
                        pass
        self.pconn.nuke()


    def __str__(self):
        return 'Worker{}@{}'.format(self.id, self.desc.hostname)


    def set_active(self, new_val):
        plusminus = ('-', '+')
        logging.debug('{}{}'.format(plusminus[int(new_val)], self))
        assert not g_cvar.acquire(False)
        if self._active == new_val:
            return
        logging.info('{}{}'.format(plusminus[int(new_val)], self))
        self._active = new_val

        for key in self.desc.keys:
            workers = available_workers_by_key.setdefault(key, [])
            if new_val:
                workers.append(self)
                g_cvar.notify()
            else:
                workers.remove(self)
                if not workers:
                    del available_workers_by_key[key]

        stats_changed()

# --

def job_accept(pconn):
    job = None
    try:
        hostname = pconn.recv().decode()
        key = pconn.recv()

        job = Job(pconn, hostname, key)
        while True:
            # Remote will kill socket if its done.
            cmd = pconn.recv()
            if cmd == b'job_workers':
                info = JobWorkersDescriptor()
                info.local_slots = 0
                info.remote_slots = 0
                with g_cvar:
                    try:
                        for worker in connected_workers_by_key[key]:
                            worker_slots = worker.desc.max_slots
                            if worker.desc.hostname == hostname:
                                info.local_slots += worker_slots
                            else:
                                info.remote_slots += worker_slots
                    except KeyError:
                        pass
                pconn.send(info.encode())
                continue

            elif cmd == b'request_worker':
                with g_cvar:
                    job.set_active(True)
                continue

            elif cmd == b'karma': # TODO: Something like this?
                to_hostname = pconn.recv().decode()
                points = pconn.recv_t(F64_T)
                with g_cvar:
                    add_karma_by_hostname(to_hostname, points)
                    add_karma_by_hostname(hostname, -points)
                continue

            logging.warning('%s: Bad cmd: %s', job, cmd)
            return
    except OSError:
        pass
    finally:
        if job:
            job.close()

# --

def worker_accept(pconn):
    worker = None
    try:
        desc = WorkerDescriptor.decode(pconn.recv())
        worker = Worker(pconn, desc)

        while pconn.alive:
            avail_slots = pconn.recv_t(F64_T)
            with g_cvar:
                worker.avail_slots = avail_slots
                logging.info('%s.avail_slots = %.2f', worker, worker.avail_slots)
                stats_changed()
                worker.set_active(bool(worker.avail_slots))
    except OSError:
        pass
    finally:
        if worker:
            worker.close()

# -

def stats():
    assert not g_cvar.acquire(False)
    lines = ['Stats:']
    lines.append(f'  {len(connected_workers)} workers:')
    for w in connected_workers:
        name = w.desc.hostname
        lines.append(f'    slots: {w.avail_slots:.2f}/{w.desc.max_slots}\t{name}\t{w.desc.keys}')

    lines.append(f'  {len(connected_workers_by_key)} keys:')
    for (k,ws) in connected_workers_by_key.items():
        avail_slots = 0
        max_slots = 0
        for w in ws:
            avail_slots += w.avail_slots
            max_slots += w.desc.max_slots
        try:
            outstanding = len(job_queue_by_key[k])
        except KeyError:
            outstanding = 0
        lines.append(f'    slots: {avail_slots:.2f}/{max_slots}\toutstanding: {outstanding}\t{k}')
    lines.append('')
    return '\n'.join(lines)

# -

g_stats_cv = threading.Condition()
g_stats_cv.update_pending = False

def stats_changed():
    with g_stats_cv:
        g_stats_cv.update_pending = True
        g_stats_cv.notify()


def th_stats():
    while True:
        with g_stats_cv:
            while not g_stats_cv.update_pending:
                g_stats_cv.wait()
            g_stats_cv.update_pending = False

        with g_cvar:
            s = stats()

        logging.warning(s)
        time.sleep(0.3)

threading.Thread(target=th_stats, daemon=True).start()

# --

def matchmake():
    next_jobs = [x[0] for x in job_queue_by_key.values()]
    next_jobs = sorted(next_jobs, key=lambda x: x.id)
    for job in next_jobs:
        try:
            workers = available_workers_by_key[job.key]
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
    try:
        with g_cvar:
            while True:
                (job, worker) = matchmake()
                if not job:
                    '''
                    info = 'Outstanding jobs:'
                    if job_queue_by_key:
                        info = '\n'.join([info] +
                            ['  {}: {}'.format(k, len(v)) for (k,v) in job_queue_by_key.items()])
                    else:
                        info += ' None'
                    '''
                    stats_changed()
                    g_cvar.wait()
                    continue

                logging.warning('Matched ({}, {})'.format(job, worker))

                wap = WorkerAssignmentPacket()
                wap.hostname = worker.desc.hostname
                wap.addrs = worker.desc.addrs
                try:
                    job.pconn.send(wap.encode())
                except OSError:
                    logging.warning('Disconnect during matchmaking.')
                    job.pconn.nuke()
                    continue
    except Exception:
        traceback.print_exc()
    finally:
        logging.critical('matchmake_loop crashed. Aborting...')
        os.kill(os.getpid(), signal.SIGTERM)


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

class MdnsListener(object):
    def add_service(self, zc, s_type, name):
        info = zc.get_service_info(s_type, name)
        print("Service %s added, service info: %s" % (name, info))


    def remove_service(self, zc, s_type, name):
        print("Service %s removed" % (name,))

# --

addr = CONFIG['JOB_SERVER_ADDR']

zc = None

if not addr[0]:
    try:
        import zeroconf
    except ImportError:
        logging.error('JOB_SERVER_ADDR[0]='' requires `pip install zeroconf`.')
        exit(1)

    logging.warning('Checking for pre-existing mDNS job_server...')
    existing = job_server_addr(timeout=1.0)
    if existing:
        logging.error('mDNS found existing job_server at %s. Aborting...', existing)
        exit(1)

    zc = zeroconf.Zeroconf()

    family=socket.AF_INET # zeroconf module doesn't support IPv6 yet.
    gais = socket.getaddrinfo('', addr[1], proto=socket.IPPROTO_TCP, family=family)
    gai = gais[0]
    addr = gai[4]
    logging.warning(f'Advertizing on mDNS.')

    host_ip_bytes = socket.inet_pton(family, addr[0])

    server_name = CONFIG['HOSTNAME'] + '.local.'
    # If we don't specify properties, it defaults to None, and asserts deep in sending.
    info = zeroconf.ServiceInfo(JOB_SERVER_MDNS_SERVICE, JOB_SERVER_MDNS_SERVICE,
                                host_ip_bytes, addr[1], properties=b'', server=server_name)
    zc.register_service(info)

if addr[0] == 'localhost':
    logging.error('Hosting job_server on localhost, which excludes remote hosts.')

logging.warning('Hosting job_server at: {}'.format(addr))
server = nu.Server([addr], target=th_on_accept)

server.listen_until_shutdown()

try:
    while True:
        time.sleep(1000 * 1000)
except KeyboardInterrupt:
    if zc:
        zc.close()
exit(0)
