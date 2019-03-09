#!/usr/bin/env python3
assert __name__ == '__main__'

from common import *
import pydra_mod

import itertools
import job_client
import psutil
import threading

# --------------------------------

LockingLogHandler.install()

log_conn_counter = itertools.count(1)

def th_on_accept_log(conn, addr):
    conn_id = next(log_conn_counter)
    conn_prefix = ''
    if CONFIG['LOG_LEVEL'] == logging.DEBUG:
        conn_prefix = '[log {}] '.format(conn_id)
    logging.debug(conn_prefix + '<connected>')

    pconn = nu.PacketConn(conn, CONFIG['KEEPALIVE_TIMEOUT'], True)
    try:
        while True:
            text = pconn.recv().decode()
            text = text.replace('\n', '\n' + ' '*len(conn_prefix))
            locked_print(conn_prefix, text)
    except OSError:
        pass
    finally:
        logging.debug(conn_prefix + '<disconnected>')
        pconn.nuke()

# --

log_server = nu.Server([CONFIG['LOG_ADDR']], target=th_on_accept_log)
log_server.listen_until_shutdown()

# ---------------------------

MODS = pydra_mod.LoadModules()
logging.info('MODS', MODS)

def get_mods_by_key():
    mods_by_key = {}
    for (mod_name,m) in MODS.items():
        for sk in m.pydra_get_subkeys():
            key = make_key(mod_name, sk)
            mods_by_key[key] = m
    return mods_by_key

# --

# --


nice_down()

worker_prefix = '[workerd] '
work_conn_counter = itertools.count(1)

utilization_cv = threading.Condition()
active_slots = 0
cpu_load = 0.0

# --

def th_on_accept_work(conn, addr):
    conn_id = next(work_conn_counter)
    conn_prefix = worker_prefix + '[worklet {}] '.format(conn_id)

    global active_slots

    try:
        active_slots += 1
        if active_slots > CONFIG['WORKERS']:
            logging.info(conn_prefix + '<refused>')
            return
        logging.debug(conn_prefix + '<connected>')
        with utilization_cv:
            utilization_cv.notify_all()

        pconn = nu.PacketConn(conn, CONFIG['KEEPALIVE_TIMEOUT'], True)
        hostname = pconn.recv().decode()
        key = pconn.recv()

        logging.debug(conn_prefix + 'hostname: ' + hostname)

        (mod_name, subkey) = key.split(b'|', 1)
        m = MODS[mod_name.decode()]
        m.pydra_job_worker(pconn, subkey)
    except OSError:
        pass
    finally:
        active_slots -= 1
        logging.debug(conn_prefix + '<disconnected>')
        with utilization_cv:
            utilization_cv.notify_all()

work_server = nu.Server([CONFIG['WORKER_BASE_ADDR']], target=th_on_accept_work)
work_server.listen_until_shutdown()

# --

def th_cpu_percent():
    try:
        import psutil
    except ImportError:
        logging.warning('cpu load tracking requires psutil, disabling...')
        return

    global cpu_load

    while True:
        cpu_load = psutil.cpu_percent(interval=None, percpu=True) # [0,100]
        with utilization_cv:
            utilization_cv.notify_all()
        time.sleep(3.0)

threading.Thread(target=th_cpu_percent, daemon=True).start()

# -

def advert_to_server():
    if not work_server:
        logging.error(worker_prefix + 'No work_server.')
        return
    mods_by_key = get_mods_by_key()
    keys = list(mods_by_key.keys())
    logging.debug(worker_prefix + 'keys: {}'.format(keys))

    gais = work_server.get_gais()
    addrs = [Address(x[4]) for x in gais]
    if not (keys and addrs):
        logging.warning(worker_prefix + (keys, addrs))
        return

    addr = CONFIG['JOB_SERVER_ADDR']
    conn = nu.connect_any([addr], timeout=CONFIG['TIMEOUT_WORKER_TO_SERVER'])
    if not conn:
        logging.error(worker_prefix + 'Failed to connect: {}'.format(addr))
        return
    pconn = nu.PacketConn(conn, CONFIG['KEEPALIVE_TIMEOUT'], True)
    logging.warning(worker_prefix + 'Connected to job_server {} as {}'.format(
            pconn.conn.getpeername(), CONFIG['HOSTNAME']))


    def th_nuke_on_recv():
        pconn.wait_for_disconnect()


    def th_nuke_on_change():
        while pconn.alive:
            new_mods_by_key = get_mods_by_key()
            new_gais = work_server.get_gais()
            if new_mods_by_key != mods_by_key:
                logging.info(worker_prefix + 'Keys changed: {}'.format(new_mods_by_key))
                break
            if new_gais != gais:
                logging.info(worker_prefix + 'Gais changed.')
                break

            time.sleep(1.0)
        pconn.nuke()


    threading.Thread(target=th_nuke_on_recv).start()
    threading.Thread(target=th_nuke_on_change).start()
    try:
        pconn.send(b'worker')

        wap = WorkerAdvertPacket()
        wap.hostname = CONFIG['HOSTNAME']
        wap.keys = keys
        wap.addrs = addrs
        pconn.send(wap.encode())

        with utilization_cv:
            while pconn.alive:
                max_slots = CONFIG['WORKERS']
                avail_slots = max_slots - active_slots
                cpu_idle = len(cpu_load) - (sum(cpu_load) / 100.0)
                avail_slots = min(avail_slots, cpu_idle)
                if avail_slots > max_slots - 1:
                    avail_slots = max_slots
                pconn.send_t(F64_T, avail_slots)

                utilization_cv.wait(10.0) # Refresh, just slowly if not notified.
                time.sleep(0.1) # Minimum delay between updates
    except OSError:
        pass
    finally:
        logging.warning(worker_prefix + 'Socket disconnected.')
        pconn.nuke()

# --

try:
    while True:
        advert_to_server()
        time.sleep(1.0)
        logging.warning(worker_prefix + 'Reconnecting to server...')
except KeyboardInterrupt:
    pass

log_server.shutdown()

#[p.terminate() for p in procs]
#dump_thread_stacks()
exit(0)
