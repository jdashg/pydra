#!/usr/bin/env python3
assert __name__ == '__main__'

from common import *
import pydra_mod

import itertools
import job_client
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

addr = CONFIG['LOG_ADDR']
log_server = nu.Server([addr], target=th_on_accept_log)
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

addr = CONFIG['WORKER_BASE_ADDR']

worker_prefix = '[workerd] '

logging.info(worker_prefix + 'addr: {}'.format(addr))
nice_down()

work_server = None
work_conn_counter = itertools.count(1)

MAX_WORKERS = CONFIG['WORKERS']
available_slots_cv = threading.Condition()
available_slots = MAX_WORKERS

# --

def th_on_accept_work(conn, addr):
    conn_id = next(work_conn_counter)
    conn_prefix = worker_prefix + '[job {}] '.format(conn_id)


    try:
        global available_slots
        available_slots -= 1
        if available_slots < 1:
            logging.info(conn_prefix + '<refused>')
            return
        logging.debug(conn_prefix + '<connected>')
        with available_slots_cv:
            available_slots_cv.notify_all()

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
        logging.debug(conn_prefix + '<disconnected>')
        available_slots += 1
        with available_slots_cv:
            available_slots_cv.notify_all()

work_server = nu.Server([addr], target=th_on_accept_work)
work_server.listen_until_shutdown()

# --

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
    logging.warning(worker_prefix + 'Connected to job_server: {}'.format(pconn.conn.getpeername()))


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

        with available_slots_cv:
            while pconn.alive:
                cur = available_slots
                if cur < 1:
                    cur = 0
                pconn.send_t(F64_T, cur)
                available_slots_cv.wait(1.0)
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
