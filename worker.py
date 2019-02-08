#!/usr/bin/env python3
assert __name__ == '__main__'

from common import *
import pydra_mod

import itertools

# --

LockingLogHandler.install()

# --

MODS = pydra_mod.LoadModules()
print('MODS', MODS)

def get_mods_by_key():
    mods_by_key = {}
    for (mod_name,m) in MODS.items():
        for sk in m.pydra_get_subkeys():
            key = make_key(mod_name, sk)
            mods_by_key[key] = m
    return mods_by_key

# --

work_server = None

def advert_to_server():
    if not work_server:
        logging.info('No work_server.')
        return
    mods_by_key = get_mods_by_key()
    keys = list(mods_by_key.keys())
    logging.info('keys: {}'.format(keys))

    gais = work_server.get_gais()
    addrs = [Address(x[4]) for x in gais]
    if not (keys and addrs):
        logging.info((keys, addrs))
        return

    addr = CONFIG['JOB_SERVER_ADDR']
    conn = nu.connect_any([addr], timeout=CONFIG['TIMEOUT_WORKER_TO_SERVER'])
    if not conn:
        logging.warning('Failed to connect: {}'.format(addr))
        return
    pconn = nu.PacketConn(conn, CONFIG['KEEPALIVE_TIMEOUT'], True)
    logging.error('Connected: {}'.format(pconn.conn.getpeername()))


    try:
        pconn.send(b'worker')

        wap = WorkerAdvertPacket()
        wap.hostname = CONFIG['HOSTNAME']
        wap.keys = keys
        wap.addrs = addrs
        pconn.send(wap.encode())

        while True:
            new_mods_by_key = get_mods_by_key()
            new_gais = work_server.get_gais()
            if new_mods_by_key != mods_by_key:
                logging.info('Keys changed: {}'.format(new_mods_by_key))
                return
            if new_gais != gais:
                logging.info('Gais changed.')
                return

            time.sleep(1.0)
    except nu.ExSocketEOF:
        logging.info('Server closed socket.')
        pass
    except socket.error:
        raise

# --

def advert_to_server_loop():
    while True:
        advert_to_server()
        time.sleep(1.0)
        logging.warning('Reconnecting to server...')

# --

work_conn_counter = itertools.count(1)

def th_on_accept_work(conn, addr):
    conn_id = next(work_conn_counter)
    conn_prefix = '[work {}]'.format(conn_id)
    logging.info(conn_prefix + '<connected>')

    pconn = nu.PacketConn(conn, CONFIG['KEEPALIVE_TIMEOUT'], True)
    try:
        hostname = pconn.recv().decode()
        key = pconn.recv()

        locked_print(conn_prefix + 'hostname:', hostname)

        (mod_name, subkey) = key.split(b'|', 1)
        m = MODS[mod_name.decode()]
        m.pydra_job_worker(pconn, subkey)

    finally:
        logging.info(conn_prefix + '<disconnected>')

# --

log_conn_counter = itertools.count(1)

def th_on_accept_log(conn, addr):
    conn_id = next(log_conn_counter)
    conn_prefix = '[log {}] '.format(conn_id)
    logging.info(conn_prefix + '<connected>')

    pconn = nu.PacketConn(conn, CONFIG['KEEPALIVE_TIMEOUT'], True)
    try:
        while True:
            text = pconn.recv().decode()
            text = text.replace('\n', '\n' + ' '*len(conn_prefix))
            locked_print(conn_prefix, text)
    except (socket.error, nu.ExSocketEOF):
        pass
    finally:
        logging.info(conn_prefix + '<disconnected>')
        pconn.nuke()

# --

addr = CONFIG['WORKER_LOG_ADDR']
log_server = nu.Server([addr], target=th_on_accept_log)
log_server.listen_until_shutdown()

addr = CONFIG['WORKER_ADDR']
work_server = nu.Server([addr], target=th_on_accept_work)
work_server.listen_until_shutdown()

threading.Thread(target=advert_to_server_loop, daemon=True).start()

# --

wait_for_keyboard()

work_server.shutdown()
log_server.shutdown()

dump_thread_stacks()
exit(0)
