#!/usr/bin/env python3
assert __name__ == '__main__'

from common import *
import pydra_mod

import itertools

# --

PRINT_FUNC = locked_print

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
        v_log(2, 'No work_server.')
        return
    mods_by_key = get_mods_by_key()
    keys = list(mods_by_key.keys())
    gais = work_server.get_gais()
    addrs = [Address(x[4]) for x in gais]
    if not (keys and addrs):
        v_log(2, (keys, addrs))
        return

    addr = CONFIG['JOB_SERVER_ADDR']
    conn = connect_any([addr], timeout=CONFIG['TIMEOUT_WORKER_TO_SERVER'])
    if not conn:
        v_log(2, 'Failed to connect: {}', addr)
        return

    conn.settimeout(None)
    set_keepalive(conn)

    try:
        send_bytes(conn, b'worker')

        wap = WorkerAdvertPacket()
        wap.hostname = CONFIG['HOSTNAME']
        wap.keys = keys
        wap.addrs = addrs
        send_bytes(conn, wap.encode())

        while True:
            new_mods_by_key = get_mods_by_key()
            new_gais = work_server.get_gais()
            if new_mods_by_key != mods_by_key:
                v_log(2, 'Keys changed.')
                return
            if new_gais != gais:
                v_log(2, 'Gais changed.')
                return

            time.sleep(1.0)
    except socket.error:
        raise

# --

def advert_to_server_loop():
    while True:
        advert_to_server()
        time.sleep(1.0)
        v_log(1, 'Reconnecting to server...')

threading.Thread(target=advert_to_server_loop, daemon=True).start()

# --

work_conn_counter = itertools.count(1)

def th_on_accept_work(conn, addr):
    conn_id = next(work_conn_counter)
    conn_prefix = '[work {}]'.format(conn_id)
    if Globals.VERBOSE >= 1:
        locked_print(conn_prefix, '<connected>')

    try:
        hostname = recv_bytes(conn).decode()
        key = recv_bytes(conn)

        locked_print(conn_prefix, 'hostname:', hostname)

        (mod_name, subkey) = key.split(b'|', 1)
        m = MODS[mod_name.decode()]
        m.pydra_job_worker(conn, subkey)

    finally:
        if Globals.VERBOSE >= 1:
            locked_print(conn_prefix, '<disconnected>')

# --

log_conn_counter = itertools.count(1)


def th_on_accept_log(conn, addr):
    conn_id = next(log_conn_counter)
    conn_prefix = '[log {}]'.format(conn_id)
    if Globals.VERBOSE >= 1:
        locked_print(conn_prefix, '<connected>')

    try:
        while True:
            text = recv_bytes(conn).decode()
            text = text.replace('\n', '\n ' + ' '*len(conn_prefix))
            locked_print(conn_prefix, text)
    finally:
        if Globals.VERBOSE >= 1:
            locked_print(conn_prefix, '<disconnected>')


# --

addr = CONFIG['WORKER_LOG_ADDR']
log_server = Server([addr], target=th_on_accept_log)
log_server.listen_until_shutdown()

addr = CONFIG['WORKER_ADDR']
work_server = Server([addr], target=th_on_accept_work)
work_server.listen_until_shutdown()

# --

wait_for_keyboard()

work_server.shutdown()
log_server.shutdown()
exit(0)
