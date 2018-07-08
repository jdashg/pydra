#!/usr/bin/env python3
assert __name__ != '__main__'

from common import *

TIMEOUT = 300

# --

def log_to_worker_failover(text):
    if Globals.VERBOSE >= 2:
        Globals.PRINT_FUNC('<<' + text + '>>')

log_conn = False
log_conn_lock = threading.RLock()

def log_to_worker(text):
    global log_conn
    with log_conn_lock:
        if log_conn == False:
            log_conn = None
            addr = CONFIG['WORKER_LOG_ADDR']
            log_conn = connect_any([addr], timeout=CONFIG['TIMEOUT_TO_LOG'])
            if log_conn:
                log_conn.settimeout(None)

        if log_conn == None:
            log_to_worker_failover(text)
            return

        send_bytes(log_conn, text.encode())

# --

def dispatch(mod_name, subkey, fn_pydra_job_client, *args):
    key = make_key(mod_name, subkey)

    addr = CONFIG['JOB_SERVER_ADDR']
    server_conn = connect_any([addr], timeout=CONFIG['TIMEOUT_CLIENT_TO_SERVER'])
    if not server_conn:
        v_log(0, 'Failed to connect to server: {}', addr)
        return False

    server_conn.settimeout(None)
    set_keepalive(server_conn)

    try:
        send_bytes(server_conn, b'job')

        send_bytes(server_conn, CONFIG['HOSTNAME'].encode())
        send_bytes(server_conn, key)

        while True:
            wap = WorkerAssignmentPacket.decode(recv_bytes(server_conn))

            addrs = [x.addr for x in wap.addrs]
            worker_conn = connect_any(addrs, timeout=CONFIG['TIMEOUT_TO_WORKER'])
            if not worker_conn:
                v_log(0, 'Failed to connect to worker: {}@{}', wap.hostname, addrs)
                send_t(server_conn, BOOL_T, False)
                continue

            worker_conn.settimeout(None)
            set_keepalive(worker_conn)

            try:
                send_bytes(worker_conn, CONFIG['HOSTNAME'].encode())
                send_bytes(worker_conn, key)

                ok = fn_pydra_job_client(worker_conn, subkey, *args)
            except socket.error:
                ok = False
            finally:
                nuke_socket(worker_conn)

            send_t(server_conn, BOOL_T, bool(ok))
            if not ok:
                continue
            break
    except socket.error:
        v_log(1, 'server_conn died:\n{}', traceback.format_exc())
    finally:
        nuke_socket(server_conn)

