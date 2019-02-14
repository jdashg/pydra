#!/usr/bin/env python3

from common import *
import pydra_mod

import itertools
import job_client
import multiprocessing

# --

# --------------------------------

if  __name__ == '__main__':
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
        except (socket.error, nu.ExSocketEOF):
            pass
        finally:
            logging.debug(conn_prefix + '<disconnected>')
            pconn.nuke()

    # --

    addr = CONFIG['LOG_ADDR']
    log_server = nu.Server([addr], target=th_on_accept_log)
    log_server.listen_until_shutdown()
else:
    job_client.LogToWorker.install()


# ---------------------------

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

def run_worker(n):
    addr = CONFIG['WORKER_BASE_ADDR']
    addr = (addr[0], addr[1] + n - 1)

    worker_prefix = '[worker {}] '.format(n)

    logging.info(worker_prefix + 'addr: {}'.format(addr))
    nice_down()

    work_server = None
    work_conn_counter = itertools.count(1)

    def th_on_accept_work(conn, addr):
        conn_id = next(work_conn_counter)
        conn_prefix = worker_prefix + '[work {}] '.format(conn_id)
        logging.debug(conn_prefix + '<connected>')

        pconn = nu.PacketConn(conn, CONFIG['KEEPALIVE_TIMEOUT'], True)
        try:
            hostname = pconn.recv().decode()
            key = pconn.recv()

            logging.debug(conn_prefix + 'hostname:', hostname)

            (mod_name, subkey) = key.split(b'|', 1)
            m = MODS[mod_name.decode()]
            m.pydra_job_worker(pconn, subkey)

        finally:
            logging.debug(conn_prefix + '<disconnected>')

    work_server = nu.Server([addr], target=th_on_accept_work)
    work_server.listen_until_shutdown()

    # --

    def advert_to_server():
        if not work_server:
            logging.warning(worker_prefix + 'No work_server.')
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
            logging.warning(worker_prefix + 'Failed to connect: {}'.format(addr))
            return
        pconn = nu.PacketConn(conn, CONFIG['KEEPALIVE_TIMEOUT'], True)
        logging.info(worker_prefix + 'Connected to job_server: {}'.format(pconn.conn.getpeername()))

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
                    logging.info(worker_prefix + 'Keys changed: {}'.format(new_mods_by_key))
                    return
                if new_gais != gais:
                    logging.info(worker_prefix + 'Gais changed.')
                    return

                time.sleep(1.0)
        except nu.ExSocketEOF:
            logging.warning(worker_prefix + 'Server closed socket.')
            pass
        except socket.error:
            raise
        finally:
            pconn.nuke()

    # --

    def advert_to_server_loop():
        while True:
            advert_to_server()
            time.sleep(1.0)
            logging.warning(worker_prefix + 'Reconnecting to server...')

    # --

    advert_to_server_loop()
    #threading.Thread(target=advert_to_server_loop, daemon=True).start()

# --

if  __name__ == '__main__':
    multiprocessing.set_start_method('spawn')

    procs = []
    for i in range(CONFIG['WORKERS']):
        procs.append( multiprocessing.Process(target=run_worker, args=(i+1,)) )

    [p.start() for p in procs]

    # --

    wait_for_keyboard()

    log_server.shutdown()
    [p.terminate() for p in procs]

    #dump_thread_stacks()
    exit(0)
