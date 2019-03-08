#!/usr/bin/env python3
assert __name__ != '__main__'

from common import *
import net_utils as nu

TIMEOUT = 300
LOCAL_SPEW = False

# --

class LogToWorker(logging.Handler):
    def __init__(self, addr):
        super().__init__()

        self.addr = addr
        self.lock = threading.RLock()
        self.pconn = None


    def close(self):
        with self.lock:
            if self.pconn:
                self.pconn.nuke()
        super().close()


    def emit(self, record):
        text = self.format(record).encode()
        with self.lock:
            try:
                if self.pconn == None:
                    self.pconn = False
                    conn = nu.connect_any([self.addr], timeout=CONFIG['TIMEOUT_TO_LOG'])
                    if conn:
                        self.pconn = nu.PacketConn(conn, CONFIG['KEEPALIVE_TIMEOUT'], True)

                if not self.pconn:
                    return

                #print('sending', text)
                self.pconn.send(text)
                #print('sent')
            except OSError as e:
                output = ['LogToWorker failed: {}'.format(e).encode()]
                output += [b'|' + x for x in text.split(b'\n')]
                sys.stderr.buffer.write(b'\n'.join(output))


    @staticmethod
    def install():
        logger = logging.getLogger()
        logger.addHandler(LogToWorker(CONFIG['LOG_ADDR']))

        backup_handler = logging.StreamHandler()
        if not LOCAL_SPEW:
            backup_handler.setLevel(logging.CRITICAL)
        logger.addHandler(backup_handler)

# --

def dispatch(mod_name, subkey, fn_pydra_job_client, *args):
    key = make_key(mod_name, subkey)

    addr = CONFIG['JOB_SERVER_ADDR']
    server_conn = nu.connect_any([addr], timeout=CONFIG['TIMEOUT_CLIENT_TO_SERVER'])
    if not server_conn:
        raise ExDispatchFailed('Failed to connect to server: {}'.format(addr))
    server_pconn = nu.PacketConn(server_conn, CONFIG['KEEPALIVE_TIMEOUT'], True)

    try:
        server_pconn.send(b'job')

        server_pconn.send(CONFIG['HOSTNAME'].encode())
        server_pconn.send(key)

        while True:
            wap = WorkerAssignmentPacket.decode(server_pconn.recv())

            addrs = [x.addr for x in wap.addrs]
            worker_conn = nu.connect_any(addrs, timeout=CONFIG['TIMEOUT_TO_WORKER'])
            if not worker_conn:
                logging.error('Failed to connect to worker: {}@{}'.format(wap.hostname, addrs))
                server_pconn.send_t(BOOL_T, False)
                continue
            worker_pconn = nu.PacketConn(worker_conn, CONFIG['KEEPALIVE_TIMEOUT'], True)

            try:
                worker_pconn.send(CONFIG['HOSTNAME'].encode())
                worker_pconn.send(key)

                ret = fn_pydra_job_client(worker_pconn, subkey, *args)
            except OSError:
                ret = None
            finally:
                worker_pconn.nuke()
            if ret == None:
                server_pconn.send(b'')
                continue

            server_pconn.nuke()
            return ret
    except OSError:
        logging.warning('server_conn died:\n' + traceback.format_exc())
    finally:
        server_pconn.nuke()
    return None

