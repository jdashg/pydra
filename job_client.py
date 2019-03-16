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
        try:
            with self.lock:
                if self.pconn == None:
                    self.pconn = False
                    conn = nu.connect_any([self.addr], timeout=CONFIG['TIMEOUT_TO_LOG'])
                    if conn:
                        self.pconn = nu.PacketConn(conn, CONFIG['KEEPALIVE_TIMEOUT'], True)

            if self.pconn:
                self.pconn.send(text)
                return
        except OSError:
            pass

        log_path = PYDRA_HOME / 'failsafe.log'
        with log_path.open('ab', buffering=0) as f:
            f.write(text.decode())


    @staticmethod
    def install():
        logger = logging.getLogger()
        logger.addHandler(LogToWorker(CONFIG['LOG_ADDR']))

        backup_handler = logging.StreamHandler()
        if not LOCAL_SPEW:
            backup_handler.setLevel(logging.CRITICAL)
        logger.addHandler(backup_handler)

# -----------------

class PydraInterface(object):
    def __init__(self, mod_name):
        self.mod_name = mod_name
        self.python_module = LoadPydraModule(mod_name)


    def shim(self, *args):
        return self.python_module.pydra_shim(self, *args)


    def register_job(self, subkey):
        timeout = CONFIG['TIMEOUT_CLIENT_TO_SERVER']
        addr = job_server_addr(timeout)
        if not addr:
            raise OSError('Failed to resolve mDNS job_server.')
        conn = nu.connect_any([addr[:2]], timeout=timeout)
        if not conn:
            raise OSError(f'Failed to connect to server: {addr}')

        pconn = nu.PacketConn(conn, CONFIG['KEEPALIVE_TIMEOUT'], True)
        pconn.send(b'job')

        job = RegisteredJob(self, subkey, pconn)

        pconn.send(CONFIG['HOSTNAME'].encode())
        pconn.send(job.key)
        return job

# -

class RegisteredJob(object):
    def __init__(self, iface, subkey, server_pconn):
        self.iface = iface
        self.subkey = subkey
        self.server_pconn = server_pconn
        self.key = make_key(iface.mod_name, subkey)


    def job_workers(self):
        self.server_pconn.send(b'job_workers')
        return JobWorkersDescriptor.decode(self.server_pconn.recv())


    def dispatch(self, *args, **kwargs):
        self.server_pconn.send(b'request_worker')
        wap = WorkerAssignmentPacket.decode(self.server_pconn.recv())

        addrs = [x.addr for x in wap.addrs]
        worker_conn = nu.connect_any(addrs, timeout=CONFIG['TIMEOUT_TO_WORKER'])
        if not worker_conn:
            logging.error('Failed to connect to worker: %s@%s', wap.hostname, addrs)
            return None

        worker_pconn = nu.PacketConn(worker_conn, CONFIG['KEEPALIVE_TIMEOUT'], True)
        try:
            worker_pconn.send(CONFIG['HOSTNAME'].encode())
            worker_pconn.send(self.key)

            fn_job_client = self.iface.python_module.pydra_job_client
            return fn_job_client(worker_pconn, self.subkey, *args, **kwargs)
        except OSError:
            return None
        finally:
            worker_pconn.nuke()
