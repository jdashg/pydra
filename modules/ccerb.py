#!/usr/bin/env python3
assert __name__ != '__main__'

import os
import socket
import subprocess
import sys
import threading
import time

from common import *

# --

def get_cc_key(path):
    p = subprocess.run([path, '--version'], capture_output=True)
    if p.stderr:
        key = p.stderr # cl
    else:
        key = p.stdout # cc-like
    (key, _) = key.split(b'\n', 1)
    (_, key) = key.split(b' ', 1)

    logging.info('{} -> {}'.format(path, key))
    return key

# --
# Find some keys!


# --

class ExShimOut(Exception):
    def __init__(self, reason):
        self.reason = reason
        return

# --

SOURCE_EXTS = ['c', 'cc', 'cpp']
BOTH_ARGS = ['-nologo', '-Tc', '-TC', '-Tp', '-TP']

def process_args(cc_args):
    args = cc_args[:]
    if not args:
        raise ExShimOut('no cc_args')

    source_file_name = None
    is_compile_only = False

    preproc = ['-E']
    compile = ['-c']
    while args:
        cur = args.pop(0)

        if cur == '-E':
            raise ExShimOut('preproc-only')

        if cur == '-c':
            is_compile_only = True
            continue

        if cur in ('-H', '-showIncludes'):
            preproc.append(cur)
            continue

        if cur in BOTH_ARGS:
            preproc.append(cur)
            compile.append(cur)
            continue

        if cur == '-I':
            preproc.append(cur)
            try:
                next = args.pop(0)
            except:
                raise ExShimOut('missing arg after -I')
            preproc.append(next)
            continue

        if cur.startswith('-D') or cur.startswith('-I'):
            preproc.append(cur)
            continue

        if cur.startswith('-Tc') or cur.startswith('-Tp'):
            raise ExShimOut('-Tp,-Tc unsupported')

        if cur == '-FI':
            preproc.append(cur)
            try:
                next = args.pop(0)
            except:
                raise ExShimOut('missing arg after -FI')
            preproc.append(next)
            continue

        if cur.startswith('-Fo'):
            if os.path.dirname(cur[2:]):
                raise ExShimOut('-Fo target is a path')
            compile.append(cur)
            continue

        split = cur.rsplit('.', 1)
        if len(split) == 2 and split[1].lower() in SOURCE_EXTS:
            if source_file_name:
                raise ExShimOut('multiple source files')

            source_file_name = os.path.basename(cur)
            preproc.append(cur)
            compile.append(source_file_name)
            continue

        compile.append(cur)
        continue

    if not is_compile_only:
        raise ExShimOut('not compile-only')

    if not source_file_name:
        raise ExShimOut('no source file')

    return (preproc, compile, source_file_name)

####
'''
EXAMPLE_CL_ARGS = [
    'cl.EXE', '-FoUnified_cpp_dom_canvas1.obj', '-c',
    '-Ic:/dev/mozilla/gecko-cinn3-obj/dist/stl_wrappers', '-DDEBUG=1', '-DTRACING=1',
    '-DWIN32_LEAN_AND_MEAN', '-D_WIN32', '-DWIN32', '-D_CRT_RAND_S',
    '-DCERT_CHAIN_PARA_HAS_EXTRA_FIELDS', '-DOS_WIN=1', '-D_UNICODE', '-DCHROMIUM_BUILD',
    '-DU_STATIC_IMPLEMENTATION', '-DUNICODE', '-D_WINDOWS', '-D_SECURE_ATL',
    '-DCOMPILER_MSVC', '-DSTATIC_EXPORTABLE_JS_API', '-DMOZ_HAS_MOZGLUE',
    '-DMOZILLA_INTERNAL_API', '-DIMPL_LIBXUL', '-Ic:/dev/mozilla/gecko-cinn3/dom/canvas',
    '-Ic:/dev/mozilla/gecko-cinn3-obj/dom/canvas',
    '-Ic:/dev/mozilla/gecko-cinn3/js/xpconnect/wrappers',
    '-Ic:/dev/mozilla/gecko-cinn3-obj/ipc/ipdl/_ipdlheaders',
    '-Ic:/dev/mozilla/gecko-cinn3/ipc/chromium/src',
    '-Ic:/dev/mozilla/gecko-cinn3/ipc/glue', '-Ic:/dev/mozilla/gecko-cinn3/dom/workers',
    '-Ic:/dev/mozilla/gecko-cinn3/dom/base', '-Ic:/dev/mozilla/gecko-cinn3/dom/html',
    '-Ic:/dev/mozilla/gecko-cinn3/dom/svg', '-Ic:/dev/mozilla/gecko-cinn3/dom/workers',
    '-Ic:/dev/mozilla/gecko-cinn3/dom/xul', '-Ic:/dev/mozilla/gecko-cinn3/gfx/gl',
    '-Ic:/dev/mozilla/gecko-cinn3/image', '-Ic:/dev/mozilla/gecko-cinn3/js/xpconnect/src',
    '-Ic:/dev/mozilla/gecko-cinn3/layout/generic',
    '-Ic:/dev/mozilla/gecko-cinn3/layout/style',
    '-Ic:/dev/mozilla/gecko-cinn3/layout/xul',
    '-Ic:/dev/mozilla/gecko-cinn3/media/libyuv/include',
    '-Ic:/dev/mozilla/gecko-cinn3/gfx/skia',
    '-Ic:/dev/mozilla/gecko-cinn3/gfx/skia/skia/include/config',
    '-Ic:/dev/mozilla/gecko-cinn3/gfx/skia/skia/include/core',
    '-Ic:/dev/mozilla/gecko-cinn3/gfx/skia/skia/include/gpu',
    '-Ic:/dev/mozilla/gecko-cinn3/gfx/skia/skia/include/utils',
    '-Ic:/dev/mozilla/gecko-cinn3-obj/dist/include',
    '-Ic:/dev/mozilla/gecko-cinn3-obj/dist/include/nspr',
    '-Ic:/dev/mozilla/gecko-cinn3-obj/dist/include/nss', '-MD', '-FI',
    'c:/dev/mozilla/gecko-cinn3-obj/mozilla-config.h', '-DMOZILLA_CLIENT', '-Oy-', '-TP',
    '-nologo', '-wd5026', '-wd5027', '-Zc:sizedDealloc-', '-Zc:threadSafeInit-',
    '-wd4091', '-wd4577', '-D_HAS_EXCEPTIONS=0', '-W3', '-Gy', '-Zc:inline', '-utf-8',
    '-FS', '-Gw', '-wd4251', '-wd4244', '-wd4267', '-wd4345', '-wd4351', '-wd4800',
    '-wd4595', '-we4553', '-GR-', '-Z7', '-Oy-', '-WX',
    '-Ic:/dev/mozilla/gecko-cinn3-obj/dist/include/cairo', '-wd4312',
    'c:/dev/mozilla/gecko-cinn3-obj/dom/canvas/Unified_cpp_dom_canvas1.cpp'
]
'''
# ------------------------------------------------------------------------------

CC_BY_KEY = {}

def pydra_get_subkeys():
    cc_list = [
        'gcc',
        'gcc-6',
        'gcc-7',
        'clang',
        'clang-3',
        'clang-4',
        'clang-5',
        'cl',
    ] + CONFIG['CC_LIST']

    for path in cc_list:
        try:
            key = get_cc_key(path)
        except FileNotFoundError:
            continue
        CC_BY_KEY[key] = path

    assert CC_BY_KEY
    return CC_BY_KEY.keys()

# -

def pydra_shim(fn_dispatch, *mod_args):
    start = time.time()
    def timer_str():
        now = time.time()
        diff = now - start
        return '{:.3f}s'.format(diff)

    logging.debug('<args: {}>'.format(args))

    # =

    if not mod_args:
        raise ExShimOut('no mod_args')

    cc_bin = mod_args[0]
    cc_args = mod_args[1:]

    cc_key = get_cc_key(cc_bin)
    logging.info('<[{}] cc_key: {}>'.format(timer_str(), cc_key))

    # -

    (preproc_args, compile_args, source_file_name) = process_args(cc_args)

    logging.info('<[{}] source_file_name: {}>'.format(timer_str(), source_file_name))
    logging.debug('<<preproc_args: {}>>', preproc_args)
    logging.debug('<<compile_args: {}>>', compile_args)

    has_show_includes = '-showIncludes' in preproc_args
    if has_show_includes:
        preproc_args.append('-nologo')

    # -

    p = subprocess.run([cc_bin] + preproc_args, capture_output=True)
    if p.returncode != 0:
        raise ExShimOut('preproc failed') # Safer to shim out.

    stdout_prefix = b''
    if has_show_includes:
        stdout_prefix = p.stderr

    # -

    ret = fn_dispatch(cc_key, compile_args, source_file_name, preproc_text, stdout_prefix)
    if ret == None:
        raise ExShimOut('dispatch failed')

    # -


    # =

    ccerb.acquire_remote_job(conn, 'wait', PREPROC_PRIORITY)

    with net_util.WaitBeacon(conn):
        (preproc_data, show_includes) = preproc(cc_bin, preproc_args)

    ########

    if not NO_LOCAL:
        t = threading.Thread(target=try_remote_conn,
                             args=(conn, cc_key, LOCAL_COMPILE_PRIORITY))
        t.daemon = True
        t.start()

    for (host, port) in CONFIG['dedicated_remotes'].viewitems():
        if not port:
            (_, port) = ccerb.CCERBD_LOCAL_ADDR
        add_remote_addr((host, port), cc_key, DEDICATED_COMPILE_PRIORITY)

    ####


    return fn_dispatch(b'', delay)

    remote_conn = remotes_future.await()
    ccerb.v_log(2, 'compiler addr: {}', remote_conn.getpeername())

    ########

    input_files = [(source_file_name, preproc_data)]
    try:
        returncode = run_remote_job_client(remote_conn, compile_args, input_files)
    except (socket.timeout, socket.error) as e:
        raise ExShimOut('{}({})'.format(type(e), e))

    if has_show_includes:
        try:
            (file_name, rest) = show_includes.split('\n', 1)
            assert file_name == source_file_name
            sys.stdout.write(rest)
            #ccerb.v_log(1, 'show_includes: {}', show_includes)
        except ValueError:
            pass

    net_util.kill_socket(remote_conn)
    exit(returncode)

except ExShimOut as e:
    v_log(1, '<shimming out: \'{}\'>', e.reason)
    v_log(2, '<<shimming out args: {}>>', args)
    pass



def pydra_job_client(pconn, subkey, delay):
    pconn.send_t(common.F64_T, delay)
    pconn.recv() # To know when we are done.
    return True


def pydra_job_worker(pconn, subkey):
    delay = pconn.recv_t(common.F64_T)
    time.sleep(delay)
    pconn.send(b'\0')
