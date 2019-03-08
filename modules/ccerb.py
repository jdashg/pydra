#!/usr/bin/env python3
assert __name__ != '__main__'

import lzma
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import zlib

from common import *

# --

COMPRESS_ZLIB_LEVEL = 0
COMPRESS_LZMA = False

COMPRESS_ZLIB_LEVEL = 1 # ~115Mbps compressing
#COMPRESS_ZLIB_LEVEL = 6 # ~35Mbps compressing
#COMPRESS_LZMA = True # ~2Mbps compressing

# --

RE_PARENS = re.compile(b'[(][^)]+[)]')

def get_cc_key(path):
    p = subprocess.run([path, '--version'], capture_output=True)
    if p.stderr:
        key = p.stderr # cl
    else:
        key = p.stdout # cc-like
    key = key.split(b'\n', 1)[0].strip()

    if b'(GCC)' in key:
        key = b' '.join(key.split(b' ')[1:2])

    parens = RE_PARENS.search(key)
    if parens:
        key = key.replace(parens.group(0), b'', 1)

    #logging.info('{} -> {}'.format(path, key))
    return key

# --

class ExShimOut(Exception):
    def __init__(self, reason, log_func=logging.debug):
        self.reason = reason
        self.log_func = log_func
        return


    def log(self, mod_args):
        self.log_func('Shimming out: \'{}\': {}'.format(self.reason, mod_args))

# --

def process_args(cc_args):
    args = list(cc_args)
    if not args:
        raise ExShimOut('no cc_args')

    source_file_name = None
    is_compile_only = False

    preproc = ['-E']
    compile = ['-c']
    while args:
        cur = args.pop(0)

        if cur == '-E':
            raise ExShimOut('already preproc-only')

        if cur == '-c':
            is_compile_only = True
            continue

        if cur in ('-H', '-showIncludes'):
            preproc.append(cur)
            continue

        #if cur in ('-nologo', '-TC', '-TP', '-MD', '-MDd', '-MT', '-MTd'):
        #    preproc.append(cur)
        #    compile.append(cur)
        #    continue
        #
        #if cur.startswith('-m'): # -mavx2
        #    preproc.append(cur)
        #    compile.append(cur)
        #    continue

        if cur == '-I':
            preproc.append(cur)
            try:
                next = args.pop(0)
            except:
                raise ExShimOut('missing arg after -I', logging.error)
            preproc.append(next)
            continue

        if cur.startswith('-D') or cur.startswith('-I'):
            preproc.append(cur)
            continue

        if cur.startswith('-Tc') or cur.startswith('-Tp'):
            raise ExShimOut('TODO: -Tp,-Tc unsupported', logging.warning)

        if cur == '-FI':
            preproc.append(cur)
            try:
                next = args.pop(0)
            except:
                raise ExShimOut('missing arg after -FI', logging.error)
            preproc.append(next)
            continue

        if cur.startswith('-Fo'):
            if os.path.dirname(cur[2:]):
                raise ExShimOut('TODO: -Fo target is a path', logging.warning)
            compile.append(cur)
            continue

        if cur == '-Xclang':
            try:
                next = args.pop(0)
            except:
                raise ExShimOut('missing arg after -Xclang', logging.error)

            if next == '-MP':
                preproc += [cur, next]
                continue

            if next.startswith('-std'):
                compile += [cur, next]
                continue

            if next in ('-dependency-file', '-MT'):
                try:
                    next_xclang = args.pop(0)
                    assert next_xclang == '-Xclang'
                    next_path = args.pop(0)
                except:
                    raise ExShimOut('missing args after -Xclang ' + next, logging.error)
                preproc += [cur, next, next_xclang, next_path]
                continue

            raise ExShimOut('TODO: unrecognized arg after -Xclang: ' + next, logging.error)

        split = cur.rsplit('.', 1)
        if len(split) == 2 and split[1].lower() in ('c', 'cc', 'cpp'):
            if source_file_name:
                raise ExShimOut('TODO: multiple source files', logging.warning)

            source_file_name = os.path.basename(cur)
            preproc.append(cur)
            compile.append(source_file_name)
            continue

        # Send to both!
        preproc.append(cur)
        compile.append(cur)
        continue

    if not is_compile_only:
        raise ExShimOut('not compile-only')

    if not source_file_name:
        raise ExShimOut('no source file detected', logging.warning)

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
'''
[log 17]  <<running: ['C:\\Users\\khetu\\.mozbuild\\clang\\bin\\clang-cl.exe',
'-c', '-FoUnified_cpp_js_src_jit2.obj',
'-MD',
'-Qunused-arguments', '-guard:cf',
'-Qunused-arguments',
'-TP', '-nologo', '-wd4800', '-wd4595', '-w15038', '-wd5026',
'-wd5027', '-Zc:sizedDealloc-', '-guard:cf', '-W3', '-Gy', '-Zc:inline', '-Gw', '-wd4244',
'-wd4267', '-wd4251', '-wd4065', '-Wno-inline-new-delete', '-Wno-invalid-offsetof',
'-Wno-microsoft-enum-value', '-Wno-microsoft-include', '-Wno-unknown-pragmas',
'-Wno-ignored-pragmas', '-Wno-deprecated-declarations', '-Wno-invalid-noreturn',
'-Wno-inconsistent-missing-override', '-Wno-implicit-exception-spec-mismatch',
'-Wno-unused-local-typedef', '-Wno-ignored-attributes', '-Wno-used-but-marked-unused',
'-we4553', '-GR-', '-Z7', '-Oy-', '-WX', '-wd4805', '-wd4661', '-wd4146', '-wd4312',
'-Xclang', '-MP', '-Xclang', '-dependency-file',
'-Xclang', '.deps/Unified_cpp_js_src_jit2.obj.pp', '-Xclang', '-MT',
'-Xclang', 'Unified_cpp_js_src_jit2.obj', 'Unified_cpp_js_src_jit2.cpp']>>
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

def read_files(root_dir):
    ret = []
    for cur_root, cur_dirs, cur_files in os.walk(root_dir):
        for x in cur_files:
            path = os.path.join(cur_root, x)
            with open(path, 'rb') as f:
                data = f.read()
            logging.debug('<read {} ({} bytes)>'.format(path, len(data)))

            rel_path = os.path.relpath(path, root_dir)
            ret.append([rel_path, data])
    return ret


def write_files(root_dir, files):
    for (file_rel_path, file_data) in files:
        dir_name = os.path.dirname(file_rel_path)
        if dir_name:
            os.makedirs(dir_name)
        file_path = os.path.join(root_dir, file_rel_path)
        with open(file_path, 'wb') as f:
            f.write(file_data)
        logging.debug('<wrote {} ({} bytes)>'.format(file_path, len(file_data)))

# -

def pydra_shim(fn_dispatch, *mod_args):
    t = MsTimer()

    logging.debug('<mod_args: {}>'.format(mod_args))

    # -

    try:
        if not mod_args:
            raise ExShimOut('no mod_args')

        cc_bin = mod_args[0]
        cc_args = mod_args[1:]

        cc_key = get_cc_key(cc_bin)
        logging.debug('<[{}] cc_key: {}>'.format(t.time(), cc_key))

        # -

        (preproc_args, compile_args, source_file_name) = process_args(cc_args)

        logging.info('  {}: ({}) Preproc...'.format(source_file_name, t.time()))
        logging.debug('    {}: mod_args: {}'.format(source_file_name, preproc_args))
        logging.debug('    {}: preproc_args: {}'.format(source_file_name, preproc_args))
        logging.debug('    {}: compile_args: {}'.format(source_file_name, compile_args))

        has_show_includes = '-showIncludes' in preproc_args
        if has_show_includes:
            preproc_args.append('-nologo')

        # -

        p = subprocess.run([cc_bin] + preproc_args, capture_output=True)
        if p.returncode != 0:
            raise ExShimOut('preproc failed', logging.info) # Safer to shim out.
        preproc_data = p.stdout
        preproc_time = t.time()
        logging.info('  {}: ({}) Preproc complete. ({} bytes) Dispatch...'.format(source_file_name,
                preproc_time, len(preproc_data)))

        stdout_prefix = b''
        if has_show_includes:
            stdout_prefix = p.stderr

        # -

        ret = fn_dispatch(cc_key, compile_args, source_file_name, preproc_data)
        if ret == None:
            raise ExShimOut('dispatch failed')
        (retcode, stdout, stderr, output_files) = ret
        total_bytes = sum([len(x) for (_,x) in output_files])
        logging.info('  {}: ({}) Dispatch complete. ({} bytes, {} files) Writing...'.format(
                source_file_name, t.time(), total_bytes, len(output_files)))

        # --

        write_files(os.getcwd(), output_files)

        sys.stdout.buffer.write(stdout_prefix)
        sys.stdout.buffer.write(stdout)
        sys.stderr.buffer.write(stderr)
        total_time = t.time()
        preproc_percent = int(100.0 * float(preproc_time) / float(total_time))
        logging.warning('{}: ({}, {}={}% preproc) Complete.'.format(source_file_name, total_time, preproc_time, preproc_percent))
        exit(retcode)
    except ExShimOut as e:
        e.log(mod_args)
        p = subprocess.run(mod_args)
        exit(p.returncode)

# -

class ScopedTempDir:
    def __init__(self):
        return

    def __enter__(self):
        self.path = tempfile.mkdtemp()
        return self

    def __exit__(self, ex_type, ex_val, ex_traceback):
        shutil.rmtree(self.path)
        return

# -

def run_in_temp_dir(input_files, args):
    with ScopedTempDir() as temp_dir:
        write_files(temp_dir.path, input_files)

        logging.debug('<<running: {}>>'.format(args))
        p = subprocess.run(args, cwd=temp_dir.path, capture_output=True)

        for (file_rel_path, _) in input_files:
            file_path = os.path.join(temp_dir.path, file_rel_path)
            os.remove(file_path)
            continue

        output_files = read_files(temp_dir.path)

    return (p.returncode, p.stdout, p.stderr, output_files)

# -

SPEW_COMPRESSION_INFO = True

def compress(data, name):
    t = MsTimer()
    d_size = len(data)

    if COMPRESS_ZLIB_LEVEL:
        data = zlib.compress(data, level=COMPRESS_ZLIB_LEVEL)

    if COMPRESS_LZMA:
        data = lzma.compress(data)

    c_size = len(data)

    diff = t.time()
    try:
        mbps = ((d_size - c_size) / 1000 / 1000) / (float(diff) / 1000)
    except ZeroDivisionError:
        mbps = float('Inf')
    percent = int(c_size / d_size * 100)
    if SPEW_COMPRESSION_INFO:
        logging.info('  <compress({}): {:.3f} Mb/s: {}->{} bytes ({}%) in {}>'.format(name, mbps, d_size, c_size, percent, str(diff)))

    return data


def decompress(data, name=''):
    t = MsTimer()
    c_size = len(data)

    if COMPRESS_LZMA:
        data = lzma.decompress(data)

    if COMPRESS_ZLIB_LEVEL:
        data = zlib.decompress(data)

    d_size = len(data)

    diff = t.time()
    try:
        mbps = ((d_size - c_size) / 1000 / 1000) / (float(diff) / 1000)
    except ZeroDivisionError:
        mbps = float('Inf')
    percent = int(c_size / d_size * 100)
    if SPEW_COMPRESSION_INFO:
        logging.debug('  <decompress({}): {:.3f} Mb/s: {}->{} bytes ({}%) in {}>'.format(name, mbps, c_size, d_size, percent, str(diff)))

    return data

# -

def pydra_job_client(pconn, subkey, compile_args, source_file_name, preproc_data):
    preproc_data = compress(preproc_data, source_file_name)

    # -

    for x in compile_args:
        pconn.send(x.encode())
    pconn.send(b'')
    pconn.send(source_file_name.encode())
    pconn.send(preproc_data)

    # -

    retcode = pconn.recv_t(I32_T)
    stdout = pconn.recv()
    stderr = pconn.recv()

    output_files = []
    while True:
        name = pconn.recv()
        if not name:
            break
        data = pconn.recv()

        output_files.append( [name.decode(), data] )

    pconn.shutdown()

    for n_d in output_files:
        n_d[1] = decompress(n_d[1], n_d[0])

    return (retcode, stdout, stderr, output_files)


def pydra_job_worker(pconn, subkey):
    cc_bin = CC_BY_KEY[subkey]
    compile_args = [cc_bin]
    while True:
        x = pconn.recv()
        if not x:
            break
        compile_args.append(x.decode())
    source_file_name = pconn.recv().decode()
    preproc_data = pconn.recv()

    input_files = [[source_file_name, preproc_data]]

    for n_d in input_files:
        n_d[1] = decompress(n_d[1], n_d[0])

    (retcode, stdout, stderr, output_files) = run_in_temp_dir(input_files, compile_args)

    for n_d in output_files:
        n_d[1] = compress(n_d[1], n_d[0])

    pconn.send_t(I32_T, retcode)
    pconn.send(stdout)
    pconn.send(stderr)

    for (name,data) in output_files:
        pconn.send(name.encode())
        pconn.send(data)
    pconn.send(b'')

    pconn.send_shutdown()
