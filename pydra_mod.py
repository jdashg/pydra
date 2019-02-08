#!/usr/bin/env python3
assert __name__ != '__main__'

import os
from pathlib import *
import socket
import struct
import subprocess
import sys
import threading
import time
import traceback

from common import *

# --

class FileGroup(object):
    def __init__(self):
        self.files = []


    def encode(self):
        bw = ByteWriter()
        bw.pack_t(U64_T, len(self.files))
        for (file_path, file_bytes) in self.files:
            bw.pack_bytes(file_path.encode())
            bw.pack_bytes(file_bytes)
        return bw.data()


    @staticmethod
    def decode(data):
        br = ByteReader(data)
        ret = FileSet()

        num_files = br.unpack_t(U64_T)
        for _ in range(num_files):
            file_path = br.unpack_bytes().decode()
            file_bytes = br.unpack_bytes()
            ret.files.append((file_path, file_bytes))

        return ret


    def add_file(self, root_path, relpath):
        path = root_path / relpath
        b = path.read_bytes()
        self.files.append((relpath, b))


    def add_files(self, root_path):
        for x in walk_path(root_path):
            relpath = x.relative_to(root_path)
            self.add_file(root_path, relpath)


    def write_files(self, root_path):
        for (relpath, b) in self.files:
            path = root_path / relpath
            path.write_bytes(b)

# --

class JobInputs(object):
    def __init__(self):
        self.args = []
        self.files = FileGroup()


    def encode(self):
        bw = ByteWriter()

        bw.pack_t(U64_T, len(self.args))
        for x in self.args:
            bw.pack_bytes(x.encode())

        b = self.files.encode()
        bw.pack_bytes(b)

        return bw.data()


    @staticmethod
    def decode(data):
        br = ByteReader(data)
        ret = JobInputs()

        num_args = br.unpack_t(U64_T);
        ret.args = [br.unpack_bytes().decode() for _ in range(num_args)]

        b = br.unpack_bytes()
        ret.files = FileGroup.decode(b)

        return ret

# --

class JobOutputs(object):
    def __init__(self, data):
        self.returncode = None
        self.stdout = None
        self.stderr = None
        self.files = FileGroup()


    def encode(self):
        bw = ByteWriter()

        bw.pack_t(I32_T, self.returncode)
        bw.pack_bytes(self.stdout)
        bw.pack_bytes(self.stderr)

        b = self.files.encode()
        bw.pack_bytes(b)

        return bw.data()


    @staticmethod
    def decode(data):
        br = ByteReader(data)
        ret = JobOutputs()

        ret.returncode = br.unpack_t(I32_T)
        ret.stdout = br.unpack_bytes()
        ret.stderr = br.unpack_bytes()

        b = br.unpack_bytes()
        ret.files = FileGroup.decode(b)

        return ret

# --

MODULE_DIRS = [
    PYDRA_HOME / 'modules',
    Path(__file__).parent / 'modules',
]

def LoadModule(mod_name):
    file_name = mod_name + '.py'

    for x in MODULE_DIRS:
        path = x / file_name
        path = path.resolve()
        if path.exists():
            break
    else:
        raise FileNotFoundError(path_bases[0] / file_name)

    import importlib.util

    spec = importlib.util.spec_from_file_location(path.stem, path.as_posix())
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


def LoadModules():
    mod_names = set()
    for d in MODULE_DIRS:
        if d.exists():
            names = [x.stem for x in d.iterdir() if x.suffix == '.py']
            mod_names.update(names)

    mods = {}
    for name in mod_names:
        try:
            mods[name] = LoadModule(name)
        except FileNotFoundError:
            pass
    return mods
