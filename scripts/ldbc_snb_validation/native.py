"""Load Rust-built verifier indexes directly into CPython."""

from collections import ChainMap
import ctypes
import os
from weakref import finalize

from graph_io import Catalog
from model import Message

_DECREF = ctypes.pythonapi.Py_DecRef
_DECREF.argtypes = [ctypes.c_void_p]
_DECREF.restype = None
PATH = ctypes.c_char_p
OBJECT = ctypes.py_object
POINTER = ctypes.c_void_p
POINTER_OUT = ctypes.POINTER(POINTER)


def take(pointer):
    if not pointer:
        raise RuntimeError("Rust graph verifier returned no result")
    value = ctypes.cast(pointer, ctypes.py_object).value
    _DECREF(pointer)
    return value


class NativeKeys:
    def __init__(self, library, pointer):
        self.library = library
        self.pointer = pointer
        finalize(self, library.snb_drop_keys, pointer)

    def __getitem__(self, ident):
        if not 0 <= ident <= 100_000_000:
            raise KeyError(ident)
        return take(self.library.snb_key(self.pointer, ident))


def open_library(library_path):
    library = ctypes.PyDLL(os.fspath(library_path))
    library.snb_key.argtypes = [POINTER, ctypes.c_uint32]
    library.snb_key.restype = POINTER
    library.snb_drop_keys.argtypes = [POINTER]
    library.snb_drop_keys.restype = None
    return library


def load(library_path, data, output, writes, model):
    library = open_library(library_path)
    library.snb_snapshot.argtypes = [
        PATH,
        PATH,
        PATH,
        OBJECT,
        OBJECT,
        OBJECT,
        POINTER_OUT,
        POINTER_OUT,
    ]
    library.snb_snapshot.restype = POINTER
    library.snb_wait.argtypes = [POINTER, OBJECT]
    library.snb_wait.restype = POINTER
    capture = POINTER()
    keys = POINTER()
    try:
        try:
            scn, extent = take(
                library.snb_snapshot(
                    os.fsencode(data),
                    os.fsencode(output),
                    os.fsencode(writes),
                    model,
                    Message,
                    None,
                    ctypes.byref(capture),
                    ctypes.byref(keys),
                )
            )
        finally:
            if capture:
                take(library.snb_wait(capture, None))
    except BaseException:
        if keys:
            library.snb_drop_keys(keys)
        raise
    return Catalog(ChainMap({}, NativeKeys(library, keys)), scn, extent)


def rebuild(library_path, data, snapshot, model):
    library = open_library(library_path)
    library.snb_rebuild.argtypes = [
        PATH,
        ctypes.c_uint64,
        ctypes.c_uint32,
        OBJECT,
        OBJECT,
        OBJECT,
        POINTER_OUT,
    ]
    library.snb_rebuild.restype = POINTER
    keys = POINTER()
    extent = snapshot["max_element_id"]
    try:
        counts = take(
            library.snb_rebuild(
                os.fsencode(data),
                snapshot["snapshot_scn"],
                extent,
                model,
                Message,
                None,
                ctypes.byref(keys),
            )
        )
        if counts != snapshot["role_rows"]:
            raise ValueError(
                "Source clone row counts differ from the verified snapshot"
            )
    except BaseException:
        if keys:
            library.snb_drop_keys(keys)
        raise
    return Catalog(
        ChainMap({}, NativeKeys(library, keys)), snapshot["snapshot_scn"], extent
    )
