"""
.. module:: pmemobj
.. moduleauthor:: R. David Murray <rdmurray@bitdance.com>

:mod:`pmemobj` -- pmem-resident objects
=========================================================

"""
import errno
import os
import sys
from _pmem import lib, ffi

# If we ever need to change how we make use of the persistent store, having a
# version as the layout will allow us to provide backward compatibility.
layout_info = (0, 0, 1)
layout_version = 'pypmemobj-{}.{}.{}'.format(*layout_info).encode()

MIN_POOL_SIZE = lib.PMEMOBJ_MIN_POOL
MAX_OBJ_SIZE = lib.PMEMOBJ_MAX_ALLOC_SIZE

# XXX move this to a central location and use in all libraries.
def coerce_fn(file_name):
    """Return 'char *' compatible file_name on both python2 and python3."""
    if sys.version_info[0] > 2 and hasattr(file_name, 'encode'):
        file_name = file_name.encode(errors='surrogateescape')
    return file_name

# This could also be centralized except that there is a per-library error
# message function.
def check_ret(value):
    """Raise appropriate error if value is ffi.NULL.

    Convert EINVAL into ValueError, all others (for the moment) into
    OSError.  Obtain the message from the pmem library.
    """
    if value != ffi.NULL:
        return
    err = ffi.errno
    msg = ffi.string(lib.pmemobj_errormsg())
    if err == errno.EINVAL:
        raise ValueError(msg)
    else:
        raise OSError(msg)

class PersistentObjectPool(object):
    """This class represents the persistent object pool created using
    :func:`~nvm.pmemobj.create` or :func:`~nvm.pmemobj.open`.
    """

    def __init__(self, pool_ptr):
        self.pool_ptr = pool_ptr
        self.closed = False

    def close(self):
        """This method closes the object pool.  The object pool itself lives on
        in the file that contains it and may be reopened at a later date, and
        all the objects in it accessed, using :func:`~nvm.pmemlog.open`.
        """
        lib.pmemobj_close(self.pool_ptr)
        self.closed = True

    def __del__(self):
        if not self.closed:
            self.close()


def open(filename):
    """This function opens an existing object pool, returning a
    :class:`PersistentObjectPool`.

    Raises RuntimeError if the file cannot be opened or mapped.

    :param filename: Filename must be an existing file containing an object
                     pool as created by :func:`nvm.pmemlog.create`.
                     The application must have permission to open the file
                     and memory map it with read/write permissions.
    :return: a :class:`PersistentObjectPool` instance that manages the pool.
    """
    ret = lib.pmemobj_open(coerce_fn(filename), layout_version)
    check_ret(ret)
    return PersistentObjectPool(ret)

def create(filename, pool_size=MIN_POOL_SIZE, mode=0o666):
    """The `create()` function creates an object pool with the given total
    `pool_size`.  Since the transactional nature of an object pool requires
    some space overhead, and immutable values are stored alongside the mutable
    containers that point to them, the space requirement of a given set of
    objects is ocnsiderably larger than a naive calculation based on
    sys.getsize would suggest.

    Raises RuntimeError if the file cannot be created or mapped.

    :param filename: specifies the name of the objectpool file to be created.
    :param pool_size: the size of the object pool in bytes.  The default
                      is pmemobj.MIN_POOL_SIZE.
    :param mode: specifies the permissions to use when creating the file.
    :return: a :class:`PersistentObjectPool` instance that manages the pool.
    """
    ret = lib.pmemobj_create(coerce_fn(filename), layout_version,
                             pool_size, mode)
    check_ret(ret)
    return PersistentObjectPool(ret)
