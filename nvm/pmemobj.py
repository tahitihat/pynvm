"""
.. module:: pmemobj
.. moduleauthor:: R. David Murray <rdmurray@bitdance.com>

:mod:`pmemobj` -- pmem-resident objects
=========================================================

"""
try:
    import collections.abc as abc
except ImportError:
    import collections as abc
    import faulthandler
    faulthandler.enable()
import errno
if not hasattr(errno, 'ECANCELED'):
    errno.ECANCELED = 125  # 2.7 errno doesn't define this, so guess.
import os
import sys
from pickle import whichmodule
from threading import RLock
from _pmem import lib, ffi

# If we ever need to change how we make use of the persistent store, having a
# version as the layout will allow us to provide backward compatibility.
layout_info = (0, 0, 1)
layout_version = 'pypmemobj-{}.{}.{}'.format(*layout_info).encode()

MIN_POOL_SIZE = lib.PMEMOBJ_MIN_POOL
MAX_OBJ_SIZE = lib.PMEMOBJ_MAX_ALLOC_SIZE
OID_NULL = lib.OID_NULL

def _is_OID_NULL(oid):
    # XXX I think == should work here, but it doesn't.
    return (oid.pool_uuid_lo == OID_NULL.pool_uuid_lo
            and oid.off == OID_NULL.off)

# XXX move this to a central location and use in all libraries.
def _coerce_fn(file_name):
    """Return 'char *' compatible file_name on both python2 and python3."""
    if sys.version_info[0] > 2 and hasattr(file_name, 'encode'):
        file_name = file_name.encode(errors='surrogateescape')
    return file_name

# This could also be centralized except that there is a per-library error
# message function.
def _raise_per_errno():
    """Raise appropriate error, based on current errno using current message.

    Assume the pmem library has detected an error, and use the current errno
    and errormessage to raise an appropriate Python exception.  Convert EINVAL
    into ValueError, ENOMEM into MemoryError, and all others into OSError.
    """
    # XXX should probably check for errno 0 and/or an unset message.
    err = ffi.errno
    msg = ffi.string(lib.pmemobj_errormsg())
    # In python3 OSError would do this check for us.
    if err == errno.EINVAL:
        raise ValueError(msg)
    elif err == errno.ENOMEM:
        raise MemoryError(msg)
    else:
        # In Python3 some errnos may result in subclass exceptions, but
        # the above are not covered by the OSError subclass logic.
        raise OSError(err, msg)

def _check_null(value):
    """Raise an error if value is NULL."""
    if value == ffi.NULL:
        _raise_per_errno()
    return value

def _check_errno(errno):
    """Raise an error if errno is not zero."""
    if errno:
        _raise_per_errno()

def _check_oid(oid):
    """Raise an error if oid is OID_NULL, otherwise return it.
    """
    if _is_OID_NULL(oid):
        _raise_per_errno()
    return oid

_class_string_cache = {}
def _class_string(cls):
    """Return a string we can use later to find the base class of cls.

    Raise TypeError if we can't compute a useful string.
    """
    try:
        return _class_string_cache[cls]
    except KeyError:
        pass
    # Most of this is borrowed from pickle.py
    name = getattr(cls, '__qualname__', None)
    if name is None:
        name = cls.__name__
    module_name = whichmodule(cls, name)
    try:
        __import__(module_name, level=0)
        module = sys.modules[module_name]
        obj2 = module
        for subpath in name.split('.'):
            if subpath == '<locals>':
                raise AttributeError(
                    "<locals> in class name {}".format(name))
            try:
                parent = obj2
                obj2 = getattr(obj2, subpath)
            except AttributeError:
                raise AttributeError(
                    "Can't get attribute {!r} in {!r}".format(name, obj2))
    except (ImportError, KeyError, AttributeError):
        raise TypeError("Can't persist {!r} instance, class not found"
                        " as {}.{}".format(cls, module_name, name))
    else:
        if obj2 is not cls:
            raise TypeError("Can't persist {!r} instance, class is"
                            " not the same object as {}.{}".format(
                            cls, module_name, name))
    if module_name == '__builtin__':
        module_name = 'builtins'
    res = _class_string_cache[cls] = "{}:{}".format(module_name, name)
    return res

_class_from_string_cache = {}
def _find_class_from_string(cls_string):
    """Return class object corresponding to class_string."""
    try:
        return _class_from_string_cache[cls_string]
    except KeyError:
        pass
    module_name, name = cls_string.split(':')
    if module_name == 'builtins' and sys.version_info[0] < 3:
        module_name = '__builtin__'
    __import__(module_name, level=0)
    res = getattr(sys.modules[module_name], name)
    _class_from_string_cache[cls_string] =  res
    return res


class PersistentObjectPool(object):
    """This class represents the persistent object pool created using
    :func:`~nvm.pmemobj.create` or :func:`~nvm.pmemobj.open`.
    """

    lock = RLock()

    #
    # Pool management
    #

    # XXX create should be a keyword-only arg but we don't have those in 2.7.
    def __init__(self, pool_ptr, filename, create=False):
        self._pool_ptr = pool_ptr
        self.filename = filename
        self.closed = False
        if create:
            self._type_table = None
            self.root = None
            self._init_caches()
            return
        # I don't think we can wrap a transaction around pool creation,
        # since we aren't using pmemobj_root_construct, so we need to check
        # each of the steps we can't bracket.  But if the type table pointer
        # is non-zero we know initialization is complete, since we have a
        # transaction wrapped around the setup that comes after the initial
        # root pmem-object creation.
        with self.lock:
            self._init_caches()
            size = lib.pmemobj_root_size(self._pool_ptr)
            if size:
                pmem_root = self._direct(lib.pmemobj_root(self._pool_ptr, 0))
                pmem_root = ffi.cast('PRoot *', pmem_root)
            # XXX I'm not sure the check for pmem_root not NULL is needed.
            if not size or pmem_root == ffi.NULL or not pmem_root.type_table:
                raise RuntimeError("Pool {} not initialized completely".format(
                    self.filename))
            self._type_table = self._resurrect(pmem_root.type_table)
            # XXX need to make root a property for assignment reasons
            self.root = self._resurrect(pmem_root.root_object)

    def close(self):
        """This method closes the object pool.  The object pool itself lives on
        in the file that contains it and may be reopened at a later date, and
        all the objects in it accessed, using :func:`~nvm.pmemlog.open`.
        """
        lib.pmemobj_close(self._pool_ptr)
        self.closed = True

    def __del__(self):
        if not self.closed:
            self.close()

    #
    # Transaction management
    #

    def begin_transaction(self):
        """Start a new (sub)transaction."""
        _check_errno(lib.pmemobj_tx_begin(self._pool_ptr, ffi.NULL, ffi.NULL))

    def commit_transaction(self):
        """Commit the current (sub)transaction."""
        lib.pmemobj_tx_commit()
        _check_errno(lib.pmemobj_tx_end())

    def _end_transaction(self):
        """End the current (sub)transaction.

        Raise an error if the returned errno is not 0 or ECNANCELED.
        """
        if lib.pmemobj_tx_end() not in (0, errno.ECANCELED):
            _raise_per_errno()

    def abort_transaction(self, errno=errno.ECANCELED):
        """Abort the current (sub)transaction."""
        lib.pmemobj_tx_abort(errno)
        self._end_transaction()

    def __enter__(self):
        self.begin_transaction()

    def __exit__(self, *args):
        stage = lib.pmemobj_tx_stage()
        if stage == lib.TX_STAGE_WORK:
            if args[0] is None:
                # If this fails we get a non-zero errno from tx_end and
                # _end_transaction will raise it.
                lib.pmemobj_tx_commit()
            else:
                # We have a Python exception that didn't result from an error
                # in the pmemobj library, so manually roll back the transaction
                # since the python block won't complete.
                # XXX we should maybe use a unique error code here and raise an
                # error on ECANCELED, I'm not sure.
                self.abort_transaction()
                # XXX It seems like we can't call tx_end after this, at least
                # not in debug mode, despite the docs saying we should.
                return
        self._end_transaction()

    #
    # Memory management
    #
    # Although I'm defining these on the POP class, there is in fact global
    # state involved here.  These methods will work only when a transaction is
    # active, and the global state means that only one object pool may have a
    # transaction executing in a given thread at a time.  XXX should add a
    # check for this, probably in __enter__.

    def _malloc(self, size):
        """Return a pointer to size bytes of newly allocated persistent memory.
        """
        # For now, just use the underlying allocator...later we'll write a
        # small object allocator on top, and the pointer size will grow.
        return _check_oid(lib.pmemobj_tx_zalloc(size, 0))

    def _malloc_ptrs(self, count):
        """Return pointer to enough persistent memory for count pointers.
        """
        return self._malloc(count * ffi.sizeof('PObjPtr'))

    def _realloc(self, oid, size):
        """Copy oid contents into size bytes of new persistent memory.

        Return pointer to the new memory.
        """
        return _check_oid(lib.pmemobj_tx_zrealloc(oid, size, 0))

    def _realloc_ptrs(self, oid, count):
        """As _realloc, but the new memory is enough for count pointers."""
        return self._realloc(oid, count * ffi.sizeof('PObjPtr'))

    def _free(self, oid):
        """Free the memory pointed to by oid."""
        _check_errno(lib.pmemobj_tx_free(oid))

    def _direct(self, oid):
        """Return the real memory address where oid lives."""
        return _check_null(lib.pmemobj_direct(oid))

    def _tx_add_range_direct(self, ptr, size):
        lib.pmemobj_tx_add_range_direct(ptr, size)

    #
    # Object Management
    #

    def _init_caches(self):
        # We have a couple of special cases to avoid infinite regress.
        self._type_code_cache = {PersistentList: 0, str: 1}
        self._persist_cache = {}
        self._resurrect_cache = {OID_NULL: None}   # XXX WeakValueDictionary?

    def _get_type_code(self, cls):
        """Return the index into the type table for cls.

        Create the type table entry if required.
        """
        try:
            return self._type_code_cache[cls]
        except KeyError:
            pass
        cls_str = _class_string(cls)
        try:
            return self._type_table.index(cls_str)
        except ValueError:
            self._type_table.append(cls_str)
            return len(self._type_table) - 1

    def _persist(self, obj):
        """Store obj in persistent memory and return its oid."""
        key = obj if hasattr(obj, '__hash__') else id(obj)
        try:
            return self._persist_cache[key]
        except KeyError:
            pass
        if hasattr(obj, '__manager__'):
            return obj._oid
        cls_str = _class_string(obj.__class__)
        persister = '_persist_' + cls_str.replace(':', '_')
        if not hasattr(self, persister):
            raise TypeError("Don't know now to persist {!r}".format(cls_str))
        res = self._persist_cache[key] = getattr(self, persister)(obj)
        return res

    def _resurrect(self, oid):
        """Return python object representing the data stored at oid."""
        try:
            # XXX The fact that oid == OID_NULL doesn't work probably
            # implies that this cache isn't going to work either, so
            # we'll need to debug that soon.
            return self._resurrect_cache[oid]
        except KeyError:
            pass
        if _is_OID_NULL(oid):
            # XXX I'm not sure we can get away with mapping OID_NULL
            # to None here, but try it and see.
            self._resurrect_cache[oid] = None
            return None
        obj_head = ffi.cast('PObject *', self._direct(oid))
        type_code = obj_head.ob_type
        # The special cases are to avoid infinite regress in the type table.
        if type_code == 0:
            res = PersistentList(__manager__=self, _oid=oid)
            self._resurrect_cache[oid] = res
            return res
        if type_code == 1:
            cls_str = 'builtins:str'
        else:
            cls_str = self._type_table[type_code]
        resurrector = '_resurrect_' + cls_str.replace(':', '_')
        if not hasattr(self, resurrector):
            # It must be a persistent type.
            cls = find_class_from_string(cls_str)
            return cls(__manager__=self, _oid=oid)
        body = ffi.cast('char *', object_head) + ffi.sizeof('PObject')
        res = self._resurrect_cache[oid] = resurrector(body)
        return res

    def _persist_builtins_str(self, obj):
        # XXX punt on figuring out how to deal with python2 bytes for now.
        chars = obj.encode('utf-8')
        with self:
            p_str_oid = self._malloc(ffi.sizeof('PObject') + len(chars) + 1)
            p_str = ffi.cast('PObject *', self._direct(p_str_oid))
            p_str.ob_type = self._get_type_code(obj.__class__)
            body = ffi.cast('char *', p_str) + ffi.sizeof('PObject')
            body = chars
        return p_str_oid

    def _resurrect_builtins_str(self, ob_body):
        # XXX punt on figuring out how to deal with python2 bytes for now.
        # XXX this string call is almost certainly wrong on python3.
        return ffi.string(ob_body).decode('utf-8')

    def _incref(self, oid):
        p_obj = ffi.cast('PObject *', self._direct(oid))
        with self:
            self._tx_add_range_direct(p_obj, ffi.sizeof('PObject'))
            p_obj.ob_refcnt += 1

    def _decref(self, oid):
        p_obj = ffi.cast('PObject *', self._direct(oid))
        with self:
            self._tx_add_range_direct(p_obj, ffi.sizeof('PObject'))
            p_obj.ob_refcnt -= 1
            assert p_obj.ob_refcnt > 0
            if p_obj.ob_refcnt < 1:
                self._free(oid)

#
# Pool access
#

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
    ret = _check_null(lib.pmemobj_open(_coerce_fn(filename), layout_version))
    return PersistentObjectPool(ret, filename)

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
    # Assume create does an atomic create of the file before it does anything
    # else persistent, so therefore we don't need a thread lock around this
    # function body.
    ret = _check_null(lib.pmemobj_create(_coerce_fn(filename), layout_version,
                                        pool_size, mode))
    pop = PersistentObjectPool(ret, filename, create=True)
    pmem_root = lib.pmemobj_root(pop._pool_ptr, ffi.sizeof('PRoot'))
    pmem_root = ffi.cast('PRoot *', pop._direct(pmem_root))
    with pop:
        # Dummy first two elements; they are handled as special cases.
        type_table = PersistentList(['', ''], __manager__=pop)
        lib.pmemobj_tx_add_range_direct(pmem_root, ffi.sizeof('PRoot'))
        pmem_root.type_table = type_table._oid
    pop._type_table = type_table
    return pop


#
# Persistent Classes
#


class PersistentList(abc.MutableSequence):
    """Persistent version of the 'list' type."""

    def __init__(self, *args, **kw):
        if '__manager__' not in kw:
            raise ValueError("__manager__ is required")
        mm = self.__manager__ = kw.pop('__manager__')
        if '_oid' not in kw:
            with mm:
                # XXX Will want to implement a freelist here, like CPython
                self._oid = mm._malloc(ffi.sizeof('PListObject'))
                ob = ffi.cast('PObject *', mm._direct(self._oid))
                ob.ob_type = mm._get_type_code(PersistentList)
        else:
            self._oid = kw.pop('_oid')
        if kw:
            raise TypeError("Unrecognized keyword argument(s) {}".format(kw))
        self._body = ffi.cast('PListObject *', mm._direct(self._oid))
        if args:
            if len(args) != 1:
                raise TypeError("PersistentList takes at most 1"
                                " argument, {} given".format(len(args)))
            self.extend(args[0])

    @property
    def _size(self):
        return ffi.cast('PVarObject *', self._body).ob_size

    @property
    def _allocated(self):
        return self._body.allocated

    @property
    def _items(self):
        ob_items = self._body.ob_items
        if _is_OID_NULL(ob_items):
            return None
        return ffi.cast('PObjPtr *',
                        self.__manager__._direct(self._body.ob_items))

    def _resize(self, newsize):
        mm = self.__manager__
        allocated = self._allocated
        # Only realloc if we don't have enough space already.
        if (allocated >= newsize and newsize >= allocated >> 1):
            assert self._items != None or newsize == 0
            with mm:
                mm._tx_add_range_direct(self._body, ffi.sizeof('PVarObject'))
                ffi.cast('PVarObject *', self._body).ob_size = newsize
            return
        # We use CPython's overallocation algorithm.
        new_allocated = (newsize >> 3) + (3 if newsize < 9 else 6) + newsize
        if newsize == 0:
            new_allocated = 0
        items = self._items
        with mm:
            if items is None:
                items = mm._malloc_ptrs(new_allocated)
            else:
                items = mm._realloc_ptrs(items, new_allocated)
            mm._tx_add_range_direct(self._body, ffi.sizeof('PListObject'))
            self._body.ob_items = items
            self._body.allocated = new_allocated
            ffi.cast('PVarObject *', self._body).ob_size = newsize

    def insert(self, index, value):
        mm = self.__manager__
        with mm:
            size = self._size
            newsize = size + 1
            if newsize > self._allocated:
                self._resize(newsize)
            if index < 0:
                index += size
                if index < 0:
                    index = 0
            if index > size:
                index = size
            items = self._items
            mm._tx_add_range_direct(items + index,
                                    ffi.offsetof('PObjPtr *', newsize))
            for i in range(size, index, -1):
                items[i+1] = items[i]
            v_oid = mm._persist(value)
            mm._incref(v_oid)
            items[index] = v_oid

    def __setitem__(self, index, value):
        mm = self.__manager__
        with mm:
            v_oid = mm._persist(value)
            mm._tx_add_range_direct(ffi.addressof(items, index),
                                    ffi.sizeof('PObjPtr *'))
            self._items[index] = v_oid
            mm._incref(v_oid)

    def __delitem__(self, index):
        mm = self.__manager__
        size = self._size
        newsize = size - 1
        items = self._items
        with mm:
            mm._tx_add_range_direct(ffi.addressof(items, index),
                                    ffi.offsetof('PObjPtr *', size))
            mm._decref(items[index])
            for i in range(index, newsize):
                items[i] = items[i+1]
            self._resize(newsize)

    def __getitem__(self, index):
        items = self._items
        if items is None:
            items = []
        try:
            index = int(index)
        except TypeError:
            # Assume it is a slice
            # XXX fixme
            raise NotImplementedError("Slicing not yet implemented")
        else:
            if index < 0:
                index += self._size
            return self.__manager__._resurrect(items[index])

    def __len__(self):
        return self._size
