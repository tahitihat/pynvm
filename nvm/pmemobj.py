"""
.. module:: pmemobj
.. moduleauthor:: R. David Murray <rdmurray@bitdance.com>

:mod:`pmemobj` -- pmem-resident objects
=========================================================

"""

# XXX Time to break this up into a package.

try:
    import collections.abc as abc
except ImportError:
    import collections as abc
    import faulthandler
    faulthandler.enable()
import errno
if not hasattr(errno, 'ECANCELED'):
    errno.ECANCELED = 125  # 2.7 errno doesn't define this, so guess.
import logging
import os
import sys
from pickle import whichmodule
from threading import RLock
from _pmem import lib, ffi

log = logging.getLogger('pynvm.pmemobj')

# If we ever need to change how we make use of the persistent store, having a
# version as the layout will allow us to provide backward compatibility.
layout_info = (0, 0, 1)
layout_version = 'pypmemobj-{}.{}.{}'.format(*layout_info).encode()

MIN_POOL_SIZE = lib.PMEMOBJ_MIN_POOL
MAX_OBJ_SIZE = lib.PMEMOBJ_MAX_ALLOC_SIZE
OID_NULL = lib.OID_NULL

def _is_OID_NULL(oid):
    # XXX I think == should work here, but it doesn't.
    return (oid.oid.pool_uuid_lo == OID_NULL.pool_uuid_lo
            and oid.oid.off == OID_NULL.off)

class oid_wrapper(object):
    """Helper class to deal with cffi structs not supporting ==.

    Whlie we're at it, a useful repr.  We wrap all oids in this
    wrapper at the earliest opportunity, and unwrap it just before
    handing it off to cffi.
    """
    # XXX This is not clean, it would be much better to enhance cffi.
    def __init__(self, oid):
        self.oid = oid
    def __eq__(self, other):
        return (self.oid.pool_uuid_lo == other.oid.pool_uuid_lo
                and self.oid.off == other.oid.off)
    def __hash__(self):
        return hash(self.oid.pool_uuid_lo) + hash(self.oid.off)
    def __repr__(self):
        return "oid({}, {})".format(self.oid.pool_uuid_lo, self.oid.off)

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
    oid = oid_wrapper(oid)
    if _is_OID_NULL(oid):
        _raise_per_errno()
    return oid

_class_string_cache = {}
def _class_string(cls):
    """Return a string we can use later to find the base class of cls.

    Raise TypeError if we can't compute a useful string.
    """
    log.debug("_class_string: %r", cls)
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
    log.debug("new _class_string: %r", res)
    return res

_class_from_string_cache = {}
def _find_class_from_string(cls_string):
    """Return class object corresponding to class_string."""
    log.debug('_find_class_from_string: %r', cls_string)
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
    log.debug('new class_from_string: %r', res)
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
        log.debug('PersistentObjectPool.__init__: %r, %r, create=%s',
                  pool_ptr, filename, create)
        self._pool_ptr = pool_ptr
        self.filename = filename
        self.closed = False
        if create:
            self._type_table = None
            self._root = None
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
                pmem_root = self._direct(oid_wrapper(lib.pmemobj_root(self._pool_ptr, 0)))
                pmem_root = ffi.cast('PRoot *', pmem_root)
            # XXX I'm not sure the check for pmem_root not NULL is needed.
            if not size or pmem_root == ffi.NULL or not pmem_root.type_table:
                raise RuntimeError("Pool {} not initialized completely".format(
                    self.filename))
            self._type_table = self._resurrect(oid_wrapper(pmem_root.type_table))
            self._root = self._resurrect(oid_wrapper(pmem_root.root_object))
            self._pmem_root = pmem_root

    def close(self):
        """This method closes the object pool.  The object pool itself lives on
        in the file that contains it and may be reopened at a later date, and
        all the objects in it accessed, using :func:`~nvm.pmemlog.open`.
        """
        log.debug('close')
        lib.pmemobj_close(self._pool_ptr)
        self.closed = True

    def __del__(self):
        if not self.closed:
            self.close()

    @property
    def root(self):
        return self._root
    @root.setter
    def root(self, value):
        log.debug("setting 'root' to %r", value)
        # XXX need a with here, a lock, an incref, and a conditional decref.
        oid = self._persist(value)
        self._pmem_root.root_object = oid.oid
        self._root = value

    #
    # Transaction management
    #

    def begin_transaction(self):
        """Start a new (sub)transaction."""
        #log.debug('start_transaction')
        _check_errno(lib.pmemobj_tx_begin(self._pool_ptr, ffi.NULL, ffi.NULL))

    def commit_transaction(self):
        """Commit the current (sub)transaction."""
        #log.debug('commit_transaction')
        lib.pmemobj_tx_commit()
        _check_errno(lib.pmemobj_tx_end())

    def _end_transaction(self):
        """End the current (sub)transaction.

        Raise an error if the returned errno is not 0 or ECNANCELED.
        """
        #log.debug('_end_transaction')
        if lib.pmemobj_tx_end() not in (0, errno.ECANCELED):
            _raise_per_errno()

    def abort_transaction(self, errno=errno.ECANCELED):
        """Abort the current (sub)transaction."""
        #log.debug('abort_transaction')
        lib.pmemobj_tx_abort(errno)
        self._end_transaction()

    def __enter__(self):
        #log.debug('__enter__')
        self.begin_transaction()

    def __exit__(self, *args):
        #log.debug('__exit__')
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

    # XXX should use a non-zero type code, so we can use it to walk the list of
    # all allocated objects in the cyclic GC that doesn't exist yet.

    def _malloc(self, size):
        """Return a pointer to size bytes of newly allocated persistent memory.
        """
        # For now, just use the underlying allocator...later we'll write a
        # small object allocator on top, and the pointer size will grow.
        log.debug('malloc: %r', size)
        if size == 0:
            return oid_wrapper(OID_NULL)
        return _check_oid(lib.pmemobj_tx_zalloc(size, 0))

    def _malloc_ptrs(self, count):
        """Return pointer to enough persistent memory for count pointers.
        """
        log.debug('malloc_ptrs: %r', count)
        return self._malloc(count * ffi.sizeof('PObjPtr'))

    def _realloc(self, oid, size):
        """Copy oid contents into size bytes of new persistent memory.

        Return pointer to the new memory.
        """
        log.debug('realloc: %r %r', oid, size)
        if size == 0:
            self._free(oid)
            return oid_wrapper(OID_NULL)
        return _check_oid(lib.pmemobj_tx_zrealloc(oid.oid, size, 0))

    def _realloc_ptrs(self, oid, count):
        log.debug('realloc_ptrs: %r %r', oid, count)
        """As _realloc, but the new memory is enough for count pointers."""
        return self._realloc(oid, count * ffi.sizeof('PObjPtr'))

    def _free(self, oid):
        """Free the memory pointed to by oid."""
        log.debug('free: %r', oid)
        _check_errno(lib.pmemobj_tx_free(oid.oid))

    def _direct(self, oid):
        """Return the real memory address where oid lives."""
        return _check_null(lib.pmemobj_direct(oid.oid))

    def _tx_add_range_direct(self, ptr, size):
        lib.pmemobj_tx_add_range_direct(ptr, size)

    #
    # Object Management
    #

    def _init_caches(self):
        # We have a couple of special cases to avoid infinite regress.
        self._type_code_cache = {PersistentList: 0, str: 1}
        self._persist_cache = {}
        # XXX WeakValueDictionary?
        self._resurrect_cache = {oid_wrapper(OID_NULL): None}

    def _get_type_code(self, cls):
        """Return the index into the type table for cls.

        Create the type table entry if required.
        """
        log.debug('get_type_code: %r', cls)
        try:
            return self._type_code_cache[cls]
        except KeyError:
            pass
        cls_str = _class_string(cls)
        try:
            code = self._type_table.index(cls_str)
            log.debug('type_code: %r', code)
            return code
        except ValueError:
            self._type_table.append(cls_str)
            code = len(self._type_table) - 1
            log.debug('new type_code: %r', code)
            return code

    def _persist(self, obj):
        """Store obj in persistent memory and return its oid."""
        key = obj if getattr(obj, '__hash__', None) else id(obj)
        log.debug('persist: %r (key %r)', obj, key)
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
        oid = getattr(self, persister)(obj)
        self._persist_cache[key] = oid
        self._resurrect_cache[oid] = obj
        log.debug('new oid: %r', oid)
        return oid

    def _resurrect(self, oid):
        """Return python object representing the data stored at oid."""
        # XXX need multiple debug levels
        #log.debug('resurrect: %r', oid)
        try:
            # XXX The fact that oid == OID_NULL doesn't work probably
            # implies that this cache isn't going to work either, so
            # we'll need to debug that soon.
            obj = self._resurrect_cache[oid]
            #log.debug('resurrect from cache: %r', self._resurrect_cache[oid])
            return obj
        except KeyError:
            pass
        if _is_OID_NULL(oid):
            # XXX I'm not sure we can get away with mapping OID_NULL
            # to None here, but try it and see.
            self._resurrect_cache[oid] = None
            return None
        obj_ptr = ffi.cast('PObject *', self._direct(oid))
        type_code = obj_ptr.ob_type
        # The special cases are to avoid infinite regress in the type table.
        if type_code == 0:
            res = PersistentList(__manager__=self, _oid=oid)
            self._resurrect_cache[oid] = res
            log.debug('resurrect PersistentList: %r', res)
            return res
        if type_code == 1:
            cls_str = 'builtins:str'
        else:
            cls_str = self._type_table[type_code]
        resurrector = '_resurrect_' + cls_str.replace(':', '_')
        if not hasattr(self, resurrector):
            # It must be a persistent type.
            cls = find_class_from_string(cls_str)
            res = cls(__manager__=self, _oid=oid)
            log.debug('resurrect %r: persistent type (%r): %r',
                      oid, cls_str, res)
            return res
        res = getattr(self, resurrector)(obj_ptr)
        self._resurrect_cache[oid] = res
        self._persist_cache[res] = oid
        log.debug('resurrect %r: immutable type (%r): %r',
                  oid, resurrector, res)
        return res

    def _persist_builtins_str(self, s):
        type_code = self._get_type_code(s.__class__)
        if sys.version_info[0] > 2:
            s = s.encode('utf-8')
        with self:
            p_str_oid = self._malloc(ffi.sizeof('PObject') + len(s) + 1)
            p_str = ffi.cast('PObject *', self._direct(p_str_oid))
            p_str.ob_type = type_code
            body = ffi.cast('char *', p_str) + ffi.sizeof('PObject')
            ffi.buffer(body, len(s))[:] = s
        return p_str_oid

    def _resurrect_builtins_str(self, obj_ptr):
        body = ffi.cast('char *', obj_ptr) + ffi.sizeof('PObject')
        s = ffi.string(body)
        if sys.version_info[0] > 2:
            s = s.decode('utf-8')
        return s

    def _persist_builtins_float(self, f):
        type_code = self._get_type_code(f.__class__)
        with self:
            p_float_oid = self._malloc(ffi.sizeof('PFloatObject'))
            p_float = ffi.cast('PObject *', self._direct(p_float_oid))
            p_float.ob_type = type_code
            p_float = ffi.cast('PFloatObject *', p_float)
            p_float.fval = f
        return p_float_oid

    def _resurrect_builtins_float(self, obj_ptr):
        return ffi.cast('PFloatObject *', obj_ptr).fval

    def _persist_builtins_int(self, i):
        # Make sure we get the int type even on python2.  The space is needed.
        type_code = self._get_type_code(1 .__class__)
        # In theory we could copy the actual CPython data directly here,
        # but that would mean we'd break on PyPy, etc.  So we serialize.
        i = repr(i)
        if sys.version_info[0] < 3:
            i = i.rstrip('L')
        with self:
            # There's a bit of extra overhead in reusing this, but not much.
            p_int_oid = self._persist_builtins_str(i)
            p_int = ffi.cast('PObject *', self._direct(p_int_oid))
            p_int.ob_type = type_code
        return p_int_oid
    _persist_builtins_long = _persist_builtins_int

    def _resurrect_builtins_int(self, obj_ptr):
        i_str = self._resurrect_builtins_str(obj_ptr)
        return int(i_str)

    def _incref(self, oid):
        log.debug('incref %r', oid)
        p_obj = ffi.cast('PObject *', self._direct(oid))
        with self:
            self._tx_add_range_direct(p_obj, ffi.sizeof('PObject'))
            p_obj.ob_refcnt += 1

    def _decref(self, oid):
        log.debug('decref %r', oid)
        p_obj = ffi.cast('PObject *', self._direct(oid))
        with self:
            self._tx_add_range_direct(p_obj, ffi.sizeof('PObject'))
            assert p_obj.ob_refcnt > 0
            p_obj.ob_refcnt -= 1
            if p_obj.ob_refcnt < 1:
                # XXX this needs to check for a del 'slot' and call it
                # if it exists (equivalent to CPython's tp_del).
                self._free(oid)

    def new(self, typ, *args, **kw):
        log.debug('new: %s, %s, %s', typ, args, kw)
        return typ(*args, __manager__=self, **kw)

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
    log.debug('open: %s', filename)
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
    log.debug('create: %s, %s, %s', filename, pool_size, mode)
    ret = _check_null(lib.pmemobj_create(_coerce_fn(filename), layout_version,
                                        pool_size, mode))
    pop = PersistentObjectPool(ret, filename, create=True)
    pmem_root = lib.pmemobj_root(pop._pool_ptr, ffi.sizeof('PRoot'))
    pmem_root = ffi.cast('PRoot *', pop._direct(oid_wrapper(pmem_root)))
    with pop:
        # Dummy first two elements; they are handled as special cases.
        type_table = PersistentList(['', ''], __manager__=pop)
        lib.pmemobj_tx_add_range_direct(pmem_root, ffi.sizeof('PRoot'))
        pmem_root.type_table = type_table._oid.oid
        temp = pmem_root.type_table
        pop._pmem_root = pmem_root
    pop._type_table = type_table
    return pop


#
# Persistent Classes
#


class PersistentList(abc.MutableSequence):
    """Persistent version of the 'list' type."""

    # XXX locking!
    # XXX tp_del method (see _decref)

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
        ob_items = oid_wrapper(self._body.ob_items)
        if _is_OID_NULL(ob_items):
            return None
        return ffi.cast('PObjPtr *',
                        self.__manager__._direct(ob_items))

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
                items = mm._realloc_ptrs(oid_wrapper(self._body.ob_items),
                                         new_allocated)
            mm._tx_add_range_direct(self._body, ffi.sizeof('PListObject'))
            self._body.ob_items = items.oid
            self._body.allocated = new_allocated
            ffi.cast('PVarObject *', self._body).ob_size = newsize

    def insert(self, index, value):
        mm = self.__manager__
        with mm:
            size = self._size
            newsize = size + 1
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
                items[i] = items[i-1]
            v_oid = mm._persist(value)
            mm._incref(v_oid)
            items[index] = v_oid.oid

    def _normalize_index(self, index):
        try:
            index = int(index)
        except TypeError:
            # Assume it is a slice
            # XXX fixme
            raise NotImplementedError("Slicing not yet implemented")
        if index < 0:
            index += self._size
        if index < 0 or index >= self._size:
            raise IndexError
        return index

    def __setitem__(self, index, value):
        index = self._normalize_index(index)
        mm = self.__manager__
        items = self._items
        with mm:
            v_oid = mm._persist(value)
            mm._tx_add_range_direct(ffi.addressof(items, index),
                                    ffi.sizeof('PObjPtr *'))
            items[index] = v_oid.oid
            mm._incref(v_oid)

    def __delitem__(self, index):
        index = self._normalize_index(index)
        mm = self.__manager__
        size = self._size
        newsize = size - 1
        items = self._items
        with mm:
            mm._tx_add_range_direct(ffi.addressof(items, index),
                                    ffi.offsetof('PObjPtr *', size))
            mm._decref(oid_wrapper(items[index]))
            for i in range(index, newsize):
                items[i] = items[i+1]
            self._resize(newsize)

    def __getitem__(self, index):
        index = self._normalize_index(index)
        items = self._items
        return self.__manager__._resurrect(oid_wrapper(items[index]))

    def __len__(self):
        return self._size

    def __repr__(self):
        return "{}([{}])".format(self.__class__.__name__,
                                 ', '.join("{!r}".format(x) for x in self))

    def __eq__(self, other):
        try:
            ol = len(other)
        except AttributeError:
            return NotImplemented
        if len(self) != ol:
            return False
        for i in range(len(self)):
            try:
                ov = other[i]
            except (AttributeError, IndexError):
                return NotImplemented
            if self[i] != ov:
                return False
        return True
