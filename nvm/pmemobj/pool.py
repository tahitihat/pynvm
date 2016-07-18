import collections
import errno
if not hasattr(errno, 'ECANCELED'):
    errno.ECANCELED = 125  # 2.7 errno doesn't define this, so guess.
import logging
import os
import sys
from pickle import whichmodule
from threading import RLock

from _pmem import lib, ffi
from .list import PersistentList

log = logging.getLogger('pynvm.pmemobj')

# If we ever need to change how we make use of the persistent store, having a
# version as the layout will allow us to provide backward compatibility.
layout_info = (0, 0, 1)
layout_version = 'pypmemobj-{}.{}.{}'.format(*layout_info).encode()

MIN_POOL_SIZE = lib.PMEMOBJ_MIN_POOL
MAX_OBJ_SIZE = lib.PMEMOBJ_MAX_ALLOC_SIZE
OID_NULL = lib.OID_NULL
# Arbitrary numbers.
POBJECT_TYPE_NUM = 20
POBJPTR_ARRAY_TYPE_NUM = 21


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
    if err == 0:
        raise OSError("raise_per_errno called with errno 0", 0)
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


class MemoryManager(object):
    """Manage a PersistentObjectPool's memory.

    This is the API to use when making a Persistent class with its own storage
    layout.
    """

    # XXX create should be a keyword-only arg but we don't have those in 2.7.
    def __init__(self, pool_ptr, type_table=None):
        log.debug('MemoryManager.__init__: %r', pool_ptr)
        self._pool_ptr = pool_ptr
        self._init_caches()
        self._track_free = None

    #
    # Transaction management
    #

    def __enter__(self):
        #log.debug('__enter__')
        _check_errno(lib.pmemobj_tx_begin(self._pool_ptr, ffi.NULL, ffi.NULL))
        return self

    def __exit__(self, *args):
        stage = lib.pmemobj_tx_stage()
        #log.debug('__exit__: %s', args)
        if stage == lib.TX_STAGE_WORK:
            if args[0] is None:
                #log.debug('committing')
                # If this fails we get a non-zero errno from tx_end and
                # _end_transaction will raise it.
                lib.pmemobj_tx_commit()
            else:
                #log.debug('aborting')
                # We have a Python exception that didn't result from an error
                # in the pmemobj library, so manually roll back the transaction
                # since the python block won't complete.
                lib.pmemobj_tx_abort(errno.ECANCELED)
        err = lib.pmemobj_tx_end()
        if err and not (err == errno.ECANCELED and args[0] is not None):
            _raise_per_errno()

    #
    # Memory management
    #
    # Although I'm defining these on the POP class, there is in fact global
    # state involved here.  These methods will work only when a transaction is
    # active, and the global state means that only one object pool may have a
    # transaction executing in a given thread at a time.  XXX should add a
    # check for this, probably in __enter__.

    def malloc(self, size, type_num=POBJECT_TYPE_NUM):
        """Return a pointer to size bytes of newly allocated persistent memory.

        By default the pmemobject type number is POBJECT_TYPE_NUM; be careful
        to specify a different type number for non-PObject allocations.
        """
        log.debug('malloc: %r', size)
        if size == 0:
            return OID_NULL
        oid = self.otuple(lib.pmemobj_tx_zalloc(size, type_num))
        if oid == self.OID_NULL:
            _raise_per_errno()
        log.debug('oid: %s', oid)
        return oid

    def malloc_ptrs(self, count):
        """Return pointer to enough persistent memory for count pointers.

        The pmem type number is set to POBJPTR_ARRAY_TYPE_NUM.
        """
        log.debug('malloc_ptrs: %r', count)
        return self.malloc(count * ffi.sizeof('PObjPtr'),
                            type_num=POBJPTR_ARRAY_TYPE_NUM)

    def realloc(self, oid, size, type_num=None):
        """Copy oid contents into size bytes of new persistent memory.

        Return pointer to the new memory.
        """
        oid = self.otuple(oid)
        log.debug('realloc: %r %r', oid, size)
        if size == 0:
            self.free(oid)
            return OID_NULL
        if type_num is None:
            type_num = lib.pmemobj_type_num(oid)
        oid = self.otuple(lib.pmemobj_tx_zrealloc(oid, size, type_num))
        if oid == self.OID_NULL:
            _raise_per_errno()
        log.debug('oid: %s', oid)
        return oid

    def realloc_ptrs(self, oid, count):
        oid = self.otuple(oid)
        log.debug('realloc_ptrs: %r %r', oid, count)
        """As realloc, but the new memory is enough for count pointers."""
        return self.realloc(oid, count * ffi.sizeof('PObjPtr'),
                             POBJPTR_ARRAY_TYPE_NUM)

    def free(self, oid):
        """Free the memory pointed to by oid."""
        oid = self.otuple(oid)
        log.debug('free: %r', oid)
        _check_errno(lib.pmemobj_tx_free(oid))

    def direct(self, oid):
        """Return the real memory address where oid lives."""
        oid = self.otuple(oid)
        return _check_null(lib.pmemobj_direct(oid))

    def snapshot_range(self, ptr, size):
        lib.pmemobj_tx_add_range_direct(ptr, size)

    #
    # Object Management
    #

    def _init_caches(self):
        # We have a couple of special cases to avoid infinite regress.
        self._type_code_cache = {PersistentList: 0, str: 1}
        self._persist_cache = {}
        # XXX WeakValueDictionary?
        # XXX I'm not sure we can get away with mapping OID_NULL
        # to None here, but try it and see.
        self._resurrect_cache = {self.otuple(OID_NULL): None}

    def _resurrect_type_table(self, oid):
        """Resurrect the type table from oid.

        This is a private method for coordination between the
        PersistentObjectPool and the MemoryManager.
        """
        self._type_table = self.resurrect(oid)

    def _create_type_table(self):
        """Create an initial type table and return its oid.

        This is a private method for coordination between the
        PersistentObjectPool and the MemoryManager.
        """
        with self:
            # Pre-fill first two elements; they are handled as special cases.
            type_table = PersistentList(
                [_class_string(PersistentList), _class_string(str)],
                __manager__=self)
            self.incref(type_table._oid)
            self._resurrect_cache[type_table._oid] = type_table
        self._type_table = type_table
        return type_table._oid

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
            log.debug('type_code for %s: %r', cls_str, code)
            return code
        except ValueError:
            self._type_table.append(cls_str)
            code = len(self._type_table) - 1
            log.debug('new type_code for %s: %r', cls_str, code)
            return code

    def persist(self, obj):
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
            raise TypeError("Don't know how to persist {!r}".format(cls_str))
        oid = getattr(self, persister)(obj)
        # This oid should always come from a malloc, and thus be a tuple,
        # and so the correct value to use as a key without calling _as_tuple.
        self._persist_cache[key] = oid
        self._resurrect_cache[oid] = obj
        log.debug('new %s object: %r', cls_str, oid)
        return oid

    def resurrect(self, oid):
        """Return python object representing the data stored at oid."""
        oid = self.otuple(oid)
        # XXX need multiple debug levels
        #log.debug('resurrect: %r', oid)
        try:
            obj = self._resurrect_cache[oid]
            #log.debug('resurrected from cache: %r', obj)
            return obj
        except KeyError:
            pass
        obj_ptr = ffi.cast('PObject *', self.direct(oid))
        type_code = obj_ptr.ob_type
        # The special cases are to avoid infinite regress in the type table.
        if type_code == 0:
            res = PersistentList(__manager__=self, _oid=oid)
            self._resurrect_cache[oid] = res
            log.debug('resurrect PersistentList: %s %r', oid, res)
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
            p_str_oid = self.malloc(ffi.sizeof('PObject') + len(s) + 1)
            p_str = ffi.cast('PObject *', self.direct(p_str_oid))
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
            p_float_oid = self.malloc(ffi.sizeof('PFloatObject'))
            p_float = ffi.cast('PObject *', self.direct(p_float_oid))
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
            p_int = ffi.cast('PObject *', self.direct(p_int_oid))
            p_int.ob_type = type_code
        return p_int_oid
    _persist_builtins_long = _persist_builtins_int

    def _resurrect_builtins_int(self, obj_ptr):
        i_str = self._resurrect_builtins_str(obj_ptr)
        return int(i_str)

    def incref(self, oid):
        """Increment the reference count of oid."""
        oid = self.otuple(oid)
        p_obj = ffi.cast('PObject *', self.direct(oid))
        log.debug('incref %r %r', oid, p_obj.ob_refcnt + 1)
        with self:
            self.snapshot_range(p_obj, ffi.sizeof('PObject'))
            p_obj.ob_refcnt += 1

    def decref(self, oid):
        """Decrement the reference count of oid, and free it if zero."""
        oid = self.otuple(oid)
        p_obj = ffi.cast('PObject *', self.direct(oid))
        log.debug('decref %r %r', oid, p_obj.ob_refcnt - 1)
        with self:
            # XXX also need to remove oid from resurrect and persist caches
            self.snapshot_range(p_obj, ffi.sizeof('PObject'))
            assert p_obj.ob_refcnt > 0
            p_obj.ob_refcnt -= 1
            if p_obj.ob_refcnt < 1:
                self._deallocate(oid)

    def xdecref(self, oid):
        """decref oid if it is not OID_NULL."""
        if self.otuple(oid) != self.OID_NULL:
            self.decref(oid)

    def _deallocate(self, oid):
        """Deallocate the memory occupied by oid."""
        log.debug("deallocating %s", oid)
        with self:
            # XXX could have a type cache so we don't have to resurrect here.
            obj = self.resurrect(oid)
            if hasattr(obj, '_deallocate'):
                obj._deallocate()
            self.free(oid)
        if self._track_free is not None:
            self._track_free.add(oid)

    #
    # Utility methods
    #

    # An oid can be in two forms: a 'ctype PMEMoid &', which directly
    # references the memory containing the PMEMoid, or a tuple containing
    # the data from the two fields.  Such a tuple can be assigned to
    # a 'ctype PMEMoid &' and the data will be copied into it correctly.
    # In an ideal world tuple(oid) would ensure that oid was in tuple
    # form, but cffi doesn't support that.  So we have a utility method
    # that does it.

    def otuple(self, oid):
        """Return the oid as a tuple; return it unchanged if it already is one.
        """
        if isinstance(oid, tuple):
            return oid
        return (oid.pool_uuid_lo, oid.off)

    OID_NULL = (lib.OID_NULL.pool_uuid_lo, lib.OID_NULL.off)


class PersistentObjectPool(object):
    """This class represents the persistent object pool created using
    :func:`~nvm.pmemobj.create` or :func:`~nvm.pmemobj.open`.
    """

    # This class  provides the API that will be used by most programs.

    lock = RLock()
    closed = False

    # XXX create should be a keyword-only arg but we don't have those in 2.7.
    def __init__(self, filename, pool_size=MIN_POOL_SIZE, mode=0o666):
        """Open or create a persistent object pool backed by filename.

        If the file gets created, use pool_size as the size of the new pool in
        bytes and mode as its access mode, otherwise ignore these parameters
        and open the existing file.

        See also the open and create functions of nvm.pmemobj, which insure
        that the file exists or does not, respectively.
        """
        log.debug('PersistentObjectPool.__init__: %r, %r, %r',
                  filename, pool_size, mode)
        self.filename = filename
        if os.path.exists(filename):
            self._open(filename)
        else:
            self._create(filename, pool_size, mode)


    def _create(self, filename, pool_size, mode):
        self._pool_ptr = _check_null(lib.pmemobj_create(_coerce_fn(filename),
                                                        layout_version,
                                                        pool_size, mode))
        mm = self.mm = MemoryManager(self._pool_ptr)
        with self as mm:
            pmem_root = lib.pmemobj_root(self._pool_ptr, ffi.sizeof('PRoot'))
            pmem_root = ffi.cast('PRoot *', mm.direct(pmem_root))
            type_table_oid = mm._create_type_table()
            mm.snapshot_range(pmem_root, ffi.sizeof('PRoot'))
            pmem_root.type_table = type_table_oid
            self._pmem_root = pmem_root

    def _open(self, filename):
        self._pool_ptr = _check_null(lib.pmemobj_open(_coerce_fn(filename),
                                                      layout_version))
        mm = self.mm = MemoryManager(self._pool_ptr)
        size = lib.pmemobj_root_size(self._pool_ptr)
        if size:
            pmem_root = mm.direct(lib.pmemobj_root(self._pool_ptr, 0))
            pmem_root = ffi.cast('PRoot *', pmem_root)
        # pmemobj_root being inside a transaction does not make it atomic; we
        # aren't using pmemobj_root_construct.  So we need to check whether or
        # not initialization completed.  Since the type_table will be non-zero
        # if and only if it did, we'll use that as our initialization flag.
        # XXX I'm not sure the check for pmem_root not NULL is needed.
        if not size or pmem_root == ffi.NULL or not pmem_root.type_table:
            raise RuntimeError("Pool {} not initialized completely; delete"
                               "the file and try again".format(
                self.filename))
        self._pmem_root = pmem_root
        mm._resurrect_type_table(pmem_root.type_table)
        # Make sure any objects orphaned by a crash are cleaned up.
        # XXX should fix this to only be called when there is a crash.
        self.gc()

    def close(self):
        """Close the object pool, freeing any unreferenced objects.

        The object pool itself lives on in the file that contains it and may be
        reopened at a later date, and all the objects in it accessed, using
        nvm.pmemobj.open.

        """
        with self.lock:
            if self.closed:
                log.debug('already closed')
                return
            log.debug('close')
            self.closed = True     # doing this early helps with debugging
            # Clean up unreferenced object cycles.
            self.gc()
            lib.pmemobj_close(self._pool_ptr)

    def __del__(self):
        if hasattr(self, '_pmem_root'):
            # Initialization was complete, do a full close.
            self.close()
        elif hasattr(self, '_pool_ptr'):
            # Initialization was not complete, just close the libpmemobj.
            lib.pmemobj_close(self._pool_ptr)
        else:
            # libpmemobj open failed, nothing to do.
            pass


    @property
    def root(self):
        """The root object of the pool's persistent object tree.

        Set this to something that can point to all the other objects that are
        to be persisted, such as a list or dictionary.  An object is retained
        between program runs *only* if it can be reached from the root object.

        """
        return self.mm.resurrect(self._pmem_root.root_object)

    @root.setter
    def root(self, value):
        log.debug("setting 'root' to %r", value)
        with self, self.lock:
            oid = self.mm.persist(value)
            self.mm.snapshot_range(
                ffi.addressof(self._pmem_root.root_object),
                ffi.sizeof('PObjPtr'))
            self.mm.xdecref(self._pmem_root.root_object)
            self._pmem_root.root_object = oid
            self.mm.incref(oid)

    def begin_transaction(self):
        """Start a new (sub)transaction."""
        log.debug('start_transaction')
        _check_errno(
            lib.pmemobj_tx_begin(self._pool_ptr, ffi.NULL, ffi.NULL))

    def commit_transaction(self):
        """Commit the current (sub)transaction."""
        log.debug('commit_transaction')
        lib.pmemobj_tx_commit()
        _check_errno(lib.pmemobj_tx_end())

    def abort_transaction(self, errno=0):
        """Abort the current (sub)transaction."""
        log.debug('abort_transaction')
        lib.pmemobj_tx_abort(errno)
        if lib.pmemobj_tx_end() not in (0, errno.ECANCELED):
            _raise_per_errno()

    def __enter__(self):
        """Begin a transaction context, optionally return the memory manager."""
        return self.mm.__enter__()

    def __exit__(self, *args, **kw):
        """End the current transaction context."""
        self.mm.__exit__(*args, **kw)

    def new(self, typ, *args, **kw):
        """Create a new instance of typ using args and kw, managed by this pool.

        typ must accept a __manager__ keyword argument and use the supplied
        MemoryManager for all persistent memory access.
        """
        log.debug('new: %s, %s, %s', typ, args, kw)
        return typ(*args, __manager__=self.mm, **kw)

    # If I didn't have to support python2 I'd make debug keyword only.
    def gc(self, debug=False):
        """Free all unreferenced objects (cyclic garbage).

        The object tree is traced from the root, and any object that is not
        referenced somewhere in the tree is freed.  This collects cyclic
        garbage, and produces warnings for unreferenced objects with incorrect
        refcounts.  Most garbage is automatically collected when the object is
        no longer referenced.  If debug is true, the debug logging output
        will include reprs of the objects encountered.
        """
        # XXX CPython uses a three generation GC in order to obtain more or
        # less linear performance against the total number of objects.
        # Currently we are not doing generations; we can get more complicated
        # later if we want to run the GC periodically.

        log.debug('gc: start')
        containers = set()
        other = set()
        orphans = set()
        types = {}
        type_counts = collections.defaultdict(int)
        gc_counts = collections.defaultdict(int)

        with self.lock:
            # Catalog all PObjects.
            oid = self.mm.otuple(lib.pmemobj_first(self._pool_ptr))
            while oid != self.mm.OID_NULL:
                type_num = lib.pmemobj_type_num(oid)
                # XXX Could make the _PTR lists PObjects too so they are tracked.
                if type_num == POBJECT_TYPE_NUM:
                    obj =  ffi.cast('PObject *', self.mm.direct(oid))
                    if debug:
                        if obj.ob_refcnt < 0:
                            log.error("Negative refcount (%s): %s %r",
                                      obj.ob_refcnt, oid, self.mm.resurrect(oid))
                    assert obj.ob_refcnt >= 0, '%s has negative refcnt' % oid
                    # XXX move this cache to the POP?
                    type_code = obj.ob_type
                    if type_code not in types:
                        types[type_code] = _find_class_from_string(
                                                self.mm._type_table[type_code])
                    typ = types[type_code]
                    type_counts[typ.__name__] += 1
                    if not obj.ob_refcnt:
                        if debug:
                            log.debug('gc: orphan: %s %s %r',
                                      oid, obj.ob_refcnt, self.mm.resurrect(oid))
                        orphans.add(oid)
                    elif hasattr(typ, '_traverse'):
                        if debug:
                            log.debug('gc: container: %s %s %r',
                                      oid, obj.ob_refcnt, self.mm.resurrect(oid))
                        containers.add(oid)
                    else:
                        if debug:
                            log.debug('gc: other: %s %s %r',
                                      oid, obj.ob_refcnt, self.mm.resurrect(oid))
                        other.add(oid)
                oid = self.mm.otuple(lib.pmemobj_next(oid))
            gc_counts['containers-total'] = len(containers)
            gc_counts['other-total'] = len(other)

            # Clean up refcount 0 orphans (from a crash or code bug).
            log.debug("gc: deallocating %s orphans", len(orphans))
            gc_counts['orphans0-gced'] = len(orphans)
            for oid in orphans:
                if debug:
                    # XXX This should be a non debug warning on close.
                    log.warning("deallocating orphan (refcount 0): %s %r",
                                oid, self.mm.resurrect(oid))
                self.mm._deallocate(oid)

            # Trace the object tree, removing objects that are referenced.
            containers.remove(self.mm._type_table._oid)
            live = [self.mm._type_table._oid]
            root_oid = self.mm.otuple(self._pmem_root.root_object)
            root = self.mm.resurrect(root_oid)
            if hasattr(root, '_traverse'):
                containers.remove(root_oid)
                live.append(root_oid)
            elif root is not None:
                if debug:
                    log.debug('gc: non-container root: %s %r', oid, root)
                other.remove(root_oid)
            for oid in live:
                if debug:
                    log.debug('gc: checking live %s %r',
                              oid, self.mm.resurrect(oid))
                for sub_oid in self.mm.resurrect(oid)._traverse():
                    sub_key = self.mm.otuple(sub_oid)
                    if sub_key in containers:
                        if debug:
                            log.debug('gc: refed container %s %r',
                                       sub_key, self.mm.resurrect(sub_oid))
                        containers.remove(sub_key)
                        live.append(sub_key)
                    elif sub_key in other:
                        if debug:
                            log.debug('gc: refed oid %s %r',
                                      sub_key, self.mm.resurrect(sub_oid))
                        other.remove(sub_key)
                        gc_counts['other-live'] += 1
            gc_counts['containers-live'] = len(live)

            # Everything left is unreferenced via the root, deallocate it.
            log.debug('gc: deallocating %s containers', len(containers))
            self.mm._track_free = set()
            for oid in containers:
                if oid in self.mm._track_free:
                    continue
                if debug:
                    log.debug('gc: deallocating container %s %r',
                              oid, self.mm.resurrect(oid))
                with self:
                    # incref so we don't try to deallocate us during cycle clear.
                    self.mm.incref(oid)
                    self.mm._deallocate(oid)
                    # deallocate frees oid, so no decref.
            gc_counts['collections-gced'] = len(containers)
            log.debug('gc: deallocating %s new orphans', len(other))
            for oid in other:
                if oid in self.mm._track_free:
                    continue
                log.warning("Orphaned with postive refcount: %s: %s",
                    oid, self.mm.resurrect(oid))
                self.mm._deallocate(oid)
                gc_counts['orphans1-gced'] += 1
            gc_counts['other-gced'] = len(other) - gc_counts['orphans1-gced']
            self.mm._track_free = None
            log.debug('gc: end')

            return dict(type_counts), dict(gc_counts)


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
    # Make sure the file exists.
    if not os.path.exists(filename):
        # Pass through the libpmemobj error message.
        _check_null(lib.pmemobj_open(_coerce_fn(filename), layout_version))
    return PersistentObjectPool(filename)

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
    log.debug('create: %s, %s, %s', filename, pool_size, mode)
    # Make sure the file does not already exist.
    if os.path.exists(filename):
        # Pass through the libpmemobj error message.
        _check_null(lib.pmemobj_create(_coerce_fn(filename), layout_version,
                                       pool_size, mode))
    return PersistentObjectPool(filename, pool_size, mode)
