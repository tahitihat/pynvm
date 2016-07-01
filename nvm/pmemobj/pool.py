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


# In general an oid is going to already be in tuple form, since that's what the
# memory management routines return.  But sometimes we get an PMEMoid from a
# list of pointers.  So we have to call _oid_as_tuple at a couple of other
# strategic locations to make handling oids as transparent as possible.  This
# all works because the tuple can be assigned to a "ctype PMEMoid &" and
# likewise be used as the argument to a function that takes a PMEMoid by value.
def _oid_as_tuple(oid):
    """Return the oid as a tuple, return it unchanged if it already is one."""
    if isinstance(oid, tuple):
        return oid
    return (oid.pool_uuid_lo, oid.off)

def _oids_eq(oid1, oid2):
    """Return True if the two oids hold the same data."""
    return _oid_as_tuple(oid1) == _oid_as_tuple(oid2)


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

def _check_oid(oid):
    """Raise an error if oid is OID_NULL, otherwise return it.
    """
    if _oids_eq(OID_NULL, oid):
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

    # This class mostly just delegates to its MemoryManager.  It provides
    # the API that will be used by most programs.

    # XXX create should be a keyword-only arg but we don't have those in 2.7.
    def __init__(self, pool_ptr, filename, create=False):
        """Provide an API to access the persistent memory object pool pool_ptr
        backed by file filename, initializing it if create is True.

        The PersistentObjectPool constructor should not be considered a public
        API.  Use nvm.pmemobj.create to create a pool, and nvm.pmemobj.open to
        open an existing pool.
        """
        self.mm = MemoryManager(pool_ptr, filename, create=create)

    @property
    def filename(self):
        """The name of the file containing the object pool."""
        return self.mm.filename

    @property
    def closed(self):
        """True if and only if close has been called."""
        return self.mm.closed

    def close(self):
        """Close the object pool, freeing any unreferenced objects.

        The object pool itself lives on in the file that contains it and may be
        reopened at a later date, and all the objects in it accessed, using
        nvm.pmemobj.open.

        """
        self.mm.close()

    @property
    def root(self):
        """The root object of the pool's persistent object tree.

        Set this to something that can point to all the other objects that are
        to be persisted, such as a list or dictionary.  An object is retained
        between program runs *only* if it can be reached from the root object.

        """
        return self.mm._root
    @root.setter
    def root(self, value):
        self.mm._root = value

    def begin_transaction(self):
        """Start a new (sub)transaction."""
        log.debug('start_transaction')
        _check_errno(
            lib.pmemobj_tx_begin(self.mm._pool_ptr, ffi.NULL, ffi.NULL))

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

    def gc(self, debug=False):
        """Free all unreferenced objects (cyclic garbage).

        The object tree is traced from the root, and any object that is not
        referenced somewhere in the tree is freed.  This collects cyclic
        garbage, and produces warnings for unreferenced objects with incorrect
        refcounts.  Most garbage is automatically collected when the object is
        no longer referenced.  If debug is true, the debug logging output
        will include reprs of the objects encountered.
        """
        return self.mm.gc(debug=debug)


class MemoryManager(object):
    """Manage a PersistentObjectPool's memory.

    This is the API to use when making a Persistent class with its own storage
    layout.
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
        self._track_free = None
        if create:
            self._type_table = None
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
            # I'm not sure the check for pmem_root not NULL is needed.
            if not size or pmem_root == ffi.NULL or not pmem_root.type_table:
                raise RuntimeError("Pool {} not initialized completely".format(
                    self.filename))
            self._pmem_root = pmem_root
            self._type_table = self._resurrect(pmem_root.type_table)
            # Make sure any objects orphaned by a crash are cleaned up.
            # XXX should fix this to only be called when there is a crash.
            self.gc()

    def close(self):
        """This method closes the object pool.  The object pool itself lives on
        in the file that contains it and may be reopened at a later date, and
        all the objects in it accessed, using :func:`~nvm.pmemlog.open`.
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
        self.close()

    @property
    def _root(self):
        return self._resurrect(self._pmem_root.root_object)
    @_root.setter
    def _root(self, value):
        log.debug("setting 'root' to %r", value)
        with self, self.lock:
            oid = self._persist(value)
            self._tx_add_range_direct(
                ffi.addressof(self._pmem_root.root_object),
                ffi.sizeof('PObjPtr'))
            self._xdecref(self._pmem_root.root_object)
            self._pmem_root.root_object = oid
            self._incref(oid)

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
                # XXX we should maybe use a unique error code here and raise an
                # error on ECANCELED, I'm not sure.
                lib.pmemobj_tx_abort(0)
        if lib.pmemobj_tx_end() not in (0, errno.ECANCELED):
            _raise_per_errno()

    #
    # Memory management
    #
    # Although I'm defining these on the POP class, there is in fact global
    # state involved here.  These methods will work only when a transaction is
    # active, and the global state means that only one object pool may have a
    # transaction executing in a given thread at a time.  XXX should add a
    # check for this, probably in __enter__.

    def _malloc(self, size, type_num=POBJECT_TYPE_NUM):
        """Return a pointer to size bytes of newly allocated persistent memory.

        By default the pmemobject type number is POBJECT_TYPE_NUM; be careful
        to specify a different type number for non-PObject allocations.
        """
        log.debug('malloc: %r', size)
        if size == 0:
            return OID_NULL
        oid = _oid_as_tuple(_check_oid(lib.pmemobj_tx_zalloc(size, type_num)))
        log.debug('oid: %s', oid)
        return oid

    def _malloc_ptrs(self, count):
        """Return pointer to enough persistent memory for count pointers.

        The pmem type number is set to POBJPTR_ARRAY_TYPE_NUM.
        """
        log.debug('malloc_ptrs: %r', count)
        return self._malloc(count * ffi.sizeof('PObjPtr'),
                            type_num=POBJPTR_ARRAY_TYPE_NUM)

    def _realloc(self, oid, size, type_num=None):
        """Copy oid contents into size bytes of new persistent memory.

        Return pointer to the new memory.
        """
        oid = _oid_as_tuple(oid)
        log.debug('realloc: %r %r', oid, size)
        if size == 0:
            self._free(oid)
            return OID_NULL
        if type_num is None:
            type_num = lib.pmemobj_type_num(oid)
        oid = _check_oid(lib.pmemobj_tx_zrealloc(oid, size, type_num))
        log.debug('oid: %s', oid)
        return oid

    def _realloc_ptrs(self, oid, count):
        oid = _oid_as_tuple(oid)
        log.debug('realloc_ptrs: %r %r', oid, count)
        """As _realloc, but the new memory is enough for count pointers."""
        return self._realloc(oid, count * ffi.sizeof('PObjPtr'),
                             POBJPTR_ARRAY_TYPE_NUM)

    def _free(self, oid):
        """Free the memory pointed to by oid."""
        oid = _oid_as_tuple(oid)
        log.debug('free: %r', oid)
        _check_errno(lib.pmemobj_tx_free(oid))

    def _direct(self, oid):
        """Return the real memory address where oid lives."""
        oid = _oid_as_tuple(oid)
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
        # XXX WeakValueDictionary?
        # XXX I'm not sure we can get away with mapping OID_NULL
        # to None here, but try it and see.
        self._resurrect_cache = {_oid_as_tuple(OID_NULL): None}

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
        # This oid should always come from a malloc, and thus be a tuple,
        # and so the correct value to use as a key without calling _as_tuple.
        self._persist_cache[key] = oid
        self._resurrect_cache[oid] = obj
        log.debug('new %s object: %r', cls_str, oid)
        return oid

    def _resurrect(self, oid):
        """Return python object representing the data stored at oid."""
        oid = _oid_as_tuple(oid)
        # XXX need multiple debug levels
        #log.debug('resurrect: %r', oid)
        try:
            obj = self._resurrect_cache[oid]
            #log.debug('resurrected from cache: %r', obj)
            return obj
        except KeyError:
            pass
        obj_ptr = ffi.cast('PObject *', self._direct(oid))
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
        """Increment the reference count of oid."""
        oid = _oid_as_tuple(oid)
        p_obj = ffi.cast('PObject *', self._direct(oid))
        log.debug('incref %r %r', oid, p_obj.ob_refcnt + 1)
        with self:
            self._tx_add_range_direct(p_obj, ffi.sizeof('PObject'))
            p_obj.ob_refcnt += 1

    def _decref(self, oid):
        """Decrement the reference count of oid, and free it if zero."""
        oid = _oid_as_tuple(oid)
        p_obj = ffi.cast('PObject *', self._direct(oid))
        log.debug('decref %r %r', oid, p_obj.ob_refcnt - 1)
        with self:
            # XXX also need to remove oid from resurrect and persist caches
            self._tx_add_range_direct(p_obj, ffi.sizeof('PObject'))
            assert p_obj.ob_refcnt > 0
            p_obj.ob_refcnt -= 1
            if p_obj.ob_refcnt < 1:
                self._deallocate(oid)

    def _xdecref(self, oid):
        """decref oid if it is not OID_NULL."""
        if not _oids_eq(OID_NULL, oid):
            self._decref(oid)

    def _deallocate(self, oid):
        """Deallocate the memory occupied by oid."""
        log.debug("deallocating %s", oid)
        with self:
            # XXX could have a type cache so we don't have to resurrect here.
            obj = self._resurrect(oid)
            if hasattr(obj, '_deallocate'):
                obj._deallocate()
            self._free(oid)
        if self._track_free is not None:
            self._track_free.add(oid)

    # If I didn't have to support python2 I'd make debug keyword only.
    def gc(self, debug=False):
        """Examine all PObjects and free those no longer referenced.

        There are two aspects to this: gc run at startup will clear out objects
        that were allocated but never assigned, and gc run other times (eg: on
        close) will look for cycles of objects with no external references and
        free the cycles.

        This is a public method to allow an application to clean up cycles
        on demand.  It is not run automatically except at open and close.
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

        # Catalog all PObjects.
        oid = lib.pmemobj_first(self._pool_ptr)
        while not _oids_eq(OID_NULL, oid):
            oid = _oid_as_tuple(oid)
            type_num = lib.pmemobj_type_num(oid)
            # XXX Could make the _PTR lists PObjects too so they are tracked.
            if type_num == POBJECT_TYPE_NUM:
                obj =  ffi.cast('PObject *', self._direct(oid))
                if debug:
                    if obj.ob_refcnt < 0:
                        log.error("Negative refcount (%s): %s %r",
                                  obj.ob_refcnt, oid, self._resurrect(oid))
                assert obj.ob_refcnt >= 0, '%s has negative refcnt' % oid
                # XXX move this cache to the POP?
                type_code = obj.ob_type
                if type_code not in types:
                    types[type_code] = _find_class_from_string(
                                            self._type_table[type_code])
                typ = types[type_code]
                type_counts[typ.__name__] += 1
                if not obj.ob_refcnt:
                    if debug:
                        log.debug('gc: orphan: %s %s %r',
                                  oid, obj.ob_refcnt, self._resurrect(oid))
                    orphans.add(oid)
                elif hasattr(typ, '_traverse'):
                    if debug:
                        log.debug('gc: container: %s %s %r',
                                  oid, obj.ob_refcnt, self._resurrect(oid))
                    containers.add(oid)
                else:
                    if debug:
                        log.debug('gc: other: %s %s %r',
                                  oid, obj.ob_refcnt, self._resurrect(oid))
                    other.add(oid)
            oid = lib.pmemobj_next(oid)
        gc_counts['containers-total'] = len(containers)
        gc_counts['other-total'] = len(other)

        # Clean up refcount 0 orphans (from a crash or code bug).
        log.debug("gc: deallocating %s orphans", len(orphans))
        gc_counts['orphans0-gced'] = len(orphans)
        for oid in orphans:
            if debug:
                # XXX This should be a non debug warning on close.
                log.warning("deallocating orphan (refcount 0): %s %r",
                            oid, self._resurrect(oid))
            self._deallocate(oid)

        # Trace the object tree, removing objects that are referenced.
        containers.remove(self._type_table._oid)
        live = [self._type_table._oid]
        if hasattr(self._root, '_traverse'):
            containers.remove(self._root._oid)
            live.append(self._root._oid)
        elif self._root is not None:
            oid = _oid_as_tuple(self._pmem_root.root_object)
            if debug:
                log.debug('gc: non-container root: %s %r', oid, self._root)
            other.remove(oid)
        for oid in live:
            if debug:
                log.debug('gc: checking live %s %r',
                          oid, self._resurrect(oid))
            for sub_oid in self._resurrect(oid)._traverse():
                sub_key = _oid_as_tuple(sub_oid)
                if sub_key in containers:
                    if debug:
                        log.debug('gc: refed container %s %r',
                                   sub_key, self_resurrect(sub_oid))
                    containers.remove(sub_key)
                    live.append(sub_key)
                elif sub_key in other:
                    if debug:
                        log.debug('gc: refed oid %s %r',
                                  sub_key, self._resurrect(sub_oid))
                    other.remove(sub_key)
                    gc_counts['other-live'] += 1
        gc_counts['containers-live'] = len(live)

        # Everything left is unreferenced via the root, deallocate it.
        log.debug('gc: deallocating %s containers', len(containers))
        self._track_free = set()
        for oid in containers:
            if oid in self._track_free:
                continue
            if debug:
                log.debug('gc: deallocating container %s %r',
                          oid, self._resurrect(oid))
            with self:
                # incref so we don't try to deallocate us during cycle clear.
                self._incref(oid)
                self._deallocate(oid)
                # deallocate frees oid, so no decref.
        gc_counts['collections-gced'] = len(containers)
        log.debug('gc: deallocating %s new orphans', len(other))
        for oid in other:
            if oid in self._track_free:
                continue
            log.warning("Orphaned with postive refcount: %s: %s",
                oid, self._resurrect(oid))
            self._deallocate(oid)
            gc_counts['orphans1-gced'] += 1
        gc_counts['other-gced'] = len(other) - gc_counts['orphans1-gced']
        self._track_free = None
        log.debug('gc: end')

        return dict(type_counts), dict(gc_counts)

    # XXX temporary
    def _oid_as_tuple(self, oid):
        return _oid_as_tuple(oid)
    def _oids_eq(self, *args):
        return _oids_eq(*args)


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
    pmem_root = lib.pmemobj_root(pop.mm._pool_ptr, ffi.sizeof('PRoot'))
    pmem_root = ffi.cast('PRoot *', pop.mm._direct(pmem_root))
    with pop:
        # Dummy first two elements; they are handled as special cases.
        type_table = PersistentList(
            [_class_string(PersistentList), _class_string(str)],
            __manager__=pop.mm)
        lib.pmemobj_tx_add_range_direct(pmem_root, ffi.sizeof('PRoot'))
        pmem_root.type_table = type_table._oid
        pop.mm._incref(type_table._oid)
        pop.mm._resurrect_cache[type_table._oid] = type_table
        pop.mm._pmem_root = pmem_root
    pop.mm._type_table = type_table
    return pop
