import collections

from .compat import recursive_repr, abc

from _pmem import lib, ffi    # XXX refactor to make this import unneeded


class PersistentList(abc.MutableSequence):
    """Persistent version of the 'list' type."""

    # XXX locking!
    # XXX tp_del method (see decref)
    # XXX All bookkeeping attrs should be _v_xxxx so that all other attrs
    #     (other than __manager__) can be made persistent.

    def __init__(self, *args, **kw):
        if '__manager__' not in kw:
            raise ValueError("__manager__ is required")
        mm = self.__manager__ = kw.pop('__manager__')
        if '_oid' not in kw:
            with mm:
                # XXX Will want to implement a freelist here, like CPython
                self._oid = mm.malloc(ffi.sizeof('PListObject'))
                ob = ffi.cast('PObject *', mm.direct(self._oid))
                ob.ob_type = mm._get_type_code(PersistentList)
        else:
            self._oid = kw.pop('_oid')
        if kw:
            raise TypeError("Unrecognized keyword argument(s) {}".format(kw))
        self._body = ffi.cast('PListObject *', mm.direct(self._oid))
        if args:
            if len(args) != 1:
                raise TypeError("PersistentList takes at most 1"
                                " argument, {} given".format(len(args)))
            self.extend(args[0])

    # Methods and properties needed to implement the ABC required methods.

    @property
    def _size(self):
        return ffi.cast('PVarObject *', self._body).ob_size

    @property
    def _allocated(self):
        return self._body.allocated

    @property
    def _items(self):
        mm = self.__manager__
        ob_items = mm.otuple(self._body.ob_items)
        if ob_items == mm.OID_NULL:
            return None
        return ffi.cast('PObjPtr *', mm.direct(ob_items))

    def _resize(self, newsize):
        mm = self.__manager__
        allocated = self._allocated
        # Only realloc if we don't have enough space already.
        if (allocated >= newsize and newsize >= allocated >> 1):
            assert self._items != None or newsize == 0
            with mm:
                mm.protect_range(self._body, ffi.sizeof('PVarObject'))
                ffi.cast('PVarObject *', self._body).ob_size = newsize
            return
        # We use CPython's overallocation algorithm.
        new_allocated = (newsize >> 3) + (3 if newsize < 9 else 6) + newsize
        if newsize == 0:
            new_allocated = 0
        items = self._items
        with mm:
            if items is None:
                items = mm.malloc_ptrs(new_allocated)
            else:
                items = mm.realloc_ptrs(self._body.ob_items, new_allocated)
            mm.protect_range(self._body, ffi.sizeof('PListObject'))
            self._body.ob_items = items
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
            mm.protect_range(items + index,
                                    ffi.offsetof('PObjPtr *', newsize))
            for i in range(size, index, -1):
                items[i] = items[i-1]
            v_oid = mm.persist(value)
            mm.incref(v_oid)
            items[index] = v_oid

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
            raise IndexError(index)
        return index

    def __setitem__(self, index, value):
        index = self._normalize_index(index)
        mm = self.__manager__
        items = self._items
        with mm:
            v_oid = mm.persist(value)
            mm.protect_range(ffi.addressof(items, index),
                                    ffi.sizeof('PObjPtr *'))
            mm.xdecref(items[index])
            items[index] = v_oid
            mm.incref(v_oid)

    def __delitem__(self, index):
        index = self._normalize_index(index)
        mm = self.__manager__
        size = self._size
        newsize = size - 1
        items = self._items
        with mm:
            mm.protect_range(ffi.addressof(items, index),
                                    ffi.offsetof('PObjPtr *', size))
            mm.decref(items[index])
            for i in range(index, newsize):
                items[i] = items[i+1]
            self._resize(newsize)

    def __getitem__(self, index):
        index = self._normalize_index(index)
        items = self._items
        return self.__manager__.resurrect(items[index])

    def __len__(self):
        return self._size

    # Additional list methods not provided by the ABC.

    @recursive_repr()
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

    def clear(self):
        if self._size == 0:
            return
        mm = self.__manager__
        items = self._items
        with mm:
            for i in range(self._size):
                # Grab oid in tuple form so the assignment can't change it
                oid = mm.otuple(items[i])
                if oid == mm.OID_NULL:
                    continue
                items[i] = mm.OID_NULL
                mm.decref(oid)
            self._resize(0)

    # Additional methods required by the pmemobj API.

    def _traverse(self):
        items = self._items
        for i in range(len(self)):
            yield items[i]

    def _deallocate(self):
        self.clear()
