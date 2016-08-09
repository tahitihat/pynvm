"""
A fake PersistentObjectPool.  It does the persistence magic by using json
on the root object to store it in a file, and the transactions are fake.  But
it allows for testing the "this persists" logic of a program without dealing
with any bugs that may exist in the real PersistentObjectPool.

"""

import os
import pickle

from contextlib import contextmanager

#from nvm.pmemobj import PersistentList, PersistentDict

class PersistentList(object):
    pass
class PersistentDict(object):
    pass

class PersistentObjectPool:
    def __init__(self, filename, flag='w', *args, **kw):
        self.filename = filename
        exists = os.path.exists(filename)
        if flag == 'w' or (flag == 'c' and exists):
            with open(filename, 'rb') as f:
                self.root = pickle.load(f)[0]
        elif flag == 'x' or (flag == 'c' and not exists):
            with open(filename, 'wb') as f:
                self.root = None
                pickle.dump([None], f)
        elif flag == 'r':
            raise ValueError("Read-only mode is not supported")
        else:
            raise ValueError("Invalid flag value {}".format(flag))

    def new(self, typ, *args, **kw):
        if typ == PersistentList:
            return list(*args, **kw)
        if typ == PersistentDict:
            return dict(*args, **kw)

    @contextmanager
    def transaction(self):
        yield None

    def close(self):
        with open(self.filename+'.tmp', 'wb') as f:
            pickle.dump([self.root], f)
        os.rename(self.filename+'.tmp', self.filename)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
