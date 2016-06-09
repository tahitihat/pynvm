import os
import sys
import unittest
import uuid

from nvm import pmemobj


class Test(unittest.TestCase):

    # XXX I'm not sure how one gets a real pmem file, so keep this factored.
    def _test_fn(self):
        fn = "{}.pmem".format(uuid.uuid4())
        self.addCleanup(lambda: os.remove if os.path.exists(fn) else None)
        return fn

    def assertMsgBits(self, msg, *bits):
        for bit in bits:
            self.assertIn(bit, msg)

    def test_create_open_close_dont_raise(self):
        fn = self._test_fn()
        pop = pmemobj.create(fn, pmemobj.MIN_POOL_SIZE)
        pop.close()
        pop = pmemobj.open(fn)
        pop.close()

    def test_implicit_close_after_create(self):
        fn = self._test_fn()
        pop = pmemobj.create(fn, pmemobj.MIN_POOL_SIZE)

    def test_implicit_close_after_open(self):
        fn = self._test_fn()
        pop = pmemobj.create(fn, pmemobj.MIN_POOL_SIZE)
        pop.close()
        pop = pmemobj.open(fn)

    def test_small_pool_size_error(self):
        fn = self._test_fn()
        with self.assertRaises(ValueError) as cm:
            pop = pmemobj.create(fn, pmemobj.MIN_POOL_SIZE-1)
        self.assertMsgBits(str(cm.exception),
                           str(pmemobj.MIN_POOL_SIZE-1),
                           str(pmemobj.MIN_POOL_SIZE))


if __name__ == '__main__':
    unittest.main()
