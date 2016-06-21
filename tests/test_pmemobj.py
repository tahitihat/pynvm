import os
import sys
import unittest
import uuid

from nvm import pmemobj


verbose = sys.argv.count('-v') + sys.argv.count('--verbose')
verbose += int(os.environ.get('TEST_VERBOSE', 0))
if verbose > 1:
    import logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(name)-15s %(levelname)-8s %(message)s')



class TestCase(unittest.TestCase):

    # XXX I'm not sure how one gets a real pmem file, so keep this factored.
    def _test_fn(self):
        fn = "{}.pmem".format(uuid.uuid4())
        self.addCleanup(lambda: os.remove(fn) if os.path.exists(fn) else None)
        return fn


class Test(TestCase):

    def assertMsgBits(self, msg, *bits):
        for bit in bits:
            self.assertIn(bit, msg)

    def test_create_open_close_dont_raise(self):
        fn = self._test_fn()
        pop = pmemobj.create(fn)
        pop.close()
        pop = pmemobj.open(fn)
        pop.close()

    def test_implicit_close_after_create(self):
        fn = self._test_fn()
        pop = pmemobj.create(fn)

    def test_implicit_close_after_open(self):
        fn = self._test_fn()
        pop = pmemobj.create(fn)
        pop.close()
        pop = pmemobj.open(fn)

    def test_small_pool_size_error(self):
        fn = self._test_fn()
        with self.assertRaises(ValueError) as cm:
            pop = pmemobj.create(fn, pmemobj.MIN_POOL_SIZE-1)
        self.assertMsgBits(str(cm.exception),
                           str(pmemobj.MIN_POOL_SIZE-1),
                           str(pmemobj.MIN_POOL_SIZE))

    def test_list_of_strings_as_root_obj(self):
        # Lists and strings are our "built in" types (handled specially by the
        # code because they are used by the type table), so this exercises
        # the absolute minimum required functionality, but doesn't fully
        # exercise anything, including not really testing the type table.
        test_list = ['a', 'b', 'c', 'd']
        fn = self._test_fn()
        pop = pmemobj.create(fn)
        pop.root = pop.new(pmemobj.PersistentList, test_list)
        pop.close()
        pop = pmemobj.open(fn)
        self.assertEqual(pop.root, test_list)


class TestPersistentList(TestCase):

    def _make_list(self, arg):
        fn = self._test_fn()
        self.pop = pmemobj.create(fn)
        return self.pop.new(pmemobj.PersistentList, arg)

    def test_repr(self):
        lst = self._make_list(['a', 'b', 'c'])
        self.assertEqual(repr(lst), "PersistentList(['a', 'b', 'c'])")



if __name__ == '__main__':
    unittest.main()
