# -*- coding: utf8 -*-
import sys
import unittest

from nvm import pmemobj

from tests.support import TestCase, parameterize


class TestFoo(object):
    pass


class TestPersistentObjectPool(TestCase):

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
        del pop

    def test_implicit_close_after_open(self):
        fn = self._test_fn()
        pop = pmemobj.create(fn)
        pop.close()
        pop = pmemobj.open(fn)
        del pop

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
        self.addCleanup(pop.close)
        pop.root = pop.new(pmemobj.PersistentList, test_list)
        pop.close()
        pop = pmemobj.open(fn)
        self.assertEqual(pop.root, test_list)

    def test_transaction_abort_on_python_exception(self):
        fn = self._test_fn()
        pop = pmemobj.create(fn)
        self.addCleanup(pop.close)
        def tester():
            with pop.transaction():
                pop.root = 10
                raise Exception('boo')
        with self.assertRaisesRegex(Exception, 'boo'):
            tester()
        self.assertEqual(pop.root, None)

    def test_duplicate_close(self):
        fn = self._test_fn()
        pop = pmemobj.create(fn)
        pop.close()
        pop.close()

    def test_create_is_error_if_already_exists(self):
        fn = self._test_fn()
        pop = pmemobj.create(fn)
        pop.close()
        with self.assertRaises(OSError):
            pmemobj.create(fn)

    def test_open_is_error_if_does_not_exist(self):
        fn = self._test_fn()
        with self.assertRaises(OSError):
            pmemobj.open(fn)

    def test_filename_is_preserved(self):
        fn = self._test_fn()
        pop = pmemobj.create(fn)
        self.addCleanup(pop.close)
        self.assertEqual(pop.filename, fn)

    def test_unknown_nonpersistent_type(self):
        fn = self._test_fn()
        pop = pmemobj.create(fn)
        self.addCleanup(pop.close)
        with self.assertRaises(TypeError) as cm:
            pop.root = TestFoo()
        self.assertMsgBits(str(cm.exception), "on't know how", "TestFoo")


@parameterize
class TestSimpleImmutablePersistence(TestCase):

    objs_params = dict(int=5,
                       float=10.5,
                       string='abcde',
                       ustring='abő')
    if sys.version_info[0] < 3:
        objs_params['long_int'] = sys.maxint * 2

    def objs_as_root_object(self, obj):
        fn = self._test_fn()
        pop = pmemobj.create(fn)
        self.addCleanup(pop.close)
        pop.root = obj
        self.assertEqual(pop.root, obj)
        pop.close()
        pop = pmemobj.open(fn)
        self.assertEqual(pop.root, obj)
        pop.close()


class TestGC(TestCase):

    def _pop(self):
        self.fn = self._test_fn()
        pop = pmemobj.create(self.fn)
        self.addCleanup(pop.close)
        return pop

    def assertGCCollectedNothing(self, gc_counts):
        for k in [k for k in gc_counts.keys() if k.endswith('-gced')]:
            self.assertEqual(gc_counts[k], 0)

    def test_type_count(self):
        pop = self._pop()
        type_counts, gc_counts = pop.gc(debug=True)
        # The type table is a persistent list, and each type string
        # it stores is a string, and we have two types to start with.
        self.assertEqual(type_counts, {
            'PersistentList': 1,
            'str': 2,
            })
        pop.root = pop.new(pmemobj.PersistentList, [1, 'a', 3.6, 3])
        type_counts, gc_counts = pop.gc(debug=True)
        # Now we also have two additional types.
        self.assertEqual(type_counts, {
            'PersistentList': 2,
            'int': 2,
            'str': 5,
            'float': 1,
            })


    def test_root_immutable_assignment_gcs(self):
        pop = self._pop()
        pop.root = 12
        before = pop.gc(debug=True)
        pop.root = 15
        after = pop.gc(debug=True)
        # We've replaced one int with another, so the types counts should be
        # the same if the first one was deallocated.
        self.assertEqual(before, after)
        # Nothing should have been collected, since refcounting handles it.
        self.assertGCCollectedNothing(after[1])

    def test_root_container_assignment_gcs(self):
        pop = self._pop()
        pop.root = pop.new(pmemobj.PersistentList, [1, 2])
        before = pop.gc(debug=True)
        pop.root = pop.new(pmemobj.PersistentList, [3, 4])
        after = pop.gc(debug=True)
        # Again we've replaced the value with one with equivalent counts.
        self.assertEqual(before, after)
        # Nothing should have been collected, since refcounting handles it.
        self.assertGCCollectedNothing(after[1])

    def test_collect_orphan(self):
        pop = self._pop()
        pop.new(pmemobj.PersistentList)
        type_counts, gc_counts = pop.gc()
        self.assertEqual(gc_counts['orphans0-gced'], 1)
        self.assertGCCollectedNothing(pop.gc()[1])

    def test_collect_cycle(self):
        pop = self._pop()
        pop.root = pop.new(pmemobj.PersistentList)
        pop.root.append(pop.new(pmemobj.PersistentList))
        pop.root.append(pop.new(pmemobj.PersistentList))
        pop.root[0].append(pop.root[1])
        pop.root[1].append(pop.root[0])
        type_counts, _ = pop.gc()
        self.assertEqual(type_counts['PersistentList'], 4)
        pop.root.clear()
        type_counts, gc_counts = pop.gc(debug=True)
        self.assertEqual(type_counts['PersistentList'], 4)
        self.assertEqual(gc_counts['collections-gced'], 2)


if __name__ == '__main__':
    unittest.main()
