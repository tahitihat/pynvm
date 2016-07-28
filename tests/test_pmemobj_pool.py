# -*- coding: utf8 -*-
import logging
import sys
import unittest

from nvm import pmemobj

from tests.support import TestCase, parameterize, errno


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

    def test_constructor_default_flag_is_w(self):
        fn = self._test_fn()
        with self.assertRaises(OSError):
            pmemobj.PersistentObjectPool(fn)

    def test_constructor_flag_x(self):
        fn = self._test_fn()
        pop = pmemobj.create(fn)
        pop.close()
        with self.assertRaises(OSError):
            pmemobj.PersistentObjectPool(fn, flag='x')

    def test_constructor_flag_c(self):
        fn = self._test_fn()
        pop = pmemobj.PersistentObjectPool(fn, flag='c')
        pop.root = 10
        pop.close()
        pop = pmemobj.PersistentObjectPool(fn)
        self.assertEqual(pop.root, 10)

    @unittest.skipIf(sys.version_info[0] < 3, 'test only runs on python3')
    def test_debug(self):
        # When debug is on, orphans are logged as warnings (in production
        # an orphan is not necessarily a bug).
        fn = self._test_fn()
        pop = pmemobj.PersistentObjectPool(fn, flag='c', debug=True)
        pop.new(pmemobj.PersistentList)
        with self.assertLogs('nvm.pmemobj', logging.WARNING) as cm:
            pop.gc()
        self.assertTrue(any('orphan' in l for l in cm.output))

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
                       ustring='abő',
                       none=None)
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


class TestTransactions(TestCase):

    def _setup(self):
        self.fn = self._test_fn()
        pop = self.pop = pmemobj.create(self.fn)
        self.addCleanup(lambda: self.pop.close())
        return pop

    def _reopen_pop(self):
        self.pop.close()
        pop = self.pop = pmemobj.open(self.fn)
        return pop

    def test_non_context_commit(self):
        pop = self._setup()
        trans = pop.transaction()
        trans.begin()
        pop.root = 10
        trans.commit()
        self.assertEqual(pop.root, 10)
        pop = self._reopen_pop()
        self.assertEqual(pop.root, 10)

    def test_non_context_abort_raises_and_resets_state(self):
        pop = self._setup()
        # XXX what to do about the invalid message?
        #with self.assertRaisesRegex(OSError, 'canceled'):
        with self.assertRaises(OSError):
            trans =  pop.transaction()
            trans.begin()
            pop.root = 10
            trans.abort(errno.ECANCELED)
        self.assertIsNone(pop.root)

    def test_context(self):
        # This just tests that no errors happen; it requires a crash
        # or abort to prove that the transaction actually worked.
        pop = self._setup()
        with pop.transaction():
            pop.root = 10
        self.assertEqual(pop.root, 10)
        pop = self._reopen_pop()
        self.assertEqual(pop.root, 10)

    def test_context_abort_raises_and_resets_state(self):
        pop = self._setup()
        #with self.assertRaisesRegex(OSError, 'canceled'):
        with self.assertRaises(OSError):
            with pop.transaction() as trans:
                pop.root = 10
                trans.abort(errno.ECANCELED)
        self.assertIsNone(pop.root)
        pop = self._reopen_pop()
        self.assertIsNone(pop.root)
        # Make sure transaction machinery is reset by doing another.
        with pop.transaction() as trans:
            pop.root = 10

    def test_context_aborts_on_python_exception(self):
        pop = self._setup()
        with self.assertRaisesRegex(Exception, 'boo'):
            with pop.transaction():
                pop.root = 10
                raise Exception('boo')
        self.assertIsNone(pop.root)
        with pop.transaction():
            pop.root = 10

    def test_non_context_commit_aborts_inside_context(self):
        pop = self._setup()
        with self.assertRaises(RuntimeError):
            with pop.transaction() as trans:
                pop.root = 10
                trans.commit()
        self.assertIsNone(pop.root)
        with pop.transaction():
            pop.root = 10

    def xest_unclosed_non_context_transaction_in_context_aborts(self):
        pop = self._setup()
        with self.assertRaises(RuntimeError):
            with pop.transaction() as trans:
                pop.root = 10
                trans.begin()
        self.assertIsNone(pop.root)
        with pop.transaction():
            pop.root = 10

    def test_abort_outside_transaction_raises(self):
        pop = self._setup()
        trans = pop.transaction()
        with self.assertRaisesRegex(RuntimeError, 'abort.*outside.*trans'):
            pop.root = 10
            trans.abort()
        self.assertEqual(pop.root, 10)

    def test_commit_outside_transaction_raises(self):
        pop = self._setup()
        trans = pop.transaction()
        with self.assertRaisesRegex(RuntimeError, 'commit.*outside.*trans'):
            pop.root = 10
            trans.commit()
        self.assertEqual(pop.root, 10)

    def test_abort_nested_transactions(self):
        pop = self._setup()
        with self.assertRaises(OSError):
            with pop.transaction():
                pop.root = 10
                with pop.transaction():
                    pop.root = 20
                    with pop.transaction() as trans:
                        pop.root = 30
                        trans.abort()
        self.assertIsNone(pop.root)
        with pop.transaction():
            pop.root = 10

    def test_context_abort_nested_transactions(self):
        pop = self._setup()
        with self.assertRaises(RuntimeError):
            with pop.transaction():
                pop.root = 10
                with pop.transaction():
                    pop.root = 20
                    with pop.transaction() as trans:
                        pop.root = 30
                        trans.commit()
        self.assertIsNone(pop.root)
        with pop.transaction():
            pop.root = 10


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
