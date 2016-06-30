# -*- coding: utf8 -*-
import unittest

from nvm import pmemobj

from tests.support import TestCase


class TestPersistentList(TestCase):

    def _make_list(self, arg):
        self.fn = self._test_fn()
        self.pop = pmemobj.create(self.fn)
        self.addCleanup(self.pop.close)
        self.pop.root = self.pop.new(pmemobj.PersistentList, arg)
        return self.pop.root

    def _reread_list(self):
        self.pop.close()
        self.pop = pmemobj.open(self.fn)
        return self.pop.root

    def test_insert(self):
        lst = self._make_list([])
        lst.insert(0, 'b')
        self.assertEqual(lst, ['b'])
        lst = self._reread_list()
        self.assertEqual(lst, ['b'])
        lst.insert(-1, 'a')
        self.assertEqual(lst, ['a', 'b'])
        lst = self._reread_list()
        self.assertEqual(lst, ['a', 'b'])
        lst.insert(2, 'c')
        self.assertEqual(lst, ['a', 'b', 'c'])
        lst = self._reread_list()
        self.assertEqual(lst, ['a', 'b', 'c'])
        lst.insert(-10, 'z')
        self.assertEqual(lst, ['z', 'a', 'b', 'c'])
        lst = self._reread_list()
        self.assertEqual(lst, ['z', 'a', 'b', 'c'])
        lst.insert(10, 'y')
        self.assertEqual(lst, ['z', 'a', 'b', 'c', 'y'])
        lst = self._reread_list()
        self.assertEqual(lst, ['z', 'a', 'b', 'c', 'y'])

    def test_repr(self):
        expected = "PersistentList(['a', 'b', 'c'])"
        lst = self._make_list(['a', 'b', 'c'])
        self.assertEqual(repr(lst), expected)
        self.assertEqual(repr(self._reread_list()), expected)

    def test_getitem(self):
        lst = self._make_list(['a', 'b', 'c'])
        self.assertEqual(lst[0], 'a')
        self.assertEqual(lst[1], 'b')
        self.assertEqual(lst[2], 'c')
        lst = self._reread_list()
        self.assertEqual(lst[0], 'a')
        self.assertEqual(lst[1], 'b')
        self.assertEqual(lst[2], 'c')

    def test_getitem_index_errors(self):
        lst = self._make_list(['a', 'b', 'c'])
        with self.assertRaises(IndexError):
            lst[3]
        with self.assertRaises(IndexError):
            lst[-4]
        with self.assertRaises(IndexError):
            lst[10]
        with self.assertRaises(IndexError):
            lst[-10]

    def test_setitem(self):
        lst = self._make_list(['a', 'b', 'c'])
        lst[1] = 'z'
        self.assertEqual(lst, ['a', 'z', 'c'])
        lst = self._reread_list()
        self.assertEqual(lst, ['a', 'z', 'c'])
        lst[-3] = 'y'
        self.assertEqual(lst, ['y', 'z', 'c'])
        lst = self._reread_list()
        self.assertEqual(lst, ['y', 'z', 'c'])

    def test_setitem_index_errors(self):
        lst = self._make_list(['a', 'b', 'c'])
        with self.assertRaises(IndexError):
            lst[3] = 'z'
        with self.assertRaises(IndexError):
            lst[-4] = 'z'
        with self.assertRaises(IndexError):
            lst[10] = 'z'
        with self.assertRaises(IndexError):
            lst[-10] = 'z'

    def test_delitem(self):
        lst = self._make_list(['a', 'b', 'c'])
        del lst[1]
        self.assertEqual(lst, ['a', 'c'])
        lst = self._reread_list()
        self.assertEqual(lst, ['a', 'c'])
        del lst[-2]
        self.assertEqual(lst, ['c'])
        lst = self._reread_list()
        self.assertEqual(lst, ['c'])
        del lst[0]
        self.assertEqual(lst, [])
        lst = self._reread_list()
        self.assertEqual(lst, [])

    def test_delitem_index_errors(self):
        lst = self._make_list(['a', 'b', 'c'])
        with self.assertRaises(IndexError):
            del lst[3]
        with self.assertRaises(IndexError):
            del lst[-4]
        with self.assertRaises(IndexError):
            del lst[10]
        with self.assertRaises(IndexError):
            del lst[-10]

    def test_len(self):
        lst = self._make_list([])
        for i in range(6):
            self.assertEqual(len(lst), i)
            lst = self._reread_list()
            self.assertEqual(len(lst), i)
            lst.append('a')

    def test_clear(self):
        lst = self._make_list([1, 3, 2])
        lst.clear()
        self.assertEqual(lst, [])
        # Make sure the clear didn't break it.
        lst.append(1)
        self.assertEqual(lst, [1])


if __name__ == '__main__':
    unittest.main()
