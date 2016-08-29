import collections
import errno
if not hasattr(errno, 'ECANCELED'):
    errno.ECANCELED = 125  # 2.7 errno doesn't define this, so guess.
import os
import sys
import unittest
import uuid

# This is an ugly hack but it works; you have to say "-v -v", not "-vv".
verbose = sys.argv.count('-v') + sys.argv.count('--verbose')
verbose += int(os.environ.get('TEST_VERBOSE', 0))
if verbose > 1:
    import logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(name)-20s %(levelname)-8s %(message)s')
    if verbose < 3:
        logging.getLogger('nvm.pmemobj.trace').setLevel(logging.WARNING)


class TestCase(unittest.TestCase):

    # XXX I'm not sure how one gets a real pmem file, so keep this factored.
    def _test_fn(self):
        fn = "{}.pmem".format(uuid.uuid4())
        self.addCleanup(lambda: os.remove(fn) if os.path.exists(fn) else None)
        return fn

    if sys.version_info[0] < 3:
        assertRaisesRegex = unittest.TestCase.assertRaisesRegexp


def parameterize(cls):
    """A test method parameterization class decorator.

    Parameters are specified as the value of a class attribute that ends with
    the string '_params'.  Call the portion before '_params' the prefix.  Then
    a method to be parameterized must have the same prefix, the string
    '_as_', and an arbitrary suffix.

    The value of the _params attribute may be either a dictionary or a list.
    The values in the dictionary and the elements of the list may either be
    single values, or a list.  If single values, they are turned into single
    element tuples.  However derived, the resulting sequence is passed via
    *args to the parameterized test function.

    In a _params dictioanry, the keys become part of the name of the generated
    tests.  In a _params list, the values in the list are converted into a
    string by joining the string values of the elements of the tuple by '_' and
    converting any blanks into '_'s, and this become part of the name.
    The  full name of a generated test is a 'test_' prefix, the portion of the
    test function name after the  '_as_' separator, plus an '_', plus the name
    derived as explained above.

    For example, if we have:

        count_params = range(2)

        def count_as_foo_arg(self, foo):
            self.assertEqual(foo+1, myfunc(foo))

    we will get parameterized test methods named:
        test_foo_arg_0
        test_foo_arg_1
        test_foo_arg_2

    Or we could have:

        example_params = {'foo': ('bar', 1), 'bing': ('bang', 2)}

        def example_as_myfunc_input(self, name, count):
            self.assertEqual(name+str(count), myfunc(name, count))

    and get:
        test_myfunc_input_foo
        test_myfunc_input_bing

    Note: if and only if the generated test name is a valid identifier can it
    be used to select the test individually from the unittest command line.

    """
    paramdicts = {}
    testers = collections.defaultdict(list)
    for name, attr in cls.__dict__.items():
        if name.endswith('_params'):
            if not hasattr(attr, 'keys'):
                d = {}
                for x in attr:
                    if not hasattr(x, '__iter__') or hasattr(x, 'encode'):
                        x = (x,)
                    n = '_'.join(str(v) for v in x).replace(' ', '_')
                    d[n] = x
                attr = d
            paramdicts[name[:-7] + '_as_'] = attr
        if '_as_' in name:
            testers[name.split('_as_')[0] + '_as_'].append(name)
    testfuncs = {}
    for name in paramdicts:
        if name not in testers:
            raise ValueError("No tester found for {}".format(name))
    for name in testers:
        if name not in paramdicts:
            raise ValueError("No params found for {}".format(name))
    for name, attr in cls.__dict__.items():
        for paramsname, paramsdict in paramdicts.items():
            if name.startswith(paramsname):
                testnameroot = 'test_' + name[len(paramsname):]
                for paramname, params in paramsdict.items():
                    if (not hasattr(params, '__iter__')
                            or hasattr(params, 'encode')):
                        params = (params,)
                    test = (lambda self, name=name, params=params:
                                    getattr(self, name)(*params))
                    testname = testnameroot + '_' + paramname
                    test.__name__ = testname
                    testfuncs[testname] = test
    for key, value in testfuncs.items():
        setattr(cls, key, value)
    return cls
