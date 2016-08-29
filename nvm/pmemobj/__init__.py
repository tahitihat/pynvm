"""
.. module:: pmemobj
.. moduleauthor:: R. David Murray <rdmurray@bitdance.com>

:mod:`pmemobj` -- persistent python objects
==================================================================
"""

from .pool import open, create, MIN_POOL_SIZE, PersistentObjectPool
from .list import PersistentList
