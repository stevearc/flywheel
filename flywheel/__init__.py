""" flywheel """
try:
    from ._version import *  # pylint: disable=F0401,W0401
except ImportError:
    __version__ = 'unknown'

from .fields import Field, Composite, GlobalIndex, Binary
from .models import Model, ValidationError
from .engine import Engine
from boto.dynamodb2.types import (STRING, NUMBER, BINARY, STRING_SET,
                                  NUMBER_SET, BINARY_SET)
