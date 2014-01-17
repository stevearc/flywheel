""" flywheel """
try:
    from ._version import __version__  # pylint: disable=F0401
except ImportError:  # pragma: no cover
    __version__ = 'unknown'

import boto.dynamodb.types
from boto.dynamodb2.exceptions import ConditionalCheckFailedException
from boto.dynamodb.types import Binary
from boto.dynamodb2.types import (STRING, NUMBER, BINARY, STRING_SET,
                                  NUMBER_SET, BINARY_SET)
from decimal import Inexact, Rounded, Decimal


# HACK to force conversion of floats to Decimals
boto.dynamodb.types.DYNAMODB_CONTEXT.traps[Inexact] = False
boto.dynamodb.types.DYNAMODB_CONTEXT.traps[Rounded] = False


def float_to_decimal(f):  # pragma: no cover
    """ Monkey-patched replacement for boto's broken version """
    n, d = f.as_integer_ratio()
    numerator, denominator = Decimal(n), Decimal(d)
    ctx = boto.dynamodb.types.DYNAMODB_CONTEXT
    return ctx.divide(numerator, denominator)

boto.dynamodb.types.float_to_decimal = float_to_decimal


from .fields import Field, Composite, GlobalIndex, TypeDefinition, set_
from .models import Model
from .engine import Engine
