""" Utilities for Python 2/3 compatibility """
import six


class UnicodeMixin(object):

    """ Mixin that handles __str__ and __bytes__. Just define __unicode__.  """
    if six.PY3:  # pragma: no cover
        def __str__(self):
            return self.__unicode__()

        def __bytes__(self):
            return self.__unicode__().encode('utf-8')
    else:
        def __str__(self):
            return self.__unicode__().encode('utf-8')
