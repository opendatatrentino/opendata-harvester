__all__ = ['flatten_dict', 'unflatten_dict']

import collections


def flatten_dict(dct):
    """
    Take a nested dictionary and "flatten" it::

        {
            'aaa': {
                'A1': 'Foo',
                'A2': 'Bar',
            },
            'bbb': {
                'B1': {'B11': 'foo'},
                'B2': 'bar',
            },
            'ccc': 'BAZ',
        }

    Becomes::

        {
            ('aaa', 'A1'): 'Foo',
            ('aaa', 'A2'): 'Bar',
            ('bbb', 'B1', 'B11'): 'foo',
            ('bbb', 'B2'): 'bar',
            ('ccc',): 'BAZ',
        }
    """

    output = {}

    def _flatten(obj, trail):
        for key, value in obj.iteritems():
            _trail = trail + (key,)

            if isinstance(value, collections.Mapping):
                # We want to keep digging
                _flatten(value, _trail)

            else:
                # We reached an end -> write the value
                output[_trail] = value

    _flatten(dct, tuple())

    return output


def unflatten_dict(dct):
    """
    Opposite of ``flatten_dict``::

        {
            ('aaa', 'A1'): 'Foo',
            ('aaa', 'A2'): 'Bar',
            ('bbb', 'B1', 'B11'): 'foo',
            ('bbb', 'B2'): 'bar',
            ('ccc',): 'BAZ',
        }

    Becomes::

        {
            'aaa': {
                'A1': 'Foo',
                'A2': 'Bar',
            },
            'bbb': {
                'B1': {'B11': 'foo'},
                'B2': 'bar',
            },
            'ccc': 'BAZ',
        }
    """
    output = {}

    def _put_value(obj, path, value):
        if len(path) == 1:
            # Found!
            obj[path[0]] = value
            return

        if len(path) == 0:
            raise ValueError("Invalid empty key")

        if path[0] not in obj:
            obj[path[0]] = {}

        _put_value(obj[path[0]], path[1:], value)

    for key, value in dct.iteritems():
        _put_value(output, key, value)

    return output
