"""
Test dictionary flattening / unflattening
"""

from harvester.utils import flatten_dict, unflatten_dict


NESTED_DICT = {
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

FLAT_DICT = {
    ('aaa', 'A1'): 'Foo',
    ('aaa', 'A2'): 'Bar',
    ('bbb', 'B1', 'B11'): 'foo',
    ('bbb', 'B2'): 'bar',
    ('ccc',): 'BAZ',
}


def test_flatten_dict():
    assert flatten_dict(NESTED_DICT) == FLAT_DICT


def test_unflatten_dict():
    assert unflatten_dict(FLAT_DICT) == NESTED_DICT
