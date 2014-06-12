"""Tests for misc utilities"""

from harvester.utils import to_ordinal


def test_to_ordinal():
    assert to_ordinal(1) == '1st'
    assert to_ordinal(2) == '2nd'
    assert to_ordinal(3) == '3rd'
    assert to_ordinal(4) == '4th'
    assert to_ordinal(5) == '5th'

    assert to_ordinal(10) == '10th'
    assert to_ordinal(11) == '11th'
    assert to_ordinal(12) == '12th'
    assert to_ordinal(13) == '13th'
    assert to_ordinal(14) == '14th'

    assert to_ordinal(20) == '20th'
    assert to_ordinal(21) == '21st'
    assert to_ordinal(22) == '22nd'
    assert to_ordinal(23) == '23rd'
    assert to_ordinal(24) == '24th'

    assert to_ordinal(111) == '111th'
    assert to_ordinal(121) == '121st'

    # Doesn't make much sense, but still..
    assert to_ordinal(0) == '0th'
    assert to_ordinal(-1) == '-1st'
    assert to_ordinal(-2) == '-2nd'
    assert to_ordinal(-3) == '-3rd'
    assert to_ordinal(-4) == '-4th'
    assert to_ordinal(-11) == '-11th'
    assert to_ordinal(-21) == '-21st'
    assert to_ordinal(-31) == '-31st'
