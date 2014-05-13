"""
Test extraction of stuff from XML.
"""


import lxml.etree

from harvester.utils import xml_to_json


def test_xml_to_json():
    XMLFILE = """
    <Person>
      <userinfo>
        <name>
          <first>John</first>
          <last>Doe</last>
        </name>
      </userinfo>
      <contactinfo>
        <email>john.doe@example.com</email>
        <fax>0123 456 789</fax>
        <address>
          <street>Somestreet</street>
          <city>Somecity</city>
        </address>
      </contactinfo>
    </Person>
    """
    CONFIGURATION = {
        'userinfo': {
            '_name': 'User information',
            'name': {
                'first': 'First name',
                'last': 'Last name',
            },
        },
        'contactinfo': {
            'address': {
                '_name': 'Address',
                'street': 'Street',
                'city': 'City',
            },
            'email': 'Email',
        }
    }
    EXPECTED_OUTPUT = {
        "User information": {
            "First name": "John",
            "Last name": "Doe"
        },
        "Address": {
            "Street": "Somestreet",
            "City": "Somecity"
        },
        "Email": "john.doe@example.com"
    }

    xml = lxml.etree.fromstring(XMLFILE)
    assert xml_to_json(xml, CONFIGURATION) == EXPECTED_OUTPUT


def test_extracting_attributes():
    XMLFILE = """
    <root><element attr="value" /></root>
    """
    CONFIGURATION = {
        '/root': {
            'element': {
                '_name': 'Element',
                '@attr': 'Attribute',
            }
        }
    }
    EXPECTED_OUTPUT = {
        'Element': {
            'Attribute': 'value',
        }
    }

    xml = lxml.etree.fromstring(XMLFILE)
    assert xml_to_json(xml, CONFIGURATION) == EXPECTED_OUTPUT


def test_extracting_multiple_attributes():
    xml = lxml.etree.fromstring("""
    <root>
        <element attr="value0" />
        <element attr="value1" />
        <element attr="value2" />
    </root>
    """)

    CONFIGURATION = {
        '/root': {
            'element': {
                '_name': 'Element',
                '@attr': {'_name': 'Attribute', '_type': 'list:str'},
            }
        }
    }
    EXPECTED_OUTPUT = {
        'Element': {
            'Attribute': ['value0', 'value1', 'value2'],
        }
    }

#     assert xml_to_json(xml, {
#         '/root/element': {
#             '_name': 'Element',
#             '@attr': 'Attribute',
#         }
#     }) == {
#         'Element': {'Attribute': ['value0', 'value1', 'value2']}
#     }


def test_matching_list():
    XMLFILE = """
    <root>
        <block1>
            <aaa1>Value-A1</aaa1>
            <aaa2>Value-A2</aaa2>
            <aaa3>Value-A3</aaa3>
            <bbb1>Value-B1</bbb1>
            <bbb2>Value-B2</bbb2>
            <bbb3>Value-B3</bbb3>
        </block1>
    </root>
    """
    xml = lxml.etree.fromstring(XMLFILE)

    assert xml_to_json(xml, {
        '/root/block1': [
            {
                '_name': 'A-Tags',
                'aaa1': 'A1',
                'aaa2': 'A2',
                'aaa3': 'A3',
            },
            {
                '_name': 'B-Tags',
                'bbb1': 'B1',
                'bbb2': 'B2',
                'bbb3': 'B3',
            },
        ]
    }) == {
        'A-Tags': {
            'A1': 'Value-A1',
            'A2': 'Value-A2',
            'A3': 'Value-A3',
        },
        'B-Tags': {
            'B1': 'Value-B1',
            'B2': 'Value-B2',
            'B3': 'Value-B3',
        },
    }
