"""
Test extraction of stuff from XML.
"""


import lxml.etree

from harvester.utils import xml_to_json


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

XMLFILE = """<Person>
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
</Person>"""

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


def test_xml_to_json():
    xml = lxml.etree.fromstring(XMLFILE)
    assert xml_to_json(xml, CONFIGURATION) == EXPECTED_OUTPUT
