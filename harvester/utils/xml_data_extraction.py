"""
Utility functions to extract stuff from XML.

A.K.A.

Some weaponry to help fighting & getting meaningful information from
a bunch of stupid xml files.
"""

from collections import defaultdict, Sequence, Mapping

import lxml.etree

__all__ = ['xml_extract_text_values',
           'create_xml_to_json_draft_conf_from_xml',
           'xml_to_json']


def xml_extract_text_values(s):
    """
    Extract all the text found in an xml, along with its path.

    :param s: The XML data, as string
    :return: a dictionary mapping ``{path: [values ...]}``
    """

    tree = lxml.etree.fromstring(s)
    found_data = defaultdict(list)

    def _get_tag_name(elem):
        localname = lxml.etree.QName(elem.tag).localname
        if elem.prefix is not None:
            return ':'.join((elem.prefix, localname))
        return localname

    def _get_tag_path(elem):
        path = [_get_tag_name(x)
                for x in reversed(list(elem.iterancestors()))]
        path.append(_get_tag_name(elem))
        return '/' + '/'.join(path)

    # for elem in tree.xpath('//*[text()]'):
    for elem in tree.xpath('//*'):
        if elem.text is None:
            continue

        text = ' '.join(elem.text.split()).strip()

        if not text:
            continue  # was just garbage..

        path = _get_tag_path(elem)
        found_data[path].append(text)

    return dict(found_data)


def create_xml_to_json_draft_conf_from_xml(xml):
    """
    Generate draft configuration for :py:func:`xml_to_json` by trying to
    figure out paths containing meaningful data.

    .. note:: we might also want to apply this on a **bunch** of xml files..
    """
    pass


def xml_to_json(xml, conf):
    """
    Convert an XML to JSON according to the specified configuration.

    Configuration is a bunch of nested dictionaries like this::

        {
            "_name": "Block name",
            <xpath_rule>: { ..other block.. }
        }

    **Example:**

    We take a configuration like this::

        {
            'userinfo': {
                '_name': 'User information',
                'name': {
                    'first': {'_name': 'First name'},
                    'last': {'_name': 'Last name'},
                },
            },
            'contactinfo': {
                'address': {
                    '_name': 'Address',
                    'street': {'_name': 'Street'},
                    'city': {'_name': 'City'},
                },
                'email': {'_name': 'Email'},
            }
        }

    Which, applied to an xml like this:

    .. code-block:: xml

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

    Will generate a json like this:

    .. code-block:: json

        {
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

    (we don't care about the fax number!)
    """

    def _split_conf_rules(conf):
        _conf = {}
        _rules = {}
        for key, value in conf.iteritems():
            if key.startswith('_'):
                _conf[key] = value
            else:
                _rules[key] = value
        return _conf, _rules

    if isinstance(conf, basestring):
        # This is just a shortcut
        conf = {'_name': conf, '_type': 'str'}

    if isinstance(conf, Sequence):
        # Just keep going with each thing, then merge
        result = {}
        for _conf in conf:
            result.update(xml_to_json(xml, _conf))
        return result

    if isinstance(conf, Mapping):
        # Split configuration and rules
        # Note: if we don't have rules

        conf.setdefault('_type', 'sub')
        _conf, _rules = _split_conf_rules(conf)

        if conf['_type'] == 'sub':
            # For each rule, apply next step to matching elements
            # then merge the results.

            result = {}
            for rule, ruleconf in _rules.iteritems():
                matching_elems = xml.xpath(rule)
                for elem in matching_elems:
                    result.update(xml_to_json(elem, ruleconf))

            if _conf.get('_name'):
                return {_conf['_name']: result}

            return result

        else:
            # This is a value -- figure out type and extract
            # values accordingly
            # todo: perform casting if required!
            value = xml if isinstance(xml, basestring) else xml.text
            return {conf['_name']: value}

    raise TypeError("Unsupported configuration type: {0!r}".format(type(conf)))
