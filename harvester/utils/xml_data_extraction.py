"""
Utility functions to extract stuff from XML.

A.K.A.

Some weaponry to help fighting & getting meaningful information from
a bunch of stupid xml files.
"""

from collections import defaultdict, Sequence, Mapping
import copy

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

    # It is possible to simply use a string as value, as a shortcut
    # for specifying the found item should just be used as a field.
    if isinstance(conf, basestring):
        # This is just a shortcut
        if conf.startswith('+'):
            conf = {'_name': conf[1:], '_type': 'list:str'}
        else:
            conf = {'_name': conf, '_type': 'str'}

    # A list of possible configurations meaning we can follow various paths.
    # Will simply keep going for each possible path, then merge the results.
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

        if not isinstance(xml, Sequence):
            xml = [xml]

        if conf['_type'].startswith('list:'):
            # We want to return a list of results get by matching
            # configuration on each XML element.

            _type = conf['_type'][len('list:'):]
            new_conf = copy.deepcopy(conf)
            new_conf['_type'] = _type
            return [xml_to_json(x, new_conf) for x in xml]

        if conf['_type'] == 'sub':
            # We want to match sub-rules and keep matching
            # with sub-configurations, for each element.

            result = {}
            for rule, ruleconf in _rules.iteritems():
                for xml_elem in xml:
                    matching_elems = xml_elem.xpath(rule)
                    result.update(xml_to_json(matching_elems, ruleconf))

            if _conf.get('_name'):
                return {_conf['_name']: result}

            return result

        # This is a plain value to be returned as-is.
        # If we still have a tag, we want to return its text;
        # otherwise, we return the literal value.
        if isinstance(xml, Sequence):
            # Just take only the latest one..
            if len(xml) < 1:
                return None
            xml = xml[-1]

        if isinstance(xml, lxml.etree._Element):
            value = xml.text
        else:
            value = xml

        # todo: perform casting if required!
        if conf['_type'] == 'str':
            value = str(value)

        elif conf['_type'] == 'int':
            value = int(value)

        elif conf['_type'] == 'float':
            value = float(value)

        elif conf['_type'] == 'bool':
            if value.lower() in ('0', 'false', 'f'):
                value = False

            elif value.lower() in ('1', 'true', 't'):
                value = True

            else:
                raise ValueError("Invalid boolean value: {0!r}".format(value))

        return {conf['_name']: value}

    raise TypeError("Unsupported configuration type: {0!r}".format(type(conf)))
