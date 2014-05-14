"""
Utility functions to extract stuff from XML.

A.K.A.

Some weaponry to help fighting & getting meaningful information from
a bunch of stupid xml files.
"""

from collections import defaultdict, Sequence
import itertools

import lxml.etree

__all__ = ['xml_extract_text_values', 'XPathHelper']


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


class XPathHelper(Sequence):
    def __init__(self, elements):
        if not isinstance(elements, (list, tuple)):
            elements = [elements]
        self._elements = elements

    def xpath(self, rule):
        # todo: we can apply caching here to avoid keeping performing
        #       the same queries over and over..
        return XPathHelper(list(itertools.chain(
            *list(el.xpath(rule, namespaces=el.nsmap)
                  for el in self._elements)
        )))

    def __call__(self, *rules):
        if len(rules) == 0:
            return self
        xph = self.xpath(rules[0])
        if len(rules) > 1:
            return xph(*rules[1:])  # Keep digging..
        return xph

    def __getitem__(self, index):
        return self._elements[index]

    def __len__(self):
        return len(self._elements)

    def __iter__(self):
        return iter(self._elements)

    def get_one(self, default=None):
        try:
            return self[0]
        except IndexError:
            return default
