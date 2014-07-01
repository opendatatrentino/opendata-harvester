"""
Scan a storage containing raw geocatalogo datasets; extract candidate URLs
for resources.
"""

from __future__ import print_function

import cgi
import sys

import lxml.etree
import requests

from harvester.utils import get_plugin, XPathHelper
from harvester_odt.pat_geocatalogo.converter import (
    API_XML_NSMAP, LINKED_XML_NSMAP)


def _check_url(url):
    try:
        response = requests.get(url, stream=True)
    except:
        return (None, [])
    else:
        status = response.status_code

    extras = []
    content_type, ctextra = cgi.parse_header(response.headers['content-type'])

    if content_type == 'application/zip':
        extras.append(('zip', _check_zip(response)))

    return status, extras


def _fmt_check_result(code):
    if code is None:
        return '\033[1;31mERR\033[0m'
    if 200 <= code < 300:
        return '\033[1;32m{0}\033[0m'.format(code)
    return '\033[1;32m{0}\033[0m'.format(code)


def _fmt_check_extra(extra):
    res = []
    for key, val in extra:
        val = '\033[1;32mSUCC\033[0m' if val else '\033[1;31mFAIL\033[0m'
        res.append('\033[0;36m{0}\033[0m:{1}'.format(key, val))
    return " ".join(res)


def _check_zip(response):
    from io import BytesIO
    import zipfile

    try:
        zf = zipfile.ZipFile(BytesIO(response.content))
    except:
        return False
    filelist = zf.filelist
    if len(filelist) == 0:
        return False  # Empty zip
    if sum(f.file_size for f in filelist) <= 0:
        return False  # Zip containing empty files
    return True


def main(storage):
    for dsid, dsobj in storage.documents['dataset'].iteritems():
        print("\033[1;36mDATASET:\033[0m \033[36m{0}\033[0m".format(dsid))

        dsxml1 = dsobj['raw_xml'].decode('latin-1')
        dsxml2 = storage.blobs['resource_xml'][dsid].decode('latin-1')

        dsxml1 = lxml.etree.fromstring(dsxml1)
        dsxml2 = lxml.etree.fromstring(dsxml2)

        xph1 = XPathHelper(dsxml1, nsmap=API_XML_NSMAP)
        xph2 = XPathHelper(dsxml2, nsmap=LINKED_XML_NSMAP)

        # ------------------------------------------------------------
        # URLs from API XML

        _url_ogd_xml = xph1('geonet:info/ogd_xml/text()').get_one()
        _url_ogd_zip = xph1('geonet:info/ogd_zip/text()').get_one()
        _url_ogd_rdf = xph1('geonet:info/ogd_rdf/text()').get_one()

        urls = [
            ('OGD XML url', _url_ogd_xml),
            ('OGD ZIP url', _url_ogd_zip),
            ('OGD RDF url', _url_ogd_rdf),
            ]

        for label, url in urls:
            if url is None:
                continue
            result, extra = _check_url(url)
            print("    {res} \033[33m{label}:\033[0m {url} {extra}"
                  .format(url=url,
                          label=label,
                          extra=_fmt_check_extra(extra),
                          res=_fmt_check_result(result)))

        # ------------------------------------------------------------
        # Links from "Linked" XML

        links_xpath = ('/gmd:MD_Metadata/gmd:distributionInfo/'
                       'gmd:MD_Distribution/gmd:transferOptions/'
                       'gmd:MD_DigitalTransferOptions/gmd:onLine/'
                       'gmd:CI_OnlineResource/gmd:linkage/gmd:URL/text()')
        links = xph2(links_xpath)
        print("\n    \033[1mLinks from ogd:xml:\033[0m")
        for link in links:
            result, extra = _check_url(link)
            if link in (_url_ogd_zip, _url_ogd_rdf):
                color = '\033[32m'
            else:
                color = '\033[31m'
            print("        {res} {color}{url}\033[0m {extra}"
                  .format(color=color, url=link,
                          extra=_fmt_check_extra(extra),
                          res=_fmt_check_result(result)))

        print("")


if __name__ == '__main__':
    storage = get_plugin('storage', sys.argv[1], [])
    main(storage)
