"""
Functions to be used as JobControl jobs
"""

from datetime import datetime
import logging
import os

from jobcontrol.globals import execution_context

from harvester.utils import (get_storage_direct,
                             jobcontrol_integration, report_progress)

logger = logging.getLogger('harvester_odt.pat_statistica')


def _get_uniqid():
    return "{0:%y%m%d-%H%M%S}-{1}".format(datetime.now(), os.getpid())


def _prepare_storage_url(url):
    return url.format(id=_get_uniqid())


def get_storage_from_arg(arg):
    """
    Get a storage instance from an argument to a function.

    This is needed for functions that may be called via
    an external tool that doesn't allow passing object instances
    directly.
    """

    from harvester.ext.storage.base import BaseStorage

    if isinstance(arg, BaseStorage):
        return arg

    if isinstance(arg, basestring):
        return get_storage_direct(
            _prepare_storage_url(arg), options={})

    return get_storage_direct(
        _prepare_storage_url(arg['url']),
        options=arg.get('conf', None))


def crawl_statistica(storage):
    """Run crawler for statistica"""

    import harvester_odt.pat_statistica.crawler
    storage = get_storage_from_arg(storage)

    with jobcontrol_integration():
        harvester_odt.pat_statistica.crawler.crawl_statistica(storage)

    return storage


def crawl_statistica_subpro(storage):
    """Run crawler for statistica - subprovinciale"""

    import harvester_odt.pat_statistica.crawler
    storage = get_storage_from_arg(storage)
    with jobcontrol_integration():
        harvester_odt.pat_statistica.crawler.crawl_statistica_subpro(storage)
    return storage


def crawl_geocatalogo(storage):
    """Run crawler for GeoCatalogo"""

    from harvester_odt.pat_geocatalogo.crawler import Geocatalogo
    crawler = Geocatalogo('', {'with_resources': False})
    storage = get_storage_from_arg(storage)
    with jobcontrol_integration():
        crawler.fetch_data(storage)
    return storage


def crawl_comunweb(storage, url):
    """Run crawler for comunweb

    :param storage: Output storage
    :param url: base URL of the ComunWeb website
    """

    from harvester_odt.comunweb.crawler import ComunWebCrawler
    crawler = ComunWebCrawler(url)
    storage = get_storage_from_arg(storage)
    with jobcontrol_integration():
        crawler.fetch_data(storage)
    return storage


def convert_statistica_to_ckan(input_storage, storage):
    """Convert data from pat_statistica to Ckan"""

    from harvester_odt.pat_statistica.converter \
        import convert_statistica_to_ckan
    input_storage = get_storage_from_arg(input_storage)
    storage = get_storage_from_arg(storage)

    with jobcontrol_integration():
        convert_statistica_to_ckan(input_storage, storage)
    return storage


def convert_statistica_subpro_to_ckan(input_storage, storage):
    """Convert data from pat_statistica_subpro to Ckan"""

    from harvester_odt.pat_statistica.converter \
        import convert_statistica_subpro_to_ckan
    input_storage = get_storage_from_arg(input_storage)
    storage = get_storage_from_arg(storage)

    with jobcontrol_integration():
        convert_statistica_subpro_to_ckan(input_storage, storage)
    return storage


def convert_geocatalogo_to_ckan(input_storage, storage):
    """Convert data from pat_geocatalogo to Ckan"""

    from harvester_odt.pat_geocatalogo.converter \
        import GeoCatalogoToCkan
    input_storage = get_storage_from_arg(input_storage)
    storage = get_storage_from_arg(storage)
    converter = GeoCatalogoToCkan('', {})

    with jobcontrol_integration():
        converter.convert(input_storage, storage)
    return storage


def debugging_job(storage):
    """
    Job to be used for debugging purposes.
    """

    storage = get_storage_from_arg(storage)

    with jobcontrol_integration():
        report_progress(None, 0, 1)

    job = execution_context.current_job
    logger.debug('Running job: {0!r}'.format(job))

    deps = list(job.get_deps())
    logger.debug('Found {0} dependencies'.format(len(deps)))

    for dep in deps:
        build = dep.get_latest_successful_build()
        if build is None:
            logger.debug('Dependency {0!r} has no builds'
                         .format(dep))
        else:
            logger.debug('Dependency {0!r} latest build returned {1!r}'
                         .format(dep, build['retval']))

    with jobcontrol_integration():
        report_progress(None, 1, 1)

    return storage
