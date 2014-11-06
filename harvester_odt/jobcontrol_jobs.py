"""
Functions to be used as JobControl jobs
"""

from datetime import datetime
import logging
import os

import eventlite

from harvester.utils import get_storage_direct, ProgressReport

# from harvester_odt.pat_statistica.conv_statistica_subpro \
#     import dataset_statistica_subpro_to_ckan
from harvester_odt.pat_statistica.constants import ORGANIZATIONS, CATEGORIES

from jobcontrol.globals import current_app, execution_context


logger = logging.getLogger('harvester_odt.pat_statistica')


def _update_progress(*a):
    current_app.storage.update_build_progress(
        execution_context.build_id, *a)


def _get_uniqid():
    return "{0:%y%m%d-%H%M%S}-{1}".format(datetime.now(), os.getpid())


def handle_events(*a, **kw):
    if len(a) and isinstance(a[0], ProgressReport):
        _update_progress(a[0].current, a[0].total)


def get_storage(url, options=None):
    url = url.format(id=_get_uniqid())
    logger.debug('Destination storage: {0}'.format(url))
    return get_storage_direct(url, options)


def crawl_statistica(storage_url, storage_options=None):
    """
    Run crawler for statistica.

    :param storage_url:
        URL of the storage to use. The following replacements
        will be applied:

        - ``{id}`` - A unique id, based on date/time and process PID.

    :param storage_options:
        Options to be passed to storage constructor.
    """

    import harvester_odt.pat_statistica.crawler
    storage = get_storage(storage_url, storage_options)
    with eventlite.handler(handle_events):
        harvester_odt.pat_statistica.crawler.crawl_statistica(storage)
    return storage


def crawl_statistica_subpro(storage_url, storage_options=None):
    """
    Run crawler for statistica - subprovinciale.

    :param storage_url:
        URL of the storage to use. The following replacements
        will be applied:

        - ``{id}`` - A unique id, based on date/time and process PID.

    :param storage_options:
        Options to be passed to storage constructor.
    """

    import harvester_odt.pat_statistica.crawler
    storage = get_storage(storage_url, storage_options)
    with eventlite.handler(handle_events):
        harvester_odt.pat_statistica.crawler.crawl_statistica_subpro(storage)
    return storage


def crawl_geocatalogo(storage_url, storage_options=None):
    """
    Run crawler for GeoCatalogo

    :param storage_url:
        URL of the storage to use. The following replacements
        will be applied:

        - ``{id}`` - A unique id, based on date/time and process PID.

    :param storage_options:
        Options to be passed to storage constructor.
    """

    from harvester_odt.pat_geocatalogo.crawler import Geocatalogo
    storage = get_storage(storage_url, storage_options)
    crawler = Geocatalogo('', {'with_resources': False})
    with eventlite.handler(handle_events):
        crawler.fetch_data(storage)
    return storage


def _get_input_storage_url():
    # Get a list of dependencies of this job
    # We expect to get exactly *one*

    deps = current_app.storage.get_job_deps(execution_context.job_id)

    if len(deps) != 1:
        raise RuntimeError("This job requires exactly *one* dependency, "
                           "which returned the URL to storage containing "
                           "the crawled data.")

    logger.debug('Job {0} dependencies: {1!r}'
                 .format(execution_context.job_id,
                         [x['id'] for x in deps]))

    # Get the output storage URL from the latest successful
    # build. Again, we need at least one successful build.

    dep_build = current_app.storage.get_latest_successful_build(deps[0]['id'])

    if dep_build is None:
        raise RuntimeError("Expected at least a successful build")

    return dep_build['retval']


def convert_statistica_to_ckan(storage_url, storage_options=None):
    """
    Convert data from pat_statistica to Ckan

    :param storage_url:
        URL of the storage to use. The following replacements
        will be applied:

        - ``{id}`` - A unique id, based on date/time and process PID.

    :param storage_options:
        Options to be passed to storage constructor.
    """

    from harvester_odt.pat_statistica.converter \
        import convert_statistica_to_ckan
    input_storage = _get_input_storage_url()
    storage = get_storage(storage_url, storage_options)
    with eventlite.handler(handle_events):
        convert_statistica_to_ckan(input_storage, storage)
    return storage


def convert_statistica_subpro_to_ckan(storage_url, storage_options=None):
    """
    Convert data from pat_statistica_subpro to Ckan

    :param storage_url:
        URL of the storage to use. The following replacements
        will be applied:

        - ``{id}`` - A unique id, based on date/time and process PID.

    :param storage_options:
        Options to be passed to storage constructor.
    """

    from harvester_odt.pat_statistica.converter \
        import convert_statistica_subpro_to_ckan
    input_storage = _get_input_storage_url()
    storage = get_storage(storage_url, storage_options)
    with eventlite.handler(handle_events):
        convert_statistica_subpro_to_ckan(input_storage, storage)
    return storage


def debugging_job(storage_url, storage_options=None):
    """
    Job to be used for debugging purposes.
    """

    storage = get_storage(storage_url, storage_options)

    with eventlite.handler(handle_events):
        eventlite.emit(ProgressReport(0, 1))

    job = current_app.get_job(execution_context.job_id)
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

    with eventlite.handler(handle_events):
        eventlite.emit(ProgressReport(1, 1))

    return storage
