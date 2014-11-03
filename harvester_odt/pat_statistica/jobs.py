"""
Functions to be used as JobControl jobs
"""

from datetime import datetime
import logging
import os

from harvester.utils import get_storage_direct
from harvester_odt.pat_statistica.client import (
    StatisticaClient, StatisticaSubproClient)
from harvester_odt.pat_statistica.conv_statistica \
    import dataset_statistica_to_ckan
from harvester_odt.pat_statistica.conv_statistica_subpro \
    import dataset_statistica_subpro_to_ckan
from harvester_odt.pat_statistica.constants import ORGANIZATIONS, CATEGORIES

from jobcontrol.globals import current_app, execution_context


logger = logging.getLogger('harvester_odt.pat_statistica')


def _update_progress(*a):
    current_app.storage.update_build_progress(
        execution_context.build_id, *a)


def _get_uniqid():
    return "{0:%y%m%d-%H%M%S}-{1}".format(datetime.now(), os.getpid())


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

    storage = get_storage(storage_url, storage_options)
    client = StatisticaClient()

    # Get the total number of datasets
    total = len(client.list_datasets())
    logger.debug('Found {0} datasets'.format(total))
    _update_progress(0, total)

    datasets = client.iter_datasets()

    for i, dataset in enumerate(datasets):
        logger.info('Got dataset #{0}: {1}'.format(i, dataset['id']))
        storage.documents['dataset'][dataset['id']] = dataset
        _update_progress(i + 1, total)

    return storage.url


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

    storage = get_storage(storage_url, storage_options)
    client = StatisticaSubproClient()

    total = len(client.list_datasets())
    logger.debug('Found {0} datasets'.format(total))
    _update_progress(0, total)

    datasets = client.iter_datasets()

    for i, dataset in enumerate(datasets):
        logger.info('Got dataset #{0}: {1}'.format(i, dataset['id']))
        storage.documents['dataset'][dataset['id']] = dataset
        _update_progress(i + 1, total)

    return storage.url


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

    input_storage_url = _get_input_storage_url()
    input_storage = get_storage_direct(input_storage_url)

    storage = get_storage(storage_url, storage_options)

    logger.debug('Converting data statistica -> ckan')
    logger.debug('Input storage: {0}'.format(input_storage.url))
    logger.debug('Output storage: {0}'.format(storage.url))

    # ------------------------------------------------------------
    # First, calculate the number of objects we are going
    # to create, to report progress.

    progress_total = len(input_storage.documents['dataset'])
    progress_total += len(CATEGORIES)
    progress_total += len(ORGANIZATIONS)

    progress_current = 0

    _update_progress(0, progress_total)

    for dataset_id in input_storage.documents['dataset']:
        logger.debug('Importing dataset {0}'.format(dataset_id))
        dataset = input_storage.documents['dataset'][dataset_id]
        clean_dataset = dataset_statistica_to_ckan(dataset)
        _dsid = clean_dataset['id']
        storage.documents['dataset'][_dsid] = clean_dataset

        progress_current += 1
        _update_progress(progress_current, progress_total)

    logger.debug('Importing groups')
    for group in CATEGORIES.itervalues():
        storage.documents['group'][group['name']] = group

        progress_current += 1
        _update_progress(progress_current, progress_total)

    logger.debug('Importing organizations')
    for org in ORGANIZATIONS.itervalues():
        storage.documents['organization'][org['name']] = org

        progress_current += 1
        _update_progress(progress_current, progress_total)


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
