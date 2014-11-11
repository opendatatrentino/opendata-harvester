import logging

# from ckan_api_client.syncing import SynchronizationClient

from harvester.ext.importer.base import ImporterPluginBase
from harvester.utils import get_storage_from_arg
from harvester.ext.importer.ckan_sync_client import SynchronizationClient


logger = logging.getLogger(__name__)


class CKANImporter(ImporterPluginBase):
    logger = logging.getLogger(__name__)

    options = [
        ('api_key', 'str', None, "CKAN API key"),
        ('source_name', 'str', None, "Name for the import source"),
        ('organization_merge_strategy', 'str', None, "One of: create, update"),
        ('group_merge_strategy', 'str', None, "One of: create, update"),
        ('dataset_preserve_names', 'bool', True,
         "if ``True`` (the default) will preserve old names of "
         "existing datasets"),
        ('dataset_preserve_organization', 'bool', True,
         "Preserve current dataset organization?"),
        ('dataset_group_merge_strategy', 'str', 'add',
         "One of: add, replace, preserve")
    ]

    def sync_data(self, storage):
        copy_conf = [
            'api_key',
            'source_name',
            'organization_merge_strategy',
            'group_merge_strategy',
            'dataset_preserve_names',
            'dataset_preserve_organization',
            'dataset_group_merge_strategy',
        ]
        kwargs = {}
        for k in copy_conf:
            kwargs[k] = self.conf[k]

        return import_to_ckan(storage=storage, ckan_url=self.url, **kwargs)


def import_to_ckan(storage, ckan_url, api_key, source_name,
                   organization_merge_strategy='create',
                   group_merge_strategy='create',
                   dataset_preserve_names=True,
                   dataset_preserve_organization=True,
                   dataset_group_merge_strategy='add'):
    """
    Import data from a storage to CKAN.

    :param storage:
        Input storage. Must contain data in CKAN format.
        Can be either a Storage object or a dict
        like ``{'url': '...', 'conf': {}}``.

    :param ckan_url: URL to the CKAN instance

    :param api_key: CKAN API key

    :param source_name: Name for the import source

    :param organization_merge_strategy:
        One of: create, update.

    :param group_merge_strategy:
        One of: create, update.

    :param dataset_preserve_names:
         If ``True`` (the default) will preserve old names of
         existing datasets. Otherwise, it will regenerate them
         (not recommended!)

    :param dataset_preserve_organization:
         Preserve current dataset organization?

    :param dataset_group_merge_strategy:
         One of: add, replace, preserve (default: 'add')

    """

    logger.info("Starting data synchronization to ckan")

    storage = get_storage_from_arg(storage)

    # ------------------------------------------------------------
    # Initialize the client taking some kwargs from configuration
    # todo: we need some way to make the sync client report status!

    client = SynchronizationClient(
        ckan_url, api_key,
        organization_merge_strategy=organization_merge_strategy,
        group_merge_strategy=group_merge_strategy,
        dataset_preserve_names=dataset_preserve_names,
        dataset_preserve_organization=dataset_preserve_organization,
        dataset_group_merge_strategy=dataset_group_merge_strategy)

    # ------------------------------------------------------------
    # New-style storage is dict-like, no need to pre-query.

    data = {
        'dataset': storage.documents['dataset'],
        'group': storage.documents['group'],
        'organization': storage.documents['organization'],
    }

    client.sync(source_name, data)


def _get_input_storage():
    """
    Get the input storage from job dependencies.
    """

    # Get a list of dependencies of this job
    # We expect to get exactly *one*

    from jobcontrol.globals import current_app, execution_context

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


def import_to_ckan_job(**kw):
    from harvester.utils import ProgressReport
    import eventlite
    from jobcontrol.globals import current_app, execution_context

    def _update_progress(*a):
        current_app.storage.update_build_progress(
            execution_context.build_id, *a)

    def handle_events(*a, **kw):
        if len(a) and isinstance(a[0], ProgressReport):
            _update_progress(a[0].current, a[0].total)

    input_storage = _get_input_storage()
    with eventlite.handler(handle_events):
        import_to_ckan(storage=input_storage, **kw)


import_to_ckan_job.__doc__ = import_to_ckan.__doc__ + """

    **Note:** in the "job" version, the storage will be taken from the result
    of dependency job(s), *not* from the argument!
"""
