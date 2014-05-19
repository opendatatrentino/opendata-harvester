"""
Harvester director.

Service to keep track / schedule jobs easily.
"""


class HarvesterDirector(object):
    def __init__(self, storage):
        self.storage = storage


class BaseJob(object):
    pass


class CrawlJob(BaseJob):
    def __init__(self, source_url=None, source_options=None,
                 storage_url=None, storage_options=None):
        pass


class ConvertJob(BaseJob):
    pass


class ImportJob(BaseJob):
    pass
