"""
Harvester director.

Service to keep track / schedule jobs easily.
"""

from flask import current_app

from harvester.ext.storage.mongodb import MongodbStorage


class HarvesterDirector(object):
    """
    Configuration is read from the Flask application.
    """

    def __init__(self, conf=None):
        self._conf = conf

    @property
    def conf(self):
        if self._conf is None:
            try:
                self._conf = current_app.config
            except RuntimeError:
                from harvester.director.web import app
                with app.app_context():
                    self._conf = app.config
        return self._conf

    def get_storage_url_options(self, coll_name):
        storage_conf = dict(self.conf['HARVESTER_MONGODB'])  # make a copy
        db_url = storage_conf.pop('host')
        db_name = storage_conf.pop('name')
        return '/'.join((db_url, db_name, coll_name)), storage_conf

    def get_storage(self, coll_name):
        """
        Get an instance of MongodbStorage, based on the configured
        database plus the ``coll_name`` collection prefix.
        """
        url, options = self.get_storage_url_options(coll_name)
        return MongodbStorage(url, options)

    def run_job(self, job):
        """
        Schedule a job for running.

        Job information is stored in MongoDB to keep track of it,
        then execution is spawned via celery.

        :return:
            a celery AsyncResult object, that can be used for further
            task control.
        """

        # Need to import here to avoid import loop
        from harvester.director.tasks import run_job

        self.set_job_conf(job['id'], job)
        return run_job.delay(job['id'])  # Run in background via celery

    def get_job_conf(self, jobid):
        """ Get configuration about a job """

        storage = self.get_storage('control')
        return storage.documents['jobs'][jobid]

    def set_job_conf(self, jobid, job):
        """ Update configuration about a job """

        storage = self.get_storage('control')
        storage.documents['jobs'][jobid] = job

    def del_job_conf(self, jobid, job):
        """ Delete configuration about a job """

        storage = self.get_storage('control')
        del storage.documents['jobs'][jobid]
