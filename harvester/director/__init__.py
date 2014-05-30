"""
Harvester director.

Service to keep track / schedule jobs easily.
"""

import logging
import time
import uuid

from flask import current_app

from harvester.ext.storage.mongodb import MongodbStorage


logger = logging.getLogger(__name__)


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

    def get_storage_url_options(self, name):
        """
        Get url/options for a sub-storage with the given name
        """

        storage_conf = dict(self.conf['HARVESTER_MONGODB'])  # make a copy
        db_url = storage_conf.pop('host')
        db_name = storage_conf.pop('name')
        return '/'.join((db_url, db_name, name)), storage_conf

    def get_storage(self, name):
        """
        Get an instance of MongodbStorage sub-storage with
        the given name.
        """

        url, options = self.get_storage_url_options(name)
        return MongodbStorage(url, options)

    def create_job(self, job, async=True):
        """
        Save information about a job to be executed.

        Job information is stored in MongoDB to keep track of it,
        then execution is spawned via celery.

        :return:
            The id of the newly created job
        """

        # Need to import here to avoid import loop

        self.set_job_conf(job['id'], job)
        return job['id']

        # if async:
        #     # Run in background via celery
        #     return run_job.delay(job['id'])

        # # Otherwise, just run the job now.
        # run_job(job['id'])

    def schedule_job(self, jobid):
        """
        Run a job in asynchronous way via celery.
        Will spawn a task to call ``execute_job()`` on a worker box.

        :return:
            a celery AsyncResult object, that can be used for further
            task control.
        """

        from harvester.director.tasks import run_job
        return run_job.delay(jobid)

    def execute_job(self, jobid):
        """Directly execute a job, by id"""

        logger.info('Starting job: {0}'.format(jobid))

        job_conf = self.get_job_conf(jobid)
        logger.debug('Got job info: type={0}'.format(job_conf['type']))

        # Mark job as started
        job_conf['started'] = True
        self.set_job_conf(jobid, job_conf)

        # Do work here..
        time.sleep(2)

        # Mark job as completed
        job_conf['end_time'] = time.time()
        job_conf['finished'] = True
        job_conf['result'] = True  # success
        self.set_job_conf(jobid, job_conf)  # Save

    def get_job_conf(self, jobid):
        """ Get configuration about a job """

        storage = self.get_storage('control')
        return storage.documents['jobs'][jobid]

    def set_job_conf(self, jobid, job):
        """Update configuration about a job"""

        job_conf = {
            'id': jobid,
            'type': None,
            'start_time': time.time(),
            'end_time': None,
            'started': False,
            'finished': False,
            'result': None,  # True | False
        }
        job_conf.update(job)

        if not job_conf['id']:
            # ..or we can autogenerate one?
            raise ValueError("A job id is required!")

        storage = self.get_storage('control')
        storage.documents['jobs'][jobid] = job_conf

        return job_conf

    def del_job_conf(self, jobid, job):
        """Delete configuration about a job"""

        storage = self.get_storage('control')
        del storage.documents['jobs'][jobid]

    def get_new_job_storage(self):
        """Get configuration"""

        storage_name = 'job.{0}'.format(str(uuid.uuid4()))
        url, opts = self.get_storage_url_options(storage_name)
        return {'url': url, 'options': opts}
