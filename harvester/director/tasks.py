"""
Celery tasks for harvester director
"""

import logging
import time

from celery import Celery

from harvester.director import HarvesterDirector
from harvester.director.web import app

logger = logging.getLogger(__name__)


def make_celery(app):
    celery = Celery(app.import_name, broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery

worker = make_celery(app)


@worker.task
def run_job(jobid):
    # We need to use the application settings to connect to mongodb,
    # retrieve job information + mark it as running (we need some nice
    # way to lock?) then feel free to update job information.

    # Also, we'd need to store logs in a collection -> use some custom
    # handler that can write on storages?

    logger.info('Starting job: {0}'.format(jobid))

    hd = HarvesterDirector()
    job_conf = hd.get_job_conf(jobid)
    logger.debug('Got job info: type={0}'.format(job_conf['type']))

    # Mark job as started
    job_conf['started'] = True
    hd.set_job_conf(jobid, job_conf)

    # Do work here..
    time.sleep(2)

    # Mark job as completed
    job_conf['end_time'] = time.time()
    job_conf['finished'] = True
    job_conf['result'] = True  # success
    hd.set_job_conf(jobid, job_conf)  # Save


@worker.task
def testing_task(jobid):
    """Task used to test functionality"""

    hd = HarvesterDirector()
    storage = hd.get_storage('test')

    task_info = storage.documents['jobs'][jobid]
    task_info['status'] = 'done'
    storage.documents['jobs'][jobid] = task_info


# To run, use ``worker.worker_main()``
