"""
Celery tasks for harvester director
"""

import sys
import logging

from celery import Celery

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
    pass


@worker.task
def testing_task(jobid):
    """Task used to test functionality"""

    from harvester.director import HarvesterDirector

    hd = HarvesterDirector()
    storage = hd.get_storage('test')

    task_info = storage.documents['jobs'][jobid]
    task_info['status'] = 'done'
    storage.documents['jobs'][jobid] = task_info


# To run, use ``worker.worker_main()``
