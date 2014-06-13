import json

import pytest

from harvester.director import HarvesterDirector
from harvester.utils import check_tcp_port

need_redis = pytest.mark.skipif(
    not check_tcp_port('127.0.0.1', 6379),
    reason='Redis unavailable')


@pytest.mark.parametrize('conf_type', ['crawler', 'converter', 'importer'])
def test_director_api_conf(director_client, conf_type):
    rv = director_client.get('/api/1/conf/{0}/'.format(conf_type))
    assert rv.status_code == 200
    assert json.loads(rv.data) == []

    rv = director_client.post(
        '/api/1/conf/{0}/foo/'.format(conf_type),
        data=json.dumps({'label': 'Foo Label', 'url': 'http://foo.com'}),
        headers={"Content-type": 'application/json'})
    assert rv.status_code == 201
    rvobj = json.loads(rv.data)
    assert rvobj['label'] == 'Foo Label'
    assert rvobj['url'] == 'http://foo.com'

    rv = director_client.get('/api/1/conf/{0}/foo/'.format(conf_type))
    assert rv.status_code == 200
    rvobj = json.loads(rv.data)
    assert rvobj['label'] == 'Foo Label'
    assert rvobj['url'] == 'http://foo.com'

    rv = director_client.get('/api/1/conf/{0}/'.format(conf_type))
    assert rv.status_code == 200
    assert len(json.loads(rv.data)) == 1

    rv = director_client.delete('/api/1/conf/{0}/foo/'.format(conf_type))
    assert rv.status_code == 200

    rv = director_client.get('/api/1/conf/{0}/foo/'.format(conf_type))
    assert rv.status_code == 404

    rv = director_client.get('/api/1/conf/{0}/'.format(conf_type))
    assert rv.status_code == 200
    assert json.loads(rv.data) == []


@need_redis
def test_simple_task_run(director_client, director_worker):
    # We want to create a dummy job and execute it via celery,
    # mostly to check that everything is working fine.

    from harvester.director.tasks import testing_task

    hd = HarvesterDirector()
    storage = hd.get_storage('test')

    # First, we want to create a dummy "task" in the test storage
    storage.documents['jobs']['job-00'] = {
        'label': 'First job',
        'status': 'scheduled',
    }

    # Then, we run the task via celery
    result = testing_task.delay('job-00')
    result.wait()

    # Check..
    assert storage.documents['jobs']['job-00'] == {
        'label': 'First job',
        'status': 'done',
    }


@need_redis
def test_dummy_task_run(director_client, director_worker):
    hd = HarvesterDirector()

    # First run: crawl a dummy source
    # ------------------------------------------------------------

    storage1 = hd.get_new_job_storage()
    job_id = hd.create_job({
        'id': 'job-crawl-01',
        'type': 'crawl',
        'crawler': {
            'url': 'dummy+http://example.com',
            'options': {},
        },
        'output_storage': storage1,
    })
    task = hd.schedule_job(job_id)

    job_conf = hd.get_job_conf('job-crawl-01')
    assert job_conf['started'] is False
    assert job_conf['finished'] is False
    assert job_conf['result'] is None

    task.wait()

    job_conf = hd.get_job_conf('job-crawl-01')
    assert job_conf['started'] is True
    assert job_conf['finished'] is True
    assert job_conf['result'] is True

    # Convert from the dummy source to another format
    # ------------------------------------------------------------

    storage2 = hd.get_new_job_storage()
    job_id = hd.create_job({
        'id': 'job-convert-01',
        'type': 'convert',
        'converter': {
            'url': 'dummy1_to_dummy2',
            'options': {},
        },
        'input_storage': storage1,
        'output_storage': storage2,
    })
    task = hd.schedule_job(job_id)

    job_conf = hd.get_job_conf('job-convert-01')
    assert job_conf['started'] is False
    assert job_conf['finished'] is False
    assert job_conf['result'] is None

    task.wait()

    job_conf = hd.get_job_conf('job-convert-01')
    assert job_conf['started'] is True
    assert job_conf['finished'] is True
    assert job_conf['result'] is True

    # Preview importing to a dummy destination
    # ------------------------------------------------------------

    storage3 = hd.get_new_job_storage()
    job_id = hd.create_job({
        'id': 'job-preview-01',
        'type': 'preview',
        'previewer': {
            'url': 'dummy2+http://example2.com',
            'options': {},
        },
        'input_storage': storage2,
        'output_storage': storage3,
    })
    task = hd.schedule_job(job_id)

    job_conf = hd.get_job_conf('job-preview-01')
    assert job_conf['started'] is False
    assert job_conf['finished'] is False
    assert job_conf['result'] is None

    task.wait()

    job_conf = hd.get_job_conf('job-preview-01')
    assert job_conf['started'] is True
    assert job_conf['finished'] is True
    assert job_conf['result'] is True

    # Actually import to dummy destination (use a storage?)
    # ------------------------------------------------------------

    job_id = hd.create_job({
        'id': 'job-import-01',
        'type': 'import',
        'importer': {
            'url': 'dummy2+http://example2.com',
            'options': {},
        },
        'input_storage': storage2,
    })
    task = hd.schedule_job(job_id)

    job_conf = hd.get_job_conf('job-import-01')
    assert job_conf['started'] is False
    assert job_conf['finished'] is False
    assert job_conf['result'] is None

    task.wait()

    job_conf = hd.get_job_conf('job-import-01')
    assert job_conf['started'] is True
    assert job_conf['finished'] is True
    assert job_conf['result'] is True
