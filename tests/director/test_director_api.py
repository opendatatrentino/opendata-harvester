import json
import time  # We need time to sleep ;)


def test_director_api_conf(director_client):
    rv = director_client.get('/api/1/conf/crawler/')
    assert rv.status_code == 200
    assert json.loads(rv.data) == []

    rv = director_client.post(
        '/api/1/conf/crawler/foo/',
        data=json.dumps({'label': 'FooSource', 'url': 'http://foo.com'}),
        headers={"Content-type": 'application/json'})
    assert rv.status_code == 201
    rvobj = json.loads(rv.data)
    assert rvobj['label'] == 'FooSource'
    assert rvobj['url'] == 'http://foo.com'

    rv = director_client.get('/api/1/conf/crawler/foo/')
    assert rv.status_code == 200
    rvobj = json.loads(rv.data)
    assert rvobj['label'] == 'FooSource'
    assert rvobj['url'] == 'http://foo.com'

    rv = director_client.get('/api/1/conf/crawler/')
    assert rv.status_code == 200
    assert len(json.loads(rv.data)) == 1

    rv = director_client.delete('/api/1/conf/crawler/foo/')
    assert rv.status_code == 200

    rv = director_client.get('/api/1/conf/crawler/foo/')
    assert rv.status_code == 404

    rv = director_client.get('/api/1/conf/crawler/')
    assert rv.status_code == 200
    assert json.loads(rv.data) == []


def test_simple_task_run(director_client, director_worker):
    # We want to create a dummy job and execute it via celery,
    # mostly to check that everything is working fine.

    from harvester.director import HarvesterDirector
    from harvester.director.tasks import testing_task

    hd = HarvesterDirector()
    storage = hd.get_storage('test')

    # First, we want to create a dummy "task" in the test storage
    storage.documents['jobs']['job-00'] = {
        'label': 'First job',
        'status': 'scheduled',
    }

    # Then, we run the task via celery
    testing_task.delay('job-00')
    time.sleep(2)

    # Check..
    assert storage.documents['jobs']['job-00'] == {
        'label': 'First job',
        'status': 'done',
    }
