import pytest
import tempfile
import storage
import os
from pathlib import Path
import numpy as np
import io
import pickle


@pytest.fixture
def client():
    db_fd, filename = tempfile.mkstemp()

    tmpdir = tempfile.TemporaryDirectory()

    app = storage.create_app(database_file=filename, data_folder=tmpdir.name)
    storage.db.create_all(app=app)
    with app.test_client() as client:
        with app.app_context():
            yield client

    os.close(db_fd)
    os.unlink(filename)


def test_info(client):
    rv = client.get('/v1/info')
    print(rv)
    json = rv.get_json()
    assert json['version'] == "1.0.0"


def test_missing(client):
    rv = client.get('/v1/sessions/187/noisedata')

    data = rv.get_json()

    assert data == []


def test_push(client):
    buffer = io.BytesIO()
    pickler = pickle.Pickler(buffer)

    testdata = np.random.randn(10)
    pickler.dump(testdata)

    rv = client.put('/v1/blobs', data=buffer.getvalue())

    json = rv.get_json()
    assert json is not None

    response_dict = {'operation': 'push', 'arguments': [json['id']]}

    rv = client.patch('/v1/sessions/42/noiseninja', json=response_dict)

    metadata_json = rv.get_json()
    assert metadata_json['contents'][0]['id'] == json['id']

    rv = client.get('/v1/sessions/42/noiseninja')
    json = rv.get_json()
    assert len(json['contents']) == 1
    uri = json['contents'][0]['uri']
    assert uri is not None

    rv = client.get(uri)

    recovered = pickle.loads(rv.data)

    assert (testdata == recovered).all()

    rv = client.get('/v1/sessions/42')
    group_json = rv.get_json()
    assert len(group_json) == 1
    assert group_json[0] == 'sessions/42/noiseninja'

    rv = client.get('/v1/sessions/4')
    group_json = rv.get_json()
    assert len(group_json) == 0
