import uuid

from flask import Flask, request, jsonify, send_from_directory, current_app
from flask_restful import Api, Resource, fields, marshal

import os
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.ext.orderinglist import ordering_list
import version
from pathlib import Path
import gevent

db = SQLAlchemy()


class DB:
    class Blob(db.Model):
        __tablename__ = 'blobs'

        blob_id = db.Column(db.String(36), primary_key=True)
        created = db.Column(db.DateTime, server_default=db.func.now())

        marshal_fields = {
            'id': fields.String(attribute='blob_id'),
            'uri': fields.Url('blobs_data_endpoint')
        }

        def marshal(self):
            return marshal(self, self.marshal_fields)

    class Leaf(db.Model):
        __tablename__ = 'leaves'
        id = db.Column(db.Integer, primary_key=True, autoincrement=True)
        path = db.Column(db.String, unique=True, nullable=False, index=True)
        created = db.Column(db.DateTime, server_default=db.func.now())
        updated = db.Column(db.DateTime, server_default=db.func.now(), server_onupdate=db.func.now())
        timeout = db.Column(db.DateTime)
        type = db.Column(db.String)
        contents = db.relationship('Entry',
                                   order_by='Entry.rank', cascade='all, delete-orphan',
                                   collection_class=ordering_list('rank'))

        marshal_fields = {
            'created': fields.DateTime,
            'updated': fields.DateTime,
            'timeout': fields.DateTime,
            'type': fields.String,
            'path': fields.String,
            'contents': fields.List(fields.Nested({
                'id': fields.String(attribute='blob_id'),
                'uri': fields.Url(endpoint='blobs_data_endpoint')
            }))
        }

        def marshal(self):
            return marshal(self, self.marshal_fields)

    class Entry(db.Model):
        __tablename__ = 'entries'

        entry_id = db.Column(db.Integer, primary_key=True, nullable=False, autoincrement=True)
        blob_id = db.Column(db.String(36), db.ForeignKey('blobs.blob_id'), nullable=False)
        leaf_id = db.Column(db.Integer, db.ForeignKey('leaves.id'), nullable=False)

        rank = db.Column(db.Integer, nullable=False)


def get_children(session, path):
    return session.query(DB.Leaf).filter(DB.Leaf.path.like(path + "/%")).order_by(DB.Leaf.path).all()


class Info(Resource):

    def get(self):
        return {
            'server': "Gadgetron Storage Manager",
            'version': version.version
        }


class BlobData(Resource):

    @classmethod
    def get(cls, blob_id):
        folder = current_app.config['DATA_FOLDER']
        return send_from_directory(folder, f"{blob_id}.bin", conditional=True)


class BlobList(Resource):

    @classmethod
    def put(cls):
        current_app.logger.info("Storing data")
        blob_id = uuid.uuid4()
        folder = current_app.config['DATA_FOLDER']

        with open(os.path.join(folder, f"{blob_id}.bin"), 'wb') as f:
            is_good = True
            while is_good:
                data =  request.stream.read(1024**2)
                f.write(data)
                is_good = len(data) > 0

        blob = DB.Blob(blob_id=str(blob_id))

        db.session.add(blob)
        db.session.commit()

        return jsonify(blob.marshal())


class Node(Resource):

    @classmethod
    def get(cls, path):
        current_app.logger.info("Getting "+path)
        path = cls.name + '/' + path

        leaf = db.session.query(DB.Leaf).filter(DB.Leaf.path == path).one_or_none()

        if leaf:
            leaf.updated = db.func.now()
            db.session.commit()
            return jsonify(leaf.marshal())
        else:
            children = get_children(db.session, path)
            return jsonify([child.path for child in children])

    @classmethod
    def patch(cls, path):
        current_app.logger.info("Patching "+path)
        path = cls.name + '/' + path
        leaf = cls._get_or_create(db.session, path)

        def push(blobs):
            for blob in reversed(blobs):
                leaf.contents.insert(0, DB.Entry(blob_id=blob))

        operations = {
            'push': push
        }

        operation = operations.get(request.json.get('operation'))
        arguments = request.json.get('arguments', [])

        operation(arguments)

        db.session.commit()

        return jsonify(leaf.marshal())

    @classmethod
    def _get_or_create(cls, session, path):

        leaf = session.query(DB.Leaf).filter(DB.Leaf.path == path).first()
        if not leaf:
            leaf = DB.Leaf(path=path)
            session.add(leaf)
            session.flush()
        else:
            leaf.updated = db.func.now()
        return leaf

    @classmethod
    def register(cls, api):
        api.add_resource(cls, f"/{cls.name}/<path:path>", endpoint=cls.endpoint)


class Sessions(Node):
    name = 'sessions'
    endpoint = 'sessions_node_endpoint'

    timeout = 3600


class Noise(Node):
    name = 'noise'
    endpoint = 'noise_node_endpoint'

    timeout = None


class Debug(Node):
    name = 'debug'
    endpoint = 'debug_node_endpoint'

    timeout = None


def garbage_collect(app):
    with app.app_context():
        db.session.query(DB.Leaf).filter((DB.Leaf.updated + DB.Leaf.timeout) > 'now()').delete()
        db.session.flush()

        leaves = db.session.query(DB.Leaf.id)

        db.session.query(DB.Entry).filter(~DB.Entry.leaf_id.in_(leaves)).delete(synchronize_session='fetch')
        db.session.flush()

        entries = db.session.query(DB.Entry.blob_id)
        blobs_to_be_deleted = db.session.query(DB.Blob).filter(~DB.Blob.blob_id.in_(entries))
        folder = current_app.config['DATA_FOLDER']

        blobs_to_be_deleted.delete(synchronize_session='fetch')
        db.session.commit()

        def delete_blob(blob_id):
            os.unlink(os.path.join(folder, f"{blob_id}.bin"))

        for blob in blobs_to_be_deleted.all():
            delete_blob(blob.blob_id)

        gevent.spawn_later(current_app.config['GC_INTERVAL'], lambda: garbage_collect(app))


def create_app(database_file=None, data_folder=None,gc_interval = 600):
    app = Flask(__name__)
    api = Api(app, prefix='/v1')

    api.add_resource(Info, '/info')

    api.add_resource(BlobList, '/blobs')
    api.add_resource(BlobData, '/blobs/<blob_id>', endpoint='blobs_data_endpoint')

    Sessions.register(api)
    Noise.register(api)
    Debug.register(api)

    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['GC_INTERVAL'] = gc_interval

    if database_file is None:
        database_file = os.path.join(app.instance_path, 'gadgetron_storage.sqlite')
    if data_folder is None:
        data_folder = os.path.join(app.instance_path, 'blob')

    database_path = Path(database_file)
    database_path.parent.mkdir(parents=True,exist_ok=True)
    if ~database_path.exists():
        database_path.touch()

    app.config.from_mapping(SQLALCHEMY_DATABASE_URI='sqlite:///' + database_path.as_posix(),
                            DATA_FOLDER=data_folder)

    db.init_app(app)
    db.create_all(app=app)

    return app
