
import uuid
import itertools

from flask import Flask, request, jsonify, send_from_directory
from flask_restful import Api, Resource, fields, marshal

from sqlalchemy import create_engine
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, UniqueConstraint

from sqlalchemy.sql import func
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

import version

engine = create_engine("sqlite:///gadgetron-storage.sqlite")

Base = declarative_base()

Session = sessionmaker()
Session.configure(bind=engine)


class DB:

    class Blob(Base):
        __tablename__ = 'blobs'

        blob = Column(String(36), name='id', primary_key=True)

        marshal_fields = {
            'id': fields.String(attribute='blob'),
            'uri': fields.Url('blobs_data_endpoint')
        }

        def __init__(self, blob):
            self.blob = str(blob)

        def marshal(self):
            return marshal(self, self.marshal_fields)

    class Entry(Base):
        __tablename__ = 'entries'
        __table_args__ = UniqueConstraint('node', 'rank'),

        id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
        blob = Column(String(36), ForeignKey('blobs.id'), nullable=False)
        node = Column(Integer, ForeignKey('nodes.id'), nullable=False)
        rank = Column(Integer, nullable=False)

    class Node(Base):
        __tablename__ = 'nodes'
        __table_args__ = UniqueConstraint('parent', 'string'),

        id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
        string = Column(String, nullable=False)
        parent = Column(Integer, ForeignKey('nodes.id'))

        created = Column(DateTime, server_default=func.now())
        updated = Column(DateTime, server_default=func.now(), server_onupdate=func.now())
        timeout = Column(DateTime)

        children = relationship('Node')
        contents = relationship('Entry', backref='nodes')

        marshal_fields = {
            'created': fields.DateTime,
            'updated': fields.DateTime,
            'timeout': fields.DateTime,
            'children': fields.List(fields.Nested({
                'string': fields.String
            })),
            'contents': fields.List(fields.Nested({
                'id': fields.String(attribute='blob'),
                'uri': fields.Url(attribute='blob', endpoint='blobs_data_endpoint')
            }))
        }

        def marshal(self):
            return marshal(self, self.marshal_fields)


class Info(Resource):

    def get(self):
        return {
            'server': "Gadgetron Storage Manager",
            'version': version.version
        }


class BlobData(Resource):

    @classmethod
    def get(cls, blob):
        return send_from_directory('blobs', f"{blob}.bin", conditional=True)


class BlobList(Resource):

    @classmethod
    def put(cls):

        blob_id = uuid.uuid4()

        with open(f"blobs/{blob_id}.bin", 'wb') as f:
            while not request.stream.is_exhausted:
                f.write(request.stream.read(1024 ** 2))

        blob = DB.Blob(blob_id)

        session = Session()
        session.add(blob)
        session.commit()

        return jsonify(blob.marshal())


class Node(Resource):

    def get(self, path):

        session = Session()
        node = self._node_from_path(session, path)
        session.commit()

        return jsonify(node.marshal())

    def patch(self, path):

        session = Session()
        node = self._node_from_path(session, path)

        def append(blobs):
            node.contents.extend([DB.Entry(node=node.id, blob=blob) for blob in blobs])

        def push(blobs):
            entries = [DB.Entry(node=node.id, blob=blob) for blob in blobs]
            entries.extend(node.contents)
            node.contents = entries

        def pop(blobs):
            node.contents = node.contents[1:]

        operations = {
            'append': append,
            'push': push,
            'pop': pop
        }

        operation = operations.get(request.json.get('operation'))
        arguments = request.json.get('arguments', [])

        operation(arguments)

        for rank, entry in enumerate(node.contents):
            entry.rank = rank

        session.commit()

        return jsonify(node.marshal())

    def _node_from_path(self, session, path):

        root = session.query(DB.Node).filter_by(string=self.name, parent=None).one()

        def node_from_path_recursive(current, tokens):

            if not tokens:
                return current

            string = tokens[0]
            tokens = tokens[1:]

            for child in current.children:
                if child.string == string:
                    return node_from_path_recursive(child, tokens)

            # No appropriate child - create one.
            child = DB.Node(
                string=string,
                parent=current.id
            )

            # Remind the session that the child should be persisted.
            session.add(child)
            session.flush()

            return node_from_path_recursive(child, tokens)

        return node_from_path_recursive(root, list(filter(bool, path.split('/'))))

    @classmethod
    def register(cls, api):

        session = Session()

        root = session.query(DB.Node).filter_by(string=cls.name, parent=None).one_or_none()
        if root is None:
            session.add(DB.Node(string=cls.name))

        session.commit()

        api.add_resource(cls, f"/{cls.name}/<path:path>")


class Sessions(Node):
    name = 'sessions'
    timeout = 3600


class Scanners(Node):
    name = 'scanners'
    timeout = None


def create_app():

    Base.metadata.create_all(engine)

    app = Flask(__name__)
    api = Api(app, prefix='/v1')

    api.add_resource(Info, '/info')

    api.add_resource(BlobList, '/blobs')
    api.add_resource(BlobData, '/blobs/<blob>', endpoint='blobs_data_endpoint')

    Sessions.register(api)
    Scanners.register(api)

    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

    return app
