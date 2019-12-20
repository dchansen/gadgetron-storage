
import uuid

from flask import Flask, request, jsonify, send_from_directory
from flask_restful import Api, Resource, fields, marshal

from sqlalchemy import create_engine
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, UniqueConstraint

from sqlalchemy.sql import func
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.orderinglist import ordering_list

import version

engine = create_engine("sqlite:///gadgetron-storage.sqlite")

Base = declarative_base()

Session = sessionmaker()
Session.configure(bind=engine)


class DB:

    class Blob(Base):
        __tablename__ = 'blobs'

        blob_id = Column(String(36), primary_key=True)

        marshal_fields = {
            'id': fields.String(attribute='blob_id'),
            'uri': fields.Url('blobs_data_endpoint')
        }

        def marshal(self):
            return marshal(self, self.marshal_fields)

    class Entry(Base):
        __tablename__ = 'entries'

        entry_id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
        blob_id = Column(String(36), ForeignKey('blobs.blob_id'), nullable=False)
        node_id = Column(Integer, ForeignKey('nodes.node_id'), nullable=False)
        rank = Column(Integer, nullable=False)

    class Node(Base):
        __tablename__ = 'nodes'
        __table_args__ = UniqueConstraint('parent', 'string'),

        node_id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
        string = Column(String, nullable=False)
        parent = Column(Integer, ForeignKey('nodes.node_id'))

        created = Column(DateTime, server_default=func.now())
        updated = Column(DateTime, server_default=func.now(), server_onupdate=func.now())
        timeout = Column(DateTime)

        children = relationship('Node')
        contents = relationship('Entry',
                                order_by='Entry.rank', cascade='all, delete-orphan',
                                collection_class=ordering_list('rank'))

        marshal_fields = {
            'created': fields.DateTime,
            'updated': fields.DateTime,
            'timeout': fields.DateTime,
            'children': fields.List(fields.Nested({
                'string': fields.String
            })),
            'contents': fields.List(fields.Nested({
                'id': fields.String(attribute='blob_id'),
                'uri': fields.Url(endpoint='blobs_data_endpoint')
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

        blob = DB.Blob(blob_id=str(blob_id))

        session = Session()
        session.add(blob)
        session.commit()

        return jsonify(blob.marshal())


class Node(Resource):

    @classmethod
    def get(cls, path):

        session = Session()
        node = cls._node_from_path(session, path)
        session.commit()

        return jsonify(node.marshal())

    @classmethod
    def patch(cls, path):

        session = Session()
        node = cls._node_from_path(session, path)

        def append(blobs):
            for blob in blobs:
                node.contents.append(DB.Entry(blob_id=blob))

        def push(blobs):
            for blob in reversed(blobs):
                node.contents.insert(0, DB.Entry(blob_id=blob))

        def pop(blobs):
            if node.contents:
                node.contents.pop(0)

        operations = {
            'append': append,
            'push': push,
            'pop': pop
        }

        operation = operations.get(request.json.get('operation'))
        arguments = request.json.get('arguments', [])

        operation(arguments)

        session.commit()

        return jsonify(node.marshal())

    @classmethod
    def _node_from_path(cls, session, path):

        root = session.query(DB.Node).filter_by(string=cls.name, parent=None).one()

        def node_from_path_recursive(current, tokens):

            if not tokens:
                return current

            string = tokens[0]
            tokens = tokens[1:]

            for child in current.children:
                if child.string == string:
                    return node_from_path_recursive(child, tokens)

            # No appropriate child - create one.
            child = DB.Node(string=string)
            current.children.append(child)

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

        api.add_resource(cls, f"/{cls.name}/<path:path>", endpoint=cls.endpoint)


class Sessions(Node):
    name = 'sessions'
    endpoint = 'sessions_node_endpoint'

    timeout = 3600


class Scanners(Node):
    name = 'scanners'
    endpoint = 'scanners_node_endpoint'

    timeout = None


def create_app():

    Base.metadata.create_all(engine)

    app = Flask(__name__)
    api = Api(app, prefix='/v1')

    api.add_resource(Info, '/info')

    api.add_resource(BlobList, '/blobs')
    api.add_resource(BlobData, '/blobs/<blob_id>', endpoint='blobs_data_endpoint')

    Sessions.register(api)
    Scanners.register(api)

    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

    return app
