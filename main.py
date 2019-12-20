
from gevent import monkey
monkey.patch_all()

from gevent.pywsgi import WSGIServer

import socket

import os
import argparse

import storage
import version

def main():
    parser = argparse.ArgumentParser(description="Gadgetron Storage Manager.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-S', '--storage-dir',
                        default=os.getcwd(),
                        help="Set the storage directory.")

    parser.add_argument('-a', '--address', type=str, default='localhost',
                        help="Only accept connections from this address.")
    parser.add_argument('-p', '--port', type=int, default=9102,
                        help="Listen for connections on this port.")

    # Silent option?

    args = parser.parse_args()

    # Ensure the 'blobs' subdirectory exists.
    os.makedirs('./blobs', exist_ok=True)

    sock = socket.socket()
    sock.bind((args.address, args.port))
    sock.listen()

    server = WSGIServer(sock, storage.create_app())

    print(f"Gadgetron Storage Server v. {version.version}")
    print(f"Accepting connections on port {sock.getsockname()[1]}")

    server.serve_forever()


if __name__ == '__main__':
    main()

