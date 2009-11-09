"""
    jpc/__init__.py

    Fast RPC server, partially compliant with JSON-RPC version 1:
    http://json-rpc.org/wiki/specification

    Copyright (C) 2009 Nir Aides <nir@winpdb.org>

    This program is free software; you can redistribute it and/or
    modify it under the terms of the GNU General Public License
    as published by the Free Software Foundation; either version 2
    of the License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
"""

"""
    Example:

    class MyHandler(BaseHandler):
        def echo(self, str):
            return str

    start_server(handler=MyHandler)

    --- client---

    conn = connect()
    Proxy(conn).echo('Hello World!')
"""



import simplejson
import logging
import inspect
import random
import socket
import getopt
import thread
import Queue
import time
import copy
import sys
import os



#
# Bind to loopback to restrict server to local requests.
#
LOOPBACK = '127.0.0.1'
DEFAULT_PORT = 52431
CHUNK_SIZE = 1024

MAX_THREADS = 256

#
# Python 2.5 logging module supports function name in format string. 
#
if logging.__version__[:3] >= '0.5':
    LOGGING_FORMAT = '[%(process)d:%(thread).5s] %(asctime)s %(levelname)s %(module)s:%(lineno)d %(funcName)s() - %(message)s'
else:
    LOGGING_FORMAT = '[%(process)d:%(thread).5s] %(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s'



class ServerError(Exception):
    """Wrap server errors by proxy."""



class EofError(Exception):
    """Socket end of file."""



class BaseHandler(object):
    """
    Handle incomming requests.
    Client can call public methods of derived classes.
    """

    def __init__(self, addr):
        self._addr = addr



class EchoHandler(BaseHandler):
    def echo_args(self, *args, **kwargs):
        """Echo back call arguments for debugging."""

        return repr({'params': args, 'kwargs': kwargs})



class ExampleHandler(BaseHandler):
    """
    Demonstrate handler inheritance.
    Start server with: start_server(handler=ExampleHandler)
    Client calls server with: Proxy(connection).add(...)
    """

    def add(self, x, y):
        return x + y



g_queue = Queue.Queue(MAX_THREADS)

def threaded(foo):
    """Run foo using bounded number of threads."""

    def wrapper1(*args, **kwargs):
        try:
            foo(*args, **kwargs)
        finally:
            g_queue.get_nowait()

    def wrapper2(*args, **kwargs):
        g_queue.put(1, block=True)
        thread.start_new_thread(wrapper1, args, kwargs)

    return wrapper2



def _serve_connection(conn, addr, handler_factory):
    """
    Serve acceptted connection.
    Should be used in the context of a threaded server, see 
    threaded_connection(), or fork server (not implemented here).
    """

    logging.info('Enter, addr=%s.', addr)

    try:
        #
        # Instantiate handler for the lifetime of the connection,
        # making it possible to manage a state between calls.
        #
        handler = handler_factory(addr)

        try:
            while True:
                data = _read(conn)
                response = _dispatch(handler, data)
                _write(conn, response)

        except EofError:
            logging.debug('Caught end of file.')

    finally:
        conn.close()



threaded_connection = threaded(_serve_connection)



def _dispatch(handler, data):
    """Dispatch call to handler."""

    try:        
        json = simplejson.loads(data)
        id = json['id']

        method = json['method']
        if method.startswith('_'):
            logging.warning('Attempt to call non-public, attribute=%s.', method)
            raise ValueError(method)

        f = getattr(handler, method.lstrip('_'))
        if not inspect.ismethod(f):
            logging.warning('Attempt to call non-method, attribute=%s.', method)

        #
        # Convert kwargs keys from unicode to strings.
        #
        kwargs = json.get('kwargs', {})
        kwargs = dict((str(k), v) for k, v in kwargs.items())
        args = json.get('params', ())

        result = f(*args, **kwargs)
        return simplejson.dumps({'result': result, 'error': None, 'id': id})

    except Exception, e:
        logging.warning('Caught exception raised by callable.', exc_info=True)
        return simplejson.dumps({'result': None, 'error': repr(e), 'id': id})



def start_server(port=DEFAULT_PORT, host=LOOPBACK, on_accept=threaded_connection, handler=EchoHandler):
    """Start server."""

    logging.info('Enter, handler=%r, port=%d, host=%s.', callable, port, host)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        s.bind((host, port))
        s.listen(1)

        while True:
            conn, addr = s.accept()
            logging.info('Accepted connection from %s.', addr)
            on_accept(conn, addr, handler)

    finally:
        s.close()



def connect(host='localhost', port=DEFAULT_PORT):
    """Connect to server."""

    logging.info('Enter, host=%s, port=%d.', host, port)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    return s



class Proxy(object):
    """
    Proxy methods of server handler.
    
    Call Proxy(connection).foo(*args, **kwargs) to invoke method
    handler.foo(*args, **kwargs) of server handler.
    """

    def __init__(self, conn):
        self._conn = conn


    def __getattr__(self, name):
        """Return proxy version of method."""

        conn = self._conn
        class Method(object):
            def __call__(self, *args, **kwargs):
                """Call method on server."""
                
                id = '%08x' % random.randint(0, 2 << 32)
                data = simplejson.dumps({
                    'method': name,
                    'params': args,
                    'kwargs': kwargs,
                    'id': id,
                })

                _write(conn, data)
                response = _read(conn)
                
                r = simplejson.loads(response)
                if r['id'] != id:
                    logging.error('Received unmatching id. sent=%s, received=%s.', id, r['id'])
                    raise ValueError(r['id'])

                if r.get('error', None) is not None:
                    logging.warning('Error returned by proxy, error=%s.', r['error'])
                    raise ServerError(r['error'])

                return r['result']

        return Method()



def _write(socket_, data):
    """Write length prefixed data to socket."""

    logging.debug('Enter, data=%.512s...', data)

    length = len(data)
    socket_.sendall('%08x' % length + data)



def _read(socket_, length=None, debug=True):
    """Read length prefixed data from socket."""

    if length is None:
        length = int(_read(socket_, 8, False), 16)

    data = ''
    while len(data) < length:
        chunk_size = min(length, CHUNK_SIZE)
        chunk = socket_.recv(chunk_size)
        if not chunk:
            raise EofError()
        data += chunk

    if debug:
        logging.debug('Return data=%.512s.', data)

    return data



def print_usage():
    scriptName = os.path.basename(sys.argv[0])
    print """
Start server:
%(name)s -s [-pPORT]

Start client:
%(name)s [-c] [-oHOST] [-pPORT] [-nN] [data]

-h, --help  Print this help.
-v          Debug output.
-s          Start server.
-c          Setup and tear down connection with each iteration.
-oHOST      Set HOST.
-pPORT      Set PORT.
-nN         Repeat client call N times.
""" % {'name': scriptName}



def main():
    """Parse options and run script."""

    try:
        options, args = getopt.getopt(
            sys.argv[1:], 
            'hvsco:p:n:', 
            ['help']
            )
        options = dict(options)

    except getopt.GetoptError:
        print_usage()
        return 2

    if '-h' in options or '--help' in options:
        print_usage()
        return

    if '-v' in options:
        level = logging.DEBUG
    else:
        level = logging.WARNING

    logging.basicConfig(
        level=level, 
        format=LOGGING_FORMAT,
        stream=sys.stderr
    )
    
    port = int(options.get('-p', DEFAULT_PORT))

    t0 = time.time()
    try:
        if '-s' in options:
            start_server(port=port)
            return

        if len(args) > 0:
            data = args[0]
        else:
            data = 'x' * 10000

        host = options.get('-o', '127.0.0.1')
        n = int(options.get('-n', 1))

        if '-c' in options:
            for i in xrange(n):
                connection = connect(host=host, port=port)
                r = Proxy(connection).echo_args(data)
                logging.info('Received %r from proxy.', r)
                connection.close()

        else:
            connection = connect(host=host, port=port)
            for i in xrange(n):
                r = Proxy(connection).echo_args(data)
                logging.info('Received %r from proxy.', r)

    finally:
        logging.warning('Running time, %f seconds.', time.time() - t0)


    
if __name__ == '__main__':
    main()



