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



import logging
import inspect
import random
import socket
import sys

try:
    import thread
    import Queue
except:
    import _thread as thread
    import queue as Queue

try:
    #
    # Use version > 2.0, for significant performance gains over
    # older versions on some platforms. Can install easily with: 
    # python easy_install simplejson
    #
    import simplejson as json
except ImportError:
    import json



__version__ = '1.0.4'

#
# Bind to loopback to restrict server to local requests.
#
LOOPBACK = '127.0.0.1'
DEFAULT_PORT = 52431
BUFFER_SIZE = 4096

MAX_THREADS = 256

ISPY3K = sys.version_info[0] >= 3



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
    """Echo back call arguments for debugging."""

    def echo(self, *args, **kwargs):
        return repr({'*args': args, '**kwargs': kwargs})



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

    c = Connection(conn)

    try:
        #
        # Instantiate handler for the lifetime of the connection,
        # making it possible to manage a state between calls.
        #
        handler = handler_factory(addr)

        try:
            while True:
                data = c.read()
                
                if ISPY3K:
                    data = data.decode('utf-8')
                
                response = _dispatch(handler, data)
                
                if ISPY3K:
                    response = response.encode('utf-8')
                
                c.write(response)

        except EofError:
            logging.debug('Caught end of file.')

    finally:
        c.close()



threaded_connection = threaded(_serve_connection)



def _dispatch(handler, data):
    """Dispatch call to handler."""

    try:    
        id = 0
        work_item = json.loads(data)
        id = work_item['id']

        #
        # Validate method.
        #
        method = work_item['method']
        if method.startswith('_'):
            logging.warning('Attempt to call non-public, attribute=%s.', method)
            raise ValueError(method)

        f = getattr(handler, method)
        if not inspect.ismethod(f):
            logging.warning('Attempt to call non-method, attribute=%s.', method)
            raise ValueError(method)

        #
        # Convert kwargs keys from unicode to strings.
        #
        kwargs = work_item.get('kwargs', {})
        kwargs = dict((str(k), v) for k, v in kwargs.items())
        args = work_item.get('params', ())

        result = f(*args, **kwargs)
        return json.dumps({'result': result, 'error': None, 'id': id})

    except Exception:
        logging.warning('Caught exception raised by callable.', exc_info=True)
        return json.dumps({'result': None, 'error': repr(sys.exc_info()[1]), 'id': id})



def start_server(host=LOOPBACK, port=DEFAULT_PORT, on_accept=threaded_connection, handler=EchoHandler):
    """Start server."""

    logging.info('Enter, handler=%r, port=%d, host=%s.', handler, port, host)

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



def connect(host=LOOPBACK, port=DEFAULT_PORT):
    """Connect to server."""

    logging.info('Enter, host=%s, port=%d.', host, port)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))

    return Connection(s)



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

        def proxy(*args, **kwargs):
            """Call method on server."""
            
            id = '%08x' % random.randint(0, 2 << 32)
            data = json.dumps({
                'method': name,
                'params': args,
                'kwargs': kwargs,
                'id': id,
            })
            
            if ISPY3K:
                data = data.encode('utf-8')

            self._conn.write(data)
            response = self._conn.read()
           
            if ISPY3K:
                response = response.decode('utf-8')

            r = json.loads(response)
            if r.get('error', None) is not None:
                logging.warning('Error returned by proxy, error=%s.', r['error'])
                raise ServerError(r['error'])

            if r['id'] != id:
                logging.error('Received unmatching id. sent=%s, received=%s.', id, r['id'])
                raise ValueError(r['id'])

            return r['result']

        return proxy



class Connection(object):
    """Wrap socket with buffered read and length prefix for data."""

    def __init__(self, conn):
        self._conn = conn
        self._buffer = ''
        
        if ISPY3K:
            self._buffer = self._buffer.encode('utf-8')


    def close(self):
        self._conn.close()


    def write(self, data):
        """Write length prefixed data to socket."""

        length = len(data)
        l = '%08x' % length
        if ISPY3K:
            l = l.encode('utf-8')

        self._conn.sendall(l + data)


    def read(self):
        """Read length prefixed data from socket."""

        length = int(self._read(8), 16)
        return self._read(length)

   
    def _read(self, length):
        buffer = self._buffer
        while len(buffer) < length:
            data = self._conn.recv(BUFFER_SIZE)
            if not data:
                raise EofError()
            buffer += data

        self._buffer = buffer[length:]
        return buffer[: length]



