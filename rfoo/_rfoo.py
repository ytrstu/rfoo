"""
    rfoo/_rfoo.py

    Fast RPC server.

    Copyright (C) 2010 Nir Aides <nir@winpdb.org>

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



import threading
import logging
import inspect
import socket
import smarx
import sys

try:
    import thread
except:
    import _thread as thread



__version__ = '1.0.8'

#
# Bind to loopback to restrict server to local requests, by default.
#
LOOPBACK = '127.0.0.1'
DEFAULT_PORT = 52431
BUFFER_SIZE = 4096

MAX_THREADS = 128

ISPY3K = sys.version_info[0] >= 3

CALL = 0
NOTIFY = 1



class ServerError(Exception):
    """Wrap server errors by proxy."""



class EofError(Exception):
    """Socket end of file."""



class BaseHandler(object):
    """
    Handle incomming requests.
    Client can call public methods of derived classes.
    """

    def __init__(self, addr=None):
        self._addr = addr
        self._methods = {}


    def _close(self):
        self._methods = {}


    def _get_method(self, name):
        """
        Get public method.
        Verify attribute is public method and use cache for performance.
        """

        m = self._methods.get(name, None)
        if m is not None:
            return m

        if name.startswith('_'):
            logging.warning('Attempt to get non-public, attribute=%s.', name)
            raise ValueError(name)

        m = getattr(self, name)
        if not inspect.ismethod(m):
            logging.warning('Attempt to get non-method, attribute=%s.', name)
            raise ValueError(name)

        self._methods[name] = m

        return m



class EchoHandler(BaseHandler):
    """Echo back call arguments for debugging."""

    def echo(self, *args, **kwargs):
        return {'*args': args, '**kwargs': kwargs}



class ExampleHandler(BaseHandler):
    """
    Demonstrate handler inheritance.
    Start server with: start_server(handler=ExampleHandler)
    Client calls server with: Proxy(connection).add(...)
    """

    def add(self, x, y):
        return x + y



class Connection(object):
    """Wrap socket with buffered read and length prefix for data."""

    def __init__(self, conn):
        self._conn = conn
        self._buffer = ''
        
        if ISPY3K:
            self._buffer = self._buffer.encode('utf-8')


    def __getattr__(self, name):
        """Delegate attributes of socket."""

        return getattr(self._conn, name)


    def close(self):
        """Shut down and close socket."""

        try:
            self._conn.shutdown(socket.SHUT_RDWR)
        except socket.error:
            pass

        self._conn.close()
        

    def write(self, data):
        """Write length prefixed data to socket."""
        
        l = '%08x' % len(data)
        if ISPY3K:
            l = l.encode('utf-8')

        self._conn.sendall(l + data)


    def read(self):
        """Read length prefixed data from socket."""

        buffer = self._buffer

        while len(buffer) < 8:
            data = self._conn.recv(BUFFER_SIZE)
            if not data:
                raise EofError(len(buffer))
            buffer += data

        length = int(buffer[:8], 16) + 8

        while len(buffer) < length:
            data = self._conn.recv(BUFFER_SIZE)
            if not data:
                raise EofError(len(buffer))
            buffer += data

        self._buffer = buffer[length:]
        return buffer[8: length]



g_threads_semaphore = threading.Semaphore(MAX_THREADS)

def threaded(foo):
    """Run foo using bounded number of threads."""

    def wrapper1(*args, **kwargs):
        try:
            foo(*args, **kwargs)
        finally:
            g_threads_semaphore.release()

    def wrapper2(*args, **kwargs):
        g_threads_semaphore.acquire()
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
                _dispatch(handler, c)

        except EofError:
            logging.debug('Caught end of file, error=%r.', sys.exc_info()[1])

    finally:
        c.close()
        handler._close()



def _dispatch(handler, conn):
    data = conn.read()
    type, name, args, kwargs = smarx.loads(data)

    foo = handler._methods.get(name, None)
    if foo is None:
        foo = handler._get_method(name)
    
    try:    
        result = foo(*args, **kwargs)
        error = None
    except Exception:
        logging.warning('Caught exception raised by callable.', exc_info=True)
        result = None
        error = repr(sys.exc_info()[1])

    if type == CALL:
        response = smarx.dumps((result, error))
        conn.write(response)



threaded_connection = threaded(_serve_connection)



def start_server(host=LOOPBACK, port=DEFAULT_PORT, on_accept=threaded_connection, handler=EchoHandler):
    """Start server."""

    logging.info('Enter, handler=%r, port=%d, host=%s.', handler, port, host)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        s.bind((host, port))
        s.listen(5)

        while True:
            conn, addr = s.accept()
            logging.info('Accepted connection from %s.', addr)
            on_accept(conn, addr, handler)

    finally:
        try:
            s.shutdown(socket.SHUT_RDWR)
        except socket.error:
            pass
        s.close()



def connect(host=LOOPBACK, port=DEFAULT_PORT, connection_type=Connection):
    """Connect to server."""

    logging.info('Enter, host=%s, port=%d.', host, port)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    s.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 0)

    return connection_type(s)



class Proxy(object):
    """
    Proxy methods of server handler.
    
    Call Proxy(connection).foo(*args, **kwargs) to invoke method
    handler.foo(*args, **kwargs) of server handler.
    """

    def __init__(self, conn):
        self._conn = conn
        self._name = None


    def __getattr__(self, name):
        self._name = name
        return self


    def __call__(self, *args, **kwargs):
        """
        Call method on server.
        Asynchhronous calls omit ID and do not wait for response.
        """
       
        data = smarx.dumps((CALL, self._name, args, kwargs))
        self._conn.write(data)
        
        response = self._conn.read()
        value, error = smarx.loads(response)
        
        if error is not None:
            logging.warning('Error returned by proxy, error=%s.', error)
            raise ServerError(error)

        return value



class Notifier(Proxy):
    """
    Proxy methods of server handler, asynchronously.
    Call Notifier(connection).foo(*args, **kwargs) to invoke method
    handler.foo(*args, **kwargs) of server handler.
    """


    def __init__(self, conn):
        self._conn = conn
        self._name = None


    def __getattr__(self, name):
        self._name = name
        return self


    def __call__(self, *args, **kwargs):
        """
        Call method on server.
        Asynchhronous calls omit ID and do not wait for response.
        """
       
        data = smarx.dumps((NOTIFY, self._name, args, kwargs))
        self._conn.write(data)
        


