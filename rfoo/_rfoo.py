"""
    rfoo/_rfoo.py

    Fast RPC server.

    Copyright (c) 2010 Nir Aides <nir@winpdb.org> and individual contributors.
    All rights reserved.

    Redistribution and use in source and binary forms, with or without modification,
    are permitted provided that the following conditions are met:

    1. Redistributions of source code must retain the above copyright notice, 
    this list of conditions and the following disclaimer.

    2. Redistributions in binary form must reproduce the above copyright 
    notice, this list of conditions and the following disclaimer in the
    documentation and/or other materials provided with the distribution.

    3. Neither the name of Nir Aides nor the names of other contributors may 
    be used to endorse or promote products derived from this software without
    specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
    ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
    WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
    DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
    ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
    (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
    ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
    SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
import sys
import os

import rfoo.marsh as marsh

try:
    import thread
except:
    import _thread as thread



__version__ = '1.1.0'

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



class ServerError(IOError):
    """Wrap server errors by proxy."""



class EofError(IOError):
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

    def __init__(self, conn=None):
        self._conn = conn
        self._buffer = ''
        
        if ISPY3K:
            self._buffer = self._buffer.encode('utf-8')

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



class InetConnection(Connection):
    """Connection type for INET sockets."""

    def __init__(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        Connection.__init__(self, s)

    def connect(self, host=LOOPBACK, port=DEFAULT_PORT):
        self._conn.connect((host, port))
        return self

        

class UnixConnection(Connection):
    """Connection type for Unix sockets."""

    def __init__(self):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        Connection.__init__(self, s)

    def connect(self, path):
        self._conn.connect(path)
        return self



class BPipe(object):
    """Interface read/write pipes as a socket."""

    def connect(self, r=None, w=None):
        self._r = r
        self._w = w

    def recv(self, size):
        return os.read(self._r, size)

    def sendall(self, data):
        return os.write(self._w, data)

    def shutdown(self, x):
        pass

    def close(self):
        if self._r is not None:
            os.close(self._r)

        if self._w is not None:
            os.close(self._w)



class PipeConnection(Connection):
    """Connection type for pipes."""

    def __init__(self):
        Connection.__init__(self, BPipe())

    def connect(self, r=None, w=None):
        self._conn.connect(r, w)
        return self



class Proxy(object):
    """Proxy methods of server handler.
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
        """Call method on server."""
       
        data = marsh.dumps((CALL, self._name, args, kwargs))
        self._conn.write(data)
        
        response = self._conn.read()
        value, error = marsh.loads(response)
        
        if error is not None:
            logging.warning('Error returned by proxy, error=%s.', error)
            raise ServerError(error)

        return value



class Notifier(Proxy):
    """Proxy methods of server handler, asynchronously.
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
        """Call method on server, don't wait for response."""
       
        data = marsh.dumps((NOTIFY, self._name, args, kwargs))
        self._conn.write(data)
        


g_threads_semaphore = threading.Semaphore(MAX_THREADS)

def run_in_thread(foo):
    """Decorate to run foo using bounded number of threads."""

    def wrapper1(*args, **kwargs):
        try:
            foo(*args, **kwargs)
        finally:
            g_threads_semaphore.release()

    def wrapper2(*args, **kwargs):
        g_threads_semaphore.acquire()
        thread.start_new_thread(wrapper1, args, kwargs)

    return wrapper2



class Server(object):
    """Serve calls over connection."""

    def __init__(self, handler_type, conn=None):
        self._handler_type = handler_type
        self._conn = conn
    
    def close(self):
        try:
            self._conn.shutdown(socket.SHUT_RDWR)
        except socket.error:
            pass
        self._conn.close()

    def start(self):
        """Start server, is it?
        Socket is excpted bound.
        """

        logging.info('Enter.')

        try:
            self._conn.listen(5)

            while True:
                conn, addr = self._conn.accept()
                self._on_accept(conn, addr)

        finally:
            self.close()

    def _on_accept(self, conn, addr):
        """Serve acceptted connection.
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
            handler = self._handler_type(addr)

            try:
                while True:
                    self._dispatch(handler, c)

            except EofError:
                logging.debug('Caught end of file, error=%r.', sys.exc_info()[1])

        finally:
            c.close()
            handler._close()

    def _dispatch(self, handler, conn):
        """Serve single call."""

        data = conn.read()
        type, name, args, kwargs = marsh.loads(data)

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
            response = marsh.dumps((result, error))
            conn.write(response)



class InetServer(Server):
    """Serve calls over INET sockets."""
    
    def __init__(self, handler_type):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        Server.__init__(self, handler_type, s)

    def start(self, host=LOOPBACK, port=DEFAULT_PORT):
        self._conn.bind((host, port))
        Server.start(self) 

    _on_accept = run_in_thread(Server._on_accept)



class UnixServer(Server):
    """Serve calls over Unix sockets."""
    
    def __init__(self, handler_type):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        Server.__init__(self, handler_type, s)

    def start(self, path):
        self._conn.bind(path)
        Server.start(self) 

    _on_accept = run_in_thread(Server._on_accept)



class PipeServer(Server):
    """Serve calls over pipes."""

    def __init__(self, handler_type):
        Server.__init__(self, handler_type, BPipe())

    def start(self, r, w=None):
        self._conn.connect(r, w)
        self._on_accept(self._conn, 'pipes')
   


def start_server(handler, host=LOOPBACK, port=DEFAULT_PORT):
    "Start server - depratcated."""

    InetServer(handler).start(host, port)



def connect(host=LOOPBACK, port=DEFAULT_PORT):
    """Connect to server - depracated."""

    return InetConnection().connect(host, port)
    


