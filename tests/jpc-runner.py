#! /usr/bin/env python

"""
    tests/jpc-runner.py

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



import threading
import logging
import socket
import getopt
import time
import jpc
import sys
import os



#
# Python 2.5 logging module supports function name in format string. 
#
if logging.__version__[:3] >= '0.5':
    LOGGING_FORMAT = '[%(process)d:%(thread).5s] %(asctime)s %(levelname)s %(module)s:%(lineno)d %(funcName)s() - %(message)s'
else:
    LOGGING_FORMAT = '[%(process)d:%(thread).5s] %(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s'



class TestHandler(jpc.BaseHandler):
    def __init__(self, *args, **kwargs):
        jpc.BaseHandler.__init__(self, *args, **kwargs)

        self._t = 0


    def iterate(self, n, verbose=False):
        """Iterate n times and return timings."""

        n0 = n
        t0 = time.time()

        while n > 0:
            n -= 1

        t1 = time.time()
        self._t += t1 - t0

        if verbose:
            logging.warning('Ran %d iterations in %f seconds.', n0, t1 - t0)

        return self._t, t1 - t0

    

class Shortcut(object):
    """Dispatch without network, for debugging."""

    def __init__(self, handler):
        self._handler = handler
        self._response = None


    def write(self, data):
        if ISPY3K:
            data = data.decode('utf-8')
        
        self._response = _dispatch(self._handler, data)
        
        if ISPY3K and self._response is not None:
            self._response = self._response.encode('utf-8')        


    def read(self):
        return self._response



def print_usage():
    scriptName = os.path.basename(sys.argv[0])
    sys.stdout.write("""
Start server:
%(name)s -s [-pPORT]

Start client:
%(name)s [-c] [-oHOST] [-pPORT] [-nN] [data]

data, if present should be an integer value, which controls the
length of a CPU intensive loop performed at the server.

-h, --help  Print this help.
-v          Debug output.
-s          Start server.
-a          Use async notifications instead of synchronous calls.
-c          Setup and tear down connection with each iteration.
-oHOST      Set HOST.
-pPORT      Set PORT.
-nN         Repeat client call N times.
-tN         Number of client threads to use.
-iF         Set thread switch interval in seconds (float).
""" % {'name': scriptName})



def main():
    """Parse options and run script."""

    try:
        options, args = getopt.getopt(
            sys.argv[1:], 
            'hvsacuo:p:n:t:i:', 
            ['help']
            )
        options = dict(options)

    except getopt.GetoptError:
        print_usage()
        return 2

    if '-h' in options or '--help' in options:
        print_usage()
        return

    #
    # Prevent timing single connection async calls since 
    # this combination will simply generate a SYN attack,
    # and is not a practical use case.
    #
    if '-a' in options and '-c' in options:
        print_usage()
        return

    if '-v' in options:
        level = logging.DEBUG
        verbose = True
    else:
        level = logging.WARNING
        verbose = False

    logging.basicConfig(
        level=level, 
        format=LOGGING_FORMAT,
        stream=sys.stderr
    )
    
    if '-i' in options:
        interval = float(options.get('-i'))
        sys.setswitchinterval(interval)

    host = options.get('-o', '127.0.0.1')
    port = int(options.get('-p', jpc.DEFAULT_PORT))

    t0 = time.time()
    try:
        if '-s' in options:
            logging.warning('Start as server.')
            jpc.start_server(host=host, port=port, handler=TestHandler)
            return
            
        logging.warning('Start as client.')

        if len(args) > 0:
            data = int(args[0])
        else:
            data = 1000

        n = int(options.get('-n', 1))
        t = int(options.get('-t', 1))
        m = int(n / t)

        if '-a' in options:
            gate = jpc.Notifier
        else:
            gate = jpc.Proxy

        def client():
            #
            # Time connection setup/teardown.
            #
            if '-c' in options:
                for i in range(m):
                    connection = jpc.connect(host=host, port=port)
                    r = jpc.Proxy(connection).iterate(data, verbose)
                    if level == logging.DEBUG:
                        logging.debug('Received %r from proxy.', r)
                    connection.shutdown(socket.SHUT_RDWR)
                    connection.close()

            #
            # Time shortcut connection (no network).
            #
            elif '-u' in options:
                handler = TestHandler()
                shortcut = jpc.Shortcut(handler)
                iterate = gate(shortcut).iterate
                for i in range(m):
                    r = iterate(data, verbose)
                    if level == logging.DEBUG:
                        logging.debug('Received %r from proxy.', r)

            #
            # Time calls synched / asynch (notifications).
            #
            else:
                connection = jpc.connect(host=host, port=port)
                iterate = gate(connection).iterate
                for i in range(m):
                    r = iterate(data, verbose)
                    if level == logging.DEBUG:
                        logging.debug('Received %r from proxy.', r)

            logging.warning('Received %r from proxy.', r)

        if t == 1:
            client()
            return

        threads = [threading.Thread(target=client) for i in range(t)]
        t0 = time.time()
        
        for t in threads:
            t.start()

        for t in threads:
            t.join()

    finally:
        logging.warning('Running time, %f seconds.', time.time() - t0)


    
if __name__ == '__main__':
    main()



