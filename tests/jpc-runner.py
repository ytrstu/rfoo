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



def print_usage():
    scriptName = os.path.basename(sys.argv[0])
    sys.stdout.write("""
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
-tN         Number of client threads to use.
""" % {'name': scriptName})



def main():
    """Parse options and run script."""

    try:
        options, args = getopt.getopt(
            sys.argv[1:], 
            'hvsco:p:n:t:', 
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
    
    host = options.get('-o', '127.0.0.1')
    port = int(options.get('-p', jpc.DEFAULT_PORT))

    t0 = time.time()
    try:
        if '-s' in options:
            logging.warning('Start as server.')
            jpc.start_server(host=host, port=port)
            return
            
        logging.warning('Start as client.')

        if len(args) > 0:
            data = args[0]
        else:
            data = 'x' * 10000

        n = int(options.get('-n', 1))
        t = int(options.get('-t', 1))
        m = int(n / t)

        def client():
            if '-c' in options:
                for i in range(m):
                    connection = jpc.connect(host=host, port=port)
                    r = jpc.Proxy(connection).echo_args(data)
                    logging.info('Received %r from proxy.', r)
                    connection.close()

            else:
                connection = jpc.connect(host=host, port=port)
                for i in range(m):
                    r = jpc.Proxy(connection).echo_args(data)
                    logging.info('Received %r from proxy.', r)

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



