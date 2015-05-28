rfoo (remote foo) is a fast Python RPC package which can do 160,000 RPC calls per second on a regular PC. It includes a fast serialization module called rfoo.marsh which extends the Python built in marshal module by eliminating serialization of code objects and protecting against bad input. The result is a safe to use ultra fast serializer.

Included with rfoo is **rconsole**, a remote Python console with auto completion, which can be used to inspect and modify the namespace of a running script. Scroll down for more information.

**Interface of rfoo.marsh:**

```
rfoo.marsh.dumps(expression)
rfoo.marsh.loads(binary_string)
```

**Serve RPC method to clients:**

```
class MyHandler(rfoo.BaseHandler):
    def echo(self, str):
        return str

rfoo.InetServer(MyHandler).start(port=50000)
```

**Call method on RPC server:**

```
c = rfoo.InetConnection().connect(port=50000)
rfoo.Proxy(c).echo('Hello, world!')
```

To send RPC notifications replace the `Proxy` class with the `Notifier` class. Notifications are calls which do not return a value and are an order of magnitude faster than synchronous calls.

**Benchmarks off a 2.4GHz Core 2 Duo laptop:**
  * Create-call-close 4000 connections per second per process.
  * Up to 30,000 calls per second per process with INET sockets.
  * Up to 45,000 calls per second per process with Unix sockets.
  * Up to 160,000 notifications per second per process with INET sockets.

**Notes:**
  * Tested on GNU/Linux, Python 2.x, 3.x.
  * Server and client can be either Python 2.x or 3.x **but not mixed**.
  * Used in production server.

### How to Install ###
**Note it is impossible to install rfoo with easy\_install or pip** because of a conflict between setuptools and Cython (http://mail.python.org/pipermail/distutils-sig/2007-September/008204.html).

rfoo depends on Cython (http://cython.org/). You also need a C compiler and the python development package to be present on the system. On Ubuntu you can take care of all those prerequisites with the following command:
```
sudo apt-get install cython python-dev build-essential
```

On CentOS or Redhat do the following:
```
sudo yum install gcc python-devel python-setuptools
sudo easy_install Cython
```

Next, download the source of rfoo and install with:
```
sudo python setup.py install
```

### rconsole ###
rconsole is a remote Python console with auto completion, which can be used to inspect and modify the namespace of a running script.

To invoke in a script do:
```
from rfoo.utils import rconsole
rconsole.spawn_server()
```

To attach from a shell do:
```
$ rconsole
```

**Security note:** The rconsole listener started with spawn\_server() will accept any local connection and may therefore be insecure to use in shared hosting or similar environments!