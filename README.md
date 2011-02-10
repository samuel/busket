Busket - Event Aggregation Service
==================================

Busket is essentially an RRD like service. You send it events over UDP, and
it calculates rates for different intervals (e.g. every minute, every hour).
The time-series data is written out to a configurable datastore (currently
only MongoDB).

Building Busket
---------------

    $ cd busket
    $ make release

This will generate busket/rel/busket which contains everything necessary.

Starting Busket
---------------

Once you have built Busket, you can start it as following:

    $ cd busket/rel/busket
    $ bin/busket start

Configuration
-------------

The configuration is stored in busket/rel/busket/etc/app.config and vm.args

UDP Packet Format
-----------------

A single packet can contain any number of events in the following format
concatenated together.

    <value : 64-bit big-endian float><category+key length : 16-bit unsigned><category : char *>\x00<key1 : char *>\x00<key2 : char*>...

Python Client
-------------

    import socket
    import struct

    class BusketTime(object):
        def __init__(self, host="127.0.0.1", port=5252):
            self.host = host
            self.port = port
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        def record(self, events):
            data = "".join(struct.pack(">cdB", x[2], x[1], len(x[0])) + x[0] for x in events)
            self.sock.sendto(data, 0, (self.host, self.port))

        def gauge(self, event, value):
            self.record([(event, value, "g")])

        def absolute(self, event, value):
            self.record([(event, value, "a")])

        def counter(self, event, value):
            self.record([(event, value, "c")])
