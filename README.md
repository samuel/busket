Busket - Time Series Service
============================

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
