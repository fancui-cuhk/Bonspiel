Configuration
-------------

To build the database:

    make

To run on a single node:

    ./rundb

To run on multiple nodes, add the ip address of each node into ifconfig.txt with one IP per line. Then execute ./rundb on each node. More details on the network setup can be found in transport/transport.cpp.

To get command line options:

    ./rundb -h

More parameter setting can be found in config.h

Output 
------

The output data should be mostly self-explanatory. More details can be found in system/stats.cpp.
