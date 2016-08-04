#Pedis

## What's Pedis?

NoSQL data store using the SEASTAR framework, compatible with REDIS.

Redis is very popular *data structures* server. For more infomation, see here: http://redis.io/
Seastar is an advanced, open-source C++ framework for high-performance server applications on modern hardware.
For more infomation, see here: http://www.seastar-project.org/


## Building Pedis on Ubuntu 14.04

In addition to required packages by Seastar, the following packages are required by Pedis.

Installing required packages:
```
sudo apt-get install libaio-dev ninja-build ragel libhwloc-dev libnuma-dev libpciaccess-dev libcrypto++-dev libboost-all-dev libxen-dev libxml2-dev xfslibs-dev
```

Installing GCC 4.9 for gnu++1y. Unlike the Fedora case above, this will
not harm the existing installation of GCC 4.8, and will install an
additional set of compilers, and additional commands named gcc-4.9,
g++-4.9, etc., that need to be used explicitly, while the "gcc", "g++",
etc., commands continue to point to the 4.8 versions.

```
# Install add-apt-repository
sudo apt-get install software-properties-common python-software-properties
# Use it to add Ubuntu's testing compiler repository
sudo add-apt-repository ppa:ubuntu-toolchain-r/test
sudo apt-get update
# Install gcc 4.9 and relatives
sudo apt-get install g++-4.9
# Also set up necessary header file links and stuff (?)
sudo apt-get install gcc-4.9-multilib g++-4.9-multilib
```

To compile Seastar explicitly using gcc 4.9, use:
```
./configure.py --compiler=g++-4.9
ninja 
```


## Run Pedis 

```
./build/release/pedis --smp 1

```
