# Pedis (Parallel Redis) [中文介绍](README_CN.md)

## What's Pedis?

Pedis is the NoSQL data store using the SEASTAR framework, compatible with REDIS. The name of Pedis is an acronym of **P**arallel r**edis**, which with high throughput and low latency.

Pedis is built on [Scylla](https://www.scylladb.com/). Scylla is compatible with Apache Cassandra, which offers developers a dramatically higher-performing and resource effective NoSQL database to power modern and demanding applications.

## Pedis Vision

Pedis will rely on the open source community to polish REDIS clusters of ** easy to use **, ** high performance **, ** easy to operate and maintain **.

## Pedis data persistence mechanism

Pedis is compatible with REDIS protocol and implements common data structures in Redis 4.0.

By analogy with MYSQL data, in Cylla, key space is equivalent to the database concept in MYSQL, and column family is equivalent to the table concept in MYSQL. Scylla clusters store data in specific keyspaces, specific tables. Therefore, when the Pedis cluster starts, the data that attempts to create keyspaces and tables for storing REDIS data structure. Pedis clusters will be stored in keyspaces called redis, where strings, lists, hashmap, set and Zset data are stored in redis. simple_objects, redis. lists, redis. maps, redis. set and redis. zset tables, respectively.

In addition, Scylla supports data in-memory storage mechanism, that is, data is stored only in memory, not in disk. In this configuration, Pedis data can be stored only in memory and deployed as a cache cluster.

## Data Consistency in Pedis Cluster

Scylla supports adjustable data consistency. Through this function, Scylla can achieve strong data consistency, and also can customize the level of read-write consistency. The formula "R + W > N" is often used to describe the implementation of adjustable data consistency. In this formula, R and W are determined by the number of nodes to read and write, respectively, by the level of consistency used. N is the replication factor. The most common way to achieve strong consistency is to use the QUORUM consistency level for reading and writing, which is often defined as a number larger than half of one node.

Therefore, Pedis cluster will also have adjustable data consistency function, which can set different levels of read-write consistency to meet business needs.

## Pedis Cluster Data and Scylla Cluster Data Interoperability

The data written through REDIS protocol is stored in a specific keyspace & table in the cluster. The data in keyspace & table can also be read and written through Cassandra protocol. That is to say, data in PEDIS cluster can be accessed either through REDIS protocol or Cassandra protocol. This brings good flexibility to applications.

## Planning

At present, the Pedis project needs to complete the following tasks:

* Support Redis 4.0 protocol (priority support Redis strings, hashmap, set, Zset data structure related protocols);
* Improve test cases;
* Publish the first available version;

## Getting started

* ```git clone git@github.com:fastio/pedis.git```
* ``` git submodule update --init --recursive```
* ```sudo ./install-dependencies.sh```
* ```./configure.py --mode=release --with=pedis```
* ```ninja-build -j16``` # Assuming 16 system threads.
* ```build/release/pedis --max-io-requests 1024 --smp 2```


