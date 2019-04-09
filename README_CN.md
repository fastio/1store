## Pedis 简介

Pedis 是Parallel Redis的简称。简单的说，Pedis 提供了一个分布式，持久化存储的Redis集群方案。

Pedis 构建在Scylla基础之上。Scylla是兼容Apache Cassandra 协议的NoSQL数据库。
目前，该项目已开源，开源协议为 Free Software Foundation’s GNU AGPL v3.0。

在Scylla的基础之上，增加支持Redis协议的功能，构建Redis集群方案。
Pedis 开源协议为  Free Software Foundation’s GNU AGPL v3.0。

## Pedis 项目愿景

Pedis 将依赖开源社区，打磨**简单易用**、**高性能**、**易运维**的Redis集群。

## Pedis 数据持久化机制

Pedis 兼容REDIS协议，实现了Redis 4.0 中常用数据结构。

与MYSQL数据类比，在Scylla中，键空间(keyspace) 相当于MYSQL中的database概念， 列族(colunm family， 在Scylla中也称为table) 相当于MYSQL的table概念。Scylla集群将数据存储在具体的keyspace，具体table中。因此，在Pedis集群启动时候，试图创建存储REDIS 数据结构的keyspace 和 table. Pedis集群的数据将存储在名为 redis 的 keyspace 中，其中，strings 、 lists、 hashmap、set 和 zset 数据分别存储在redis.simple_objects 、redis.lists、redis.maps、redis.set和redis.zset 表中。

通过配置这些表的数据备份数（replication-factor）, 可以确定将Pedis 数据持久化的存储在Scylla集群的各个节点中。当Scylla集群具备跨数据中心部署能力，Pedis 集群将继承该能力。

另外，Scylla支持数据In-memory 存储机制，即数据只存储在内存中，不存储在磁盘中。在这种配置情况下，Pedis 数据可以只存储在内存中，部署成为缓存集群。

## Pedis 集群数据一致性问题

Scylla支持可调节数据一致性功能。通过该功能，Scylla可以实现数据强一致性，也可以自定义读写一致性级别。公式“R + W > N”常用于描述可调节数据一致性的实现。在这个公式里，R 和 W 分别是由所用的一致性级别决定的要读和写的节点数。N 是复制因子。实现强一致性的最常见的办法就是对读和写使用 QUORUM 一致性级别，QUORUM 常被定义成一个大于半数节点的数字。

因此，Pedis集群也将具备可调节的数据一致性功能，可以设置不同级别的读写一致性级别，满足业务需求。

## Pedis 集群数据与Scylla 集群数据互操作性

通过REDIS协议写入的数据存储在集群中的特定的keyspace & table中。这些keyspace & table 中数据也可以通过Cassandra协议来读写。也就是PEDIS集群中的数据，既可以通过REDIS协议来访问，也可以通过Cassandra协议来访问。这为应用带来良好的灵活性。

## Pedis 项目规划

目前，Pedis项目需要完成如下工作：

* 支持Redis 4.0 协议 (优先支持Redis strings, hashmap, set, zset 数据结构相关的协议）；
* 完善测试用例；
* 发布第一个可用版本；

## Pedis 项目进展

目前，Pedis项目已经实现了Redis 4.0协议中大部分命令，具体如下。


* 支持string相关的命令，详情如下：

| 命令名称 | 是否支持 | 备注 |
|--|--|--|
|set | 已支持 | set 命令只接受2个参数(原REDIS支持 NX, XX, PX等选项)|
|setnx | 已支持 | |
|setex | 已支持 |  |
|mset | 已支持 | |
|msetnx | 暂时不支持 | |
|get | 已支持| |
|getset | 已支持 |  |
|del | 已支持 |  |
|exists | 已支持 |  |
|strlen | 已支持| |
|append | 已支持 | |
|incr | 已支持 | |
|decr | 已支持 | |
|incrby | 已支持 |
|decrby | 已支持 |
|setrange | 暂时不支持 |
|getrange | 暂时不支持 |


* 支持list 数据结构相关的命令，详情如下：

| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|lpush | 已支持 | |
|lpushx | 已支持 | |
|rpush | 已支持 | |
|rpushx | 已支持 | |
|lpop | 已支持 |
|rpop | 已支持 |
|lrange | 已支持 | |
|llen | 已支持 | |
|lindex | 已支持 | |
|lrem | 已支持 | |
|lset | 已支持  | |
|ltrim | 已支持 | |
|blpop | 暂时不支持 | |
|brpop | 暂时不支持 | |
|brpoplpush | 暂时不支持 | |

* 支持 hashmap 数据结构相关的命令，详情如下：

| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|hset | 已支持 |
|hsetnx | 已支持 |
|hget | 已支持 |
|hexists | 已支持 |
|hdel | 已支持 |
|hlen | 已支持 |
|hstrlen | 暂时不支持 |
|hincry | 暂时不支持 | 
|hincrbyfloat | 暂时不支持 |
|hmset | 已支持 |
|hmget | 已支持 |
|hkeys | 已支持 |
|hvals | 已支持 |
|hgetall | 已支持 |
|hscan | 已支持 |

* 支持 set 数据结构相关的命令，详情如下：

| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|sadd | 已支持 | |
| sismember | 已支持 | |
| spop | 已支持 | |
| srandmember | 暂时不支持 | |
| srem | 已支持| |
| smove | 暂时不支持 | |
| scard| 暂时不支持| |
| smembers | 已支持 | |
| sscan | 暂时不支持 | |
| sinter | 暂时不支持 | |
| sinterstore | 暂时不支持 | |
| sunion| 暂时不支持 | |
| sunionstore | 暂时不支持 | |
| sdiff | 暂时不支持 | |
| sdiffstore | 暂时不支持 ||

* 支持 zset 数据结构相关的命令, 详情如下：

| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|zadd | 已支持 | |
|zscore | 已支持 | |
|zincrby | 已支持 | |
|zcard | 已支持 | |
|zcount | 已支持 | |
|zrange | 已支持 | |
|zrevrange | 已支持 | |
| zrangebyscore | 已支持 | |
| zrevrangebyscore | 已支持 | |
| zrank | 已支持 | |
| zrevrank | 已支持 | | 
| zrem | 已支持 | |
|zremrangebyrank| 已支持 | |
|zremrangebyscore | 已支持 | |
|zrangebylex | 暂时不支持 | |
| zlexcount | 暂时不支持 | |
| zremrangebyflx | 暂时不支持 | |
|zscan | 暂时不支持 | |
|zunionstore | 暂时不支持 | |
|zintestore | 暂时不支持 | |

* 支持 HyperLogLog 数据结构相关命令，详情如下:

| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|PFADD | 暂时不支持 | |
|PFCOUNT | 暂时不支持 | |
|PFMERGE | 暂时不支持 | |

* 支持GEO 数据结构相关命令，详情如下：

| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|GEOADD | 暂时不支持 |
|GEOPOS | 暂时不支持 |
|GEODIST | 暂时不支持 |
|GEORADIUS | 暂时不支持 |
|GEORADIUSBYMEMBER | 暂时不支持 |
|GEOHASH | 暂时不支持 |


* 支持 Bitmap 数据结构相关的命令，详情如下：


| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|SETBIT | 暂时不支持 |
|GETBIT | 暂时不支持 |
|BITCOUNT | 暂时不支持 |
|BITPOS | 暂时不支持 |
|BITOP | 暂时不支持 |
|BITFIELD | 暂时不支持 |

* 支持 事物 相关命令，详情如下：

| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|MULTI | 暂时不支持|
|EXEC | 暂时不支持 |
|DISCARD | 暂时不支持 |
|WATCH | 暂时不支持 |
|UNWATCH | 暂时不支持 |


* 自动过期相关命令，详情如下：

| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|EXPIRE  | 已支持 | |
|EXPIREAT | 暂时不支持 | |
|TTL | 暂时不支持 |
|PERSIST |已支持 | |
|PEXPIRE |暂时不支持| |
|PEXPIREAT | 暂时不支持 | |
|PTTL | |

* 支持LUA脚本相关命令，详情如下：


| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|EVAL | 暂时不支持 |
|EVALSHA | 暂时不支持 |
|SCRIPT_LOAD | 暂时不支持 |
|SCRIPT_EXISTS | 暂时不支持 |
|SCRIPT_FLUSH | 暂时不支持 |
|SCRIPT_KILL | 暂时不支持 |


* 持久化相关命令，详情如下：

| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|SAVE | 不支持 | Pedis 有完善的数据持久化能力|
|BGSAVE | 不支持| 理由同上|
|BGREWRITEAOF | 不支持 | 理由同上 |
|LASTSAVE | 不支持 | 理由同上 |

* 订阅与发布

| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|PUBLISH | 暂时不支持 |
|SUBSCRIBE | 暂时不支持 |
|PSUBSCRIBE | 暂时不支持 |
|UNSUBSCRIBE | 暂时不支持 |
|PUNSUBSCRIBE | 暂时不支持 |
|PUBSUB | 暂时不支持 |
