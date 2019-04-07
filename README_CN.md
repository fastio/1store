## Pedis 简介

Pedis 是Parallel Redis的简称。简单的说，Pedis 提供了一个分布式，持久化存储的Redis集群方案。

Pedis 构建在Scylla基础之上。Scylla是兼容Apache Cassandra 协议的暂时不支持，将实现SQL数据库。
由Avi Kivity 带领的团队，采用C++语言实现的高性能暂时不支持，将实现SQL数据库。
目前，该项目已开源，开源协议为 Free Software Foundation’s GNU AGPL v3.0。

在Scylla的基础之上，增加支持Redis协议的功能，构建Redis集群方案。
Pedis 开源协议为  Free Software Foundation’s GNU AGPL v3.0。

## Pedis 项目愿景

Pedis 将依赖开源社区，打磨**简单易用**、**高性能**、**易运维**的Redis集群。


## Pedis 项目规划

目前，Pedis项目需要完成如下工作：

* 支持Redis 4.0 协议 (优先支持Redis strings, hashmap, set, zset 数据结构相关的协议）；
* 完善测试用例；
* 发布第一个可用版本；

## Pedis 项目进展

目前，Pedis项目已经实现了Redis 4.0协议中大部分命令，具体如下。


以下表格中，“是否支持” 这一栏中，Yes 表示Pedis 已经支持该命令; 暂时不支持，将实现
表示目前还没支持，后续会支持; 如果Pedis 不支持该命令，会明确注明
"不支持"。


* 支持string相关的命令，详情如下：

| 命令名称 | 是否支持 | 备注 |
|--|--|--|
|set | Yes | set 命令只接受2个参数|
|setnx | Yes | |
|setex | Yes |  |
|mset | Yes | |
|msetnx | 暂时不支持，将实现 | |
|get | Yes| |
|getset | Yes |  |
|del | Yes |  |
|exists | Yes |  |
|strlen | Yes| |
|append | Yes | |
|incr | Yes | |
|decr | Yes | |
|incrby | Yes |
|decrby | Yes |
|setrange | 暂时不支持，将实现 |
|getrange | 暂时不支持，将实现 |


* 支持list 数据结构相关的命令，详情如下：

| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|lpush | Yes | |
|lpushx | Yes | |
|rpush | Yes | |
|rpushx | Yes | |
|lpop | Yes |
|rpop | Yes |
|lrange | Yes | |
|llen | Yes | |
|lindex | Yes | |
|lrem | Yes | |
|lset | Yes  | |
|ltrim | Yes | |
|blpop | 暂时不支持，将实现 | |
|brpop | 暂时不支持，将实现 | |
|brpoplpush | 暂时不支持，将实现 | |

* 支持 hashmap 数据结构相关的命令，详情如下：

| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|hset | Yes |
|hsetnx | Yes |
|hget | Yes |
|hexists | Yes |
|hdel | Yes |
|hlen | Yes |
|hstrlen | 暂时不支持，将实现 |
|hincry | 暂时不支持，将实现 | 
|hincrbyfloat | 暂时不支持，将实现 |
|hmset | Yes |
|hmget | Yes |
|hkeys | Yes |
|hvals | Yes |
|hgetall | Yes |
|hscan | Yes |

* 支持 set 数据结构相关的命令，详情如下：

| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|sadd | Yes | |
| sismember | Yes | |
| spop | Yes | |
| srandmember | 暂时不支持，将实现 | |
| srem | Yes| |
| smove | 暂时不支持，将实现 | |
| scard| 暂时不支持，将实现| |
| smembers | Yes | |
| sscan | 暂时不支持，将实现 | |
| sinter | 暂时不支持，将实现 | |
| sinterstore | 暂时不支持，将实现 | |
| sunion| 暂时不支持，将实现 | |
| sunionstore | 暂时不支持，将实现 | |
| sdiff | 暂时不支持，将实现 | |
| sdiffstore | 暂时不支持，将实现 ||

* 支持 zset 数据结构相关的命令, 详情如下：

| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|zadd | Yes | |
|zscore | Yes | |
|zincrby | Yes | |
|zcard | Yes | |
|zcount | Yes | |
|zrange | Yes | |
|zrevrange | Yes | |
| zrangebyscore | Yes | |
| zrevrangebyscore | Yes | |
| zrank | Yes | |
| zrevrank | Yes | | 
| zrem | Yes | |
|zremrangebyrank| Yes | |
|zremrangebyscore | Yes | |
|zrangebylex | 暂时不支持，将实现 | |
| zlexcount | 暂时不支持，将实现 | |
| zremrangebyflx | 暂时不支持，将实现 | |
|zscan | 暂时不支持，将实现 | |
|zunionstore | 暂时不支持，将实现 | |
|zintestore | 暂时不支持，将实现 | |

* 支持 HyperLogLog 数据结构相关命令，详情如下:

| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|PFADD | 暂时不支持，将实现 | |
|PFCOUNT | 暂时不支持，将实现 | |
|PFMERGE | 暂时不支持，将实现 | |

* 支持GEO 数据结构相关命令，详情如下：

| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|GEOADD | 暂时不支持，将实现 |
|GEOPOS | 暂时不支持，将实现 |
|GEODIST | 暂时不支持，将实现 |
|GEORADIUS | 暂时不支持，将实现 |
|GEORADIUSBYMEMBER | 暂时不支持，将实现 |
|GEOHASH | 暂时不支持，将实现 |


* 支持 Bitmap 数据结构相关的命令，详情如下：


| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|SETBIT | 暂时不支持，将实现 |
|GETBIT | 暂时不支持，将实现 |
|BITCOUNT | 暂时不支持，将实现 |
|BITPOS | 暂时不支持，将实现 |
|BITOP | 暂时不支持，将实现 |
|BITFIELD | 暂时不支持，将实现 |

* 支持 事物 相关命令，详情如下：

| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|MULTI | 暂时不支持，将实现|
|EXEC | 暂时不支持，将实现 |
|DISCARD | 暂时不支持，将实现 |
|WATCH | 暂时不支持，将实现 |
|UNWATCH | 暂时不支持，将实现 |


* 自动过期相关命令，详情如下：

| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|EXPIRE  | Yes | |
|EXPIREAT | 暂时不支持，将实现 | |
|TTL | 暂时不支持，将实现 |
|PERSIST |Yes | |
|PEXPIRE |暂时不支持，将实现| |
|PEXPIREAT | 暂时不支持，将实现 | |
|PTTL | |

* 支持LUA脚本相关命令，详情如下：


| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|EVAL | 暂时不支持，将实现 |
|EVALSHA | 暂时不支持，将实现 |
|SCRIPT_LOAD | 暂时不支持，将实现 |
|SCRIPT_EXISTS | 暂时不支持，将实现 |
|SCRIPT_FLUSH | 暂时不支持，将实现 |
|SCRIPT_KILL | 暂时不支持，将实现 |


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
|PUBLISH | 暂时不支持，将实现 |
|SUBSCRIBE | 暂时不支持，将实现 |
|PSUBSCRIBE | 暂时不支持，将实现 |
|UNSUBSCRIBE | 暂时不支持，将实现 |
|PUNSUBSCRIBE | 暂时不支持，将实现 |
|PUBSUB | 暂时不支持，将实现 |
