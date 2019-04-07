## Pedis 简介

Pedis 是Parallel Redis的简称。简单的说，Pedis 提供了一个分布式，持久化存储的Redis集群方案。

Pedis 构建在Scylla基础之上。Scylla是兼容Apache Cassandra 协议的NoSQL数据库。
由Avi Kivity 带领的团队，采用C++语言实现的高性能NoSQL数据库。
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

* 支持string相关的命令，详情如下：

| 命令名称 | 是否支持 | 备注 |
|--|--|--|
|set | Yes | set 命令只接受2个参数|
|setnx | Yes | |
|setex | Yes |  |
|mset | Yes | |
|msetnx | No | |
|get | Yes| |
|getset | Yes |  |
|del | Yes |  |
|exists | Yes |  |
|expire | Yes | |
|persist |Yes | |
|strlen | Yes| |
|append | Yes | |
|incr | Yes | |
|decr | Yes | |
|incrby | Yes |
|decrby | Yes |
|setrange | No |
|getrange | No |
|ttl | No |
|pttl | No |
|pexpire | No |
|expireat | No |
|pexpireat | No |


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
|blpop | No | |
|brpop | No | |
|brpoplpush | No | |

* 支持 hashmap 数据结构相关的命令，详情如下：

| 命令名称 | 支持情况 | 备注 |
|--|--|--|
|hset | Yes |
|hsetnx | Yes |
|hget | Yes |
|hexists | Yes |
|hdel | Yes |
|hlen | Yes |
|hstrlen | No |
|hincry | No | 
|hincrbyfloat | No |
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
| srandmember | No | |
| srem | Yes| |
| smove | No | |
| scard| No| |
| smembers | Yes | |
| sscan | No | |
| sinter | No | |
| sinterstore | No | |
| sunion| No | |
| sunionstore | No | |
| sdiff | No | |
| sdiffstore | No ||

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
|zrangebylex | No | |
| zlexcount | No | |
| zremrangebyflx | No | |
|zscan | No | |
|zunionstore | No | |
|zintestore | No | |
