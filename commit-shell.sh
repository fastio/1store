git  commit -m "
    Redis API in Scylla

    Scylla has advantage and amazing features. If Redis build on the top of Scylla,
    it has the above features automatically. It's achived great progress
    in cluster master managment, data persistence, failover and replication.

    The benefits to the users are easy to use and develop in their production
    environment, and taking avantages of Scylla.

    Using the Ragel to parse the Redis request, server abtains the command name
    and the parameters from the request, invokes the Scylla's internal API to
    read and write the data, then replies to client.

    Signed-off-by: Peng Jian, <pengjian.uestc@gmail.com>
"
