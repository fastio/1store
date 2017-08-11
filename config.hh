/*
 * Copyright (C) 2015 Scylla
 *
 */

/*
 * This file is part of Scylla. Modified by Pedis.
 * Remove the unused and invalid parameters.
 *
 * Redis is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Redis is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Redis.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include <unordered_map>
#include "core/sstring.hh"
#include "core/future.hh"

class file;

namespace redis {

class string_map : public std::unordered_map<sstring, sstring> {
public:
    using std::unordered_map<sstring, sstring>::unordered_map;
};

/*
 * This type is not use, and probably never will be.
 * So it makes sense to jump through hoops just to ensure
 * it is in fact handled properly...
 */

class config {
public:
    enum class value_status {
        Used,
        Unused,
        Invalid,
    };

    enum class config_source : uint8_t {
        None,
        SettingsFile,
        CommandLine
    };

    template<typename T, value_status S>
    struct value {
        typedef T type;
        typedef value<T, S> MyType;

        value(const T& t = T()) : _value(t)
        {}
        value_status status() const {
            return S;
        }
        config_source source() const {
            return _source;
        }
        bool is_set() const {
            return _source > config_source::None;
        }
        MyType & operator()(const T& t) {
            _value = t;
            return *this;
        }
        MyType & operator()(T&& t) {
            _value = std::move(t);
            return *this;
        }
        const T& operator()() const {
            return _value;
        }
        T& operator()() {
            return _value;
        }
    private:
        friend class config;
        T _value = T();
        config_source _source = config_source::None;
    };

    config();

    // Throws exception if experimental feature is disabled.
    void check_experimental(const sstring& what) const;

    boost::program_options::options_description
    get_options_description();

    boost::program_options::options_description_easy_init&
    add_options(boost::program_options::options_description_easy_init&);

    void read_from_yaml(const sstring&);
    void read_from_yaml(const char *);
    future<> read_from_file(const sstring&);
    future<> read_from_file(file);

    /**
     * Scans the environment variables for configuration files directory
     * definition. It's either $Redis_CONF, $Redis_HOME/conf or "conf" if none
     * of Redis_CONF and Redis_HOME is defined.
     *
     * @return path of the directory where configuration files are located
     *         according the environment variables definitions.
     */
    static boost::filesystem::path get_conf_dir();
    using string_map = redis::string_map;
    typedef std::vector<sstring> string_list;

    /*
     * All values and documentation taken from
     * http://docs.datastax.com/en/cassandra/2.1/cassandra/configuration/configCassandra_yaml_r.html
     *
     * Big fat x-macro expansion of _all_ (at this writing) cassandra opts, with the "documentation"
     * included.
     *
     * X-macro syntax:
     *
     * X(member, type, default, status, desc [, value, value...])
     *
     * Where
     *  member: is the property name -> config member name
     *  type:   is the value type (bool, uint32_t etc)
     *  status: is the current _usage_ of the opt. I.e. if you actually use the value, set it to "Used".
     *          Most values are set to "Unused", as in "will probably have an effect eventually".
     *          Values set to "Invalid" have no meaning/usage in Redis, and should (and will currently)
     *          be signaled to a user providing a config with them, that these settings are pointless.
     *  desc:   documentation.
     *  value...: enumerated valid values if any. Not currently used, but why not...
     *
     *
     * Note:
     * Only values marked as "Used" will be added as command line options.
     * Options marked as "Invalid" will be warned about if read from config
     * Options marked as "Unused" will also warned about.
     *
     */

#define _make_config_values(val)                \
    /* Initialization properties */             \
    /* The minimal properties needed for configuring a cluster. */  \
    val(cluster_name, sstring, "Test", Used,   \
            "The name of the cluster; used to prevent machines in one logical cluster from joining another. All nodes participating in a cluster must have the same value."   \
    )                                           \
    val(listen_address, sstring, "localhost", Used,     \
            "The IP address or hostname that Redis binds to for connecting to other Redis nodes. Set this parameter or listen_interface, not both. You must change the default setting for multiple nodes to communicate:\n"    \
            "\n"    \
            "Generally set to empty. If the node is properly configured (host name, name resolution, and so on), Redis uses InetAddress.getLocalHost() to get the local address from the system.\n" \
            "For a single node cluster, you can use the default setting (localhost).\n" \
            "If Redis can't find the correct address, you must specify the IP address or host name.\n"  \
            "Never specify 0.0.0.0; it is always wrong."  \
    )                                                   \
    /* Default directories */   \
    /* If you have changed any of the default directories during installation, make sure you have root access and set these properties: */  \
    val(commitlog_directory, sstring, "${REDIS_HOME}/commitlog", Used,   \
            "The directory where the commit log is stored. For optimal write performance, it is recommended the commit log be on a separate disk partition (ideally, a separate physical device) from the data file directories."   \
    )                                           \
    val(data_file_directories, string_list, { "${REDIS_HOME}/data" }, Used,   \
            "The directory location where table data (SSTables) is stored"   \
    )                                           \
    val(saved_caches_directory, sstring, "${REDIS_HOME}/saved_caches", Unused, \
            "The directory location where table key and row caches are stored."  \
    )                                                   \
    /* Commonly used properties */  \
    /* Properties most frequently used when configuring Redis. */   \
    /* Before starting a node for the first time, you should carefully evaluate your requirements. */   \
    /* Common initialization properties */  \
    /* Note: Be sure to set the properties in the Quick start section as well. */   \
    val(commit_failure_policy, sstring, "stop", Used, \
            "Policy for commit disk failures:\n"    \
            "\n"    \
            "\tdie          Shut down gossip and Thrift and kill the JVM, so the node can be replaced.\n"   \
            "\tstop         Shut down gossip and Thrift, leaving the node effectively dead, but can be inspected using JMX.\n" \
            "\tstop_commit  Shut down the commit log, letting writes collect but continuing to service reads (as in pre-2.0.5 Cassandra).\n"    \
            "\tignore       Ignore fatal errors and let the batches fail."\
            , "die", "stop", "stop_commit", "ignore"    \
    )   \
    val(disk_failure_policy, sstring, "stop", Used, \
            "Sets how Redis responds to disk failure. Recommend settings are stop or best_effort.\n"    \
            "\n"    \
            "\tdie              Shut down gossip and Thrift and kill the JVM for any file system errors or single SSTable errors, so the node can be replaced.\n"   \
            "\tstop_paranoid    Shut down gossip and Thrift even for single SSTable errors.\n"    \
            "\tstop             Shut down gossip and Thrift, leaving the node effectively dead, but available for inspection using JMX.\n"    \
            "\tbest_effort      Stop using the failed disk and respond to requests based on the remaining available SSTables. This means you will see obsolete data at consistency level of ONE.\n"    \
            "\tignore           Ignores fatal errors and lets the requests fail; all file system errors are logged but otherwise ignored. Redis acts as in versions prior to Cassandra 1.2.\n"    \
            "\n"    \
            "Related information: Handling Disk Failures In Cassandra 1.2 blog and Recovering from a single disk failure using JBOD.\n"    \
            , "die", "stop_paranoid", "stop", "best_effort", "ignore"   \
    )   \
    val(rpc_address, sstring, "localhost", Used,     \
            "The listen address for client connections. Valid values are:\n"    \
            "\n"    \
            "\tunset:   Resolves the address using the hostname configuration of the node. If left unset, the hostname must resolve to the IP address of this node using /etc/hostname, /etc/hosts, or DNS.\n"  \
            "\t0.0.0.0 : Listens on all configured interfaces, but you must set the broadcast_rpc_address to a value other than 0.0.0.0.\n"   \
            "\tIP address\n"    \
            "\thostname\n"  \
            "Related information: Network\n"    \
    )                                                   \
    val(seeds, sstring, "127.0.0.1", Used, \
            "he addresses of hosts deemed contact points. Redis nodes use the -seeds list to find each other and learn the topology of the ring." \
    ) \
    /* Common compaction settings */    \
    val(phi_convict_threshold, uint32_t, 8, Used,     \
            "Adjusts the sensitivity of the failure detector on an exponential scale. Generally this setting never needs adjusting.\n"  \
            "Related information: Failure detection and recovery"  \
    )                                                   \
    /* Performance tuning properties */ \
    /* Tuning performance and system reso   urce utilization, including commit log, compaction, memory, disk I/O, CPU, reads, and writes. */    \
    /* Commit log settings */   \
    val(commitlog_sync, sstring, "periodic", Used,     \
            "The method that Redis uses to acknowledge writes in milliseconds:\n"   \
            "\n"    \
            "\tperiodic : Used with commitlog_sync_period_in_ms (Default: 10000 - 10 seconds ) to control how often the commit log is synchronized to disk. Periodic syncs are acknowledged immediately.\n"   \
            "\tbatch : Used with commitlog_sync_batch_window_in_ms (Default: disabled **) to control how long Redis waits for other writes before performing a sync. When using this method, writes are not acknowledged until fsynced to disk.\n"  \
            "Related information: Durability"   \
    )                                                   \
    val(commitlog_segment_size_in_mb, uint32_t, 64, Used,     \
            "Sets the size of the individual commitlog file segments. A commitlog segment may be archived, deleted, or recycled after all its data has been flushed to SSTables. This amount of data can potentially include commitlog segments from every table in the system. The default size is usually suitable for most commitlog archiving, but if you want a finer granularity, 8 or 16 MB is reasonable. See Commit log archive configuration.\n"  \
            "Related information: Commit log archive configuration" \
    )                                                   \
    /* Note: does not exist on the listing page other than in above comment, wtf? */    \
    val(commitlog_sync_period_in_ms, uint32_t, 10000, Used,     \
            "Controls how long the system waits for other writes before performing a sync in \"periodic\" mode."    \
    )   \
    /* Note: does not exist on the listing page other than in above comment, wtf? */    \
    val(commitlog_sync_batch_window_in_ms, uint32_t, 10000, Used,     \
            "Controls how long the system waits for other writes before performing a sync in \"batch\" mode."    \
    )   \
    val(commitlog_total_space_in_mb, int64_t, -1, Used,     \
            "Total space used for commitlogs. If the used space goes above this value, Redis rounds up to the next nearest segment multiple and flushes memtables to disk for the oldest commitlog segments, removing those log segments. This reduces the amount of data to replay on startup, and prevents infrequently-updated tables from indefinitely keeping commitlog segments. A small total commitlog space tends to cause more flush activity on less-active tables.\n"  \
            "Related information: Configuring memtable throughput"  \
    )                                                   \
    /* Compaction settings */   \
    /* Related information: Configuring compaction */   \
    val(defragment_memory_on_idle, bool, true, Used, "Set to true to defragment memory when the cpu is idle.  This reduces the amount of work Redis performs when processing client requests.") \
    /* Memtable settings */ \
    /* Cache and index settings */  \
    val(listen_on_broadcast_address, bool, false, Used, "When using multiple physical network interfaces, set this to true to listen on broadcast_address in addition to the listen_address, allowing nodes to communicate in both interfaces.  Ignore this property if the network configuration automatically routes between the public and private networks such as EC2." \
        )\
    val(storage_port, uint16_t, 7000, Used,                \
            "The port for inter-node communication."  \
    )                                                   \
    val(internode_compression, sstring, "none", Used,     \
            "Controls whether traffic between nodes is compressed. The valid values are:\n" \
            "\n"    \
            "\tall: All traffic is compressed.\n"   \
            "\tdc : Traffic between data centers is compressed.\n"  \
            "\tnone : No compression."  \
    )   \
    val(service_port, uint16_t, 6379, Used,                \
            "Port on which the redis service listens for clients."  \
    )   \
    val(permissions_validity_in_ms, uint32_t, 2000, Used,     \
            "How long permissions in cache remain valid. Depending on the authorizer, such as CassandraAuthorizer, fetching permissions can be resource intensive. This setting disabled when set to 0 or when AllowAllAuthorizer is set.\n"  \
            "Related information: Object permissions"   \
    )   \
    val(permissions_update_interval_in_ms, uint32_t, 2000, Used,     \
            "Refresh interval for permissions cache (if enabled). After this interval, cache entries become eligible for refresh. On next access, an async reload is scheduled and the old value is returned until it completes. If permissions_validity_in_ms , then this property must benon-zero."   \
    )   \
    val(permissions_cache_max_entries, uint32_t, 1000, Used,    \
            "Maximum cached permission entries" \
    )   \
    val(server_encryption_options, string_map, /*none*/, Used,     \
            "Enable or disable inter-node encryption. You must also generate keys and provide the appropriate key and trust store locations and passwords. No custom encryption options are currently enabled. The available options are:\n"    \
            "\n"    \
            "internode_encryption : (Default: none ) Enable or disable encryption of inter-node communication using the TLS_RSA_WITH_AES_128_CBC_SHA cipher suite for authentication, key exchange, and encryption of data transfers. The available inter-node options are:\n"  \
            "\tall : Encrypt all inter-node communications.\n"  \
            "\tnone : No encryption.\n" \
            "\tdc : Encrypt the traffic between the data centers (server only).\n"  \
            "\track : Encrypt the traffic between the racks(server only).\n"    \
            "certificate : (Default: conf/Redis.crt) The location of a PEM-encoded x509 certificate used to identify and encrypt the internode communication.\n"    \
            "keyfile : (Default: conf/Redis.key) PEM Key file associated with certificate.\n"  \
            "truststore : (Default: <system truststore> ) Location of the truststore containing the trusted certificate for authenticating remote servers.\n"    \
            "Related information: Node-to-node encryption"  \
    )   \
    val(client_encryption_options, string_map, /*none*/, Used,     \
            "Enable or disable client-to-node encryption. You must also generate keys and provide the appropriate key and certificate. No custom encryption options are currently enabled. The available options are:\n"    \
            "\n"    \
            "\tenabled : (Default: false ) To enable, set to true.\n"    \
            "\tcertificate: (Default: conf/Redis.crt) The location of a PEM-encoded x509 certificate used to identify and encrypt the client/server communication.\n"   \
            "\tkeyfile: (Default: conf/Redis.key) PEM Key file associated with certificate.\n"   \
            "Related information: Client-to-node encryption"    \
    )   \
    val(ssl_storage_port, uint32_t, 7001, Used,     \
            "The SSL port for encrypted communication. Unused unless enabled in encryption_options."  \
    )                                                   \
    val(default_log_level, sstring, "info", Used, \
            "Default log level for log messages.  Valid values are trace, debug, info, warn, error.") \
    val(logger_log_level, string_map, /* none */, Used,\
            "map of logger name to log level.  Valid values are trace, debug, info, warn, error.  " \
            "Use --help-loggers for a list of logger names") \
    val(log_to_stdout, bool, true, Used, "Send log output to stdout") \
    val(log_to_syslog, bool, false, Used, "Send log output to syslog") \
    val(enable_in_memory_data_store, bool, false, Used, "Enable in memory mode (system tables are always persisted)") \
    val(enable_cache, bool, true, Used, "Enable cache") \
    val(enable_commitlog, bool, true, Used, "Enable commitlog") \
    val(volatile_system_keyspace_for_testing, bool, false, Used, "Don't persist system keyspace - testing only!") \
    val(api_port, uint16_t, 10000, Used, "Http Rest API port") \
    val(api_address, sstring, "", Used, "Http Rest API address") \
    val(api_ui_dir, sstring, "swagger-ui/dist/", Used, "The directory location of the API GUI") \
    val(api_doc_dir, sstring, "api/api-doc/", Used, "The API definition file directory") \
    val(join_ring, bool, true, Used, "When set to true, a node will join the token ring. When set to false, a node will not join the token ring. User can use nodetool join to initiate ring joinging later. Same as -Dcassandra.join_ring in cassandra.") \
    val(load_ring_state, bool, true, Used, "When set to true, load tokens and host_ids previously saved. Same as -Dcassandra.load_ring_state in cassandra.") \
    val(replace_node, sstring, "", Used, "The UUID of the node to replace. Same as -Dcassandra.replace_node in cssandra.") \
    val(replace_token, sstring, "", Used, "The tokens of the node to replace. Same as -Dcassandra.replace_token in cassandra.") \
    val(replace_address, sstring, "", Used, "The listen_address or broadcast_address of the dead node to replace. Same as -Dcassandra.replace_address.") \
    val(replace_address_first_boot, sstring, "", Used, "Like replace_address option, but if the node has been bootstrapped successfully it will be ignored. Same as -Dcassandra.replace_address_first_boot.") \
    val(override_decommission, bool, false, Used, "Set true to force a decommissioned node to join the cluster") \
    val(ring_delay_ms, uint32_t, 30 * 1000, Used, "Time a node waits to hear from other nodes before joining the ring in milliseconds. Same as -Dcassandra.ring_delay_ms in cassandra.") \
    val(shutdown_announce_in_ms, uint32_t, 2 * 1000, Used, "Time a node waits after sending gossip shutdown message in milliseconds. Same as -Dcassandra.shutdown_announce_in_ms in cassandra.") \
    val(developer_mode, bool, false, Used, "Relax environment checks. Setting to true can reduce performance and reliability significantly.") \
    val(skip_wait_for_gossip_to_settle, int32_t, -1, Used, "An integer to configure the wait for gossip to settle. -1: wait normally, 0: do not wait at all, n: wait for at most n polls. Same as -Dcassandra.skip_wait_for_gossip_to_settle in cassandra.") \
    val(experimental, bool, false, Used, "Set to true to unlock experimental features.") \
    val(lsa_reclamation_step, size_t, 1, Used, "Minimum number of segments to reclaim in a single step") \
    val(prometheus_port, uint16_t, 9180, Used, "Prometheus port, set to zero to disable") \
    val(prometheus_address, sstring, "0.0.0.0", Used, "Prometheus listening address") \
    val(prometheus_prefix, sstring, "Redis", Used, "Set the prefix of the exported Prometheus metrics. Changing this will break Redis's dashboard compatibility, do not change unless you know what you are doing.") \
    val(abort_on_lsa_bad_alloc, bool, false, Used, "Abort when allocation in LSA region fails") \
    val(murmur3_partitioner_ignore_msb_bits, unsigned, 0, Used, "Number of most siginificant token bits to ignore in murmur3 partitioner; increase for very large clusters") \
    val(virtual_dirty_soft_limit, double, 0.6, Used, "Soft limit of virtual dirty memory expressed as a portion of the hard limit") \
    /* done! */

#define _make_value_member(name, type, deflt, status, desc, ...)    \
    value<type, value_status::status> name;

    _make_config_values(_make_value_member)

private:
    struct bound_value;
    struct bound_values;
    struct cmdline_args;
    struct yaml_config;

    int _dummy;
};

}
