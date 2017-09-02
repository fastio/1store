/*
 * Copyright (C) 2015 ScyllaDB
 * Modified by Peng Jian, pstack@163.com
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */
#pragma once

#include <core/sstring.hh>
#include <core/future.hh>
#include <core/distributed.hh>
#include <unordered_map>
#include "config.hh"
#include "utils/log.hh"
#include "utils/file_lock.hh"
#include "utils/disk-error-handler.hh"

namespace bpo = boost::program_options;
extern logging::logger startlog;

class init_utils {
public:
// Note: would be neat if something like this was in config::string_map directly
// but that cruds up the YAML/boost integration so until I want to spend hairpulling
// time with that, this is an acceptable helper
template<typename K, typename V, typename KK, typename VV = V>
static V get_or_default(const std::unordered_map<K, V>& src, const KK& key, const VV& def = V()) {
    auto i = src.find(key);
    if (i != src.end()) {
        return i->second;
    }
    return def;
}

static boost::filesystem::path relative_conf_dir(boost::filesystem::path path) {
    static auto conf_dir = redis::config::get_conf_dir(); // this is not gonna change in our life time
    return conf_dir / path;
}

static future<> read_config(bpo::variables_map& opts, redis::config& cfg) {
    using namespace boost::filesystem;
    sstring file;

    if (opts.count("options-file") > 0) {
        file = opts["options-file"].as<sstring>();
    } else {
        file = relative_conf_dir("redis.yaml").string();
    }
    return check_direct_io_support(file).then([file, &cfg] {
        return cfg.read_from_file(file);
    }).handle_exception([file](auto ep) {
        startlog.error("Could not read configuration file {}: {}", file, ep);
        return make_exception_future<>(ep);
    });
}

static void do_help_loggers() {
    print("Available loggers:\n");
    for (auto&& name : logging::logger_registry().get_all_logger_names()) {
        print("    %s\n", name);
    }
}

static logging::log_level to_loglevel(sstring level) {
    try {
        return boost::lexical_cast<logging::log_level>(std::string(level));
    } catch(boost::bad_lexical_cast e) {
        throw std::runtime_error("Unknown log level '" + level + "'");
    }
}

static void apply_logger_settings(sstring default_level, redis::config::string_map levels,
        bool log_to_stdout, bool log_to_syslog) {
    logging::logger_registry().set_all_loggers_level(to_loglevel(default_level));
    for (auto&& kv: levels) {
        auto&& k = kv.first;
        auto&& v = kv.second;
        try {
            logging::logger_registry().set_logger_level(k, to_loglevel(v));
        } catch(std::out_of_range e) {
            throw std::runtime_error("Unknown logger '" + k + "'. Use --help-loggers to list available loggers.");
        }
    }
    logging::logger::set_stdout_enabled(log_to_stdout);
    logging::logger::set_syslog_enabled(log_to_syslog);
}

static void tcp_syncookies_sanity() {
    try {
        auto f = file_desc::open("/proc/sys/net/ipv4/tcp_syncookies", O_RDONLY | O_CLOEXEC);
        char buf[128] = {};
        f.read(buf, 128);
        if (sstring(buf) == "0\n") {
            startlog.warn("sysctl entry net.ipv4.tcp_syncookies is set to 0.\n"
                    "For better performance, set following parameter on sysctl is strongly recommended:\n"
                    "net.ipv4.tcp_syncookies=1");
        }
    } catch (const std::system_error& e) {
        startlog.warn("Unable to check if net.ipv4.tcp_syncookies is set {}", e);
    }
}
};

class directories {
public:
    future<> touch_and_lock(sstring path) {
        return io_check(recursive_touch_directory, path).then_wrapped([this, path] (future<> f) {
            try {
                f.get();
                return utils::file_lock::acquire(path + "/.lock").then([this](utils::file_lock lock) {
                   _locks.emplace_back(std::move(lock));
                }).handle_exception([path](auto ep) {
                    // only do this because "normal" unhandled exception exit in seastar
                    // _drops_ system_error message ("what()") and thus does not quite deliver
                    // the relevant info to the user
                    try {
                        std::rethrow_exception(ep);
                    } catch (std::exception& e) {
                        startlog.error("Could not initialize {}: {}", path, e.what());
                        throw;
                    } catch (...) {
                        throw;
                    }
                });
            } catch (...) {
                startlog.error("Directory '{}' cannot be initialized. Tried to do it but failed with: {}", path, std::current_exception());
                throw;
            }
        });
    }
    template<typename _Iter>
    future<> touch_and_lock(_Iter i, _Iter e) {
        return parallel_for_each(i, e, [this](sstring path) {
           return touch_and_lock(std::move(path));
        });
    }
    template<typename _Range>
    future<> touch_and_lock(_Range&& r) {
        return touch_and_lock(std::begin(r), std::end(r));
    }
private:
    std::vector<utils::file_lock>
        _locks;
};

class bad_configuration_error : public std::exception {};

future<> init_message_failuredetector_gossiper(sstring listen_address
                , uint16_t storage_port
                , uint16_t ssl_storage_port
                , sstring ms_encrypt_what
                , sstring ms_trust_store
                , sstring ms_cert
                , sstring ms_key
                , sstring ms_compress
                , sstring seeds 
                , sstring cluster_name = "Test Cluster"
                , double phi = 8
                , bool sltba = false);
