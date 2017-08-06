/*
* Pedis is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* You may obtain a copy of the License at
*
*     http://www.gnu.org/licenses
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
*  Copyright (c) 2016-2026, Peng Jian, pstack@163.com. All rights reserved.
*
*/
#include "redis.hh"
#include "db.hh"
#include "redis.hh"
#include "redis_protocol.hh"
#include "server.hh"
#include "util/log.hh"
#include "core/prometheus.hh"
#include "token_ring_manager.hh"
#include "storage_proxy.hh"
#include "storage_service.hh"
#include "init.hh"
#define PLATFORM "seastar"
#define VERSION "v1.0"
#define VERSION_STRING PLATFORM " " VERSION

using logger =  seastar::logger;
static logger main_log ("main");


int main(int ac, char** av) {
    int return_value = 0;
    prometheus::config prometheus_config;
    httpd::http_server_control prometheus_server;
    namespace bpo = boost::program_options;
    app_template app;
    auto opt_add = app.add_options();
    auto cfg = make_lw_shared<redis::config>();
    bool help_loggers = false;
    bool help_version = false;

    cfg->add_options(opt_add)
    ("options-file", bpo::value<sstring>(), "configuration file (i.e. <PEDIS_HOME>/etc/redis.yaml)")
    ("help-loggers", bpo::bool_switch(&help_loggers), "print a list of logger names and exit")
    ("version", bpo::bool_switch(&help_version), "print version number and exit")
    ;

    try {
        return app.run_deprecated(ac, av, [&] {
            return seastar::async([ac, av, cfg, &app, &prometheus_config, &prometheus_server, &return_value] () {
                auto& db = redis::get_database();
                auto& server = redis::get_server();
                auto& redis = redis::get_redis_service();
                auto& token_ring = redis::ring();
                auto& proxy = redis::get_storage_proxy();
                auto& ss = redis::get_storage_service();

                engine().at_exit([&] { return db.stop(); });
                engine().at_exit([&] { return server.stop(); });
                engine().at_exit([&] { return redis.stop(); });
                engine().at_exit([&] { return token_ring.stop(); });
                engine().at_exit([&] { return proxy.stop(); });
                engine().at_exit([&] { return ss.stop(); });
                engine().at_exit([&] { return prometheus_server.stop(); });

                auto&& config = app.configuration();
                auto port = config["port"].as<uint16_t>();
                auto pport = config["prometheus_port"].as<uint16_t>();
                // start databse
                db.start().get();

                // start gossper
                sstring listen_address = cfg->listen_address();
                uint16_t storage_port = cfg->storage_port();
                uint16_t ssl_storage_port = cfg->ssl_storage_port();
                double phi = cfg->phi_convict_threshold();
                auto seed_provider= cfg->seed_provider();
                sstring cluster_name = cfg->cluster_name();

                const auto& ssl_opts = cfg->server_encryption_options();
                auto encrypt_what = init_utils::get_or_default(ssl_opts, "internode_encryption", "none");
                auto trust_store = init_utils::get_or_default(ssl_opts, "truststore");
                auto cert = init_utils::get_or_default(ssl_opts, "certificate", init_utils::relative_conf_dir("redis.crt").string());
                auto key = init_utils::get_or_default(ssl_opts, "keyfile", init_utils::relative_conf_dir("redis.key").string());
                init_message_failuredetector_gossiper(
                        listen_address
                        , storage_port
                        , ssl_storage_port
                        , encrypt_what
                        , trust_store
                        , cert
                        , key
                        , cfg->internode_compression()
                        , seed_provider
                        , cluster_name
                        , phi
                        , cfg->listen_on_broadcast_address()
                ).get();

                server.start(port).get();
                server.invoke_on_all(&redis::server::start).get();
                token_ring.start().get();
                proxy.start().get();
                ss.start().get();
                prometheus_config.metric_help = "Redis server statistics";
                prometheus_config.prefix = "redis";
                prometheus_server.start("prometheus").get();
                prometheus::start(prometheus_server, prometheus_config).get();
                listen_options lo;
                lo.reuse_address = true;
                prometheus_server.listen(make_ipv4_address({pport})).get();
                main_log.info("Parallel Redis ... [{}]", port);
            }).then_wrapped([&return_value] (auto && f) {
                try {
                   f.get();
                } catch (...) {
                   return_value = 1;
                }
                return return_value;
            });
        });
    } catch (...) {
        return return_value;
    }
}
