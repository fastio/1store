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
*  Copyright (c) 2016-2026, Peng Jian, pengjian.uest@gmail.com. All rights reserved.
*
*/
#include "server.hh"
#include "core/execution_stage.hh"
namespace redis {

// FIXME: handle should catch all exceptions?
static thread_local auto reqeust_process_stage = seastar::make_execution_stage("request_proccessor", &redis_protocol::handle);

void server::setup_metrics()
{
    namespace sm = seastar::metrics;
    _metrics.add_group("connections", {
        sm::make_counter("opened_total", [this] { return _stats._connections_total; }, sm::description("Total number of connections opened.")),
        sm::make_counter("current_total", [this] { return _stats._connections_current; }, sm::description("Total number of connections current opened.")),
    });

    _metrics.add_group("reqests", {
        sm::make_counter("served_total", [this] { return 0; }, sm::description("Total number of served requests.")),
        sm::make_counter("serving_total", [this] { return 0; }, sm::description("Total number of requests being serving.")),
        sm::make_counter("exception_total", [this] { return 0; }, sm::description("Total number of bad requests.")),
    });
}

void server::start()
{
    listen_options lo;
    lo.reuse_address = true;
    _listener = engine().listen(make_ipv4_address({_port}), lo);
    keep_doing([this] {
       return _listener->accept().then([this] (connected_socket fd, socket_address addr) mutable {
           ++_stats._connections_total;
           ++_stats._connections_current;
           auto conn = make_lw_shared<connection>(std::move(fd), addr, _redis, _use_native_parser);
           do_until([conn] { return conn->_in.eof(); }, [this, conn] {
               return reqeust_process_stage(&conn->_proto, seastar::ref(conn->_in), seastar::ref(conn->_out)).then([this, conn] {
                   return conn->_out.flush();
               });
           }).finally([this, conn] {
               --_stats._connections_current;
               return conn->_out.close().finally([conn]{});
           });
       });
   }).or_terminate();
}
}
