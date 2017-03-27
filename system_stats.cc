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
#include "system_stats.hh"
#include "http/function_handlers.hh"
#include "core/scollectd.hh"
#include "core/scollectd.hh"
#include "core/scollectd_api.hh"
namespace redis {
future<> metric_server::start(uint16_t port)
{
    return _httpd.start().then([this] {
        return _httpd.set_routes([this](httpd::routes& r) {
            httpd::future_handler_function f = [](std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
                return do_with(std::vector<scollectd::value_map>(), [rep = std::move(rep)] (auto& vec) mutable {
                    vec.resize(smp::count);
                    return parallel_for_each(boost::irange(0u, smp::count), [&vec] (auto cpu) {
                        return smp::submit_to(cpu, [] {
                            return scollectd::get_value_map();
                        }).then([&vec, cpu] (auto res) {
                            vec[cpu] = res;
                        });
                    }).then([rep = std::move(rep), &vec]() mutable {
                        // encode metric data to rep
                        rep->_content += "{\"message\": \"ok\"}";
                        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                    });
                });
            };
            r.put(GET, "/metrics", new httpd::function_handler(f, "proto"));
        });
    }).then([this, port] {
        return _httpd.listen(port);
    });
}

future<> metric_server::stop()
{
    return _httpd.stop();
}
}
