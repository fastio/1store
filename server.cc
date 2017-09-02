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
#include "server.hh"
namespace redis {

distributed<server> _the_server;

void server::setup_metrics()
{
    namespace sm = seastar::metrics;
    _metrics.add_group("connections", {
        sm::make_counter("opened_total", [this] { return _stats._connections_total; }, sm::description("Total number of connections opened.")),
        sm::make_counter("current_total", [this] { return _stats._connections_current; }, sm::description("Total number of connections current opened.")),
    });
}
}
