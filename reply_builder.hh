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
#pragma once
#include "core/app-template.hh"
#include "core/future-util.hh"
#include "core/timer-set.hh"
#include "core/shared_ptr.hh"
#include "core/stream.hh"
#include "core/memory.hh"
#include "core/units.hh"
#include "core/distributed.hh"
#include "core/vector-data-sink.hh"
#include "core/bitops.hh"
#include "core/slab.hh"
#include "core/align.hh"
#include "net/api.hh"
#include "net/packet-data-source.hh"
#include "cache.hh"
#include "dict_lsa.hh"
#include "sset_lsa.hh"
#include "geo.hh"
#include "bytes.hh"
namespace redis {

using scattered_message_ptr = foreign_ptr<lw_shared_ptr<scattered_message<char>>>;
// The defination of `item was copied from apps/memcached
static const bytes msg_crlf {"\r\n"};
static const bytes msg_ok {"+OK\r\n"};
static const bytes msg_pong {"+PONG\r\n"};
static const bytes msg_err = {"-ERR\r\n"};
static const bytes msg_zero = {":0\r\n"};
static const bytes msg_one = {":1\r\n"};
static const bytes msg_neg_one = {":-1\r\n"};
static const bytes msg_neg_two = {":-2\r\n"};
static const bytes msg_null_blik = {"$-1\r\n"};
static const bytes msg_null_multi_bulk = {"*-1\r\n"};
static const bytes msg_empty_multi_bulk = {"*0\r\n"};
static const bytes msg_type_err = {"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"};
static const bytes msg_nokey_err = {"-ERR no such key\r\n"};
static const bytes msg_syntax_err = {"-ERR syntax error\r\n"};
static const bytes msg_same_object_err = {"-ERR source and destination objects are the same\r\n"};
static const bytes msg_out_of_range_err = {"-ERR index out of range\r\n"};
static const bytes msg_not_integer_err = {"-ERR ERR hash value is not an integer\r\n" };
static const bytes msg_not_float_err = {"-ERR ERR hash value is not an float\r\n" };
static const bytes msg_str_tag = {"+"};
static const bytes msg_num_tag = {":"};
static const bytes msg_sigle_tag = {"*"};
static const bytes msg_batch_tag = {"$"};
static const bytes msg_not_found = {"+(nil)\r\n"};
static const bytes msg_nil = {"+(nil)\r\n"};
static constexpr const int REDIS_OK = 0;
static constexpr const int REDIS_ERR = 1;
static constexpr const int REDIS_NONE = -1;
static constexpr const int REDIS_WRONG_TYPE = -2;
static const bytes msg_type_string {"+string\r\n"};
static const bytes msg_type_none {"+none\r\n"};
static const bytes msg_type_list {"+list\r\n"};
static const bytes msg_type_hll {"+hyperloglog\r\n"};
static const bytes msg_type_set {"+set\r\n"};
static const bytes msg_type_zset {"+zset\r\n"};
static const bytes msg_type_hash {"+hash\r\n"};

class reply_builder final {
static inline future<scattered_message_ptr> do_build_message(const bytes& message) {
    auto m = make_lw_shared<scattered_message<char>>();
    //m->append(message);
    return make_ready_future<scattered_message_ptr>(foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m));
}
static inline future<> do_build_message(const bytes& message, output_stream<char>& out) {
    return out.write(message);
}
public:
static inline future<scattered_message_ptr> build_type_error_message() {
    return do_build_message(msg_type_err);
}
static inline future<> build_type_error_message(output_stream<char>& out) {
    return do_build_message(msg_type_err, out);
}

static inline future<scattered_message_ptr> build_invalid_argument_message() {
    return do_build_message(msg_syntax_err);
}
static inline future<> build_invalid_argument_message(output_stream<char>& out) {
    return do_build_message(msg_syntax_err, out);
}
static future<scattered_message_ptr> build(const bytes& message)
{
   auto m = make_lw_shared<scattered_message<char>>();
 //  m->append(message);
   return make_ready_future<scattered_message_ptr>(foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m));
}
};
}
