/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#include "redis_commands.hh"
#include "redis.hh"
namespace redis {
std::vector<sstring> redis_commands::_number_str;
redis_commands::redis_commands()
{
  init_number_str_array();
  _dummy = [] (args_collection&, output_stream<char>& out) {
       return out.write("+BAD\r\n");
  };
  // INCR
  regist_handler(sstring("INCR"), [this] (args_collection& args, output_stream<char>& out) -> future<> {
     return _redis->incr(args).then([this, &out] (uint64_t r) {
       scattered_message<char> msg;
       this_type::append_item(msg, r);
       return out.write(std::move(msg));
     });
  });
  // DECR
  regist_handler(sstring("DECR"), [this] (args_collection& args, output_stream<char>& out) -> future<> {
     return _redis->decr(args).then([this, &out] (uint64_t r) {
       scattered_message<char> msg;
       this_type::append_item(msg, r);
       return out.write(std::move(msg));
     });
  });
  // INCRBY
  regist_handler(sstring("INCRBY"), [this] (args_collection& args, output_stream<char>& out) -> future<> {
     return _redis->incrby(args).then([this, &out] (uint64_t r) {
       scattered_message<char> msg;
       this_type::append_item(msg, r);
       return out.write(std::move(msg));
     });
  });
  // DECRBY
  regist_handler(sstring("DECRBY"), [this] (args_collection& args, output_stream<char>& out) -> future<> {
     return _redis->decrby(args).then([this, &out] (uint64_t r) {
       scattered_message<char> msg;
       this_type::append_item(msg, r);
       return out.write(std::move(msg));
     });
  });
  // SET
  regist_handler(sstring("SET"), [this] (args_collection& args, output_stream<char>& out) -> future<> {
     return _redis->set(args).then([this, &out] (int r) { return out.write(r == 0 ? msg_ok : msg_err); });
  });
  // GET
  regist_handler("GET", [this] (args_collection& args, output_stream<char>& out) -> future<> {
     return _redis->get(args).then([this, &out] (item_ptr it) {
       scattered_message<char> msg;
       this_type::append_item(msg, std::move(it));
        return out.write(std::move(msg));
     });
  });
  // COMMAND
  regist_handler(sstring("COMMAND"), [this] (args_collection& args, output_stream<char>& out) -> future<> {
     return out.write(msg_ok);
  });
  // DEL
  regist_handler("DEL", [this] (args_collection& args, output_stream<char>& out) -> future<> {
     return _redis->del(args).then([this, &out] (int count) {
       scattered_message<char> msg;
       this_type::append_item(msg, std::move(count));
       return out.write(std::move(msg));
     });
  });
  // EXISTS 
  regist_handler("EXISTS", [this] (args_collection& args, output_stream<char>& out) -> future<> {
     return _redis->exists(args).then([this, &out] (int count) {
       scattered_message<char> msg;
       this_type::append_item(msg, std::move(count));
       return out.write(std::move(msg));
     });
  });
  // APPEND 
  regist_handler("APPEND", [this] (args_collection& args, output_stream<char>& out) -> future<> {
     return _redis->append(args).then([this, &out] (int count) {
       scattered_message<char> msg;
       this_type::append_item(msg, std::move(count));
       return out.write(std::move(msg));
     });
  });
  // STRLEN 
  regist_handler("STRLEN", [this] (args_collection& args, output_stream<char>& out) -> future<> {
     return _redis->strlen(args).then([this, &out] (int count) {
       scattered_message<char> msg;
       this_type::append_item(msg, std::move(count));
       return out.write(std::move(msg));
     });
  });
  // LPUSH 
  regist_handler("LPUSH", [this] (args_collection& args, output_stream<char>& out) -> future<> {
     return _redis->lpush(args).then([this, &out] (int count) {
       scattered_message<char> msg;
       this_type::append_item(msg, std::move(count));
       return out.write(std::move(msg));
     });
  });
  // LPUSHX 
  regist_handler("LPUSHX", [this] (args_collection& args, output_stream<char>& out) -> future<> {
     return _redis->lpushx(args).then([this, &out] (int count) {
       scattered_message<char> msg;
       this_type::append_item(msg, std::move(count));
       return out.write(std::move(msg));
     });
  });
  // LPOP 
  regist_handler("LPOP", [this] (args_collection& args, output_stream<char>& out) -> future<> {
     return _redis->lpop(args).then([this, &out] (item_ptr item) {
       scattered_message<char> msg;
       this_type::append_item(msg, std::move(item));
       return out.write(std::move(msg));
     });
  });
  // LLEN 
  regist_handler("LLEN", [this] (args_collection& args, output_stream<char>& out) -> future<> {
     return _redis->llen(args).then([this, &out] (int count) {
       scattered_message<char> msg;
       this_type::append_item(msg, std::move(count));
       return out.write(std::move(msg));
     });
  });
  // LINDEX 
  regist_handler("LINDEX", [this] (args_collection& args, output_stream<char>& out) -> future<> {
     return _redis->lindex(args).then([this, &out] (item_ptr item) {
       scattered_message<char> msg;
       this_type::append_item(msg, std::move(item));
       return out.write(std::move(msg));
     });
  });
  // LINSERT 
  regist_handler("LINSERT", [this] (args_collection& args, output_stream<char>& out) -> future<> {
     return _redis->linsert(args).then([this, &out] (int count) {
       scattered_message<char> msg;
       this_type::append_item(msg, std::move(count));
       return out.write(std::move(msg));
     });
  });
  // LRANGE 
  regist_handler("LRANGE", [this] (args_collection& args, output_stream<char>& out) -> future<> {
       return _redis->lrange(args).then([this, &out] (std::vector<item_ptr> items) {
         scattered_message<char> msg;
         msg.append_static("*");
         msg.append_static(to_sstring(items.size()));
         msg.append_static(msg_crlf);
         for (size_t i = 0; i < items.size(); ++i) {
           msg.append_static("$");
           msg.append_static(to_sstring(items[i]->value_size()));
           msg.append_static(msg_crlf);
           msg.append_static(items[i]->value());
           msg.append_static(msg_crlf);
         }
         msg.on_delete([item = std::move(items)] {});
         return out.write(std::move(msg));
     });
  });
  // LSET 
  regist_handler("LSET", [this] (args_collection& args, output_stream<char>& out) -> future<> {
     return _redis->lset(args).then([this, &out] (int count) {
       scattered_message<char> msg;
       this_type::append_item(msg, std::move(count));
       return out.write(std::move(msg));
     });
  });
  // RPUSH 
  regist_handler("RPUSH", [this] (args_collection& args, output_stream<char>& out) -> future<> {
     return _redis->rpush(args).then([this, &out] (int count) {
       scattered_message<char> msg;
       this_type::append_item(msg, std::move(count));
       return out.write(std::move(msg));
     });
  });
  // RPUSHX 
  regist_handler("RPUSHX", [this] (args_collection& args, output_stream<char>& out) -> future<> {
     return _redis->rpushx(args).then([this, &out] (int count) {
       scattered_message<char> msg;
       this_type::append_item(msg, std::move(count));
       return out.write(std::move(msg));
     });
  });
  // RPOP 
  regist_handler("RPOP", [this] (args_collection& args, output_stream<char>& out) -> future<> {
     return _redis->rpop(args).then([this, &out] (item_ptr item) {
       scattered_message<char> msg;
       this_type::append_item(msg, std::move(item));
       return out.write(std::move(msg));
     });
  });
}

}
