#include "redis_protocol.hh"
#include "redis.hh"
#include "base.hh"
#include <algorithm>
#include "redis_commands.hh"
namespace redis {
redis_protocol::redis_protocol(sharded_redis& redis) : _redis(redis)
{
}

void redis_protocol::prepare_request()
{
  _command_args._command_args_count = _parser._args_count - 1;
  _command_args._command = std::move(_parser._command);
  _command_args._command_args = std::move(_parser._args_list);
}

future<> redis_protocol::handle(input_stream<char>& in, output_stream<char>& out)
{
  _parser.init();
  return in.consume(_parser).then([this, &out] () -> future<> {
      switch (_parser._state) {
        case redis_protocol_parser::state::eof:
          return make_ready_future<>();

        case redis_protocol_parser::state::error:
          return out.write(msg_err);

        case redis_protocol_parser::state::ok:
        {
          prepare_request();
          if (_command_args._command_args_count <= 0 || _command_args._command_args.empty()) {
            return out.write(msg_err);
          }
          sstring& command = _command_args._command;
          std::transform(command.begin(), command.end(), command.begin(), ::toupper);
          auto command_handler = redis_commands_ptr()->get(_command_args._command);
          return command_handler( _command_args, out);
        }
      };
      std::abort();
  }).then_wrapped([this, &out] (auto&& f) -> future<> {
        try {
          f.get();
        } catch (std::bad_alloc& e) {
          return out.write(msg_err);
        }
        return make_ready_future<>();
  });
}
}
