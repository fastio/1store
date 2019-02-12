#include "redis/commands/get.hh"
#include "redis/commands/unexpected.hh"
#include "redis/reply_builder.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
namespace redis {
namespace commands {
shared_ptr<abstract_command> get::prepare(request&& req)
{
    if (req._args_count < 1) {
        return unexpected::prepare(std::move(req._command), std::move(bytes { msg_syntax_err }) );
    }
    return make_shared<get>(std::move(req._command), std::move(req._args[0]));
}

future<reply> get::execute()
{
    return make_ready_future<reply>();
}
}
}
