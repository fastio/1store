#include "redis/commands/set.hh"
#include "redis/commands/unexpected.hh"
#include "seastar/core/shared_ptr.hh"
#include "redis/reply_builder.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
namespace redis {
namespace commands {
shared_ptr<abstract_command> set::prepare(request&& req)
{
    if (req._args_count < 2) {
        return unexpected::prepare(std::move(req._command), std::move(bytes {msg_syntax_err}));
    }
    else if (req._args_count > 2) {
        // FIXME: more other options
    }
    return make_shared<set> (std::move(req._command), std::move(req._args[0]), std::move(req._args[1]));
}

future<reply> set::execute()
{
    return make_ready_future<reply>();
}
}
}
