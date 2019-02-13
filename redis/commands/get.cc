#include "redis/commands/get.hh"
#include "redis/commands/unexpected.hh"
#include "redis/reply_builder.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "timeout_config.hh"
namespace redis {
namespace commands {
shared_ptr<abstract_command> get::prepare(request&& req)
{
    if (req._args_count < 1) {
        return unexpected::prepare(std::move(req._command), std::move(bytes { msg_syntax_err }) );
    }
    return make_shared<get>(std::move(req._command), std::move(req._args[0]));
}

future<reply> get::execute(service::storage_proxy&, db::consistency_level cl, db::timeout_clock::time_point timeout, const timeout_config& tc, tracing::trace_state_ptr trace_state)
{
    return make_ready_future<reply>();
}
}
}
