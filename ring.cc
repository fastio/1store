#include "ring.hh"
namespace redis {
future<> ring::start()
{
    return make_ready_future<>();
}

future<> ring::stop()
{
    return make_ready_future<>();
}
}
