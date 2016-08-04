#ifndef _REDIS_PROTOCOL_HH
#define _REDIS_PROTOCOL_HH
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <iomanip>
#include <sstream>
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
#include "src/redis_protocol_parser.hh"
#include <unistd.h>
#include <cstdlib>
#include "base.hh"
namespace redis {
class redis_commands;
class sharded_redis;
using item_ptr = foreign_ptr<boost::intrusive_ptr<item>>;
class redis_protocol {
private:
    sharded_redis& _redis;
    redis_protocol_parser _parser;
    args_collection _command_args;
public:
    redis_protocol(sharded_redis& redis);
    void prepare_request();
    future<> handle(input_stream<char>& in, output_stream<char>& out);
};
}
#endif
