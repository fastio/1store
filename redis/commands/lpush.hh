#pragma once
#include "redis/command_with_single_schema.hh"
#include "redis/request.hh"
#include <vector>

namespace service {
class storage_proxy;
}
class timeout_config;
namespace redis {
namespace commands {
class lpush : public command_with_single_schema {
protected:
    bytes _key;
    std::vector<bytes> _datas;
public:
     class precision_time {
        public:
            // Our reference time (1 jan 2010, 00:00:00) in milliseconds.
            static constexpr db_clock::time_point REFERENCE_TIME{std::chrono::milliseconds(1262304000000)};
        private:
            static thread_local precision_time _last;
        public:
            db_clock::time_point _millis;
            int32_t _nanos;

            static precision_time get_next(db_clock::time_point millis);
     };  
                         
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, request&& req);
    lpush(bytes&& name, const schema_ptr schema, bytes&& key, std::vector<bytes>&& datas) 
        : command_with_single_schema(std::move(name), schema)
        , _key(std::move(key))
        , _datas(std::move(datas))
    {
    }
    ~lpush() {}
    future<reply> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};
}
}
