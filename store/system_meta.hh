#pragma once
namespace store {
class system_meta {
public:
    system_meta();
    ~system_meta();
    future<> load_system_meta();
};
}
