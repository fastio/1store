#pragma once
namespace redis {
class ring {
private:
public:
    ring()
    {
    }
    future<> start();
    future<> stop();
};
}
