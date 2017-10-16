#include "store/commit_log.hh"
namespace store {

future<> commit_log::append(lw_shared_ptr<partition> entry)
{
    return make_ready_future<>();
}

}
