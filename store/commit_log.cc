#include "store/commit_log.hh"
namespace store {

commit_log::commit_log()
{
}

commit_log::commit_log(bytes fn)
     : _file_name (std::move(fn))
     , _writer(make_lw_shared<log::writer>(_file_name))
{
}

commit_log::~commit_log()
{
}

future<> commit_log::append(lw_shared_ptr<partition> entry)
{
    return make_ready_future<>();
}

}
