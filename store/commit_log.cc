#include "store/commit_log.hh"
#include "store/checked_file_impl.hh"
namespace store {

commit_log::commit_log()
    : _file_name()
    , _writer(nullptr)
{
}

commit_log::commit_log(sstring fn)
    : _file_name (std::move(fn))
    , _writer(nullptr)
{
}

future<> commit_log::initialize()
{
    size_t max_size = 32 * 1024 * 1024;
    file_open_options opt;
    opt.extent_allocation_size_hint = max_size;
    return open_checked_file_dma(commit_error_handler, _file_name, open_flags::wo | open_flags::create, opt).then([this, max_size](file f) {
        // xfs doesn't like files extended betond eof, so enlarge the file
        return f.truncate(max_size).then([this, f] () mutable {
            _writer = make_lw_shared<store::log::writer>(std::move(f));
            return make_ready_future<>();
        });
    });
}

future<> commit_log::append(lw_shared_ptr<partition> entry)
{
    if (!_writer) {
        return initialize().then([this, entry = std::move(entry)] {
            return this->append(entry);
        });
    }
    assert(_writer);
    return do_with(entry->serialize(), [this] (auto& data) {
        bytes_view bv { data };
        return _writer->append(bv);
    });
}

lw_shared_ptr<commit_log> make_commit_log(sstring file_name)
{
    return make_lw_shared<commit_log>(std::move(file_name));
}
}
