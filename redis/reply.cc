#include "reply.hh"
namespace redis {
future<redis_message> redis_message::make_list_bytes(map_return_type r, size_t begin, size_t end) {
    auto m = make_lw_shared<scattered_message<char>> ();
    m->append(sstring(sprint("*%d\r\n", end - begin + 1)));
    auto& data = r->data();
    for (size_t i = 0; i < data.size(); ++i) {
        if (i >= begin && i <= end) {
            write_bytes(m, *(data[i].first));
        }
    }
    m->on_delete([ r = std::move(foreign_ptr { r }) ] {});
    return make_ready_future<redis_message>(m);
}

future<redis_message> redis_message::make_list_bytes(map_return_type r, size_t index) {
    assert(r->has_data());
    assert(r->data().size() > 0);
    assert(index >= 0 && index < r->data().size());

    auto m = make_lw_shared<scattered_message<char>> ();
    auto& data = *(r->data()[index].second);

    write_bytes(m, data);
    m->on_delete([ r = std::move(foreign_ptr { r }) ] {});

    return make_ready_future<redis_message>(m);
}

future<redis_message> redis_message::make_map_key_bytes(map_return_type r) {
    auto m = make_lw_shared<scattered_message<char>> ();
    auto& data = r->data();
    m->append(sstring(sprint("*%d\r\n", data.size())));
    for (size_t i = 0; i < data.size(); ++i) {
        write_bytes(m, *(data[i].first));
    }
    m->on_delete([ r = std::move(foreign_ptr { r }) ] {});
    return make_ready_future<redis_message>(m);
}

future<redis_message> redis_message::make_map_val_bytes(map_return_type r) {
    auto m = make_lw_shared<scattered_message<char>> ();
    auto& data = r->data();
    m->append(sstring(sprint("*%d\r\n", data.size())));
    for (size_t i = 0; i < data.size(); ++i) {
        write_bytes(m, *(data[i].second));
    }
    m->on_delete([ r = std::move(foreign_ptr { r }) ] {});
    return make_ready_future<redis_message>(m);
}

future<redis_message> redis_message::make_map_bytes(map_return_type r) {
    auto m = make_lw_shared<scattered_message<char>> ();
    auto& data = r->data();
    m->append(sstring(sprint("*%d\r\n", 2 * data.size())));
    for (size_t i = 0; i < data.size(); ++i) {
        write_bytes(m, *(data[i].first));
        write_bytes(m, *(data[i].second));
    }
    m->on_delete([ r = std::move(foreign_ptr { r }) ] {});
    return make_ready_future<redis_message>(m);
}

future<redis_message> redis_message::make_set_bytes(map_return_type r, size_t index) {
    return make_set_bytes(r, std::vector<size_t> { index }); 
}

future<redis_message> redis_message::make_set_bytes(map_return_type r, std::vector<size_t> indexes) {
    auto m = make_lw_shared<scattered_message<char>> ();
    auto& data = r->data();

    m->append(sstring(sprint("*%d\r\n", indexes.size())));
    for (auto index : indexes) {
        assert(index >= 0 && index < data.size());
        write_bytes(m, *(data[index].first));
    }
    m->on_delete([ r = std::move(foreign_ptr { r }) ] {});
    return make_ready_future<redis_message>(m);
}

future<redis_message> redis_message::make_zset_bytes(lw_shared_ptr<std::vector<std::optional<bytes>>> r) {
    auto m = make_lw_shared<scattered_message<char>> ();
    m->append(sstring(sprint("*%d\r\n", r->size())));
    for (auto& e : *r) {
        write_bytes(m, *e);
    }
    m->on_delete([ r = std::move(foreign_ptr { r }) ] {});
    return make_ready_future<redis_message>(m);
}
future<redis_message> redis_message::make_mbytes(mbytes_return_type r) {
    auto m = make_lw_shared<scattered_message<char>> ();
    auto& data = r->data();
    m->append(sstring(sprint("*%d\r\n", data.size())));
    for (auto e : data) {
        write_bytes(m, e.second);
    }
    m->on_delete([ r = std::move(foreign_ptr { r }) ] {});
    return make_ready_future<redis_message>(m);
}
}
