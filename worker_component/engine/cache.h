#pragma once
#include "engine/common.h"

namespace tkrzw { class CacheDBM; }

namespace faas::engine {

class ValueCache {
public:
    explicit ValueCache(int mem_cap_mb);
    ~ValueCache();

    std::optional<CacheLine> Get(const std::string& key);
    void Put(const std::string& key, const TransactionID& version, std::span<const char> value);

private:
    std::unique_ptr<tkrzw::CacheDBM> cache_;
    DISALLOW_COPY_AND_ASSIGN(ValueCache);
};

}
