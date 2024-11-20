#pragma once
#include "base/common.h"
#include "common/transaction.h"

namespace tkrzw { class CacheDBM; }

namespace faas::engine {

class ValueCache {
public:
    explicit ValueCache(int mem_cap_mb);
    ~ValueCache();

    std::optional<CacheLine> Get(const std::string& key);
    void Put(const std::string& key, const TransactionID& version, std::string_view value);



private:
    std::unique_ptr<tkrzw::CacheDBM> cache_;
};

}
