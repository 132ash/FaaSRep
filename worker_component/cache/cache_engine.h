#pragma once
#include "base/common.h"
#include "common/transaction.h"
#include "server/server_base.h"
#include "cache/cache.h"
#include "httplib.h"
#include "json.hpp"


namespace faas::engine {

class CacheEngine : public server::ServerBase{
public:
    CacheEngine(int cache_mem_cap_mb);
    ~CacheEngine();

    std::optional<CacheLine> GetCacheLine(const std::string& key);
    void PutCacheLine(const std::string& key, const TransactionID& version, std::string_view value);


protected:
    void SetupRoutes() override;
    int FetchCacheLine(const std::string& key, CacheLine* cache_line);

private:
    int cache_mem_cap_mb_;
    ValueCache cache_;
};

}