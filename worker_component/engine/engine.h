#pragma once
#include "engine/common.h"
#include "engine/cache.h"
#include "httplib.h"
#include "json.hpp"


namespace faas::engine {

class LocalEngine {
public:
    LocalEngine(int cache_mem_cap_mb);
    ~LocalEngine();

    void StartHttpServer(int port);
    void StopHttpServer();

    std::optional<CacheLine> GetCacheLine(const std::string& key);
    void PutCacheLine(const std::string& key, const TransactionID& version, std::span<const char> value);

private:
    int cache_mem_cap_mb_;
    int port_;
    ValueCache cache_;
    std::unique_ptr<httplib::Server> http_server_;

    void SetupRoutes();

    DISALLOW_COPY_AND_ASSIGN(LocalEngine);
};

}