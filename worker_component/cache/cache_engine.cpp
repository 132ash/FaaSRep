#include "cache/cache_engine.h"
#include "httplib.h"

namespace faas::engine {

CacheEngine::CacheEngine(int cache_mem_cap_mb)
    : cache_(cache_mem_cap_mb) {
        cache_mem_cap_mb_ = cache_mem_cap_mb;
    }

CacheEngine::~CacheEngine() {
    StopHttpServer();
}

std::optional<CacheLine> CacheEngine::GetCacheLine(const std::string& key) {
    return cache_.Get(key);
}

void CacheEngine::PutCacheLine(const std::string& key, const TransactionID& version, std::string_view value) {
    cache_.Put(key, version, value);
}

void CacheEngine::SetupRoutes() {
    http_server_->Get("/get", [this](const httplib::Request& req, httplib::Response& res) {
        auto key = req.get_param_value("key");
        auto cache_line = GetCacheLine(key);
        if (cache_line) {
            res.set_content(std::to_string(cache_line->version)+cache_line->value, "text/plain");
        } else {
            res.status = 404;
        }
    });

    http_server_->Post("/put", [this](const httplib::Request& req, httplib::Response& res) {
        auto key = req.get_param_value("key");
        auto json_body = nlohmann::json::parse(req.body);
        TransactionID version = json_body["version"];
        std::string value_str = json_body["value"];
        std::string_view value(value_str);
        PutCacheLine(key, version, value);
        res.status = 200;
    });
}

}