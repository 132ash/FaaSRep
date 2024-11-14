#include "engine/engine.h"
#include "httplib.h"

namespace faas::engine {

LocalEngine::LocalEngine(int cache_mem_cap_mb)
    : cache_(cache_mem_cap_mb) {
        cache_mem_cap_mb_ = cache_mem_cap_mb;
    }

LocalEngine::~LocalEngine() {
    StopHttpServer();
}

void LocalEngine::StartHttpServer(int port) {
    http_server_ = std::make_unique<httplib::Server>();
    SetupRoutes();
    port_ = port;
    http_server_->listen("0.0.0.0", port);
}

void LocalEngine::StopHttpServer() {
    if (http_server_) {
        http_server_->stop();
        http_server_.reset();
    }
}

std::optional<CacheLine> LocalEngine::GetCacheLine(const std::string& key) {
    return cache_.Get(key);
}

void LocalEngine::PutCacheLine(const std::string& key, const TransactionID& version, std::span<const char> value) {
    cache_.Put(key, version, value);
}

void LocalEngine::SetupRoutes() {
    http_server_->Get("/get", [this](const httplib::Request& req, httplib::Response& res) {
        auto key = req.get_param_value("key");
        auto cache_line = GetCacheLine(key);
        if (cache_line) {
            res.set_content(cache_line->value, "text/plain");
        } else {
            res.status = 404;
        }
    });

    http_server_->Post("/put", [this](const httplib::Request& req, httplib::Response& res) {
        auto key = req.get_param_value("key");
        auto json_body = nlohmann::json::parse(req.body);
        TransactionID version = json_body["version"];
        std::string value_str = json_body["value"];
        std::span<const char> value(value_str.data(), value_str.size());
        PutCacheLine(key, version, value);
        res.status = 200;
    });
}

}