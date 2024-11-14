#include <iostream>
#include <string>
#include "tkrzw_dbm_cache.h"
#include "httplib.h"

using namespace tkrzw;
using namespace httplib;

namespace faas {

class CacheEngine {
public:
    CacheEngine() {
        // dbm_ = std::make_unique<CacheDBM>();
        // dbm_->Open("", true, "cap_rec_num=1000");
    }

    std::string Get(const std::string& key) {
        // std::string value;
        // auto status = dbm_->Get(key, &value);
        // if (status == Status::SUCCESS) {
        //     return value;
        // } else {
        //     return "Key not found";
        // }
        return "Key not found";
    }

    void Update(const std::string& key, const std::string& value) {
        // dbm_->Set(key, value);
    }

private:
    std::unique_ptr<CacheDBM> dbm_;
};
}

int main() {
    CacheEngine cache_engine;

    Server svr;

    svr.Get("/get", [&](const Request& req, Response& res) {
        auto key = req.get_param_value("key");
        auto value = cache_engine.Get(key);
        res.set_content(value, "text/plain");
    });

    svr.Post("/update", [&](const Request& req, Response& res) {
        auto key = req.get_param_value("key");
        auto value = req.get_param_value("value");
        cache_engine.Update(key, value);
        res.set_content("Updated", "text/plain");
    });

    std::cout << "Server is running on http://localhost:8080" << std::endl;
    svr.listen("localhost", 8080);

    return 0;
}