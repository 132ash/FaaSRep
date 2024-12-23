#include "httplib.h"
#include "json.hpp"
#include <iostream>

int main() {
    httplib::Client client("localhost", 8080);

    // 测试 PUT 请求
    nlohmann::json put_data;
    put_data["version"] = 10011001;
    put_data["value"] = "test_value";

    auto put_res = client.Post("/put?key=test_key", put_data.dump(), "application/json");
    if (put_res && put_res->status == 200) {
        std::cout << "PUT request successful" << std::endl;
    } else {
        std::cerr << "PUT request failed" << std::endl;
        return 1;
    }

    // 测试 GET 请求
    auto get_res = client.Get("/get?key=test_key");
    if (get_res && get_res->status == 200) {
        std::cout << "GET request successful: " << get_res->body << std::endl;
    } else {
        std::cerr << "GET request failed" << std::endl;
        return 1;
    }

    return 0;
}