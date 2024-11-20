#pragma once
#include "httplib.h"
#include "json.hpp"

namespace faas::server {

class ServerBase {
public:
    ServerBase();
    virtual ~ServerBase();

    void StartHttpServer(int port);
    void StopHttpServer();

protected:
    virtual void SetupRoutes() = 0;

    std::unique_ptr<httplib::Server> http_server_;

private:
    int port_;
};

}