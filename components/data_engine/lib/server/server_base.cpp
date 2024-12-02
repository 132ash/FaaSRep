#include "server/server_base.h"

namespace faas::server {

ServerBase::ServerBase() : port_(0), http_server_(std::make_unique<httplib::Server>()) {}

ServerBase::~ServerBase() {
    StopHttpServer();
}

void ServerBase::StartHttpServer(int port) {
    port_ = port;
    SetupRoutes();
    http_server_->listen("0.0.0.0", port_);
}

void ServerBase::StopHttpServer() {
    if (http_server_) {
        http_server_->stop();
        http_server_.reset();
    }
}

}