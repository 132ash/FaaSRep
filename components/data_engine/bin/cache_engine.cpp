#include "cache/cache_engine.h"
#include <absl/flags/parse.h>
#include <absl/flags/flag.h>

ABSL_FLAG(int, memSize, 100, "Memory size for the cache in MB");
ABSL_FLAG(int, port, 8080, "Port number for cache server");

namespace faas {

void CacheEngineMain(int argc, char* argv[]) {
    absl::ParseCommandLine(argc, argv);

    int mem_size = absl::GetFlag(FLAGS_memSize);
    int port = absl::GetFlag(FLAGS_port);
    std::cout << "Cache engine started on port " << port << " with mem size " << mem_size << std::endl;

    engine::CacheEngine engine(mem_size);
    engine.StartHttpServer(port);
    while (true) {
        usleep(1000000);
    }
}
}

int main(int argc, char* argv[]) {
    faas::CacheEngineMain(argc, argv);
    return 0;
}