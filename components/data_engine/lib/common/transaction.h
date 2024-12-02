#pragma once

#include "base/common.h"

namespace faas::engine {

#define TransactionID int64_t

// struct TransactionID {
//     int32_t commit_timestamp;
//     int32_t txID;
// };

struct CacheLine {
    TransactionID version;
    std::string value;
};

}



