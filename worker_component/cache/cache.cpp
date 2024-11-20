#include "cache/cache.h"
#include "tkrzw_dbm_cache.h"


namespace faas::engine {
    
    ValueCache::ValueCache(int mem_cap_mb){
        int64_t cap_mem_size = -1;
        if (mem_cap_mb > 0) {
            cap_mem_size = int64_t{mem_cap_mb} << 20;
        }
        cache_.reset(new tkrzw::CacheDBM(/* cap_rec_num= */ -1, cap_mem_size));
    }

    ValueCache::~ValueCache() {}

namespace {
    // cacheline is composed of version and value, cache format is string
    static inline std::string EncodeCacheLine(const TransactionID& version, std::string_view value){
        size_t total_size = value.size() + sizeof(TransactionID);
        std::string encoded;
        encoded.resize(total_size);
        char* ptr = encoded.data();
        memcpy(ptr, &version, sizeof(TransactionID));
        ptr += sizeof(TransactionID);
        memcpy(ptr, value.data(), value.size());
        return encoded;
    }

    static inline void DecodeCacheLine(std::string encoded, CacheLine* cache_line){
        const char* ptr = encoded.data();
        memcpy(&cache_line->version, ptr, sizeof(TransactionID));
        ptr += sizeof(TransactionID);
        cache_line->value = std::string(ptr, encoded.size() - sizeof(TransactionID));
    }
}

    std::optional<CacheLine> ValueCache::Get(const std::string& key){
        std::string encoded;
        tkrzw::Status status = cache_->Get(key, &encoded);
        if (status.IsOK()){
            CacheLine cache_line;
            DecodeCacheLine(encoded, &cache_line);
            return cache_line;
        } else {
            return std::nullopt;
        }
    }

    void ValueCache::Put(const std::string& key, const TransactionID& version, std::string_view value) {
        std::string encoded_value = EncodeCacheLine(version, value);
        cache_->Set(key, encoded_value);
    }


}