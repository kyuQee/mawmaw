#pragma once
#include "ingestor/i_ingestor.hpp"
#include <dlfcn.h>
#include <memory>
#include <stdexcept>
#include <string>

namespace mawmaw::ingestor {

// Owns one loaded plugin .so and the IIngestor instance inside it.
class PluginHandle {
public:
    using CreateFn  = IIngestor*(*)();
    using DestroyFn = void(*)(IIngestor*);

    static PluginHandle load(const std::string& path) {
        void* handle = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (!handle)
            throw std::runtime_error("dlopen failed: " + std::string(dlerror()));

        auto create  = reinterpret_cast<CreateFn> (dlsym(handle, "mawmaw_create"));
        auto destroy = reinterpret_cast<DestroyFn>(dlsym(handle, "mawmaw_destroy"));

        if (!create || !destroy) {
            dlclose(handle);
            throw std::runtime_error("Plugin missing required symbols: " + path);
        }
        return PluginHandle(handle, create(), destroy);
    }

    PluginHandle(const PluginHandle&) = delete;
    PluginHandle& operator=(const PluginHandle&) = delete;

    PluginHandle(PluginHandle&& o) noexcept
        : dl_handle_(o.dl_handle_), instance_(o.instance_), destroy_(o.destroy_)
    { o.dl_handle_ = nullptr; o.instance_ = nullptr; }

    ~PluginHandle() {
        if (instance_ && destroy_) { instance_->stop(); destroy_(instance_); }
        if (dl_handle_) dlclose(dl_handle_);
    }

    IIngestor* operator->() { return instance_; }
    IIngestor& operator*()  { return *instance_; }

private:
    PluginHandle(void* handle, IIngestor* inst, DestroyFn destroy)
        : dl_handle_(handle), instance_(inst), destroy_(destroy) {}

    void* dl_handle_ = nullptr;
    IIngestor* instance_  = nullptr;
    DestroyFn  destroy_   = nullptr;
};

} // namespace mawmaw::ingestor
