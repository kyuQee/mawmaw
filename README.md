<div align="center">

<img src="MAWMAW_logo.png" alt="MAWMAW" width="420">

**A high-performance telemetry and event processing engine.**

</div>

---    


**MAWMAW** is a high-performance, plugin-driven event processing engine written in modern C++.

It ingests events from arbitrary sources, processes them through configurable pipelines, and publishes the results to arbitrary destinations—all without modifying or recompiling the core engine.

Designed for applications where low latency, extensibility, and runtime configurability matter, MAWMAW can power telemetry systems, industrial automation, financial data pipelines, monitoring infrastructure, and other real-time event-driven applications.

**Core principles**

* Runtime-loadable plugins
* Configurable processing pipelines
* Native C++ performance
* Hot-reloadable components
* Source and destination agnostic
* Minimal core, application-specific logic lives in plugins


---    

## Guides

plugin guide: [plugin_guide.md](https://github.com/kyuQee/mawmaw/blob/master/ingestor/plugin_guide.md)
config guide: [config_guide.md](https://github.com/kyuQee/mawmaw/blob/master/config/config_guide.md)
docs: [docs.md](https://github.com/kyuQee/mawmaw/blob/master/docs/docs.md)

---    




## Cloning
```bash
git clone --recurse-submodules https://github.com/kyuQee/mawmaw.git
```

---


## Ubuntu / Debian

```bash
sudo apt update

sudo apt install -y \
    build-essential \
    clang \
    lld \
    llvm \
    cmake \
    ninja-build \
    libasan8 \
    libubsan1
```

---

## Rocky Linux / RHEL / AlmaLinux / CentOS Stream

```bash
sudo dnf install -y \
    gcc \
    gcc-c++ \
    clang \
    llvm \
    lld \
    cmake \
    ninja-build \
    libasan \
    libubsan
```

---

## Fedora

```bash
sudo dnf install -y \
    gcc \
    gcc-c++ \
    clang \
    llvm \
    lld \
    cmake \
    ninja-build \
    libasan \
    libubsan
```

---

## Arch Linux

```bash
sudo pacman -Syu --needed \
    gcc \
    clang \
    llvm \
    lld \
    cmake \
    ninja
```

(`libasan` and `libubsan` are provided with GCC.)

---

## openSUSE

```bash
sudo zypper install \
    gcc \
    gcc-c++ \
    clang \
    llvm \
    lld \
    cmake \
    ninja
```

(Sanitizer runtimes come with GCC.)

---

## Alpine Linux

```bash
sudo apk add \
    build-base \
    clang \
    llvm \
    lld \
    cmake \
    ninja
```

---

## macOS (Homebrew)

```bash
brew install \
    llvm \
    cmake \
    ninja \
    gcc
```

---

## Windows

dont use

or wsl2 (not preferred)



