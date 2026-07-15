# MAWMAW
---

## Cloning
```bash
git clone --recurse-submodules https://github.com/yourname/mawmaw.git
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

or use Visual Studio Build Tools with the **Desktop development with C++** workload.
