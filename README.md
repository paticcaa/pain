# Pain

[![Linux](https://github.com/paticcaa/pain/actions/workflows/ubuntu.yml/badge.svg)](https://github.com/paticcaa/pain/actions/workflows/ubuntu.yml)
[![Coverage Status](https://coveralls.io/repos/github/paticcaa/pain/badge.svg)](https://coveralls.io/github/paticcaa/pain)

A high-performance distributed storage system designed for scalability and reliability.

## System Requirements

### Prerequisites

The following tools and dependencies are required for building and running Pain:

- **bazel(>=8.3)** - Build system
- **Python 3** - Script execution and automation
- **Ansible** - Deployment automation
- **Docker** - Container runtime

### Installation Instructions

```bash
# Install bazel
# Please refer to https://bazel.build/install

# Install Ansible automation tool
sudo apt install ansible

# Install Prometheus monitoring roles
ansible-galaxy collection install prometheus.prometheus

# Install development tools and system packages

## basic development tools
sudo apt install -y p7zip clang-format-16 ccache clang-tidy patchelf meson nasm autoconf automake libtool pkg-config

## Python related
sudo apt install -y python3 python3-pip python3-dev python3-pyelftools

## system tool
sudo apt install -y unzip openssh-server

## development library
sudo apt install -y libcunit1-dev libaio-dev libssl-dev libjson-c-dev libcmocka-dev uuid-dev libiscsi-dev liblz4-dev libkeyutils-dev libncurses5-dev libncursesw5-dev libfuse3-dev libibverbs-dev librdmacm-dev libelf-dev libnuma-dev

## create clang-format symbol link
sudo ln -sf /usr/bin/clang-format-16 /usr/bin/clang-format
```

## Build and Development

### Environment Variable

```bash
export CC=$(which gcc)
export CXX=$(which g++)
```

### Core Build Commands

```bash
# set build cache dir
# /mnt/bazel_cache is default path
export PAIN_BAZEL_CACHE_DIR=/mnt/bazel_cache

# Build the project
./z.py build    # or ./z.py b

# Run test suite
./z.py test     # or ./z.py t

# Install the system
./z.py install  # or ./z.py i

# Deploy Pain to ~/deployment directory
./z.py deploy -a start
```

### Additional Development Commands

```bash
# Display available build targets
./z.py show     # or ./z.py s

# Build specific target (e.g., sad component)
./z.py b sad

# Execute specific test suite
./z.py test test_manusya    # or ./z.py t test_manusya

# Run complete test suite
./z.py t

# Format source code (recommended before submitting pull requests)
./z.py format   # or ./z.py f

# Perform code quality analysis
./z.py lint     # or ./z.py l
```

For comprehensive command information, execute `./z.py -h`.

## Deployment

Pain provides an automated Ansible-based deployment solution for establishing minimal cluster configurations.

### Deployment Prerequisites

```bash
# Configure SSH key authentication
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

# Install Prometheus monitoring collection
ansible-galaxy collection install prometheus.prometheus
```

### Cluster Deployment Commands

```bash
# Deploy core components (deva/manusya)
ansible-playbook -i ./deploy/hosts ./deploy/deploy.yml -t start

# Deploy Jaeger distributed tracing
ansible-playbook -i ./deploy/hosts ./deploy/deploy.yml -t start-jaeger

# Deploy Prometheus monitoring
ansible-playbook -i ./deploy/hosts ./deploy/deploy.yml -t start-prometheus
```

## Observability and Tracing

Pain integrates with OpenTelemetry for comprehensive distributed tracing capabilities. Jaeger can be deployed using the following Docker command:

```bash
docker run --rm \
  -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 \
  -p 16686:16686 \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 9411:9411 \
  jaegertracing/all-in-one:latest
```

## Software Development Kit (SDK)

### C++ Integration Example

The following example demonstrates basic file operations using the Pain SDK:

```c++
#include <gflags/gflags.h>
#include <pain/base/plog.h>
#include <pain/base/scope_exit.h>
#include <pain/base/tracer.h>
#include <pain/pain.h>
#include <memory>
#include <spdlog/spdlog.h>

DEFINE_string(filesystem, "list://192.168.10.1:8001,192.168.10.2:8001,192.168.10.3:8001", "filesystem");

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    spdlog::set_level(spdlog::level::debug);
    pain::init_tracer("pain_demo");
    SCOPE_EXIT {
        PLOG_WARN(("desc", "pain_demo exit"));
        pain::cleanup_tracer();
    };

    pain::FileSystem* fs = nullptr;
    auto status = pain::FileSystem::create(FLAGS_filesystem.c_str(), &fs);
    if (!status.ok()) {
        std::cerr << "create filesystem failed: " << status.error_str() << std::endl;
        return 1;
    }
    std::unique_ptr<pain::FileSystem> fs_guard(fs);
    pain::FileStream* file = nullptr;
    status = fs->open("/hello.txt", O_CREAT | O_WRONLY, &file);
    if (!status.ok()) {
        std::cerr << "open file failed: " << status.error_str() << std::endl;
        return 1;
    }
    std::unique_ptr<pain::FileStream> file_guard(file);
    pain::proto::FileService::Stub stub(file);
    pain::Controller cntl;
    pain::proto::AppendRequest request;
    pain::proto::AppendResponse response;
    cntl.request_attachment().append("hello world");
    stub.Append(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        file->close();
        std::cerr << "append failed: " << cntl.ErrorText() << std::endl;
        return 1;
    }
    std::cout << "append success, offset: " << response.offset() << std::endl;
    file->close();
    return 0;
}
```

## Contributing

We welcome contributions from the community. Please refer to our [Contributing Guidelines](docs/en/contributing.md) for detailed information on how to participate in the project development.

## License

This project is licensed under the terms specified in the LICENSE file.

## Support

For technical support and community discussions, please refer to the project's GitHub Issues and Discussions sections.
