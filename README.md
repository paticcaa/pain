# Pain

[![Linux](https://github.com/ivanallen/pain/actions/workflows/ubuntu.yml/badge.svg)](https://github.com/ivanallen/pain/actions/workflows/ubuntu.yml)
[![Coverage Status](https://coveralls.io/repos/github/ivanallen/pain/badge.svg)](https://coveralls.io/github/ivanallen/pain)

A high-performance distributed storage system designed for scalability and reliability.

## System Requirements

### Prerequisites

The following tools and dependencies are required for building and running Pain:

- **bazel** - Build system
- **Python 3** - Script execution and automation
- **Ansible** - Deployment automation
- **Docker** - Container runtime

### Installation Instructions

```bash
# Instal bazel
# Please refer to https://bazel.build/install

# Install Ansible automation tool
sudo apt install ansible

# Install Prometheus monitoring roles
ansible-galaxy collection install prometheus.prometheus

# Install development tools and system packages
sudo apt install -y clang-format-16
sudo apt install -y libaio-dev
sudo apt install -y uuid-dev
sudo apt install -y libibverbs-dev
sudo apt install -y librdmacm-dev
sudo apt install -y libelf-dev
sudo apt install -y libssl-dev
sudo apt install -y liblz4-dev
sudo apt install -y libnuma-dev
sudo apt install -y python3
sudo apt install -y python3-pip
sudo apt install -y patchelf
sudo apt install -y python3-pyelftools
sudo apt install -y meson
sudo apt install -y nasm
sudo apt install -y ccache
sudo apt install -y autoconf automake libtool pkg-config
```

## Build and Development

### Core Build Commands

```bash
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
#include <pain/pain.h>

int main() {
    // Initialize filesystem connection to cluster
    auto fs = pain::FileSystem::create("list://192.168.10.1:8001,192.168.10.2:8001,192.168.10.3:8001");
    
    // Open file for writing
    auto file = fs->open("/tmp/hello.txt", O_CREAT | O_WRONLY);
    
    // Create service stub for file operations
    pain::proto::FileService::Stub stub(file.get());
    
    // Prepare request and response objects
    pain::Controller cntl;
    pain::proto::AppendRequest request;
    pain::proto::AppendResponse response;
    
    // Configure request parameters
    request.set_direct_io(true);
    cntl.request_attachment().append("hello world");
    
    // Execute append operation
    stub.Append(&cntl, &request, &response, nullptr);
    
    // Handle operation results
    if (cntl.Failed()) {
        std::cerr << "Append operation failed: " << cntl.ErrorText() << std::endl;
        return 1;
    }
    
    std::cout << "Append operation successful, offset: " << response.offset() << std::endl;
    return 0;
}
```

## Contributing

We welcome contributions from the community. Please refer to our [Contributing Guidelines](docs/en/contributing.md) for detailed information on how to participate in the project development.

## License

This project is licensed under the terms specified in the LICENSE file.

## Support

For technical support and community discussions, please refer to the project's GitHub Issues and Discussions sections.
