# pain
[![Linux](https://github.com/ivanallen/pain/actions/workflows/ubuntu.yml/badge.svg)](https://github.com/ivanallen/pain/actions/workflows/ubuntu.yml)

A distributed storage system.


## Prerequisites

- xmake
- python3
- ansible

```bash
# install xmake
curl -fsSL https://xmake.io/shget.text | bash

# install ansible
sudo apt install ansible

# install prometheus roles
ansible-galaxy collection install prometheus.prometheus

# install tools and package
sudo apt install -y clang-format-16
sudo apt install -y libaio-dev
sudo apt install -y uuid-dev
sudo apt install -y libibverbs-dev
sudo apt install -y librdmacm-dev
sudo apt install -y libelf-dev
sudo apt install -y libssl-dev
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

## Build

- Build

```bash
./z.py build # or ./z.py b
./z.py deploy -a start # deploy pain to ~/deployment
```

- More commands

```bash
# show targes
./z.py show # or ./z.py s

# only build sad target
./z.py b sad

# test
./z.py test test_manusya # or ./z.py t test_manusya
./z.py t # test all cases

# format your project and it's useful before pulling a request
./z.py format # or ./z.py f

# lint
./z.py lint # or ./z.py l
```

Using `./z.py -h` for more information.

## Deploy

Pain provide a easy ansible script to deploy minimal cluster

```bash
# prerequisites
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
ansible-galaxy collection install prometheus.prometheus
```

```bash
# deploy deva/manusya
ansible-playbook -i ./deploy/hosts ./deploy/deploy.yml -t start

# deploy jaeger
ansible-playbook -i ./deploy/hosts ./deploy/deploy.yml -t start-jaeger

# deploy promethues
ansible-playbook -i ./deploy/hosts ./deploy/deploy.yml -t start-prometheus
```

## Trace

Pain supports opentelemetry. You can install jaeger using follow command.

```bash
docker run --rm \
  -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 \
  -p 16686:16686 \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 9411:9411 \
  jaegertracing/all-in-one:latest
```

## SDK Examples

```c++
#include <pain/pain.h>

int main() {
    auto fs = pain::FileSystem::create("list://192.168.10.1:8001,192.168.10.2:8001,192.168.10.3:8001");
    auto file = fs->open("/tmp/hello.txt", O_CREAT | O_WRONLY);
    pain::proto::FileService::Stub stub(file.get());
    pain::Controller cntl;
    pain::proto::AppendRequest request;
    pain::proto::AppendResponse response;
    
    request.set_direct_io(true);
    cntl.request_attachment().append("hello world");
    stub.Append(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        std::cerr << "append failed: " << cntl.ErrorText() << std::endl;
        return 1;
    }
    std::cout << "append success, offset: " << response.offset() << std::endl;
    return 0;
}
```
