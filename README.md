# pain
[![Linux](https://github.com/ivanallen/pain/actions/workflows/ubuntu.yml/badge.svg)](https://github.com/ivanallen/pain/actions/workflows/ubuntu.yml)

A distributed storage system.


## Prerequisites

- xmake
- python3

## Build

```
./build.py config -m debug

# build and install to ${PROJECT_DIR}/output
./build.py build --install

# only build
./build.py build

# only build sad target
./build.py build sad
```

Using `./build.py -h` for more information.

## Trace

```
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
#include "pain/pain.h"
#include "spdlog/spdlog.h"

int main() {
    spdlog::set_level(spdlog::level::debug);
    auto fs = pain::FileSystem::create("list://192.168.10.1:8001,192.168.10.2:8001,192.168.10.3:8001");
    auto file = fs->open("/tmp/hello.txt", O_CREAT | O_WRONLY);
    pain::core::FileService::Stub stub(file.get());
    pain::Controller cntl;
    pain::core::AppendRequest request;
    pain::core::AppendResponse response;
    cntl.request_attachment().append("hello world");
    stub.append(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        std::cerr << "append failed: " << cntl.ErrorText() << std::endl;
        return 1;
    }
    std::cout << "append success, offset: " << response.offset() << std::endl;
    return 0;
}
```
