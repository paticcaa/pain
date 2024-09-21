[![Linux](https://github.com/ivanallen/pain/actions/workflows/ubuntu.yml/badge.svg)](https://github.com/ivanallen/pain/actions/workflows/ubuntu.yml)

# pain
A distributed storage system.


## Prerequisites

- xmake
- python3

## Build

```
./build.py config -m debug
./build.py build --install
```

Using `./build.py -h` for more infomations.

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
