FROM --platform=linux/amd64 ubuntu:24.04 AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ libc6-dev \
    && rm -rf /var/lib/apt/lists/*
COPY main.cpp /src/main.cpp
RUN g++ -O3 -fno-rtti -std=c++17 -pthread -static -o /ss /src/main.cpp

FROM --platform=linux/amd64 ubuntu:24.04
COPY --from=build /ss /ss
ENTRYPOINT ["/ss"]
