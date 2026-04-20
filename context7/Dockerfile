FROM debian:11 AS builder

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Build
RUN mkdir build && cd build && cmake .. && make -j$(nproc)

# Test
RUN cd tests && python3 test_freq.py

FROM debian:11-slim

WORKDIR /app
COPY --from=builder /app/build/freq /usr/local/bin/freq

ENTRYPOINT ["freq"]
