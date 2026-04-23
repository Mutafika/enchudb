FROM node:22-slim AS builder

RUN apt-get update && apt-get install -y curl build-essential && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

WORKDIR /build
COPY Cargo.toml Cargo.lock build.rs ./
COPY src/ ./src/

ENV PATH="/root/.cargo/bin:${PATH}"
RUN cargo build --release && \
    cp target/release/libenchu_extend.so enchu-extend-linux.node 2>/dev/null || \
    cp target/release/libenchu_extend.dylib enchu-extend-linux.node 2>/dev/null || true

FROM node:22-slim

WORKDIR /bench

COPY --from=builder /build/enchu-extend-linux.node ./
COPY native.js package.json bench_real.ts ./

RUN npm install pg 2>/dev/null
RUN npm install -g tsx 2>/dev/null

CMD ["tsx", "bench_real.ts"]
