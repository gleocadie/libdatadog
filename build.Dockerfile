FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ENV CARGO_TARGET_DIR=/ 
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build -p logs_tailer

FROM node
WORKDIR /serverless/node_poc
COPY --from=builder /debug/liblogs_tailer.so /liblogs_tailer.so

ENV LD_PRELOAD=/liblogs_tailer.so

ENTRYPOINT ["npm", "start"]

# CMD LD_PRELOAD=/liblogs_tailer.so npm start
