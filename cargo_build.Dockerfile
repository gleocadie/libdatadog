FROM rust

ENV CARGO_TARGET_DIR=/
WORKDIR /app
COPY ./ ./
CMD cargo run -p logs
