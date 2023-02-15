FROM debian:latest

WORKDIR /app

ADD . /app/

# install rust
RUN apt-get -y update
RUN apt-get -y install build-essential
RUN apt-get -y install openssl pkg-config libssl-dev
RUN apt-get -y install gcc
RUN apt-get -y install curl
RUN apt-get -y install nodejs npm
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"

# build our native .node addon
RUN npm install -g @napi-rs/cli

CMD npm run build
