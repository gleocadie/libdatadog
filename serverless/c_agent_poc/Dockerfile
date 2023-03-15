FROM golang:latest as builder

COPY ./datadog-agent/ /datadog-agent
WORKDIR /datadog-agent/cmd/agent
RUN go build -o datadog-agent .

FROM gcc:latest

ENV DD_LOG_LEVEL=debug

WORKDIR /app
COPY [^datadog-agent]* ./
COPY --from=builder /datadog-agent/cmd/agent/datadog-agent /app/datadog-agent

RUN make

COPY datadog.yaml /etc/datadog-agent/datadog.yaml

CMD LD_PRELOAD=./libddpreload_init.so ./customer_function
