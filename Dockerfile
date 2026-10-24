# Build stage
FROM golang:1.27-alpine AS builder

WORKDIR /src

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /bin/llm-gateway ./cmd/llm-gateway && \
    cd drivers/http && CGO_ENABLED=0 go build -ldflags="-s -w" -o /bin/http-driver . && \
    cd ../acp && CGO_ENABLED=0 go build -ldflags="-s -w" -o /bin/acp-driver .

# Runtime stage
FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

COPY --from=builder /bin/llm-gateway /usr/local/bin/llm-gateway
COPY --from=builder /bin/http-driver /usr/local/bin/http-driver
COPY --from=builder /bin/acp-driver /usr/local/bin/acp-driver

EXPOSE 9208 9209

ENTRYPOINT ["llm-gateway"]
CMD ["--help"]
