# Dependency-free gateway core.
FROM golang:1.27-alpine AS core-builder
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/llm-gateway ./cmd/llm-gateway

# Go drivers remain separate modules with their own dependency trees.
FROM core-builder AS go-drivers-builder
RUN cd drivers/http && CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/http-driver . && \
    cd ../acp && CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/acp-driver . && \
    cd ../jev && CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/jev-driver . && \
    cd ../embeddings && CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/embeddings-driver .

# docker build --target core builds the core-only image.
FROM alpine:3.20 AS core
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app
COPY --from=core-builder /out/llm-gateway /usr/local/bin/llm-gateway
EXPOSE 9208
ENTRYPOINT ["llm-gateway"]
CMD ["--help"]

# The default image includes HTTP, ACP, JEV and embeddings. Build/install web-driver separately
# when browser automation is required.
FROM core AS gateway
COPY --from=go-drivers-builder /out/http-driver /usr/local/bin/http-driver
COPY --from=go-drivers-builder /out/acp-driver /usr/local/bin/acp-driver

COPY --from=go-drivers-builder /out/jev-driver /usr/local/bin/jev-driver

COPY --from=go-drivers-builder /out/embeddings-driver /usr/local/bin/embeddings-driver
