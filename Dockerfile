# Build stage
FROM golang:1.27-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /bin/llm-gateway ./cmd/llm-gateway

# Runtime stage
FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

COPY --from=builder /bin/llm-gateway /usr/local/bin/llm-gateway

EXPOSE 9208 9209

ENTRYPOINT ["llm-gateway"]
CMD ["--help"]
