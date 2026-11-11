FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY pkg/ pkg/
COPY internal/ internal/
COPY cmd/ cmd/

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/orchestrator ./cmd/orchestrator

FROM alpine:3.20

RUN apk add --no-cache ca-certificates iptables iproute2
COPY --from=builder /bin/orchestrator /usr/local/bin/orchestrator

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/orchestrator"]
CMD ["-mode=server", "-port=8080"]
