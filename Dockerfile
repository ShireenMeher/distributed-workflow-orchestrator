FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /bin/api ./cmd/api && \
    go build -o /bin/scheduler ./cmd/scheduler && \
    go build -o /bin/worker ./cmd/worker

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
ARG SERVICE=api
ENV SERVICE=${SERVICE}
COPY --from=builder /bin/api /bin/api
COPY --from=builder /bin/scheduler /bin/scheduler
COPY --from=builder /bin/worker /bin/worker
ENTRYPOINT ["/bin/sh", "-c", "exec /bin/$SERVICE"]
