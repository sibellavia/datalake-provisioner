# syntax=docker/dockerfile:1.7

FROM golang:1.25 AS builder
WORKDIR /src

# Cache dependencies first
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags='-s -w' -o /out/datalake-provisioner ./cmd/server

# Minimal runtime image
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app

COPY --from=builder /out/datalake-provisioner /app/datalake-provisioner

EXPOSE 8081
USER nonroot:nonroot
ENTRYPOINT ["/app/datalake-provisioner"]
