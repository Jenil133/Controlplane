# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/controlplane ./cmd/controlplane && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/cpctl ./cmd/cpctl

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/controlplane /out/cpctl /usr/local/bin/
# 9090: gRPC (admin + distribution), 8080: /healthz and /readyz
EXPOSE 9090 8080
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/controlplane"]
