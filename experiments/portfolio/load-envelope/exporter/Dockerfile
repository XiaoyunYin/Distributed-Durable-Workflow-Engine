FROM golang:1.27.4-bookworm@sha256:4fa7c968ba93f1635670522dbb01a2c78150443e1385746924566079f36570ea AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY experiments/portfolio/load-envelope/exporter/main.go ./main.go
RUN CGO_ENABLED=0 GOMAXPROCS=2 go build -p 1 -buildvcs=false -trimpath -ldflags="-s -w" -o /exporter ./main.go
FROM scratch
COPY --from=build /exporter /exporter
USER 65532:65532
ENTRYPOINT ["/exporter"]
