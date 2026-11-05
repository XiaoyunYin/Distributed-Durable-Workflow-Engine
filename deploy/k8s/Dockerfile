# K3 images include bounded evidence tools; default Compose targets are untouched.
FROM golang:1.27.3-bookworm@sha256:972b4110ba0f47539a975c31e52c64765e1ff19f2d74469ae4a595c6676e80a5 AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd ./cmd
COPY internal ./internal
COPY migrations ./migrations
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/runtime ./cmd/runtime \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/migrate ./cmd/dur053-migrate \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/loadgen ./cmd/dur053-loadgen \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/checker ./cmd/dur049-checker
FROM scratch AS runtime
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/ /
USER 65532:65532
ENTRYPOINT ["/runtime"]
FROM python:3.12.7-slim-bookworm@sha256:60d9996b6a8a3689d36db740b49f4327be3be09a21122bd02fb8895abb38b50d AS worker
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/python
WORKDIR /app
COPY requirements-worker.txt ./
RUN pip install --no-cache-dir --require-hashes -r requirements-worker.txt
COPY python ./python
COPY --from=build /out/loadgen /loadgen
COPY --from=build /out/checker /checker
USER 65532:65532
ENTRYPOINT ["python", "-m", "workers"]
