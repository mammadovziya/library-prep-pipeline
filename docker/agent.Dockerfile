FROM golang:1.26.5-bookworm AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w -buildid=" -o /out/agent ./cmd/agent

FROM nvidia/cuda:12.8.1-base-ubuntu24.04
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/agent /usr/local/bin/agent
RUN useradd --uid 65532 --no-create-home --shell /usr/sbin/nologin worker
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/agent"]
