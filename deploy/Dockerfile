# One Dockerfile for every Go binary. Pick the binary with --build-arg SERVICE=<dir under cmd/>.

FROM golang:1.24.12-alpine AS build
# Fail loudly instead of downloading a different Go toolchain.
ENV GOTOOLCHAIN=local CGO_ENABLED=0
ARG SERVICE
WORKDIR /src

# Dependencies first: this layer is reused until go.mod or go.sum change.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${SERVICE}

FROM alpine:3.22.6
# Run as an unprivileged user.
RUN adduser -D -u 10001 app
USER app
COPY --from=build /out/app /usr/local/bin/app
ENTRYPOINT ["/usr/local/bin/app"]
