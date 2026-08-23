# Build and runtime images are deliberately different Debian releases: the
# binary is linked against the builder's glibc and runs against the runtime's,
# and that direction (older build, newer runtime) is the one glibc supports.
FROM rust:1.96-slim AS build

WORKDIR /src

# Only what the binary needs: the tests/ and monitoring/ trees would invalidate
# this layer on every edit without changing the build.
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# --locked: the image is built from the committed Cargo.lock or not at all.
# --bin rpc-load-balancer leaves the mock_node binary out of the release build.
RUN cargo build --release --locked --bin rpc-load-balancer

FROM debian:trixie-slim

# rustls verifies upstream certificates against the system trust store
# (rustls-native-certs), so this is what stands between the balancer and a
# failed TLS handshake with every provider.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# /data is the working directory because quotas.json is written relative to it.
# It is owned by the unprivileged user the process runs as: a named volume
# mounted there for the first time inherits this ownership, which is what makes
# the quota file writable without running as root.
RUN useradd --system --uid 10001 --create-home --home-dir /data balancer

COPY --from=build /src/target/release/rpc-load-balancer /usr/local/bin/rpc-load-balancer

WORKDIR /data
USER balancer

# No CMD arguments: everything is configured through CONFIG_PATH and the
# environment the config's $VAR references resolve against.
ENTRYPOINT ["rpc-load-balancer"]
