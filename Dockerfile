# Use Docker Desktop's bundled BuildKit frontend (cache mounts require BuildKit).
# No external syntax image is needed before the build can start.
ARG RUST_IMAGE=rust:1.95.0-slim-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim

FROM --platform=$BUILDPLATFORM ${RUST_IMAGE} AS builder

ARG BUILDARCH
ARG TARGETARCH
ARG TARGETOS

# Run rustc natively, including when an Apple Silicon host builds for AMD64.
# Native C dependencies (ring, mimalloc, zstd) need the target C toolchain too.
RUN set -eux; \
    test "$TARGETOS" = linux; \
    case "$TARGETARCH" in \
        amd64) rust_target=x86_64-unknown-linux-gnu; cc_target=x86-64-linux-gnu ;; \
        arm64) rust_target=aarch64-unknown-linux-gnu; cc_target=aarch64-linux-gnu ;; \
        *) echo "Unsupported Docker target architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    cross_packages=""; \
    if [ "$BUILDARCH" != "$TARGETARCH" ]; then \
        cross_packages="gcc-$cc_target g++-$cc_target"; \
    fi; \
    apt-get update; \
    apt-get install --yes --no-install-recommends build-essential pkg-config perl python3 $cross_packages; \
    rm -rf /var/lib/apt/lists/*; \
    rustup target add "$rust_target"; \
    printf '%s' "$rust_target" > /rust-target

ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc

WORKDIR /usr/src/mydb

COPY Cargo.toml Cargo.lock rust-toolchain.toml build.rs ./
COPY src ./src
COPY vendor ./vendor
COPY clients/mydb-cli ./clients/mydb-cli
COPY assets/mysql80 ./assets/mysql80
COPY assets/third-party-licenses ./assets/third-party-licenses
COPY scripts/release_licenses.py ./scripts/release_licenses.py

# Bound Cargo and rustc codegen concurrency on memory-limited Docker hosts.
# Override with --build-arg CARGO_BUILD_JOBS=N when more memory is available.
ARG CARGO_BUILD_JOBS=1

RUN --mount=type=cache,id=mydb-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=mydb-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=mydb-target-${BUILDARCH}-${TARGETARCH},target=/usr/src/mydb/target,sharing=locked \
    cargo build \
    --jobs "$CARGO_BUILD_JOBS" \
    --release \
    --locked \
    --target "$(cat /rust-target)" \
    --features tools \
    --bin mydb --bin mydb-backup \
    && install -D "target/$(cat /rust-target)/release/mydb" /out/mydb \
    && install -D "target/$(cat /rust-target)/release/mydb-backup" /out/mydb-backup \
    && cargo metadata --locked --features tools --filter-platform "$(cat /rust-target)" --format-version 1 > /out/metadata.json \
    && cargo build --manifest-path clients/mydb-cli/Cargo.toml --jobs "$CARGO_BUILD_JOBS" --release --locked --target "$(cat /rust-target)" --target-dir target \
    && install -D "target/$(cat /rust-target)/release/mydb-cli" /out/mydb-cli \
    && cargo metadata --manifest-path clients/mydb-cli/Cargo.toml --locked --filter-platform "$(cat /rust-target)" --format-version 1 > /out/cli-metadata.json \
    && python3 scripts/release_licenses.py --source /usr/src/mydb --output /out --metadata /out/metadata.json --extra-metadata /out/cli-metadata.json --extra-lockfile clients/mydb-cli/Cargo.lock

FROM ${RUNTIME_IMAGE} AS runtime

LABEL org.opencontainers.image.title="mydb" \
      org.opencontainers.image.description="MySQL-protocol-compatible database written in Rust" \
      org.opencontainers.image.source="https://github.com/yzsind/mydb" \
      org.opencontainers.image.licenses="Apache-2.0"

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends ca-certificates default-mysql-client libgcc-s1 tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 mydb \
    && useradd \
        --uid 10001 \
        --gid 10001 \
        --home-dir /var/lib/mydb \
        --no-create-home \
        --no-log-init \
        --shell /usr/sbin/nologin \
        mydb \
    && install --directory --owner mydb --group mydb /var/lib/mydb \
    && install --directory /etc/mydb /usr/share/licenses/mydb

COPY --from=builder /out/mydb /usr/local/bin/mydb
COPY --from=builder /out/mydb-backup /usr/local/bin/mydb-backup
COPY --from=builder /out/mydb-cli /usr/local/bin/mydb-cli
COPY --from=builder /out/third-party /usr/share/licenses/mydb/third-party
COPY --chmod=0755 docker/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
COPY --chmod=0644 docker/mydb.cnf /etc/mydb/mydb.cnf
COPY --chmod=0644 LICENSE /usr/share/licenses/mydb/LICENSE

WORKDIR /var/lib/mydb
USER 10001:10001

VOLUME ["/var/lib/mydb"]
EXPOSE 8738
STOPSIGNAL SIGTERM

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
