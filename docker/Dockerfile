# ---------- Builder stage ----------
FROM postgres:17-trixie AS builder

ENV TS_VERSIONS="2.21.3 2.22.0" \
    TLK_VERSION=1.21.0 \
    TDS_VERSION=v2.0.5 \
    CARGO_HOME=/build/cargo \
    CARGO_TARGET_DIR=/build/target \
    RUSTUP_HOME=/build/rustup \
    PGRX_HOME=/build/pgrx \
    PATH=/build/cargo/bin:$PATH

ARG DEBIAN_FRONTEND=noninteractive

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsybdb5 \
    freetds-dev \
    freetds-common \
    postgresql-server-dev-${PG_MAJOR} \
    gcc \
    cmake \
    make \
    git \
    curl \
    ca-certificates \
    clang \
    llvm \
    libssl-dev \
    pkg-config \
    && mkdir -p $PGRX_HOME \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path --profile minimal -c clippy,rustfmt \
    && cargo install cargo-pgrx --version '=0.12.9' \
    && cargo pgrx init --pg${PG_MAJOR} pg_config

# ---------- Build TimescaleDB ----------
RUN set -ex && \
    for ver in $TS_VERSIONS; do \
    echo "=== Building TimescaleDB $ver ===" && \
    mkdir /build/timescaledb-$ver && \
    git clone --branch $ver https://github.com/timescale/timescaledb /build/timescaledb-$ver \
    && cd /build/timescaledb-$ver \
    && ./bootstrap -DPROJECT_INSTALL_METHOD="docker" \
    && cd build && make install; \
    done \
    && sed -r -i "s/[#]*\s*(shared_preload_libraries)\s*=\s*'(.*)'/\1 = 'timescaledb,\2'/;s/,'/'/" \
         /usr/share/postgresql/${PG_MAJOR}/postgresql.conf.sample \
    && rm -rf /build/timescaledb-*

# ---------- Build Timescale Toolkit ----------
RUN set -ex && \
    git clone --branch $TLK_VERSION https://github.com/timescale/timescaledb-toolkit.git /build/timescaledb-toolkit \
    && cd /build/timescaledb-toolkit/extension \
    && cargo pgrx install --release \
    && cargo run --release --manifest-path ../tools/post-install/Cargo.toml -- pg_config \
    && rm -rf /build/timescaledb-toolkit

# ---------- Build TDS-FDW ----------
RUN git clone --branch $TDS_VERSION https://github.com/tds-fdw/tds_fdw.git /build/tds_fdw \
    && cd /build/tds_fdw \
    && make USE_PGXS=1 \
    && make install USE_PGXS=1 \
    && echo "[default]\n\tdate format = %b %e %Y %I:%M:%S.%z%p" >> /etc/freetds/locales.conf \
    && rm -rf /build \
    && apt-get clean
# change the date format used by freetds
# https://github.com/tds-fdw/tds_fdw/issues/271#issuecomment-731581415

# ---------- Runtime stage ----------
FROM postgres:17-trixie AS release

# --------- Metadata ----------
LABEL org.opencontainers.image.title="Custom PostgreSQL image with TimescaleDB, Toolkit & TDS-FDW" \
      org.opencontainers.image.description='Postgres 17.6 + TimescaleDB 2.22.0 + Toolkit 1.21.0 + TDS-FDW 2.0.5' \
      org.opencontainers.image.version="0.1a2" \
      org.opencontainers.image.authors="Grigory Sharov <gsharov@mrc-lmb.cam.ac.uk>" \
      org.opencontainers.image.licenses="GPL-3.0-or-later" \
      org.opencontainers.image.source="https://github.com/azazellochg/em_health"

# Copy built extensions & freetds runtime only
COPY --from=builder /usr/lib/postgresql/${PG_MAJOR} /usr/lib/postgresql/${PG_MAJOR}
COPY --from=builder /usr/share/postgresql/${PG_MAJOR}/extension/ /usr/share/postgresql/${PG_MAJOR}/extension/
COPY --from=builder /etc/freetds/ /etc/freetds/

ARG DEBIAN_FRONTEND=noninteractive

# freetds runtime deps, timescaledb tools, pgBackRest
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsybdb5 \
    freetds-common \
    curl \
    ca-certificates \
    postgresql-${PG_MAJOR}-pgtap \
    pgbackrest \
    libtap-parser-sourcehandler-pgtap-perl \
    && curl -L -o /tmp/timescaledb-tools.deb "https://packagecloud.io/timescale/timescaledb/packages/debian/trixie/timescaledb-tools_0.18.1~debian13_amd64.deb/download.deb?distro_version_id=221" \
    && dpkg -i /tmp/timescaledb-tools.deb \
    && rm -f /tmp/timescaledb-tools.deb \
    && apt-get purge -y curl ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# https://github.com/docker-library/postgres/commit/bfc5d81c8f5647c690f452dc558e64fddb1802f6
STOPSIGNAL SIGINT

# Drop privileges
USER postgres
WORKDIR /var/lib/postgresql

# Keep the original entrypoint
EXPOSE 5432
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["postgres"]
