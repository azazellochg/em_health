# ---------- Builder stage ----------
FROM postgres:17-trixie AS builder

ENV TS_VERSIONS="2.21.3 2.22.0" \
    TLK_VERSION=1.21.0 \
    TDS_VERSION=v2.0.4 \
    CARGO_HOME=/build/cargo \
    RUSTUP_HOME=/build/rustup \
    PGRX_HOME=/build/pgrx \
    PATH=/build/cargo/bin:$PATH

ARG DEBIAN_FRONTEND=noninteractive

# --------- Metadata ----------
LABEL org.opencontainers.image.title="Custom PostgreSQL image with TimescaleDB, Toolkit & TDS-FDW" \
      org.opencontainers.image.description='Postgres ${PG_MAJOR} + TimescaleDB ${TS_VERSIONS} + Toolkit ${TLK_VERSION} + TDS-FDW ${TDS_VERSION}' \
      org.opencontainers.image.version="0.1a1" \
      org.opencontainers.image.authors="Grigory Sharov <gsharov@mrc-lmb.cam.ac.uk>" \
      org.opencontainers.image.licenses="GPL-3.0-or-later" \
      org.opencontainers.image.source="https://github.com/azazellochg/em_health"

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
    pkg-config

# ---------- Install Rust & cargo-pgrx ----------
RUN mkdir -p $PGRX_HOME \
  && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path \
  && cargo install --profile release --version '=0.12.9' --force cargo-pgrx \
  && cargo pgrx init --pg${PG_MAJOR} pg_config

# ---------- Build TimescaleDB ----------
RUN for ver in $TS_VERSIONS; do \
    echo "=== Building TimescaleDB $ver ===" && \
    mkdir /build/timescaledb-$ver && \
    git clone --branch $ver https://github.com/timescale/timescaledb /build/timescaledb-$ver \
    && cd /build/timescaledb-$ver \
    && ./bootstrap -DCMAKE_BUILD_TYPE=RelWithDebInfo \
                   -DREGRESS_CHECKS=OFF -DTAP_CHECKS=OFF \
                   -DGENERATE_DOWNGRADE_SCRIPT=ON \
                   -DWARNINGS_AS_ERRORS=OFF \
                   -DPROJECT_INSTALL_METHOD="docker" \
    && cd build && make install; \
    done

RUN sed -r -i "s/[#]*\s*(shared_preload_libraries)\s*=\s*'(.*)'/\1 = 'timescaledb,\2'/;s/,'/'/" \
         /usr/share/postgresql/${PG_MAJOR}/postgresql.conf.sample

# ---------- Build Timescale Toolkit ----------
RUN git clone --branch $TLK_VERSION https://github.com/timescale/timescaledb-toolkit.git /build/timescaledb-toolkit \
    && cd /build/timescaledb-toolkit/extension \
    && cargo pgrx install --release \
    && cargo run --release --manifest-path ../tools/post-install/Cargo.toml -- pg_config

# ---------- Build TDS-FDW ----------
RUN git clone --branch $TDS_VERSION https://github.com/tds-fdw/tds_fdw.git /build/tds_fdw \
    && cd /build/tds_fdw \
    && make USE_PGXS=1 \
    && make install USE_PGXS=1

# change the date format used by freetds
# https://github.com/tds-fdw/tds_fdw/issues/271#issuecomment-731581415
RUN echo "[default]\n\tdate format = %b %e %Y %I:%M:%S.%z%p" >> /etc/freetds/locales.conf

# ---------- Clean up to reduce CI cache bloat ----------
RUN rm -rf /build \
    && apt-get clean

# ---------- Runtime stage ----------
FROM postgres:17-trixie AS release

# Copy built extensions & freetds runtime only
COPY --from=builder /usr/lib/postgresql/${PG_MAJOR} /usr/lib/postgresql/${PG_MAJOR}
COPY --from=builder /usr/share/postgresql/${PG_MAJOR}/extension/ /usr/share/postgresql/${PG_MAJOR}/extension/
COPY --from=builder /etc/freetds/ /etc/freetds/

ARG DEBIAN_FRONTEND=noninteractive

# freetds runtime deps and timescaledb tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsybdb5 freetds-common curl ca-certificates postgresql-${PG_MAJOR}-pgtap \
    && curl -L -o /tmp/timescaledb-tools.deb "https://packagecloud.io/timescale/timescaledb/packages/debian/trixie/timescaledb-tools_0.18.1~debian13_amd64.deb/download.deb?distro_version_id=221" \
    && dpkg -i /tmp/timescaledb-tools.deb \
    && rm -f /tmp/timescaledb-tools.deb \
    && apt-get purge -y curl ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Drop privileges
USER postgres
WORKDIR /var/lib/postgresql

# Keep the original entrypoint
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["postgres"]
