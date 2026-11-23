# ---------- Component versions ----------
# Single source of truth for component versions. The GHCR image description is generated
# from these ARG lines by .github/workflows/docker-publish.yml, so keep the format
# "ARG NAME=value" (one per line). The image tag itself lives in docker/compose.yaml.
ARG PG_IMAGE=docker.io/library/postgres:18.7-trixie@sha256:ec003ee60c23d320d1b81868f255a6fc724ac06d8f1ced4468532f7d8593f357

# ---------- Common build dependencies ----------
FROM ${PG_IMAGE} AS build-base

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    clang \
    cmake \
    curl \
    freetds-common \
    freetds-dev \
    gcc \
    git \
    libssl-dev \
    libsybdb5 \
    llvm \
    make \
    pkg-config \
    libicu-dev \
    postgresql-server-dev-${PG_MAJOR}

# The three extensions below are built in independent stages, so they can build in parallel
# (BuildKit does this by default, podman needs --jobs) and bumping one version doesn't rebuild the others.

# ---------- Build TimescaleDB ----------
FROM build-base AS timescaledb

# TimescaleDB: all listed versions are built (older ones allow restoring old backups); the last one is the default
ARG TS_VERSIONS="2.30.2 2.32.1"

# Only update scripts starting from a version built here are kept
RUN set -ex && \
    for ver in $TS_VERSIONS; do \
    echo "=== Building TimescaleDB $ver ===" && \
    git clone --depth 1 --branch $ver https://github.com/timescale/timescaledb /build/timescaledb-$ver \
    && cd /build/timescaledb-$ver \
    && ./bootstrap -DTAP_CHECKS=OFF -DREGRESS_CHECKS=OFF -DWARNINGS_AS_ERRORS=OFF -DPROJECT_INSTALL_METHOD="docker" \
    && cd build && make -j$(nproc) install; \
    done \
    && strip --strip-unneeded /usr/lib/postgresql/${PG_MAJOR}/lib/timescaledb*.so \
    && cd /usr/share/postgresql/${PG_MAJOR}/extension \
    && for f in timescaledb--*--*.sql; do \
    from=${f#timescaledb--}; from=${from%%--*}; \
    case " $TS_VERSIONS " in *" $from "*) ;; *) rm "$f" ;; esac; \
    done

# ---------- Build Timescale Toolkit ----------
FROM build-base AS toolkit

ENV CARGO_HOME=/build/cargo \
    CARGO_TARGET_DIR=/build/target \
    RUSTUP_HOME=/build/rustup \
    PGRX_HOME=/build/pgrx \
    PATH=/build/cargo/bin:$PATH

# Before changing pgrx version, see https://github.com/timescale/timescaledb-toolkit/blob/main/extension/Cargo.toml
RUN mkdir -p $PGRX_HOME \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path --profile minimal \
    && cargo install cargo-pgrx --version '=0.18.1' \
    && cargo pgrx init --pg${PG_MAJOR} pg_config

ARG TLK_VERSION=1.28.0

RUN set -ex && \
    git clone --depth 1 --branch $TLK_VERSION https://github.com/timescale/timescaledb-toolkit.git /build/timescaledb-toolkit \
    && cd /build/timescaledb-toolkit/extension \
    && cargo pgrx install --release \
    && cargo run --release --manifest-path ../tools/post-install/Cargo.toml -- pg_config \
    && strip --strip-unneeded /usr/lib/postgresql/${PG_MAJOR}/lib/timescaledb_toolkit*.so

# ---------- Build TDS-FDW ----------
FROM build-base AS tds_fdw

ARG TDS_VERSION=v2.0.5

# FreeTDS hands SQL Server dates to tds_fdw as text in its locale "date format". The default
# (%b %e %Y %I:%M%p) drops the seconds, so add seconds and fractions (%z is fractional seconds in
# FreeTDS, not a time zone). There is no specifier for the offset, so a datetimeoffset still loses it:
# UEC ErrorDtm is converted to UTC on the SQL Server side instead (see fdw_manager.py).
# https://github.com/tds-fdw/tds_fdw/issues/271#issuecomment-731581415
RUN git clone --depth 1 --branch $TDS_VERSION https://github.com/tds-fdw/tds_fdw.git /build/tds_fdw \
    && cd /build/tds_fdw \
    && make -j$(nproc) USE_PGXS=1 \
    && make install USE_PGXS=1 \
    && strip --strip-unneeded /usr/lib/postgresql/${PG_MAJOR}/lib/tds_fdw.so \
    && echo "[default]\n\tdate format = %b %e %Y %I:%M:%S.%z%p" >> /etc/freetds/locales.conf

# ---------- Runtime stage ----------
FROM ${PG_IMAGE} AS release

# --------- Metadata ----------
# Static labels only. Version and description are added by the CI workflow.
LABEL org.opencontainers.image.title="Custom PostgreSQL image with TimescaleDB, Toolkit & TDS-FDW" \
      org.opencontainers.image.authors="Grigory Sharov <gsharov@mrclmb.ac.uk>" \
      org.opencontainers.image.licenses="GPL-3.0-or-later" \
      org.opencontainers.image.source="https://github.com/azazellochg/em_health"

# Copy only the files the extensions installed
COPY --from=timescaledb /usr/lib/postgresql/${PG_MAJOR}/lib/timescaledb*.so /usr/lib/postgresql/${PG_MAJOR}/lib/
COPY --from=timescaledb /usr/share/postgresql/${PG_MAJOR}/extension/timescaledb* /usr/share/postgresql/${PG_MAJOR}/extension/
COPY --from=toolkit /usr/lib/postgresql/${PG_MAJOR}/lib/timescaledb_toolkit*.so /usr/lib/postgresql/${PG_MAJOR}/lib/
COPY --from=toolkit /usr/share/postgresql/${PG_MAJOR}/extension/timescaledb_toolkit* /usr/share/postgresql/${PG_MAJOR}/extension/
COPY --from=tds_fdw /usr/lib/postgresql/${PG_MAJOR}/lib/tds_fdw.so /usr/lib/postgresql/${PG_MAJOR}/lib/
COPY --from=tds_fdw /usr/lib/postgresql/${PG_MAJOR}/lib/bitcode/tds_fdw /usr/lib/postgresql/${PG_MAJOR}/lib/bitcode/tds_fdw/
COPY --from=tds_fdw /usr/lib/postgresql/${PG_MAJOR}/lib/bitcode/tds_fdw.index.bc /usr/lib/postgresql/${PG_MAJOR}/lib/bitcode/
COPY --from=tds_fdw /usr/share/postgresql/${PG_MAJOR}/extension/tds_fdw* /usr/share/postgresql/${PG_MAJOR}/extension/
COPY --from=tds_fdw /etc/freetds/ /etc/freetds/

ARG DEBIAN_FRONTEND=noninteractive

# freetds runtime deps, pgBackRest
RUN apt-get update && apt-get install -y --no-install-recommends \
    freetds-common \
    libsybdb5 \
    libtap-parser-sourcehandler-pgtap-perl \
    pgbackrest \
    postgresql-${PG_MAJOR}-pgtap \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# https://github.com/docker-library/postgres/commit/bfc5d81c8f5647c690f452dc558e64fddb1802f6
STOPSIGNAL SIGINT

# Drop privileges
USER postgres
WORKDIR /var/lib/postgresql/${PG_MAJOR}/docker

# Keep the original entrypoint
EXPOSE 5432
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["postgres"]
