FROM timescale/timescaledb-ha:pg17

USER root

ENV TDS_FDW_VERSION 2.0.4

RUN apt update && apt install -y --no-install-recommends \
        libsybdb5 freetds-dev freetds-common gnupg gcc make wget postgresql-server-dev-17 \
    && wget https://github.com/tds-fdw/tds_fdw/archive/v${TDS_FDW_VERSION}.tar.gz -O /tmp/tds_fdw.tar.gz \
    && cd /tmp && tar xzf tds_fdw.tar.gz \
    && cd tds_fdw-${TDS_FDW_VERSION} && make USE_PGXS=1 && make USE_PGXS=1 install \
    && apt purge -y gcc make postgresql-server-dev-17 wget \
    && apt autoremove -y \
    && rm -rf /tmp/tds_fdw* /var/lib/apt/lists/*

# change the date format used by freetds
# https://github.com/tds-fdw/tds_fdw/issues/271#issuecomment-731581415
RUN echo "[default]\n\tdate format = %b %e %Y %I:%M:%S.%z%p" >> /etc/freetds/locales.conf

USER postgres
