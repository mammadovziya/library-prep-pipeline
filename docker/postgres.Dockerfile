FROM postgres:18.4-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends pgbackrest openssh-client ca-certificates \
    && mkdir -p /var/spool/pgbackrest \
    && chown postgres:postgres /var/spool/pgbackrest \
    && rm -rf /var/lib/apt/lists/*
