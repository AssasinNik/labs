FROM postgres:15

# Установим нужные зависимости + pg_config
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    make \
    gcc \
    libpq-dev \
    postgresql-server-dev-15 \
    && rm -rf /var/lib/apt/lists/*

# Клонируем репозиторий и устанавливаем с USE_PGXS=1
RUN git clone https://github.com/eulerto/wal2json.git /tmp/wal2json \
    && cd /tmp/wal2json \
    && USE_PGXS=1 make \
    && USE_PGXS=1 make install \
    && rm -rf /tmp/wal2json