FROM postgres:9.4

COPY postgresql.conf /tmp/
COPY init.sh /docker-entrypoint-initdb.d/
COPY init.sql /docker-entrypoint-initdb.d/