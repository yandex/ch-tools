ARG CLICKHOUSE_VERSION
FROM chtools/test-clickhouse:$CLICKHOUSE_VERSION

COPY *.whl /
RUN pip3 install *.whl
