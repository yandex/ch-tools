ARG CLICKHOUSE_VERSION
FROM chtools/test-clickhouse:$CLICKHOUSE_VERSION

COPY dist/*.whl /
RUN pip3 install *.whl
