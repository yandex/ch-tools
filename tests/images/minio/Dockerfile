FROM clickhouse/minio-minio:RELEASE.2024-09-13T20-26-02Z

ENV MINIO_ACCESS_KEY {{ conf.s3.access_key_id }}
ENV MINIO_SECRET_KEY {{ conf.s3.access_secret_key }}

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

HEALTHCHECK --interval=30s --timeout=5s CMD curl --fail --silent http://localhost:9000/minio/health/live

EXPOSE 9000

CMD ["server", "/export"]
