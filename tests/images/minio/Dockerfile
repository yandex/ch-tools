FROM bitnamilegacy/minio:2025.7.23-debian-12-r3

USER root

ENV MINIO_ACCESS_KEY {{ conf.s3.access_key_id }}
ENV MINIO_SECRET_KEY {{ conf.s3.access_secret_key }}

ENTRYPOINT ["minio"]

HEALTHCHECK --interval=30s --timeout=5s CMD curl --fail --silent http://localhost:9000/minio/health/live

EXPOSE 9000

CMD ["server", "/export"]
