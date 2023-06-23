FROM alpine
RUN apk add --no-cache minio minio-client
COPY images/minio/config/mc.json /root/.mcli/config.json
ENV MINIO_ACCESS_KEY {{ conf.s3.access_key_id }}
ENV MINIO_SECRET_KEY {{ conf.s3.access_secret_key }}
VOLUME ["/export"]
CMD ["minio", "server", "/export"]
