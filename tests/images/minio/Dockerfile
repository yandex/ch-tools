FROM alpine

ENV MINIO_ACCESS_KEY={{ conf.s3.access_key_id }}
ENV MINIO_SECRET_KEY={{ conf.s3.access_secret_key }}

RUN apk add --no-cache minio minio-client
COPY tests/images/minio/config/mc.json /root/.mcli/config.json
VOLUME ["/export"]
CMD ["minio", "server", "/export"]
