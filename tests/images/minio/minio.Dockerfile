FROM alpine
RUN apk add --no-cache minio minio-client
COPY images/minio/config/mc.json /root/.mcli/config.json
VOLUME ["/export"]
CMD ["minio", "server", "/export"]
