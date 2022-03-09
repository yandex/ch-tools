FROM ch-tools-tests-base

COPY ./bin/minio /usr/bin/minio
COPY ./bin/mc /usr/bin/mc
RUN chmod +x /usr/bin/minio /usr/bin/mc

ADD images/minio/config /config/

COPY images/minio/config/minio.supervisor.conf /etc/supervisor/conf.d/minio.conf
COPY images/minio/config/mc.json /root/.mc/config.json

ENV MINIO_ACCESS_KEY {{ conf.s3.access_key_id }}
ENV MINIO_SECRET_KEY {{ conf.s3.access_secret_key }}

VOLUME ["/export"]

EXPOSE 9000

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/supervisord.conf"]
