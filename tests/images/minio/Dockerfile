FROM ch-tools-tests-base

COPY ./bin/minio /usr/bin/minio
COPY ./bin/mc /usr/bin/mc
RUN chmod +x /usr/bin/minio /usr/bin/mc

ADD config /config/

COPY ./config/minio.supervisor.conf /etc/supervisor/conf.d/minio.conf
COPY ./config/mc.json /root/.mc/config.json

RUN mkdir -m 750 -p /etc/nginx/ssl && chown root:www-data /etc/nginx/ssl
RUN ln -s /config/cert.pem /etc/nginx/ssl/cert.pem
RUN ln -s /config/key.pem /etc/nginx/ssl/key.pem

ENV MINIO_ACCESS_KEY {{ conf.s3.access_key_id }}
ENV MINIO_SECRET_KEY {{ conf.s3.access_secret_key }}

VOLUME ["/export"]

EXPOSE 9000

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/supervisord.conf"]
