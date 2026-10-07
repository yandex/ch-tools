# chadmin

ClickHouse administration tool.

For getting list of available command, run
```shell
$ chadmin -h
```

To generate S3 credentials for multiple endpoints, repeat `--endpoint`:

```shell
chadmin s3-credentials-config update \
  --endpoint https://storage.yandexcloud.net \
  --endpoint https://storage.pe.yandexcloud.net
```

The first endpoint uses the existing `cloud_storage` XML section. Additional
endpoints use `cloud_storage_1`, `cloud_storage_2`, and so on. Duplicate endpoints
are ignored, and all sections share one IAM token obtained per update. Each update
replaces the generated configuration, removing sections for omitted endpoints.
