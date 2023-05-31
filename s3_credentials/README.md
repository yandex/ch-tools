# ch-s3-credentials

A ClickHouse S3 credentials management tool.
It provides ability to check and update S3 credentials for ClickHouse over S3 installations.

## Check

Check outputs in following format:
```
<status code>;<message>
```
Where `<status code>` is one of
- `0` - OK
- `1` - WARN
- `2` - CRIT

## Update

Updates endpoint and token in ClickHouse config required for interaction with S3.
