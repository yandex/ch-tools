group "default" {
  targets = ["http-mock", "minio", "zookeeper", "clickhouse"]
}

target "http-mock" {
  dockerfile = "images/http_mock/http-mock.Dockerfile"
  tags = ["chtools/test-http-mock"]
  context = "tests"
  cache-from = [
    {
      type = "registry",
      ref = "chtools/test-http-mock"
    }
  ]
}

target "minio" {
  dockerfile = "images/minio/minio.Dockerfile"
  tags = ["chtools/test-minio"]
  context = "tests"
  cache-from = [
    {
      type = "registry",
      ref = "chtools/test-minio"
    }
  ]
}

target "zookeeper" {
  dockerfile = "images/zookeeper/zookeeper.Dockerfile"
  tags = ["chtools/test-zookeeper"]
  context = "tests"
  cache-from = [
    {
      type = "registry",
      ref = "chtools/test-zookeeper"
    }
  ]
}

variable "CLICKHOUSE_VERSIONS" {
  default = ""
}

target "clickhouse" {
  name = "clickhouse-${replace(ch_version, ".", "_")}"
  context = "tests"
  dockerfile = "images/clickhouse/clickhouse.Dockerfile"
  tags = ["chtools/test-clickhouse:${ch_version}"]
  matrix = { ch_version = split(",", replace("${CLICKHOUSE_VERSIONS}", " " , "")) }
  args = { CLICKHOUSE_VERSION = "${ch_version}" }
  cache-from = [
    {
      type = "registry",
      ref = "chtools/test-clickhouse:${ch_version}"
    }
  ]
}
