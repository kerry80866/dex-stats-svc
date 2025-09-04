ARG BASE_IMAGE=registry.cn-hongkong.aliyuncs.com/lbank/alpine:v2
FROM ${BASE_IMAGE}
ARG ENV
WORKDIR /app

# 复制 go build 的二进制产物到docker
COPY ./cmd/dex-stats-svc/dex-stats-svc .

# 复制对应环境的配置文件（ENV=act/prod/test/press/pre等）
COPY ./etc/dex-stats-svc/${ENV}.yaml ./config.yaml

CMD ["./dex-stats-svc", "-f", "config.yaml"]
