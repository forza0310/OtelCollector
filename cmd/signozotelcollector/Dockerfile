# use a minimal debian image
FROM debian:bookworm-slim

# Configurable Debian apt mirror. Defaults to Aliyun because the upstream
# deb.debian.org is unreachable from the typical build host for this fork.
# Override for CI / non-China builds: --build-arg APT_MIRROR=deb.debian.org
ARG APT_MIRROR=mirrors.aliyun.com

# add ca-certificates in case you need them
RUN sed -i "s|deb.debian.org|${APT_MIRROR}|g" /etc/apt/sources.list.d/debian.sources \
 && apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# define arguments and default values
ARG TARGETOS TARGETARCH
ARG USER_UID=10001

# create a non-root user for running the collector
USER ${USER_UID}

# copy the binaries from the multi-stage build
COPY .build/${TARGETOS}-${TARGETARCH}/signoz-otel-collector /signoz-otel-collector

# copy the config file
COPY conf/default.yaml /etc/otel/config.yaml

# expose OTLP ports for the collector
EXPOSE 4317 4318

# run the binary as the entrypoint and pass the default config file as a flag
ENTRYPOINT [ "/signoz-otel-collector" ]
CMD ["--config", "/etc/otel/config.yaml"]
