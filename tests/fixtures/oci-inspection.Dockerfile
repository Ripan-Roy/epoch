FROM scratch

ARG RUNTIME_USER=65532:65532
ARG EPOCH_VERSION=0.1.0-test
ARG VCS_REF=fixture-revision
ARG SOURCE_URL=https://github.com/Ripan-Roy/epoch

LABEL org.opencontainers.image.title="Epoch CLI" \
      org.opencontainers.image.description="OCI inspection contract fixture" \
      org.opencontainers.image.url="${SOURCE_URL}" \
      org.opencontainers.image.source="${SOURCE_URL}" \
      org.opencontainers.image.documentation="${SOURCE_URL}/blob/main/docs/CLI.md" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.version="${EPOCH_VERSION}" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.vendor="Epoch"

USER ${RUNTIME_USER}
ENTRYPOINT ["/usr/local/bin/epoch"]
