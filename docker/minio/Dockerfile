# MinIO stopped publishing community Docker images on 2025-10-23, so we build pinned releases from source.
# The server tag is the last community line that ships the full web console (object browser UI).
FROM golang:1.24.13-alpine AS build
ARG MINIO_TAG=RELEASE.2025-04-22T22-12-26Z
ARG MC_TAG=RELEASE.2025-04-16T18-13-26Z
RUN apk add --no-cache git
ENV CGO_ENABLED=0 GOBIN=/out
RUN go install -trimpath -ldflags "-s -w" github.com/minio/minio@${MINIO_TAG} \
 && go install -trimpath -ldflags "-s -w" github.com/minio/mc@${MC_TAG}

FROM alpine:3.21.8
RUN apk add --no-cache ca-certificates curl bash
COPY --from=build /out/minio /usr/local/bin/minio
COPY --from=build /out/mc /usr/local/bin/mc
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
