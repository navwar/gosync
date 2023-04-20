# build stage
FROM golang:1.20-alpine3.17 AS builder

RUN apk update && apk add --no-cache git make gcc g++ ca-certificates && update-ca-certificates

WORKDIR /src

COPY . .

RUN rm -f bin/gox bin/gosync_linux_amd64 && make bin/gox && bin/gox \
-cgo \
-os="linux" \
-arch="amd64" \
-ldflags "-s -w" \
-output "bin/{{.Dir}}_{{.OS}}_{{.Arch}}" \
github.com/deptofdefense/gosync/cmd/gosync

# final stage
FROM alpine:3.17

RUN apk update && apk add --no-cache ca-certificates && update-ca-certificates

COPY --from=builder /src/bin/gosync_linux_amd64 /bin/gosync

ENTRYPOINT ["/bin/gosync"]

CMD ["--help"]
