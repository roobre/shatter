FROM golang:1.21-alpine as builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN go build -o /bin/refractor ./cmd

FROM alpine:3.18.4

COPY --from=builder /bin/refractor /bin
COPY --from=builder /src/refractor.yaml /config/refractor.yaml

ENTRYPOINT ["/bin/refractor"]
CMD ["-config", "/config/refractor.yaml"]
