FROM golang:1.24-alpine AS build
WORKDIR /src
RUN apk add --no-cache ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/ledger ./cmd/ledger
RUN CGO_ENABLED=0 go build -o /out/reconciler ./cmd/reconciler

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /out/ledger /app/ledger
COPY --from=build /out/reconciler /app/reconciler
ENTRYPOINT ["/app/ledger"]
