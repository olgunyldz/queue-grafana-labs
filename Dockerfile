FROM golang:1.27.3-alpine3.24 AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/producer ./cmd/producer && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/consumer ./cmd/consumer

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/producer /producer
COPY --from=build /out/consumer /consumer
ENTRYPOINT ["/producer"]
