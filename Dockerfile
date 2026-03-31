FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# We use an ARG to decide which binary to build (brain, muscle, or client)
ARG APP
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/app ./cmd/${APP}

# Run Stage
FROM alpine:latest

COPY --from=builder /bin/app /usr/local/bin/app

WORKDIR /app

EXPOSE 8080

CMD ["app"]