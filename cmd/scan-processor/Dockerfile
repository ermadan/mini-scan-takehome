FROM golang:1.20-alpine AS builder

# Build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
RUN apk add --no-cache gcc musl-dev
RUN go build -o scan-processor ./cmd/scan-processor

# Copy binary into slim image
FROM alpine
WORKDIR app
COPY --from=builder /src/scan-processor .
CMD ["/app/scan-processor"]
