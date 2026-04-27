# ==========================================
# Stage 1: The Builder
# ==========================================
# Change this line:
FROM golang:alpine AS builder
RUN apk --no-cache add ca-certificates
WORKDIR /app

# Copy the module files and download dependencies
COPY go.mod ./
# COPY go.sum ./ (Uncomment this later when you add external packages like Redis)

# Copy the actual code
COPY . .

# Build the Go binary for the cloud (AMD64 Linux)
# CGO_ENABLED=0 ensures it's a completely standalone, statically linked binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /pisces-gateway ./cmd/server

# ==========================================
# Stage 2: The Final Scratch Image
# ==========================================
# 'scratch' is an empty Docker image. No OS, no shell, no utilities.
FROM scratch

# Copy the SSL certificates from the builder so Go can make HTTPS requests
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy ONLY the compiled binary from the builder stage
COPY --from=builder /pisces-gateway /pisces-gateway

# Expose the port your server listens on
EXPOSE 8080

# Execute the binary
ENTRYPOINT ["/pisces-gateway"]