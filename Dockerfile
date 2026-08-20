# Build stage
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# CGO_ENABLED=0 -> static binary; linux nonroot-friendly.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /email-sync .

# Runtime: distroless nonroot (uid 65532) — matches homelab hardened pattern.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /email-sync /email-sync
ENV PORT=8080 DB_PATH=/data/email.db
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/email-sync"]
