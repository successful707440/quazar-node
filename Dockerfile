FROM rust:1.96 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/quazar_registry /app/
EXPOSE 8080
ENV DATABASE_URL=postgres://quazar:quazar@postgres:5432/quazar
ENV QUAZAR_NODE_ID=QZ-NODE
ENV QUAZAR_NODE_URL=http://localhost:8080
ENV QUAZAR_PORT=8080
CMD ["/app/quazar_registry"]
