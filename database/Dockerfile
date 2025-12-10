FROM rust:1.91.1-bullseye as builder

WORKDIR /usr/src/database
COPY . .
WORKDIR /usr/src/database/api
RUN cargo install --path . --bin api

FROM debian:trixie-slim
COPY --from=builder /usr/local/cargo/bin/api /usr/local/bin/api
RUN mkdir /data
RUN api init /data
ENTRYPOINT ["bash", "-c", "api init /data && api serve /data 0.0.0.0:3000"]
EXPOSE 3000/tcp
