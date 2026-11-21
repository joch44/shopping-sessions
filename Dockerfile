# Stage 1: the fast path (Team 67's Express Lane design, MIT; see README).
FROM rust:1.100.0-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo .cargo
COPY src src
RUN cargo build --release --target x86_64-unknown-linux-musl \
 && cp target/x86_64-unknown-linux-musl/release/pipeline /pipeline

# Stage 2: the general pipeline (ours) that the fast path hands off to when the
# data breaks its assumptions (long or opaque IDs, unusual categories, ...).
FROM rust:1.100.0-slim-bookworm AS fallback
WORKDIR /src
COPY fallback/Cargo.toml fallback/Cargo.lock ./
COPY fallback/src ./src
COPY fallback/pgo ./pgo
ENV RUSTFLAGS="-C target-cpu=cascadelake -C target-feature=+crt-static -Cprofile-use=/src/pgo/merged.profdata"
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/pipeline /fallback \
 && chmod 0755 /fallback

# Stage 3: the same general pipeline for hosts without AVX-512 (src/cpuguard.rs picks one
# before any AVX-512 code runs): x86-64-v3 (AVX2) and x86-64-v2.
FROM fallback AS portable
RUN RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static" cargo build --release --locked --target x86_64-unknown-linux-gnu --target-dir /t3 \
 && cp /t3/x86_64-unknown-linux-gnu/release/pipeline /fallback-v3 \
 && RUSTFLAGS="-C target-cpu=x86-64-v2 -C target-feature=+crt-static" cargo build --release --locked --target x86_64-unknown-linux-gnu --target-dir /t2 \
 && cp /t2/x86_64-unknown-linux-gnu/release/pipeline /fallback-v2 \
 && chmod 0755 /fallback-v3 /fallback-v2

FROM scratch
COPY --from=build /pipeline /pipeline
COPY --from=fallback /fallback /fallback
COPY --from=portable /fallback-v3 /fallback-v3
COPY --from=portable /fallback-v2 /fallback-v2
USER 1000:1000
CMD ["/pipeline"]
