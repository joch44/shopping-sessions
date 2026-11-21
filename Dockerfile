# Shopping Sessions pipeline. Built natively by the scorer (linux/amd64).
# Stage 1 compiles a static binary for Cascade Lake (the scorer's minimum CPU,
# never `native`); stage 2 is an empty image holding only that binary, which
# starts faster than any distro image.
FROM rust:1.100.0-slim-bookworm AS build
WORKDIR /src
COPY pipeline/Cargo.toml pipeline/Cargo.lock ./
COPY pipeline/src ./src
# Profile-guided optimization: pipeline/pgo/merged.profdata, regenerated with tools/pgo.sh
# (make pgo) before each submission. It only changes code layout and inlining.
COPY pipeline/pgo ./pgo
ENV RUSTFLAGS="-C target-cpu=cascadelake -C target-feature=+crt-static -Cprofile-use=/src/pgo/merged.profdata"
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/pipeline /pipeline \
 && chmod 0755 /pipeline

FROM scratch
COPY --from=build /pipeline /pipeline
# The scorer runs as uid 1000 with a read-only root; the binary needs neither a
# home nor /tmp, and reads DATA_DIR / OUTPUT_DIR from the environment.
CMD ["/pipeline"]
