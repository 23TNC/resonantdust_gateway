# Build image for the gate.
#
# The SpacetimeDB SDK pulls in native-tls/openssl-sys, which needs pkg-config
# and the OpenSSL headers to compile. rust:slim ships neither, so add them
# here. The *runtime* needs only libssl.so.3, which rust:slim already has — so
# the `gate` service runs on the stock image and only `build` uses this one.
FROM rust:slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends pkg-config libssl-dev \
 && rm -rf /var/lib/apt/lists/*
