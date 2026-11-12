# EpochFlow binaries on Ubuntu 24.04, built with the same apt packages as local
# development (docs/decisions/0001-cpp-toolchain-and-dependencies.md).

FROM ubuntu:24.04 AS base
ENV DEBIAN_FRONTEND=noninteractive
# libpqxx 8 comes from the PostgreSQL (PGDG) repository, as in CI.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates postgresql-common \
 && /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y \
 && rm -rf /var/lib/apt/lists/*

FROM base AS build
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      g++ cmake ninja-build pkg-config \
      libgrpc++-dev protobuf-compiler-grpc libprotobuf-dev protobuf-compiler \
      libpqxx-dev libgtest-dev libgmock-dev \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY CMakeLists.txt CMakePresets.json ./
COPY proto proto
COPY sql sql
COPY src src
COPY apps apps
COPY tests tests
# Two parallel compile jobs (preset) keep peak memory low.
RUN cmake --preset dev -DCMAKE_BUILD_TYPE=Release \
 && cmake --build --preset dev --target epochflow-coordinator epochflow-worker epochflowctl

FROM base AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      libgrpc++1.51t64 libprotobuf32t64 libpqxx-8.0 postgresql-client \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --no-create-home epochflow
COPY --from=build /src/build/bin/ /usr/local/bin/
COPY scripts/check-invariants.sql /usr/local/share/epochflow/check-invariants.sql
USER epochflow
