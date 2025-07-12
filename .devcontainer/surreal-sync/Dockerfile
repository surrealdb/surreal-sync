FROM mcr.microsoft.com/devcontainers/rust:1-bookworm

# Install required system dependencies
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    build-essential \
    cmake \
    libssl-dev \
    pkg-config \
    protobuf-compiler \
    curl \
    wget \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

# Install Rust components
RUN rustup component add rustfmt clippy

# Create a non-root user if it doesn't exist
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN if ! getent group $USERNAME > /dev/null 2>&1; then \
        groupadd --gid $USER_GID $USERNAME; \
    fi \
    && if ! getent passwd $USERNAME > /dev/null 2>&1; then \
        useradd --uid $USER_UID --gid $USER_GID -m $USERNAME; \
    fi \
    && apt-get update \
    && apt-get install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

# Set up the working directory
WORKDIR /workspace

# Switch to non-root user
USER $USERNAME

# This needs to be after the USER command to ensure permissions are correct
# For example, you might see random permission error like the below if you cargo-install BEFORE the USER command:
#
# $ cargo test
#     Updating crates.io index
# warning: failed to write cache, path: /usr/local/cargo/registry/index/index.crates.io-1949cf8c6b5b557f/.cache/bs/on/bson, error: Permission denied (os error 13)
#   Downloaded allocator-api2 v0.2.21
# error: failed to open `/usr/local/cargo/registry/cache/index.crates.io-1949cf8c6b5b557f/allocator-api2-0.2.21.crate`
#
# Caused by:
#   Permission denied (os error 13)
RUN cargo install cargo-edit && cargo install cargo-make
