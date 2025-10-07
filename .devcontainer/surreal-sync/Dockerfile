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
    postgresql-client \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

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

# Install Rust components
# This needs to be done as the non-root user to ensure the components are installed in the correct location
# Otherwise, you will get:
#   Error: error: 'cargo-fmt' is not installed for the toolchain
RUN rustup component add --toolchain 1.88.0-x86_64-unknown-linux-gnu rustfmt clippy
