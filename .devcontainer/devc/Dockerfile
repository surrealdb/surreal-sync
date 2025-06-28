# https://github.com/devcontainers/images/tree/main/src/typescript-node
FROM mcr.microsoft.com/devcontainers/typescript-node:22-bullseye

# Set up the working directory
WORKDIR /workspace

# # Create a non-root user if it doesn't exist
# ARG USERNAME=vscode
# ARG USER_UID=1001
# ARG USER_GID=$USER_UID

# RUN if ! getent group $USERNAME > /dev/null 2>&1; then \
#         groupadd --gid $USER_GID $USERNAME; \
#     fi \
#     && if ! getent passwd $USERNAME > /dev/null 2>&1; then \
#         useradd --uid $USER_UID --gid $USER_GID -m $USERNAME; \
#     fi \
#     && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
#     && chmod 0440 /etc/sudoers.d/$USERNAME

# # Switch to non-root user
# USER $USERNAME

# https://github.com/devcontainers/cli
RUN npm install -g @devcontainers/cli
