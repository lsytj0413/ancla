FROM mcr.microsoft.com/devcontainers/rust:1-1-bullseye

RUN sed -i 's|http://deb.debian.org/|http://mirrors.aliyun.com/|g' /etc/apt/sources.list \
    && apt-get update