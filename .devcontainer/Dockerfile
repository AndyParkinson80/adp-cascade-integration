FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /workspace

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libffi-dev \
    libssl-dev \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

COPY ../requirements.txt .

RUN pip install -r requirements.txt && rm -rf ~/.cache/pip

ARG USERNAME=vscode
RUN useradd -m $USERNAME && chown -R $USERNAME /workspace
USER $USERNAME
