FROM python:3.10-slim

ARG project_dir=/code
RUN mkdir /code
WORKDIR $project_dir
ARG env_file=$project_dir/.env

RUN apt-get update && apt-get install -y \
    curl  \
    gcc  \
    python3-dev  \
    vim \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN curl -sSL https://install.python-poetry.org | python -
ENV PATH /root/.local/bin:$PATH
RUN pip install --upgrade pip
COPY pyproject.toml .
RUN poetry config virtualenvs.create false && poetry install
