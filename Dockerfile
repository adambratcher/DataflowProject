FROM gcr.io/dataflow-templates-base/python38-template-launcher-base as template-base

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}
# Credits to michaeloliverx
# https://github.com/michaeloliverx/python-poetry-docker-example/blob/master/docker/Dockerfile

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    PYSETUP_PATH="/opt/pysetup" \
    VENV_PATH="/opt/pysetup/.venv"

ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

# builder-base is used to build dependencies
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        curl \
        build-essential

# Install Poetry
ENV POETRY_VERSION=1.1.13
RUN curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python

COPY poetry.lock pyproject.toml ${WORKDIR}/
RUN poetry remove apache-beam && poetry export --without-hashes -f requirements.txt --output requirements.txt
COPY src/main/python/ .

# Do not include `apache-beam` in requirements.txt
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/template/template.py"
ENV FLEX_TEMPLATE_PYTHON_PY_OPTIONS=""
ENV FLEX_TEMPLATE_PYTHON_EXTRA_PACKAGES="./template/,./utils/"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=""

# Install apache-beam and other dependencies to launch the pipeline
RUN pip install apache-beam[gcp]
RUN pip install -U -r ./requirements.txt
