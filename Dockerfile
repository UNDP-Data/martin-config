FROM --platform=linux/amd64 python:3.8-slim as python-base
LABEL name="docker-martin-config"
LABEL maintainer="Jin Igarashi <jin.igarashi@undp.org>"

# Setup env
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1

WORKDIR /home/undp/src

# Install pipenv and compilation dependencies
RUN pip install pipenv
RUN apt-get update && apt-get install -y --no-install-recommends gcc wget

# Install python dependencies in /.venv
COPY Pipfile .
COPY Pipfile.lock .
RUN pipenv install --system

# Install application into container
COPY . .
RUN python setup.py install

ENV PYTHONPATH "${PYTHONPATH}:/home/undp/src/src"

WORKDIR /home/undp/src
CMD ["/bin/bash"]