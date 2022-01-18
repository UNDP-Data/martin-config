FROM --platform=linux/amd64 alpine as azcopy
RUN apk add --no-cache wget \
&&	wget https://aka.ms/downloadazcopy-v10-linux -O /tmp/azcopy.tgz \
&&	export BIN_LOCATION=$(tar -tzf /tmp/azcopy.tgz | grep "/azcopy") \
&&	tar -xzf /tmp/azcopy.tgz $BIN_LOCATION --strip-components=1 -C /usr/bin

FROM --platform=linux/amd64 python:3.8-slim as python-base
LABEL name="docker-azcopy-martin-config"
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

## for azcopy
COPY --from=azcopy /usr/bin/azcopy /usr/bin/azcopy

WORKDIR /home/undp/src
CMD ["/bin/bash"]