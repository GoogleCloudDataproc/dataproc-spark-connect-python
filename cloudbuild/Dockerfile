FROM python:3.10-bookworm

WORKDIR /opt/tests
# NOTE: We copy the requirements files explicitly _before_ installing
# dependencies to allow layer caching. Afterward, we copy the rest of the build
# sources. The intention is to reuse the preinstalled dependencies when
# individual source files change.
COPY requirements-dev.txt ./
COPY requirements-test.txt ./
RUN python3 -m pip install -U pip
RUN python3 -m pip install --no-cache-dir -r requirements-dev.txt -r requirements-test.txt
COPY . .
RUN python setup.py sdist bdist_wheel egg_info
# TODO: Figure out how to get the .egg-info/ directory included in the output
# image. Something is suppressing it at the moment, so we have to `pip install`
# this package to allow tests to run.
RUN pip install .
