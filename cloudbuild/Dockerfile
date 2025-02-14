FROM python:3.10-bookworm

WORKDIR /opt/tests
# NOTE: We copy the requirements files explicitly _before_ installing
# dependencies to allow layer caching. Afterward, we copy the rest of the build
# sources. The intention is to reuse the preinstalled dependencies when
# individual source files change.
COPY requirements-dev.txt ./
COPY requirements-test.txt ./
RUN python3 -m pip install -U --no-cache-dir -r requirements-dev.txt -r requirements-test.txt pip
COPY . .
RUN python setup.py sdist bdist_wheel egg_info
RUN ls -lR
