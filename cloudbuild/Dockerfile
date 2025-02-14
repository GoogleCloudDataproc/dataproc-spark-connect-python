FROM python:3.10-bookworm

WORKDIR /opt/tests
COPY . .
RUN pip install -U pip && pip install --no-cache-dir -r requirements-dev.txt -r requirements-test.txt
RUN python setup.py sdist bdist_wheel
