# Developing

To get started, create a fresh `venv` or conda environment. Then, update `pip`
and install the required dev dependencies within the new environment.

```
pip install -U pip
pip install -r requirements-dev.txt
# Install the test dependencies to run unit and integration tests.
pip install -r requirements-test.txt
```

# Linting/formatting

We use `pyink` to lint/format the code. To apply changes to your local
environment, run:

```
pyink .
```

This will ensure that your changes pass CI code linting.

# Testing

We use `pytest` for testing. To run _all_ tests, simply run `pytest`. For this
to work, you have to have your ambient environment configured to correctly
resolve all configuration details such as GCP project, region, etc.

To make testing more deterministic, it is instead recommended to specify
configuration details on the command line. For example:

```
env GOOGLE_CLOUD_SUBNET='subnet-id' \
  GOOGLE_CLOUD_PROJECT='project-id'
  GOOGLE_CLOUD_REGION='us-central1' \
  pytest --tb=auto -v
```

The integration tests in particular can take a while to run. To speed up the
testing cycle, you can run them in parallel. You can do so using the `xdist`
plugin by setting the `-n` flag to the number of parallel runners you want to
use. This will be set automatically if you set it to `auto`. For example:

```
env GOOGLE_CLOUD_SUBNET='subnet-id' \
  GOOGLE_CLOUD_PROJECT='project-id'
  GOOGLE_CLOUD_REGION='us-central1' \
  pytest -n auto --tb=auto -v
```
