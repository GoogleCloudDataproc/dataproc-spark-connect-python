# Dataproc Spark Connect Client

A wrapper of the Apache [Spark Connect](https://spark.apache.org/spark-connect/)
client with additional functionalities that allow applications to communicate
with a remote Dataproc Spark Session using the Spark Connect protocol without
requiring additional steps.

## Install

```sh
pip install dataproc_spark_connect
```

## Uninstall

```sh
pip uninstall dataproc_spark_connect
```

## Setup

This client requires permissions to
manage [Dataproc Sessions and Session Templates](https://cloud.google.com/dataproc-serverless/docs/concepts/iam).
If you are running the client outside of Google Cloud, you must set following
environment variables:

* `GOOGLE_CLOUD_PROJECT` - The Google Cloud project you use to run Spark
  workloads
* `GOOGLE_CLOUD_REGION` - The Compute
  Engine [region](https://cloud.google.com/compute/docs/regions-zones#available)
  where you run the Spark workload.
* `GOOGLE_APPLICATION_CREDENTIALS` -
  Your [Application Credentials](https://cloud.google.com/docs/authentication/provide-credentials-adc)

## Usage

1. Install the latest version of Dataproc Python client and Dataproc Spark
   Connect modules:

   ```sh
   pip install google_cloud_dataproc dataproc_spark_connect --force-reinstall
   ```

2. Add the required imports into your PySpark application or notebook and start
   a Spark session with the following code instead of using
   environment variables:

   ```python
   from google.cloud.dataproc_spark_connect import DataprocSparkSession
   from google.cloud.dataproc_v1 import Session
   session_config = Session()
   session_config.environment_config.execution_config.subnetwork_uri = '<subnet>'
   session_config.runtime_config.version = '2.2'
   spark = DataprocSparkSession.builder.dataprocSessionConfig(session_config).getOrCreate()
   ```

## Developing

For development instructions see [guide](DEVELOPING.md).

## Contributing

We'd love to accept your patches and contributions to this project. There are
just a few small guidelines you need to follow.

### Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License
Agreement. You (or your employer) retain the copyright to your contribution;
this simply gives us permission to use and redistribute your contributions as
part of the project. Head over to <https://cla.developers.google.com> to see
your current agreements on file or to sign a new one.

You generally only need to submit a CLA once, so if you've already submitted one
(even if it was for a different project), you probably don't need to do it
again.

### Code reviews

All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests.
