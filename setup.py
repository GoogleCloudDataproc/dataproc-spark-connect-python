# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from setuptools import find_namespace_packages, setup
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()


setup(
    name="google-spark-connect",
    version="0.4.1",
    description="Google client library for Spark Connect",
    long_description=long_description,
    author="Google LLC",
    url="https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python",
    license="Apache 2.0",
    packages=find_namespace_packages(include=["google.*"]),
    install_requires=[
        "google-api-core>=2.19.1",
        "google-cloud-dataproc>=5.15.1",
        "wheel",
        "websockets",
        "pyspark>=3.5",
        "pandas",
        "pyarrow",
    ],
)
