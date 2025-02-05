# Changelog

## [0.5.1](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.5.0...v0.5.1) (2025-02-05)


### Bug Fixes

* Support Spark Connect Server URIs w/o trailing / ([23dff94](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/23dff94c204548a61efd49d33f7525efc27d186b))

## [0.5.0](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.4.1...v0.5.0) (2025-02-04)


### ⚠ BREAKING CHANGES

* rename dataprocConfig to googleSessionConfig

### Bug Fixes

* rename dataprocConfig to googleSessionConfig ([f041e9d](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/f041e9d857d33634361bf2399a2f8b7790d04a76))

## [0.4.1](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.4.0...v0.4.1) (2025-01-31)


### Bug Fixes

* remove import reference of old package ([#31](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/31)) ([28651cb](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/28651cb78ebb2c8bc7910afd0c17d8081e7d78f5))

## [0.4.0](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.2.0...v0.4.0) (2025-01-29)


### ⚠ BREAKING CHANGES

* rename python package to google-spark-connect ([#25](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/25))
* Rename package to google.cloud.spark_connect.GoogleSparkSession

### Bug Fixes

* Remove unused/invalid "spark" field in session config proto ([d349b15](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/d349b159e7f00072f4003830246815a23b86d3be))
* terminate s8s session on kernel termination ([#24](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/24)) ([beeaa98](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/beeaa98ef60fa6f92c490ba0e8a69945c8bbf0b4))


### Code Refactoring

* rename package and class to google.cloud.spark_connect.GoogleSparkSession ([#21](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/21)) ([313dba4](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/313dba423f80c5b15535a40e239db1ab6e886ace))
* rename python package to google-spark-connect ([#25](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/25)) ([357d1fe](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/357d1fe8383040e506a251aa7b3af99a07752058))

## [0.2.0](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.1.0...v0.2.0) (2024-12-05)


### Miscellaneous Chores

* release 0.2.0 ([78da6a1](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/78da6a180cfed022e66b53c642f025e5c015af1f))

## 0.1.0 (2024-12-05)


### Bug Fixes

* Rename /regions/ path to /locations/ ([8d7e4c8](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/8d7e4c88497eebb06949c6319b0e995a3f27ef0b))
* Rename /regions/ path to /locations/ in client ([#13](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/13)) ([f51151a](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/f51151a4f0eb63af9c593881f199a01a9c004023))
