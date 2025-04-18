# Changelog

## [0.5.6](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.5.5...v0.5.6) (2025-03-13)


### Bug Fixes

* return error message in _render_traceback_ ([#60](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/60)) ([9bc71d7](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/9bc71d7b2d4c081d0840ec72fdc1093920884584))

## [0.5.5](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.5.4...v0.5.5) (2025-03-12)


### Bug Fixes

* Check if the s8s session is terminated before creating connection ([#55](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/55)) ([9f7e806](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/9f7e806580e0e27a8041247eb19a84aff3019b6c))

## [0.5.4](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.5.3...v0.5.4) (2025-03-10)


### Bug Fixes

* catch PermissionDenied errors while creating session ([#53](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/53)) ([b0905cf](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/b0905cfd0916af2d92b892f206f6bc7275e78976))
* integration test table cleanup ([#58](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/58)) ([755de6d](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/755de6dc6f9da83702928b31356059f8c4d37104))
* only use error message to throw RuntimError ([#51](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/51)) ([1f9136e](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/1f9136eda682cade422bc1fae68cc2b23273888c))
* suppress stack trace for InvalidArgument and PermissionDenied ([#56](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/56)) ([87e5eb0](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/87e5eb0be1874d9909e55f8ffa0893677c67a6ac))
* unit test expected error message ([#54](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/54)) ([e43c8bd](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/e43c8bddb28ca0c95c68dc19e0c8abfb24c34460))

## [0.5.3](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.5.2...v0.5.3) (2025-02-28)


### Bug Fixes

* fixed a bug in the TCP-over-websocket proxy where  one end of the connection might not be closed when the other end is closed. ([932cb48](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/932cb482646cbec4acaead035a4f68750e4878ae))
* Switch pyspark dependency to pyspark[connect] ([#43](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/43)) ([23aec22](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/23aec22642a7d7e11999047e31fef4f2b90dd67b))


### Documentation

* Add basic developer docs ([#44](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/44)) ([d77626b](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/d77626b85d786660c20e0a8fd7f05decc64cf334))

## [0.5.2](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.5.1...v0.5.2) (2025-02-11)


### Bug Fixes

* append project to session URIs and use location instead of the region in the builder ([#40](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/40)) ([b7134d9](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/b7134d90722f22b8511c46f861737f968fdb8eb0))

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
* Rename package to google.cloud.dataproc_spark_connect.GoogleSparkSession

### Bug Fixes

* Remove unused/invalid "spark" field in session config proto ([d349b15](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/d349b159e7f00072f4003830246815a23b86d3be))
* terminate s8s session on kernel termination ([#24](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/24)) ([beeaa98](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/beeaa98ef60fa6f92c490ba0e8a69945c8bbf0b4))


### Code Refactoring

* rename package and class to google.cloud.dataproc_spark_connect.GoogleSparkSession ([#21](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/21)) ([313dba4](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/313dba423f80c5b15535a40e239db1ab6e886ace))
* rename python package to google-spark-connect ([#25](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/25)) ([357d1fe](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/357d1fe8383040e506a251aa7b3af99a07752058))

## [0.2.0](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.1.0...v0.2.0) (2024-12-05)


### Miscellaneous Chores

* release 0.2.0 ([78da6a1](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/78da6a180cfed022e66b53c642f025e5c015af1f))

## 0.1.0 (2024-12-05)


### Bug Fixes

* Rename /regions/ path to /locations/ ([8d7e4c8](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/8d7e4c88497eebb06949c6319b0e995a3f27ef0b))
* Rename /regions/ path to /locations/ in client ([#13](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/13)) ([f51151a](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/f51151a4f0eb63af9c593881f199a01a9c004023))
