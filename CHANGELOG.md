# Changelog

## [1.0.0rc3](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v1.0.0rc2...v1.0.0rc3) (2025-08-21)


### Features

* Add server/client runtime version compatibility check ([#128](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/128)) ([065cb50](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/065cb509842ab97795965d70f0d6f989c0f12317))


### Bug Fixes

* Fix per-operation Spark UI link display logic ([#131](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/131)) ([547b2fe](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/547b2fe19a6505ad868b0bbde9b32d8e3fdb97ba))

## [1.0.0rc2](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v1.0.0rc1...v1.0.0rc2) (2025-08-18)


### Features

* Per-operation execution progress bar ([#129](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/129)) ([ba68104](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/ba68104903c767f0499b51cac05db7142b362f64))

## [1.0.0rc1](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.9.0...v1.0.0rc1) (2025-08-05)


### ⚠ BREAKING CHANGES

* Upgrade to Spark 4 client ([#111](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/111))

### Features

* Upgrade to Spark 4 client ([#111](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/111)) ([931b560](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/931b56015cb83ad6db19df8a7219c9d829dd2f08))

## [0.9.0](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.8.3...v0.9.0) (2025-07-31)


### Features

* add builder methods for session configuration ([#115](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/115)) ([3a6a218](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/3a6a21883782ccc8b62624906bee53e0cb708e64))
* Add Python version check and warning ([#114](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/114)) ([65e8d4d](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/65e8d4d83c35e02a0ed3cac66c95db5cb5255a9f))
* Add view session details button ([#123](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/123)) ([01b8f05](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/01b8f05e6fbff2d359fe01924fb2e0d212d34402))
* Refactor and centralize client environment detection ([#117](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/117)) ([3b1a0a0](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/3b1a0a0c928b89773ee583aa2303d1f334dbc82d))


### Bug Fixes

* Detect only IDE (not plugins) in the `environment.py` ([#125](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/125)) ([4733981](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/47339810b8015169951fd418fe11e3de5f78359f))

## [0.8.3](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.8.2...v0.8.3) (2025-07-21)


### Bug Fixes

* Update per-operation Spark UI URL print logic to skip SELECT clause ([#118](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/118)) ([c01f9a5](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/c01f9a54d3c0101b6d6cd54d4653338b7321af39))

## [0.8.2](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.8.1...v0.8.2) (2025-07-08)


### Bug Fixes

* Set SPARK_CONNECT_MODE_ENABLED env var ([#112](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/112)) ([f3e0b7a](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/f3e0b7a482146aa25420c10c3d1b21cde1b576ed))

## [0.8.1](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.8.0...v0.8.1) (2025-07-01)


### Features

* Hyperlink Session URL on Session creation ([#105](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/105)) ([58d3cdb](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/58d3cdbbab0d6a08e528451bb1bdfc3605d30ecc))


### Bug Fixes

* Don't show IPython display object for non-interactive IPython Environment ([#106](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/106)) ([832d018](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/832d01890d7401c11bcca9bf8e8ac1573a70a98e))
* Fix unit tests providing needed mock dependencies ([#110](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/110)) ([d6e2ed6](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/d6e2ed619cbce8c31ef01f1cb55bdf780c1a3cdf))
* Hide 'View Session Details' button from the output. ([#107](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/107)) ([a36df90](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/a36df90ea3b465a007d96e388ba12022b2be6ae4))

## [0.8.0](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.7.5...v0.8.0) (2025-06-13)


### Features

* Add 'View Session Details' button on Session create. ([#88](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/88)) ([54e42b7](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/54e42b7324e291ae6353266e1b32f093937ab981))
* Add colab-notebook-id label to Dataproc Spark Connect session ([#94](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/94)) ([66883f7](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/66883f7a1c8c3ef98a450fc99780467d67961e04))
* Per-operation Spark UI URL ([#91](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/91)) ([09392c7](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/09392c7bc30ccd3ff93212a04d63cdce04f6f170))


### Bug Fixes

* Extract Colab notebook ID from full path ([#103](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/103)) ([c209798](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/c209798476dc66e31fd6524846cb216b14ab5572))
* Validate Colab notebook ID for Dataproc session labels ([#104](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/104)) ([d9fbf51](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/d9fbf518f2eb6b11bc1a1a519522e304c78f4a6f))

## [0.7.5](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.7.4...v0.7.5) (2025-05-27)


### Bug Fixes

* Pin PySpark dependency until new version is supported ([#92](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/92)) ([0d238f3](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/0d238f3568d4efeff332227e7ef048a7e898346b))

## [0.7.4](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.7.3...v0.7.4) (2025-05-16)


### Features

* Add default datasource config via env var ([#87](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/87)) ([71c3c1a](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/71c3c1ae42566b7b55d303c0e98cf12092aa3369))

## [0.7.3](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.7.2...v0.7.3) (2025-05-08)


### Bug Fixes

* change default runtime version to 2.3 ([#82](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/82)) ([7796cfe](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/7796cfee4fa0d0b3c4ea0398d09e516b0df3b4c6))
* use event to stop pbar thread ([#80](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/80)) ([14c9e01](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/14c9e013577c215a29c2f3851a7b6e853dc0eba1))

## [0.7.2](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.7.1...v0.7.2) (2025-04-28)


### Bug Fixes

* stop session creation progress bar on cell termination ([#78](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/78)) ([ebf71c8](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/ebf71c8163c5f3367f090dfab2a0dc0dbe0d941a))

## [0.7.1](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.7.0...v0.7.1) (2025-04-28)


### Bug Fixes

* remove version validation for SC client/server ([#76](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/76)) ([7c9a4e6](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/7c9a4e6f30360c64f3aa3cddd29edc50ed49e37d))

## [0.7.0](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.6.0...v0.7.0) (2025-04-24)


### Features

* display progress bar during session creation ([#70](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/70)) ([bd74d93](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/bd74d937702c1f2897023e4f3980996d7696fc9c))
* Migrate to env vars from the config file for defaults configuration ([#73](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/73)) ([5be4355](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/5be4355c0d27387fc22f9147269af98362a28622))


### Bug Fixes

* fix unit test run from hanging indefinitely ([#71](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/71)) ([eb78141](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/eb781413ff8728fffe88b2f9e31eae0e221a30cf))
* improve log and output messages consistency ([#74](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/74)) ([6558119](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/655811908b20695d86dc3bab9569cf058ef616d2))
* minor code cleanup and output fixes ([#68](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/68)) ([474e15c](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/474e15c57093fc958965f9cc365bbc34fc2e0302))

## [0.6.0](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/compare/v0.5.6...v0.6.0) (2025-04-22)


### ⚠ BREAKING CHANGES

* rename code and Python package to dataproc-spark-connect ([#66](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/66))

### Code Refactoring

* rename code and Python package to dataproc-spark-connect ([#66](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/66)) ([aa58789](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/aa58789d67d31ccb62c1f711b37526bd935ecf77))

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
* only use error message to throw RuntimeError ([#51](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/issues/51)) ([1f9136e](https://github.com/GoogleCloudDataproc/dataproc-spark-connect-python/commit/1f9136eda682cade422bc1fae68cc2b23273888c))
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
