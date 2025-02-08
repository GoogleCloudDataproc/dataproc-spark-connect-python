import json
import logging
import os
import tempfile

from packaging.requirements import Requirement

logger = logging.getLogger(__name__)


class PyPiArtifacts:

    @staticmethod
    def __try_parsing_package(packages: set[str]) -> list[Requirement]:
        reqs = [Requirement(p) for p in packages]
        if 0 in [len(req.specifier) for req in reqs]:
            logger.info("It is recommended to pin the version of the package")
        return reqs

    def __init__(self, packages: set[str]):
        self.requirements = PyPiArtifacts.__try_parsing_package(packages)

    def dump_to_file(self, s8s_uuid: str) -> str:
        dependencies = {
            "client-version": "0.5.0",
            "type": "PYPI",
            "packages": [str(req) for req in self.requirements],
        }

        # Can't use the same file as Spark throws exception that file already exists
        file_path = os.path.join(
            tempfile.tempdir,
            ".deps-" + s8s_uuid + "-" + self.__str__() + ".json",
        )

        with open(file_path, "w") as json_file:
            json.dump(dependencies, json_file, indent=4)
        return file_path
