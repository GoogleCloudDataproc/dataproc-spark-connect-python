import json
import os
import unittest

from packaging.requirements import InvalidRequirement

from google.cloud.spark_connect.pypi_artifacts import PyPiArtifacts


class PyPiArtifactsTest(unittest.TestCase):

    @staticmethod
    def test_valid_inputs():
        file = PyPiArtifacts(
            {
                "spacy",
                "spacy==1.2.3",
                "spacy>=1.2",
                "spacy==1.2.*",
                "spacy==1.*",
            }
        ).dump_to_file("uuid")
        os.remove(file)

    def test_bad_format(self):
        with self.assertRaisesRegex(
            InvalidRequirement,
            "Expected end or semicolon \(after name and no valid version specifier\).*",
        ):
            PyPiArtifacts({"pypi://spacy:23"})

    def test_validate_file_content(self):
        file_path = PyPiArtifacts(
            {"spacy==1.2", "pkg>=1.2", "abc"}
        ).dump_to_file("uuid")
        actual = json.load(open(file_path))
        self.assertEqual("0.5.0", actual["client-version"])
        self.assertEqual("PYPI", actual["type"])
        self.assertEqual(
            ["abc", "pkg>=1.2", "spacy==1.2"], sorted(actual["packages"])
        )
