import importlib.util
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("railpull_crawl", ROOT / "ntes" / "crawl.py")
crawl = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(crawl)


class NtesErrorClassificationTests(unittest.TestCase):
    def test_empty_search_result_is_semantic_not_transient(self):
        error = Exception("NTESError: request failed: No match found for 995 !")

        self.assertFalse(crawl.is_transient_ntes_error(error))

    def test_transport_failure_remains_transient(self):
        error = Exception("NTESError: request failed: connection timed out")

        self.assertTrue(crawl.is_transient_ntes_error(error))


if __name__ == "__main__":
    unittest.main()
