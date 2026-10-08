import os
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def _patterns():
    return [
        "I" + "s" + "h" + "a" + "n", "claud" + "ex", "sl" + "c", "unto" + "-",
        "/Users/" + "i" + "shan", "/home/" + "i" + "shan", "Har" + "ness",
        "scratch" + "pad", "research" + "/", "i" + "shan" + ".kar" + "@" + "gmail.com",
    ]

class PrivacyScanTests(unittest.TestCase):
    def test_no_private_identifiers_in_repository_files(self):
        hits = []
        for path in ROOT.rglob("*"):
            if not path.is_file() or ".git" in path.parts or "__pycache__" in path.parts:
                continue
            data = path.read_text(encoding="utf-8", errors="replace")
            for pattern in _patterns():
                if re.search(re.escape(pattern), data, re.IGNORECASE):
                    hits.append(f"{path.relative_to(ROOT)}: {pattern}")
        self.assertEqual(hits, [], "private identifiers found: " + ", ".join(hits))
