"""Portfolio standard checks: the lock holds, the required files exist, no public grade."""
import hashlib
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Both files as committed in 15d4e12 (tag proposal-v1), before any code or result.
HYPOTHESES_SHA256 = "a651186e34ea344b722f9900657c417cc778a0d0269c39bda766128a711228b6"
# SPEC.md at the lock was 10,677 bytes; section 10 (amendments A1, A2, made before any
# trial) was appended after it in 7e31706. The locked bytes must stay a byte-exact prefix.
SPEC_LOCKED_BYTES = 10677
SPEC_LOCKED_SHA256 = "50305840e176b375ebf3121313f66824f80b7659c95354eb339939b4d2f0ec33"


def _read(name):
    with open(os.path.join(ROOT, name), "rb") as f:
        return f.read()


def test_hypotheses_unchanged_since_the_lock():
    assert hashlib.sha256(_read("HYPOTHESES.md")).hexdigest() == HYPOTHESES_SHA256, \
        "HYPOTHESES.md changed after its lock; record an amendment in DECISIONS.md instead"


def test_spec_locked_text_unchanged():
    spec = _read("SPEC.md")
    assert hashlib.sha256(spec[:SPEC_LOCKED_BYTES]).hexdigest() == SPEC_LOCKED_SHA256, \
        "the locked part of SPEC.md changed; append amendments instead"
    assert spec[SPEC_LOCKED_BYTES:].lstrip().startswith(b"## 10. Amendments"), \
        "only the amendments section may follow the locked text"


def test_required_files_exist():
    need = ["README.md", "SPEC.md", "HYPOTHESES.md", "REPORT.md", "RESULTS.md", "DECISIONS.md",
            "METHODS.md", "RESEARCH.md", "CHANGELOG.md", "CITATION.cff", "SECURITY.md", "LICENSE",
            "results/trials.jsonl", "results/summary.json", "results/environment.json"]
    missing = [p for p in need if not os.path.exists(os.path.join(ROOT, p))]
    assert not missing, "missing: " + ", ".join(missing)


def test_readme_opens_with_the_abstract():
    text = _read("README.md").decode()
    for field in ("**Question.**", "**Headline.**", "**Field.**", "**Job family.**", "**Verdict.**", "## Lifecycle"):
        assert field in text, "README lacks " + field


def test_no_public_grade():
    names = [n for n in os.listdir(ROOT) if n.endswith(".md")] + ["site/src/App.jsx", "site/src/docs.mjs"]
    for name in names:
        text = _read(name).decode()
        for mark in ("of 10,000", "/10000", "Grade:", "## Grade"):
            assert mark not in text, name + " carries a grade (" + mark + "); grades are internal"
