"""
Load S3 importer credentials from a file into the environment before test collection.

The IMP_CREDS_FILE env var (set in docker-compose for the test-container) points to a
file written by either minio-create-bucket or ceph-setup. Reading it here, at module
level, ensures the credentials are in os.environ before any test module is imported.
"""

import os
from pathlib import Path


def _load_creds():
    creds_file = os.environ.get("IMP_CREDS_FILE")
    if not creds_file:
        return
    p = Path(creds_file)
    if not p.exists():
        return
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        os.environ.setdefault(key.strip(), val.strip())


_load_creds()
