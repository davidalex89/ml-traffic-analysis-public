"""CANARY: do not merge.

Exists only to prove the CodeQL check fails a pull request. Two planted
findings: an unused import (code-quality query) and a TLS 1.0 context
(security query, high severity).
"""
import os
import ssl


def insecure_context():
    return ssl.SSLContext(ssl.PROTOCOL_TLSv1)
