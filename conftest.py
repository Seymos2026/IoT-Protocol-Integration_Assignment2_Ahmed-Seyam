"""
Root conftest.py — applied to all tests.

Sets AIOCOAP_SERVER_TRANSPORT=simplesocketserver before any test module is collected.
This prevents aiocoap from starting its WebSocket transport on port 8683
(CoAP port + 3000) which causes an 'address already in use' OSError, and avoids
the udp6 transport's IPv4 tuple-index bug on macOS (sockaddr has 2 elements,
not 4, so `sockaddr[3]` raises IndexError in udp6.py:134).
"""
import os

# Restrict aiocoap server to UDP only — no WebSocket transport.
os.environ.setdefault("AIOCOAP_SERVER_TRANSPORT", "simplesocketserver")
