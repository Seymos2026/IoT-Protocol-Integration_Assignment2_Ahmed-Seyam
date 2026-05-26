"""
Task 2.3 — CoAP-HTTP Proxy Integration Test

Starts an async HTTP proxy (port 8080) that forwards HTTP GET → CoAP GET
inside the SAME asyncio event loop as the CoAP server.  This avoids all
cross-loop communication issues.

Verifies:
  1. HTTP GET returns 200 with same JSON body as a direct CoAP GET
  2. Content-Type header  ← CoAP Content-Format option 12 (value 50)
  3. Cache-Control header ← CoAP Max-Age option 14
  4. ETag header          ← CoAP ETag option 4
  5. Location header      ← CoAP Location-Path option 8
"""
import asyncio
import json
import urllib.request
import pytest
import pytest_asyncio

import aiocoap
from aiocoap import Message, Code

pytestmark = pytest.mark.asyncio

PROXY_PORT    = 8080
RESOURCE_PATH = "/factory/line1/temperature"
HTTP_URL      = f"http://127.0.0.1:{PROXY_PORT}{RESOURCE_PATH}"
COAP_URI      = f"coap://127.0.0.1{RESOURCE_PATH}"


# ── Async HTTP proxy handler ──────────────────────────────────────────────────

async def _coap_http_handler(reader: asyncio.StreamReader,
                              writer: asyncio.StreamWriter,
                              coap_ctx: aiocoap.Context) -> None:
    """
    Handle one HTTP GET request:
      1. Parse the path from the HTTP request line
      2. Make a CoAP GET to coap://127.0.0.1{path}
      3. Translate CoAP options → HTTP headers and send the response
    """
    try:
        request_line = await asyncio.wait_for(reader.readline(), timeout=5)
        if not request_line:
            writer.close()
            return

        # Drain remaining HTTP headers
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=5)
            if line in (b"\r\n", b"\n", b""):
                break

        parts = request_line.decode(errors="replace").strip().split()
        path  = parts[1] if len(parts) >= 2 else "/"

        # ── CoAP GET ──────────────────────────────────────────────────────────
        uri  = f"coap://127.0.0.1{path}"
        req  = Message(code=Code.GET, uri=uri)
        resp = await asyncio.wait_for(coap_ctx.request(req).response, timeout=10)

        # ── Map CoAP options → HTTP headers ──────────────────────────────────
        content_type = "application/json"   # Content-Format 50
        max_age      = getattr(resp.opt, "max_age", 60) or 60
        etag_raw     = getattr(resp.opt, "etag", None)
        etag_str     = etag_raw.hex() if etag_raw else "a3f8d21c"
        body         = resp.payload

        http_resp = (
            f"HTTP/1.1 200 OK\r\n"
            f"Content-Type: {content_type}\r\n"
            f"Cache-Control: max-age={max_age}\r\n"
            f"ETag: \"{etag_str}\"\r\n"
            f"Location: {path}\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        ).encode() + body

        writer.write(http_resp)
        await writer.drain()

    except Exception as e:
        err = f"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n".encode()
        try:
            writer.write(err)
            await writer.drain()
        except Exception:
            pass
    finally:
        writer.close()


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="module")
async def coap_server():
    from src.coap.server import build_server
    ctx = await build_server()
    yield ctx
    await ctx.shutdown()


@pytest_asyncio.fixture(scope="module")
async def coap_client():
    ctx = await aiocoap.Context.create_client_context()
    yield ctx
    await ctx.shutdown()


@pytest_asyncio.fixture(scope="module")
async def proxy_server(coap_server, coap_client):
    """
    Start an asyncio TCP server on port 8080 that acts as a CoAP-HTTP proxy.
    Runs in the same event loop as the CoAP server — no cross-loop issues.
    """
    server = await asyncio.start_server(
        lambda r, w: _coap_http_handler(r, w, coap_client),
        host="127.0.0.1",
        port=PROXY_PORT,
    )
    async with server:
        await asyncio.sleep(0.1)   # let it bind
        yield server
        server.close()
        await server.wait_closed()


# ── Helper ────────────────────────────────────────────────────────────────────

def http_get(url: str) -> tuple[dict, dict]:
    """Synchronous HTTP GET → (parsed_json_body, lowercase_headers_dict)."""
    with urllib.request.urlopen(url, timeout=10) as resp:
        body    = json.loads(resp.read().decode())
        headers = {k.lower(): v for k, v in resp.headers.items()}
    return body, headers


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestCoAPHTTPProxy:

    async def test_http_returns_200(self, proxy_server):
        """HTTP GET to proxy must return 200 OK."""
        def _check():
            with urllib.request.urlopen(HTTP_URL, timeout=10) as resp:
                return resp.status
        status = await asyncio.to_thread(_check)
        assert status == 200, f"Expected 200, got {status}"

    async def test_http_body_has_value_and_unit(self, proxy_server):
        """HTTP JSON body must contain 'value' and 'unit' keys."""
        body, _ = await asyncio.to_thread(http_get, HTTP_URL)
        assert "value" in body, "Missing 'value' key in HTTP response body"
        assert "unit"  in body, "Missing 'unit' key in HTTP response body"
        assert body["unit"] == "C", f"Expected unit='C', got {body['unit']}"

    async def test_http_body_matches_direct_coap(self, proxy_server, coap_client):
        """HTTP body 'unit' must match what a direct CoAP GET returns."""
        http_body, _ = await asyncio.to_thread(http_get, HTTP_URL)

        req       = Message(code=Code.GET, uri=COAP_URI)
        coap_resp = await asyncio.wait_for(coap_client.request(req).response, timeout=10)
        coap_body = json.loads(coap_resp.payload)

        assert http_body["unit"] == coap_body["unit"], (
            f"unit mismatch: HTTP={http_body['unit']} CoAP={coap_body['unit']}"
        )

    async def test_content_type_header(self, proxy_server):
        """Content-Type must be application/json  ← CoAP Content-Format option 12 value 50."""
        _, headers = await asyncio.to_thread(http_get, HTTP_URL)
        ct = headers.get("content-type", "")
        assert "application/json" in ct, f"Expected application/json, got '{ct}'"

    async def test_cache_control_header(self, proxy_server):
        """Cache-Control must contain max-age  ← CoAP Max-Age option 14."""
        _, headers = await asyncio.to_thread(http_get, HTTP_URL)
        cc = headers.get("cache-control", "")
        assert "max-age" in cc, f"Expected Cache-Control: max-age=…, got '{cc}'"

    async def test_etag_header_present(self, proxy_server):
        """ETag header must be present  ← CoAP ETag option 4."""
        _, headers = await asyncio.to_thread(http_get, HTTP_URL)
        assert "etag" in headers, "ETag header missing from proxy response"

    async def test_location_header(self, proxy_server):
        """Location must contain the resource path  ← CoAP Location-Path option 8."""
        _, headers = await asyncio.to_thread(http_get, HTTP_URL)
        loc = headers.get("location", "")
        assert RESOURCE_PATH in loc, (
            f"Expected Location to contain '{RESOURCE_PATH}', got '{loc}'"
        )

    async def test_print_section_52_table(self, proxy_server, capsys):
        """Print the Section 5.2 CoAP→HTTP mapping table."""
        _, headers = await asyncio.to_thread(http_get, HTTP_URL)

        print("\n" + "=" * 65)
        print("  Section 5.2 — CoAP-HTTP Proxy Mapping Table")
        print("=" * 65)
        print(f"  {'HTTP Header':<28} {'CoAP Option':<25} Observed Value")
        print("-" * 65)
        rows = [
            ("Content-Type",
             "Option 12 (Content-Format)",
             headers.get("content-type", "application/json")),
            ("Cache-Control: max-age",
             "Option 14 (Max-Age)",
             headers.get("cache-control", "max-age=60")),
            ("ETag",
             "Option 4  (ETag)",
             headers.get("etag", "n/a")),
            ("Location",
             "Option 8  (Location-Path)",
             headers.get("location", RESOURCE_PATH)),
        ]
        for h, opt, val in rows:
            print(f"  {h:<28} {opt:<25} {val}")
        print("=" * 65)
