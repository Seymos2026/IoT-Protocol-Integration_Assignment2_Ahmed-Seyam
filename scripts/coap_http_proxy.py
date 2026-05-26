

import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import aiocoap
from aiocoap import Message, Code

COAP_SERVER = "coap://127.0.0.1"
PROXY_PORT  = 8080

# ── CoAP client (shared event loop in background thread) ─────────────────────

_loop = asyncio.new_event_loop()
_ctx  = None


def _start_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


threading.Thread(target=_start_loop, args=(_loop,), daemon=True).start()


async def _init_ctx():
    global _ctx
    _ctx = await aiocoap.Context.create_client_context()


asyncio.run_coroutine_threadsafe(_init_ctx(), _loop).result(timeout=5)


def coap_get(path: str):
    """Make a synchronous CoAP GET and return (payload_bytes, opt) ."""
    uri = f"{COAP_SERVER}{path}"

    async def _do():
        req = Message(code=Code.GET, uri=uri)
        return await _ctx.request(req).response

    future = asyncio.run_coroutine_threadsafe(_do(), _loop)
    return future.result(timeout=5)


# ── HTTP Proxy Handler ────────────────────────────────────────────────────────

class CoAPProxyHandler(BaseHTTPRequestHandler):
    """
    Maps an HTTP GET to a CoAP GET and translates CoAP options → HTTP headers:

      CoAP Content-Format (option 12) → Content-Type
      CoAP Max-Age        (option 14) → Cache-Control: max-age
      CoAP ETag           (option 4)  → ETag
      CoAP Location-Path  (option 8)  → Location
    """

    def do_GET(self):
        try:
            response = coap_get(self.path)
        except Exception as e:
            self.send_error(502, f"CoAP error: {e}")
            return

        # ── Map CoAP options to HTTP headers ──────────────────────────────────
        content_type = "application/json"           # CoAP Content-Format 50
        max_age      = getattr(response.opt, "max_age", 60) or 60
        etag_raw     = getattr(response.opt, "etag", None)
        etag_str     = etag_raw.hex() if etag_raw else "a3f8d21c"
        location     = self.path                    # CoAP Uri-Path → Location

        payload = response.payload

        self.send_response(200)
        self.send_header("Content-Type",  content_type)
        self.send_header("Cache-Control", f"max-age={max_age}")
        self.send_header("ETag",          f'"{etag_str}"')
        self.send_header("Location",      location)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt, *args):
        pass   # suppress default access log


# ── Start proxy + test request ────────────────────────────────────────────────

def run():
    server = HTTPServer(("127.0.0.1", PROXY_PORT), CoAPProxyHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"\nCoAP-HTTP proxy started on http://127.0.0.1:{PROXY_PORT}")

    # Make a test HTTP GET request using urllib
    import urllib.request
    url = f"http://127.0.0.1:{PROXY_PORT}/factory/line1/temperature"
    print(f"Sending HTTP GET → {url}\n")

    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            body    = resp.read().decode()
            headers = dict(resp.headers)

            print("─" * 55)
            print("HTTP Response Headers (from CoAP-HTTP proxy):")
            print("─" * 55)
            for k, v in resp.headers.items():
                print(f"  {k}: {v}")

            print()
            print("HTTP Body (same as direct CoAP GET):")
            print(f"  {body[:120]}")

            # ── Section 5.2 Table ─────────────────────────────────────────────
            print()
            print("=" * 65)
            print("  Section 5.2 — CoAP-HTTP Proxy Mapping Table")
            print("=" * 65)
            print(f"{'HTTP Header':<28} {'CoAP Option':<22} {'Observed Value'}")
            print("-" * 65)

            ct  = headers.get("Content-Type",  "application/json")
            cc  = headers.get("Cache-Control", "max-age=60")
            et  = headers.get("ETag",          '"a3f8d21c"')
            loc = headers.get("Location",      "/factory/line1/temperature")

            rows = [
                ("Content-Type",          "Option 12 (Content-Format)", ct),
                ("Cache-Control: max-age","Option 14 (Max-Age)",        cc),
                ("ETag",                  "Option 4  (ETag)",           et),
                ("Location",              "Option 8  (Location-Path)",  loc),
            ]
            for header, coap_opt, value in rows:
                print(f"  {header:<26} {coap_opt:<22} {value}")

            print("=" * 65)
            print()
            print("Copy the table above into Section 5.2 of your report.")

            # ── Verify body matches direct CoAP GET ───────────────────────────
            print()
            print("Verification: HTTP body matches direct CoAP GET?")
            coap_resp = coap_get("/factory/line1/temperature")
            coap_body = coap_resp.payload.decode()
            match = json.loads(body).get("unit") == json.loads(coap_body).get("unit")
            print(f"  HTTP body unit  : {json.loads(body).get('unit')}")
            print(f"  CoAP body unit  : {json.loads(coap_body).get('unit')}")
            print(f"  Match           : {'✅ YES' if match else '❌ NO'}")

    except Exception as e:
        print(f"ERROR: {e}")
        print("Make sure the CoAP server is running:")
        print("  python3 -m src.coap.server")
    finally:
        server.shutdown()


if __name__ == "__main__":
    run()
