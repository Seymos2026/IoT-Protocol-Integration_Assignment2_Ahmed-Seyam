"""
Module 1 Assignment — Task 2.2
CoAP Observer Client

Complete all TODO sections.

Run with:  python -m src.coap.observer
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

import aiocoap
from aiocoap import Message, Code

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

SERVER_BASE = "coap://localhost"
OBSERVE_DURATION = 60   # seconds before clean deregister

# CoAP observe sequence numbers wrap at 2^24
_SEQ_MAX = 0xFFFFFF


class FactoryObserver:
    """Observes CoAP sensor resources and reassembles Block2 transfers."""

    def __init__(self):
        self._ctx = None
        self._last_seq: dict[str, int] = {}     # uri -> last observe sequence number
        self._stale_count: dict[str, int] = {}  # uri -> stale notification count

    # ── Setup ──────────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Create the aiocoap client context."""
        self._ctx = await aiocoap.Context.create_client_context()

    async def stop(self) -> None:
        """Clean up the context."""
        if self._ctx:
            await self._ctx.shutdown()

    # ── Observation ────────────────────────────────────────────────────────────

    async def observe_resource(self, uri: str) -> None:
        """
        Subscribe to a single observable CoAP resource for OBSERVE_DURATION seconds,
        then cleanly deregister.
        """
        request = Message(code=Code.GET, uri=uri, observe=0)
        pr = self._ctx.request(request)

        try:
            # First response (acknowledgement of subscription)
            first_response = await asyncio.wait_for(pr.response, timeout=10)
            self._handle_notification(uri, first_response)

            # Stream notifications until timeout
            async def _drain():
                async for response in pr.observation:
                    self._handle_notification(uri, response)

            await asyncio.wait_for(_drain(), timeout=OBSERVE_DURATION)
        except asyncio.TimeoutError:
            pass
        finally:
            # Deregister with Observe=1
            pr.observation.cancel()
            log.info("Deregistered from %s", uri)

    def _handle_notification(self, uri: str, response: Message) -> None:
        """
        Process a single Observe notification.
        Detects stale notifications using the CoAP Observe sequence number.
        """
        seq = response.opt.observe

        # Check for stale notification (handle wrap-around at 2^24)
        if uri in self._last_seq:
            last = self._last_seq[uri]
            # A notification is stale if seq <= last, accounting for wrap-around
            # (a "fresh" notification can be up to 2^23 ahead of the last)
            delta = (seq - last) & _SEQ_MAX
            if delta == 0 or delta > (_SEQ_MAX // 2):
                self._stale_count[uri] = self._stale_count.get(uri, 0) + 1
                log.warning("STALE notification on %s: seq=%d <= last=%d", uri, seq, last)
                return

        self._last_seq[uri] = seq

        try:
            data = json.loads(response.payload.decode())
            value = data.get("value", "?")
            unit  = data.get("unit", "")
            ts    = data.get("ts", datetime.now(timezone.utc).isoformat())
        except (json.JSONDecodeError, UnicodeDecodeError):
            value, unit, ts = response.payload, "", datetime.now(timezone.utc).isoformat()

        log.info("[OBSERVE] %s  seq=%d  val=%s %s  @ %s", uri, seq, value, unit, ts)

    # ── Block2 Transfer ────────────────────────────────────────────────────────

    async def fetch_manifest(self) -> None:
        """
        Perform a GET on /factory/manifest. aiocoap reassembles Block2 automatically.
        """
        uri = f"{SERVER_BASE}/factory/manifest"
        request = Message(code=Code.GET, uri=uri)
        response = await self._ctx.request(request).response

        payload = response.payload
        log.info("Manifest received: %d bytes", len(payload))

        try:
            data = json.loads(payload.decode())
            if isinstance(data, dict):
                count = len(data.get("entries", data))
            else:
                count = len(data)
            log.info("Firmware entries in manifest: %d", count)
        except json.JSONDecodeError as exc:
            log.error("Manifest JSON parse error: %s", exc)

        log.info("Block2 transfer complete")

    # ── Run ────────────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """
        Run observations on both temperature resources concurrently for OBSERVE_DURATION
        seconds, then fetch the firmware manifest and print a stale-count summary.
        """
        await self.start()
        try:
            uri1 = f"{SERVER_BASE}/factory/line1/temperature"
            uri2 = f"{SERVER_BASE}/factory/line2/temperature"

            await asyncio.gather(
                self.observe_resource(uri1),
                self.observe_resource(uri2),
            )

            await self.fetch_manifest()

            print("\n── Stale Notification Summary ───────────────")
            for uri, count in self._stale_count.items():
                print(f"  {uri}: {count} stale notification(s)")
            if not self._stale_count:
                print("  No stale notifications detected.")
            print("─────────────────────────────────────────────")
        finally:
            await self.stop()


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    observer = FactoryObserver()
    asyncio.run(observer.run())
