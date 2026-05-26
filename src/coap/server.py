"""
Module 1 Assignment — Task 2.1
CoAP Sensor Resource Server

Complete all TODO sections. The resource classes must match the
URIs and behaviours listed in the assignment spec.

Run with:  python -m src.coap.server
"""

import asyncio
import json
import logging
import random
from datetime import datetime, timezone

import aiocoap
import aiocoap.resource as resource
from aiocoap import Code, Message

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

# ── Sensor simulation helpers ─────────────────────────────────────────────────

SENSOR_CONFIG = {
    "temperature": {"unit": "C",    "base": 70.0, "noise": 3.0},
    "vibration":   {"unit": "mm/s", "base": 1.2,  "noise": 0.3},
    "power":       {"unit": "kW",   "base": 45.0, "noise": 5.0},
}

def _sim(sensor: str) -> dict:
    cfg = SENSOR_CONFIG[sensor]
    # 10% chance of a thermal spike on temperature to trigger CRITICAL alerts
    if sensor == "temperature" and random.random() < 0.10:
        value = round(random.uniform(86.0, 95.0), 3)
    else:
        value = round(cfg["base"] + random.gauss(0, cfg["noise"]), 3)
    return {
        "value": value,
        "unit":  cfg["unit"],
        "ts":    datetime.now(timezone.utc).isoformat(),
    }

def _json(data: dict) -> bytes:
    return json.dumps(data).encode()


# ── Observable Sensor Resource ────────────────────────────────────────────────

class SensorResource(resource.ObservableResource):
    """
    An observable CoAP resource that represents a single sensor on a line.
    """

    def __init__(self, line: str, sensor_type: str):
        super().__init__()
        self.line        = line
        self.sensor_type = sensor_type
        self._reading    = _sim(sensor_type)
        asyncio.ensure_future(self._update_loop())

    async def _update_loop(self) -> None:
        """Every 5 seconds, simulate a new reading and notify observers."""
        while True:
            await asyncio.sleep(5)
            self._reading = _sim(self.sensor_type)
            self.updated_state()

    async def render_get(self, request: Message) -> Message:
        """Return the current sensor reading as a JSON response."""
        payload = _json(self._reading)
        return Message(code=Code.CONTENT, payload=payload, content_format=50)


# ── Actuator Resource ─────────────────────────────────────────────────────────

class ActuatorResource(resource.Resource):
    """
    A CoAP resource representing a controllable fan actuator.
    """

    def __init__(self):
        super().__init__()
        self._state = "OFF"

    async def render_get(self, request: Message) -> Message:
        """Return current fan state as JSON."""
        payload = _json({"state": self._state})
        return Message(code=Code.CONTENT, payload=payload, content_format=50)

    async def render_put(self, request: Message) -> Message:
        """Accept ON/OFF command and update state."""
        try:
            data = json.loads(request.payload.decode())
            state = data.get("state", "").upper()
            if state not in ("ON", "OFF"):
                raise ValueError(f"Invalid state: {state!r}")
            self._state = state
            log.info("Fan actuator set to %s", state)
            return Message(code=Code.CHANGED, payload=_json({"state": self._state}), content_format=50)
        except (json.JSONDecodeError, ValueError, AttributeError) as exc:
            return Message(code=Code.BAD_REQUEST, payload=str(exc).encode())


# ── Block-wise Manifest Resource ──────────────────────────────────────────────

class ManifestResource(resource.Resource):
    """
    A large resource that triggers CoAP Block2 transfer (>= 3 KB payload).
    """

    async def render_get(self, request: Message) -> Message:
        """Return a >= 3 KB JSON firmware manifest."""
        entries = []
        sensors = ["temperature", "vibration", "power", "humidity", "pressure",
                   "co2", "proximity", "current", "voltage", "rpm"]
        lines   = ["line1", "line2", "line3", "line4", "line5"]
        for i in range(60):
            sensor = sensors[i % len(sensors)]
            line   = lines[i % len(lines)]
            entries.append({
                "id":          f"fw-{line}-{sensor}-{i:04d}",
                "device":      f"{line}-{sensor}-sensor",
                "firmware":    f"2.{i // 10}.{i % 10}",
                "build":       f"20250501-{i:04d}",
                "checksum_md5": f"a{i:031x}",
                "checksum_sha256": f"b{i:063x}",
                "size_bytes":  1024 + i * 17,
                "url":         f"https://ota.smartfactory.internal/fw/{line}/{sensor}/v2.{i}.bin",
                "min_hw_rev":  f"rev{(i % 4) + 1}",
                "changelog":   (
                    f"Fix sensor drift on {sensor} channel {i}. "
                    f"Improved power management for {line} deployment. "
                    "Extended MQTT keep-alive to 120 s. "
                    "Addressed CVE-2025-1234 in TLS stack. "
                    "Reduced boot time by 200 ms."
                ),
                "deployed_to": [f"{line}-node-{j:02d}" for j in range(1, 5)],
                "rollback_fw": f"2.{max(i//10-1,0)}.0",
                "timestamp":   datetime.now(timezone.utc).isoformat(),
            })

        manifest = {
            "schema_version": "1.0",
            "generated_at":   datetime.now(timezone.utc).isoformat(),
            "factory":        "SmartFactory-Inc",
            "total_devices":  len(entries),
            "entries":        entries,
        }
        payload = json.dumps(manifest, indent=2).encode()
        assert len(payload) >= 3072, f"Manifest too small: {len(payload)} bytes"
        log.info("Serving manifest: %d bytes", len(payload))
        return Message(code=Code.CONTENT, payload=payload, content_format=50)


# ── Resource Tree & Server Setup ──────────────────────────────────────────────

async def build_server() -> aiocoap.Context:
    """
    Build the CoAP resource tree and create the server context.
    """
    root = resource.Site()

    root.add_resource(["factory", "line1", "temperature"],
                      SensorResource("line1", "temperature"))
    root.add_resource(["factory", "line1", "vibration"],
                      SensorResource("line1", "vibration"))
    root.add_resource(["factory", "line1", "power"],
                      SensorResource("line1", "power"))
    root.add_resource(["factory", "line2", "temperature"],
                      SensorResource("line2", "temperature"))
    root.add_resource(["actuator", "line1", "fan"],
                      ActuatorResource())
    root.add_resource(["factory", "manifest"],
                      ManifestResource())

    root.add_resource([".well-known", "core"],
                      resource.WKCResource(root.get_resources_as_linkheader))

    context = await aiocoap.Context.create_server_context(root, bind=("127.0.0.1", 5683))
    return context


async def main() -> None:
    context = await build_server()
    log.info("CoAP server running on coap://localhost:5683")
    log.info("Resources: /factory/line{1,2}/{temperature,vibration,power}, /actuator/line1/fan, /factory/manifest")
    await asyncio.get_event_loop().create_future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
