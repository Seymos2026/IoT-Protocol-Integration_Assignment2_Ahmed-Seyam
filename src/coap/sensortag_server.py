"""
CoAP Server + TI CC1350 SensorTag BLE reader
Run: python server.py
"""

import asyncio
import struct
import logging
from aiocoap import resource, Context, Message, Code
from bleak import BleakClient, BleakScanner

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

# ── SensorTag BLE configuration ───────────────────────────────────────────────

# macOS UUID for this SensorTag (found during first successful scan)
SENSOR_TAG_UUID  = "484460F2-A1C7-D36C-9262-AB3710BFC273"

TEMP_DATA_UUID   = "f000aa01-0451-4000-b000-000000000000"
TEMP_CONFIG_UUID = "f000aa02-0451-4000-b000-000000000000"
TEMP_PERIOD_UUID = "f000aa03-0451-4000-b000-000000000000"


def parse_temperature(data: bytes) -> float:
    """
    Convert the 4-byte raw payload from the SensorTag IR temperature sensor.
    bytes 0-1 = raw object temp, bytes 2-3 = raw ambient/die temp.
    CC1350/CC2650 formula: (raw_amb >> 2) * 0.03125
    """
    if len(data) < 4:
        raise ValueError(f"Expected 4 bytes, got {len(data)}: {data.hex()}")
    raw_obj, raw_amb = struct.unpack('<hh', data)
    log.info("raw_obj=%d  raw_amb=%d", raw_obj, raw_amb)
    return round((raw_amb >> 2) * 0.03125, 2)


# ── CoAP Resource ─────────────────────────────────────────────────────────────

class TemperatureResource(resource.Resource):
    def __init__(self):
        super().__init__()
        self.temperature = None

    async def render_get(self, request):
        if self.temperature is None:
            return Message(code=Code.CONTENT,
                           payload=b"Waiting for first SensorTag reading...")
        payload = f"Temperature: {self.temperature:.2f} C".encode("utf-8")
        return Message(code=Code.CONTENT, payload=payload)

    async def render_post(self, request):
        try:
            self.temperature = float(request.payload.decode("utf-8").strip())
            return Message(code=Code.CHANGED,
                           payload=f"Updated to {self.temperature:.2f} C".encode())
        except ValueError:
            return Message(code=Code.BAD_REQUEST, payload=b"Invalid temperature value")

    async def render_put(self, request):
        return Message(code=Code.CHANGED, payload=b"PUT received")

    async def render_delete(self, request):
        return Message(code=Code.DELETED, payload=b"Deleted")


# ── BLE loop ──────────────────────────────────────────────────────────────────

async def read_sensortag_loop(temp_resource: TemperatureResource) -> None:
    """
    Connects to the SensorTag by its known macOS UUID.
    Uses both BLE notifications AND direct reads every 2 s as a fallback,
    because CC1350 firmware versions vary in notification behaviour.
    Retries automatically if the connection drops.
    """
    while True:
        try:
            log.info("Connecting to SensorTag — press side button if LED is off ...")

            async with BleakClient(SENSOR_TAG_UUID) as client:
                log.info("Connected!")

                # Step 1 — subscribe to notifications FIRST
                def on_temperature(sender, data: bytearray):
                    log.info("NOTIFICATION raw bytes: %s  (len=%d)", data.hex(), len(data))
                    try:
                        temp = parse_temperature(bytes(data))
                        temp_resource.temperature = temp
                        log.info("SensorTag → %.2f C  (via notification)", temp)
                    except Exception as e:
                        log.error("Notification parse error: %s", e)

                await client.start_notify(TEMP_DATA_UUID, on_temperature)
                log.info("Notifications subscribed")

                # Step 2 — set measurement period: 0x64 = 100 x 10 ms = 1 second
                await client.write_gatt_char(TEMP_PERIOD_UUID, b'\x64')
                log.info("Period set to 1 s")

                # Step 3 — enable the IR temperature sensor LAST
                await client.write_gatt_char(TEMP_CONFIG_UUID, b'\x01')
                log.info("Sensor enabled — also polling every 2 s as fallback ...")

                # Poll directly every 2 s as a fallback in case notifications
                # do not fire (CC1350 firmware behaviour varies by version)
                while True:
                    await asyncio.sleep(2)
                    raw = await client.read_gatt_char(TEMP_DATA_UUID)
                    log.info("Direct read: %s  (len=%d)", raw.hex(), len(raw))
                    if any(raw):
                        try:
                            temp = parse_temperature(bytes(raw))
                            temp_resource.temperature = temp
                            log.info("SensorTag → %.2f C  (via direct read)", temp)
                        except Exception as e:
                            log.error("Read parse error: %s", e)
                    else:
                        log.warning("All-zero read — sensor still warming up ...")

        except Exception as exc:
            log.error("BLE error (%s: %s) — retrying in 10 s", type(exc).__name__, exc)
            await asyncio.sleep(10)


# ── Server entry point ────────────────────────────────────────────────────────

async def main() -> None:
    root = resource.Site()
    temp_resource = TemperatureResource()
    root.add_resource(("temperature",), temp_resource)

    await Context.create_server_context(root, bind=("127.0.0.1", 5683))
    log.info("CoAP server ready at coap://127.0.0.1:5683/temperature")

    asyncio.ensure_future(read_sensortag_loop(temp_resource))
    await asyncio.get_running_loop().create_future()


if __name__ == "__main__":
    asyncio.run(main())
