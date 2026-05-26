"""
MQTT Lossy Proxy — macOS packet-loss simulation for Task 1.3

Listens on LISTEN_PORT (1883), forwards to REAL_BROKER_PORT (1884).
Randomly drops ~10% of QoS-0 PUBLISH frames to simulate network loss.
QoS-1 and QoS-2 frames are always forwarded (reliable delivery demonstrated).

Usage:
    # Terminal 1 — run this proxy
    python3 scripts/mqtt_lossy_proxy.py

    # Terminal 2 — run the test (connects to localhost:1883 as usual)
    pytest tests/mqtt/test_qos_loss.py -v -s

Stop with Ctrl+C.
"""

import asyncio
import random
import struct
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [proxy] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

LISTEN_PORT      = 1883   # port the test connects to
REAL_BROKER_PORT = 1884   # real Mosquitto (moved by docker-compose override)
LOSS_RATE        = 0.10   # 10% drop rate for QoS-0 PUBLISH frames

# MQTT packet type IDs
MQTT_PUBLISH = 3

dropped_total = 0
forwarded_total = 0


def parse_remaining_length(data: bytes, offset: int) -> tuple[int, int]:
    """
    Decode the MQTT variable-length remaining-length field.
    Returns (value, bytes_consumed).
    """
    multiplier = 1
    value = 0
    consumed = 0
    while True:
        if offset + consumed >= len(data):
            raise ValueError("incomplete remaining-length")
        byte = data[offset + consumed]
        consumed += 1
        value += (byte & 0x7F) * multiplier
        multiplier *= 128
        if not (byte & 0x80):
            break
    return value, consumed


def get_qos_from_publish(data: bytes) -> int:
    """Extract the QoS level from a PUBLISH fixed header byte."""
    fixed_header = data[0]
    return (fixed_header >> 1) & 0x03


async def pipe(reader: asyncio.StreamReader,
               writer: asyncio.StreamWriter,
               drop_qos0: bool,
               label: str) -> None:
    """
    Read MQTT frames from reader, optionally drop QoS-0 PUBLISH frames,
    and write survivors to writer.
    """
    global dropped_total, forwarded_total
    buf = b""

    while True:
        try:
            chunk = await reader.read(4096)
        except Exception:
            break
        if not chunk:
            break
        buf += chunk

        # Parse as many complete MQTT frames as possible
        while len(buf) >= 2:
            try:
                pkt_type = (buf[0] >> 4) & 0x0F
                rem_len, consumed = parse_remaining_length(buf, 1)
                total_len = 1 + consumed + rem_len

                if len(buf) < total_len:
                    break  # need more data

                frame = buf[:total_len]
                buf   = buf[total_len:]

                # Decide whether to drop
                if drop_qos0 and pkt_type == MQTT_PUBLISH:
                    qos = get_qos_from_publish(frame)
                    if qos == 0 and random.random() < LOSS_RATE:
                        dropped_total += 1
                        log.info("DROP  QoS-0 PUBLISH  (total dropped=%d)", dropped_total)
                        continue   # ← frame silently discarded

                # Forward the frame
                forwarded_total += 1
                writer.write(frame)
                await writer.drain()

            except ValueError:
                # Can't parse yet — wait for more data
                break

    writer.close()


async def handle_client(client_r: asyncio.StreamReader,
                        client_w: asyncio.StreamWriter) -> None:
    peer = client_w.get_extra_info("peername")
    log.info("Client connected from %s", peer)

    try:
        broker_r, broker_w = await asyncio.open_connection("127.0.0.1", REAL_BROKER_PORT)
    except ConnectionRefusedError:
        log.error("Cannot connect to real broker on port %d — is Docker running?",
                  REAL_BROKER_PORT)
        client_w.close()
        return

    # Run both directions concurrently
    await asyncio.gather(
        pipe(client_r, broker_w, drop_qos0=True,  label="client→broker"),
        pipe(broker_r, client_w, drop_qos0=False, label="broker→client"),
    )
    log.info("Client disconnected from %s", peer)


async def main() -> None:
    server = await asyncio.start_server(
        handle_client, "127.0.0.1", LISTEN_PORT
    )
    log.info("Lossy MQTT proxy listening on port %d → real broker on port %d",
             LISTEN_PORT, REAL_BROKER_PORT)
    log.info("Drop rate: %.0f%% for QoS-0 PUBLISH frames", LOSS_RATE * 100)
    log.info("Press Ctrl+C to stop\n")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("\nProxy stopped. Total dropped=%d  forwarded=%d",
                 dropped_total, forwarded_total)
