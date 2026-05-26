"""
BLE scanner — run this once to find your SensorTag's macOS UUID.

Usage:
    python -m src.coap.scan_ble
"""

import asyncio
from bleak import BleakScanner


async def scan():
    print("Scanning for BLE devices for 10 seconds …\n")
    devices = await BleakScanner.discover(timeout=10.0)

    if not devices:
        print("No BLE devices found. Make sure Bluetooth is ON and SensorTag is awake.")
        return

    print(f"{'UUID':<40}  {'RSSI':>5}  Name")
    print("-" * 70)
    for d in sorted(devices, key=lambda x: x.rssi or -999, reverse=True):
        name = d.name or "(unknown)"
        print(f"{str(d.address):<40}  {d.rssi or '?':>5}  {name}")

    print("\nLook for a device named 'CC2650 SensorTag', 'SensorTag', or similar.")
    print("Copy its UUID and paste it into SENSOR_TAG_MAC in sensortag_server.py")


if __name__ == "__main__":
    asyncio.run(scan())
