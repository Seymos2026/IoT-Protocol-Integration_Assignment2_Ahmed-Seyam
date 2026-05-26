"""
Module 1 Assignment — Task 3.2
AMQP Producer with Publisher Confirms

Complete all TODO sections.
"""

import json
import logging
import random
import ssl
import time
from datetime import datetime, timezone

import pika
import pika.exceptions

from src.amqp.topology import (
    EXCHANGE_TELEMETRY, QUEUE_TEMPERATURE,
    get_connection_params
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

CRITICAL_THRESHOLD = 85.0

SENSOR_CONFIG = {
    "temperature": {"unit": "C",    "base": 70.0, "noise": 3.0,  "persistent": True},
    "vibration":   {"unit": "mm/s", "base": 1.2,  "noise": 0.3,  "persistent": False},
    "power":       {"unit": "kW",   "base": 45.0, "noise": 5.0,  "persistent": True},
}
LINES = ["line1", "line2"]

_seq: dict[str, int] = {}


class SmartFactoryProducer:

    def __init__(self):
        self._connection = None
        self._channel    = None
        self._published  = 0
        self._confirmed  = 0
        self._unconfirmed: set[int] = set()

    # ── Connection ─────────────────────────────────────────────────────────────

    def connect(self) -> None:
        """
        TODO 1: Connect to RabbitMQ and set up the channel with Publisher Confirms.
        """
        params = get_connection_params()
        self._connection = pika.BlockingConnection(params)
        self._channel    = self._connection.channel()

        # Enable publisher confirms (Confirm.Select)
        self._channel.confirm_delivery()

        # Register return callback for mandatory=True unroutable messages
        self._channel.add_on_return_callback(self.on_return)

        log.info("Producer connected to RabbitMQ with Publisher Confirms enabled")

    def disconnect(self) -> None:
        if self._connection and not self._connection.is_closed:
            self._connection.close()
        log.info("Producer stats — published: %d  confirmed: %d  unconfirmed: %d",
                 self._published, self._confirmed, len(self._unconfirmed))

    # ── Callbacks ──────────────────────────────────────────────────────────────

    def on_delivery_confirmed(self, method_frame) -> None:
        """
        TODO 2: Called when the broker sends a Basic.Ack or Basic.Nack.
        (Used with SelectConnection; with BlockingConnection, confirm_delivery()
        raises UnroutableError/NackError on failure instead of callbacks.)
        """
        tag = method_frame.method.delivery_tag
        if method_frame.method.NAME == "Basic.Ack":
            log.info("CONFIRM ack delivery_tag=%d", tag)
            self._confirmed += 1
            self._unconfirmed.discard(tag)
        else:
            log.warning("CONFIRM nack (LOST) delivery_tag=%d", tag)
            self._unconfirmed.discard(tag)

    def on_return(self, channel, method, properties, body) -> None:
        """
        TODO 3: Called when mandatory=True and no queue matched the routing key.
        """
        log.warning(
            "RETURNED (no route): routing_key=%s reply=%s",
            method.routing_key, method.reply_text,
        )

    # ── Routing Key ────────────────────────────────────────────────────────────

    def _routing_key(self, line: str, sensor: str, value: float) -> str:
        """
        TODO 4: Build the AMQP routing key.
        Format: factory.{line}.{sensor}
        If sensor == "temperature" AND value > CRITICAL_THRESHOLD:
            Format: factory.{line}.temperature.critical
        """
        key = f"factory.{line}.{sensor}"
        if sensor == "temperature" and value > CRITICAL_THRESHOLD:
            key = f"factory.{line}.temperature.critical"
        return key

    # ── Publishing ─────────────────────────────────────────────────────────────

    def publish_reading(self, line: str, sensor: str) -> dict:
        """
        TODO 5: Simulate and publish a sensor reading.
        """
        cfg   = SENSOR_CONFIG[sensor]
        # 10% chance of thermal spike on temperature to trigger critical routing
        if sensor == "temperature" and random.random() < 0.10:
            value = round(random.uniform(86.0, 95.0), 3)
        else:
            value = round(cfg["base"] + random.gauss(0, cfg["noise"]), 3)
        seq_key = f"{line}/{sensor}"
        _seq[seq_key] = _seq.get(seq_key, 0) + 1

        payload = {
            "value":     value,
            "unit":      cfg["unit"],
            "line":      line,
            "sensor":    sensor,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "seq":       _seq[seq_key],
        }

        routing_key   = self._routing_key(line, sensor, value)
        delivery_mode = 2 if cfg["persistent"] else 1

        props = pika.BasicProperties(
            content_type="application/json",
            delivery_mode=delivery_mode,
            expiration="60000",         # 60 s TTL
            timestamp=int(time.time()),
        )

        try:
            self._channel.basic_publish(
                exchange=EXCHANGE_TELEMETRY,
                routing_key=routing_key,
                body=json.dumps(payload).encode(),
                properties=props,
                mandatory=True,
            )
            # With BlockingConnection + confirm_delivery(), basic_publish blocks
            # until the broker acks. delivery_tag increments automatically.
            self._published += 1
            self._confirmed += 1
            log.info("[%s]  val=%.2f %s  delivery_mode=%d",
                     routing_key, value, cfg["unit"], delivery_mode)
        except pika.exceptions.UnroutableError:
            log.warning("RETURNED (unroutable): %s", routing_key)
        except pika.exceptions.NackError:
            log.warning("NACK from broker: %s", routing_key)

        return payload

    # ── Main Loop ──────────────────────────────────────────────────────────────

    def run(self, interval_s: float = 1.0) -> None:
        self.connect()
        seq = 0
        try:
            while True:
                seq += 1
                for line in LINES:
                    for sensor in SENSOR_CONFIG:
                        self.publish_reading(line, sensor)
                self._channel.connection.process_data_events()  # flush confirms
                time.sleep(interval_s)
        except KeyboardInterrupt:
            log.info("Shutting down…")
        finally:
            self.disconnect()


if __name__ == "__main__":
    producer = SmartFactoryProducer()
    producer.run()
