"""
Module 1 Assignment — Task 3.3
AMQP Consumer with Manual ACK and DLX Inspection

Complete all TODO sections.
"""

import json
import logging
import random
import time
from datetime import datetime, timezone

import pika
import pika.exceptions

from src.amqp.topology import (
    QUEUE_ALL, QUEUE_DLX,
    EXCHANGE_TELEMETRY, EXCHANGE_DLX,
    get_connection_params
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

PREFETCH_COUNT   = 5
FAILURE_RATE     = 0.10          # 10% random processing failures
DLX_POLL_EVERY   = 30            # seconds between DLX queue polls


class SmartFactoryConsumer:

    def __init__(self):
        self._connection    = None
        self._channel       = None
        self._processed     = 0
        self._failed        = 0
        self._alerts_seen   = 0
        self._last_dlx_poll = time.time()

    # ── Connection ─────────────────────────────────────────────────────────────

    def connect(self) -> None:
        """
        TODO 1: Connect to RabbitMQ, set QoS, and register message callback.
        """
        params = get_connection_params()
        self._connection = pika.BlockingConnection(params)
        self._channel    = self._connection.channel()

        # Prefetch limit — don't deliver more than 5 unacked messages at once
        self._channel.basic_qos(prefetch_count=PREFETCH_COUNT, global_qos=False)

        # Register consumer with manual ACK
        self._channel.basic_consume(
            queue=QUEUE_ALL,
            on_message_callback=self.on_message,
            auto_ack=False,
        )
        log.info("Connected to RabbitMQ. Consuming from %s (prefetch=%d)", QUEUE_ALL, PREFETCH_COUNT)

    # ── Message Handler ────────────────────────────────────────────────────────

    def on_message(
        self,
        channel: pika.adapters.blocking_connection.BlockingChannel,
        method:  pika.spec.Basic.Deliver,
        props:   pika.spec.BasicProperties,
        body:    bytes,
    ) -> None:
        """
        TODO 2: Main message handler.
        """
        tag         = method.delivery_tag
        routing_key = method.routing_key

        # 1. Parse body as JSON
        try:
            payload = json.loads(body.decode())
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            log.error("Bad JSON on tag=%d key=%s: %s", tag, routing_key, exc)
            channel.basic_nack(delivery_tag=tag, requeue=False)
            return

        # 2. Critical alert path — ACK and return early
        if routing_key.endswith(".critical"):
            self._print_critical_alert(routing_key, payload)
            self._alerts_seen += 1
            channel.basic_ack(delivery_tag=tag)
            return

        # 3. Simulate random processing failure (10%)
        if random.random() < FAILURE_RATE:
            log.warning("NACK (simulated failure) tag=%d key=%s", tag, routing_key)
            self._failed += 1
            channel.basic_nack(delivery_tag=tag, requeue=False)
        else:
            log.info("[PROCESSED] %s  val=%s  tag=%d",
                     routing_key, payload.get("value", "?"), tag)
            self._processed += 1
            channel.basic_ack(delivery_tag=tag)

        # 4. Periodic DLX inspection
        now = time.time()
        if now - self._last_dlx_poll >= DLX_POLL_EVERY:
            self._last_dlx_poll = now
            self._poll_dlx()

    def _print_critical_alert(self, routing_key: str, payload: dict) -> None:
        """
        TODO 3: Print a formatted critical temperature alert.
        """
        value = payload.get("value", "?")
        ts    = payload.get("timestamp", datetime.now(timezone.utc).isoformat())
        print("╔══════════════════════════════════════╗")
        print(f"║  ⚠ CRITICAL ALERT — {routing_key}")
        print(f"║  Temperature: {value}°C")
        print(f"║  Timestamp:   {ts}")
        print("╚══════════════════════════════════════╝")

    # ── DLX Inspector ─────────────────────────────────────────────────────────

    def _poll_dlx(self) -> None:
        """
        TODO 4: Drain and inspect all messages currently in QUEUE_DLX.
        """
        n = 0
        while True:
            method, props, body = self._channel.basic_get(QUEUE_DLX, auto_ack=True)
            if method is None:
                break
            n += 1
            try:
                payload = json.loads(body.decode())
            except (json.JSONDecodeError, UnicodeDecodeError):
                payload = {}

            x_death = []
            if props and props.headers:
                x_death = props.headers.get("x-death", [])

            if x_death:
                print(f"[DEAD LETTER] routing_key={method.routing_key}")
                print(f"  Original queue: {x_death[0].get('queue', '?')}")
                print(f"  Death reason:   {x_death[0].get('reason', '?')}")
                print(f"  Death count:    {x_death[0].get('count', '?')}")
                print(f"  Value:          {payload.get('value', '?')}")
            else:
                log.info("[DEAD LETTER] routing_key=%s val=%s (no x-death header)",
                         method.routing_key, payload.get("value", "?"))

        log.info("DLX poll complete — %d dead-lettered messages inspected", n)

    # ── Run ────────────────────────────────────────────────────────────────────

    def run(self) -> None:
        self.connect()
        log.info("Consumer ready. Consuming from %s (prefetch=%d)", QUEUE_ALL, PREFETCH_COUNT)
        try:
            self._channel.start_consuming()
        except KeyboardInterrupt:
            self._channel.stop_consuming()
        finally:
            if self._connection and not self._connection.is_closed:
                self._connection.close()
            log.info("Final stats — processed: %d  failed(DLX): %d  alerts: %d",
                     self._processed, self._failed, self._alerts_seen)


if __name__ == "__main__":
    consumer = SmartFactoryConsumer()
    consumer.run()
