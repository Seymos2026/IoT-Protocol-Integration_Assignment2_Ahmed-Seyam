"""
Module 1 Assignment — Task 3.1
AMQP Broker Topology Declaration

Complete all TODO sections. Run this module once to set up the
RabbitMQ topology before running producer or consumer.

Run with:  python -m src.amqp.topology
"""

import logging
import ssl
import pika

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

# ── Connection parameters ─────────────────────────────────────────────────────
BROKER_HOST   = "localhost"
BROKER_PORT   = 5672          # plain AMQP (use 5671 for TLS in Tasks 3.2/3.3)
VHOST         = "/"
CREDENTIALS   = pika.PlainCredentials("guest", "guest")


def get_connection_params(host=BROKER_HOST, port=BROKER_PORT) -> pika.ConnectionParameters:
    return pika.ConnectionParameters(
        host=host, port=port,
        virtual_host=VHOST,
        credentials=CREDENTIALS,
        heartbeat=60,
    )


# ── Exchange names ─────────────────────────────────────────────────────────────
EXCHANGE_TELEMETRY = "iot.telemetry"   # main topic exchange
EXCHANGE_DLX       = "iot.dlx"         # dead letter exchange

# ── Queue names ────────────────────────────────────────────────────────────────
QUEUE_ALERTS      = "alerts-queue"
QUEUE_TEMPERATURE = "temperature-queue"
QUEUE_ALL         = "all-telemetry-queue"
QUEUE_DLX         = "dead-letter-queue"
QUEUE_LINE1       = "line1-queue"


def declare_topology(channel: pika.adapters.blocking_connection.BlockingChannel) -> None:
    """
    Declare all exchanges, queues, and bindings for the SmartFactory topology.
    This function is idempotent — safe to call multiple times.
    """

    # ── Exchanges ─────────────────────────────────────────────────────────────

    # TODO 1: Declare EXCHANGE_TELEMETRY as a durable topic exchange
    channel.exchange_declare(
        exchange=EXCHANGE_TELEMETRY,
        exchange_type="topic",
        durable=True,
    )
    log.info("Exchange declared: %s (topic, durable)", EXCHANGE_TELEMETRY)

    # TODO 2: Declare EXCHANGE_DLX as a durable direct exchange
    channel.exchange_declare(
        exchange=EXCHANGE_DLX,
        exchange_type="direct",
        durable=True,
    )
    log.info("Exchange declared: %s (direct, durable)", EXCHANGE_DLX)

    # ── Dead Letter Queue (declare before queues that reference it) ────────────

    # TODO 3: Declare QUEUE_DLX as a durable queue (no special arguments needed)
    channel.queue_declare(queue=QUEUE_DLX, durable=True)
    log.info("Queue declared: %s", QUEUE_DLX)

    # TODO 4: Bind QUEUE_DLX to EXCHANGE_DLX with routing_key="dead"
    channel.queue_bind(
        queue=QUEUE_DLX,
        exchange=EXCHANGE_DLX,
        routing_key="dead",
    )
    log.info("Bound %s → %s (key=dead)", QUEUE_DLX, EXCHANGE_DLX)

    # ── Application Queues ────────────────────────────────────────────────────

    # TODO 5: Declare QUEUE_ALERTS — durable, bound to EXCHANGE_TELEMETRY with key "#.critical"
    channel.queue_declare(queue=QUEUE_ALERTS, durable=True)
    channel.queue_bind(
        queue=QUEUE_ALERTS,
        exchange=EXCHANGE_TELEMETRY,
        routing_key="#.critical",
    )
    log.info("Queue declared + bound: %s (key=#.critical)", QUEUE_ALERTS)

    # TODO 6: Declare QUEUE_TEMPERATURE — durable, with TTL + DLX args
    channel.queue_declare(
        queue=QUEUE_TEMPERATURE,
        durable=True,
        arguments={
            "x-message-ttl":              60000,
            "x-dead-letter-exchange":     EXCHANGE_DLX,
            "x-dead-letter-routing-key":  "dead",
        },
    )
    channel.queue_bind(
        queue=QUEUE_TEMPERATURE,
        exchange=EXCHANGE_TELEMETRY,
        routing_key="*.*.temperature",
    )
    log.info("Queue declared + bound: %s (key=*.*.temperature, TTL=60s, DLX=iot.dlx)", QUEUE_TEMPERATURE)

    # TODO 7: Declare QUEUE_ALL — durable, with max-length + overflow + DLX
    channel.queue_declare(
        queue=QUEUE_ALL,
        durable=True,
        arguments={
            "x-max-length":               10000,
            "x-overflow":                 "reject-publish-dlx",
            "x-dead-letter-exchange":     EXCHANGE_DLX,
            "x-dead-letter-routing-key":  "dead",
        },
    )
    channel.queue_bind(
        queue=QUEUE_ALL,
        exchange=EXCHANGE_TELEMETRY,
        routing_key="factory.#",
    )
    log.info("Queue declared + bound: %s (key=factory.#, max-length=10000)", QUEUE_ALL)

    # TODO 8: Declare QUEUE_LINE1 — durable, bound with key "factory.line1.#"
    channel.queue_declare(queue=QUEUE_LINE1, durable=True)
    channel.queue_bind(
        queue=QUEUE_LINE1,
        exchange=EXCHANGE_TELEMETRY,
        routing_key="factory.line1.#",
    )
    log.info("Queue declared + bound: %s (key=factory.line1.#)", QUEUE_LINE1)

    log.info("Topology declared successfully")


def setup() -> None:
    """Connect to RabbitMQ and declare the full topology."""
    params = get_connection_params()
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    try:
        declare_topology(channel)
    finally:
        connection.close()
    log.info("Topology setup complete. Check: http://localhost:15672")


if __name__ == "__main__":
    setup()
