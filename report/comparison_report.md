# Module 2 Assignment — Protocol Comparison Report

**Student Name:** Ahmed Seyam
**Student ID:**   101039783
**Date:**         2026-05-23

---

## 5.1 QoS Comparison Results Table (Evidence for this task as a screenshot [Screenshots_Evidences / Task 1_QOS.png &Task1_CoAP.png])

> MQTT rows: `pytest tests/mqtt/test_qos_loss.py -v -s` via a lossy proxy (10% QoS-0 drop rate, loopback).
> CoAP rows: `python3 scripts/coap_experiment.py` with 10% simulated NON loss (N=100 requests each).

========================================================================
      QoS Comparison Results (Target: 100 msgs, ~10% loss)
========================================================================
QoS          Sent   Received     Lost    Loss%    Dupes    Avg Lat(ms)
------------------------------------------------------------------------
QoS 0         100         92        8     8.0%        0           2.3
QoS 1         100        100        0     0.0%        0           2.6
QoS 2         100        100        0     0.0%        0           5.7
CoAP NON      100         90       10     10.0%       0           1.7
CoAP CON      100        100       0      0.0%        0           1.9
========================================================================



**Analysis Questions:**

1. **Why does QoS 0 lose messages while QoS 1 and 2 do not?**

   QoS 0 is a "fire and forget" delivery — once the PUBLISH packet leaves the sender there is no acknowledgement, no retransmit buffer, and no session state. Under 10% packet loss, roughly one in ten packets is silently dropped at the network layer and no recovery occurs. QoS 1 and QoS 2 both require the receiver to send an acknowledgement (PUBACK or the four-part PUBREC/PUBREL/PUBCOMP handshake); if the ACK does not arrive within the keep-alive window, the broker or client re-sends the PUBLISH with the DUP flag set, guaranteeing eventual delivery even under sustained packet loss.

2. **QoS 1 may show duplicates. Under what circumstances does this happen, and is it a problem for sensor telemetry?**

   A QoS 1 duplicate occurs when the sender transmits a PUBLISH, the receiver processes and ACKs it, but the PUBACK is lost in transit. The sender times out waiting for the ACK and re-sends the same PUBLISH with the DUP flag set, so the receiver delivers it a second time. For sensor telemetry — where readings carry a monotonically increasing `seq` number and a UTC timestamp — duplicates are easy to detect and idempotently discard on the consumer side. They therefore represent a minor overhead rather than a correctness problem, making QoS 1 a reasonable default for high-frequency sensor streams.

3. **QoS 2 has higher latency than QoS 1. What causes this, and when is the trade-off worth it?**

   QoS 2 requires a four-message handshake: PUBLISH → PUBREC → PUBREL → PUBCOMP. Each round-trip adds network latency, and the sender must hold state for each in-flight message until the full exchange completes. Our experiment confirms this directly: QoS 2 measured **5.7 ms** average latency versus QoS 1's **2.6 ms** — a 2.2× overhead that maps precisely to the extra round-trip the handshake requires. Zero duplicates were recorded for QoS 1 because the lossy proxy drops frames before they reach the broker, so the broker never generates a PUBACK that could be lost; in a real network where PUBACKs can be lost in transit, QoS 1 would show duplicates (the sender retransmits when the ACK does not arrive). The trade-off is worthwhile for actuator commands where exactly-once semantics are safety-critical — for example, switching a cooling fan on/off, where a duplicate could cause unintended toggling — but unnecessary for continuous sensor readings where the `seq` field already allows consumer-side deduplication.

4. **Why does CoAP NON lose messages while CoAP CON does not?**

   CoAP NON (Non-Confirmable) messages are sent over UDP with no acknowledgement and no retransmission — equivalent to MQTT QoS 0. Under 10% packet loss, a NON message that is dropped is permanently lost with no recovery mechanism. Our experiment confirmed this exactly: **10 out of 100 NON requests were lost (10.0%)** — matching the 10% simulation target precisely. Average latency for the surviving 90 messages was **1.7 ms**. CoAP CON (Confirmable) messages require the receiver to send an ACK; if no ACK arrives within the retransmission timeout, aiocoap automatically retransmits the message with exponential backoff (up to 4 attempts). This guarantees delivery even under packet loss, which our experiment confirmed: **0 out of 100 CON requests were lost (0.0%)** with an average latency of **1.9 ms** — only 0.2 ms higher than NON, showing that the ACK overhead is negligible on loopback.

5. **CoAP CON shows 0 duplicates in the GET request-response pattern. Would duplicates appear in a different scenario?**

   In a direct GET request-response pattern, each request produces exactly one response — duplicates cannot occur because each transaction is independent. However, in a CoAP **Observe** (pub-sub) subscription, duplicates can occur when a CON notification is retransmitted: if the server sends a notification, the client ACKs it, but the ACK is lost, the server retransmits the same notification — the client receives it twice. This is equivalent to MQTT QoS 1 duplicates. Our observer implementation detects this using the Observe sequence number; stale or duplicate notifications are identified when the incoming sequence number is not greater than the last seen value (accounting for wrap-around at 2^24).

---

## 5.2 CoAP–HTTP Proxy Mapping (Evidence for this task as a video [Evidence Videos / Task 2.mp4])

> Results from running `pytest tests/coap/test_proxy.py -v -s` with the aiocoap built-in forward proxy.

=================================================================
  Section 5.2 — CoAP-HTTP Proxy Mapping Table
=================================================================
HTTP Header                  CoAP Option            Observed Value
-----------------------------------------------------------------
  Content-Type               Option 12 (Content-Format) application/json
  Cache-Control: max-age     Option 14 (Max-Age)    max-age=60
  ETag                       Option 4  (ETag)       "a3f8d21c"
  Location                   Option 8  (Location-Path) /factory/line1/temperature
=================================================================

**Mapping explanation:** The CoAP-HTTP proxy performs a semantic translation between the two protocol namespaces. CoAP **Content-Format** (option number 12) carries a numeric code that identifies the payload type; code **50** means `application/json`, which the proxy translates to the HTTP `Content-Type: application/json` header. The CoAP **Max-Age** option (option 14) specifies how many seconds a resource representation remains fresh for caching; the proxy maps this directly to the HTTP `Cache-Control: max-age=60` header, preserving the same TTL value. CoAP **ETag** (option 4) is a short binary opaque identifier used for conditional requests; the proxy hex-encodes the raw bytes and places the result in the HTTP `ETag` header (e.g. `"a3f8d21c"`). Finally, the resource's URI path — carried as CoAP **Uri-Path** / **Location-Path** (option 8) — is reflected in the HTTP `Location` header so the HTTP client knows the canonical path of the resource it retrieved.

---

## 5.3 Protocol Selection Recommendation

### Data Path Recommendations

| Data Path | Recommended Protocol | Justification |
|-----------|---------------------|---------------|
| Sensor → Cloud (high frequency, <100 ms latency) | MQTT QoS 1 | Lowest overhead, broker fan-out, 2.6 ms avg latency under 10% loss |
| Actuator commands (safety-critical, exactly-once) | MQTT QoS 2 | Four-way handshake guarantees exactly-once delivery |
| Backend service-to-service routing | AMQP | Topic-exchange routing, dead-letter queues, persistent delivery |
| OTA firmware delivery to constrained MCU (Class 2) | CoAP Block2 | Native block-wise transfer, fits UDP MTU on constrained devices |

### Detailed Justification

**Sensor → Cloud (MQTT QoS 1)**

In the SmartFactory scenario, six sensors publish readings every second across two production lines, yielding 6 readings/s per line and 12 readings/s overall. Latency must stay below 100 ms for real-time dashboarding and anomaly detection. Our MQTT publisher implementation demonstrated that QoS 1 delivers an average round-trip of **2.6 ms under 10% simulated packet loss** — well within the 100 ms budget — while losing zero messages (unlike QoS 0, which lost 8 of 100 readings, an 8.0% loss rate, during the experiment). Even at 2.6 ms the result leaves a 38× margin against the 100 ms latency requirement. The packet capture of a QoS 1 PUBLISH shows a compact fixed header of just 2 bytes plus a 2-byte Packet Identifier, keeping per-message overhead minimal. Broker-side fan-out to multiple subscribers (dashboards, alerting systems, InfluxDB) requires no additional work from the publisher, which further justifies the choice over point-to-point HTTP polling. QoS 2 measured 5.7 ms in our experiment — higher than QoS 1's 2.6 ms, confirming that the four-way handshake adds measurable latency even on loopback. QoS 1's simpler two-message handshake is faster, and its at-least-once guarantee is sufficient for sensor readings that carry a sequence number for consumer-side deduplication.

**Actuator Commands (MQTT QoS 2)**

Cooling fan commands are safety-critical: sending "ON" twice to an already-running fan, or losing an "OFF" command during a thermal emergency, could damage machinery or create a hazard. MQTT QoS 2's four-message PUBLISH/PUBREC/PUBREL/PUBCOMP handshake is the only MQTT delivery mode that eliminates both loss and duplication at the protocol level. Our experiment measured **5.7 ms** average latency for QoS 2 versus **2.6 ms** for QoS 1 — the 2.2× difference matches the additional round-trip introduced by the PUBREC/PUBREL/PUBCOMP exchange. This penalty is irrelevant for actuator commands because they are infrequent (fired only when temperature exceeds 85 °C) and the safety guarantee of exactly-once delivery outweighs the small latency cost. The persistent session (clean_session=False) ensures that an offline actuator gateway will receive buffered commands immediately upon reconnection, preventing missed state transitions. CoAP CON requests could also achieve exactly-once semantics at the application level with an idempotency token, but the MQTT broker already provides this guarantee without additional application logic.

**Backend Service-to-Service Routing (AMQP)** *(implemented as bonus)*

The SmartFactory backend needs to route temperature data to the alerting microservice, vibration data to the predictive-maintenance service, and all readings to the data warehouse — each with different reliability and ordering guarantees. AMQP's topic exchange with binding keys (e.g., `factory.line1.#` or `*.*.temperature`) enables fine-grained, configurable routing without changing publisher code. Dead-letter queues capture failed or expired messages for audit, which is essential for compliance in a manufacturing environment. Publisher confirms give the backend producer end-to-end acknowledgement from the broker rather than just the TCP layer. Neither MQTT nor CoAP offers server-side routing logic or dead-letter semantics natively, making AMQP the correct choice for service mesh communication despite its higher setup complexity.

**OTA Firmware Delivery to Constrained MCU Class 2 (CoAP Block2)**

Class 2 constrained devices (≤ 10 KiB RAM, ≤ 100 KiB Flash, IEEE 802.15.4 with 127-byte frames) cannot run a full TCP/TLS stack and have no buffer for large MQTT payloads. CoAP runs over UDP and natively supports Block2 transfer, which segments a large payload (our firmware manifest was > 3 KB) into negotiated-size blocks (typically 64–1024 bytes each). The MCU requests each block individually with a `BLOCK2` option carrying the block number and size, re-requesting any block that times out — this is exactly the incremental, resumable download that a constrained device needs. aiocoap reassembles the full payload transparently on both client and server sides, as confirmed by our observer implementation. HTTP would require a TCP connection and TLS stack memory that Class 2 devices cannot provide; MQTT would require holding the entire firmware image in a single retained message in broker RAM; only CoAP with Block2 delivers the right combination of low memory footprint, UDP transport, and built-in segmentation.

---

## 5.4 Reflection

### Technical Challenge

The most significant implementation challenge was correctly implementing the CoAP observable resource with asyncio. The `SensorResource._update_loop` coroutine must be started as a background task before the event loop begins serving requests, but `asyncio.ensure_future()` requires a running event loop at the time `__init__` is called. In the test fixture, `build_server()` is called inside an async context so the future is created correctly, but running the server standalone via `asyncio.run(main())` also works because `asyncio.run()` starts an event loop before any code executes. The subtlety is that the future must be created *inside* the running loop, not outside it. I resolved this by placing `asyncio.ensure_future(self._update_loop())` directly inside `__init__`, which is only ever called after the event loop is running in both the test and standalone execution paths.

### Most Surprising Protocol Difference

The most surprising observation during the packet capture task was the sheer verbosity difference between MQTT and CoAP at the wire level. An MQTT QoS 1 PUBLISH for a temperature reading (with JSON payload of roughly 120 bytes) requires a fixed header of just 2 bytes plus a 2-byte Packet Identifier — only 4 bytes of protocol overhead. The corresponding CoAP GET/response exchange, while also compact, encodes each URI path segment as a separate `Uri-Path` option with delta encoding, meaning the path `/factory/line1/temperature` is transmitted across three separate option TLV triplets. What surprised me was that despite CoAP's reputation as a constrained protocol, a complete CON GET + ACK 2.05 exchange for a simple temperature read involves more bytes than a single MQTT PUBLISH/PUBACK pair, once Uri-Path options and the 4-byte token are included. CoAP's efficiency advantage over HTTP is clear, but the comparison to MQTT is more nuanced than the literature suggests.

### Most Complex Protocol to Implement Correctly

CoAP was the most complex protocol to implement correctly, specifically because of the observe subscription lifecycle and stale notification detection. The CoAP Observe option specification (RFC 7641) defines a wrap-around sequence number space of 2^24, and the rule for detecting staleness — "a notification is fresh if its sequence number is in the range (last, last + 2^23] modulo 2^24" — is not immediately intuitive and easy to implement incorrectly as a simple less-than comparison. Additionally, the aiocoap API for observations uses an `async for` loop over `pr.observation` that continues indefinitely until the observation is cancelled, which interacts subtly with `asyncio.wait_for` — the timeout cancels the inner task but the observation object must still be explicitly cancelled to send the Observe=1 deregistration message to the server. Getting clean deregistration right while also handling the first response (which arrives via `pr.response`, not the observation iterator) required careful reading of the aiocoap source and RFC 7641.

---


