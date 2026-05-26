# Module 2 Assignment — Packet Analysis
## Task 4: Wire-Level Protocol Annotation

> Captures taken with **tshark** on **lo0** (macOS loopback) while MQTT publisher,
> CoAP observer, and AMQP producer ran simultaneously.
> Files: `captures/mqtt.pcap` (386 pkts), `captures/coap.pcap` (17 pkts), `captures/amqp.pcap` (1077 pkts).
> Fresh-connection capture for CONNECT: `captures/mqtt_connect.pcap`.

---

## 4.2 MQTT Packet Annotations

### 4.2.1 — CONNECT Packet
*(mqtt_connect.pcap, Frame 5 — publisher opens a brand-new TCP connection to port 1883)*

**Raw MQTT bytes (after 56-byte IP+TCP headers):**
```
10 45 00 04 4d 51 54 54 04 2c 00 3c 00 1a
73 6d 61 72 74 66 61 63 74 6f 72 79 2d 70
75 62 6c 69 73 68 65 72 2d 30 30 31 00 14
66 61 63 74 6f 72 79 2f 6c 69 6e 65 31 2f
73 74 61 74 75 73 00 07 6f 66 66 6c 69 6e 65
```

| Field | Offset | Raw Hex | Decoded Value |
|-------|--------|---------|---------------|
| Fixed header byte 1 | 0 | `10` | Type = **CONNECT** (0001), flags = 0000 |
| Remaining length | 1 | `45` | **69 bytes** (single-byte, bit 7 = 0) |
| Protocol name length | 2–3 | `00 04` | 4 |
| Protocol name | 4–7 | `4d 51 54 54` | **"MQTT"** |
| Protocol version | 8 | `04` | **4** → MQTT v3.1.1 |
| Connect flags | 9 | `2c` | See bit expansion below |
| Keep-alive | 10–11 | `00 3c` | **60 seconds** |
| Client ID length | 12–13 | `00 1a` | 26 |
| Client ID | 14–39 | `73 6d 61 72 74 66 61 63 74 6f 72 79 2d 70 75 62 6c 69 73 68 65 72 2d 30 30 31` | **"smartfactory-publisher-001"** |
| Will Topic length | 40–41 | `00 14` | 20 |
| Will Topic | 42–61 | `66 61 63 74 6f 72 79 2f 6c 69 6e 65 31 2f 73 74 61 74 75 73` | **"factory/line1/status"** |
| Will Message length | 62–63 | `00 07` | 7 |
| Will Message | 64–70 | `6f 66 66 6c 69 6e 65` | **"offline"** (LWT payload) |

**Connect Flags byte breakdown (0x2C = 0010 1100):**

| Bit | 7 | 6 | 5 | 4 | 3 | 2 | 1 | 0 |
|-----|---|---|---|---|---|---|---|---|
| Name | UserName | Password | Will Retain | Will QoS (MSB) | Will QoS (LSB) | Will Flag | Clean Session | Reserved |
| Value | `0` | `0` | `1` | `0` | `1` | `1` | `0` | `0` |
| Meaning | no username | no password | **LWT retained** | Will QoS = **01 = 1** | → | **LWT present** | **persistent session** | — |

> **Interpretation:** The publisher sets `clean_session=False` (persistent session), declares an LWT on `factory/line1/status` with QoS 1 and retain=True. If the publisher disconnects ungracefully, the broker publishes "offline" to that topic automatically.

---

### 4.2.2 — QoS 1 PUBLISH Packet
*(mqtt_connect.pcap, Frame 17 — topic: `factory/line1/temperature`, MID = 3)*

**Raw MQTT bytes:**
```
32 9f 01 00 19 66 61 63 74 6f 72 79 2f 6c 69
6e 65 31 2f 74 65 6d 70 65 72 61 74 75 72 65
00 03 7b 22 6c 69 6e 65 22 3a 20 22 6c 69 6e
65 31 22 2c 20 22 73 65 6e 73 6f 72 22 3a 20
22 74 65 6d 70 65 72 61 74 75 72 65 22 2c 20
22 76 61 6c 75 65 22 3a 20 37 36 2e 33 38 ...
```

| Field | Offset | Raw Hex | Decoded Value |
|-------|--------|---------|---------------|
| Fixed header byte 1 | 0 | `32` | See bit expansion below |
| Remaining length | 1–2 | `9f 01` | **159 bytes** (multi-byte: 0x9F & 0x7F = 31; 0x01 × 128 = 128; total = 159) |
| Topic length | 3–4 | `00 19` | **25** |
| Topic string | 5–29 | `66 61 63 74 6f 72 79 2f 6c 69 6e 65 31 2f 74 65 6d 70 65 72 61 74 75 72 65` | **"factory/line1/temperature"** |
| Packet Identifier | 30–31 | `00 03` | **3** |
| Payload | 32–… | `7b 22 6c 69 6e 65 22 3a 20 22 6c 69 6e 65 31 22 …` | `{"line": "line1", "sensor": "temperature", "value": 76.38, "unit": "C", "timestamp": "2026-05-25T01:08:55...", "seq": 1}` |

**Fixed header byte 1 bit expansion (0x32 = 0011 0010):**

| Bits 7–4 (packet type) | Bit 3 (DUP) | Bits 2–1 (QoS) | Bit 0 (RETAIN) |
|------------------------|-------------|----------------|----------------|
| `0011` = **PUBLISH (3)** | `0` = not a duplicate | `01` = **QoS 1** | `0` = not retained |

**Multi-byte remaining length decoding:**
```
Byte 1: 0x9F = 1001 1111  → continuation bit=1, value bits = 001 1111 = 31
Byte 2: 0x01 = 0000 0001  → continuation bit=0, value bits = 000 0001 = 1
Result: 31 + (1 × 128) = 159 bytes
```

---

### 4.2.3 — PUBACK Packet
*(mqtt_connect.pcap, Frame 23 — broker acknowledges MID = 3)*

**Raw MQTT bytes:**
```
40 02 00 03
```

| Field | Offset | Raw Hex | Decoded Value |
|-------|--------|---------|---------------|
| Fixed header | 0 | `40` | Type = **PUBACK** (0100), flags = 0000 |
| Remaining length | 1 | `02` | 2 bytes |
| Packet Identifier | 2–3 | `00 03` | **3** |

**Packet Identifier match confirmation:**

| Packet | MID | Match? |
|--------|-----|--------|
| PUBLISH (Frame 17) | **3** | — |
| PUBACK  (Frame 23) | **3** | **✓ YES** |

> The broker echoes the same Packet Identifier in PUBACK, confirming delivery of that specific PUBLISH. The publisher removes the message from its retry buffer on receipt of this ACK.

---

## 4.3 CoAP Packet Annotations

### 4.3.1 — CON GET Request (with Observe=0 to subscribe)
*(coap.pcap, Frame 2 — observer subscribes to `coap://localhost/factory/line1/temperature`, MID = 14993)*

**Raw CoAP bytes (after 28-byte null+IP+UDP headers):**
```
42 01 3a 91 d5 c8 39 6c 6f 63 61 6c 68 6f 73 74
30 57 66 61 63 74 6f 72 79 05 6c 69 6e 65 31 0b
74 65 6d 70 65 72 61 74 75 72 65
```

| Field | Bytes | Raw Hex | Decoded Value |
|-------|-------|---------|---------------|
| Header byte 0 | 0 | `42` | See full bit expansion below |
| Code | 1 | `01` | **0.01 = GET** |
| Message ID | 2–3 | `3a 91` | **14993** |
| Token | 4–5 | `d5 c8` | 0xD5C8 (2-byte token) |
| Opt #1 Uri-Host (delta=3, len=9) | 6–15 | `39` + `6c 6f 63 61 6c 68 6f 73 74` | Option **3**, "**localhost**" |
| Opt #2 Observe (delta=3, len=0) | 16 | `30` | Option **6**, value=0 → **Register** |
| Opt #3 Uri-Path (delta=5, len=7) | 17–24 | `57` + `66 61 63 74 6f 72 79` | Option **11**, "**factory**" |
| Opt #4 Uri-Path (delta=0, len=5) | 25–30 | `05` + `6c 69 6e 65 31` | Option 11, "**line1**" |
| Opt #5 Uri-Path (delta=0, len=11) | 31–42 | `0b` + `74 65 6d 70 65 72 61 74 75 72 65` | Option 11, "**temperature**" |

**Header byte 0 full bit expansion (0x42 = 0100 0010):**

| Bit 7 | Bit 6 | Bit 5 | Bit 4 | Bit 3 | Bit 2 | Bit 1 | Bit 0 |
|-------|-------|-------|-------|-------|-------|-------|-------|
| Ver   | Ver   | T     | T     | TKL   | TKL   | TKL   | TKL   |
| `0`   | `1`   | `0`   | `0`   | `0`   | `0`   | `1`   | `0`   |
| **Version=1** | | **Type=00=CON** | | **TKL=0010=2 bytes** | | | |

**Uri-Path delta encoding explanation:**
- Options are encoded as *delta from previous option number*.
- Opt #1: delta=3 → 0+3 = **option 3** (Uri-Host)
- Opt #2: delta=3 → 3+3 = **option 6** (Observe)
- Opt #3: delta=5 → 6+5 = **option 11** (Uri-Path)  ← first path segment
- Opt #4: delta=0 → 11+0 = **option 11** (Uri-Path) ← second segment
- Opt #5: delta=0 → 11+0 = **option 11** (Uri-Path) ← third segment

---

### 4.3.2 — ACK 2.05 Content Response
*(coap.pcap, Frame 3 — server's response to MID=14993)*

**Raw CoAP bytes:**
```
62 45 3a 91 d5 c8 60 61 32 ff 7b 22 76 61 6c 75
65 22 3a 20 39 30 2e 32 31 33 2c 20 22 75 6e 69
74 22 3a 20 22 43 22 2c 20 22 74 73 22 3a 20 22
32 30 32 36 2d 30 35 2d 32 36 54 30 32 3a 30 38
3a 35 34 2e 31 32 35 37 34 33 2b 30 30 3a 30 30
22 7d
```

| Field | Bytes | Raw Hex | Decoded Value |
|-------|-------|---------|---------------|
| Header byte 0 | 0 | `62` | Ver=01, T=**10=ACK**, TKL=0010 (2-byte token) |
| Code | 1 | `45` | **2.05 = Content** |
| Message ID | 2–3 | `3a 91` | **14993** ← matches request ✓ |
| Token | 4–5 | `d5 c8` | **0xD5C8** ← matches request token ✓ |
| Opt Observe (delta=6, len=0) | 6 | `60` | Option **6**, sequence = **0** (first notification) |
| Opt Content-Format (delta=6, len=1) | 7–8 | `61 32` | Option **12**, value = **0x32 = 50** = application/json |
| Payload Marker | 9 | `ff` | **0xFF** — mandatory separator before payload |
| Payload | 10–… | `7b 22 76 61 6c 75 65 22 3a 20 39 30 2e 32 31 33 …` | `{"value": 90.213, "unit": "C", "ts": "2026-05-26T02:08:54.125743+00:00"}` |

**Content-Format option decoding (bytes `61 32`):**
```
61 = 0110 0001
     ^^^^ = delta nibble = 6  → option# = 6 (prev) + 6 = 12 (Content-Format)
          ^^^^ = length nibble = 1  → 1 byte value follows
32 = 0x32 = 50 decimal  → IANA CoAP Content-Format 50 = application/json
```

---

### 4.3.3 — Observe Notification (CON from server)
*(coap.pcap, Frame 6 — server pushes fresh temperature reading to subscribed observer)*

**Raw CoAP bytes (Frame 6):**
```
42 45 64 e8 d5 c8 61 01 61 32 ff 7b 22 76 61 6c
75 65 22 3a 20 36 39 2e 39 39 35 2c 20 22 75 6e
69 74 22 3a 20 22 43 22 ...
```

| Field | Value | Notes |
|-------|-------|-------|
| Observe option number | **6** | Defined in RFC 7641 |
| Observe sequence value | **1** (`61 01`) | Increments from 0 with each server-push notification |
| Message type | **CON** (Confirmable, byte `42`) | Server requires ACK from observer for reliable delivery |
| Response code | **2.05 Content** (byte `45`) | Carries updated sensor reading |
| MID | **25832** (`64 e8`) | Echoed in ACK from observer |
| Token | **0xD5C8** | Same token as original GET request |
| Frame 7 | Empty ACK, MID=25832 | Observer sends ACK immediately on receipt |

**Sequence progression in coap.pcap:**

| Frame | Direction | Type | MID | Observe seq | Payload value |
|-------|-----------|------|-----|-------------|---------------|
| 6 | Server→Client | CON | 25832 | **1** | 69.995 °C (line1) |
| 7 | Client→Server | ACK | 25832 | — | empty ACK |
| 8 | Server→Client | CON | 25833 | **1** | (line2 temperature) |
| 9 | Client→Server | ACK | 25833 | — | empty ACK |
| 10 | Server→Client | CON | 25834 | **2** | (line1 temperature) |
| 11 | Client→Server | ACK | 25834 | — | empty ACK |

> Observe sequence numbers increment monotonically. Stale/duplicate notifications are detected when the incoming sequence number does NOT satisfy `(seq - last) & 0xFFFFFF < 0x7FFFFF` (RFC 7641 §4.4 half-open window in 2^24 space).

---

## 4.4 AMQP Frame Annotations — [I know it should be igonred, but i did as extra]



### 4.4.1 — Basic.Publish Method Frame
*(amqp.pcap, Frame 1 — producer publishes a CRITICAL temperature alert)*

**Raw AMQP bytes (after TCP headers):**
```
01 00 01 00 00 00 38 00 3c 00 28 00 00 0d 69 6f
74 2e 74 65 6c 65 6d 65 74 72 79 22 66 61 63 74
6f 72 79 2e 6c 69 6e 65 31 2e 74 65 6d 70 65 72
61 74 75 72 65 2e 63 72 69 74 69 63 61 6c 01 ce
```

| Field | Offset | Raw Hex | Decoded Value |
|-------|--------|---------|---------------|
| Frame Type byte | 0 | `01` | **Method frame** (1) |
| Channel | 1–2 | `00 01` | Channel **1** |
| Payload Size (frame length) | 3–6 | `00 00 00 38` | **56 bytes** |
| Class ID | 7–8 | `00 3c` | **60 = Basic** |
| Method ID | 9–10 | `00 28` | **40 = Publish** |
| Ticket (deprecated) | 11–12 | `00 00` | 0 |
| Exchange name length | 13 | `0d` | 13 |
| Exchange name | 14–26 | `69 6f 74 2e 74 65 6c 65 6d 65 74 72 79` | **"iot.telemetry"** |
| Routing key length | 27 | `22` | 34 |
| Routing key | 28–61 | `66 61 63 74 6f 72 79 2e 6c 69 6e 65 31 2e 74 65 6d 70 65 72 61 74 75 72 65 2e 63 72 69 74 69 63 61 6c` | **"factory.line1.temperature.critical"** |
| Mandatory flag | 62 | `01` | **True** — broker must route or return as undeliverable |
| Frame End byte | 63 | `ce` | **0xCE** (decimal 206) — mandatory AMQP frame terminator |

> **Mandatory=True** means the broker will send a `Basic.Return` if no queue matches the routing key. Our `#.critical` binding on `alerts-queue` ensures this message is always routed without triggering a return.

---

### 4.4.2 — Content-Header Frame
*(amqp.pcap, Frame 2 — carries message metadata for the same publish)*

**Raw AMQP bytes:**
```
02 00 01 00 00 00 2e 00 3c 00 00 00 00 00 00 00
00 00 84 91 40 10 61 70 70 6c 69 63 61 74 69 6f
6e 2f 6a 73 6f 6e 02 05 36 30 30 30 30 00 00 00
00 6a 13 9e 5c ce
```

| Field | Offset | Raw Hex | Decoded Value |
|-------|--------|---------|---------------|
| Frame Type byte | 0 | `02` | **Content-Header frame** (2) |
| Channel | 1–2 | `00 01` | Channel **1** |
| Frame length | 3–6 | `00 00 00 2e` | **46 bytes** |
| Class ID | 7–8 | `00 3c` | **60 = Basic** |
| Weight | 9–10 | `00 00` | 0 (always 0, reserved) |
| Body size | 11–18 | `00 00 00 00 00 00 00 84` | **132 bytes** (total payload body) |
| Property flags | 19–20 | `91 40` | **0x9140** — see flag breakdown below |
| Content-Type | 21– | `10` + `61 70 70 6c 69 63 61 74 69 6f 6e 2f 6a 73 6f 6e` | length=16, **"application/json"** |
| Delivery-Mode | — | `02` | **2 = Persistent** (survives broker restart) |
| Expiration | — | `05` + `36 30 30 30 30` | length=5, **"60000"** ms TTL |
| Timestamp | — | `00 00 00 00 6a 13 9e 5c` | Unix epoch **1779670620** = 2026-05-25 00:57:00 UTC |
| Frame End byte | last | `ce` | **0xCE** |

**Property flags breakdown (0x9140 = 1001 0001 0100 0000):**

| Bit 15 | 14 | 13 | 12 | 11 | 10 | 9 | 8 | 7 | 6 | 5–0 |
|--------|----|----|----|----|----|----|---|---|---|-----|
| Content-Type | Encoding | Headers | Delivery-Mode | Priority | Corr-Id | Reply-To | Expiration | Msg-Id | Timestamp | … |
| **1** ✓ | 0 | 0 | **1** ✓ | 0 | 0 | 0 | **1** ✓ | 0 | **1** ✓ | 0 |

> Properties present: **Content-Type**, **Delivery-Mode**, **Expiration**, **Timestamp**.

---

### 4.4.3 — Heartbeat Frame
*(amqp.pcap, Frame 437 — broker → producer keepalive)*

**Raw AMQP bytes:**
```
08 00 00 00 00 00 00 ce
```

| Field | Offset | Raw Hex | Decoded Value |
|-------|--------|---------|---------------|
| Frame Type byte | 0 | `08` | **Heartbeat frame** (8) |
| Channel | 1–2 | `00 00` | Channel **0** — always channel 0 |
| Frame length | 3–6 | `00 00 00 00` | **0 bytes** — empty payload |
| Frame End byte | 7 | `ce` | **0xCE** |

**Total wire size:** 8 bytes (AMQP) inside a 64-byte TCP segment.

**Why the payload is empty:**
A Heartbeat frame's sole purpose is to prove the TCP connection is alive between broker and client. It carries no application data — the frame type byte `08` is the entire signal. AMQP 0-9-1 specifies that both sides send a Heartbeat every *n* seconds (negotiated during Connection.Tune; our producer uses `heartbeat=60`). If either side misses two consecutive heartbeats, it considers the connection dead and closes the socket. Because there is nothing to communicate beyond "I am alive," the payload length is always 0 and only the 7-byte frame envelope (type + channel + length + frame-end) is needed.

---


