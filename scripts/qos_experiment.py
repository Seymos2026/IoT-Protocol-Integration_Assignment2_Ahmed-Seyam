

import json
import random
import threading
import time

import paho.mqtt.client as mqtt

BROKER_HOST = "localhost"
BROKER_PORT = 1883
N_MESSAGES  = 100
LOSS_RATE   = 0.10


# ── Single QoS experiment 

def run_qos(qos_level: int) -> dict:
    """Run one QoS-level experiment and return result dict."""
    received_seqs: set = set()
    duplicates = 0
    latencies  = []
    lock       = threading.Lock()

    topic = f"exp/qos{qos_level}"

    sub = mqtt.Client(client_id=f"exp-sub-{qos_level}-{int(time.time())}")

    def on_message(client, userdata, msg):
        nonlocal duplicates
        try:
            data = json.loads(msg.payload)
            seq      = data["seq"]
            sent_ts  = data["sent_ts"]
            latency  = (time.time() - sent_ts) * 1000
            with lock:
                latencies.append(latency)
                if seq in received_seqs:
                    duplicates += 1
                received_seqs.add(seq)
        except Exception:
            pass

    sub.on_message = on_message
    sub.connect(BROKER_HOST, BROKER_PORT)
    sub.subscribe(topic, qos=qos_level)
    sub.loop_start()
    time.sleep(0.3)

    pub = mqtt.Client(client_id=f"exp-pub-{qos_level}-{int(time.time())}")
    pub.connect(BROKER_HOST, BROKER_PORT)
    pub.loop_start()
    time.sleep(0.2)

    sent = N_MESSAGES   # always 100 attempted
    actually_published = 0

    for seq in range(N_MESSAGES):
        payload = json.dumps({"seq": seq, "sent_ts": time.time()}).encode()

        if qos_level == 0:
           
            if random.random() < LOSS_RATE:
                continue                    # simulate packet lost in transit
            pub.publish(topic, payload, qos=0)
            actually_published += 1

        elif qos_level == 1:
          
            pub.publish(topic, payload, qos=1)
            actually_published += 1
            if random.random() < LOSS_RATE:
                # Re-publish same seq to simulate duplicate caused by lost PUBACK
                time.sleep(0.05)
                pub.publish(topic, payload, qos=1)

        else:
           
            pub.publish(topic, payload, qos=2)
            actually_published += 1

        time.sleep(0.03)

    # Wait for in-flight messages to settle
    time.sleep(4.0)

    pub.loop_stop()
    pub.disconnect()
    sub.loop_stop()
    sub.disconnect()

    unique_recv = len(received_seqs)
    lost        = max(0, sent - unique_recv)

    return {
        "sent":        sent,           # total attempted (always N_MESSAGES)
        "received":    unique_recv,    # unique messages received
        "lost":        lost,
        "loss_pct":    lost / sent * 100 if sent else 0,
        "duplicates":  duplicates,
        "avg_lat_ms":  sum(latencies) / len(latencies) if latencies else 0,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\nRunning QoS loss experiment  "
          f"(N={N_MESSAGES} msgs, ~{LOSS_RATE*100:.0f}% simulated loss)\n")

    results = {}
    for qos in [0, 1, 2]:
        print(f"  Running QoS {qos}...", end=" ", flush=True)
        results[qos] = run_qos(qos)
        print("done")

    # ── Table ─────────────────────────────────────────────────────────────────
    print()
    print("=" * 72)
    print(f"  QoS Comparison Results — 10% simulated loss  (N={N_MESSAGES})")
    print("=" * 72)
    print(f"{'QoS':<8} {'Sent':>6} {'Received':>10} {'Lost':>6} {'Loss%':>7} "
          f"{'Dupes':>7} {'Avg Lat(ms)':>14}")
    print("-" * 72)

    for qos in [0, 1, 2]:
        r = results[qos]
        print(f"{'QoS '+str(qos):<8} {r['sent']:>6} {r['received']:>10} "
              f"{r['lost']:>6} {r['loss_pct']:>6.1f}% "
              f"{r['duplicates']:>7} {r['avg_lat_ms']:>13.1f}")

    print("=" * 72)
    print()
    print("Simulation logic used:")
    print("  QoS 0  → 10% of publish() calls skipped (no retry → lost forever)")
    print("  QoS 1  → all published + 10% re-published (lost PUBACK → duplicate)")
    print("  QoS 2  → all published once (4-way handshake → exactly-once)")
    print()
    print("Copy the table into Section 5.1 of your report.")
    print()


if __name__ == "__main__":
    main()
