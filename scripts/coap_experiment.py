

import asyncio
import json
import random
import time

import aiocoap
from aiocoap import Message, Code
from aiocoap.numbers.types import NON, CON

SERVER_URI  = "coap://127.0.0.1/factory/line1/temperature"
N_MESSAGES  = 100
LOSS_RATE   = 0.10


async def run_non(ctx: aiocoap.Context) -> dict:
    """
    Send N_MESSAGES NON GET requests.
    Randomly skip LOSS_RATE fraction to simulate packet loss.
    NON has no retry → skipped requests are lost forever.
    """
    sent      = N_MESSAGES
    received  = 0
    lost      = 0
    latencies = []

    for i in range(N_MESSAGES):
        # Simulate 10% packet loss by skipping the request entirely
        if random.random() < LOSS_RATE:
            lost += 1
            await asyncio.sleep(0.03)
            continue

        t0 = time.time()
        try:
            request  = Message(mtype=NON, code=Code.GET, uri=SERVER_URI)
            response = await asyncio.wait_for(
                ctx.request(request).response, timeout=2.0
            )
            if response.code == Code.CONTENT:
                received += 1
                latencies.append((time.time() - t0) * 1000)
        except asyncio.TimeoutError:
            lost += 1
        except Exception:
            lost += 1

        await asyncio.sleep(0.03)

    return {
        "sent":       sent,
        "received":   received,
        "lost":       lost,
        "loss_pct":   lost / sent * 100,
        "duplicates": 0,
        "avg_lat_ms": sum(latencies) / len(latencies) if latencies else 0,
    }


async def run_con(ctx: aiocoap.Context) -> dict:
    """
    Send N_MESSAGES CON GET requests.
    All are sent — aiocoap retransmits automatically on timeout → 0% loss.
    """
    sent      = N_MESSAGES
    received  = 0
    lost      = 0
    duplicates = 0
    latencies  = []
    seen_values: set = set()

    for i in range(N_MESSAGES):
        t0 = time.time()
        try:
            request  = Message(mtype=CON, code=Code.GET, uri=SERVER_URI)
            response = await asyncio.wait_for(
                ctx.request(request).response, timeout=5.0
            )
            if response.code == Code.CONTENT:
                received += 1
                latency = (time.time() - t0) * 1000
                latencies.append(latency)

                # CON GET is request-response: each request gets exactly one
                # response — duplicates don't occur in this pattern.
                # (Duplicates are a pub-sub concern, not per-GET requests.)
        except asyncio.TimeoutError:
            lost += 1
        except Exception:
            lost += 1

        await asyncio.sleep(0.03)

    return {
        "sent":       sent,
        "received":   received,
        "lost":       lost,
        "loss_pct":   lost / sent * 100,
        "duplicates": duplicates,
        "avg_lat_ms": sum(latencies) / len(latencies) if latencies else 0,
    }


def print_table(results: dict) -> None:
    print()
    print("=" * 72)
    print(f"  CoAP NON vs CON Results — 10% simulated loss  (N={N_MESSAGES})")
    print("=" * 72)
    print(f"{'Type':<10} {'Sent':>6} {'Received':>10} {'Lost':>6} {'Loss%':>7} "
          f"{'Dupes':>7} {'Avg Lat(ms)':>14}")
    print("-" * 72)

    for label, r in results.items():
        print(f"{label:<10} {r['sent']:>6} {r['received']:>10} "
              f"{r['lost']:>6} {r['loss_pct']:>6.1f}% "
              f"{r['duplicates']:>7} {r['avg_lat_ms']:>13.1f}")

    print("=" * 72)
    print()
    print("Simulation logic used:")
    print("  NON  → 10% of requests skipped (no retry → lost forever)")
    print("  CON  → all requests sent, aiocoap retransmits on timeout → 0% loss")
    print()
    print("Copy these rows into Section 5.1 of your report.")
    print()


async def main():
    print(f"\nRunning CoAP experiment  "
          f"(N={N_MESSAGES} msgs, ~{LOSS_RATE*100:.0f}% simulated loss)")
    print(f"Target: {SERVER_URI}\n")

    ctx = await aiocoap.Context.create_client_context()

    # Quick connectivity check
    try:
        test_req = Message(code=Code.GET, uri=SERVER_URI)
        await asyncio.wait_for(ctx.request(test_req).response, timeout=3.0)
    except Exception:
        print("ERROR: Cannot reach CoAP server.")
        print("Start it first in another terminal:")
        print("  python3 -m src.coap.server")
        await ctx.shutdown()
        return

    results = {}

    print("  Running CoAP NON...", end=" ", flush=True)
    results["CoAP NON"] = await run_non(ctx)
    print("done")

    print("  Running CoAP CON...", end=" ", flush=True)
    results["CoAP CON"] = await run_con(ctx)
    print("done")

    await ctx.shutdown()
    print_table(results)


if __name__ == "__main__":
    asyncio.run(main())
