# Flight Booking — Concurrent Locking Demo

Simulates N passengers concurrently racing to book **100 seats** on a flight.
Choose the PostgreSQL row-locking strategy at runtime and observe results on a **5×20 grid**.

## Locking strategies

| Choice | Query clause | Behaviour |
|--------|-------------|-----------|
| 1 | `FOR UPDATE` | Waits for any locked row — every transaction serializes. All available seats fill; extra customers queue and get `ErrNoRows` at the end. |
| 2 | `FOR UPDATE SKIP LOCKED` | Skips rows locked by another transaction — true parallelism, no waiting. Under contention some goroutines may find no unlocked row and get nothing. |
| 3 | `FOR UPDATE NOWAIT` | Raises an error immediately if the row is locked — goroutines that collide fail outright and get no seat. |

## Grid legend

```
   X  →  seat not booked
  42  →  booked by customer 42
```

---

## Quick start

### 1. Start PostgreSQL

```bash
docker compose up -d
```

### 2. Apply schema (once)

```bash
docker compose exec -T postgres psql -U postgres -d flight_booking -f /dev/stdin < schema.sql
```

### 3. Interactive mode (pick strategy, 100 customers)

```bash
go run .
```

### 4. Benchmark mode (all 3 strategies × 50/100/200/300/400/500 customers)

```bash
go run . -bench
```

---

## Environment variables

| Variable  | Default        |
|-----------|----------------|
| DB_HOST   | localhost      |
| DB_PORT   | 5432           |
| DB_USER   | postgres       |
| DB_PASS   | postgres       |
| DB_NAME   | flight_booking |

---

## Benchmark results

> 100 seats, varying number of concurrent customers firing simultaneously.

### Seats booked

| Customers | FOR UPDATE | SKIP LOCKED | NOWAIT |
|----------:|:----------:|:-----------:|:------:|
|        50 |         50 |          50 |      1 |
|       100 |        100 |         100 |      1 |
|       200 |        100 |         100 |      1 |
|       300 |        100 |         100 |      1 |
|       400 |        100 |         100 |      1 |
|       500 |        100 |         100 |      1 |

### Latency (ms)

| Customers | FOR UPDATE | SKIP LOCKED | NOWAIT |
|----------:|----------:|------------:|-------:|
|        50 |       59ms |         10ms |    4ms |
|       100 |       73ms |         17ms |    8ms |
|       200 |       78ms |         38ms |   28ms |
|       300 |       91ms |         49ms |   80ms |
|       400 |      118ms |         64ms |   72ms |
|       500 |      160ms |        148ms |   85ms |

---

## Latency curve

```
  159ms |                                                       #
        |                                                       *
        |
        |
        |                                             #
        |
        |                                   #                   +
   79ms |                         #          +
        |               #                             +
        |     #                                       *
        |                                   *
        |                         *
        |                         +
        |     *         *
    0ms |     +         +
        +------------------------------------------------------>
            50   100   200   300   400   500   customers

  FOR UPDATE (#)    SKIP LOCKED (*)    NOWAIT (+)
```

---

## Inferences

### FOR UPDATE — reliable but serialized

- Books **every available seat**, regardless of how many customers compete.
  Extra customers beyond 100 simply queue and walk away empty-handed after waiting.
- Latency grows **sub-linearly** with customer count: 59ms → 160ms for 50→500 customers
  (10× customers, only ~2.7× latency). PostgreSQL's lock queue is efficient — the
  overhead is dominated by the serialized transaction chain, not connection setup.
- This is the **correct choice** when you must guarantee no seat is left empty and
  overbooking is impossible: every goroutine either books or waits until it's certain
  no seat remains.

### FOR UPDATE SKIP LOCKED — fast parallelism, converges under pressure

- Also books **every available seat** (100/100) once customers ≥ seats, because each
  goroutine skips locked rows and grabs a different available one — true parallelism.
- **5–7× faster** than FOR UPDATE at low-to-medium concurrency (50–300 customers):
  10ms vs 59ms, 49ms vs 91ms.
- At 500 customers the gap nearly closes (148ms vs 160ms). At high contention, goroutines
  must skip an increasing number of already-locked rows before finding a free one,
  turning the sequential scan into a bottleneck that approaches serial behavior.
- Best for **job queues and work-stealing** patterns where throughput matters more than
  strict turn-taking and you can tolerate some workers finding no work.

#### What happens inside PostgreSQL with 50 customers

##### FOR UPDATE — one queue, one lane

Every goroutine runs the same query:

```sql
SELECT seat_id FROM seats WHERE status = 'available' LIMIT 1 FOR UPDATE
```

Without `ORDER BY`, PostgreSQL returns the first physical row — **seat 1** — to everyone.

- Goroutine 1 locks seat 1 → runs → commits → releases
- Goroutine 2 tried to lock seat 1, was **blocked**, now wakes up, sees seat 1 is `booked`, re-scans, gets seat 2
- Goroutine 3 was blocked behind goroutine 2, now wakes up...

All 50 goroutines form a **serial chain**. Even though they were fired concurrently, they execute one at a time.

```
Time →
G1:  [---txn---]
G2:            [---txn---]
G3:                      [---txn---]
...
G50:                                         [---txn---]

Wall clock = 50 × one transaction time = ~59ms
```

##### FOR UPDATE SKIP LOCKED — 50 lanes

- Goroutine 1 locks seat 1
- Goroutine 2 sees seat 1 locked → **skips immediately** → locks seat 2
- Goroutine 3 sees seat 1, 2 locked → skips → locks seat 3
- ...all happening **at the same time**

```
Time →
G1:  [---txn---]
G2:  [---txn---]
G3:  [---txn---]
...
G50: [---txn---]

Wall clock = 1 × one transaction time = ~10ms
```

The 50 goroutines run truly in parallel. Wall-clock time is roughly the cost of a **single transaction**, not 50.

##### Why they converge at 500 customers

With 500 goroutines and only 100 seats, once all 100 seats are locked, the remaining 400 goroutines scan the entire table, skip all 100 locked rows, find nothing, and return empty. That sequential skip-scan of 100 rows is non-trivial work, and 400 goroutines doing it simultaneously creates its own pressure — which is why 148ms approaches FOR UPDATE's 160ms.

### FOR UPDATE NOWAIT — broken without retry logic

- Consistently books **only 1 seat** regardless of how many customers are sent.
- **Root cause:** the query `SELECT … LIMIT 1 FOR UPDATE NOWAIT` without `ORDER BY`
  returns the same physical row (seat_id = 1) for every goroutine. All 100+ goroutines
  race for that single row; exactly one wins, and every other transaction receives a
  PostgreSQL error `55P03 (lock_not_available)` immediately and gives up. No other
  seat is ever attempted because there is no retry.
- Latency is the fastest of all (4–85ms) precisely because failures are instant — no
  waiting, no work done.
- NOWAIT only makes sense paired with a **retry loop** (pick a new random seat and
  try again) or with `ORDER BY random()` so competing goroutines don't all target the
  same row. As implemented it is effectively a single-slot mutex.

### Summary comparison

| Property | FOR UPDATE | SKIP LOCKED | NOWAIT |
|---|---|---|---|
| Seats filled (100 available) | 100% | 100% | 1 (no retry) |
| Latency at 100 customers | 73ms | 17ms | 8ms |
| Latency at 500 customers | 160ms | 148ms | 85ms |
| Scales with contention | sub-linear | flat → converges | flat (all errors) |
| Safe against double-booking | yes | yes | yes |
| Suitable for production booking | yes | yes | only with retry |
