# Symbol-sharded matching engine routing

## Why

`MatchingEngine` stores per-symbol orderbook state in process memory
(`DashMap<String, Arc<Orderbook>>`). With N≥2 K8s replicas behind a
round-robin Service, each pod accumulates a divergent view: a place
landing on pod A and a cancel on pod B leaves the cancel returning
`Ok(false)` and the order stuck. The 2026-04-26 integrity probe
confirmed both directions of divergence (45.8% silent-remove rate, up
to 1.55× engine over-count vs DB on active symbols).

This module routes each request that touches engine state to a
deterministic owner pod via consistent hash on symbol. Engine code stays
single-master per symbol (its existing assumption); we keep N replicas
for HA.

## Status

PoC wired through every write handler:

- `/fapi/v1/order` (POST): `developer_trade::new_order` ✓
- `/fapi/v1/order` (DELETE): `developer_trade::cancel_order` ✓
- `/fapi/v1/order` (PUT): `developer_trade::modify_order` ✓
- `/fapi/v1/allOpenOrders` (DELETE): `developer_trade::cancel_all_open_orders` ✓
- `/fapi/v1/batchOrders` (DELETE): `developer_trade::batch_cancel_orders` ✓
- `/api/v1/orders` (POST, JWT): `order::create_order` ✓
- `/api/v1/orders/:id` (DELETE, JWT): `order::cancel_order` ✓ (PK lookup → forward)
- `/api/v1/orders/:id` (PUT, JWT): `order::update_order` ✓ (PK lookup → forward)
- `/api/v1/orders/batch` (POST, JWT): `order::batch_cancel` ✓ (bulk lookup → all-local OR single-owner forward; multi-owner-split logs WARN and runs locally — fan-out deferred)

Engine-state recovery on startup is sharding-aware: `engine::recover_orders_from_db`
takes an `Option<&ShardingConfig>` and skips orders whose symbol's owner
is a different ordinal. Wired in `bootstrap.rs` immediately after the
matching engine is constructed.

Still TODO before flipping the flag in prod:

- **Read handlers** (`market::orderbook`, `developer_trade::open_orders`):
  until they're sharded, depth / open-orders queries against a non-owner
  pod will return empty / stale data. Reads don't cause divergence, so
  this is a UX bug, not a correctness one.
- **Multi-owner batch_cancel fan-out** — currently logs WARN and runs
  locally on the not-owned slice. Most batches target a single symbol or
  one hash bucket, so this is rare; still worth fixing.

Default OFF via `MATCHING_SHARDING_ENABLED=false` so the build can ship
ahead of the K8s rollout.

## Configuration

| Env var | Required | Default | Notes |
|---|---|---|---|
| `MATCHING_SHARDING_ENABLED` | yes (to enable) | `false` | Master kill switch. |
| `MATCHING_REPLICAS` | yes when enabled | `1` | Must equal the StatefulSet's `replicas:`. Re-deploy on scale-up. |
| `POD_ORDINAL` | optional | parsed from `HOSTNAME` | StatefulSet sets `HOSTNAME=ztdx-backend-N`; we strip the trailing index. Set explicitly for non-StatefulSet deployments. |
| `MATCHING_PEER_DNS` | optional | `ztdx-backend-{ord}.ztdx-backend-headless:8080` | DNS template; `{ord}` is replaced per request. |

## Rollout sequence

1. **Ship code with sharding off.** `MATCHING_SHARDING_ENABLED=false`
   on every pod. No behaviour change. Confirm
   `engine_db_integrity_total{kind=missing_from_engine}` rate is
   unchanged.
2. **Convert Deployment → StatefulSet** (manifest below). Pod naming
   becomes `ztdx-backend-0`, `ztdx-backend-1`, … . Add a headless
   Service (`clusterIP: None`) so peer DNS resolves.
3. **Stage flip on one pod.** Set `MATCHING_SHARDING_ENABLED=true` on
   `ztdx-backend-0` only. Confirm:
   - integrity counter for symbols owned by pod 0 drops to zero on pod 0
   - other pods' integrity counter for those symbols stays low (because
     they now forward instead of running locally)
4. **Flip remaining pods.** Same flag, all replicas.
5. **Wire remaining write handlers** (modify, cancel-all,
   batch-cancel, JWT path). Until they're sharded, they bypass routing
   and re-create the original divergence on their slice of traffic.
6. **Wire read handlers.** Orderbook depth + open-orders per-symbol need
   the same forwarding to return correct data — until then, depth
   queries against non-owner pods show empty / stale books.
7. **Replace static hash with K8s Lease-based leader election.** This
   removes the "owner pod down ⇒ symbol unavailable" failure mode by
   reassigning ownership dynamically.

## K8s manifest sketch

```yaml
# headless service — gives each pod a stable DNS name
apiVersion: v1
kind: Service
metadata:
  name: ztdx-backend-headless
  namespace: prod
spec:
  clusterIP: None
  selector:
    app: ztdx-backend
  ports:
    - port: 8080
      targetPort: 8080
      name: http

---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ztdx-backend
  namespace: prod
spec:
  serviceName: ztdx-backend-headless
  replicas: 2
  selector:
    matchLabels:
      app: ztdx-backend
  template:
    metadata:
      labels:
        app: ztdx-backend
    spec:
      containers:
        - name: ztdx-backend
          image: ztdx-backend:<tag>
          ports:
            - containerPort: 8080
          env:
            # HOSTNAME is set to the pod name automatically by K8s
            # (ztdx-backend-0, ztdx-backend-1, …). The sharding
            # config parses the trailing ordinal.
            - name: MATCHING_SHARDING_ENABLED
              value: "true"
            - name: MATCHING_REPLICAS
              value: "2"  # MUST match .spec.replicas above
            - name: MATCHING_PEER_DNS
              value: "ztdx-backend-{ord}.ztdx-backend-headless:8080"
            # … rest of existing env (DB, redis, etc.) …
```

## Engine state recovery on startup

`engine::recover_orders_from_db(pool, Some(&sharding))` rebuilds each
pod's owned slice of the orderbook from the `orders` table. Filters by
`shard.owner_for(symbol) == shard.my_ordinal` per row. Maker / taker
fee rates are resolved from each user's *current* VIP tier (the original
rate isn't persisted on the row); a per-user cache keeps DB cost
O(N_users) not O(N_orders). Triggered orders (STOP_*, TAKE_PROFIT_*)
live in `trigger_orders` and don't need engine recovery.

Wired in `bootstrap.rs` before routes are bound, so a fresh pod or a
shard-ownership flip is rebuilt to a consistent state before traffic is
accepted. With sharding disabled the call is a no-op filter — every pod
recovers everything, matching pre-PoC behaviour.

## Loop / drift handling

Forwarded requests carry `X-ZTDX-Shard-Forwarded: 1`. If a pod
receives a forwarded request but its own `route_for(symbol)` says it's
*not* the owner, that's hash drift (replica scaled mid-flight, or
`MATCHING_REPLICAS` mismatched across pods). The handler logs a warn
and falls through to local execution — better one degraded request than
an infinite forward loop. Watch the warn rate during scale-up; spikes
indicate the new pod hasn't picked up the new `MATCHING_REPLICAS` value
yet.

## What's NOT done

- Read-side routing (orderbook depth, open-orders).
- Multi-owner `batch_cancel` fan-out (degraded, not unsafe — see Status).
- Dynamic leader election. Static hash is fine for pre-launch; replace
  before prod traffic arrives.
