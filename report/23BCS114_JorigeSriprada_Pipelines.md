# 23BCS114_JorigeSriprada_Pipelines

**Roll No:** 23BCS114 · **Name:** Jorige Sriprada

GitHub repo: https://github.com/Sriprada30/freshcart-pipelines

---

## Part A — Git and CI/CD

### A1. Repository (5 marks)

`git log --oneline` (9 commits, spread across the assignment, newest first):
```
95221b3 fix: revert prepare_data test_size to 0.2 (CI back to green, A3)
1b7393c test: bump prepare_data test_size to 0.3 to demonstrate a failing CI gate (A3)
abdeffb feat: execute FreshCart ELT notebook against live Snowflake, fill report with real B1-B5 results
ec82635 docs: add report template with live D1 routing proof and D2/D3 stream evidence
6836b9a feat: add RabbitMQ dispatcher, driver (ACK/NACK), and lakehouse bonus consumer
84cbbc9 feat: add FreshCart ELT pipeline for Snowflake (bronze/silver/dim/gold, SCD2, idempotent merge)
87b5a13 test: add pytest for prepare_data's 80/20 split (Part A3)
6e2d403 feat: add Mage iris_classifier pipeline (load, prepare, train gate, evaluate, save)
eb955e1 chore: scaffold repo structure, CI workflow, and requirements
```

### A2. Where does each command act? (4 marks)

| Command | Where the change happens | What exactly happens? |
|---|---|---|
| `git init` | Local repository | Creates a new, empty `.git` folder in the current directory — this is what turns an ordinary folder into a Git repository with its own history. |
| `git add train.py` | Staging area | Copies the current content of `train.py` from the working folder into the index (staging area), marking it as "ready to be committed" — the file on disk is untouched. |
| `git commit -m "msg"` | Local repository | Takes a permanent snapshot of everything currently in the staging area and adds it as a new commit to the local repository's history, on your current branch. |
| `git remote add origin URL` | Local repository (config only) | Registers a remote named `origin` pointing at `URL` in your local Git config. No files or commits move — it's purely bookkeeping. |
| `git push -u origin main` | Remote repository (GitHub) | Uploads the commits on your local `main` branch to `origin`, creates/updates `main` there, and sets `origin/main` as the upstream tracking branch for future `git push`/`git pull`. |
| `git pull origin main` | Working folder + local repository | Fetches new commits from `origin/main` and merges them into your current local branch, updating both the local history and the files in your working folder. |

**After running `git commit`, can your teammate see your code on GitHub?**
No. `git commit` only writes the snapshot to your *local* repository — it never
talks to the network. Your teammate can only see the change after you run
`git push`, which is the step that actually uploads commits to GitHub.

### A3. CI workflow (6 marks)

`tests/test_prepare_data.py` and `.github/workflows/ci.yml` are already in the repo.
Both the red and green runs below are real GitHub Actions runs on this repo —
`TODO`: open each link and take your own screenshot of the Actions tab for the
appendix (I can't screenshot a browser from here).

- **Green run** (`TEST_SIZE = 0.2`): https://github.com/Sriprada30/freshcart-pipelines/actions/runs/43928764046 — passed.
- **Red run** (`TEST_SIZE = 0.3`, commit `1b7393c`): https://github.com/Sriprada30/freshcart-pipelines/actions/runs/43928833204 — failed, because `test_split_is_80_20` asserts exactly 120 train / 30 test rows, which only holds for `test_size=0.2` on 150 rows.
- **Green again** (reverted, commit `95221b3`): https://github.com/Sriprada30/freshcart-pipelines/actions/runs/43929049491 — passed.

---

## Part B — FreshCart ELT on Snowflake

> Executed for real against a live Snowflake trial account
> (`gj05109.ap-southeast-7.aws`, role `ACCOUNTADMIN`) via
> `elt/freshcart_elt.ipynb` — every number below is actual printed output,
> not a placeholder. The notebook was run twice while fixing two client-side
> bugs (Snowflake returns unquoted columns in UPPERCASE, and the `REASONS`
> array comes back from `fetch_pandas_all()` as a JSON *string*, not a
> native list) — that's why bronze shows 16 rows below instead of 8: it's
> two real runs of the same 8-row batch, which is itself live proof of B4's
> "bronze grows every run" point. `TODO`: swap in your own screenshots of
> these same notebook cells.

### B1. Baseline (5 marks)

| Check | This run's actual output |
|---|---|
| Raw order rows in bronze | 16 (2 runs × 8: 6 web + 2 mobile each) |
| Rows that pass cleaning (silver.orders_checked) | 10 (2 runs × 5) |
| Rows in quarantine | 6 (2 runs × 3: ORD-1003's web copy, ORD-1004, ORD-1006) |
| Orders after deduplication | 4 (ORD-1001, ORD-1002, ORD-1003, ORD-1005) |
| Final gold line | 4 orders, revenue ₹9,000.00, 139 loyalty points |
| Share of rows rejected | 37.5% (6/16) |

Note for your own single run: expect 8 raw / 5 clean / 3 quarantined / 4
deduped / 37.5% rejected (3/8) — the scaffold's mock data has ORD-1003's
*web* copy missing its amount too, not just ORD-1004 and ORD-1006, so 3 rows
are quarantined per run, not 2.

`TODO`: screenshots of the B1 notebook cell outputs.

### B2. Trace one record by hand (6 marks)

**ORD-1003:**
- The web row has `order_id/customer_id/amount/currency/order_date/updated_at`
  with `amount = NULL` (price wasn't known yet at checkout time on web). The
  mobile row uses different field names for the same concepts
  (`cust_id`, `amt`, `order_ts`) because the mobile app's event schema was
  designed independently of the web team's — this is exactly why
  `SQL_10_ORDERS_CLEAN` has two `SELECT` branches in a `UNION ALL`, one per
  source, each aliasing its own field names onto the same common columns.
- Both rows get the same `order_id` after that mapping, so
  `QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1`
  in `SQL_20_ORDERS_DEDUP` keeps whichever row has the **latest** `updated_at`
  — in this dataset that's the mobile row, since it arrived later and has a
  non-null amount.
- **Actual arithmetic (real run):** ORD-1003's gold row has `amount_inr = 2100.00`,
  `tier_used = Silver` (Rahul Verma, flat Silver tier the whole time), so
  `loyalty_points = FLOOR(2100 / 100 * 1.5) = FLOOR(31.5) = 31` — matches the
  gold table exactly.

**ORD-1005 vs ORD-1001 (tier multiplier):** both orders belong to Priya
(CUST-01), but `gold.fct_orders` joins `silver.orders_dedup` to
`dim.customers_scd2` **on `order_date >= effective_from AND order_date < effective_to`**
— a point-in-time join, not "whatever the customer's tier is today." Priya's
`dim.customers_scd2` history is Silver from `2026-01-01` to `2026-07-05`, then
Gold from `2026-07-05` onward. ORD-1001's `order_date = 2026-07-01` falls in
the Silver window → `amount_inr=1500.00`, `tier_used=Silver`,
`loyalty_points = FLOOR(1500/100*1.5) = 22`. ORD-1005's `order_date = 2026-07-08`
falls in the Gold window → `amount_inr=3200.00`, `tier_used=Gold`,
`loyalty_points = FLOOR(3200/100*2.0) = 64`. Both numbers are real output from
`gold.fct_orders` in this run.

### B3. Add a validation rule (6 marks)

The one line added inside `ARRAY_CONSTRUCT_COMPACT(...)`:
```sql
IFF(currency NOT IN ('INR', 'USD'), 'unsupported_currency', NULL)
```
**Actual output (real run):**
- Before: `reasons = ["negative_amount"]`
- After: `reasons = ["negative_amount", "unsupported_currency"]`
- Gold total after the change: still 4 rows, ₹9,000.00 revenue — unchanged.

Explanation: the gold total does **not** change, because ORD-1006 was
already excluded from `silver.orders_dedup` (and therefore from gold) by its
pre-existing reason — adding a second reason to the same row's array doesn't
remove or add a row anywhere downstream. It also does **not** create a second
quarantine row: `ARRAY_CONSTRUCT_COMPACT` builds one array per row, so a
second failing condition just appends a second element to the *same* row's
`reasons` array (`WHERE ARRAY_SIZE(reasons) = 0` in the dedup step is the line
that decides pass/quarantine, and it only looks at whether the array is empty,
not how many reasons are in it).

### B4. Change history and idempotency (7 marks)

| Quantity | Prediction (before running) | Actual after run (real) |
|---|---|---|
| Rows for CUST-02 in dim.customers_scd2 | 2 (Bronze row gets an `effective_to` capped at the new event's date, plus a new Gold row) | 2 — Bronze `2026-01-01→2026-08-15`, Gold `2026-08-15→9999-12-31` |
| Tier used for ORD-1002 | Bronze, unchanged — its `order_date` (2026-07-02) is *before* the new tier's `effective_from` (2026-08-15) | Bronze (unchanged) |
| Loyalty points for ORD-1002 | 22, unchanged | 22 (unchanged) |
| Total loyalty points in gold | 139, unchanged | 139 (unchanged) |
| Number of gold rows whose published_at changed | 0, if only *changed* rows get touched | **4 (all of them)** — see note below |

**Note on `published_at`:** the real run shows *every* gold row's
`published_at` advancing on each MERGE (e.g. ORD-1002's went from
`10:22:23.967` to `10:22:26.344`), not just rows whose business data
changed. That's because this `MERGE`'s `WHEN MATCHED` clause always refreshes
`published_at` whenever `row_hash` matches — i.e. `published_at` means "last
time this exact row was reconfirmed by a run," not "last time this row's
value changed." The idempotency guarantee that actually matters (row counts
never grow, nothing duplicates) still held: gold stayed at exactly 4 rows
across both runs.

`row_hash` and `MERGE` idempotency, in 4–6 sentences: `row_hash` is computed
from every business column that matters for an order's gold row (order id,
customer, amount, and the tier used) — so two runs over unchanged source data
always produce the exact same hash for the exact same order. The gold `MERGE`
matches on `row_hash`, so when nothing changed, `WHEN MATCHED` fires and only
refreshes `published_at`; no new row is ever inserted for an order that's
already there. An order whose underlying facts genuinely change (e.g. a tier
change that moves which SCD2 row it joins to) gets a *different* `row_hash`,
which is what would actually insert a new gold row rather than just
touching a timestamp on an existing one — that's the real idempotency
guarantee (row count never grows for unchanged data), not that `published_at`
itself is only touched when something changed. Bronze still grows by 8 rows every run because the raw
landing layer is deliberately append-only with no dedup at all — the
load step (`COPY INTO ... FORCE=TRUE`) re-ingests the same source files every
time on purpose, so idempotency has to live downstream (at the gold MERGE),
not at the raw layer.

### B5. Add a data-quality check (6 marks)

Sixth check added: orders in `silver.orders_dedup` whose `order_date` is in
the future (see `quality_checks(..., include_future_date_check=True)` in
`freshcart_lib.py`).

**Actual output (real run):**
```
quality_checks()           -> {'duplicate_orders_in_gold': 0, 'negative_loyalty_points': 0,
                                'orders_missing_tier': 0, 'clean_rows_with_null_amount': 0,
                                'quarantined_rows_with_no_real_reason': 0, 'future_dated_orders': 0}

after injecting a mock order dated 30 days in the future (ORD-9999):
PROBLEM ->                    {..., 'future_dated_orders': 1}

after deleting ORD-9999:
back to clean ->              {..., 'future_dated_orders': 0}
```

**ELT vs ETL for FreshCart (3–4 sentences):** ELT is the better fit because
Snowflake's compute is elastic and cheap to scale up only when a transform
runs, so it's more efficient to load raw JSON as-is (schema-on-read) and let
SQL do every bronze→silver→gold step inside the warehouse — and keeping the
raw payload around means a wrong transform rule can just be re-run, not
re-extracted. ETL would still win in a situation where the raw data itself
can't be allowed to land anywhere shared unmasked — e.g. a strict
compliance/PII requirement to strip or anonymize sensitive fields *before*
the data is ever stored, which forces the transform step ahead of load.

---

## Part C — ML pipeline with Mage

### C1. Pipeline graph (4 marks)

Blocks: `load_iris` (data_loader) → `check_quality` (transformer, C4's custom
block) → `prepare_data` (transformer) → `train_model` (transformer) →
`{save_results (data_exporter), evaluate_model (transformer)}`.

Tests attached: `load_iris` → shape/column test; `check_quality` → no missing
values / exactly 3 classes; `prepare_data` → 120/30 split; `train_model` →
`test_accuracy >= 0.80` (the gate); `evaluate_model` → confusion matrix shape;
`save_results` → file-written check.

`TODO`: your hand-drawn diagram. Real `mage run iris_project iris_classifier --test` output:
```
1/1 tests passed.   (load_iris)
2/2 tests passed.   (check_quality)
2/2 tests passed.   (prepare_data)
1/1 tests passed.   (train_model)
Confusion matrix:
 [[10  0  0]
 [ 0  9  1]
 [ 0  1  9]]
1/1 tests passed.   (save_results)
1/1 tests passed.   (evaluate_model)
Pipeline run completed.
```

### C2. Tests as a gate (6 marks)

Verified for real via `mage run iris_project iris_classifier --test --runtime-vars '{"max_depth": N}'`
for every N (not just by calling the block functions directly in Python):

| max_depth | Test accuracy | test_accuracy passed? | iris_predictions.csv created? |
|---|---|---|---|
| 1 | 0.667 | No (below 0.80 gate — pipeline raised `Exception: Failed to pass tests for block train_model`) | No |
| 2 | 0.933 | Yes | Yes |
| 3 | 0.967 | Yes | Yes |
| 4 | 0.933 | Yes | Yes |
| 5 | 0.933 | Yes | Yes |

`TODO`: attach your own screenshots of these runs — the numbers above are
real output from this exact repo's code, not placeholders.

**Two real bugs found and fixed while verifying this against the actual Mage
CLI** (worth mentioning in your E reflection if you want a true "something
broke" story that isn't fabricated):
1. The block files originally used `try: from mage_ai...import X except ImportError: X = identity` to stay importable outside Mage. That's wrong — Mage injects its *own* tracking decorators into each block's `globals()` at run time, and its real convention is `if "X" not in globals(): from mage_ai... import X`. The try/except version always re-imported the plain package decorator and silently shadowed Mage's tracking one, so Mage reported "block has no decorated functions." Fixed by switching every block to the `globals()`-check form.
2. `evaluate_model`'s test originally asserted on the live return value (`numpy.ndarray.shape`, a pandas `DataFrame.index`), but Mage stores each block's output to disk and re-reads it before running the block's test — and a `None` value or certain numpy/pandas objects don't round-trip cleanly through mage-ai 0.9.79's variable serializer. Fixed by having `evaluate_model` return plain `list`/`dict` structures instead.

**Why a failing `@test` stopping `save_results` is better than save-then-check:**
Mage runs tests as a hard gate between blocks — if `train_model`'s
`test_accuracy_above_threshold` test raises `AssertionError`, the pipeline
halts right there and `save_results` never executes, so a bad model's
predictions are never written anywhere a downstream consumer could read.
Saving the file first and checking afterwards means a bad result is visible
(and possibly already consumed) for however long it takes to notice and
delete it — the gate approach makes "bad data never gets published" a
guarantee instead of a race against whoever reads the file next.

### C3. Evaluation branch (5 marks)

`iris_project/pipelines/iris_classifier/metadata.yaml` already wires
`train_model`'s `downstream_blocks` to both `save_results` and
`evaluate_model`. `evaluate_model` (verified by direct execution above)
produces a 3×3 confusion matrix and a precision/recall/f1 table per class.

Real metrics table (default run): precision/recall/f1 all 1.00 for class 0,
0.90 for classes 1 and 2; overall accuracy 0.933. `TODO`: screenshot of the
graph (through the Cloudflare tunnel) showing the branch.

### C4. Your own block (5 marks)

Added `transformers/check_quality.py` between `load_iris` and `prepare_data`:
asserts zero missing values and exactly 3 classes in the raw Iris data, with
its own `@test`. `TODO`: confirm it's committed and CI still passes.

---

## Part D — Food-order stream with RabbitMQ

### D1. Routing on paper (6 marks)

| # | Routing key | Receiving queues |
|---|---|---|
| 1 | bangalore.koramangala.food | Ravi, Meena, Control |
| 2 | bangalore.indiranagar.ride | Priya, Arjun, Control |
| 3 | bangalore.whitefield.ride | Arjun, Control |
| 4 | chennai.tnagar.food | Karthik |
| 5 | chennai.whitefield.ride | dropped |
| 6 | bangalore.koramangala | Control |

**Why #6 behaves differently:** `bangalore.koramangala` has only **two**
words. Every pattern except Control's requires exactly **three** words — a
literal second word (Priya, Karthik) or a `*` (Ravi, Arjun, Meena) each
occupy one fixed segment, so with only two words present there's no third
segment for those patterns to even test, and they simply can't match
regardless of its content. Control's pattern, `bangalore.#`, is different:
`#` matches **zero or more** trailing words, so it happily matches
`bangalore` followed by just one word (`koramangala`) — or by nothing at all,
or by three more words. That's the only pattern flexible enough to match a
key of this length.

### D2. Build it (8 marks)

Run live against a local RabbitMQ broker with the dispatcher + 4 drivers
(Ravi, Priya, Control, Meena) for ~3 minutes. Full logs are in
`report/evidence/{dispatcher,ravi,priya,control,meena}.log`. Sample proving
D1's prediction for `bangalore.indiranagar.ride` (Priya, Arjun/not run here,
Control):
```
[dispatcher] #5 -> bangalore.indiranagar.ride: {'id': '1f736e...', 'city': 'bangalore', 'zone': 'indiranagar', 'type': 'ride', ...}
[Priya]   received on 'bangalore.indiranagar.ride': {...'id': '1aff670d...'}
[Control] received on 'bangalore.indiranagar.ride': {...'id': '1aff670d...'}
```
(Ravi's and Meena's logs show zero messages with that exact key, as predicted
— their patterns don't match `indiranagar`/`ride`.)

### D3. ACK and NACK (6 marks)

Implemented in `driver.py`: any message missing `zone` is rejected with
`basic_nack(requeue=False)`; everything else is `basic_ack`'d.
`dispatcher.py` drops the `zone` field from every 10th message.

**Actual output (real run, `report/evidence/priya.log` and `control.log`):**
```
[Priya]   REJECTED (missing zone): b'{"id": "b474faa4-...", "city": "bangalore", "type": "ride", "ts": 1794851924.7}'
[Control] REJECTED (missing zone): b'{"id": "bb6644ed-...", "city": "bangalore", "type": "food", "ts": 1794851884.6}'
```
followed immediately by normal `received on '...'` lines for every well-formed
message after it — confirming bad messages are discarded (NACK'd, not
requeued) while good ones keep flowing.

**If the driver crashes before ACKing:** RabbitMQ notices the consumer's
connection/channel is gone and automatically requeues that unacknowledged
message, redelivering it (marked `redelivered`) to another consumer on the
queue — or back to the same queue if the driver reconnects — so the message
is not lost, just delayed.

**With `requeue=True` for a message that is *always* bad:** it creates an
infinite redelivery loop — the driver NACKs it with `requeue=True`, RabbitMQ
immediately puts it back at the queue, the driver gets it again, NACKs it
again, forever. The message never leaves the queue and keeps consuming
consumer/broker resources; that's exactly why permanently-bad messages should
be NACK'd with `requeue=False` (optionally into a dead-letter exchange) rather
than requeued.

---

## Part E — Integration

### Diagram

`TODO`: one diagram — code in Git → CI testing it → ELT pipeline producing
gold data → ML pipeline training on data → stream feeding events (and, if you
did the bonus, the stream feeding into the same gold table).

### Reflection (half page)

While verifying Part C against the real Mage CLI (not just by calling the
block functions in Python), `mage run` kept failing with "Block load_iris
does not have any decorated functions." I'd written each block's decorator
import as `try: from mage_ai...import X except ImportError: X = identity`,
assuming that was just a fallback for when mage-ai isn't installed. Since
mage-ai *was* installed, the import always succeeded — so I couldn't
understand why Mage said the function wasn't decorated. Reading Mage's own
source (`mage_ai/data_preparation/models/block/__init__.py`) showed the real
mechanism: Mage injects its own tracking version of `data_loader` /
`transformer` / etc. directly into each block's `globals()` before running
it, and its own block templates always check `if "X" not in globals()`
before importing — my unconditional import was silently overwriting Mage's
injected tracking decorator with the inert package-level one, so Mage never
saw a "real" decorated function. The fix was switching every block to that
exact `globals()`-check pattern. A second, smaller version of the same class
of bug showed up in `evaluate_model`: its `@test` passed when I called the
function directly, but failed under Mage because Mage serializes a block's
output to disk and re-reads it before running the block's test, and its
serializer in this version can't reconstruct a `None` value or round-trip
certain numpy/pandas objects — so I switched that block to return plain
`list`/`dict` structures instead.

---

## Bonus — Stream into the lakehouse (+10 marks)

`stream/consumer_lakehouse.py` collects FreshCart-style messages for 60s,
lands them, then reuses `freshcart_lib.run_transform()` so they reach
`gold.fct_orders`. `TODO`: run it twice, paste both gold totals, and confirm
they didn't double (same row_hash + MERGE idempotency as Part B).
