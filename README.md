# freshcart-pipelines

End-to-end data engineering assignment: Git/CI, a Snowflake ELT pipeline (FreshCart),
a Mage ML pipeline (Iris classifier), and a RabbitMQ event stream (food-order dispatch),
tied together in one report.

> **Note on Part B/C data:** this repo was built without access to the original
> course notebook/dataset, so the mock orders in `elt/freshcart_elt.ipynb` are a
> self-consistent reconstruction (same bronze → silver → quarantine → gold → SCD2 →
> idempotent-MERGE mechanics the assignment asks about), not the instructor's exact
> dataset. Run it yourself and report the numbers *your* run produces — the report
> template has placeholders for exactly that.

## Repo layout

```
freshcart-pipelines/
├── README.md
├── .gitignore
├── requirements.txt
├── elt/freshcart_elt.ipynb       Part B — Snowflake ELT notebook
├── iris_project/                 Part C — Mage project
├── stream/                       Part D — dispatcher.py, driver.py, consumer_lakehouse.py (bonus)
├── tests/test_prepare_data.py    Part A3 — pytest for the Mage prepare_data block
├── .github/workflows/ci.yml      Part A3 — CI workflow
└── report/REPORT_TEMPLATE.md     Part E — fill in your numbers/screenshots, export to PDF
```

## Part A — Git and CI/CD

```bash
cd freshcart-pipelines
git init
git add README.md .gitignore requirements.txt
git commit -m "chore: scaffold repo structure"
# create an empty repo named freshcart-pipelines on GitHub first, then:
git remote add origin https://github.com/<you>/freshcart-pipelines.git
git branch -M main
git push -u origin main
```

Make **at least 5 separate commits** as you work through B/C/D (not one giant commit) —
e.g. "feat: baseline ELT run", "feat: add currency validation rule", "feat: evaluate_model branch",
"feat: dispatcher/driver scripts", "fix: ACK/NACK handling". Run `git log --oneline` at the end
and paste it into the report.

CI runs automatically on every push via `.github/workflows/ci.yml` (installs
`requirements.txt`, runs `pytest`). To get the required red-then-green pair of screenshots:
edit `iris_project/transformers/prepare_data.py`, change `test_size=0.2` to `test_size=0.3`,
commit/push (red run), then revert and commit/push again (green run).

## Part B — FreshCart ELT on Snowflake

1. Create a Snowflake trial account (snowflake.com/trial).
2. Open `elt/freshcart_elt.ipynb` in Colab or Jupyter, `pip install -r requirements.txt`.
3. Create a `.env` file (gitignored) next to the notebook with:
   ```
   SNOWFLAKE_ACCOUNT=...
   SNOWFLAKE_USER=...
   SNOWFLAKE_PASSWORD=...
   SNOWFLAKE_WAREHOUSE=COMPUTE_WH
   SNOWFLAKE_DATABASE=FRESHCART
   SNOWFLAKE_ROLE=ACCOUNTADMIN
   ```
4. Run the notebook top to bottom. It will create the database/schemas, extract the
   mock "API" data to local files, `PUT` + `COPY INTO` bronze, transform through
   silver/dim/ref/gold, and run `quality_checks()`.
5. Screenshot every output cell listed in B1's table — your own numbers go in the report.

## Part C — Mage ML pipeline

```bash
pip install mage-ai
mage start iris_project
```
Open the Mage UI (locally, or via the Cloudflare tunnel if running in Colab) and you'll
see the `iris_classifier` pipeline: `load_iris → prepare_data → train_model → {save_results, evaluate_model}`,
plus the custom block from C4. Run it with:
```bash
mage run iris_project iris_classifier --test
mage run iris_project iris_classifier --test --runtime-vars '{"max_depth": 3}'
```

## Part D — RabbitMQ stream

```bash
# Colab: !apt-get install -y rabbitmq-server && !service rabbitmq-server start
python stream/dispatcher.py
python stream/driver.py Ravi "bangalore.koramangala.*"
python stream/driver.py Priya "bangalore.indiranagar.*"
python stream/driver.py Control "bangalore.#"
```
Keep any hosted-broker URL (e.g. CloudAMQP) in `.env`, never in Git.

## Part E — Report

Fill in `report/REPORT_TEMPLATE.md` (answers for A–D are either pre-filled where they're
pure logic, or marked `TODO: your run` where they need your own output) and export to PDF
as `RollNo_Name_Pipelines.pdf`.

## Bonus — Stream into the lakehouse

`stream/consumer_lakehouse.py` consumes FreshCart-style order messages for 60 seconds,
writes them as a landing file, then reuses the Part B `load_file()` / transform SQL so
they land in `gold.fct_orders`. Idempotency comes from the same `row_hash` + `MERGE`
pattern as the rest of Part B — run it twice and gold totals shouldn't double.
