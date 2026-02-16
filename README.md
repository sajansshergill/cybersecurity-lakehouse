# Cybersecurity Lakehouse: Real-Time Threat Analytics (Spark + Scala)

A production-style **cybersecurity data platform** that ingests high-volume security telemetry (auth logs, API access logs, network events), processes it with **Apache Spark (Scala)**, and **distributed compute best practices.**

---

## Why this project

Security teams rely on large-scale telemetry to detect:
- brute-force login attempts
- suspicious IP/device behavior
- impossible travel
- privilege escalation signals
- anomalous usage patterns

This platform turns raw logs into:
- curated, query-friendly datasets
- aggregates threat signal & risk scores
- feature tables for ML workflows

---

## Architecture

**High level flow**

Raw Logs (Auth/API/Net) → Ingest → Bronze (Raw) → Silver (Normalized) → Gold (Signals/Risk)
↓
Feature Engineering (ML-ready)

**Compute**
- Spark batch and/or Structured Streaming jobs (Scala)
- Lakehouse storage format: Parquet (or Delta if enabled)

**Storage layers**
- **Bronze**: raw, append-only, schema-enforced ingestion
- **Silver**: cleaned + normalized + enriched events
- **Gold**: business/secutity-ready aggregates + risk scoring tables

---

## Data layers & tables

### Bronze (Raw)
- `bronze_auth_events`
- `bronze_api_events`
- `bronze_network_events`

**Characteristics**
- partitioned by `event_date`
- minimal transforms
- schema evolution supported (optional)

## Silver (Cleaned/Normalized)
- `silver_security_events` (unified schem across sources)
- `silver_user_dim` (user metadata / role / org)
- `silver_device_dim` (device fingerprint)
- `silver_ip_dim` (ip -> geo / ASN enrichment)

**Typical transfroms**
- deduplication
- timestamp normalization
- IP/device formatting
- enrichment joins
- late-arriving handling (if streaming)

### Gold (Threat Analytics)
- `gold_user_daily_risk`
- `gold_ip_reputation_signals`
- `gold_failed_login_windows`
- `gold_impossible_travel_flags`
- `gold_privilege_change_signals`

**Outputs**
- risk scores per user/day
- suspicious entities (IPs/devices)
- rolling-window alerts

### Features Store (ML-ready)
- `features_user_behnavior_7d`
- `features_session_behavior`
- `features_ip_behavior`

---

### Data model (example)

**Unified Silver schema** (`silver_security_events`)
| Column | Type | Description |
|---|---|---|
| event_ts | timestamp | Event timestamp (UTC) |
| event_date | date | Partition key |
| event_type | string | login/api/network/etc |
| user_id | string | User identifier |
| device_id | string | Device fingerprint/ID |
| ip | string | Source IP |
| geo_country | string | Enriched country |
| geo_city | string | Enriched city |
| action | string | login_success/login_fail/api_call/... |
| resource | string | endpoint/service/resource |
| status | string/int | status code or result |
| metadata | map<string,string> | flexible payload |

---

## Tech stack

- **Scala** (JVM)
- **Apache Spark** (SQL + Structured Streaming optional)
- Storage: **Parquet** (or **Delta Lake** if enabled)
- Optional:
  - Kafka (stremaing ingestion)
  - Great Expectations / Deequ (data quality)
  - ML: Spark MLlib / XGBoost (external) for anomaly detection
 
---

## Repo structure

<img width="586" height="1594" alt="image" src="https://github.com/user-attachments/assets/bd57ae13-07c6-4e04-932d-436f6507c896" />

### Getting started

### Prerequisites
- Java 11+ (or compatible with your Spark build)
- Scala 2.12/2.13 (match Spark version)
- Spark 3.x
- sbt

Check:
```bash
java --version
sbt --version
spark-submit --version
```

### Configuration
Edit conf/app.conf
- input paths (raw / streaming)
- output base path for lakehouse tables
- partition columns
- opetional Delta enablement

Example(conceptual):
- storage.base_path = "warehouse/"
- storage.format = "parquet" (or delta)
- spark.shuffle.partitions = 200

### Run locally (batch)
1) Generate synthetic security events (optional)
python3 scripts/generate_synthetic_data.py \
  --out data/synthetic \
  --rows 5000000

2) Ingest -> Bronze
spark-submit \
  --class jobs.IngestBronzeJob \
  target/scala-2.12/cyber-lakehouse-assembly.jar \
  --input data/synthetic \
  --output warehouse

3) Bronze -> Silver
spark-submit \
  --class jobs.SilverTransformJob \
  target/scala-2.12/cyber-lakehouse-assembly.jar \
  --input warehouse \
  --output warehouse

4) Silver -> Gold (signals + risk)
spark-submit \
  --class jobs.GoldSignalsJob \
  target/scala-2.12/cyber-lakehouse-assembly.jar \
  --input warehouse \
  --output warehouse

5) Build feature tables
spark-submit \
  --class jobs.FeatureEngineeringJob \
  target/scala-2.12/cyber-lakehouse-assembly.jar \
  --input warehouse \
  --output warehouse

### Run locally (streaming) — optional
If using Kafka:
-  kafka.bootstrap.servers
- configure topics per log type

Then run a streaming version of ingest job:
spark-submit \
  --class jobs.IngestBronzeJob \
  target/scala-2.12/cyber-lakehouse-assembly.jar \
  --mode streaming \
  --checkpoint warehouse/_checkpoints/bronze \
  --output warehouse

### Data quality & validation
Quality checks are applied at Silver and Gold:
- schema validation (required columns present)
- null checks (user_id, event_ts, ip)
- referential checks (user_id exists in user_dim)
- range check (status codes, timestamps)
- duplicate detection (event_id / natural key)

Run validations:
bash scripts/validate_tables.sh warehouse

### Governance-minded design (what we enforce)
- **PII minimizations**: avoid strong raw identifiers when possible, hash/salt user IDs in curated layers (optional)
- **Access patterns**: separate raw (Bronze) from curated (Silver/Gold)
- **Data contracts**: schema enforcement + versioning
- **Auditability**: append-only raw logs, deterministic transforms

### Performance considerations
- Partition by event_date (+ optionally event_type)
- ASvoid skewed joins by:
  - saliting keys for heavy IP aggregations
  - broadcast join for small dims (user_dim, geo_dim)
- Use incremental processing windows for Gold
- Tune:
  - spark.sql.shuffle.partitions
  - adaptive query execution (AQE) where available
 
### TESTING
Unit tests:
- schema parsing
- transform correctness (sample DF)
- quality checks fail/pass cases

Run:
sbt test

### EXAMPLE queries (Gold)
Top risk users (last 7 days):
SELECT user_id, SUM(risk_score) AS total_risk
FROM gold_user_daily_risk
WHERE event_date >= date_sub(current_date(), 7)
GROUP BY user_id
ORDER BY total_risk DESC
LIMIT 50;

Most suspicipus IPs:
SELECT ip, COUNT(*) AS flags
FROM gold_ip_reputation_signals
WHERE is_suspicious = true
GROUP BY ip
ORDER BY flags DESC
LIMIT 50;

### Roadmap:
- Delta Lake support + time travel
- Streaming Gold updates with watermarking
- Knowledge graph: user ↔ device ↔ IP relationships
- ML anomaly models + evaluation harness
- CI/CD for Spark jobs + data contract checks
- Dataset scale tests (10M-100M+ synthetic events)
