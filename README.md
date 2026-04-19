# pipewatch

> Lightweight CLI monitor for tracking ETL pipeline health and failures in real time.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourname/pipewatch.git && cd pipewatch && pip install .
```

---

## Usage

Start monitoring a pipeline by pointing pipewatch at your log source or pipeline config:

```bash
pipewatch monitor --config pipeline.yaml
```

Watch a specific log file for failures in real time:

```bash
pipewatch watch --log /var/log/etl/pipeline.log --alert-on failure,timeout
```

Check the current health status of all registered pipelines:

```bash
pipewatch status
```

Filter status output to show only failing or warning pipelines:

```bash
pipewatch status --filter unhealthy
```

Example output:

```
PIPELINE          STATUS     LAST RUN         FAILURES
─────────────────────────────────────────────────────
ingest_orders     ✅ OK       2 minutes ago    0
transform_users   ❌ FAILED   10 minutes ago   3
load_warehouse    ⚠️  WARN    1 hour ago       1
```

---

## Configuration

pipewatch reads from a `pipeline.yaml` file by default:

```yaml
pipelines:
  - name: ingest_orders
    log: /var/log/etl/ingest.log
  - name: transform_users
    log: /var/log/etl/transform.log
alert_on: [failure, timeout, stall]
notify:
  slack_webhook: https://hooks.slack.com/services/your/webhook/url
```

Supported `alert_on` values:

| Value     | Description                                      |
|-----------|--------------------------------------------------|
| `failure` | Triggered when a pipeline exits with an error    |
| `timeout` | Triggered when a pipeline exceeds its time limit |
| `stall`   | Triggered when no log activity is detected       |

---

## License

MIT © 2024 [yourname](https://github.com/yourname)
