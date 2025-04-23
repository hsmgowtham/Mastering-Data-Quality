# Data Quality Experiments with PySpark

This repository contains notebooks and examples focused on ensuring data quality across different stages of a data pipeline, especially when data lands in the final layer like a data warehouse for analytics and reporting.

We'll walk through the core components of high-quality data workflows and provide hands-on PySpark code to explore, validate, and monitor your data.

---

## End-to-End Pipeline Flow

```
Ingestion → Data Profiling → Data Transformations → Data Quality Checks → Data Validation → Aggregation & Storage → Data Observability
```

Each step plays a critical role in ensuring that downstream data is accurate, complete, consistent, and trustworthy.

---

## 1. Data Profiling

Get to know your raw data before building anything:

- Structure: Schema, data types
- Content: Value ranges, distributions
- Quality: Nulls, duplicates, anomalies
- Relationships: Keys, joins, referential patterns

```python
# Schema & basic stats
df.printSchema()
df.describe().show()

# Null counts
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# Cardinality & frequency
df.select("channel").distinct().show()
df.groupBy("channel").count().show()

# All column cardinality
from pyspark.sql.functions import countDistinct
df.agg(*[countDistinct(c).alias(c) for c in df.columns]).show()
```

---

## 2. Data Quality Checks (Before Transformation)

Detect and isolate invalid data based on business rules.

Examples:
- Salary must be positive
- Department must be one of a defined set

```python
invalid_salary = df.filter(col("salary") < 0)
invalid_dept = df.filter(~col("department").isin("HR", "Finance", "Tech", "Marketing"))

invalid_salary.show()
invalid_dept.show()
```

---

## 3. Data Transformations

Prepare data for downstream use by cleaning, reshaping, or enriching it.

Common steps:
- Standardize formats
- Remove duplicates
- Handle nulls
- Create derived columns

```python
from pyspark.sql.functions import to_date, lit

df = df.withColumn("pay_date", to_date(col("pay_date"), "MM/dd/yyyy"))
df = df.dropDuplicates()
df = df.fillna({"bonus": 0, "department": "Unknown"})
df = df.withColumn("net_salary", col("salary") + col("bonus") - col("tax"))
```

---

## 4. Data Validation (Post Transformation)

Think of this as unit testing for your transformed data.

- Check outcome logic
- Validate schema
- Ensure referential integrity
- Confirm record counts

```python
# Check derived values
df.withColumn("expected_net", col("salary") + col("bonus") - col("tax")) \
  .filter(col("net_salary") != col("expected_net")).show()

# Validate column presence
expected_columns = {"employee_id", "salary", "net_salary"}
assert expected_columns.issubset(set(df.columns))
```

---

## 5. Data Observability

Monitor your data pipeline in motion and after it lands in the warehouse.

### In Pipeline (Streaming / ETL)

- Data freshness
- Schema drift
- Volume anomalies
- Failed jobs

### Post-Warehouse

- Metric drift
- Lineage tracking
- Data quality metrics
- Unusual access patterns

```python
from datetime import datetime

metrics = {
    "record_count": df.count(),
    "null_bonus": df.filter(col("bonus").isNull()).count(),
    "run_time": str(datetime.utcnow())
}
print(metrics)
```

---

## Real-World Monitoring Examples

Imagine you're loading sales data into Snowflake every 15 minutes:

**During Pipeline:**
- Was there a schema change in the API?
- Did one batch silently fail?
- Did a job take longer than usual?

**After Warehouse:**
- Are rows missing product IDs?
- Did today’s sales drop to zero?
- Is revenue abnormally low in one country?
- Is someone querying sensitive data at odd hours?

---

## Tools That Help

| Feature                  | Tools (Open Source + Commercial)                                  |
|--------------------------|--------------------------------------------------------------------|
| Data Quality Monitoring  | Great Expectations, Soda, Deequ                                   |
| Lineage Tracking         | OpenMetadata, Apache Atlas                                        |
| Anomaly Detection        | Monte Carlo, Databand, Soda                                       |
| Dashboards & Alerting    | Prometheus + Grafana, Airflow UI, Looker                          |
| Warehouse Observability  | dbt Cloud, Snowflake’s QUERY_HISTORY, AWS CloudWatch              |

---

## Work In Progress

This repo will grow as more use cases and datasets are added. Stay tuned for more practical notebooks covering diverse domains and real-world data challenges.

