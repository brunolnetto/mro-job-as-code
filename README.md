# MRO Job as Code

This bundle defines the complete Databricks MRO DAG.

## Validate

```bash
databricks bundle validate -t dev
```

## Deploy

```bash
databricks bundle deploy -t dev
```

## Run

```bash
databricks bundle run -t dev mro_data_pipeline
```

Override job parameters:

```bash
databricks bundle run -t dev \
  --params ticks_per_run=24,step_minutes=60 \
  mro_data_pipeline
```

Bootstrap only:

```bash
databricks bundle run -t dev \
  --params bootstrap_only=true \
  mro_data_pipeline
```

The schedule is defined but PAUSED by default.

Notebook tasks omit explicit cluster configuration so Databricks can use
serverless compute where supported by the workspace.
