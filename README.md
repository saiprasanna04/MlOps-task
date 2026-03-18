# MLOps Batch Job

##  Overview
This project implements a reproducible and observable batch pipeline that:
- Computes rolling mean
- Generates trading signals
- Outputs structured metrics and logs

## Run Locally

```bash
pip install -r requirements.txt

python run.py \
  --input data.csv \
  --config config.yaml \
  --output metrics.json \
  --log-file run.log
