# DLT Serverless Ingestion Benchmark Demo

This benchmark compares the performance of loading 100K JSON files into a Delta table using DLT serverless and DLT classic.

## Steps

1. Generate 100K cloud files using the [data generator notebook](./00_Data_Generator.py).
2. Create a DLT serverless pipeline using the [notebook](./01_DLT_Ingestion_Notebook.py), and start an update.
3. Create a DLT classic pipeline using the [notebook](./01_DLT_Ingestion_Notebook.py) and the desired pipeline configuration (e.g. Enhanced Autoscaling with 1 to 64 workers and photon disabled), and start an update.
4. Observe the difference in cost and latency for each update.
