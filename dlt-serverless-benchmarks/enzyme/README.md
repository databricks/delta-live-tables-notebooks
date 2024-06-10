# DLT Serverless Enzyme Benchmark Demo

This benchmark compares the performance of DLT serverless vs DLT classic by aggregating a 200B rows Delta table over 2B keys (100:1 ratio) into a materialized view.
We perform 4 updates, with 1000 rows inserted into the source Delta table after each update.

## Steps

1. Generate the 200B rows Delta table, and three 1K rows Delta tables using the [data generator notebook](./00_Data_Generator.py).
2. Create a DLT serverless pipeline using the [notebook](./01_DLT_Aggregation_Notebook.py), and start an update.
3. Create a DLT classic pipeline with the desired pipeline configuration using the [notebook](./01_DLT_Aggregation_Notebook.py), and start an update.
4. After the first update, insert the first 1K rows table into the source table (`INSERT INTO main.data.customers SELECT * FROM main.data.customers_1`).
5. Start the second update in DLT serverless and DLT classic.
6. After the second update, insert the second 1K rows table into the source table (`INSERT INTO main.data.customers SELECT * FROM main.data.customers_2`).
7. Start the third update in DLT serverless and DLT classic.
8. After the third update, insert the second 1K rows table into the source table (`INSERT INTO main.data.customers SELECT * FROM main.data.customers_3`).
9. Start the final update in DLT serverless and DLT classic.
10. Observe the difference in cost and latency for each update.
