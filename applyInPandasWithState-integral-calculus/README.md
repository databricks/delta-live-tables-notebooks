These are accompanying code samples for the Databricks blog post `Integrating Insights: Integral Calculus for Low-Latency Streaming Analytics`. You should [use Repos to import then run this example into your Databricks workspace](https://docs.databricks.com/en/repos/git-operations-with-repos.html#clone-a-repo-connected-to-a-remote-repo).

# Integrating Insights: Integral Calculus for Low-Latency Streaming Analytics

## Problem Statement: Integrals are needed to calculate valuable metrics

Mechanical and digital engineers both have a reliance on math and statistics to coax insights out of complex, noisy data. Among the most important domains is calculus, which gives us integrals, most commonly described as calculating the area under a curve. This is useful for data engineers as many data that express a rate can be integrated to produce a useful measurement. For example:
* Point-in-time sensor readings, once integrated, can produce time-weighted averages
* The integral of vehicle velocities can be used to calculate distance traveled
* Data volume transferred result from integrating network transfer rates 

Of course, at some point most students learn how to calculate integrals, and the computation itself is simple on batch, static data. However, there are common engineering patterns that require low-latency, incremental computation of integrals to realize business value, such as setting alerts based on equipment performance thresholds, or detecting anomalies in logistics use-cases.

Calculating integrals are just one tool in a toolbelt for modern data engineers working on real-world sensor data. These are just a few examples, and while the techniques described below can be adapted to many data engineering pipelines, the remainder of this blog will focus on calculating streaming integrals on real-world sensor data to derive time-weighted averages.

The supporting code in this repo:
* Describes the data engineering problem to be solved, and a final solution using Delta Live Tables --> `README.md`
* Step-by-step Runbook to build a DLT pipeline and simulate data arrival --> `00_Runbook_for_demo`
* DLT logic as a notebook library performing stateful time-weighted averages --> `01_DLT_StatefulTimeWeightedAverage`

### Calculation Overview

In this set of examples notebooks, we will build a performant data pipeline to calculate **time-weighted averages** on wind turbine sensor data. See the accompanying blog post for more context on why this is useful, but here we'll focus on the technical details. 

The input data is an append-only stream of sensor readings, similar to this example:
| location_id | sensor     | timestamp        | value |
|-------------|------------|------------------|-------|
| L1          | Wind_Speed | 2024-01-01T12:11 | 10    |
| L1          | Wind_Speed | 2024-01-01T12:12 | 20    |
| L1          | Wind_Speed | 2024-01-01T12:14 | 40    |
| L1          | Wind_Speed | 2024-01-01T12:19 | 30    |
| L2          | Wind_Speed | 2024-01-01T12:10 | 15    |
| L2          | Oil_Temp   | 2024-01-01T12:12 | 200   |
| ...         | ...        | ...              | ...   |

Notice that we can have many locations (hundreds or thousands), many sensors (dozens or hundreds), across many incremental time periods that may arrive to our table out of order. 

Our task then is to write a Python fun that calculates an accurate time-weighted average for each sensor, uniquely per location and time interval. In this computation, we want to use the Riemann sum method to calculate the integral of the measurements, then divide by the total duration of each time interval (10 minutes in this example). 

Taking the first 4 rows, we define a set of "keys" for this group: 
* `location_id` = `L1`
* `sensor` = `Wind_Speed`
* `time_interval` = `12:10 -> 12:20`

**Our time-weighted average result should be: 29**

![time weighted average ex](https://github.com/tj-cycyota/delta-live-tables-notebooks/blob/main/applyInPandasWithState-integral-calculus/resources/twa_ex.png?raw=true)

Detailed calculation steps: 
1. `10 x 1 min` --> We use the first reading in our interval as a "synthetic" data point, as we do not know the value the last interval ended as each set of state keys are independent

1. `10 x 1 min` --> The first value 10 lasts for 1 min, from 12:11 to 12:12

1. `20 x 2 min` --> The second value 20 lasts for 2 mins, from 12:12 to 12:14

1. `40 x 5 min` --> The third value 40 lasts for 5 mins, from 12:14 to 12:19

1. `30 x 1 min` -> The fourth value 30 lasts for 1 min until the end of our interval

1. `(10+10+40+200+30)/10 = 29`


## Solution: DLT + ApplyInPandasWithState for streaming integral calculus

Introduced in Apache Spark 3.4.0, [applyInPandasWithState()](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.GroupedData.applyInPandas.html), allows you to efficiently apply a function written in Pandas to grouped data in Spark while maintaining state. It is conceptually similar to [applyInPandas()](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.GroupedData.applyInPandas.html), with the added benefit of being able to use it on a Stateful Streaming pipeline with watermarking in place. See this blog for a basic overview of this concept: [Python Arbitrary Stateful Processing in Structured Streaming](https://www.databricks.com/blog/2022/10/18/python-arbitrary-stateful-processing-structured-streaming.html)

The `applyInPandasWithState()` Spark action will run one instance of our function (`func` in the signature below) on each of the groupings of data for which unique keys exist in that Structured Streaming microbatch. This is a scalable method to perform arbitrary computations in Python (integral calculus via Rieman sums, in our case) on large volumes of streaming data. 

Let’s review the signature of using this function in Spark Structured Streaming :

```
def applyInPandasWithState(
    func:             # Takes in Pandas DF, perform any Python actions needed 
    outputStructType: # Schema for the output DataFrame.
    stateStructType:  # Schema for the state variable  
    outputMode:       # Output mode such as "Update" or "Append"
    timeoutConf:      # Timeout setting for when to trigger group state timeout
) -> DataFrame        # Returns DataFrame
```

The notebook `00_Runbook_for_Demo` will walk you through step-by-step to create a highly performant and easy to understand DLT pipeline focused on these concepts. The results demonstrate that it is possible to efficiently and incrementally compute integral-derived metrics such as time-weighted averages on high-volume data such as wind turbine sensors. Integrals are just one tool in the modern data engineers toolbelt, and with Databricks it is simple to apply them to business-critical applications involving streaming data. 

**Let's get started!** Navigate to the notebook `00_Runbook_for_Demo` in this same repo. 
