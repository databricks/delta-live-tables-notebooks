<h1>Delta Live Tables Example Notebooks</h1>

<p align="center">
  <img src="https://delta.io/wp-content/uploads/2019/04/delta-lake-logo-tm.png" width="140"/><br>
  <strong>Delta Live Tables</strong> is a new framework designed to enable customers to successfully declaratively define, deploy, test & upgrade data pipelines and eliminate operational burdens associated with the management of such pipelines.
</p>
<p align="center">
  This repo contains Delta Live Table examples designed to get customers started with
  building, deploying and running pipelines.
</p>

## Getting Started

* Connect your Databricks workspace using the <img src="https://databricks.com/wp-content/uploads/2021/05/repos.png" width="140" style=" vertical-align:middle"/> feature to [this repo](https://github.com/databricks/delta-live-tables-notebooks)

* Choose one of the examples and create your pipeline!

## Examples
### Loan Risk Analysis

This Loan Risk Analysis pipeline sample is based on the [Loan Risk Analysis with XGBoost and Databricks Runtime for Machine Learning](https://databricks.com/blog/2018/08/09/loan-risk-analysis-with-xgboost-and-databricks-runtime-for-machine-learning.html).  This pipelne is in two parts:
1. The [Loan Risk Pipeline]() Delta Live Tables notebook is a sample Delta medallion architecture ala bronze (BZ), silver (Ag), and gold (Au) data quality layers.  It processes the loan risk data through multiple transformations and two gold tables.  The pipeline also runs a pyspark logistic regression ML pipeline to predict bad loans based on this data.

 <img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/Loan%20Risk%20Pipeline.png" width=500>

1. The [Loan Risk Analysis]() notebook produces a number of graphs as well as executes a ML pipeline for better loan risk predictions.


### NYC Taxi Dataset

The NYC Taxi demo is available in `scala` and `python` to process the [NYC Taxi dataset](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) specific to the year 2015.  The Python code generates the following pipeline in the form of a Delta medallion architecture ala bronze (BZ), silver (Ag), and gold (Au) data quality layers. 

<img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/2015%20NYCTaxi%20Dashboard%20Pipeline%20(BZ-AG-AU).png" width=600>


You can modify the pipeline to process all of the data but for this demo, we restricted it to 2015 because it was the most recent year of data that contained  pickup and dropoff longitude and latitude points.  This data is used in the following **2015 NYC Taxi Dashboard with Expectations**.

<img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/2015-nyctaxi-dashboard-with-expectations.gif" width=700/>

To run this demo, please run the following steps:
1. Run the [GeoMesa + H3 Notebook](https://github.com/databricks/delta-live-tables-notebooks/blob/main/scala/NYC%20Taxi/GeoMesa%20%2B%20H3%20Notebook.scala) to create the `map_point2Location` table that maps the latitude and longitude data into NYC borough information.  Note and/or modify the location of the data in the last cell of this notebook.    This is a modified version of the notebook from the blog [Processing Geospatial Data at Scale With Databricks](https://databricks.com/blog/2019/12/05/processing-geospatial-data-at-scale-with-databricks.html).

1. Run the first three *stages* of the [2015 NYC Taxi Dashboard Pipeline Queries](https://github.com/databricks/delta-live-tables-notebooks/blob/main/scala/NYC%20Taxi/2015%20NYC%20Taxi%20Dashboard%20Pipeline%20Queries.scala) notebook.  It will create the source table `DAIS21.nyctaxi_greencab_source` with one day of data.  It also creates the origin table `DAIS21.nyctaxi_greencab_origin` which you can use to load more data into the source table that the pipeline will read.   When you need to load new data, modify and run the **3. Load Data** cell.  

1. Create your pipeline using the following parameters:
   * From your Databricks workspace, click **Jobs** and then **Pipelines**; click on **Create Pipeline**
   * Fill in the **Pipeline Name**, e.g. `2015 NYC Taxi Dashboard Pipeline`
   * For the **Notebook Path**, fill in the path of the notebook.  
      * The Scala notebook can be found at: `scala/NYC Taxi/2015 NYC Taxi Dashboard Pipeline Queries.scala`
      * The Python notebook can be found at: `python/NYC Taxi/2015 NYC Taxi Dashboard Pipeline (BZ-AG-AU).py`
      * You can also get the notebook path using this command: `dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()`

1. Click **Start**

The following tables are created:
* `DAIS21.bz_green_cab`: The bronze table that contains the Green Cab NYC Taxi data
* `DAIS21.au_summary_stats`: Contains the gold table summary statistics
* `DAIS21.au_payment_by_hour`: Contains the gold table payment by hour
* `DAIS21.au_boroughs`: Contains the gold tqable of NYC boroughs
* `DAIS21.expectations_log`: References the Delta Live Table data quality metrics

The **2015 NYC Taxi Dashboard** queries the above listed tables; we will include the scripts for this dashboard soon.



