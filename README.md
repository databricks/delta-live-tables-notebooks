<h1>Delta Live Tables Example Notebooks</h1>

<p align="center">
  <img src="https://delta.io/wp-content/uploads/2019/04/delta-lake-logo-tm.png" width="140"/><br>
  <strong>Delta Live Tables</strong> is a new framework designed to enable customers to successfully declaratively define, deploy, test & upgrade data pipelines and eliminate operational burdens associated with the management of such pipelines.
</p>
<p align="center">
  This repo contains Delta Live Table examples designed to get customers started with
  building, deploying and running pipelines.
</p>

# Getting Started

* Connect your Databricks workspace using the <img src="https://databricks.com/wp-content/uploads/2021/05/repos.png" width="140" style=" vertical-align:middle"/> feature to [this repo](https://github.com/databricks/delta-live-tables-notebooks)

* Choose one of the examples and create your pipeline!

# Examples
## Wikipedia
This wikipedia clickstream sample is a great way to jump start using Delta Live Tables (DLT).  It is a simple bificating pipeline that creates a view on your JSON data and then creates two tables for you.  

<img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/dlt-wikipedia_wiki-spark.png" width="400"/>

To do this, load the [Delta Live Tables > Wikipedia]() Python demo notebook and notice within the sample code the definition of a single view and two tables.

### Reviewing the notebook code

**1. Define the `json_clickstream` view**

  The following  create your temporary view of the clickstream data which is in JSON format 

  ```
  @create_view()              # Define your DLT view
  def json_clickstream():     # Python function for this view
    return (
      # The data that makes the `json_clickstream` view
      spark.read.json("/.../2015_2_clickstream.json")  
    )
  ```

**2. Define the wiki_spark table**

  Based on the Wikipedia February 2015 clickstream data, let's find all the occurences where the current title is "Apache Spark" (`curr_title = 'Apache_Spark'`) order by the number of occurences of the (referer, resource) pair (`n`).

  ```
  @create_table()           # Define your persistented DLT table* 
  def wiki_spark():         # Python function for this table
    return (
      # Read the `json_clickstream` view you had previously defined
      (read("json_clickstream")   
        .withColumn("n", expr("CAST(n AS integer) AS n"))
        .filter(expr("curr_title == 'Apache_Spark'"))
        .sort(desc("n"))
    )

  * Note, you will need to set DLT pipeline `target` configuration settings for the table to persistent; more info 
  ```

**3. Define the wiki_top50 table**

  Based on the Wikipedia February 2015 clickstream data, let's find the top 50 occurences 
  ```
  @create_table()         # Define your persistented DLT table* 
  def wiki_top50():       # Python function fof this table
    return (
      # Read the `json_clickstream` view you had previously defined
      (read("json_clickstream")
        .groupBy("curr_title")
        .agg(sum("n").alias("sum_n"))
        .sort(desc("sum_n")).limit(50))
    )
  ```

### Running your pipeline

**1. Create your pipeline using the following parameters**

  * From your Databricks workspace, click **Jobs** and then **Pipelines**; click on **Create Pipeline**
  * Fill in the **Pipeline Name**, e.g. `Wikipedia`
  * For the **Notebook Path**, fill in the path of the notebook.  
    * The Python notebook can be found at: `python/Wikipedia/Wikipedia Pipeline.py`
    * You can also get the notebook path using this command: `dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()`

**2. Edit your pipeline JSON**

  Once you have setup your pipeline, click **Edit Settings** near the top, the JSON will look similar to below
  ```
  {
    "id": "26827819-9d34-42ad-932c-5571e334e0c8",
    "name": "Wikipedia",
    "storage": "dbfs:/pipelines/26827819-9d34-42ad-932c-5571e334e0c8",
    "libraries": [
        {
            "notebook": {
                "path": "/Repos/.../DLT/Python/Wikipedia/Wikipedia Pipeline"
            }
        }
    ],
    "target": "wiki_demo",
    "continuous": false
  }
  ```
  To persist your tables, add the `target` parameter to specify which database you want to persist your tables, e.g. `wiki_demo`.

**3. Click Start**

  To view the progress of your pipeline, refer to the progress flow near the bottom of the pipeline details UI as noted in the following image. 

  <img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/dlt-wikipedia_wiki-spark-progress.png" width="500"/>


**4. Reviewing the results**

  Once your pipeline has completed processing, you can review the data by opening up a new Databricks notebook and running the following SQL statements:

  ```
  %sql
  -- Review the top referrers to Wikipedia's Apache Spark articles
  SELECT * FROM wiki_demo.wiki_spark
  ```

  Unsurprisingly, the top referrer is "Google" which you can see graphically when you convert your table into an area chart.
  <img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/dlt-wikipedia_wiki-spark-area-chart.png" width="600"/>

Now that you are familar with this basic pipeline, let's go to our next pipeline to introduce expectations.


## Flight Performance

**TO DO**: Include flight performance basic pipeline with expectations



## Loan Risk Analysis

This Loan Risk Analysis pipeline sample is based on the [Loan Risk Analysis with XGBoost and Databricks Runtime for Machine Learning](https://databricks.com/blog/2018/08/09/loan-risk-analysis-with-xgboost-and-databricks-runtime-for-machine-learning.html).  This pipelne is in two parts:
1. The [Loan Risk Pipeline]() Delta Live Tables notebook is a sample Delta medallion architecture ala bronze (BZ), silver (Ag), and gold (Au) data quality layers.  It processes the loan risk data through multiple transformations and two gold tables.  The pipeline also runs a pyspark logistic regression ML pipeline to predict bad loans based on this data.

 <img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/Loan%20Risk%20Pipeline.png" width=400>

2. The [Loan Risk Analysis]() notebook produces a number of graphs as well as executes a ML pipeline for better loan risk predictions.

 <img src="https://databricks.com/wp-content/uploads/2018/08/Screen-Shot-2018-08-09-at-3.13.56-AM.png" width=600>

3. Create your pipeline using the following parameters:
   * From your Databricks workspace, click **Jobs** and then **Pipelines**; click on **Create Pipeline**
   * Fill in the **Pipeline Name**, e.g. `Loan Risk Pipeline`
   * For the **Notebook Path**, fill in the path of the notebook.  
      * The Python notebook can be found at: `python/Loan Risk Analysis/Loan Risk Pipeline.py`
      * You can also get the notebook path using this command: `dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()`
   * Modify the `database_name` parameter (e.g. `database_name = "loan.risk"`)  

4. Click **Start**

5. Once your pipeline has finished, you can view the results using the *Loan Risk Analysis* notebook.


## NYC Taxi Dataset

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



