<h1>Delta Live Tables Example Notebooks</h1>

<p align="center">
  <img src="https://delta.io/wp-content/uploads/2019/04/delta-lake-logo-tm.png" width="140"/><br>
  <strong>Delta Live Tables</strong> is a new framework designed to enable customers to successfully declaratively define, deploy, test & upgrade data pipelines and eliminate operational burdens associated with the management of such pipelines.
</p>
<p align="center">
  This repo contains Delta Live Table examples designed to get customers started with building, deploying and running pipelines.
</p>

## Getting Started

* Connect your Databricks workspace using the <img src="https://databricks.com/wp-content/uploads/2021/05/repos.png" width="140" style=" vertical-align:middle"/> feature to [this repo](https://github.com/databricks/delta-live-tables-notebooks)

* Choose one of the examples and create your pipeline!

## Examples
### Loan Risk Analysis


### NYC Taxi Dataset

The NYC Taxi demo is available in `scala` and `python` to process the [NYC Taxi dataset](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) specific to the year 2015.  The Python code generates the following pipeline in the form of a Delta medallion architecture ala bronze (BZ), silver (Ag), and gold (Au) data quality layers. 

<img src="images/2015 NYCTaxi Dashboard Pipeline (BZ-AG-AU).png" width=600>


You can modify the pipeline to process all of the data but for this demo, we restricted it to 2015 because it was the most recent year of data that contained  pickup and dropoff longitude and latitude points.  This data is used in the following **2015 NYC Taxi Dashboard with Expectations**.

<img src="images/2015-nyctaxi-dashboard-with-expectations.gif" width=800/>


- Put steps on how to run a simple example (perhaps boston housing demo)
- create a pipeline to point to this notebook (create an animated GIF showing this)
- Run the the example (show it running)
- Show the output (create sample notebook to read it, fill in pipelines ID to read it)



