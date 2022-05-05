# Delta-Live-Tables (DLT)

Welcome to the repository for the Databricks Delta Live Tables Divvy Bike demo.

You can use [Databricks Projects](https://docs.databricks.com/repos.html) to clone this repo and get started with this demo, or download the .dbc archive and import the notebooks manually.

## Reading Resources

* [Video of Demo on YouTube](https://youtu.be/BIxwoO65ylY)

## Setup/Requirements
- Import the notebooks "data_generator.py" and "change-data-capture-example.sql"
- Run data_generator.py first to generate the data for the example
- Add the configuration to enable Change Data Capture according to the [documentation](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-cdc.html#requirements)

- To enable the OpenWeather API data stream please sign up for a free account at [Open Weather Map](https://home.openweathermap.org/users/sign_up) to get an API key. You will need to use this with the python-weatherinfo-api-ingest notebook.
- To configure the data ingest process to capture the source data create 2 separate Databricks Jobs as follows:
- Create a single task job called Get_Station_Availability_Status. Point this job at the python-divvybike-api-ingest-stationstatus notebook and schedule it to run every minute.
- Create a job called Get_Station_Weather_Info. Create a task that points to the python-divvybike-api-ingest-stationinformation notebook. Create a second task (that depends on the previous task) and point it at the python-weatherinfo-api-ingest notebook. Schedule the notebook to run every hour.
- Create a new Delta Live Tables Pipeline and point it at the dlt-sql-streaming-divvybikes notebook using the database target of your choice. Be sure to create this as a 'Continuous' pipeline (vs triggered).
- If you would like to recreate the Databricks SQL dashboard seen in the demo video refer to the dbsql-divvybike-dashboard-sql notebook.


### DBR Version
The features used in the notebooks were tested on DBR 9.1 LTS