-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Kimball Dimensional Modeling
-- MAGIC
-- MAGIC Dimensional modeling is a design concept used in data warehousing to structure data for easy retrieval and analysis. It involves organizing data into fact and dimension tables. Fact tables store quantitative data for analysis, while dimension tables store descriptive attributes related to the facts.
-- MAGIC
-- MAGIC #### Dimensions
-- MAGIC Dimensions provide context to the facts, making the data meaningful and easier to understand.
-- MAGIC
-- MAGIC ##### Characteristics
-- MAGIC - **Descriptive Attributes**: Dimensions contain descriptive information, such as names, dates, or categories.
-- MAGIC - **Unique Row Key**: Each row has a [surrogate key](https://en.wikipedia.org/wiki/Surrogate_key) that uniquely identifies each record. This is analogous to a primary key in transactional DBs.
-- MAGIC - **Handle Unknown, Unresolved, or Invalid Data**: Kimball dimensions typically include special-purpose rows to manage incomplete or missing data. These rows often contain descriptions like “Unknown,” “Not Applicable,” or “Pending” for cases where data is missing or unresolved. This ensures referential integrity and allows for analysis even with incomplete data.
-- MAGIC - **Denormalizated**: Dimension tables in Kimball design are usually denormalized to avoid complex joins and support faster query performance, simplifying the data structure for business intelligence and reporting.
-- MAGIC - **Can track changes in different ways**: Dimensions come in different "types", refered to as [slowly changing dimension (SCD) types](https://en.wikipedia.org/wiki/Slowly_changing_dimension). For example, a table that contains the current snapshot of the world is called an SCD Type 1 dimension; but if the table tracks historical changes over time, it's an SCD Type 2 dimension. Databricks has [built-in functions](https://docs.databricks.com/en/delta-live-tables/cdc.html) to make creating these types easy.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Dimension: customers_dim
-- MAGIC ##### Type: SCD 1
-- MAGIC
-- MAGIC This table contains customer-related information, providing the current descriptive attributes for each customer. It includes details such as the customer's name, address, nation, region, and phone number.
-- MAGIC
-- MAGIC | Column  | Type   | Description                        |
-- MAGIC |---------|--------|------------------------------------|
-- MAGIC | key     | BIGINT | Unique key for each record  |
-- MAGIC | name    | STRING | Customer's name                    |
-- MAGIC | address | STRING | Customer's address                 |
-- MAGIC | nation  | STRING | Customer's nation                  |
-- MAGIC | region  | STRING | Customer's region                  |
-- MAGIC | phone   | STRING | Customer's phone number            |

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW customers_dim (
  key BIGINT NOT NULL,
  name STRING,
  address STRING,
  nation STRING,
  region STRING,
  phone STRING,
  CONSTRAINT customers_dim_pk PRIMARY KEY(key)
) AS 
WITH nation_regions AS (
  SELECT
    n.n_nationkey AS nationkey,
    n.n_name AS nation,
    r.r_name AS region
  FROM
    samples.tpch.nation n
    JOIN samples.tpch.region r ON n.n_regionkey = r.r_regionkey
)
SELECT
  c.c_custkey AS key,
  c.c_name AS name,
  c.c_address AS address,
  n.nation AS nation,
  n.region AS region,
  c.c_phone AS phone
FROM
  samples.tpch.customer c
  JOIN nation_regions n ON c.c_nationkey = n.nationkey
UNION ALL 
SELECT -1, 'unknown name', 'unknown address', 'unknown nation', 'unknown region', 'unknown phone number'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dimension: parts_dim
-- MAGIC ##### Type: SCD 1
-- MAGIC
-- MAGIC This table contains information on the parts sold by the company, providing the current descriptive attributes for each part. It includes details such as the part's name, manufacturer, brand, type, size, container, and price.
-- MAGIC
-- MAGIC | Column      | Type          | Description                        |
-- MAGIC |-------------|---------------|------------------------------------|
-- MAGIC | key         | BIGINT        | Unique key for each record         |
-- MAGIC | name        | STRING        | Part's name                        |
-- MAGIC | manufacturer| STRING        | Part's manufacturer                |
-- MAGIC | brand       | STRING        | Part's brand                       |
-- MAGIC | type        | STRING        | Part's type                        |
-- MAGIC | size        | INT           | Part's size                        |
-- MAGIC | container   | STRING        | Part's container                   |
-- MAGIC | price       | DECIMAL(18,2) | Part's price                       |

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW parts_dim (
  key BIGINT NOT NULL,
  name STRING,
  manufacturer STRING,
  brand STRING,
  type STRING,
  size INT,
  container STRING,
  price DECIMAL(18,2),
  CONSTRAINT parts_dim_pk PRIMARY KEY(key)
) AS 
SELECT
  p.p_partkey AS key,
  p.p_name AS name,
  p.p_mfgr AS manufacturer,
  p.p_brand as brand,
  p.p_type AS type,
  p.p_size AS size,
  p.p_container AS container,
  p.p_retailprice AS price
FROM samples.tpch.part p
UNION ALL
SELECT -1, 'unknown part', 'unknown manufacturer', 'unknown brand', 'unknown type', NULL, 'unknown container', NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dimension: date_dim
-- MAGIC ##### Type: SCD 1
-- MAGIC
-- MAGIC This table contains date-related information, providing various attributes for each day in time. It includes details such as the date itself, day of the week, day of the month, day of the year, week of the year, month name, month number, quarter, year, and whether it is a weekend.
-- MAGIC
-- MAGIC | Column           | Type    | Description                              |
-- MAGIC |------------------|---------|------------------------------------------|
-- MAGIC | key              | BIGINT  | Unique key for each record               |
-- MAGIC | date             | DATE    | The date                                 |
-- MAGIC | day_of_week_name | STRING  | Name of the day of the week              |
-- MAGIC | day_of_week      | INT     | Day of the week (1 = Sunday, 2 = Monday) |
-- MAGIC | day_of_month     | INT     | Day of the month (1 to 31)               |
-- MAGIC | day_of_year      | INT     | Day of the year (1 to 366)               |
-- MAGIC | week_of_year     | INT     | Week number of the year (1 to 53)        |
-- MAGIC | month_name       | STRING  | Name of the month                        |
-- MAGIC | month_number     | INT     | Month number (1 to 12)                   |
-- MAGIC | quarter          | INT     | Quarter of the year (1 to 4)             |
-- MAGIC | year             | INT     | Year                                      |
-- MAGIC | is_weekend       | BOOLEAN | Flag indicating if the date is a weekend |

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW date_dim (
  key BIGINT NOT NULL,
  date DATE,
  day_of_week_name STRING,
  day_of_week INT,
  day_of_month INT,
  day_of_year INT,
  week_of_year INT,
  month_name STRING,
  month_number INT,
  quarter INT,
  year INT,
  is_weekend BOOLEAN,
  CONSTRAINT date_dim_pk PRIMARY KEY(key)
) AS
WITH dates AS (
  SELECT 
    explode(sequence(DATE'1992-01-01', DATE'1998-12-31', INTERVAL 1 DAY)) as date
)
SELECT
    CAST(DATE_FORMAT(date, 'yyyyMMdd') AS BIGINT) AS key,    -- Format YYYYMMDD
    date,
    DATE_FORMAT(date, 'EEEE') AS day_of_week_name,            -- Day of the week name (e.g., 'Monday')
    DAYOFWEEK(date) AS day_of_week,                           -- Day of the week (1 = Sunday, 2 = Monday, ...)
    DAY(date) AS day_of_month,                                -- Day of the month (1 to 31)
    DAYOFYEAR(date) AS day_of_year,                           -- Day of the year (1 to 366)
    WEEKOFYEAR(date) AS week_of_year,                         -- Week number of the year (1 to 53)
    DATE_FORMAT(date, 'MMMM') AS month_name,                  -- Month name (e.g., 'January')
    MONTH(date) AS month_number,                              -- Month number (1 to 12)
    QUARTER(date) AS quarter,                                 -- Quarter of the year (1 to 4)
    YEAR(date) AS year,                                       -- Year (e.g., 2024)
    CASE WHEN DAYOFWEEK(date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend -- Weekend flag
FROM dates
UNION ALL
SELECT -1, NULL, 'unknown day of week', NULL, NULL, NULL, NULL, 'unknown month', NULL, NULL, NULL, NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dimension: month_dim
-- MAGIC ##### Type: SCD 1
-- MAGIC
-- MAGIC This table contains month-related information, providing various attributes for each month in time. It includes details such as the month name, month number, quarter, year, and the number of days in the month.
-- MAGIC
-- MAGIC | Column      | Type    | Description                              |
-- MAGIC |-------------|---------|------------------------------------------|
-- MAGIC | key         | BIGINT  | Unique key for each record               |
-- MAGIC | date        | DATE    | The first date of the month              |
-- MAGIC | month_name  | STRING  | Name of the month                        |
-- MAGIC | month_number| INT     | Month number (1 to 12)                   |
-- MAGIC | quarter     | INT     | Quarter of the year (1 to 4)             |
-- MAGIC | year        | INT     | Year                                     |
-- MAGIC | num_days    | INT     | Number of days in the month              |

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW month_dim (
  key BIGINT NOT NULL,
  date DATE,
  month_name STRING,
  month_number INT,
  quarter INT,
  year INT,
  num_days INT,
  CONSTRAINT month_dim_pk PRIMARY KEY(key)
) AS
WITH dates1 AS (
  SELECT
    explode(sequence(DATE'1992-01-01', DATE'1998-12-31', INTERVAL 1 MONTH)) as date
), dates AS (
  SELECT
      CAST(DATE_FORMAT(date, 'yyyyMM') AS BIGINT) AS key,
      date,
      DATE_FORMAT(date, 'MMMM') AS month_name,
      MONTH(date) AS month_number,
      QUARTER(date) AS quarter,
      YEAR(date) AS year,
      DATEDIFF(add_months(date, 1), date) AS num_days
  FROM dates1
)
SELECT * FROM dates WHERE key IS NOT NULL
UNION ALL
SELECT -1, NULL, 'unknown month', NULL, NULL, NULL, NULL