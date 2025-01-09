-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS dbdemos.tpch_kimball

-- COMMAND ----------

CREATE OR REPLACE FUNCTION dbdemos.tpch_kimball.date_key(input_date DATE)
RETURNS BIGINT
COMMENT 'Converts a DATE into a Surrogate Key for the `date_dim` table'
RETURN CAST(DATE_FORMAT(input_date, 'yyyyMMdd') AS BIGINT)

-- COMMAND ----------

CREATE OR REPLACE FUNCTION dbdemos.tpch_kimball.net_sales(
  price DECIMAL(18, 2),
  discount DECIMAL(18, 2),
  tax DECIMAL(18, 2),
  is_returned INT
)
RETURNS DECIMAL(18, 2)
COMMENT 'Calculates the price after discounts and taxes have been applied'
RETURN price * (1 - discount) * (1 + tax) * (1 - is_returned)
