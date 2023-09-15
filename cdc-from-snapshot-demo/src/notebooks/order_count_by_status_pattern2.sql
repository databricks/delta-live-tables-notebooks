-- Databricks notebook source
CREATE OR REFRESH LIVE TABLE order_status_count
AS
SELECT order_status, count(order_id) as orders_count
FROM LIVE.orders_pattern2
GROUP BY order_status
