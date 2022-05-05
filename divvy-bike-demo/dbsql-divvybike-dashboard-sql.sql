-- Databricks notebook source
-- DBTITLE 1,Station List Parameter Query
WITH station_list AS (
  SELECT
    DISTINCT(name)
  FROM
    divvy_bikes.cleaned_station_information
)
SELECT
  *
FROM
  station_list
UNION ALL
SELECT
  '-- ALL STATIONS --' AS stations
ORDER BY
  name asc;

-- COMMAND ----------

-- DBTITLE 1,Weather Headlines
-- Powers the dashboard Counter visualizations for weather info 
SELECT
  (avg(wi.main_temp) - 273.15) * 9/5 + 32 AS avg_temp_F, 
  avg(wi.wind_speed) AS avg_wind_speed_MPH
FROM
  divvy_bikes.cleaned_weather_information wi, divvy_bikes.cleaned_station_information si
WHERE
  (wi.dt, wi.station_id) IN (
    SELECT
      max(dt),
      station_id
    FROM
      divvy_bikes.cleaned_weather_information
    WHERE
      dt_hr - 18000 = unix_timestamp(date_trunc('HOUR', '{{ date_and_time }}'))
    GROUP BY station_id
  )
  AND wi.station_id = si.station_id
  AND (si.name IN ({{ station_list }}) OR "{{ station_list }}" LIKE "%-- ALL STATIONS --%")

-- COMMAND ----------

-- DBTITLE 1,Station Status Headlines
-- Powers the dashboard Counter visualizations for station status info 

SELECT
  sum(ss.num_bikes_available),
  sum(ss.num_docks_available)
FROM
  divvy_bikes.cleaned_station_status ss, divvy_bikes.cleaned_station_information si
WHERE
  ss.last_updated = (
    SELECT
      max(last_updated)
    FROM
      divvy_bikes.cleaned_station_status
    WHERE
      last_updated_hr - 18000 = unix_timestamp(date_trunc('HOUR', '{{ date_and_time }}'))
  )
AND ss.station_id = si.station_id
AND (si.name IN ({{ station_list }}) OR "{{ station_list }}" LIKE "%-- ALL STATIONS --%")

-- COMMAND ----------

-- DBTITLE 1,Bike Availability for Last 24hrs
-- Powers the dashboard Line Chart visualization for station status over time 

SELECT
  sum(ss.num_bikes_available) AS total_bikes_available,
  sum(ss.num_docks_available) AS total_docks_available,
  from_utc_timestamp(ss.last_updated_ts, 'America/Chicago') AS last_updated_CST
FROM
  divvy_bikes.cleaned_station_status ss, divvy_bikes.cleaned_station_information si
WHERE
  ss.last_updated >= (
    SELECT
      max(last_updated) - 86400
    FROM
      divvy_bikes.cleaned_station_status
    WHERE
      last_updated_hr - 18000 = unix_timestamp(date_trunc('HOUR', '{{ date_and_time }}'))
  )
AND last_updated_hr - 18000 <= unix_timestamp(date_trunc('HOUR', '{{ date_and_time }}'))
AND ss.station_id = si.station_id
AND (si.name IN ({{ station_list }}) OR "{{ station_list }}" LIKE "%-- ALL STATIONS --%")
GROUP BY
  ss.last_updated_ts
ORDER BY
  ss.last_updated_ts ASC

-- COMMAND ----------

-- DBTITLE 1,Weather for Last 24hrs
-- Powers the dashboard Bar Chart visualization for weather stats over time 

SELECT
  (avg(wi.main_temp) - 273.15) * 9/5 + 32 AS avg_temp_F, 
  avg(wi.wind_speed) AS avg_wind_speed_MPH,
  avg(wi.rain_1h) AS avg_rain_1h,
  avg(wi.snow_1h) AS avg_snow_1h,
  max(wi.dt) AS max_dt,
  CAST(HOUR(from_utc_timestamp(wi.dt_ts, 'America/Chicago')) AS string) AS dt_hr_CST
FROM
  divvy_bikes.cleaned_weather_information wi, divvy_bikes.cleaned_station_information si
WHERE
  (wi.dt) >= (
    SELECT
      max(dt) - 86400
    FROM
      divvy_bikes.cleaned_weather_information
    WHERE
      dt_hr - 18000 = unix_timestamp(date_trunc('HOUR', '{{ date_and_time }}'))
  )
  AND dt_hr - 18000 <= unix_timestamp(date_trunc('HOUR', '{{ date_and_time }}'))
  AND wi.station_id = si.station_id
  AND (si.name IN ({{ station_list }}) OR "{{ station_list }}" LIKE "%-- ALL STATIONS --%")
  GROUP BY dt_hr_CST
  ORDER BY max_dt ASC

-- COMMAND ----------

-- DBTITLE 1,Station and Weather Headlines for Now
-- Powers the Real-Time Station & Weather Status Table visualization 

SELECT
  si.name as station_name,
  si.station_id as station_id,
  si.station_type as station_type,
  wi.name as weather_station,
  regexp_replace(CAST(wi.weather_description AS string),'\\[|\\]','') AS weather_desc,
  (wi.main_temp - 273.15) * 9/5 + 32 AS temp_F,
  wi.snow_1h AS snow_1h,
  wi.rain_1h AS rain_1h,
  si.capacity as capacity,
  ss.num_bikes_available AS bikes_available,
  ss.num_docks_available AS docks_available,
  ss.num_ebikes_available AS ebikes_available,
  ss.num_bikes_disabled AS bikes_disabled,
  ss.num_docks_disabled AS docks_disabled,
  from_utc_timestamp(ss.last_updated_ts, 'America/Chicago') AS last_updated_CST,
  from_utc_timestamp(ss.last_reported_ts, 'America/Chicago') AS last_reported_CST,
  ss.secs_since_last_reported
FROM
  divvy_bikes.cleaned_station_information si
  INNER JOIN divvy_bikes.cleaned_station_status ss ON si.station_id = ss.station_id
  AND (si.name IN ({{ station_list }}) OR "{{ station_list }}" LIKE "%-- ALL STATIONS --%")
  AND
    ss.last_updated = (
      SELECT
        max(last_updated)
      FROM
        divvy_bikes.cleaned_station_status
      WHERE
        last_updated_hr - 18000 = unix_timestamp(date_trunc('HOUR', '{{ date_and_time }}')) - 3600
    )
  LEFT JOIN divvy_bikes.cleaned_weather_information wi ON si.station_id = wi.station_id
  AND ss.last_updated_hr = wi.dt_hr
  ORDER BY ss.num_bikes_available DESC;

-- COMMAND ----------

-- DBTITLE 1,Weather Word Cloud
-- Powers the dashboard Word Cloud for weather description

SELECT
  weather_description,
  SUM(num_stations) AS num_stations
FROM
  (
    SELECT
      explode(wi.weather_description) AS weather_description,
      count(*) AS num_stations
    FROM
      divvy_bikes.cleaned_weather_information wi
    WHERE
      (wi.dt, wi.station_id) IN (
        SELECT
          max(dt),
          station_id
        FROM
          divvy_bikes.cleaned_weather_information
        WHERE
          dt_hr - 18000 = unix_timestamp(date_trunc('HOUR', '{{ date_and_time }}')) - 3600
        GROUP BY
          station_id
      )
    GROUP BY
      weather_description
  )
GROUP BY
  weather_description

-- COMMAND ----------

-- DBTITLE 1,Zero Availability Map
-- Powers the Map visualization showing stations with zero availability

SELECT
  si.name AS station_name,
  si.station_type as station_type,
  si.capacity as capacity,
  si.lat AS lat,
  si.lon AS lon,
  ss.num_bikes_available AS bikes_available,
  ss.num_docks_available AS docks_available,
  ss.num_ebikes_available AS ebikes_available,
  ss.num_bikes_disabled AS bikes_disabled,
  ss.num_docks_disabled AS docks_disabled,
  ss.last_reported_ts AS last_reported,
  ss.last_updated_ts AS last_updated,
  ss.secs_since_last_reported,
  'No Bikes Available' AS availability
FROM
  divvy_bikes.cleaned_station_information si
  INNER JOIN divvy_bikes.cleaned_station_status ss ON si.station_id = ss.station_id
  AND
    ss.last_updated = (
      SELECT
        max(last_updated)
      FROM
        divvy_bikes.cleaned_station_status
      WHERE
        last_updated_hr - 18000 = unix_timestamp(date_trunc('HOUR', '{{ date_and_time }}'))
        )
WHERE ss.num_bikes_available = 0
UNION 
SELECT
  si.name AS station_name,
  si.station_type as station_type,
  si.capacity as capacity,
  si.lat AS lat,
  si.lon AS lon,
  ss.num_bikes_available AS bikes_available,
  ss.num_docks_available AS docks_available,
  ss.num_ebikes_available AS ebikes_available,
  ss.num_bikes_disabled AS bikes_disabled,
  ss.num_docks_disabled AS docks_disabled,
  ss.last_reported_ts AS last_reported,
  ss.last_updated_ts AS last_updated,
  ss.secs_since_last_reported,
  'No Docks Available' AS availability
FROM
  divvy_bikes.cleaned_station_information si
  INNER JOIN divvy_bikes.cleaned_station_status ss ON si.station_id = ss.station_id
  AND
    ss.last_updated = (
      SELECT
        max(last_updated)
      FROM
        divvy_bikes.cleaned_station_status
      WHERE
        last_updated_hr - 18000 = unix_timestamp(date_trunc('HOUR', '{{ date_and_time }}'))
        )
WHERE ss.num_docks_available = 0