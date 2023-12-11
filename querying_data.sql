-- Databricks notebook source
-- MAGIC %python
-- MAGIC df_pin = spark.read.parquet('/mnt/data/df_pin.parquet')
-- MAGIC df_geo = spark.read.parquet('/mnt/data/df_geo.parquet')
-- MAGIC df_user = spark.read.parquet('/mnt/data/df_user.parquet')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_pin.createOrReplaceTempView('pin')
-- MAGIC df_geo.createOrReplaceTempView('geo')
-- MAGIC df_user.createOrReplaceTempView('user')

-- COMMAND ----------

SELECT pin.category,
       geo.country,
       COUNT(category) AS category_count
FROM
  pin
LEFT OUTER JOIN
  geo ON pin.ind = geo.ind
GROUP BY
  pin.category,
  geo.country;

-- COMMAND ----------

SELECT pin.category,
       YEAR(geo.timestamp) AS post_year,
       COUNT(pin.category) AS category_count
FROM
  pin
LEFT OUTER JOIN
  geo ON pin.ind = geo.ind
WHERE
  YEAR(geo.timestamp) > 2017
GROUP BY
  pin.category,
  YEAR(geo.timestamp);

-- COMMAND ----------

SELECT pin.poster_name,
       pin.follower_count,
       geo.country
FROM
  pin
LEFT OUTER JOIN
  geo ON pin.ind = geo.ind
GROUP BY 
  geo.country,
  pin.poster_name,
  pin.follower_count;


-- COMMAND ----------

SELECT
       pin.follower_count,
       geo.country
FROM
  pin
LEFT OUTER JOIN
  geo ON pin.ind = geo.ind
GROUP BY 
  geo.country,
  pin.follower_count
ORDER BY
  pin.follower_count DESC
LIMIT 1;

-- COMMAND ----------

SELECT pin.category,
       COUNT(pin.category) AS category_count,
       CASE 
        WHEN user.age >= 18 AND user.age < 25 THEN '18-24'
        WHEN user.age >= 25 AND user.age < 36 THEN '25-35'
        WHEN user.age >= 36 AND user.age < 51 THEN '36-50'
        ELSE '50+'
      END AS age_group
FROM
  pin
LEFT OUTER JOIN
  user ON pin.ind = user.ind
GROUP BY 
  pin.category,
  user.age,
  age_group
ORDER BY
  category_count DESC;


                

-- COMMAND ----------

WITH cte AS (
SELECT pin.follower_count,
       CASE 
        WHEN user.age >= 18 AND user.age < 25 THEN '18-24'
        WHEN user.age >= 25 AND user.age < 36 THEN '25-35'
        WHEN user.age >= 36 AND user.age < 51 THEN '36-50'
        ELSE '50+'
      END AS age_group
FROM
  pin
LEFT OUTER JOIN
  user ON pin.ind = user.ind
)
SELECT age_group,
       percentile_approx(follower_count, 0.5) AS median_follower_count
FROM
  cte
GROUP BY
  age_group;


-- COMMAND ----------

SELECT YEAR(geo.timestamp) AS post_year,
       COUNT(user.date_joined) AS number_users_joined
FROM
  user
LEFT OUTER JOIN
  geo ON user.ind = geo.ind
WHERE
  YEAR(geo.timestamp) BETWEEN 2015 AND 2020
GROUP BY
  YEAR(geo.timestamp)
ORDER BY
  post_year DESC;

-- COMMAND ----------

WITH cte AS (
  SELECT YEAR(geo.timestamp) AS post_year,
       pin.follower_count
FROM
  pin
LEFT OUTER JOIN
  geo ON pin.ind = geo.ind
WHERE
  YEAR(geo.timestamp) >= 2015 AND YEAR(geo.timestamp) <= 2020
GROUP BY
  YEAR(geo.timestamp),
  pin.follower_count
)
SELECT post_year,
       percentile_approx(follower_count, 0.5) AS median_follower_count
FROM
  cte
GROUP BY
  post_year;

-- COMMAND ----------

WITH cte AS (
  SELECT YEAR(geo.timestamp) AS post_year,
       pin.follower_count,
      CASE 
        WHEN user.age >= 18 AND user.age < 25 THEN '18-24'
        WHEN user.age >= 25 AND user.age < 36 THEN '25-35'
        WHEN user.age >= 36 AND user.age < 51 THEN '36-50'
        ELSE '50+'
      END AS age_group
FROM
  pin
LEFT OUTER JOIN
  geo ON pin.ind = geo.ind
LEFT OUTER JOIN
  user ON pin.ind = user.ind
WHERE
  YEAR(geo.timestamp) >= 2015 AND YEAR(geo.timestamp) <= 2020
)
SELECT post_year,
       age_group,
       percentile_approx(follower_count, 0.5) AS median_follower_count
FROM
  cte
GROUP BY
  post_year,
  age_group
ORDER BY
  post_year DESC;

-- COMMAND ----------


