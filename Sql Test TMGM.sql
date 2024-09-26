/************************ 
   Part 1. Generate base table (base_table) that contains dt_report, login_hash, server_hash, symbol, currency, total volume, count of trades 
           for enabled users in users table.
           ** In this calculation, I presume open_time of trade is dt_report (date of report).
************************/

DROP TABLE IF EXISTS base_table;

SELECT A.open_time::DATE AS dt_report, 
       A.login_hash,
	   A.server_hash,
	   REPLACE(A.symbol, ',', '') AS symbol, -- repaire the value 'USD,CHF'
	   B.currency,
	   SUM(A.volume) AS totalVolume,
	   COUNT(*) AS countOfTrades
INTO base_table
FROM trades AS A JOIN (SELECT DISTINCT login_hash, currency from users WHERE enable = 1) AS B
ON A.login_hash = B.login_hash
GROUP BY A.open_time::DATE, A.login_hash, A.server_hash, A.symbol, B.currency;

/************************
   Part 2. Generate base table for each day between 2020-06-01 and 2020-09-30
*************************/

-- Generate base table (base_table_dt) with for each day between 2020-06-01 and 2020-09-30 for each enabled user
-- Here I only considered the combinations of login_hash, server_hash, symbol only when the combination appear in table trades.

DROP TABLE IF EXISTS base_table_dt;

SELECT 
    B.foundDate::DATE AS dt_report,
    A.login_hash,
    A.server_hash,
	A.symbol,
	A.currency,
	0.0 AS volume,
	0 AS countOfTrades,
	0.0 AS sum_volume_prev_7d,
	0.0 AS sum_volume_prev_all,
	0 AS rank_volume_symbol_prev_7d,
	0 AS rank_count_prev_7d,
	0.0 AS sum_volume_2020_08,
	'1900-01-01'::TIMESTAMP AS date_first_trade,
	0 AS row_number
INTO base_table_dt
FROM (SELECT DISTINCT login_hash, server_hash, symbol, currency FROM base_table) AS A
CROSS JOIN 
    generate_series('2020-06-01'::date, '2020-09-30'::date, '1 day'::interval) AS B(foundDate)
ORDER BY B.foundDate;

-- Update volume and count of trades based on table base_table generated from part 1

UPDATE base_table_dt AS A
SET volume = B.totalVolume,
    countOfTrades = B.countOfTrades
FROM base_table AS B
WHERE A.dt_report = B.dt_report AND 
      A.login_hash = B.login_hash AND 
	  A.server_hash = B.server_hash AND 
	  a.symbol = B.symbol AND 
	  a.currency = B.currency;

-- Add column id of type serial

ALTER TABLE base_table_dt ADD COLUMN id SERIAL PRIMARY KEY;

/***************************
   Part 3: Calculate for sum_volume_prev_7d, sum_volume_prev_all, rank_volume_symbol_prev_7d, rank_count_prev_7d, sum_volume_2020_08, 
           date_first_trade, row_number
***************************/

-- sum_volume_prev_7d

DROP TABLE IF EXISTS tem_table;

SELECT A.id, SUM(B.volume) AS totalVolume
INTO tem_table
FROM base_table_dt AS A LEFT JOIN base_table_dt AS B
ON A.login_hash = B.login_hash AND
   A.server_hash = B.server_hash AND
   A.symbol = B.symbol AND
   A.dt_report - B.dt_report >= 0 AND
   A.dt_report - B.dt_report <= 7
WHERE B.login_hash IS NOT null
GROUP BY A.id;

UPDATE base_table_dt AS A
SET sum_volume_prev_7d = B.totalVolume
FROM tem_table AS B
WHERE A.id = B.id;

DROP TABLE IF EXISTS tem_table;

-- sum_volume_prev_all

DROP TABLE IF EXISTS tem_table;

SELECT A.id, SUM(B.volume) AS totalVolume
INTO tem_table
FROM base_table_dt AS A LEFT JOIN base_table_dt AS B
ON A.login_hash = B.login_hash AND
   A.server_hash = B.server_hash AND
   A.symbol = B.symbol AND
   A.dt_report - B.dt_report >= 0
WHERE B.login_hash IS NOT null
GROUP BY A.id;

UPDATE base_table_dt AS A
SET sum_volume_prev_all = B.totalVolume
FROM tem_table AS B
WHERE A.id = B.id;

DROP TABLE IF EXISTS tem_table;

-- rank_volume_symbol_prev_7d

DROP TABLE IF EXISTS tem_table;
DROP TABLE IF EXISTS tem_table_1;

SELECT A.id, A.login_hash, A.symbol, SUM(B.volume) AS totalVolume
INTO tem_table
FROM base_table_dt AS A LEFT JOIN base_table_dt AS B
ON A.login_hash = B.login_hash AND
   A.server_hash = B.server_hash AND
   A.symbol = B.symbol AND
   A.currency = B.currency AND
   A.dt_report - B.dt_report >= 0 AND
   A.dt_report - B.dt_report <= 7
WHERE B.login_hash IS NOT null
GROUP BY A.id, A.login_hash, A.symbol;

SELECT A.id,
       DENSE_RANK() OVER (PARTITION BY A.login_hash, A.symbol ORDER BY totalVolume DESC) AS denseRank
INTO tem_table_1
FROM tem_table AS A;

UPDATE base_table_dt AS A
SET rank_volume_symbol_prev_7d = B.denseRank
FROM tem_table_1 AS B
WHERE A.id = B.id;

DROP TABLE IF EXISTS tem_table;
DROP TABLE IF EXISTS tem_table_1;

-- rank_count_prev_7d

DROP TABLE IF EXISTS tem_table;
DROP TABLE IF EXISTS tem_table_1;

SELECT A.id, A.login_hash, SUM(B.countoftrades) AS totalCount
INTO tem_table
FROM base_table_dt AS A LEFT JOIN base_table_dt AS B
ON A.login_hash = B.login_hash AND
   A.server_hash = B.server_hash AND
   A.symbol = B.symbol AND
   A.currency = B.currency AND
   A.dt_report - B.dt_report >= 0 AND
   A.dt_report - B.dt_report <= 7
WHERE B.login_hash IS NOT null
GROUP BY A.id, A.login_hash;

SELECT A.id,
       DENSE_RANK() OVER (PARTITION BY A.login_hash ORDER BY totalCount DESC) AS denseRank
INTO tem_table_1
FROM tem_table AS A;

UPDATE base_table_dt AS A
SET rank_count_prev_7d = B.denseRank
FROM tem_table_1 AS B
WHERE A.id = B.id;

DROP TABLE IF EXISTS tem_table;
DROP TABLE IF EXISTS tem_table_1;

-- sum_volume_2020_08

DROP TABLE IF EXISTS tem_table;

SELECT A.id, SUM(B.volume) AS totalVolume
INTO tem_table
FROM base_table_dt AS A LEFT JOIN base_table_dt AS B
ON A.login_hash = B.login_hash AND
   A.server_hash = B.server_hash AND
   A.symbol = B.symbol AND
   A.dt_report - B.dt_report >= 0 AND
   B.dt_report >= '2020-08-01'
WHERE B.login_hash IS NOT null
GROUP BY A.id;

UPDATE base_table_dt AS A
SET sum_volume_2020_08 = B.totalVolume
FROM tem_table AS B
WHERE A.id = B.id;

DROP TABLE IF EXISTS tem_table;

-- date_first_trade

DROP TABLE IF EXISTS tem_table;

SELECT A.login_hash, A.server_hash, A.symbol, MIN(A.open_time) AS firstDateTime
INTO tem_table
FROM trades AS A
GROUP BY A.login_hash, A.server_hash, A.symbol;

UPDATE base_table_dt AS A
SET date_first_trade = B.firstDateTime
FROM tem_table AS B
WHERE A.login_hash = B.login_hash AND a.server_hash = B.server_hash AND a.symbol = B.symbol;

DROP TABLE IF EXISTS tem_table;

-- row_number and generate final result table cyx_result0828v2

DROP TABLE IF EXISTS cyx_result0828v2;

SELECT
     A.id,
	 A.dt_report,
	 A.login_hash,
	 A.server_hash,
	 A.symbol,
	 A.currency,
	 A.sum_volume_prev_7d,
	 A.sum_volume_prev_all,
	 A.rank_volume_symbol_prev_7d,
	 A.rank_count_prev_7d,
	 A.sum_volume_2020_08,
	 A.date_first_trade,
	 ROW_NUMBER() OVER (ORDER BY A.dt_report, A.login_hash, A.server_hash, A.symbol) AS row_number
INTO cyx_result0828v2
FROM base_table_dt AS A;

/***********************
   Part 4: Clean intermediate tables
***********************/

DROP TABLE IF EXISTS base_table;

DROP TABLE IF EXISTS base_table_dt;
