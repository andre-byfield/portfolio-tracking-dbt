{{ config(materialized='ephemeral') }}

WITH
src_data as ( 
    SELECT
    NAME as NAME -- TEXT
    , ID as ID -- TEXT
    , COUNTRY as COUNTRY -- TEXT
    , CITY as CITY -- TEXT
    , ZONE as ZONE -- TEXT
    , DELTA as DELTA -- FLOAT
    , DST_PERIOD as DST_PERIOD -- TEXT
    , OPEN as OPEN -- TEXT
    , CLOSE as CLOSE -- TEXT
    , LUNCH as LUNCH -- TEXT
    , OPEN_UTC as OPEN_UTC -- TEXT
    , CLOSE_UTC as CLOSE_UTC -- TEXT
    , LUNCH_UTC as LUNCH_UTC -- TEXT
    , LOAD_TS as LOAD_TS -- NUMBER
    , 'SEED.ABC_Bank_EXCHANGE' as RECORD_SOURCE
FROM {{ source('seeds', 'ABC_Bank_EXCHANGE') }}
 ),
default_record as (
  SELECT
      'Missing' as NAME
    , 'Missing' as ID
    , 'Missing' as COUNTRY
    , 'Missing' as CITY
    , 'Missing' as ZONE
    , 'Missing' as DELTA
    , 'Missing' as DST_PERIOD
    , 'Missing' as OPEN
    , 'Missing' as CLOSE
    , 'Missing' as LUNCH
    , 'Missing' as OPEN_UTC
    , 'Missing' as CLOSE_UTC
    , 'Missing' as LUNCH_UTC
    , '2020-01-01'          as LOAD_TS_UTC
    , 'System.DefaultKey'   as RECORD_SOURCE
),
with_default_record as(
    SELECT * FROM src_data
    UNION ALL
    SELECT * FROM default_record
),
 hashed as (
    SELECT
      {{ dbt_utils.surrogate_key(['ID'])
      }} as EXCHANGE_HKEY
    , {{ dbt_utils.surrogate_key([
             'ID', 'COUNTRY',
             'CITY', 'ZONE', 'DELTA',
             'DST_PERIOD', 'OPEN', 'CLOSE',
             'LUNCH', 'OPEN_UTC', 'CLOSE_UTC', 'LUNCH_UTC' ])
      }} as EXCHANGE_HDIFF
        , * EXCLUDE LOAD_TS
        , LOAD_TS as LOAD_TS_UTC
    FROM src_data
)

SELECT * FROM hashed