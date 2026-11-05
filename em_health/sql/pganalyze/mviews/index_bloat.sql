/* Create a materialized view of estimated B-tree index bloat.
   pg_catalog and timescale cache schemas are ignored, as well as compressed chunks from hypertables.
   Minimum relation size (in 8 KiB pages) to analyze is based on the total number of objects in the database (0 / 250 / 2500).
   If is_na = true, do not trust the stats
   This SQL is adapted from https://gitlab.com/postgres-ai/postgres-checkup under AGPLv3 license
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS pganalyze.index_bloat AS
WITH
  object_count AS (
    SELECT COUNT(*) AS objects_count
    FROM pg_class
  ),
  settings AS (
    SELECT
      CASE WHEN objects_count > 100000 THEN 2500
           WHEN objects_count >= 10000 THEN 250
           ELSE 0 END AS default_min_relpages
    FROM
      object_count
  ),
  pg_rel_stats AS (
    SELECT
      schemaname,
      tablename,
      attname,
      avg_width,
      null_frac
    FROM pg_stats
  ),
  step0 AS (
    SELECT
      tbl.oid AS tblid,
      nspname,
      tbl.relname AS tblname,
      idx.relname AS idxname,
      idx.reltuples,
      idx.relpages,
      idx.relam,
      indrelid,
      indexrelid,
      COALESCE(SUBSTRING(ARRAY_TO_STRING(idx.reloptions, ' ') FROM 'fillfactor=([0-9]+)') :: SMALLINT,
               90) AS fillfactor
    FROM
      pg_index
      JOIN pg_class idx
        ON idx.oid = pg_index.indexrelid
      JOIN pg_class tbl
        ON tbl.oid = pg_index.indrelid
      JOIN pg_namespace
        ON pg_namespace.oid = idx.relnamespace
      JOIN pg_am a
        ON idx.relam = a.oid
      CROSS JOIN settings st
    WHERE
      a.amname = 'btree'
      AND pg_index.indisvalid
      AND tbl.relkind = 'r'
      AND idx.relpages > st.default_min_relpages
      AND pg_namespace.nspname NOT SIMILAR TO '(pg_catalog|information_schema|_timescaledb_c%)'
      AND tbl.relname NOT SIMILAR TO '(%compress%|bgw%)'
  ),
  step1 AS (
    SELECT
      i.tblid,
      i.nspname AS schema_name,
      i.tblname AS table_name,
      i.idxname AS index_name,
      i.reltuples,
      i.relpages,
      CURRENT_SETTING('block_size') :: NUMERIC AS bs,
      fillfactor,
      CASE WHEN VERSION() ~ 'mingw32|64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END AS maxalign,
      24 AS pagehdr,
      16 AS pageopqdata,
      /* per tuple header: add IndexAttributeBitMapData if some cols are null-able */
      CASE WHEN MAX(COALESCE(s.null_frac, 0)) = 0 THEN 2 -- IndexTupleData size
           ELSE 2 + ((32 + 8 - 1) / 8) -- IndexTupleData size + IndexAttributeBitMapData size ( max num filed per index + 8 - 1 /8)
        END AS index_tuple_hdr_bm,
      /* data len: we remove null values save space using it fractional part from stats */
      SUM((1 - COALESCE(s.null_frac, 0)) * COALESCE(s.avg_width, 1024)) AS nulldatawidth,
      MAX(CASE WHEN a.atttypid = 'pg_catalog.name' :: regtype THEN 1 ELSE 0 END) > 0 AS is_na
    FROM
      pg_attribute AS a
      JOIN step0 AS i
        ON a.attrelid = i.indexrelid
      JOIN pg_rel_stats AS s
        ON s.schemaname = i.nspname AND ((s.tablename = i.tblname AND s.attname =
                                                                      pg_catalog.pg_get_indexdef(a.attrelid, a.attnum, TRUE)) -- stats from tbl
        OR (s.tablename = i.idxname AND s.attname = a.attname) -- stats from functional cols
        )
    WHERE
      a.attnum > 0
    GROUP BY 1,2,3,4,5,6,7,8
  ),
  step2 AS (
    SELECT *,
      (index_tuple_hdr_bm + maxalign -- Add padding to the index tuple header to align on MAXALIGN
         - CASE WHEN index_tuple_hdr_bm % maxalign = 0 THEN maxalign ELSE index_tuple_hdr_bm % maxalign END +
       nulldatawidth + maxalign -- Add padding to the data to align on MAXALIGN
        - CASE WHEN nulldatawidth = 0 THEN 0
               WHEN nulldatawidth :: INTEGER % maxalign = 0 THEN maxalign
               ELSE nulldatawidth :: INTEGER % maxalign END) :: NUMERIC AS nulldatahdrwidth
    FROM
      step1
  ),
  step3 AS (
    SELECT *,
      -- ItemIdData size + computed avg size of a tuple (nulldatahdrwidth)
      COALESCE(1 + CEIL(reltuples / FLOOR((bs - pageopqdata - pagehdr) / (4 + nulldatahdrwidth) :: FLOAT)),
               0) AS est_pages,
      COALESCE(1 + CEIL(reltuples /
                        FLOOR((bs - pageopqdata - pagehdr) * fillfactor / (100 * (4 + nulldatahdrwidth) :: FLOAT))),
               0) AS est_pages_ff
    FROM step2
  ),
  step4 AS (
    SELECT *,
      bs * (relpages) :: BIGINT AS real_size,
      bs * (relpages - est_pages) :: BIGINT AS extra_size,
      CASE WHEN relpages > est_pages_ff THEN bs * (relpages - est_pages_ff) ELSE 0 END AS bloat_size
    FROM
      step3
  ),
  step5 AS (
    SELECT
      s.bs,
      s.relpages,
      REGEXP_REPLACE(s.index_name, '^(.*?_chunk_|(?:[0-9]+_)+)', '') AS indexrelname,
      COALESCE(ch.hypertable_schema, s.schema_name) AS schema_name,
      COALESCE(ch.hypertable_name, s.table_name) AS table_name,
      s.real_size,
      s.extra_size,
      s.bloat_size,
      s.fillfactor,
      CASE s.is_na WHEN TRUE THEN 'TRUE' ELSE '' END AS is_na
    from step4 s
         LEFT JOIN timescaledb_information.chunks ch
        ON ch.chunk_schema = s.schema_name AND ch.chunk_name = s.table_name
  ),
  step6 AS (
    SELECT
      indexrelname,
      schema_name,
      table_name,
      SUM(real_size) AS real_size,
      SUM(extra_size) AS extra_size,
      SUM(bloat_size) AS bloat_size,
      SUM(relpages) AS relpages,
      fillfactor,
      is_na,
      bs
    FROM step5
    GROUP BY indexrelname, schema_name, table_name, fillfactor, is_na, bs
  )
SELECT
  indexrelname,
  schema_name,
  table_name,
  real_size AS real_size_bytes,
  100 * extra_size:: FLOAT / (bs * relpages) AS extra_ratio_percent,
  extra_size AS extra_size_bytes,
  bloat_size AS bloat_size_bytes,
  100 * bloat_size:: FLOAT / (bs * relpages) AS bloat_ratio_percent,
  CASE WHEN bloat_size :: NUMERIC >= 0 AND (real_size - bloat_size) :: NUMERIC >= 0
         THEN real_size :: NUMERIC / (real_size - bloat_size) :: NUMERIC
       END AS bloat_ratio_factor,
  fillfactor,
  is_na
FROM
  step6
WHERE
  bloat_size > 0
ORDER BY
  bloat_ratio_percent DESC
