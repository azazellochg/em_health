/* Create a materialized view of estimated heap/table bloat.
   pg_catalog and timescale cache schemas are ignored, as well as compressed chunks from hypertables.
   Minimum relation size (in 8 KiB pages) to analyze is based on the total number of objects in the database (0 / 250 / 2500).
   If is_na = true, do not trust the stats
   This SQL is adapted from https://gitlab.com/postgres-ai/postgres-checkup under AGPLv3 license
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS pganalyze.table_bloat AS
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
    step1 AS (
      SELECT
        tbl.oid AS tblid,
        ns.nspname AS schema_name,
        tbl.relname AS table_name,
        tbl.reltuples,
        tbl.relpages AS heappages,
        COALESCE(toast.relpages, 0) AS toastpages,
        COALESCE(toast.reltuples, 0) AS toasttuples,
        COALESCE(SUBSTRING(ARRAY_TO_STRING(tbl.reloptions, ' ') FROM '%fillfactor=#"__#"%' FOR '#') :: int2,
                 100) AS fillfactor,
        CURRENT_SETTING('block_size') :: NUMERIC AS bs,
        CASE WHEN VERSION() ~ 'mingw32|64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END AS ma,
        24 AS page_hdr,
        23 + CASE WHEN MAX(COALESCE(s.null_frac, 0)) > 0 THEN (7 + COUNT(s.attname)) / 8 ELSE 0 :: INT END +
        CASE WHEN BOOL_OR(att.attname = 'oid' AND att.attnum < 0) THEN 4 ELSE 0 END AS tpl_hdr_size,
        SUM((1 - COALESCE(s.null_frac, 0)) * COALESCE(s.avg_width, 0)) AS tpl_data_size,
        BOOL_OR(att.atttypid = 'pg_catalog.name' :: regtype) OR
        SUM(CASE WHEN att.attnum > 0 THEN 1 ELSE 0 END) <> COUNT(s.attname) AS is_na
      FROM
        pg_attribute AS att
        JOIN pg_class AS tbl
          ON att.attrelid = tbl.oid AND tbl.relkind IN ('r', 'm')
        JOIN pg_namespace AS ns
          ON ns.oid = tbl.relnamespace
        JOIN pg_stats AS s
          ON s.schemaname = ns.nspname AND s.tablename = tbl.relname AND NOT s.inherited AND s.attname = att.attname
        LEFT JOIN pg_class AS toast
          ON tbl.reltoastrelid = toast.oid
        CROSS JOIN settings st
      WHERE
        NOT att.attisdropped
        AND s.schemaname NOT SIMILAR TO '(pg_catalog|information_schema|_timescaledb_c%)'
        AND s.tablename NOT SIMILAR TO '(%compress%|bgw%)'
        AND tbl.relpages > st.default_min_relpages
        AND NOT EXISTS (
          SELECT
            1
          FROM
            timescaledb_information.chunks ch
          WHERE
            ch.chunk_schema = ns.nspname
            AND ch.chunk_name = tbl.relname
            AND ch.is_compressed = TRUE
        )
      GROUP BY 1,2,3,4,5,6,7,8,9,10
    ),
    step2 AS (
      SELECT *,
        (4 + tpl_hdr_size + tpl_data_size + (2 * ma) -
         CASE WHEN tpl_hdr_size % ma = 0 THEN ma ELSE tpl_hdr_size % ma END -
         CASE WHEN CEIL(tpl_data_size) :: INT % ma = 0 THEN ma ELSE CEIL(tpl_data_size) :: INT % ma END) AS tpl_size,
        bs - page_hdr AS size_per_block,
        (heappages + toastpages) AS tblpages
      FROM
        step1
    ),
    step3 AS (
      SELECT *,
        CEIL(reltuples / ((bs - page_hdr) / tpl_size)) + CEIL(toasttuples / 4) AS est_tblpages,
        CEIL(reltuples / ((bs - page_hdr) * fillfactor / (tpl_size * 100))) + CEIL(toasttuples / 4) AS est_tblpages_ff
      FROM
        step2
    ),
    step4 AS (
      SELECT *,
        tblpages * bs AS real_size,
        (tblpages - est_tblpages) * bs AS extra_size,
        CASE WHEN tblpages - est_tblpages_ff > 0 THEN (tblpages - est_tblpages_ff) * bs ELSE 0 END AS bloat_size
      FROM
        step3
    ),
    step5 AS (
      SELECT
        s.bs,
        s.tblpages,
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
        schema_name,
        table_name,
        SUM(real_size) AS real_size,
        SUM(extra_size) AS extra_size,
        SUM(bloat_size) AS bloat_size,
        SUM(tblpages) AS tblpages,
        fillfactor,
        is_na,
        bs
      FROM step5
      GROUP BY schema_name, table_name, fillfactor, is_na, bs
    )
  SELECT
    schema_name,
    table_name,
    real_size AS real_size_bytes,
    extra_size AS extra_size_bytes,
    100 * extra_size:: FLOAT / (bs * tblpages) AS extra_ratio_percent,
    bloat_size AS bloat_size_bytes,
    100 * bloat_size:: FLOAT / (bs * tblpages) AS bloat_ratio_percent,
    CASE WHEN real_size :: NUMERIC > 0 AND (real_size - bloat_size) :: NUMERIC > 0
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
