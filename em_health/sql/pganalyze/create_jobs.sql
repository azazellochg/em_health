SELECT add_job('pganalyze.get_stat_statements', schedule_interval=>'1 min'::INTERVAL);

SELECT add_job('pganalyze.parse_logs', schedule_interval=>'1 min'::INTERVAL);

SELECT add_job('pganalyze.parse_sysinfo', schedule_interval=>'10 min'::INTERVAL);

SELECT add_job('pganalyze.get_db_stats', schedule_interval=>'10 min'::INTERVAL);

SELECT add_job('pganalyze.get_table_stats', schedule_interval=>'10 min'::INTERVAL);

SELECT add_job('pganalyze.get_index_stats', schedule_interval=>'10 min'::INTERVAL);

SELECT add_job('pganalyze.purge_stats', schedule_interval=>'1 day'::INTERVAL, config => '{"drop_after":"3 months"}');
