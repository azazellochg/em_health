CREATE OR REPLACE FUNCTION events.purge_old_chunks(
    p_hypertable_name text,
    p_retain_days int,
    OUT chunks_dropped int
)
RETURNS int
LANGUAGE plpgsql
AS $$
DECLARE
    v_interval interval;
BEGIN
    chunks_dropped := 0;

    IF p_retain_days <= 0 THEN RETURN;
    END IF;

    v_interval := p_retain_days * INTERVAL '1 day';

    SELECT COUNT(*)
    INTO chunks_dropped
    FROM drop_chunks(
        p_hypertable_name,
        older_than => v_interval
    );

    RAISE NOTICE '% chunks older than % days dropped from %s', chunks_dropped, p_retain_days, p_hypertable_name;

    RETURN;
END;
$$;