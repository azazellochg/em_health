/* Create a materialized view of EPU-D acquisition runs */
CREATE MATERIALIZED VIEW IF NOT EXISTS events.epud_runs AS
    WITH state_param AS (
        SELECT instrument_id, param_id, enum_id
        FROM events.parameters
        WHERE
            param_name = 'AutomatedAcquisitionState'
            AND subsystem = 'EPU-D'
    ), state_enum AS (
        SELECT p.instrument_id, p.param_id, e.value AS started_value
        FROM
            state_param p
            JOIN events.enum_values e ON e.enum_id = p.enum_id
        WHERE e.member_name = 'Started'
    ), runs AS (
        SELECT sp.instrument_id, sp.param_id, st.state, st.start_time, st.end_time
        FROM
            state_param sp
            CROSS JOIN LATERAL (
                SELECT state, start_time, end_time
                FROM
                    state_int_timeline((
                        SELECT state_agg(d.time, d.value_num::bigint)
                        FROM events.data d
                        WHERE
                            d.instrument_id = sp.instrument_id
                            AND d.param_id = sp.param_id
                    ))
            ) st
    )
    SELECT
        r.instrument_id, r.start_time, r.end_time,
        r.end_time - r.start_time AS total_duration
    FROM
        runs r
        JOIN state_enum se USING (instrument_id, param_id)
    WHERE
        r.state = se.started_value
        AND (r.end_time - r.start_time) > '0 second'::interval
    ORDER BY r.instrument_id, r.param_id, r.start_time
