CREATE SCHEMA IF NOT EXISTS uec
;

-- Creating uec.device_type
CREATE TABLE IF NOT EXISTS uec.device_type (
    devicetypeid int PRIMARY KEY,
    identifyingname text NOT NULL UNIQUE
)
;

-- Creating uec.device_instance
CREATE TABLE IF NOT EXISTS uec.device_instance (
    deviceinstanceid int NOT NULL,
    devicetypeid int NOT NULL REFERENCES uec.device_type (devicetypeid),
    identifyingname text NOT NULL,
    PRIMARY KEY (deviceinstanceid, devicetypeid),
    UNIQUE (devicetypeid, identifyingname)
)
;

-- Creating uec.error_code
CREATE TABLE IF NOT EXISTS uec.error_code (
    devicetypeid int NOT NULL REFERENCES uec.device_type (devicetypeid),
    errorcodeid int NOT NULL,
    identifyingname text NOT NULL,
    PRIMARY KEY (devicetypeid, errorcodeid)
)
;

-- Creating uec.subsystem
CREATE TABLE IF NOT EXISTS uec.subsystem (
    subsystemid int PRIMARY KEY,
    identifyingname text NOT NULL UNIQUE
)
;

-- Creating uec.error_definitions
CREATE TABLE IF NOT EXISTS uec.error_definitions (
    errordefinitionid int PRIMARY KEY,
    subsystemid int NOT NULL REFERENCES uec.subsystem (subsystemid),
    devicetypeid int NOT NULL REFERENCES uec.device_type (devicetypeid),
    errorcodeid int NOT NULL,
    deviceinstanceid int NOT NULL,
    UNIQUE (errorcodeid, subsystemid, devicetypeid, deviceinstanceid),
    CONSTRAINT fk_error_definitions_device_instance FOREIGN KEY (deviceinstanceid, devicetypeid) REFERENCES uec.device_instance (deviceinstanceid, devicetypeid),
    CONSTRAINT fk_error_definitions_error_code FOREIGN KEY (devicetypeid, errorcodeid) REFERENCES uec.error_code (devicetypeid, errorcodeid)
)
;

-- Creating uec.errors
CREATE TABLE IF NOT EXISTS uec.errors (
    time timestamptz NOT NULL,
    instrument_id int NOT NULL REFERENCES public.instruments (id) ON DELETE CASCADE,
    errorid int NOT NULL REFERENCES uec.error_definitions (errordefinitionid) ON DELETE CASCADE,
    messagetext text,
    UNIQUE (time, instrument_id, errorid)
)
; GRANT usage ON SCHEMA uec TO grafana, emhealth
; GRANT select ON ALL TABLES IN SCHEMA uec TO grafana
; ALTER DEFAULT PRIVILEGES IN SCHEMA uec GRANT select ON TABLES TO grafana
; GRANT select, delete ON ALL TABLES IN SCHEMA uec TO emhealth
; ALTER DEFAULT PRIVILEGES IN SCHEMA uec GRANT select, delete ON TABLES TO emhealth
