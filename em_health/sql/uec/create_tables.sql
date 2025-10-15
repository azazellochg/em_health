CREATE SCHEMA IF NOT EXISTS uec;

-- Creating uec.device_type
CREATE TABLE IF NOT EXISTS uec.device_type (
                                               DeviceTypeID INTEGER PRIMARY KEY,
                                               IdentifyingName TEXT NOT NULL UNIQUE
);

-- Creating uec.device_instance
CREATE TABLE IF NOT EXISTS uec.device_instance (
                                                   DeviceInstanceID INTEGER NOT NULL,
                                                   DeviceTypeID INTEGER NOT NULL REFERENCES uec.device_type(DeviceTypeID),
                                                   IdentifyingName TEXT NOT NULL,
                                                   PRIMARY KEY (DeviceInstanceID, DeviceTypeID),
                                                   UNIQUE (DeviceTypeID, IdentifyingName)
);

-- Creating uec.error_code
CREATE TABLE IF NOT EXISTS uec.error_code (
                                              DeviceTypeID INTEGER NOT NULL REFERENCES uec.device_type(DeviceTypeID),
                                              ErrorCodeID INTEGER NOT NULL,
                                              IdentifyingName TEXT NOT NULL,
                                              PRIMARY KEY (DeviceTypeID, ErrorCodeID)
);

-- Creating uec.subsystem
CREATE TABLE IF NOT EXISTS uec.subsystem (
                                             SubsystemID INTEGER PRIMARY KEY,
                                             IdentifyingName TEXT NOT NULL UNIQUE
);

-- Creating uec.error_definitions
CREATE TABLE IF NOT EXISTS uec.error_definitions (
                                                     ErrorDefinitionID INTEGER PRIMARY KEY,
                                                     SubsystemID INTEGER NOT NULL REFERENCES uec.subsystem(SubsystemID),
                                                     DeviceTypeID INTEGER NOT NULL REFERENCES uec.device_type(DeviceTypeID),
                                                     ErrorCodeID INTEGER NOT NULL,
                                                     DeviceInstanceID INTEGER NOT NULL,
                                                     UNIQUE (ErrorCodeID, SubsystemID, DeviceTypeID, DeviceInstanceID),
                                                     CONSTRAINT fk_error_definitions_device_instance
                                                         FOREIGN KEY (DeviceInstanceID, DeviceTypeID)
                                                             REFERENCES uec.device_instance(DeviceInstanceID, DeviceTypeID),
                                                     CONSTRAINT fk_error_definitions_error_code
                                                         FOREIGN KEY (DeviceTypeID, ErrorCodeID)
                                                             REFERENCES uec.error_code(DeviceTypeID, ErrorCodeID)
);

-- Creating uec.errors
CREATE TABLE IF NOT EXISTS uec.errors (
                                          Time TIMESTAMPTZ NOT NULL,
                                          InstrumentID INTEGER NOT NULL REFERENCES public.instruments(id) ON DELETE CASCADE,
                                          ErrorID INTEGER NOT NULL REFERENCES uec.error_definitions(ErrorDefinitionID) ON DELETE CASCADE,
                                          MessageText TEXT,
                                          UNIQUE (Time, InstrumentID, ErrorID)
);

GRANT USAGE ON SCHEMA uec TO grafana, emhealth;
GRANT SELECT ON ALL TABLES IN SCHEMA uec TO grafana;
ALTER DEFAULT PRIVILEGES IN SCHEMA uec GRANT SELECT ON TABLES TO grafana;

GRANT SELECT, DELETE ON ALL TABLES IN SCHEMA uec TO emhealth;
ALTER DEFAULT PRIVILEGES IN SCHEMA uec GRANT SELECT, DELETE ON TABLES TO emhealth;
