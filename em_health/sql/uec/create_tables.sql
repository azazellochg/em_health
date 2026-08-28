-- Creating uec.device_type
CREATE TABLE IF NOT EXISTS uec.device_type (
  devicetypeid INT PRIMARY KEY,
  identifyingname TEXT NOT NULL UNIQUE
);

-- Creating uec.device_instance
CREATE TABLE IF NOT EXISTS uec.device_instance (
  deviceinstanceid INT NOT NULL,
  devicetypeid INT NOT NULL REFERENCES uec.device_type (devicetypeid),
  identifyingname TEXT NOT NULL,
  PRIMARY KEY (deviceinstanceid, devicetypeid),
  UNIQUE (devicetypeid, identifyingname)
);

-- Creating uec.error_code
CREATE TABLE IF NOT EXISTS uec.error_code (
  devicetypeid INT NOT NULL REFERENCES uec.device_type (devicetypeid),
  errorcodeid INT NOT NULL,
  identifyingname TEXT NOT NULL,
  PRIMARY KEY (devicetypeid, errorcodeid)
);

-- Creating uec.subsystem
CREATE TABLE IF NOT EXISTS uec.subsystem (
  subsystemid INT PRIMARY KEY,
  identifyingname TEXT NOT NULL UNIQUE
);

-- Creating uec.error_definitions
CREATE TABLE IF NOT EXISTS uec.error_definitions (
  errordefinitionid INT PRIMARY KEY,
  subsystemid INT NOT NULL REFERENCES uec.subsystem (subsystemid),
  devicetypeid INT NOT NULL REFERENCES uec.device_type (devicetypeid),
  errorcodeid INT NOT NULL,
  deviceinstanceid INT NOT NULL,
  UNIQUE (errorcodeid, subsystemid, devicetypeid, deviceinstanceid),
  CONSTRAINT fk_error_definitions_device_instance FOREIGN KEY (deviceinstanceid, devicetypeid) REFERENCES uec.device_instance (deviceinstanceid, devicetypeid),
  CONSTRAINT fk_error_definitions_error_code FOREIGN KEY (devicetypeid, errorcodeid) REFERENCES uec.error_code (devicetypeid, errorcodeid)
);

-- Creating uec.errors
CREATE TABLE IF NOT EXISTS uec.errors (
  time timestamptz NOT NULL,
  instrument_id INT NOT NULL REFERENCES events.instruments (id) ON DELETE CASCADE,
  errorid INT NOT NULL REFERENCES uec.error_definitions (errordefinitionid) ON DELETE CASCADE,
  messagetext TEXT,
  UNIQUE (time, instrument_id, errorid)
);
