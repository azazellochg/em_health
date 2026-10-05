-- Creating uec.device_type
CREATE TABLE IF NOT EXISTS uec.device_type (
  device_type_id INT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

-- Creating uec.device_instance
CREATE TABLE IF NOT EXISTS uec.device_instance (
  device_instance_id INT NOT NULL,
  device_type_id INT NOT NULL REFERENCES uec.device_type (device_type_id),
  name TEXT NOT NULL,
  PRIMARY KEY (device_instance_id, device_type_id),
  UNIQUE (device_type_id, name)
);

-- Creating uec.error_code
CREATE TABLE IF NOT EXISTS uec.error_code (
  device_type_id INT NOT NULL REFERENCES uec.device_type (device_type_id),
  error_code_id INT NOT NULL,
  name TEXT NOT NULL,
  PRIMARY KEY (device_type_id, error_code_id)
);

-- Creating uec.subsystem
CREATE TABLE IF NOT EXISTS uec.subsystem (
  subsystem_id INT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

-- Creating uec.error_definitions
CREATE TABLE IF NOT EXISTS uec.error_definitions (
  error_definition_id INT PRIMARY KEY,
  subsystem_id INT NOT NULL REFERENCES uec.subsystem (subsystem_id),
  device_type_id INT NOT NULL REFERENCES uec.device_type (device_type_id),
  error_code_id INT NOT NULL,
  device_instance_id INT NOT NULL,
  UNIQUE (error_code_id, subsystem_id, device_type_id, device_instance_id),
  FOREIGN KEY (device_instance_id, device_type_id) REFERENCES uec.device_instance (device_instance_id, device_type_id),
  FOREIGN KEY (device_type_id, error_code_id) REFERENCES uec.error_code (device_type_id, error_code_id)
);

-- Creating uec.errors
CREATE TABLE IF NOT EXISTS uec.errors (
  time timestamptz NOT NULL,
  instrument_id BIGINT NOT NULL REFERENCES events.instruments (id) ON DELETE CASCADE,
  error_id INT NOT NULL REFERENCES uec.error_definitions (error_definition_id) ON DELETE CASCADE,
  message TEXT,
  UNIQUE (instrument_id, error_id, time)
);

COMMENT ON TABLE uec.errors IS 'Main UEC table with alarm events';
