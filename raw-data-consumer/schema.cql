-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS plant_monitoring
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE plant_monitoring;

-- Uncomment after change scheme
-- DROP TABLE sensor_data;
-- DROP TABLE weather_data;


-- Sensor readings table (time-series data)
CREATE TABLE IF NOT EXISTS sensor_data (
    device_id text,
    timestamp timestamp,
    temperature double,
    humidity double,
    soil_moisture double,
    light_level double,
    status text,
    duration double,
    PRIMARY KEY (device_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
AND default_time_to_live = 2592000;  -- 30 days TTL

-- Weather data table
CREATE TABLE IF NOT EXISTS weather_data (
    city_id int,
    timestamp timestamp,
    city_name text,
    latitude double,
    longitude double,
    temperature double,
    weather_condition text,
    weather_description text,
    PRIMARY KEY (city_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
AND default_time_to_live = 2592000;  -- 30 days TTL

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS ON sensor_data (timestamp);
CREATE INDEX IF NOT EXISTS ON weather_data (timestamp);
