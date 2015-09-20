

/** tables **/
CREATE TABLE IF NOT EXISTS OGN_MAX_RANGE(	
	date INTEGER NOT NULL,
	receiver_name VARCHAR(9) NOT NULL,
	range DECIMAL(3,2) NOT NULL,
	timestamp INTEGER NOT NULL,
	aircraft_id VARCHAR(9) NOT NULL,
	aircraft_reg VARCHAR(9),
	alt DECIMAL(4,1) NOT NULL,
	PRIMARY KEY (date,receiver_name)
);


CREATE TABLE IF NOT EXISTS OGN_STATS(     
    date INTEGER NOT NULL,
    online_receivers INTEGER NOT NULL DEFAULT 0,    
    PRIMARY KEY (date)
);

CREATE TABLE IF NOT EXISTS OGN_RECEIVER(	
	date INTEGER NOT NULL,
	receiver_name VARCHAR(9) NOT NULL,
	beacons_received INTEGER DEFAULT 0,
	max_alt DECIMAL(5,1),
	PRIMARY KEY (date,receiver_name)
);

/** views **/
CREATE VIEW IF NOT EXISTS OGN_STATS_V 
AS SELECT date(date/1000,'unixepoch','localtime') as date, online_receivers 
FROM OGN_STATS;

CREATE VIEW IF NOT EXISTS OGN_MAX_RANGE_V 
AS SELECT 
  date(date/1000,'unixepoch','localtime') as date, receiver_name, round(range,2) as range, timestamp, aircraft_id, round(alt,2) as alt, aircraft_reg
FROM OGN_MAX_RANGE;	

CREATE VIEW IF NOT EXISTS OGN_RECEIVER_V 
AS SELECT date(date/1000,'unixepoch','localtime') as date, receiver_name, beacons_received, max_alt
FROM OGN_RECEIVER;