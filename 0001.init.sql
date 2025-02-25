CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- append only table
CREATE TABLE execution_log (
	id UUID NOT NULL DEFAULT gen_random_uuid(), 
	done BOOLEAN,	
	inflight BOOLEAN,
	version TEXT,
	worker TEXT,
	time TIMESTAMP DEFAULT now(),
	defer TIMESTAMP DEFAULT NULL,
	data JSONB
);

-- ? NULL time for current ala some scd setups instead?
CREATE VIEW execution_state AS (
	SELECT id, done, inflight, version, worker, time, defer, data
	FROM (SELECT id, done, inflight, version, worker, time, defer, data, row_number()
	      OVER (PARTITION BY id ORDER BY time DESC) AS rn
	FROM execution_log) AS t
	WHERE rn = 1
);

