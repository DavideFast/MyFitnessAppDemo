CREATE DATABASE IF NOT EXISTS bigintensive;
USE bigintensive;

-- Running samples
CREATE TABLE IF NOT EXISTS running_samples_local (
  sample_id UInt64,
  athlete_id UInt64,
  session_id UInt64,
  timestamp DateTime DEFAULT now(),
  heart_rate UInt8,
  velocity DECIMAL(5, 2) DEFAULT 0.0,
  latitude DECIMAL(9, 6),
  longitude DECIMAL(9, 6),
  altitude DECIMAL(5, 2),
  temperature DECIMAL(4, 2),
  cadence UInt8,
  event_type String,
  created_at DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/bigintensive/running_samples', '{replica}')
ORDER BY (timestamp, athlete_id, session_id)
PARTITION BY toYYYYMM(timestamp);

CREATE TABLE IF NOT EXISTS running_samples (
  sample_id UInt64,
  athlete_id UInt64,
  session_id UInt64,
  timestamp DateTime DEFAULT now(),
  heart_rate UInt8,
  velocity DECIMAL(5, 2) DEFAULT 0.0,
  latitude DECIMAL(9, 6),
  longitude DECIMAL(9, 6),
  altitude DECIMAL(5, 2),
  temperature DECIMAL(4, 2),
  cadence UInt8,
  event_type String,
  created_at DateTime DEFAULT now()
) ENGINE = Distributed(bigintensive_cluster, bigintensive, running_samples_local, cityHash64(athlete_id));


-- Workout sessions
CREATE TABLE IF NOT EXISTS allenamenti_local (
    allenamento_id UInt64,
    athlete_id UInt64 ,
    data_allenamento DateTime NOT NULL,
    nome_esercizio String NOT NULL,
    serie_allenamento UInt8 NOT NULL,
    ripetizioni_allenamento UInt8 NOT NULL,
    recupero_allenamento UInt8 NOT NULL,
    peso_allenamento DECIMAL(5, 2),
    created_at DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/bigintensive/allenamenti', '{replica}')
ORDER BY (data_allenamento, athlete_id, allenamento_id)
PARTITION BY toYYYYMM(data_allenamento);

CREATE TABLE IF NOT EXISTS allenamenti (
  allenamento_id UInt64,
  athlete_id UInt64,
  data_allenamento DateTime NOT NULL,
  nome_esercizio String NOT NULL,
  serie_allenamento UInt8 NOT NULL,
  ripetizioni_allenamento UInt8 NOT NULL,
  recupero_allenamento UInt8 NOT NULL,
  peso_allenamento DECIMAL(5, 2),
  created_at DateTime DEFAULT now()
) ENGINE = Distributed(bigintensive_cluster, bigintensive, allenamenti_local, cityHash64(athlete_id));

-- Local staging table: the distributed table with the public name is defined below.
CREATE TABLE IF NOT EXISTS allenamenti_raw_local
(
    allenamento_id UInt64,
    athlete_id UInt64,
    data_allenamento DateTime,
    struttura_allenamento String,
    created_at DateTime
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/bigintensive/allenamenti_raw', '{replica}')
ORDER BY allenamento_id
PARTITION BY toYYYYMM(data_allenamento);

CREATE TABLE IF NOT EXISTS allenamenti_raw (
  allenamento_id UInt64,
  athlete_id UInt64,
  data_allenamento DateTime,
  struttura_allenamento String,
  created_at DateTime
) ENGINE = Distributed(bigintensive_cluster, bigintensive, allenamenti_raw_local, cityHash64(athlete_id));
