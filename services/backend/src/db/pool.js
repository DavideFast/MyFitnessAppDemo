import pg from "pg";
import { createClient } from "@clickhouse/client";

export function createDbPool() {
  return new pg.Pool({
    user: process.env.POSTGRES_USER || "postgres",
    password: process.env.POSTGRES_PASSWORD || "postgres",
    host: process.env.POSTGRES_HOST || "postgres",
    port: process.env.POSTGRES_PORT || 5432,
    database: process.env.POSTGRES_DB || "bigintensive",
  });
}

const clickhouseHost = process.env.CLICKHOUSE_HOST || "clickhouse";
const clickhousePort = process.env.CLICKHOUSE_PORT || "8123";
const clickhouseUrl = /^https?:\/\//i.test(clickhouseHost) ? clickhouseHost : `http://${clickhouseHost}:${clickhousePort}`;

export const createClickhouseClient = createClient({
  url: clickhouseUrl,
  username: process.env.CLICKHOUSE_USER || "default",
  password: process.env.CLICKHOUSE_PASSWORD || "",
  database: process.env.CLICKHOUSE_DB || "bigintensive",
});
