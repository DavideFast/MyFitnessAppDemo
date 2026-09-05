import { createDbPool } from "../db/pool.js";
import { createOriginChecker } from "../middleware/corsOrigin.js";

export function createServerContext() {
  return {
    port: Number(process.env.PORT || 3001),
    isAllowedOrigin: createOriginChecker(process.env.CORS_ORIGIN || "http://localhost:5173"),
    pool: createDbPool(),
  };
}
