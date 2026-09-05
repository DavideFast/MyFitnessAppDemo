export function createOriginChecker(corsOriginRaw) {
  const explicitAllowedOrigins = String(corsOriginRaw || "http://localhost:5173")
    .split(",")
    .map((origin) => origin.trim())
    .filter(Boolean);

  return function isAllowedOrigin(origin) {
    if (!origin) {
      return true;
    }

    if (explicitAllowedOrigins.includes(origin)) {
      return true;
    }

    try {
      const parsed = new URL(origin);
      return parsed.hostname === "localhost" || parsed.hostname === "127.0.0.1";
    } catch {
      return false;
    }
  };
}
