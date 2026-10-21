/**
 * Logger for external driver.
 * IMPORTANT: Driver stdout is strictly reserved for NDJSON protocol messages.
 * All diagnostic and debug logs must be written to stderr.
 */
export const logger = {
  info(message: string): void {
    process.stderr.write(`[muse-driver] [INFO] ${message}\n`);
  },
  warn(message: string): void {
    process.stderr.write(`[muse-driver] [WARN] ${message}\n`);
  },
  error(message: string): void {
    process.stderr.write(`[muse-driver] [ERROR] ${message}\n`);
  },
  debug(message: string): void {
    if (process.env.DEBUG || process.env.MUSE_DEBUG) {
      process.stderr.write(`[muse-driver] [DEBUG] ${message}\n`);
    }
  },
};
