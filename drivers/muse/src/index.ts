#!/usr/bin/env node
import readline from 'readline';
import fs from 'fs';
import { Command } from 'commander';
import { Driver } from './driver.js';
import { InboundMessage, emitEvent } from './protocol.js';
import { DriverConfig } from './types.js';
import { logger } from './logger.js';

function parseCliConfig(): { driverConfig: DriverConfig; isLogin: boolean; isCleanFiles: boolean } {
  const program = new Command()
    .name('muse-driver')
    .description('Universal Playwright driver for Meta Muse in llm-gateway')
    .option('--login', 'Interactive browser login session')
    .option('--clean-files', 'Clean cloud/VM system files inside Muse workspace')
    .option('--cdp-host <host>', 'Host of already running browser with remote debugging')
    .option('--cdp-port <port>', 'Port of already running browser', (v) => parseInt(v, 10))
    .option('--cdp-url <url>', 'Alias for --cdp-endpoint')
    .option('--cdp-endpoint <url>', 'Full CDP WebSocket or HTTP endpoint URL')
    .option('--user-data-dir <path>', 'Custom browser profile data directory')
    .option('--browser <type>', 'Browser engine (chromium, firefox, webkit)')
    .option('--headless [boolean]', 'Run browser in headless mode', (v) => v !== 'false')
    .option('--timeout <ms>', 'Default timeout in milliseconds', (v) => parseInt(v, 10))
    .option('--config <path>', 'Path to JSON configuration file')
    .allowUnknownOption(true);

  program.parse(process.argv);
  const opts = program.opts();

  let fileConfig: Partial<DriverConfig> = {};
  if (opts.config && fs.existsSync(opts.config)) {
    try {
      fileConfig = JSON.parse(fs.readFileSync(opts.config, 'utf-8'));
    } catch (err: any) {
      logger.warn(`Failed to read config file ${opts.config}: ${err.message}`);
    }
  }

  const cdpEndpoint = opts.cdpEndpoint || opts.cdpUrl;

  const driverConfig: DriverConfig = {
    ...fileConfig,
    ...(opts.cdpHost && { cdpHost: opts.cdpHost }),
    ...(opts.cdpPort && { cdpPort: opts.cdpPort }),
    ...(cdpEndpoint && { cdpEndpoint }),
    ...(opts.userDataDir && { userDataDir: opts.userDataDir }),
    ...(opts.browser && { browserType: opts.browser }),
    ...(opts.headless !== undefined && { headless: opts.headless }),
    ...(opts.timeout && { timeoutMs: opts.timeout }),
  };

  return {
    driverConfig,
    isLogin: Boolean(opts.login),
    isCleanFiles: Boolean(opts.cleanFiles),
  };
}

async function main() {
  const { driverConfig, isLogin, isCleanFiles } = parseCliConfig();
  const driver = new Driver(driverConfig);

  // Clean shutdown handler
  const shutdown = async () => {
    try {
      await driver.close();
    } catch {
      // Ignore errors on shutdown
    }
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  // Standalone CLI command: Clean cloud/VM system files
  if (isCleanFiles) {
    try {
      await driver.cleanFiles();
      process.exit(0);
    } catch (err: any) {
      logger.error(`Failed to clean system files: ${err.message}`);
      process.exit(1);
    }
  }

  // Standalone CLI command: Interactive login
  if (isLogin) {
    try {
      await driver.runInteractiveLogin();
      process.exit(0);
    } catch (err: any) {
      logger.error(`Login failed: ${err.message}`);
      process.exit(1);
    }
  }

  // Gateway Driver mode: stdin/stdout NDJSON protocol
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: false,
  });

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    let message: InboundMessage;
    try {
      message = JSON.parse(trimmed);
    } catch (err: any) {
      emitEvent({ type: 'error', error: `Malformed inbound message: ${err.message}` });
      continue;
    }

    try {
      switch (message.type) {
        case 'start': {
          const threadId = await driver.start(message.resume_id, message.system);
          emitEvent({ type: 'ready', resume_id: threadId });
          break;
        }

        case 'prompt': {
          await driver.prompt(message.text);
          break;
        }

        case 'tool_result': {
          const followUp = `Tool Result [${message.id}]:\n${message.content}`;
          await driver.prompt(followUp);
          break;
        }

        default: {
          emitEvent({ type: 'error', error: `Unknown inbound message type: ${(message as any).type}` });
          break;
        }
      }
    } catch (err: any) {
      emitEvent({ type: 'error', error: err.message || String(err) });
    }
  }
}

main().catch((err) => {
  logger.error(`Fatal driver error: ${err.stack || err}`);
  process.exit(1);
});
