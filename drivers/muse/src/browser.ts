import { Browser, BrowserContext, Page, chromium, firefox, webkit, BrowserType } from 'playwright';
import path from 'path';
import os from 'os';
import fs from 'fs';
import { DriverConfig } from './types.js';
import { logger } from './logger.js';

export class BrowserManager {
  private browser: Browser | null = null;
  private context: BrowserContext | null = null;
  private page: Page | null = null;
  private isCdp: boolean = false;
  private readonly config: DriverConfig & {
    browserType: 'chromium' | 'firefox' | 'webkit';
    userDataDir: string;
    storageStatePath: string;
    headless: boolean;
    timeoutMs: number;
  };

  constructor(config?: DriverConfig) {
    const baseConfigDir = path.join(os.homedir(), '.config', 'llm-gateway');
    const defaultDataDir = path.join(baseConfigDir, 'muse-browser-profile');
    const defaultStorageState = path.join(baseConfigDir, 'muse_auth.json');

    let cdpEndpoint = config?.cdpEndpoint || process.env.MUSE_CDP_ENDPOINT;
    const cdpHost = config?.cdpHost || process.env.MUSE_CDP_HOST;
    const cdpPort = config?.cdpPort || (process.env.MUSE_CDP_PORT ? parseInt(process.env.MUSE_CDP_PORT, 10) : undefined);

    if (!cdpEndpoint && cdpHost) {
      const port = cdpPort || 9222;
      const host = cdpHost.includes('://') ? cdpHost : `http://${cdpHost}`;
      cdpEndpoint = host.endsWith(`:${port}`) ? host : `${host}:${port}`;
    }

    this.config = {
      ...config,
      cdpEndpoint,
      cdpHost,
      cdpPort,
      browserType: config?.browserType || (process.env.MUSE_BROWSER as any) || 'chromium',
      userDataDir: config?.userDataDir || process.env.MUSE_USER_DATA_DIR || defaultDataDir,
      storageStatePath: config?.storageStatePath || process.env.MUSE_STORAGE_STATE || defaultStorageState,
      headless: config?.headless !== undefined ? config.headless : process.env.MUSE_HEADLESS !== 'false',
      timeoutMs: config?.timeoutMs || 60000,
    };
  }

  isCDP(): boolean {
    return Boolean(this.config.cdpEndpoint);
  }

  getConfig(): DriverConfig & {
    browserType: 'chromium' | 'firefox' | 'webkit';
    userDataDir: string;
    storageStatePath: string;
    headless: boolean;
    timeoutMs: number;
  } {
    return this.config;
  }

  setHeadless(headless: boolean): void {
    this.config.headless = headless;
  }

  private getLauncher(): BrowserType {
    switch (this.config.browserType) {
      case 'firefox':
        return firefox;
      case 'webkit':
        return webkit;
      case 'chromium':
      default:
        return chromium;
    }
  }

  /**
   * Initializes browser connection or launches persistent context.
   * If cdpEndpoint / cdpHost is provided, connects to the existing browser.
   */
  async ensurePage(): Promise<Page> {
    if (this.context && this.page && !this.page.isClosed()) {
      return this.page;
    }

    if (this.config.cdpEndpoint) {
      this.isCdp = true;
      logger.info(`Connecting to running browser via CDP at ${this.config.cdpEndpoint}...`);

      this.browser = await chromium.connectOverCDP(this.config.cdpEndpoint, {
        timeout: this.config.timeoutMs,
      });

      const contexts = this.browser.contexts();
      this.context = contexts.length > 0 ? contexts[0] : await this.browser.newContext();

      const pages = this.context.pages();
      // Reuse existing muse.ai tab if present
      const existingMuse = pages.find((p) => !p.isClosed() && p.url().includes('muse.ai'));
      if (existingMuse) {
        logger.info(`Attached to existing muse.ai tab: ${existingMuse.url()}`);
        this.page = existingMuse;
      } else {
        const blankPage = pages.find((p) => !p.isClosed() && p.url() === 'about:blank');
        if (blankPage) {
          this.page = blankPage;
        } else {
          logger.info('Opening new tab for muse.ai...');
          this.page = await this.context.newPage();
        }
      }

      this.page.setDefaultTimeout(this.config.timeoutMs);
      return this.page;
    }

    fs.mkdirSync(this.config.userDataDir, { recursive: true });
    const launcher = this.getLauncher();
    logger.info(`Launching ${this.config.browserType} (headless: ${this.config.headless})...`);

    const launchArgs = [
      '--disable-blink-features=AutomationControlled',
    ];

    this.context = await launcher.launchPersistentContext(this.config.userDataDir, {
      headless: this.config.headless,
      viewport: { width: 1280, height: 800 },
      args: launchArgs,
    });

    const pages = this.context.pages();
    this.page = pages.length > 0 ? pages[0] : await this.context.newPage();
    this.page.setDefaultTimeout(this.config.timeoutMs);

    return this.page;
  }

  getContext(): BrowserContext | null {
    return this.context;
  }

  getPage(): Page | null {
    return this.page;
  }

  async close(): Promise<void> {
    if (this.isCdp) {
      if (this.browser) {
        try {
          // Playwright: browser.close() on connectOverCDP simply disconnects transport
          // and preserves the user's running browser process and tabs.
          await this.browser.close();
        } catch (err: any) {
          logger.debug(`CDP disconnect error: ${err.message}`);
        }
        this.browser = null;
      }
      this.context = null;
      this.page = null;
      return;
    }

    if (this.context) {
      try {
        await this.context.close();
      } catch (err: any) {
        logger.debug(`Context close error: ${err.message}`);
      }
      this.context = null;
      this.page = null;
    }
  }
}
