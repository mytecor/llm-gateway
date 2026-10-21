import { BrowserContext, Page } from 'playwright';
import path from 'path';
import fs from 'fs';
import { BrowserManager } from './browser.js';
import { Cleaner } from './cleaner.js';
import { SELECTORS, waitForAnySelector } from './selectors.js';
import { logger } from './logger.js';

export class AuthManager {
  constructor(
    private readonly browserManager: BrowserManager,
    private readonly cleaner: Cleaner = new Cleaner()
  ) {}

  /**
   * Run interactive login session for the user.
   * After login, it clears all system files inside web workspace,
   * while keeping the local browser profile on disk completely intact.
   */
  async runInteractiveLogin(): Promise<void> {
    if (!this.browserManager.isCDP()) {
      this.browserManager.setHeadless(false);
    }
    const page = await this.browserManager.ensurePage();
    const context = this.browserManager.getContext();

    if (!context) {
      throw new Error('Failed to initialize browser context');
    }

    logger.info('Opening https://muse.ai/ for authentication...');
    await page.goto('https://muse.ai/', { waitUntil: 'domcontentloaded' });

    process.stderr.write('\n=======================================================\n');
    process.stderr.write(' [LOGIN] Please log in to your account.                 \n');
    process.stderr.write(' Waiting for interface to be ready...                   \n');
    process.stderr.write('=======================================================\n\n');

    const chatReady = await waitForAnySelector(page, SELECTORS.loginReady, 300000); // 5 min timeout

    if (!chatReady) {
      throw new Error('Authentication timeout: interface did not become ready.');
    }

    logger.info('Authentication confirmed. Local browser profile preserved on disk.');

    // Save storageState backup if configured
    await this.exportStorageState(context);

    // Clean internal cloud/VM system files
    await this.cleaner.clean(page);

    logger.info('Session ready for llm-gateway.');
    await this.browserManager.close();
  }

  /**
   * Asserts that current page is not redirected to a login or auth page.
   */
  checkAuthenticated(page: Page): void {
    const currentUrl = page.url();
    if (currentUrl.includes('login') || currentUrl.includes('auth')) {
      throw new Error('Session is not authenticated. Run `pnpm run login` in driver directory.');
    }
  }

  /**
   * Export storage state JSON as backup.
   */
  async exportStorageState(context: BrowserContext): Promise<void> {
    const storagePath = this.browserManager.getConfig().storageStatePath;
    try {
      const storageDir = path.dirname(storagePath);
      fs.mkdirSync(storageDir, { recursive: true });
      await context.storageState({ path: storagePath });
      logger.debug(`Exported storage state to ${storagePath}`);
    } catch (e: any) {
      logger.warn(`StorageState export: ${e.message}`);
    }
  }
}
