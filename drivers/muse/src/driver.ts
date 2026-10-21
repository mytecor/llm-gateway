import { Page } from 'playwright';
import { DriverConfig } from './types.js';
import { emitEvent } from './protocol.js';
import { BrowserManager } from './browser.js';
import { AuthManager } from './auth.js';
import { Cleaner } from './cleaner.js';
import { ResponseStreamer } from './streamer.js';
import { SELECTORS, findFirstVisible } from './selectors.js';
import { logger } from './logger.js';

export class Driver {
  private readonly browserManager: BrowserManager;
  private readonly authManager: AuthManager;
  private readonly cleaner: Cleaner;
  private readonly streamer: ResponseStreamer;

  private currentThreadId: string | null = null;
  private pendingSystemPrompt: string | null = null;

  constructor(config?: DriverConfig) {
    this.browserManager = new BrowserManager(config);
    this.cleaner = new Cleaner();
    this.authManager = new AuthManager(this.browserManager, this.cleaner);
    this.streamer = new ResponseStreamer();
  }

  /**
   * Initializes or navigates to a conversation thread.
   */
  async start(resumeId?: string, systemPrompt?: string): Promise<string> {
    const page = await this.browserManager.ensurePage();

    const targetUrl = resumeId && resumeId.trim() !== ''
      ? (resumeId.startsWith('http') ? resumeId : `https://muse.ai/thread/${resumeId}`)
      : 'https://muse.ai/thread/new';

    logger.info(`Navigating to ${targetUrl}...`);
    const response = await page.goto(targetUrl, { waitUntil: 'domcontentloaded' });

    if (response && response.status() === 404) {
      emitEvent({ type: 'session_not_found' });
      throw new Error(`Session ${resumeId} not found`);
    }

    this.authManager.checkAuthenticated(page);

    const input = await findFirstVisible(page, SELECTORS.chatInput);
    if (!input) {
      throw new Error('Failed to locate chat input on interface page.');
    }

    if (systemPrompt && systemPrompt.trim() !== '') {
      this.pendingSystemPrompt = systemPrompt.trim();
    }

    this.currentThreadId = this.extractThreadId(page.url()) || resumeId || '';
    return this.currentThreadId;
  }

  /**
   * Sends prompt text to the web model and streams response deltas.
   */
  async prompt(text: string): Promise<void> {
    const page = await this.browserManager.ensurePage();

    const input = await findFirstVisible(page, SELECTORS.chatInput);
    if (!input) {
      throw new Error('Chat input not found');
    }

    // Prepend pending system prompt if this is the start of a thread
    let messageText = text;
    if (this.pendingSystemPrompt) {
      messageText = `[System Instructions]:\n${this.pendingSystemPrompt}\n\n[User]:\n${text}`;
      this.pendingSystemPrompt = null;
    }

    logger.info('Submitting prompt...');
    await input.click();
    await input.fill(messageText);

    const preCount = await this.getMessageCount(page);

    // Language-agnostic submit
    const sendButton = await findFirstVisible(page, SELECTORS.sendButton, async (el) => el.isEnabled());
    if (sendButton) {
      await sendButton.click();
    } else {
      await input.press('Enter');
    }

    try {
      await this.streamer.streamResponse(page, preCount);
    } finally {
      const finalUrl = page.url();
      const newThreadId = this.extractThreadId(finalUrl);
      if (newThreadId) {
        this.currentThreadId = newThreadId;
      }
      emitEvent({ type: 'done' });
    }
  }

  /**
   * Run standalone interactive login.
   */
  async runInteractiveLogin(): Promise<void> {
    await this.authManager.runInteractiveLogin();
  }

  /**
   * Clean internal storage and system files.
   */
  async cleanFiles(): Promise<number> {
    const page = await this.browserManager.ensurePage();
    try {
      return await this.cleaner.clean(page);
    } finally {
      await this.close();
    }
  }

  async close(): Promise<void> {
    this.streamer.stop();
    await this.browserManager.close();
  }

  private async getMessageCount(page: Page): Promise<number> {
    const messageSelectors = SELECTORS.assistantMessage.join(', ');
    return page.evaluate((sel) => document.querySelectorAll(sel).length, messageSelectors);
  }

  private extractThreadId(url: string): string | null {
    const match = url.match(/\/thread\/([a-zA-Z0-9_-]+)/);
    return match ? match[1] : null;
  }
}

