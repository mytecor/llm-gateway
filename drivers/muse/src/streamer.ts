import { Page } from 'playwright';
import { emitEvent } from './protocol.js';
import { SELECTORS } from './selectors.js';
import { logger } from './logger.js';

export interface StreamOptions {
  heartbeatIntervalMs?: number;
  domPollIntervalMs?: number;
  maxIdleCycles?: number;
}

export class ResponseStreamer {
  private isStreaming = false;

  constructor(private readonly options: StreamOptions = {}) {}

  /**
   * Captures response stream from browser page using WebSocket frames with DOM polling fallback.
   * Emits NDJSON wire events (text, heartbeat).
   */
  async streamResponse(page: Page, preMessageCount: number): Promise<void> {
    this.isStreaming = true;
    let wsDeltasReceived = false;

    // 1. Setup WebSocket listener for low-latency streaming
    const wsHandler = (frame: any) => {
      try {
        const payload = frame.payload;
        if (typeof payload === 'string') {
          const data = JSON.parse(payload);
          const chunk = data.delta || data.text || data.chunk || data.content;
          if (chunk && typeof chunk === 'string') {
            wsDeltasReceived = true;
            emitEvent({ type: 'text', text: chunk });
          }
        }
      } catch {
        // Ignore non-JSON frames
      }
    };

    const attachWs = (ws: any) => {
      if (ws.url().includes('metaaivm.com') || ws.url().includes('hatch')) {
        ws.on('framereceived', wsHandler);
      }
    };

    page.on('websocket', attachWs);

    // 2. Setup periodic heartbeats
    const heartbeatMs = this.options.heartbeatIntervalMs || 5000;
    const heartbeatTimer = setInterval(() => {
      if (this.isStreaming) {
        emitEvent({ type: 'heartbeat' });
      }
    }, heartbeatMs);

    try {
      // 3. Wait for new message container to appear
      const messageSelectors = SELECTORS.assistantMessage.join(', ');
      await page.waitForFunction(
        ({ count, sel }) => {
          const messages = document.querySelectorAll(sel);
          return messages.length > count;
        },
        { count: preMessageCount, sel: messageSelectors },
        { timeout: 30000 }
      ).catch(() => {
        logger.debug('Waiting for assistant message container timed out or container already present.');
      });

      // 4. DOM observer / polling loop
      await this.pollDomUntilDone(page, () => wsDeltasReceived);
    } finally {
      clearInterval(heartbeatTimer);
      this.isStreaming = false;
      page.off('websocket', attachWs);
    }
  }

  private async pollDomUntilDone(page: Page, hasWsDeltas: () => boolean): Promise<void> {
    let lastText = '';
    let idleCycles = 0;
    const maxIdleCycles = this.options.maxIdleCycles || 40;
    const pollInterval = this.options.domPollIntervalMs || 100;

    const stopSelectors = SELECTORS.stopButtonOrGenerating.join(', ');
    const messageSelectors = SELECTORS.assistantMessage.join(', ');

    while (this.isStreaming) {
      await page.waitForTimeout(pollInterval);

      const status = await page.evaluate(
        ({ stopSel, msgSel }) => {
          const stopBtn = document.querySelector(stopSel);
          const isGenerating = !!stopBtn;

          const nodes = document.querySelectorAll(msgSel);
          let text = '';
          if (nodes.length > 0) {
            text = (nodes[nodes.length - 1] as HTMLElement).innerText || '';
          } else {
            text = document.body.innerText.slice(-1000);
          }

          return { isGenerating, text };
        },
        { stopSel: stopSelectors, msgSel: messageSelectors }
      );

      // If WebSocket is not providing chunks, fallback to DOM delta extraction
      if (!hasWsDeltas() && status.text.length > lastText.length) {
        const delta = status.text.slice(lastText.length);
        lastText = status.text;
        idleCycles = 0;
        emitEvent({ type: 'text', text: delta });
      } else if (!hasWsDeltas() && status.text === lastText && lastText.length > 0) {
        idleCycles++;
      }

      // Check if generation finished
      if (!status.isGenerating && lastText.length > 0) {
        await page.waitForTimeout(400);
        break;
      }

      // Safeguard: break on persistent idle after receiving text
      if (idleCycles >= maxIdleCycles && lastText.length > 0) {
        logger.info('Generation completed (inactivity detection).');
        break;
      }
    }
  }

  stop(): void {
    this.isStreaming = false;
  }
}
