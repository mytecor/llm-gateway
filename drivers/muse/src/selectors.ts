import { Page, ElementHandle } from 'playwright';

/**
 * Language-agnostic selectors for web UI driver.
 * Uses semantic roles, testids, DOM structure, and SVG paths rather than localized text.
 */
export const SELECTORS = {
  chatInput: [
    '[data-testid*="chat-input"]',
    '[data-testid*="message-input"]',
    'form textarea',
    'main textarea',
    'textarea[rows]',
    'textarea',
    'div[contenteditable="true"][role="textbox"]',
    'div[contenteditable="true"]',
  ],

  loginReady: [
    'form textarea',
    'main textarea',
    'textarea',
    'div[contenteditable="true"][role="textbox"]',
    'div[contenteditable="true"]',
    '[data-testid="hatch-nav-search"]',
    '[data-testid*="chat-input"]',
  ],

  sendButton: [
    'button[type="submit"]',
    'form button[type="submit"]',
    '[data-testid*="send"]',
    'form button:not([disabled])',
  ],

  stopButtonOrGenerating: [
    '[data-testid*="stop"]',
    'button:has(svg rect)',
    'button:has(rect)',
    '[aria-busy="true"]',
    '[class*="animate-spin"]',
  ],

  assistantMessage: [
    '[data-role="assistant"]',
    '.prose',
    '[data-testid*="message"]',
    '[data-message-author-role="assistant"]',
  ],

  storage: {
    systemFilesNavRow: [
      'nav div[class*="py-"] [data-hatch-library-skeleton-row="true"]',
      'nav div[class*="py-"] [class*="group/nav-row"]',
      'nav [class*="group/nav-row"]:has(svg path[d*="7.01 1.13"])',
    ],
    actionMenuButtons: [
      '[data-hatch-library-skeleton-content="true"] button[aria-haspopup="menu"]',
      '[data-testid="hatch-route-header-content"] ~ * button[aria-haspopup="menu"]',
      'main button[aria-haspopup="menu"]',
      'main button:has(svg circle)',
      '[data-testid*="more"]',
      '[data-testid*="menu"]',
      '[data-testid*="action"]',
    ],
    directDeleteButtons: [
      'main button[class*="destructive"]',
      'main button[class*="danger"]',
      'main button:has(svg path[d*="trash"])',
      '[data-testid*="delete"]',
      '[data-testid*="remove"]',
    ],
    deleteMenuItems: [
      '[role="menu"] [role="menuitem"][class*="destructive"]',
      '[role="menu"] [role="menuitem"][class*="danger"]',
      '[role="menu"] [role="menuitem"][class*="red"]',
      '[role="menu"] [role="menuitem"]:has(svg path[d*="trash"])',
      '[role="menu"] [data-testid*="delete"]',
      '[role="menu"] [data-testid*="remove"]',
      '[role="menu"] [role="menuitem"]:last-child',
    ],
    confirmDialogButtons: [
      '[role="dialog"] button[type="submit"]',
      '[role="dialog"] button[class*="destructive"]',
      '[role="dialog"] button[class*="danger"]',
      '[role="dialog"] button[class*="bg-red"]',
      '[role="dialog"] [class*="justify-end"] button:last-child',
      '[role="dialog"] footer button:last-child',
      '[aria-modal="true"] button[class*="destructive"]',
      '[aria-modal="true"] footer button:last-child',
    ],
  },
};

/**
 * Searches sequentially through selectors for the first visible and interactive element.
 */
export async function findFirstVisible(
  page: Page,
  selectors: string[],
  predicate?: (handle: ElementHandle<SVGElement | HTMLElement>) => Promise<boolean>
): Promise<ElementHandle<SVGElement | HTMLElement> | null> {
  for (const sel of selectors) {
    const el = await page.$(sel);
    if (el && (await el.isVisible())) {
      if (!predicate || (await predicate(el))) {
        return el;
      }
    }
  }
  return null;
}

/**
 * Waits for any of the given selectors to appear and become visible.
 */
export async function waitForAnySelector(
  page: Page,
  selectors: string[],
  timeoutMs: number
): Promise<string | null> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    for (const sel of selectors) {
      const el = await page.$(sel);
      if (el && (await el.isVisible())) {
        return sel;
      }
    }
    await page.waitForTimeout(500);
  }
  return null;
}
