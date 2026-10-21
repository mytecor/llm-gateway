import { Page } from 'playwright';
import { SELECTORS, findFirstVisible } from './selectors.js';
import { logger } from './logger.js';

export class Cleaner {
  /**
   * Cleans internal system files and artifacts in web workspace.
   * 100% LANGUAGE-AGNOSTIC: uses route URLs, testids, DOM structure, and semantic roles.
   */
  async clean(page: Page): Promise<number> {
    logger.info('Navigating to Library to clean system files...');
    try {
      await page.goto('https://muse.ai/library/files', { waitUntil: 'domcontentloaded' });
      await page.waitForTimeout(1500);

      // In the library sidebar navigation, click system files section if needed
      const systemNav = await findFirstVisible(page, SELECTORS.storage.systemFilesNavRow);
      if (systemNav) {
        await systemNav.click().catch(() => {});
        await page.waitForTimeout(1000);
      }

      let deletedCount = 0;
      const maxIterations = 50;

      for (let i = 0; i < maxIterations; i++) {
        // Look for action menu buttons on file rows
        const actionButtons = await page.$$(SELECTORS.storage.actionMenuButtons.join(', '));

        if (actionButtons.length === 0) {
          // Check for direct delete buttons (e.g. trash icon or destructive role)
          const directDeleteBtns = await page.$$(SELECTORS.storage.directDeleteButtons.join(', '));
          if (directDeleteBtns.length === 0) {
            break; // No files remaining
          }

          for (const btn of directDeleteBtns) {
            if (await btn.isVisible()) {
              await btn.click();
              await this.confirmDeleteDialog(page);
              deletedCount++;
              await page.waitForTimeout(500);
            }
          }
          break;
        }

        // Click the first action menu button
        const menuBtn = actionButtons[0];
        if (await menuBtn.isVisible()) {
          await menuBtn.click();
          await page.waitForTimeout(400);

          // Find delete menu item in popup
          const deleteMenuItem = await findFirstVisible(page, SELECTORS.storage.deleteMenuItems);

          if (deleteMenuItem) {
            await deleteMenuItem.click();
            await page.waitForTimeout(400);
            await this.confirmDeleteDialog(page);
            deletedCount++;
            await page.waitForTimeout(800);
          } else {
            // Dismiss menu by pressing Escape
            await page.keyboard.press('Escape');
            break;
          }
        } else {
          break;
        }
      }

      if (deletedCount > 0) {
        logger.info(`Successfully deleted ${deletedCount} system file(s).`);
      } else {
        logger.info('System files are clean (0 files found).');
      }

      return deletedCount;
    } catch (err: any) {
      logger.warn(`Note during system files cleanup: ${err.message}`);
      return 0;
    }
  }

  /**
   * Language-agnostic confirmation of modal dialogs.
   * Finds the primary/destructive button in the dialog.
   */
  private async confirmDeleteDialog(page: Page): Promise<void> {
    try {
      const confirmBtn = await findFirstVisible(page, SELECTORS.storage.confirmDialogButtons);
      if (confirmBtn) {
        await confirmBtn.click();
        await page.waitForTimeout(500);
      }
    } catch {
      // Ignore confirmation errors
    }
  }
}

