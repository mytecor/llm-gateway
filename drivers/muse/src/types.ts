export interface DriverConfig {
  browserType?: 'chromium' | 'firefox' | 'webkit';
  userDataDir?: string;
  storageStatePath?: string;
  headless?: boolean;
  timeoutMs?: number;
  cdpEndpoint?: string;
  cdpHost?: string;
  cdpPort?: number;
}

export interface StreamCallbacks {
  onText?: (delta: string) => void;
  onReasoning?: (delta: string) => void;
  onHeartbeat?: () => void;
}

