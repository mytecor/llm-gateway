/**
 * Wire protocol types for llm-gateway external driver communication (NDJSON over stdin/stdout).
 * Reference: internal/executor/external/driver.go
 */

export interface StartMessage {
  type: 'start';
  model: string;
  system?: string;
  resume_id?: string;
  tools?: any[];
  mcp_servers?: any[];
}

export interface PromptMessage {
  type: 'prompt';
  text: string;
}

export interface ToolResultMessage {
  type: 'tool_result';
  id: string;
  content: string;
}

export type InboundMessage = StartMessage | PromptMessage | ToolResultMessage;

export interface ReadyEvent {
  type: 'ready';
  resume_id?: string;
}

export interface SessionNotFoundEvent {
  type: 'session_not_found';
}

export interface TextEvent {
  type: 'text';
  text: string;
}

export interface ReasoningEvent {
  type: 'reasoning';
  text: string;
}

export interface HeartbeatEvent {
  type: 'heartbeat';
}

export interface UsageEvent {
  type: 'usage';
  input_tokens?: number;
  output_tokens?: number;
}

export interface MessageEvent {
  type: 'message';
  id: string;
  text: string;
}

export interface ToolCallEvent {
  type: 'tool_call';
  id: string;
  name: string;
  arguments: Record<string, any>;
}

export interface DoneEvent {
  type: 'done';
}

export interface ErrorEvent {
  type: 'error';
  error: string;
}

export type OutboundEvent =
  | ReadyEvent
  | SessionNotFoundEvent
  | TextEvent
  | ReasoningEvent
  | HeartbeatEvent
  | UsageEvent
  | MessageEvent
  | ToolCallEvent
  | DoneEvent
  | ErrorEvent;

/**
 * Emit an event to stdout formatted as a single JSON line.
 * Stdout is strictly reserved for protocol events.
 */
export function emitEvent(event: OutboundEvent): void {
  const line = JSON.stringify(event);
  process.stdout.write(line + '\n');
}
