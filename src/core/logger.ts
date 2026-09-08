export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

const LEVEL_PRIORITY: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

function sanitize(val: unknown): unknown {
  if (typeof val === 'string') {
    // Redact tokens, keys, passwords, secrets
    return val
      .replace(/bot[0-9]+:[a-zA-Z0-9_-]{20,}/g, 'bot[REDACTED]')
      .replace(/eyJ[a-zA-Z0-9_-]{20,}\.[a-zA-Z0-9_-]{20,}\.[a-zA-Z0-9_-]{20,}/g, '[JWT_REDACTED]')
      .replace(/(apikey|token|password|secret|bearer)\s*[:=]\s*["']?[^"'\s,]+["']?/gi, '$1=[REDACTED]');
  }
  if (val && typeof val === 'object') {
    if (Array.isArray(val)) {
      return val.map(sanitize);
    }
    const sanitizedObj: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(val)) {
      const lowerKey = k.toLowerCase();
      if (
        lowerKey.includes('token') ||
        lowerKey.includes('secret') ||
        lowerKey.includes('password') ||
        lowerKey.includes('apikey') ||
        lowerKey.includes('auth')
      ) {
        sanitizedObj[k] = '[REDACTED]';
      } else {
        sanitizedObj[k] = sanitize(v);
      }
    }
    return sanitizedObj;
  }
  return val;
}

export class Logger {
  constructor(private context: string, private minLevel: LogLevel = 'info') {}

  private shouldLog(level: LogLevel): boolean {
    return LEVEL_PRIORITY[level] >= LEVEL_PRIORITY[this.minLevel];
  }

  private format(level: LogLevel, message: string, meta?: unknown): string {
    const timestamp = new Date().toISOString();
    const entry: Record<string, unknown> = {
      timestamp,
      level: level.toUpperCase(),
      context: this.context,
      message,
    };
    if (meta !== undefined) {
      entry.meta = sanitize(meta);
    }
    return JSON.stringify(entry);
  }

  debug(message: string, meta?: unknown): void {
    if (this.shouldLog('debug')) {
      console.debug(this.format('debug', message, meta));
    }
  }

  info(message: string, meta?: unknown): void {
    if (this.shouldLog('info')) {
      console.info(this.format('info', message, meta));
    }
  }

  warn(message: string, meta?: unknown): void {
    if (this.shouldLog('warn')) {
      console.warn(this.format('warn', message, meta));
    }
  }

  error(message: string, meta?: unknown): void {
    if (this.shouldLog('error')) {
      console.error(this.format('error', message, meta));
    }
  }

  child(subContext: string): Logger {
    return new Logger(`${this.context}:${subContext}`, this.minLevel);
  }
}

export const rootLogger = new Logger('NoxImporter');
