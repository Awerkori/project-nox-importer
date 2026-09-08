export type LogLevel = 'debug' | 'info' | 'warn' | 'error';
export declare class Logger {
    private context;
    private minLevel;
    constructor(context: string, minLevel?: LogLevel);
    private shouldLog;
    private format;
    debug(message: string, meta?: unknown): void;
    info(message: string, meta?: unknown): void;
    warn(message: string, meta?: unknown): void;
    error(message: string, meta?: unknown): void;
    child(subContext: string): Logger;
}
export declare const rootLogger: Logger;
