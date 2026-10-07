import fs from 'node:fs/promises';
import path from 'node:path';

export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function nowIso() {
  return new Date().toISOString();
}

export function monotonicMs() {
  return performance.now();
}

export async function withTimeout(valueOrPromise, timeoutMs, label = 'operation') {
  const timeout = Math.max(1, Number(timeoutMs) || 1);
  let timer;
  const timeoutPromise = new Promise((_, reject) => {
    timer = setTimeout(() => {
      const error = new Error(`${label} timed out after ${timeout}ms`);
      error.code = 'MONITOR_TIMEOUT';
      reject(error);
    }, timeout);
  });

  try {
    return await Promise.race([Promise.resolve(valueOrPromise), timeoutPromise]);
  } finally {
    clearTimeout(timer);
  }
}

export async function atomicWriteJson(filePath, value) {
  const target = path.resolve(filePath);
  await fs.mkdir(path.dirname(target), { recursive: true });
  const temporary = `${target}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
  await fs.rename(temporary, target);
}

export async function readJson(filePath, fallback = null) {
  try {
    return JSON.parse(await fs.readFile(filePath, 'utf8'));
  } catch {
    return fallback;
  }
}

export function percentile(values, fraction) {
  const valid = values.filter((value) => Number.isFinite(value)).sort((a, b) => a - b);
  if (!valid.length) return null;
  const index = Math.min(valid.length - 1, Math.max(0, Math.ceil(valid.length * fraction) - 1));
  return valid[index];
}

export async function fetchWithTimeout(url, options = {}) {
  const timeoutMs = Math.max(1, Number(options.timeoutMs ?? 5000));
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const started = monotonicMs();

  try {
    const response = await fetch(url, {
      method: options.method ?? 'GET',
      headers: options.headers,
      body: options.body,
      redirect: options.redirect ?? 'follow',
      signal: controller.signal,
    });
    const durationMs = Math.round(monotonicMs() - started);
    // We only need status and timing. Cancelling the body prevents a response
    // stream from keeping the worker alive after a bounded probe.
    if (response.body) await response.body.cancel().catch(() => {});
    return { status: response.status, ok: response.ok, durationMs, finalUrl: response.url };
  } catch (error) {
    return {
      status: null,
      ok: false,
      durationMs: Math.round(monotonicMs() - started),
      finalUrl: url,
      error: error?.name === 'AbortError' ? 'TIMEOUT' : String(error?.message ?? error),
    };
  } finally {
    clearTimeout(timer);
  }
}

export function parseJsonValue(value) {
  if (value === null || value === undefined) return null;
  if (typeof value !== 'string') return value;
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}
