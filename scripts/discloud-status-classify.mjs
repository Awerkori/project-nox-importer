// Unknown responses must never be treated as an offline container.
export function discloudStatus(responseOk, body) {
  if (!responseOk || body?.status === 'error') return 'UNKNOWN';
  const app = body?.apps || body;
  if (!app || typeof app !== 'object' || Array.isArray(app)) return 'UNKNOWN';
  if (app.online === true) return 'ONLINE';
  const names = [app.container, app.status].filter(x => typeof x === 'string').map(x => x.toLowerCase());
  if (names.includes('online')) return 'ONLINE';
  if (app.online === false || names.includes('offline')) return 'OFFLINE';
  return 'UNKNOWN';
}
