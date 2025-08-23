import https from 'node:https';
import fetch from 'node-fetch';

const AGENT = new https.Agent({ keepAlive: true });

export async function http(url, opts = {}, { retries = 3, backoffMs = 500 } = {}) {
  let attempt = 0;
  let lastErr;
  while (attempt <= retries) {
    const res = await fetch(url, { agent: AGENT, ...opts });
    if (res.status === 429) {
      const ra = Number(res.headers.get('retry-after') || 1);
      await delay((ra + 1) * 1000);
      attempt++; continue;
    }
    if (res.ok) return res;
    lastErr = new Error(`${opts.method || 'GET'} ${url} => ${res.status} ${await safeText(res)}`);
    if (res.status >= 500 && attempt < retries) { await delay(backoffMs * Math.pow(2, attempt++)); continue; }
    throw lastErr;
  }
  throw lastErr;
}

const delay = (ms) => new Promise(r => setTimeout(r, ms));
async function safeText(res) { try { return await res.text(); } catch { return ''; } }
