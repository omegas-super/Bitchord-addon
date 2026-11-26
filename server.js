#!/usr/bin/env node
/**
 * bitchord-tido-addon — bring Tido / Tidal Hi-Res music into BitChord.
 *
 * A self-hosted bridge that speaks the BitChord (Eclipse-family) addon protocol
 * on the front and answers search + streams from:
 *   1. your Tido deployment (https://github.com/RickyAddons — Stremio/Eclipse
 *      addon that fronts Tidal through hifi-api), and
 *   2. any hifi-api instance (https://github.com/binimum/hifi-api), e.g. your
 *      own self-hosted one, as automatic failover.
 *
 * Front-end protocol (mirrors rairulyle/bitchord-selfhosted-addon exactly):
 *   GET  /health
 *   GET  /{slug}/{secret}/manifest.json
 *   GET  /{slug}/{secret}/search?q=...
 *   GET  /{slug}/{secret}/stream/{id}
 *   GET  /{slug}/{secret}/file/{id}          (byte proxy, Range aware)
 *   GET  /{slug}/{secret}/art/{id}           (artwork proxy, cached)
 *   POST /{slug}/{secret}/sync               (clears caches)
 *   OPTIONS /{slug}/{secret}/...             (CORS preflight)
 *   GET  /setup                              (configuration web UI)
 *
 * Zero npm dependencies. Node.js >= 18.
 */
'use strict';

const http = require('node:http');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { Readable } = require('node:stream');

const VERSION = require('./package.json').version || '1.0.0';

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

const env = (name, fallback) => {
  const v = process.env[name];
  return v === undefined || v === '' ? fallback : v;
};
const envBool = (name, fallback) => {
  const v = process.env[name];
  if (v === undefined || v === '') return fallback;
  return !/^(0|false|no|off)$/i.test(v);
};

const PORT = parseInt(env('PORT', '8080'), 10);
const DATA_DIR = pickDataDir();
const LOG_LEVEL = env('LOG_LEVEL', 'info').toLowerCase();
const SLUG = sanitizeSlug(env('SLUG', 'tidal-1'));

const KEEP_ALIVE = envBool('KEEP_ALIVE', !!process.env.RENDER_EXTERNAL_URL || !!process.env.SELF_PING_URL);
const SELF_PING_URL = env('SELF_PING_URL', process.env.RENDER_EXTERNAL_URL || '');
const KEEP_ALIVE_INTERVAL = Math.max(60, parseInt(env('KEEP_ALIVE_INTERVAL', '240'), 10)); // seconds

const SEARCH_CACHE_TTL = parseInt(env('SEARCH_CACHE_TTL', '60'), 10) * 1000;
const STREAM_CACHE_TTL = parseInt(env('STREAM_CACHE_TTL', '600'), 10) * 1000; // 10 min
const META_CACHE_TTL = 60 * 60 * 1000; // 1 h
const BACKEND_COOLDOWN = 45 * 1000; // after a failure, skip a backend for 45 s
const SEARCH_LIMIT = 50;
const LOGGED_QUERY_RUNES = 200;

function pickDataDir() {
  const fromEnv = process.env.DATA_DIR;
  if (fromEnv) return fromEnv;
  // Docker images run as uid 65532 with /data; local runs fall back to ./data.
  try {
    fs.accessSync('/data', fs.constants.W_OK);
    return '/data';
  } catch {
    return path.join(process.cwd(), 'data');
  }
}

function sanitizeSlug(s) {
  const clean = String(s).replace(/[^0-9A-Za-z_-]/g, '').slice(0, 64);
  return clean || 'tidal-1';
}

// ---------------------------------------------------------------------------
// Logging (secrets never printed)
// ---------------------------------------------------------------------------

const LEVELS = { debug: 10, info: 20, warn: 30, error: 40 };
function log(level, msg, extra) {
  if ((LEVELS[level] || 20) < (LEVELS[LOG_LEVEL] || 20)) return;
  const line = { t: new Date().toISOString(), level, msg };
  if (extra) line.extra = extra;
  process.stdout.write(JSON.stringify(line) + '\n');
}
const debug = (m, e) => log('debug', m, e);
const info = (m, e) => log('info', m, e);
const warn = (m, e) => log('warn', m, e);
const errorLog = (m, e) => log('error', m, e);

/** Redact path segments that look like the live secret or a Tido token. */
function redactUrl(u) {
  try {
    const url = new URL(u);
    const out = [url.origin];
    for (const seg of url.pathname.split('/')) {
      if (!seg) continue;
      out.push(seg === 'manifest.json' ? seg : seg.length >= 16 ? '***' : seg);
    }
    return out.join('/');
  } catch {
    return 'invalid-url';
  }
}
function redactText(s) {
  if (!s) return s;
  let out = String(s);
  for (const secret of [state.config?.secret, state.sessionKey].filter(Boolean)) {
    out = out.split(secret).join('***');
  }
  return out;
}

// ---------------------------------------------------------------------------
// Small utilities
// ---------------------------------------------------------------------------

const sha256 = (s) => crypto.createHash('sha256').update(s).digest();
const timingSafeEqualStr = (a, b) => {
  const da = sha256(String(a));
  const db = sha256(String(b));
  return crypto.timingSafeEqual(da, db);
};
const randomHex = (n) => crypto.randomBytes(n).toString('hex');
const b64url = (buf) => Buffer.from(buf).toString('base64url');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function nowSec() { return Math.floor(Date.now() / 1000); }

/** LRU-ish cache with TTL and byte budget. */
class Cache {
  constructor(maxItems, maxBytes = 0) {
    this.maxItems = maxItems;
    this.maxBytes = maxBytes;
    this.bytes = 0;
    this.map = new Map(); // insertion-ordered, most recent last
  }
  get(key) {
    const hit = this.map.get(key);
    if (!hit) return null;
    if (hit.expires && hit.expires < Date.now()) {
      this.map.delete(key);
      if (hit.size) this.bytes -= hit.size;
      return null;
    }
    // refresh recency
    this.map.delete(key);
    this.map.set(key, hit);
    return hit.value;
  }
  put(key, value, ttlMs, size = 0) {
    if (this.map.has(key)) {
      const old = this.map.get(key);
      if (old.size) this.bytes -= old.size;
      this.map.delete(key);
    }
    this.map.set(key, { value, expires: ttlMs ? Date.now() + ttlMs : 0, size });
    this.bytes += size;
    while (this.map.size > this.maxItems || (this.maxBytes && this.bytes > this.maxBytes)) {
      const oldest = this.map.keys().next().value;
      const old = this.map.get(oldest);
      if (!old) break;
      if (old.size) this.bytes -= old.size;
      this.map.delete(oldest);
    }
  }
  del(key) {
    const old = this.map.get(key);
    if (old) {
      if (old.size) this.bytes -= old.size;
      this.map.delete(key);
    }
  }
  clear() {
    this.map.clear();
    this.bytes = 0;
  }
  get size() { return this.map.size; }
}

/** HTTP fetch helpers with timeouts. Node >= 18 global fetch. */
async function fetchJson(url, opts = {}) {
  const res = await fetchUrl(url, opts);
  let json = null;
  try { json = await res.json(); } catch { /* non-JSON body */ }
  return { status: res.status, headers: res.headers, json };
}

async function fetchUrl(url, opts = {}) {
  const controller = new AbortController();
  const timeoutMs = opts.timeoutMs || 15000;
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, {
      method: opts.method || 'GET',
      headers: opts.headers || {},
      body: opts.body || undefined,
      signal: controller.signal,
      redirect: opts.redirect || 'follow',
    });
  } finally {
    clearTimeout(timer);
  }
}

class HttpError extends Error {
  constructor(status, message, extra) {
    super(message);
    this.status = status;
    this.extra = extra;
  }
}

// ---------------------------------------------------------------------------
// Configuration store (data/addon.json)
// ---------------------------------------------------------------------------

const QUALITIES = ['MAX', 'HIRES', 'LOSSLESS', 'HIGH', 'LOW'];
const VIDEO_QUALITIES = ['HIGH', 'MEDIUM', 'LOW'];

function defaultConfig() {
  return {
    schema: 1,
    version: 1,
    publicUrl: env('PUBLIC_URL', ''),
    slug: SLUG,
    secret: randomHex(16),
    linkKey: randomHex(32),
    sessionKey: randomHex(32),
    label: env('LABEL', 'Tido Tidal Hi-Res'),
    tidoUrl: env('TIDO_URL', ''),
    tidoRegenUrl: env('TIDO_REGEN_URL', ''),
    hifiApiUrls: env('HIFI_API_URLS', '')
      .split(/[\n,]+/).map((s) => s.trim()).filter(Boolean),
    audioQuality: QUALITIES.includes(env('AUDIO_QUALITY', '')) ? env('AUDIO_QUALITY', '') : 'MAX',
    exactQuality: envBool('EXACT_QUALITY', false),
    hideExplicit: envBool('HIDE_EXPLICIT', false),
    videosInSearch: envBool('VIDEOS_IN_SEARCH', true),
    videoQuality: VIDEO_QUALITIES.includes(env('VIDEO_QUALITY', '')) ? env('VIDEO_QUALITY', '') : 'HIGH',
    monoUrl: env('MONO_URL', 'https://tracks.monochrome.st'),
    monoEnabled: envBool('MONO_ENABLED', true),
    passwordHash: '', // set on first /setup visit or via SETUP_PASSWORD
  };
}

const state = {
  config: null,
  passwordAttempts: new Map(), // ip -> {count, first}
};

function configPath() { return path.join(DATA_DIR, 'addon.json'); }

function loadConfig() {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  const cfg = defaultConfig();
  let saved = null;
  try {
    saved = JSON.parse(fs.readFileSync(configPath(), 'utf8'));
  } catch {
    /* first boot */
  }
  if (saved && typeof saved === 'object') {
    for (const key of Object.keys(cfg)) {
      if (saved[key] !== undefined && saved[key] !== null) cfg[key] = saved[key];
    }
  }
  const presetPassword = env('SETUP_PASSWORD', '');
  if (!cfg.passwordHash && presetPassword) cfg.passwordHash = hashPassword(presetPassword);
  if (!cfg.slug) cfg.slug = SLUG;
  state.config = cfg;
  if (saved) info('config loaded', { dataDir: DATA_DIR, slug: cfg.slug });
  else saveConfig(cfg);
  return cfg;
}

function saveConfig(cfg) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  const tmp = configPath() + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(cfg, null, 2) + '\n', { mode: 0o600 });
  fs.renameSync(tmp, configPath());
  debug('config saved');
}

function hashPassword(pw) {
  const salt = randomHex(16);
  const hash = crypto.scryptSync(String(pw), salt, 32).toString('hex');
  return `scrypt$${salt}$${hash}`;
}

function verifyPassword(pw, stored) {
  if (!stored) return false;
  const parts = String(stored).split('$');
  if (parts.length !== 3 || parts[0] !== 'scrypt') return false;
  const hash = crypto.scryptSync(String(pw), parts[1], 32).toString('hex');
  const a = Buffer.from(hash, 'hex');
  const b = Buffer.from(parts[2], 'hex');
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

/** The public URL as seen by clients (headers beat config, config beats env). */
function publicUrl(req) {
  const proto = req.headers['x-forwarded-proto'] || (req.socket.encrypted ? 'https' : 'http');
  let host = req.headers['x-forwarded-host'] || req.headers.host || '';
  host = String(host).split(',')[0].trim();
  if (host) return `${proto}://${host}`;
  const cfgUrl = state.config.publicUrl || process.env.PUBLIC_URL || '';
  return cfgUrl.replace(/\/+$/, '');
}

function baseLink(req) {
  // {public}/{slug}/{secret} — the prefix every protocol URL hangs off.
  const cfg = state.config;
  const pub = (req ? publicUrl(req) : cfg.publicUrl) || `http://127.0.0.1:${PORT}`;
  return `${pub.replace(/\/+$/, '')}/${cfg.slug}/${cfg.secret}`;
}

// ---------------------------------------------------------------------------
// Sessions (cookie = exp.hmac)
// ---------------------------------------------------------------------------

const SESSION_COOKIE = 'bitdo_session';
const SESSION_TTL = 12 * 60 * 60 * 1000;

function sessionToken() {
  const exp = Date.now() + SESSION_TTL;
  const sig = crypto.createHmac('sha256', state.config.sessionKey).update(String(exp)).digest('base64url');
  return `${exp}.${sig}`;
}

function checkSession(req) {
  const raw = req.headers.cookie || '';
  const m = /(?:^|;\s*)bitdo_session=([^;]+)/.exec(raw);
  if (!m) return false;
  const [expRaw, sig] = m[1].split('.');
  const exp = parseInt(expRaw, 10);
  if (!exp || exp < Date.now()) return false;
  const want = crypto.createHmac('sha256', state.config.sessionKey).update(String(exp)).digest('base64url');
  try {
    return crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(want));
  } catch {
    return false;
  }
}

function parseCookies(req) {
  const out = {};
  const raw = req.headers.cookie || '';
  for (const part of raw.split(';')) {
    const idx = part.indexOf('=');
    if (idx > 0) out[part.slice(0, idx).trim()] = part.slice(idx + 1).trim();
  }
  return out;
}

function readBody(req, limit = 64 * 1024) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let total = 0;
    req.on('data', (c) => {
      total += c.length;
      if (total > limit) { reject(new HttpError(413, 'body too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

function clientIp(req) {
  const fwd = req.headers['x-forwarded-for'];
  if (fwd) return String(fwd).split(',')[0].trim();
  return req.socket.remoteAddress || '?';
}

function loginThrottled(ip) {
  const rec = state.passwordAttempts.get(ip);
  if (!rec) return false;
  if (rec.count >= 10 && Date.now() - rec.first < 15 * 60 * 1000) return true;
  if (Date.now() - rec.first >= 15 * 60 * 1000) state.passwordAttempts.delete(ip);
  return false;
}

function recordLoginFailure(ip) {
  const rec = state.passwordAttempts.get(ip) || { count: 0, first: Date.now() };
  rec.count += 1;
  state.passwordAttempts.set(ip, rec);
}

// ---------------------------------------------------------------------------
// Tidal track metadata normalization
// ---------------------------------------------------------------------------

/**
 * Track metadata we keep for every id we hand out.
 *   kind: "track" (audio) | "video" (a standalone Tidal video) | "video-of-track"
 * Ids handed to BitChord:  t{tidalId}  /  v{videoId or trackId}
 */
function metaFromTidalItem(item, backend, hifiUrl) {
  const artist = (item.artists && item.artists.map((a) => a.name).filter(Boolean).join(', '))
    || (item.artist && (item.artist.name || item.artist))
    || '';
  const album = item.album && typeof item.album === 'object' ? (item.album.title || '') : (item.album || '');
  const cover = item.album && item.album.cover ? item.album.cover
    : (item.cover && typeof item.cover === 'string' ? item.cover : null);
  return {
    kind: 'track',
    backend, // "tido" | "hifi"
    hifiUrl: hifiUrl || null,
    tidalId: String(item.id || ''),
    tidoTrackId: String(item.id || ''),
    title: String(item.title || ''),
    artist,
    album,
    duration: parseInt(item.duration || 0, 10) || 0,
    explicit: !!item.explicit,
    isrc: item.isrc ? String(item.isrc) : null,
    cover: cover ? String(cover) : null, // Tidal cover uuid, for artwork
    artworkURL: typeof item.artworkURL === 'string' ? item.artworkURL : null,
    musicVideoId: item.musicVideoId != null ? String(item.musicVideoId) : null,
    imageId: item.imageId != null ? String(item.imageId) : null, // videos carry imageId
  };
}

/** Tidal cover uuid (a1b2c3-...) or imageId -> resources.tidal.com image URL. */
function tidalCoverUrl(uuid, size = 1280) {
  if (!uuid) return null;
  const slug = String(uuid).replace(/-/g, '/');
  return `https://resources.tidal.com/images/${slug}/${size}x${size}.jpg`;
}

const LOSSLESS_CODECS = ['flac', 'alac', 'wav', 'aiff', 'wavpack', 'ape', 'dsd', 'eac3_joc'];

function qualityLabelForTrack(cfg) {
  return ['MAX', 'HIRES', 'LOSSLESS'].includes(cfg.audioQuality) ? 'LOSSLESS' : 'HIGH';
}

// ---------------------------------------------------------------------------
// Backends: Tido (Eclipse protocol), hifi-api (binimum/hifi-api) and the
// Monochrome tracks API (tracks.monochrome.st — the backend behind
// monochrome.tf; see github.com/monochrome-music/monochrome, js/tracks-api.js)
// ---------------------------------------------------------------------------

const backends = {
  tido: { name: 'Tido', cooldownUntil: 0, lastError: '', okCount: 0, failCount: 0 },
  hifi: [], // {url, name, cooldownUntil, lastError, okCount, failCount}
  mono: { name: 'monochrome', cooldownUntil: 0, lastError: '', okCount: 0, failCount: 0 },
};

function syncBackendRegistry() {
  const cfg = state.config;
  const urls = Array.isArray(cfg.hifiApiUrls) ? cfg.hifiApiUrls.filter(Boolean) : [];
  // keep state for unchanged urls, add new, drop removed
  const existing = new Map(backends.hifi.map((b) => [b.url, b]));
  backends.hifi = urls.map((u) => existing.get(u) || {
    url: u, name: shortName(u), cooldownUntil: 0, lastError: '', okCount: 0, failCount: 0,
  });
}

function shortName(u) {
  try {
    const url = new URL(u);
    return url.hostname;
  } catch {
    return 'hifi-api';
  }
}

function tidoTarget() {
  const raw = String(state.config.tidoUrl || '').trim();
  if (!raw) return null;
  let u = raw.replace(/\/+$/, '');
  u = u.replace(/\/manifest\.json$/i, '');
  const idx = u.lastIndexOf('/');
  let base = u;
  let token = '';
  if (idx > 0) {
    token = u.slice(idx + 1);
    base = u.slice(0, idx);
    if (token === 'manifest.json' || token === '') { token = ''; }
  }
  return { base, token };
}

/** Eclipse settings are appended as query params on every call. */
function tidoSettingsParams() {
  const cfg = state.config;
  const p = new URLSearchParams();
  p.set('quality', cfg.audioQuality);
  p.set('exactQuality', String(!!cfg.exactQuality));
  p.set('videosEnabled', 'true'); // needed for v{id} video streams
  p.set('videoQuality', cfg.videoQuality);
  p.set('hideExplicit', String(!!cfg.hideExplicit));
  return p;
}

/**
 * Client headers for Tido requests. The UA deliberately avoids the substring
 * "bitchord": Tido sits behind Cloudflare's browser-integrity check, which
 * answers that shape with an HTML error page instead of the app's JSON
 * (measured 2026-11; "Bitdo/1.0" passes). X-Eclipse-Access is forwarded
 * verbatim when a signed-in Eclipse client sent one, so Tido can verify the
 * actual user if it ever starts enforcing the header.
 */
function tidoClientHeaders(xea) {
  const h = { 'user-agent': 'Bitdo/1.0 (+self-hosted Eclipse addon)' };
  if (xea) h['x-eclipse-access'] = String(xea).slice(0, 2048);
  return h;
}

/**
 * Eclipse-family clients append their app settings as query params on every
 * request — the same convention Tido's manifest settings describe (quality,
 * exactQuality, videosEnabled...). Server config is the default; the client's
 * per-request values win. This is what lets BitChord's own quality setting
 * drive the addon instead of being ignored.
 */
function clientSettings(params, cfg) {
  const q = params && QUALITIES.includes(params.get('quality')) ? params.get('quality') : cfg.audioQuality;
  const pick = (name, cfgValue) => {
    const raw = params ? params.get(name) : null;
    return raw === null ? cfgValue : raw === 'true';
  };
  return {
    quality: q,
    exactQuality: pick('exactQuality', !!cfg.exactQuality),
    videosEnabled: pick('videosEnabled', !!cfg.videosInSearch),
  };
}

// --- Tido manifest regeneration --------------------------------------------
// Tido can mint a fresh manifest via POST /generate?k=<key>. Measured behavior
// (2026-11): when the whole Tido upstream is down every token 403s "Unavailable"
// identically, so regeneration cannot help — the cooldown + failover ladder
// carries the traffic and the rate limit below avoids hammering /generate.
// When a *specific* token is rate-limited or banned while the upstream is up,
// swapping in a fresh manifest is exactly the fix.
const REGEN_MIN_INTERVAL = 10 * 60 * 1000; // auto regen at most every 10 min
const regenState = { busy: false, lastAt: 0, lastResult: '', lastOkUrl: '', count: 0 };

function tidoRegenUrl() {
  return String(state.config.tidoRegenUrl || '').trim();
}

/**
 * Settings JSON for POST /generate. Measured 2026-11: quality binds per token
 * when /generate receives a body — posting none makes the fresh manifest fall
 * back to Tido's own default (we saw MAX rotate down to HIRES that way). The
 * current link's manifest is re-read first so rotation preserves whatever the
 * link already carries; local config is the fallback.
 */
async function tidoSettingsPayload() {
  const cfg = state.config;
  const payload = {
    quality: cfg.audioQuality,
    videosEnabled: true, // needed for v{id} video streams
    videoQuality: cfg.videoQuality,
    hideExplicit: !!cfg.hideExplicit,
    importsEnabled: false,
    artistGridMode: 'off',
    exactQuality: !!cfg.exactQuality,
  };
  const t = tidoTarget();
  if (t) {
    try {
      const res = await fetchJson(`${t.base}/${t.token ? t.token + '/' : ''}manifest.json`,
        { timeoutMs: 10000, headers: tidoClientHeaders() });
      const settings = res.status === 200 && res.json && Array.isArray(res.json.settings) ? res.json.settings : [];
      for (const s of settings) {
        if (s && s.key && s.key in payload && s.default !== undefined) payload[s.key] = s.default;
      }
    } catch { /* keep local defaults */ }
  }
  payload.videosEnabled = true; // same reason as tidoSettingsParams()
  return payload;
}

/**
 * POST the generate endpoint and adopt the fresh manifestUrl. Manual calls
 * (from /setup) bypass the interval; automatic calls are rate-limited.
 * Never throws: failures are logged (redacted) and returned.
 */
async function regenerateTidoManifest({ manual = false } = {}) {
  const url = tidoRegenUrl();
  if (!url) { regenState.lastResult = 'no regen URL configured'; return { ok: false, reason: regenState.lastResult }; }
  if (regenState.busy) { regenState.lastResult = 'already in progress'; return { ok: false, reason: regenState.lastResult }; }
  if (!manual && Date.now() - regenState.lastAt < REGEN_MIN_INTERVAL) {
    regenState.lastResult = 'rate limited';
    return { ok: false, reason: regenState.lastResult, retryInMs: REGEN_MIN_INTERVAL - (Date.now() - regenState.lastAt) };
  }
  regenState.busy = true;
  try {
    const settings = await tidoSettingsPayload();
    const res = await fetchJson(url, {
      method: 'POST',
      timeoutMs: 15000,
      headers: { 'content-type': 'application/json', ...tidoClientHeaders() },
      body: JSON.stringify(settings),
    });
    const manifestUrl = res.json && typeof res.json.manifestUrl === 'string' ? res.json.manifestUrl.trim() : '';
    if (res.status !== 200 || !/^https?:\/\//i.test(manifestUrl)) {
      regenState.lastAt = Date.now();
      regenState.lastResult = `generate endpoint answered ${res.status}`;
      warn('tido regen failed', { status: res.status });
      return { ok: false, reason: regenState.lastResult };
    }
    state.config.tidoUrl = manifestUrl;
    regenState.lastAt = Date.now();
    regenState.lastResult = 'ok';
    regenState.lastOkUrl = manifestUrl;
    regenState.count += 1;
    backends.tido.cooldownUntil = 0;
    backends.tido.failCount = 0;
    backends.tido.lastError = '';
    searchCache.clear(); streamCache.clear(); metaCache.clear();
    saveConfig(state.config);
    info('tido manifest regenerated', { url: redactUrl(manifestUrl), count: regenState.count, quality: settings.quality });
    return { ok: true, url: manifestUrl };
  } catch (e) {
    regenState.lastAt = Date.now();
    regenState.lastResult = String(e.message || e).slice(0, 200);
    warn('tido regen error', { error: redactText(regenState.lastResult) });
    return { ok: false, reason: regenState.lastResult };
  } finally {
    regenState.busy = false;
  }
}

async function tidoSearch(q, xea) {
  const t = tidoTarget();
  if (!t) throw new HttpError(503, 'no Tido URL configured');
  const p = tidoSettingsParams();
  p.set('q', q);
  const url = `${t.base}/${t.token ? t.token + '/' : ''}search?${p.toString()}`;
  const res = await fetchJson(url, { timeoutMs: 12000, headers: tidoClientHeaders(xea) });
  if (res.status !== 200 || !res.json) {
    throw new HttpError(res.status === 403 ? 503 : 502, `Tido search failed (${res.status})`);
  }
  const tracks = Array.isArray(res.json.tracks) ? res.json.tracks : [];
  const out = [];
  for (const item of tracks) {
    if (!item || item.id === undefined) continue;
    if (state.config.hideExplicit && item.explicit) continue;
    const meta = {
      kind: 'track',
      backend: 'tido',
      tidalId: String(item.id),
      tidoTrackId: String(item.id),
      title: String(item.title || ''),
      artist: String(item.artist || ''),
      album: String(item.album || ''),
      duration: parseInt(item.duration || 0, 10) || 0,
      explicit: !!item.explicit,
      isrc: item.isrc ? String(item.isrc) : null,
      cover: null,
      artworkURL: typeof item.artworkURL === 'string' ? item.artworkURL : null,
      musicVideoId: null,
      imageId: null,
    };
    out.push(meta);
  }
  return out;
}

async function tidoStreamTrack(meta, xea) {
  const t = tidoTarget();
  if (!t) throw new HttpError(503, 'no Tido URL configured');
  const p = tidoSettingsParams();
  const url = `${t.base}/${t.token ? t.token + '/' : ''}stream/${encodeURIComponent(meta.tidoTrackId)}?${p.toString()}`;
  const res = await fetchJson(url, { timeoutMs: 20000, headers: tidoClientHeaders(xea) });
  if (res.status !== 200 || !res.json) {
    throw new HttpError(res.status === 404 ? 404 : 502, `Tido stream failed (${res.status})`);
  }
  return res.json;
}

// ---------------------------------------------------------------------------
// hifi-api backend
// ---------------------------------------------------------------------------

const HIFI_QUALITY_LADDERS = {
  MAX: ['ATMOS', 'HI_RES_LOSSLESS', 'LOSSLESS', 'HIGH', 'LOW'],
  HIRES: ['HI_RES_LOSSLESS', 'LOSSLESS', 'HIGH', 'LOW'],
  LOSSLESS: ['LOSSLESS', 'HIGH', 'LOW'],
  HIGH: ['HIGH', 'LOW'],
  LOW: ['LOW'],
};

function hifiBase(b) { return b.url.replace(/\/+$/, ''); }

async function hifiSearchTracks(b, q) {
  const url = `${hifiBase(b)}/search/?s=${encodeURIComponent(q)}&limit=${SEARCH_LIMIT}&offset=0`;
  const res = await fetchJson(url, { timeoutMs: 12000 });
  if (res.status !== 200 || !res.json) throw new HttpError(502, `hifi-api search failed (${res.status})`);
  const payload = res.json.data && typeof res.json.data === 'object' ? res.json.data : res.json;
  const items = Array.isArray(payload.items) ? payload.items : [];
  const out = [];
  for (const item of items) {
    if (!item || item.id === undefined) continue;
    if (state.config.hideExplicit && item.explicit) continue;
    out.push(metaFromTidalItem(item, 'hifi', b.url));
  }
  return out;
}

async function hifiSearchVideos(b, q, limit = 20) {
  const url = `${hifiBase(b)}/search/?v=${encodeURIComponent(q)}&limit=${limit}&offset=0`;
  const res = await fetchJson(url, { timeoutMs: 12000 });
  if (res.status !== 200 || !res.json) throw new HttpError(502, `hifi-api video search failed (${res.status})`);
  const payload = res.json.data && typeof res.json.data === 'object' ? res.json.data : res.json;
  const hits = payload.topHits || payload.items || [];
  const out = [];
  for (const hit of hits) {
    const item = hit && hit.value && typeof hit.value === 'object' ? hit.value : hit;
    if (!item || item.id === undefined) continue;
    if (state.config.hideExplicit && item.explicit) continue;
    const artist = (item.artists && item.artists.map((a) => a.name).filter(Boolean).join(', '))
      || (item.artist && (item.artist.name || item.artist)) || '';
    out.push({
      kind: 'video',
      backend: 'hifi',
      hifiUrl: b.url,
      tidalId: String(item.id),
      videoId: String(item.id),
      tidoTrackId: String(item.id),
      title: String(item.title || ''),
      artist,
      album: item.album && item.album.title ? String(item.album.title) : '',
      duration: parseInt(item.duration || 0, 10) || 0,
      explicit: !!item.explicit,
      cover: item.album && item.album.cover ? String(item.album.cover) : null,
      artworkURL: typeof item.artworkURL === 'string' ? item.artworkURL : null,
      imageId: item.imageId != null ? String(item.imageId) : null,
    });
  }
  return out;
}

async function hifiTrackInfo(b, tidalId) {
  const url = `${hifiBase(b)}/info/?id=${encodeURIComponent(tidalId)}`;
  const res = await fetchJson(url, { timeoutMs: 12000 });
  if (res.status !== 200 || !res.json) throw new HttpError(502, `hifi-api info failed (${res.status})`);
  const item = res.json.data && typeof res.json.data === 'object' ? res.json.data : res.json;
  return metaFromTidalItem(item, 'hifi', b.url);
}

/**
 * A playback request may answer 202 {status:"pending", requestId, statusUrl}.
 * Poll statusUrl until the original playback response arrives.
 */
async function playbackGet(b, pathAndQuery, totalWaitMs = 25000) {
  const first = await fetchJson(`${hifiBase(b)}${pathAndQuery}`, { timeoutMs: 20000 });
  let payload = first.json;
  let status = first.status;
  const started = Date.now();
  while (
    (status === 202 || (payload && payload.status === 'pending'))
    && Date.now() - started < totalWaitMs
  ) {
    const statusUrl = payload && (payload.statusUrl || payload.cancelUrl);
    const requestId = payload && payload.requestId;
    if (!statusUrl && !requestId) break;
    const pollUrl = statusUrl ? (statusUrl.startsWith('http') ? statusUrl : hifiBase(b) + statusUrl)
      : `${hifiBase(b)}/playback/requests/${requestId}`;
    const retryAfter = parseFloat(first.headers && first.headers.get ? first.headers.get('retry-after') : '') || 1.5;
    await sleep(Math.min(3000, Math.max(500, retryAfter * 1000)));
    const poll = await fetchJson(pollUrl, { timeoutMs: 10000 });
    status = poll.status;
    payload = poll.json;
    if (payload && (payload.status === 'completed' || payload.response)) {
      payload = payload.response && typeof payload.response === 'object' ? payload.response : payload;
      return payload;
    }
    if (payload && payload.status && payload.status !== 'pending' && payload.status !== 'processing'
      && payload.status !== 'completed') {
      throw new HttpError(502, `hifi-api playback request ${payload.status}`);
    }
    if (payload && payload.status === 'completed') return payload;
  }
  if (payload && (payload.status === 'pending' || status === 202)) {
    const err = new Error('hifi-api playback queue still busy');
    err.status = 503;
    err.retryAfter = 10;
    throw err;
  }
  if (!payload) throw new HttpError(502, 'hifi-api playback gave no JSON');
  return payload;
}

/**
 * Decode a Tidal playbackinfo manifest (v1 /track/ style).
 * Returns {url, manifest, codec, container, sampleRate, bitDepth, bitrate, encryption}
 * manifest is "none" for direct files, "dash"/"hls" for adaptive manifests.
 */
function decodePlaybackManifest(playback) {
  const manifestStr = playback && playback.manifest;
  const mime = String((playback && playback.manifestMimeType) || '');
  if (typeof manifestStr !== 'string' || !manifestStr) return null;
  if (/^https?:\/\//i.test(manifestStr)) {
    return { url: manifestStr, manifest: 'none', codec: null, container: null };
  }
  let rawText;
  try {
    rawText = Buffer.from(manifestStr, 'base64').toString('utf8');
  } catch {
    return null;
  }
  if (mime.includes('tidal.bts')) {
    let mj;
    try { mj = JSON.parse(rawText); } catch { return null; }
    const urls = mj.urls || mj.urlsList || [];
    if (!Array.isArray(urls) || !urls.length) return null;
    const { codec, container } = codecFromBts(mj);
    return {
      url: String(urls[0]),
      manifest: 'none',
      codec, container,
      sampleRate: mj.sampleRate || playback.sampleRate || 0,
      bitDepth: mj.bitDepth || playback.bitDepth || 0,
      encryption: mj.encryptionType || 'NONE',
      mimeType: mj.mimeType || '',
    };
  }
  if (mime.toLowerCase().includes('dash')) {
    return { url: null, manifest: 'dash', mpd: rawText, codec: null, container: null };
  }
  if (rawText.trimStart().startsWith('<?xml') || rawText.trimStart().startsWith('<MPD')) {
    return { url: null, manifest: 'dash', mpd: rawText, codec: null, container: null };
  }
  // JSON manifest without bts mime
  try {
    const mj = JSON.parse(rawText);
    const urls = mj.urls || mj.urlsList || [];
    if (Array.isArray(urls) && urls.length) {
      const { codec, container } = codecFromBts(mj);
      return {
        url: String(urls[0]), manifest: 'none', codec, container,
        sampleRate: mj.sampleRate || playback.sampleRate || 0,
        bitDepth: mj.bitDepth || playback.bitDepth || 0,
        encryption: mj.encryptionType || 'NONE',
      };
    }
  } catch { /* not JSON */ }
  if (mime.toLowerCase().includes('hls') || rawText.trimStart().startsWith('#EXTM3U')) {
    return { url: null, manifest: 'hls', raw: rawText, codec: null, container: null };
  }
  return null;
}

function codecFromBts(mj) {
  const codecs = String(mj.codecs || '');
  const mime = String(mj.mimeType || '');
  if (codecs.includes('flac') || mime.includes('flac')) return { codec: 'flac', container: 'flac' };
  if (codecs.includes('eac3') || codecs.includes('ec-3') || codecs.includes('joc')) return { codec: 'eac3_joc', container: 'mp4' };
  if (codecs.includes('alac')) return { codec: 'alac', container: 'mp4' };
  if (codecs.includes('mp4a') || codecs.includes('aac') || mime.includes('mp4')) return { codec: 'aac', container: 'mp4' };
  if (codecs.includes('mp3') || mime.includes('mpeg')) return { codec: 'mp3', container: 'mp3' };
  return { codec: null, container: null };
}

const TIER_GRANTED = { HI_RES_LOSSLESS: 4, LOSSLESS: 3, HIGH: 2, LOW: 1 };

function grantedRank(playback, decoded) {
  const q = String((playback && playback.audioQuality) || '');
  if (TIER_GRANTED[q]) return TIER_GRANTED[q];
  if (decoded && decoded.codec === 'eac3_joc') return 5; // Atmos
  if (decoded && decoded.codec === 'flac') {
    return decoded.sampleRate && decoded.sampleRate > 48000 ? 4 : 3;
  }
  return 2;
}

/** Resolve one audio tier for a track via hifi-api. Returns decoded info or null. */
async function hifiResolveTier(b, tidalId, tier, immersive) {
  if (tier === 'ATMOS') {
    const payload = await playbackGet(b,
      `/track/?id=${encodeURIComponent(tidalId)}&quality=HI_RES_LOSSLESS&playbackmode=STREAM&assetpresentation=FULL&immersiveaudio=true`);
    const playback = payload.data && payload.data.manifest !== undefined ? payload.data : payload;
    const decoded = decodePlaybackManifest(playback);
    if (!decoded || decoded.encryption !== 'NONE' && decoded.encryption) return null;
    if (decoded && decoded.codec === 'eac3_joc' && decoded.url) {
      return { ...decoded, tier: 'ATMOS', playback };
    }
    if (decoded && decoded.manifest === 'dash' && decoded.mpd) {
      const direct = extractDirectUrlFromMpd(decoded.mpd);
      if (direct) return { ...decoded, url: direct, manifest: 'none', tier: 'ATMOS' };
      // Atmos DASH with no direct URL: not directly playable, try next tier
      return null;
    }
    return null; // immersive copy not available
  }
  if (tier === 'HI_RES_LOSSLESS' || tier === 'LOSSLESS') {
    const formats = tier === 'HI_RES_LOSSLESS' ? ['FLAC_HIRES', 'FLAC'] : ['FLAC'];
    const params = formats.map((f) => `formats=${f}`).join('&');
    try {
      const payload = await playbackGet(b,
        `/trackManifests/?id=${encodeURIComponent(tidalId)}&${params}&adaptive=false&manifestType=MPEG_DASH&uriScheme=HTTPS&usage=PLAYBACK`);
      // JSON:API shape: data.attributes.uri (up to 3 levels of "data")
      let inner = payload;
      for (let i = 0; i < 3 && inner && typeof inner === 'object' && !inner.attributes; i++) {
        inner = inner.data;
      }
      const attrs = inner && typeof inner === 'object' && inner.attributes ? inner.attributes : null;
      if (attrs && typeof attrs.uri === 'string' && attrs.uri.startsWith('http')) {
        const granted = String(attrs.audioQuality || '');
        // uri is an MPD; with adaptive=false it usually holds a single file.
        const mpdText = await fetchText(attrs.uri);
        if (mpdText) {
          const direct = extractDirectUrlFromMpd(mpdText);
          const dash = !direct && /<(?:\w+:)?(MPD|SegmentTemplate)/i.test(mpdText);
          if (direct) {
            return {
              url: direct, manifest: 'none', codec: 'flac', container: 'flac',
              sampleRate: parseInt(attrs.sampleRate || 0, 10) || 0,
              bitDepth: parseInt(attrs.bitDepth || 0, 10) || 0,
              tier, granted, encryption: 'NONE',
            };
          }
          if (dash) {
            return { url: attrs.uri, manifest: 'dash', codec: 'flac', container: 'fmp4', tier, granted };
          }
        }
        // MPD fetch failed; pass the manifest URL through as DASH
        return { url: attrs.uri, manifest: 'dash', codec: 'flac', container: 'fmp4', tier, granted };
      }
    } catch (e) {
      if (e.status && e.status >= 500 && e.status < 600 && e.status !== 502) throw e;
      debug('trackManifests failed, falling back to /track/', { err: redactText(String(e.message || e)) });
    }
    // fall back to v1 /track
    const payload = await playbackGet(b,
      `/track/?id=${encodeURIComponent(tidalId)}&quality=${tier}&playbackmode=STREAM&assetpresentation=FULL&immersiveaudio=false`);
    const playback = payload.data && payload.data.manifest !== undefined ? payload.data : payload;
    const decoded = decodePlaybackManifest(playback);
    if (!decoded) return null;
    if (decoded.encryption && decoded.encryption !== 'NONE') return null;
    if (decoded.manifest === 'dash') {
      if (decoded.mpd) {
        const direct = extractDirectUrlFromMpd(decoded.mpd);
        if (direct) return { ...decoded, url: direct, manifest: 'none', tier };
      }
      return null; // adaptive-only for a lossless tier: skip
    }
    return { ...decoded, tier, playback };
  }
  // HIGH / LOW
  const payload = await playbackGet(b,
    `/track/?id=${encodeURIComponent(tidalId)}&quality=${tier}&playbackmode=STREAM&assetpresentation=FULL&immersiveaudio=false`);
  const playback = payload.data && payload.data.manifest !== undefined ? payload.data : payload;
  const decoded = decodePlaybackManifest(playback);
  if (!decoded) return null;
  if (decoded.encryption && decoded.encryption !== 'NONE') return null;
  if (decoded.manifest === 'dash') {
    if (decoded.mpd) {
      const direct = extractDirectUrlFromMpd(decoded.mpd);
      if (direct) return { ...decoded, url: direct, manifest: 'none', tier };
    }
    return null; // DASH for lossy tiers: CDN segment auth is short-lived, skip
  }
  return { ...decoded, tier, playback };
}

async function fetchText(url, timeoutMs = 12000) {
  try {
    const res = await fetchUrl(url, { timeoutMs, headers: { accept: '*/*' } });
    if (res.status !== 200) return null;
    return await res.text();
  } catch {
    return null;
  }
}

/** Extract a direct file URL from a DASH MPD (single-file SegmentBase shape). */
function extractDirectUrlFromMpd(mpd) {
  if (!mpd) return null;
  if (/<(?:\w+:)?SegmentTemplate/i.test(mpd) && !/<(?:\w+:)?SegmentBase/i.test(mpd)) return null;
  const re = /<(?:\w+:)?BaseURL[^>]*>\s*(https?:\/\/[^\s<]+)\s*<\/(?:\w+:)?BaseURL>/g;
  const urls = [];
  let m;
  while ((m = re.exec(mpd))) urls.push(m[1]);
  for (const u of urls) {
    if (/\.(flac|m4a|mp4|aac|mp3|ogg|ts)(\?|$)/i.test(u)) return u;
  }
  return urls.length ? urls[0] : null;
}

/** Resolve a music video stream via hifi-api. */
async function hifiResolveVideo(b, meta) {
  let videoId = meta.videoId || meta.musicVideoId || null;
  if (!videoId) {
    // find the official video by searching videos for title + artist
    const query = `${meta.title} ${meta.artist}`.trim();
    if (!query) return null;
    const vids = await hifiSearchVideos(b, query, 12);
    const wantTitle = meta.title.toLowerCase();
    const wantArtist = meta.artist.toLowerCase();
    const duration = meta.duration || 0;
    let best = null;
    let bestScore = -1;
    for (const v of vids) {
      const t = v.title.toLowerCase().replace(/\s*\(.*?\)\s*/g, ' ');
      const a = v.artist.toLowerCase();
      let score = 0;
      if (t.includes(wantTitle) || wantTitle.includes(t)) score += 2;
      if (a.includes(wantArtist) || wantArtist.includes(a)) score += 2;
      if (duration && v.duration && Math.abs(v.duration - duration) <= 5) score += 1;
      if (score > bestScore) { bestScore = score; best = v; }
    }
    if (best && bestScore >= 3) videoId = best.videoId;
    else return null;
  }
  const payload = await playbackGet(b,
    `/video/?id=${encodeURIComponent(videoId)}&quality=${state.config.videoQuality}&mode=STREAM&presentation=FULL`);
  const playback = payload.video && payload.video.manifest !== undefined ? payload.video
    : (payload.data && payload.data.manifest !== undefined ? payload.data : payload);
  const decoded = decodePlaybackManifest(playback);
  if (!decoded) return null;
  if (decoded.encryption && decoded.encryption !== 'NONE') return null;
  if (decoded.manifest === 'dash') {
    if (decoded.mpd) {
      const direct = extractDirectUrlFromMpd(decoded.mpd);
      if (direct) return { ...decoded, url: direct, manifest: 'none', isVideo: true };
    }
    return null; // DRM/adaptive video: not playable through this bridge
  }
  return { ...decoded, isVideo: true };
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Monochrome tracks backend (tracks.monochrome.st — github.com/monochrome-music)
// Protocol from js/tracks-api.js: GET /search/tracks?q=, GET /track/{id} →
// direct FLAC bytes with byte-range support; artwork is an absolute URL.
// ---------------------------------------------------------------------------

function monoBase() { return String(state.config.monoUrl || '').replace(/\/+$/, ''); }

async function monoSearch(q) {
  const base = monoBase();
  if (!base) throw new HttpError(503, 'monochrome backend disabled');
  const url = `${base}/search/tracks?q=${encodeURIComponent(q)}&limit=${SEARCH_LIMIT}`;
  const res = await fetchJson(url, { timeoutMs: 12000 });
  if (res.status !== 200 || !res.json) throw new HttpError(502, `monochrome search failed (${res.status})`);
  const items = Array.isArray(res.json.tracks) ? res.json.tracks : [];
  const out = [];
  for (const item of items) {
    if (!item || item.trackId === undefined) continue;
    if (item.playable === false) continue;
    if (state.config.hideExplicit && item.explicit) continue;
    const durationRaw = parseInt(item.duration || 0, 10) || 0;
    const duration = durationRaw > 1000 ? Math.round(durationRaw / 1000) : durationRaw;
    const artist = (Array.isArray(item.artistNames) ? item.artistNames.join(', ') : '')
      || (Array.isArray(item.artists) ? item.artists.map((a) => a.name || a.displayName).filter(Boolean).join(', ') : '')
      || '';
    out.push({
      kind: 'track',
      backend: 'mono',
      monoTrackId: String(item.trackId),
      tidalId: '',
      tidoTrackId: '',
      title: String(item.title || ''),
      artist,
      album: String(item.albumTitle || item.releaseTitle || ''),
      duration,
      explicit: !!item.explicit,
      isrc: item.isrc ? String(item.isrc) : null,
      cover: null,
      artworkURL: typeof item.artwork === 'string' && /^https?:\/\//.test(item.artwork) ? item.artwork : null,
      musicVideoId: null,
      imageId: null,
    });
  }
  return out;
}

/**
 * Parse a FLAC STREAMINFO header (first ~64 bytes of the file) into
 * {sampleRate, bitDepth}. Bit layout after "fLaC" + 4-byte block header:
 * 16+16+24+24 bits of block/frame sizes, then 20 bits sample rate,
 * 3 bits channels-1, 5 bits bits-per-sample-1, 36 bits total samples.
 */
function parseFlacStreamInfo(buf) {
  try {
    if (!buf || buf.length < 42) return null;
    if (buf[0] !== 0x66 || buf[1] !== 0x4c || buf[2] !== 0x61 || buf[3] !== 0x43) return null; // "fLaC"
    // first metadata block must be STREAMINFO (type 0, last-flag bit 0x80)
    if ((buf[4] & 0x7f) !== 0) return null;
    const body = buf.subarray(8); // 4 magic + 4 block header
    let bitPos = 80; // skip minBS/maxBS/minFS/maxFS
    const read = (n) => {
      let v = 0;
      for (let i = 0; i < n; i++) {
        const byte = body[bitPos >> 3] || 0;
        const bit = (byte >> (7 - (bitPos & 7))) & 1;
        v = (v << 1) | bit;
        bitPos += 1;
      }
      return v;
    };
    const sampleRate = read(20);
    const channels = read(3) + 1;
    const bitDepth = read(5) + 1;
    const totalSamples = read(36);
    if (!sampleRate || sampleRate > 1048575) return null;
    return { sampleRate, channels, bitDepth, totalSamples };
  } catch {
    return null;
  }
}

/**
 * Resolve a monochrome stream. The /track/{id} URL serves FLAC directly, so a
 * single ranged GET doubles as the probe: Content-Range gives the file size,
 * the first bytes carry the FLAC STREAMINFO. One request per resolution —
 * the backend rate-limits (429), so we retry once with the Retry-After hint.
 */
async function monoResolveStream(meta) {
  const base = monoBase();
  const trackId = meta.monoTrackId;
  if (!base || !trackId) return null;
  const url = `${base}/track/${encodeURIComponent(trackId)}`;

  let res = null;
  for (let attempt = 0; attempt < 2; attempt++) {
    const r = await fetchUrl(url, {
      method: 'GET', timeoutMs: 15000,
      headers: { range: 'bytes=0-63', 'accept-encoding': 'identity' },
    });
    if (r.status === 429 && attempt === 0) {
      const ra = parseFloat(r.headers.get('retry-after') || '') || 1.5;
      await sleep(Math.min(5000, ra * 1000));
      try { await r.arrayBuffer(); } catch { /* drain */ }
      continue;
    }
    res = r;
    break;
  }
  if (!res || (res.status !== 200 && res.status !== 206)) {
    const status = res ? res.status : 0;
    try { await res?.arrayBuffer(); } catch { /* drain */ }
    if (status === 429) {
      const err = new Error('monochrome backend rate-limited the stream');
      err.status = 503;
      err.retryAfter = 5;
      throw err;
    }
    throw new HttpError(status === 404 ? 404 : 502, `monochrome stream failed (${status})`);
  }

  let contentLength = parseInt(res.headers.get('content-length') || '0', 10) || 0;
  const contentRange = res.headers.get('content-range') || '';
  const rangeTotal = /\/(\d+)$/.exec(contentRange);
  if (rangeTotal) contentLength = parseInt(rangeTotal[1], 10);

  let codec = 'flac';
  let container = 'flac';
  let sampleRate = 0;
  let bitDepth = 0;
  try {
    const buf = Buffer.from(await res.arrayBuffer());
    const info = parseFlacStreamInfo(buf);
    if (info) {
      sampleRate = info.sampleRate;
      bitDepth = info.bitDepth;
    } else {
      // Not FLAC: sniff the container from the magic bytes so the labels and
      // the /file Content-Type stay honest (the real monochrome serves FLAC
      // today, but never hardcode that assumption).
      const head = buf.subarray(0, 12);
      if (head.length >= 8 && head.subarray(4, 8).toString('latin1') === 'ftyp') {
        codec = 'aac';
        container = 'm4a';
      } else if (head.length >= 3 && (head.subarray(0, 3).toString('latin1') === 'ID3'
        || (head[0] === 0xff && (head[1] & 0xe0) === 0xe0))) {
        codec = 'mpeg';
        container = 'mp3';
      }
    }
  } catch { /* labels stay 0 */ }

  let bitrate = 0;
  if (contentLength && meta.duration) bitrate = Math.round((contentLength * 8) / Math.max(1, meta.duration));
  return {
    upstreamUrl: url,
    url,
    manifest: 'none',
    codec,
    container,
    sampleRate,
    bitDepth,
    bitrate,
    quality: codec === 'flac'
      ? qualityText('flac', sampleRate, bitDepth)
      : (bitrate ? `${Math.round(bitrate / 1000)}kbps` : codec),
    tier: codec === 'flac' ? 'LOSSLESS' : 'HIGH',
  };
}

// Search + stream pipeline with backend failover
// ---------------------------------------------------------------------------

const metaCache = new Cache(4000);          // id -> track metadata
const streamCache = new Cache(300);         // id -> resolved stream info
const searchCache = new Cache(120);         // query -> results
const artCache = new Cache(256, 32 * 1024 * 1024); // artwork bytes

function rememberMeta(id, meta) { metaCache.put(id, meta, META_CACHE_TTL); }

function orderedBackends() {
  const out = [];
  if (tidoTarget()) out.push({ type: 'tido' });
  for (const b of backends.hifi) out.push({ type: 'hifi', b });
  if (state.config.monoEnabled && state.config.monoUrl) out.push({ type: 'mono' });
  return out;
}

function backendAlive(entry) {
  if (entry.type === 'tido') return backends.tido.cooldownUntil < Date.now();
  if (entry.type === 'mono') return backends.mono.cooldownUntil < Date.now();
  return entry.b.cooldownUntil < Date.now();
}

function backendLabel(entry) {
  return entry.type === 'tido' ? backends.tido : entry.type === 'mono' ? backends.mono : entry.b;
}

function noteBackendFailure(entry, err) {
  const label = backendLabel(entry);
  label.cooldownUntil = Date.now() + BACKEND_COOLDOWN;
  label.failCount += 1;
  label.lastError = redactText(String(err.message || err)).slice(0, 300);
  warn(`backend failed: ${nameOf(entry)}`, { error: label.lastError });
  // A failing Tido token may be individually rate-limited/banned — try to
  // mint a fresh manifest (fire-and-forget, internally rate-limited; when
  // the whole upstream is down it no-ops and the ladder serves instead).
  if (entry.type === 'tido' && tidoRegenUrl()) {
    regenerateTidoManifest().catch(() => {});
  }
}

function noteBackendSuccess(entry) {
  const label = backendLabel(entry);
  label.cooldownUntil = 0;
  label.okCount += 1;
  label.lastError = '';
}

/**
 * Search across backends; first healthy backend with results wins.
 * Also builds music-video entries when enabled.
 */
async function searchPipeline(q, { fresh = false, xea, cs } = {}) {
  const cfg = state.config;
  const eff = cs || { quality: cfg.audioQuality, exactQuality: !!cfg.exactQuality, videosEnabled: !!cfg.videosInSearch };
  const key = `${eff.quality}|${cfg.hideExplicit ? 1 : 0}|${eff.videosEnabled ? 1 : 0}|${q.toLowerCase()}`;
  if (!fresh) {
    const hit = searchCache.get(key);
    if (hit) return hit;
  }
  const errors = [];
  let tracks = null;
  let winner = null;
  for (const entry of orderedBackends()) {
    if (!backendAlive(entry)) continue;
    try {
      let list;
      if (entry.type === 'tido') list = await tidoSearch(q, xea);
      else if (entry.type === 'mono') list = await monoSearch(q);
      else list = await hifiSearchTracks(entry.b, q);
      if (list && list.length) {
        tracks = list;
        winner = entry;
        noteBackendSuccess(entry);
        break;
      }
      if (list) { noteBackendSuccess(entry); errors.push({ backend: nameOf(entry), error: 'no results' }); }
    } catch (e) {
      noteBackendFailure(entry, e);
      errors.push({ backend: nameOf(entry), error: String(e.message || e).slice(0, 200) });
    }
  }
  const result = { tracks: tracks || [], winner: winner ? nameOf(winner) : null, errors, cs: eff };
  searchCache.put(key, result, SEARCH_CACHE_TTL);
  return result;
}

function nameOf(entry) {
  if (entry.type === 'tido') return 'Tido';
  if (entry.type === 'mono') return backends.mono.name;
  return entry.b.name;
}

/** Populate metadata for ids we hand to BitChord and build video entries. */
async function materializeResults(result) {
  const cfg = state.config;
  const videosEnabled = result.cs ? result.cs.videosEnabled : !!cfg.videosInSearch;
  const out = [];
  const seen = new Set();
  for (const meta of result.tracks) {
    const id = `t${meta.monoTrackId || meta.tidalId}`;
    if (seen.has(id)) continue;
    seen.add(id);
    rememberMeta(id, { ...meta, kind: 'track' });
    out.push({ id, meta });
  }
  if (videosEnabled && out.length) {
    // Videos as their own searchable entries so BitChord can list & play them.
    const videoEntries = [];
    if (result.winner === 'Tido') {
      // Tido attaches the official video to a track's stream response, so
      // video ids here are track ids resolved lazily.
      for (const { id, meta } of out.slice(0, 12)) {
        const vid = `v${meta.tidalId}`;
        if (seen.has(vid)) continue;
        seen.add(vid);
        rememberMeta(vid, { ...meta, kind: 'video-of-track' });
        videoEntries.push({ id: vid, meta: { ...meta, kind: 'video-of-track', title: `[Video] ${meta.title}` } });
      }
    } else if (result.winner && result.winner !== 'monochrome') {
      const hifiEntry = backends.hifi.find((b) => b.name === result.winner);
      if (hifiEntry) {
        try {
          const vids = await hifiSearchVideos(hifiEntry, queryForVideos(result.tracks), 16);
          for (const v of vids.slice(0, 12)) {
            const vid = `v${v.videoId}`;
            if (seen.has(vid)) continue;
            seen.add(vid);
            rememberMeta(vid, v);
            videoEntries.push({ id: vid, meta: { ...v, title: `[Video] ${v.title}` } });
          }
        } catch (e) {
          debug('video search failed', { error: redactText(String(e.message || e)).slice(0, 200) });
        }
      }
      // also allow per-track official videos for the top hits
      for (const { meta } of out.slice(0, 8)) {
        const vid = `vt${meta.tidalId}`;
        if (seen.has(vid)) continue;
        if (meta.musicVideoId) {
          seen.add(vid);
          rememberMeta(vid, { ...meta, kind: 'video-of-track', videoId: meta.musicVideoId });
          videoEntries.push({ id: vid, meta: { ...meta, kind: 'video-of-track', videoId: meta.musicVideoId, title: `[Video] ${meta.title}` } });
        }
      }
    }
    for (const v of videoEntries) out.push(v);
  }
  return out.slice(0, 60);
}

function queryForVideos(tracks) {
  if (!tracks.length) return '';
  const first = tracks[0];
  return `${first.title} ${first.artist}`.trim().slice(0, 120);
}

/**
 * Resolve a stream for an id. Tries the metadata's backend first, then all
 * others. Returns { url, manifest, codec, container, sampleRate, bitDepth,
 * bitrate, quality, kind, upstreamUrl, expiresAt } or throws HttpError.
 */
async function resolveStream(id, { xea, quality, exactQuality } = {}) {
  const cfg = state.config;
  const eff = {
    quality: QUALITIES.includes(quality) ? quality : cfg.audioQuality,
    exactQuality: exactQuality === undefined ? !!cfg.exactQuality : !!exactQuality,
  };
  // Cache per effective quality: BitChord can re-request the same track at a
  // different app setting and must not be served the stale rung's answer.
  const cacheKey = `${id}|${eff.quality}|${eff.exactQuality ? 1 : 0}`;
  const cached = streamCache.get(cacheKey);
  if (cached) return cached;
  let meta = metaCache.get(id);
  if (!meta) {
    // unknown id: BitChord may send an id from an older session. Numeric ids
    // are re-routed through the first configured backend.
    const tid = String(id).replace(/^[tv]/, '');
    if (!/^[0-9A-Za-z_-]{1,128}$/.test(id) || !/^\d+$/.test(tid)) throw new HttpError(404, 'unknown id');
    const primary = orderedBackends()[0];
    meta = primary && primary.type === 'mono'
      ? { kind: id.startsWith('v') ? 'video-of-track' : 'track', monoTrackId: tid, tidalId: '', title: '', artist: '', backend: 'mono' }
      : { kind: id.startsWith('v') ? 'video-of-track' : 'track', tidalId: tid, tidoTrackId: tid, title: '', artist: '', backend: 'tido' };
    rememberMeta(id, meta);
  }
  const wantVideo = id.startsWith('v');
  const attempts = [];
  if (meta.backend === 'hifi' && meta.hifiUrl) {
    const b = backends.hifi.find((x) => x.url === meta.hifiUrl);
    if (b) attempts.push({ type: 'hifi', b });
  } else if (meta.backend === 'tido' && tidoTarget()) {
    attempts.push({ type: 'tido' });
  } else if (meta.backend === 'mono' && state.config.monoEnabled) {
    attempts.push({ type: 'mono' });
  }
  for (const e of orderedBackends()) {
    const dupe = attempts.some((a) => a.type === e.type && (!e.b || !a.b || a.b.url === e.b.url));
    if (!dupe) attempts.push(e);
  }

  const errors = [];
  for (const entry of attempts) {
    if (entry.type === 'tido' && !tidoTarget()) continue;
    if (entry.type === 'mono' && (!state.config.monoEnabled || !meta.monoTrackId)) continue;
    try {
      const resolved = wantVideo
        ? await resolveVideoVia(entry, meta, xea)
        : await resolveAudioVia(entry, meta, eff, xea);
      if (resolved) {
        noteBackendSuccess(entry);
        const info = { ...resolved, id, kind: wantVideo ? 'video' : 'audio', eff, cacheKey, resolvedAt: Date.now() };
        streamCache.put(cacheKey, info, STREAM_CACHE_TTL);
        return info;
      }
      errors.push({ backend: nameOf(entry), error: 'no stream' });
    } catch (e) {
      if (e.status === 404) throw e; // track genuinely gone everywhere? try others too
      noteBackendFailure(entry, e);
      errors.push({ backend: nameOf(entry), error: String(e.message || e).slice(0, 200) });
    }
  }
  // hifi fallback by numeric id when Tido id differs
  throw new HttpError(502, 'stream resolution failed on every backend', { errors });
}

async function resolveAudioVia(entry, meta, eff, xea) {
  if (entry.type === 'tido') {
    const stream = await tidoStreamTrack(meta, xea);
    return tidoStreamToInfo(stream);
  }
  if (entry.type === 'mono') {
    return monoResolveStream(meta);
  }
  const b = entry.b;
  const tidalId = numericTidalId(meta);
  if (!tidalId) return null;
  const ladder = HIFI_QUALITY_LADDERS[eff.quality] || HIFI_QUALITY_LADDERS.MAX;
  const tiers = eff.exactQuality ? [ladder[0]] : ladder;
  for (const tier of tiers) {
    try {
      const resolved = await hifiResolveTier(b, tidalId, tier, tier === 'ATMOS');
      if (resolved && resolved.url) return hifiTierToInfo(resolved);
    } catch (e) {
      if (e.status === 404) return null;
      debug(`tier ${tier} failed`, { error: redactText(String(e.message || e)).slice(0, 200) });
    }
  }
  return null;
}

async function resolveVideoVia(entry, meta, xea) {
  if (entry.type === 'tido') {
    // Tido attaches the official video to the track's stream response.
    const stream = await tidoStreamTrack(meta, xea);
    const video = pickTidoVideo(stream);
    if (!video || !video.url) return null;
    return {
      upstreamUrl: video.url,
      url: video.url, // rewritten to our file link by the caller layer
      manifest: 'none',
      codec: 'aac',
      container: 'mp4',
      sampleRate: 0, bitDepth: 0, bitrate: 0,
      quality: tidoVideoQualityLabel(video),
      isVideo: true,
    };
  }
  const b = entry.b;
  const resolved = await hifiResolveVideo(b, meta);
  if (!resolved || !resolved.url) return null;
  return {
    upstreamUrl: resolved.url,
    url: resolved.url,
    manifest: 'none',
    codec: 'aac',
    container: 'mp4',
    sampleRate: 0, bitDepth: 0, bitrate: 0,
    quality: `${cfgVideoHeight()}p`,
    isVideo: true,
  };
}

function cfgVideoHeight() {
  return { HIGH: 1080, MEDIUM: 720, LOW: 480 }[state.config.videoQuality] || 1080;
}

function pickTidoVideo(stream) {
  if (!stream || typeof stream !== 'object') return null;
  if (stream.video && stream.video.url) return stream.video;
  if (Array.isArray(stream.renditions) && stream.renditions.length) {
    const heights = { HIGH: 1080, MEDIUM: 720, LOW: 480 };
    const want = heights[state.config.videoQuality] || 1080;
    let best = null;
    for (const r of stream.renditions) {
      if (!r || !r.url) continue;
      const h = parseInt(r.height || 0, 10) || 0;
      if (h <= want && (!best || h > best.height)) best = { ...r, height: h };
    }
    if (!best) best = stream.renditions[0];
    return { url: best.url, height: best.height };
  }
  return null;
}

function tidoVideoQualityLabel(video) {
  const h = parseInt(video.height || 0, 10);
  return h ? `${h}p` : `${cfgVideoHeight()}p`;
}

/** Map a Tido (Eclipse) stream response to our stream info. */
function tidoStreamToInfo(stream) {
  if (!stream || !stream.url) throw new HttpError(404, 'Tido returned no stream url');
  const codec = String(stream.codec || stream.format || '').toLowerCase() || null;
  const { codec: c2, container } = codec
    ? { codec, container: containerForCodec(codec) }
    : { codec: null, container: null };
  return {
    upstreamUrl: stream.url,
    url: stream.url,
    manifest: ['none', 'dash', 'hls'].includes(stream.manifest) ? stream.manifest : 'none',
    codec: c2,
    container: stream.container || container,
    sampleRate: parseInt(stream.sampleRate || 0, 10) || 0,
    bitDepth: parseInt(stream.bitDepth || 0, 10) || 0,
    bitrate: parseInt(stream.bitrate || 0, 10) || 0,
    quality: String(stream.quality || qualityText(c2, stream.sampleRate, stream.bitDepth)),
  };
}

function containerForCodec(codec) {
  if (codec === 'flac') return 'flac';
  if (codec === 'mp3') return 'mp3';
  if (['aac', 'he-aac', 'alac', 'eac3_joc'].includes(codec)) return 'mp4';
  if (['opus', 'vorbis'].includes(codec)) return 'ogg';
  return null;
}

function hifiTierToInfo(resolved) {
  const codec = resolved.codec || 'flac';
  const sampleRate = resolved.sampleRate || 0;
  const bitDepth = resolved.bitDepth || 0;
  return {
    upstreamUrl: resolved.url,
    url: resolved.url,
    manifest: resolved.manifest || 'none',
    codec,
    container: resolved.container || containerForCodec(codec) || 'flac',
    sampleRate,
    bitDepth,
    bitrate: estimateBitrate(codec, sampleRate, bitDepth, resolved.tier),
    quality: qualityText(codec, sampleRate, bitDepth, resolved.tier),
    granted: resolved.granted,
  };
}

function estimateBitrate(codec, sampleRate, bitDepth, tier) {
  if (codec === 'eac3_joc') return 768000;
  if (codec === 'flac') {
    if (sampleRate && bitDepth) return Math.round(sampleRate * bitDepth * 2 * 0.6);
    return 900000;
  }
  if (tier === 'HIGH') return 320000;
  if (tier === 'LOW') return 96000;
  return 0;
}

function qualityText(codec, sampleRate, bitDepth, tier) {
  if (codec === 'eac3_joc') return 'Dolby Atmos (EAC3 JOC)';
  if (codec === 'flac') {
    let out = 'lossless';
    if (bitDepth > 0) out += ` ${bitDepth}-bit`;
    if (sampleRate > 0) out += ` ${(sampleRate / 1000).toFixed(sampleRate % 1000 ? 1 : 0).replace(/\.0$/, '')}kHz`;
    return out;
  }
  if (tier === 'HIGH') return '320kbps';
  if (tier === 'LOW') return '96kbps';
  return codec || '';
}

function numericTidalId(meta) {
  const raw = String(meta.tidalId || meta.tidoTrackId || '');
  if (/^\d+$/.test(raw)) return raw;
  const digits = raw.replace(/\D/g, '');
  return digits || null;
}

// ---------------------------------------------------------------------------
// BitChord protocol JSON shapes (mirrors bitchord-selfhosted-addon)
// ---------------------------------------------------------------------------

const ID_PATTERN = /^[0-9A-Za-z_-]{1,128}$/;

function manifestJSON(cfg) {
  return {
    id: `app.bitchord-tido.${cfg.slug}`,
    name: cfg.label || 'Tido Tidal Hi-Res',
    version: VERSION,
    description: 'Tidal search & Hi-Res / Dolby Atmos / music-video streaming through Tido and hifi-api',
    resources: ['search', 'stream'],
    types: ['track'],
    contentType: 'music',
  };
}

function trackJSON(base, entry, quality) {
  const { id, meta } = entry;
  const isVideo = id.startsWith('v');
  const lossless = ['MAX', 'HIRES', 'LOSSLESS'].includes(quality || state.config.audioQuality);
  const out = {
    id,
    title: meta.title,
    artist: meta.artist,
    album: meta.album,
    duration: meta.duration || 0,
    artworkURL: `${base}/art/${id}`,
    format: isVideo ? 'm4a' : (lossless ? 'flac' : 'aac'),
    audioQuality: lossless ? 'LOSSLESS' : 'HIGH',
  };
  return out;
}

function streamJSON(base, info) {
  const isVideo = info.kind === 'video';
  const proxied = info.manifest === 'none'; // direct file: serve bytes through /file
  let url = proxied ? `${base}/file/${info.id}` : info.upstreamUrl;
  if (proxied && !isVideo && info.eff
    && (info.eff.quality !== state.config.audioQuality || info.eff.exactQuality !== !!state.config.exactQuality)) {
    // The /file proxy re-resolves from the id, so a client-quality rung rides
    // along as query params (the same settings-transport convention the
    // Eclipse protocol uses everywhere). Default-quality links stay clean.
    url += `?quality=${encodeURIComponent(info.eff.quality)}&exactQuality=${info.eff.exactQuality}`;
  }
  return {
    url,
    format: info.codec || info.container || '',
    quality: info.quality || (isVideo ? 'video' : ''),
    codec: info.codec || '',
    container: info.container || '',
    manifest: info.manifest || 'none',
    sampleRate: info.sampleRate || 0,
    bitDepth: info.bitDepth || 0,
    bitrate: info.bitrate || 0,
  };
}

// ---------------------------------------------------------------------------
// HTTP plumbing
// ---------------------------------------------------------------------------

function writeJSON(res, status, value, extraHeaders) {
  const body = JSON.stringify(value) + '\n';
  const headers = { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body), ...extraHeaders };
  res.writeHead(status, headers);
  res.end(body);
}

function quiet404(res) { res.writeHead(404); res.end(); }

function applyCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Accept-Ranges, Retry-After');
}

function sendError(res, err) {
  const status = err.status && err.status >= 400 && err.status < 600 ? err.status : 500;
  const headers = {};
  if (err.retryAfter) headers['Retry-After'] = String(err.retryAfter);
  writeJSON(res, status, { error: redactText(String(err.message || 'internal error')).slice(0, 300) }, headers);
  if (status >= 500) errorLog('request failed', { status, error: redactText(String(err.message || err)).slice(0, 300) });
}

const FORWARDED_REQ_HEADERS = ['range', 'if-range'];
const FORWARDED_RES_HEADERS = ['content-type', 'content-length', 'content-range', 'accept-ranges', 'etag', 'last-modified'];
// monochrome answers "application/octet-stream" for audio bytes; give
// BitChord a real audio MIME derived from the sniffed codec instead.
const AUDIO_MIME = {
  flac: 'audio/flac', alac: 'audio/mp4', mpeg: 'audio/mpeg', mp3: 'audio/mpeg',
  aac: 'audio/mp4', mp4: 'audio/mp4', eac3_joc: 'audio/eac3',
};

/** Effective quality params carried on our own /file links (see streamJSON). */
function fileLinkEff(req) {
  try {
    const u = new URL(req.url, 'http://localhost');
    const q = u.searchParams.get('quality');
    const x = u.searchParams.get('exactQuality');
    if (!q && x === null) return {};
    return { quality: q || undefined, exactQuality: x === null ? undefined : x === 'true' };
  } catch {
    return {};
  }
}

/**
 * Byte proxy: streams the resolved upstream URL to BitChord with Range
 * support, re-resolving once if the upstream link has expired.
 */
async function handleFile(id, req, res) {
  const eff = fileLinkEff(req);
  const xea = req.headers['x-eclipse-access'];
  let info;
  try {
    info = await resolveStream(id, { xea, ...eff });
  } catch (e) {
    return sendError(res, e);
  }
  if (info.manifest !== 'none' || !info.upstreamUrl) {
    // Adaptive manifests are handed out as their own URL, never proxied here.
    return quiet404(res);
  }
  let upstream = await openUpstream(info.upstreamUrl, req);
  if (upstream && [403, 404, 410].includes(upstream.status)) {
    // CDN link probably expired: re-resolve once with a fresh manifest.
    streamCache.del(info.cacheKey || id);
    try {
      info = await resolveStream(id, { xea, ...eff });
    } catch (e) {
      return sendError(res, e);
    }
    if (!info.upstreamUrl || info.manifest !== 'none') return quiet404(res);
    upstream = await openUpstream(info.upstreamUrl, req);
  }
  if (!upstream) return sendError(res, new HttpError(502, 'upstream unavailable'));
  if (![200, 206, 416].includes(upstream.status)) {
    errorLog('upstream answered the byte request badly', { status: upstream.status });
    upstream.body?.cancel?.();
    return sendError(res, new HttpError(502, `upstream status ${upstream.status}`));
  }
  for (const name of FORWARDED_RES_HEADERS) {
    const v = upstream.headers.get(name);
    if (v) res.setHeader(name, v);
  }
  const ct = String(res.getHeader('Content-Type') || '');
  if (info.codec && AUDIO_MIME[info.codec] && (!ct || /octet-stream/i.test(ct))) {
    res.setHeader('Content-Type', AUDIO_MIME[info.codec]);
  }
  res.setHeader('Cache-Control', 'no-store');
  res.writeHead(upstream.status);
  if (req.method === 'HEAD') {
    upstream.body?.cancel?.();
    res.end();
    return;
  }
  const nodeStream = Readable.fromWeb(upstream.body);
  nodeStream.pipe(res);
  nodeStream.on('error', (e) => {
    debug('stream ended early', { error: redactText(String(e.message || e)).slice(0, 200) });
    res.destroy();
  });
  req.on('close', () => nodeStream.destroy());
}

async function openUpstream(url, req) {
  const headers = { 'accept-encoding': 'identity', accept: '*/*' };
  for (const name of FORWARDED_REQ_HEADERS) {
    const v = req.headers[name];
    if (v) headers[name] = v;
  }
  try {
    let upstream = await fetchUrl(url, { method: req.method, headers, timeoutMs: 30000 });
    // Rate-limited upstreams (tracks.monochrome.st throttles bursts):
    // drain, wait out the hint, and try once more before giving up.
    if (upstream.status === 429) {
      const ra = parseFloat(upstream.headers.get('retry-after') || '') || 1.5;
      try { await upstream.arrayBuffer(); } catch { /* drain */ }
      await sleep(Math.min(5000, ra * 1000));
      upstream = await fetchUrl(url, { method: req.method, headers, timeoutMs: 30000 });
    }
    return upstream;
  } catch (e) {
    debug('upstream fetch failed', { error: redactText(String(e.message || e)).slice(0, 200) });
    return null;
  }
}

/** Artwork proxy with an in-memory cache. */
async function handleArt(id, req, res) {
  const meta = metaCache.get(id);
  if (!meta) return quiet404(res);
  const cached = artCache.get(id);
  if (cached) {
    res.writeHead(200, {
      'Content-Type': cached.contentType,
      'Content-Length': cached.body.length,
      'Cache-Control': 'public, max-age=86400',
    });
    if (req.method !== 'HEAD') res.end(cached.body);
    else res.end();
    return;
  }
  const candidates = [];
  if (meta.artworkURL) candidates.push(meta.artworkURL);
  for (const size of [1280, 640, 80]) {
    const u = tidalCoverUrl(meta.imageId || meta.cover, size);
    if (u) candidates.push(u);
  }
  // hifi-api /cover/ lookup: resolves artwork even when the search item had
  // no cover field (and stashes the URL back into the metadata).
  if (meta.backend === 'hifi' && meta.hifiUrl && meta.tidalId && !meta.artworkURL) {
    try {
      const res = await fetchJson(`${meta.hifiUrl.replace(/\/+$/, '')}/cover/?id=${encodeURIComponent(meta.tidalId)}`, { timeoutMs: 10000 });
      const covers = res.json && Array.isArray(res.json.covers) ? res.json.covers : [];
      const entry = covers[0] || null;
      if (entry) {
        for (const size of ['1280', '640', '80']) {
          if (entry[size]) candidates.push(entry[size]);
        }
        if (entry['1280']) {
          meta.artworkURL = entry['1280'];
          metaCache.put(id, meta, META_CACHE_TTL);
        }
      }
    } catch { /* fall through to candidates */ }
  }
  // try each candidate until one serves bytes
  for (const url of candidates) {
    try {
      const upstream = await fetchUrl(url, { timeoutMs: 10000, headers: { accept: 'image/*' } });
      if (upstream.status !== 200) continue;
      const contentType = upstream.headers.get('content-type') || 'image/jpeg';
      const buf = Buffer.from(await upstream.arrayBuffer());
      if (!buf.length || buf.length > 8 * 1024 * 1024) continue;
      if (contentType.startsWith('image/')) artCache.put(id, { body: buf, contentType }, 24 * 60 * 60 * 1000, buf.length);
      res.writeHead(200, {
        'Content-Type': contentType,
        'Content-Length': buf.length,
        'Cache-Control': 'public, max-age=86400',
      });
      if (req.method !== 'HEAD') res.end(buf);
      else res.end();
      return;
    } catch { /* try next */ }
  }
  quiet404(res);
}

// ---------------------------------------------------------------------------
// Protocol request handlers
// ---------------------------------------------------------------------------

async function handleSearch(q, req, res, base, params) {
  const query = String(q || '').trim();
  if (!query) {
    return writeJSON(res, 200, { tracks: [], albums: [], artists: [], playlists: [] });
  }
  const cs = clientSettings(params, state.config);
  const started = Date.now();
  let result;
  try {
    result = await searchPipeline(query, { xea: req && req.headers['x-eclipse-access'], cs });
  } catch (e) {
    return sendError(res, e);
  }
  const entries = await materializeResults(result);
  const tracks = entries.map((e) => trackJSON(base, e, cs.quality));
  const logged = [...query].slice(0, LOGGED_QUERY_RUNES).join('');
  info('search', {
    q: query.length > LOGGED_QUERY_RUNES ? logged + '…' : logged,
    returned: tracks.length,
    backend: result.winner || 'none',
    quality: cs.quality,
    errors: result.errors.length,
    took: `${Date.now() - started}ms`,
  });
  const body = { tracks, albums: [], artists: [], playlists: [] };
  if (!tracks.length && result.errors.length) {
    // Eclipse-family notice: surfaces backend trouble in the app UI.
    body.notice = {
      id: 'bitdo-backends',
      level: 'warning',
      title: 'No Tidal results',
      message: 'All backends failed — check /setup. ' + result.errors.map((e) => `${e.backend}: ${e.error}`).join('; ').slice(0, 80),
    };
  }
  writeJSON(res, 200, body);
}

async function handleStream(id, req, res, base, params) {
  if (!ID_PATTERN.test(id)) return quiet404(res);
  const cs = clientSettings(params, state.config);
  const started = Date.now();
  try {
    const streamInfo = await resolveStream(id, {
      xea: req.headers['x-eclipse-access'],
      quality: cs.quality,
      exactQuality: cs.exactQuality,
    });
    const descriptor = streamJSON(base, streamInfo);
    info('stream', {
      id,
      want: cs.quality,
      quality: descriptor.quality,
      format: descriptor.format,
      manifest: descriptor.manifest,
      kind: streamInfo.kind,
      took: `${Date.now() - started}ms`,
    });
    return writeJSON(res, 200, descriptor);
  } catch (e) {
    if (e.status === 503 && e.retryAfter) {
      res.setHeader('Retry-After', String(e.retryAfter));
      return writeJSON(res, 503, { error: 'upstream busy, retry shortly' });
    }
    return sendError(res, e);
  }
}

function handleManifest(res, cfg) {
  writeJSON(res, 200, manifestJSON(cfg));
}

function handleSync(res) {
  searchCache.clear();
  streamCache.clear();
  metaCache.clear();
  writeJSON(res, 200, {
    ok: true,
    server: state.config.slug,
    status: 'caches cleared',
    refreshed: new Date().toISOString(),
  });
}

// ---------------------------------------------------------------------------
// Setup web UI + API
// ---------------------------------------------------------------------------

function requireSession(req, res) {
  if (!checkSession(req)) {
    writeJSON(res, 401, { error: 'not logged in' });
    return false;
  }
  return true;
}

function publicConfigView(cfg) {
  return {
    label: cfg.label,
    slug: cfg.slug,
    publicUrl: cfg.publicUrl,
    tidoUrl: cfg.tidoUrl,
    tidoRegenUrl: cfg.tidoRegenUrl,
    hifiApiUrls: cfg.hifiApiUrls,
    audioQuality: cfg.audioQuality,
    exactQuality: cfg.exactQuality,
    hideExplicit: cfg.hideExplicit,
    videosInSearch: cfg.videosInSearch,
    videoQuality: cfg.videoQuality,
    monoUrl: cfg.monoUrl,
    monoEnabled: cfg.monoEnabled,
    hasPassword: !!cfg.passwordHash,
  };
}

function applyConfigPatch(cfg, patch) {
  const str = (v, max = 300) => (typeof v === 'string' ? v.trim().slice(0, max) : undefined);
  const url = (v) => {
    const s = str(v, 500);
    if (s === undefined) return undefined;
    if (s === '') return '';
    try {
      const u = new URL(s);
      if (!/^https?:$/.test(u.protocol)) return undefined;
      return s;
    } catch {
      return undefined;
    }
  };
  const apply = (key, value) => { if (value !== undefined) cfg[key] = value; };
  apply('label', str(patch.label, 64) || undefined);
  apply('publicUrl', url(patch.publicUrl));
  apply('tidoUrl', url(patch.tidoUrl));
  apply('tidoRegenUrl', url(patch.tidoRegenUrl));
  if (Array.isArray(patch.hifiApiUrls)) {
    const list = patch.hifiApiUrls
      .map((s) => (typeof s === 'string' ? s.trim() : ''))
      .filter(Boolean)
      .map((s) => { try { return new URL(s).protocol.startsWith('http') ? s.replace(/\/+$/, '') : null; } catch { return null; } })
      .filter(Boolean)
      .slice(0, 8);
    cfg.hifiApiUrls = list;
  }
  if (patch.audioQuality && QUALITIES.includes(patch.audioQuality)) cfg.audioQuality = patch.audioQuality;
  if (patch.videoQuality && VIDEO_QUALITIES.includes(patch.videoQuality)) cfg.videoQuality = patch.videoQuality;
  apply('monoUrl', url(patch.monoUrl));
  if (typeof patch.monoEnabled === 'boolean') cfg.monoEnabled = patch.monoEnabled;
  for (const key of ['exactQuality', 'hideExplicit', 'videosInSearch']) {
    if (typeof patch[key] === 'boolean') cfg[key] = patch[key];
  }
  if (patch.slug !== undefined) {
    const s = sanitizeSlug(patch.slug);
    if (s && s !== cfg.slug) {
      cfg.slug = s;
      metaCache.clear(); streamCache.clear(); searchCache.clear();
    }
  }
}

async function setupApi(method, pathname, req, res) {
  const cfg = state.config;
  const route = pathname.replace(/^\/setup\/api\/?/, '').replace(/\/+$/, '');
  if (route === 'login' && method === 'POST') {
    const ip = clientIp(req);
    if (loginThrottled(ip)) return sendError(res, new HttpError(429, 'too many attempts, wait 15 minutes'));
    let body = {};
    try { body = JSON.parse(await readBody(req) || '{}'); } catch { /* empty */ }
    if (!cfg.passwordHash) {
      // first run: adopt the password the operator typed
      const pw = String(body.password || '');
      if (pw.length < 6) return sendError(res, new HttpError(400, 'password must be at least 6 characters'));
      cfg.passwordHash = hashPassword(pw);
      saveConfig(cfg);
    } else if (!verifyPassword(String(body.password || ''), cfg.passwordHash)) {
      recordLoginFailure(ip);
      await sleep(400);
      return sendError(res, new HttpError(401, 'wrong password'));
    }
    state.passwordAttempts.delete(ip);
    res.setHeader('Set-Cookie', `${SESSION_COOKIE}=${sessionToken()}; Path=/setup; HttpOnly; SameSite=Lax; Max-Age=${SESSION_TTL / 1000}`);
    return writeJSON(res, 200, { ok: true });
  }
  if (route === 'logout' && method === 'POST') {
    res.setHeader('Set-Cookie', `${SESSION_COOKIE}=; Path=/setup; HttpOnly; SameSite=Lax; Max-Age=0`);
    return writeJSON(res, 200, { ok: true });
  }
  if (route === 'config') {
    if (method === 'GET') {
      if (!requireSession(req, res)) return;
      return writeJSON(res, 200, { config: publicConfigView(cfg), link: `${baseLink(req)}/`, version: VERSION });
    }
    if (method === 'POST') {
      if (!requireSession(req, res)) return;
      let patch;
      try { patch = JSON.parse(await readBody(req) || '{}'); } catch { return sendError(res, new HttpError(400, 'invalid JSON')); }
      applyConfigPatch(cfg, patch);
      syncBackendRegistry();
      saveConfig(cfg);
      searchCache.clear(); streamCache.clear(); metaCache.clear();
      return writeJSON(res, 200, { config: publicConfigView(cfg), link: `${baseLink(req)}/` });
    }
  }
  if (route === 'rotate-secret' && method === 'POST') {
    if (!requireSession(req, res)) return;
    cfg.secret = randomHex(16);
    saveConfig(cfg);
    metaCache.clear(); streamCache.clear(); searchCache.clear();
    return writeJSON(res, 200, { link: `${baseLink(req)}/` });
  }
  if (route === 'regen' && method === 'POST') {
    if (!requireSession(req, res)) return;
    const result = await regenerateTidoManifest({ manual: true });
    return writeJSON(res, 200, {
      ...result,
      regenUrlConfigured: !!tidoRegenUrl(),
      tidoUrl: result.ok ? redactUrl(result.url) : undefined,
    });
  }
  if (route === 'test-search' && method === 'POST') {
    if (!requireSession(req, res)) return;
    let body = {};
    try { body = JSON.parse(await readBody(req) || '{}'); } catch { /* empty */ }
    const q = String(body.q || '').trim();
    if (!q) return sendError(res, new HttpError(400, 'missing q'));
    const started = Date.now();
    let result;
    try { result = await searchPipeline(q, { fresh: true }); } catch (e) { return sendError(res, e); }
    const entries = await materializeResults({ ...result, winner: result.winner });
    return writeJSON(res, 200, {
      backend: result.winner,
      errors: result.errors,
      tookMs: Date.now() - started,
      tracks: entries.slice(0, 20).map((e) => trackJSON(baseLink(req), e)),
    });
  }
  if (route === 'test-stream' && method === 'POST') {
    if (!requireSession(req, res)) return;
    let body = {};
    try { body = JSON.parse(await readBody(req) || '{}'); } catch { /* empty */ }
    const id = String(body.id || '');
    if (!ID_PATTERN.test(id)) return sendError(res, new HttpError(400, 'bad id'));
    try {
      const info = await resolveStream(id);
      const descriptor = streamJSON(baseLink(req), info);
      // HEAD the upstream to prove the link actually plays.
      let headStatus = null;
      if (info.manifest === 'none' && info.upstreamUrl) {
        try {
          const head = await fetchUrl(info.upstreamUrl, { method: 'HEAD', timeoutMs: 15000, headers: { 'accept-encoding': 'identity' } });
          headStatus = head.status;
        } catch { headStatus = null; }
      }
      return writeJSON(res, 200, { descriptor, headStatus });
    } catch (e) {
      return sendError(res, e);
    }
  }
  if (route === 'status' && method === 'GET') {
    if (!requireSession(req, res)) return;
    const checks = [];
    const t = tidoTarget();
    if (t) {
      // manifest.json can answer 200 while the data endpoints 403 (observed
      // 2026-11: whole upstream down -> {"error":"Unavailable"} on /search).
      // Probe both so the status page tells the truth.
      const started = Date.now();
      let manifestStatus = 0;
      let dataStatus = 0;
      let lastError = backends.tido.lastError;
      try {
        const res2 = await fetchJson(`${t.base}/${t.token ? t.token + '/' : ''}manifest.json`, { timeoutMs: 10000 });
        manifestStatus = res2.status;
      } catch (e) { lastError = String(e.message || e).slice(0, 200); }
      try {
        const probe = new URLSearchParams({ q: 't', quality: 'LOW', exactQuality: 'false', videosEnabled: 'false' });
        const res3 = await fetchJson(`${t.base}/${t.token ? t.token + '/' : ''}search?${probe.toString()}`, { timeoutMs: 10000 });
        dataStatus = res3.status;
      } catch { dataStatus = 0; }
      const detail = manifestStatus === 200 && dataStatus === 403
        ? 'manifest ok but data path 403 (Tido upstream down — fresh tokens do not help; failover carries requests)'
        : dataStatus === 200 ? 'manifest + data path ok' : `manifest ${manifestStatus}, data ${dataStatus}`;
      checks.push({
        name: 'Tido', url: redactUrl(`${t.base}/…`), ok: dataStatus === 200,
        status: dataStatus, manifestStatus, dataStatus, detail,
        tookMs: Date.now() - started, lastError,
      });
    }
    for (const b of backends.hifi) {
      const started = Date.now();
      try {
        const res2 = await fetchJson(`${hifiBase(b)}/`, { timeoutMs: 10000 });
        checks.push({ name: b.name, url: redactUrl(b.url), ok: res2.status === 200, status: res2.status, tookMs: Date.now() - started, lastError: b.lastError });
      } catch (e) {
        checks.push({ name: b.name, url: redactUrl(b.url), ok: false, status: 0, tookMs: Date.now() - started, lastError: String(e.message || e).slice(0, 200) });
      }
    }
    if (cfg.monoEnabled && cfg.monoUrl) {
      const started = Date.now();
      try {
        const res2 = await fetchJson(`${monoBase()}/`, { timeoutMs: 10000 });
        checks.push({ name: 'monochrome', url: redactUrl(monoBase()), ok: res2.status === 200, status: res2.status, tookMs: Date.now() - started, lastError: backends.mono.lastError });
      } catch (e) {
        checks.push({ name: 'monochrome', url: redactUrl(monoBase()), ok: false, status: 0, tookMs: Date.now() - started, lastError: String(e.message || e).slice(0, 200) });
      }
    }
    return writeJSON(res, 200, {
      checks,
      caches: { search: searchCache.size, streams: streamCache.size, meta: metaCache.size, art: artCache.size },
      keepAlive: { enabled: KEEP_ALIVE, target: SELF_PING_URL || null },
      regen: {
        configured: !!tidoRegenUrl(),
        lastResult: regenState.lastResult,
        count: regenState.count,
        minutesSinceLast: regenState.lastAt ? Math.round((Date.now() - regenState.lastAt) / 60000) : null,
        autoIntervalMinutes: Math.round(REGEN_MIN_INTERVAL / 60000),
      },
      version: VERSION,
    });
  }
  quiet404(res);
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

const SETUP_HTML_PATH = path.join(__dirname, 'setup.html');

async function route(req, res) {
  applyCors(res);
  const method = req.method === 'HEAD' ? 'GET' : req.method;
  const u = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const pathname = u.pathname;
  const cfg = state.config;

  if (pathname === '/health') {
    res.writeHead(200); res.end();
    return;
  }
  if (pathname === '/' || pathname === '/setup') {
    try {
      const html = fs.readFileSync(SETUP_HTML_PATH);
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store', 'Content-Length': html.length });
      res.end(req.method === 'HEAD' ? undefined : html);
    } catch {
      sendError(res, new HttpError(500, 'setup.html missing next to server.js'));
    }
    return;
  }
  if (pathname.startsWith('/setup/api/') || pathname === '/setup/api') {
    try {
      await setupApi(method, pathname, req, res);
    } catch (e) {
      sendError(res, e);
    }
    return;
  }

  // ---- BitChord protocol routes: /{slug}/{secret}/... ----
  const segs = pathname.split('/').filter(Boolean).map((s) => decodeURIComponent(s));
  if (segs.length >= 2) {
    const [slugSeg, secretSeg] = segs;
    const rest = segs.slice(2).join('/');
    const secretOk = timingSafeEqualStr(secretSeg, cfg.secret);
    const slugOk = slugSeg === cfg.slug;
    if (req.method === 'OPTIONS' && slugOk) {
      res.setHeader('Access-Control-Allow-Methods', 'GET, HEAD, POST, OPTIONS');
      res.setHeader('Access-Control-Allow-Headers', 'Range, If-Range, Content-Type, X-BitChord-Token, X-BitChord-Proof, X-Eclipse-Access');
      res.setHeader('Access-Control-Max-Age', '86400');
      res.writeHead(204);
      res.end();
      return;
    }
    if (secretOk && slugOk) {
      const base = baseLink(req);
      if (rest === 'manifest.json' && method === 'GET') return handleManifest(res, cfg);
      if (rest === 'search' && method === 'GET') return handleSearch(u.searchParams.get('q'), req, res, base, u.searchParams);
      if (rest === 'sync' && (method === 'POST' || method === 'GET')) return handleSync(res);
      const streamMatch = /^stream\/([0-9A-Za-z_-]{1,128})$/.exec(rest);
      if (streamMatch && method === 'GET') return handleStream(streamMatch[1], req, res, base, u.searchParams);
      const fileMatch = /^file\/([0-9A-Za-z_-]{1,128})$/.exec(rest);
      if (fileMatch && (method === 'GET' || req.method === 'HEAD')) return handleFile(fileMatch[1], req, res);
      const artMatch = /^art\/([0-9A-Za-z_-]{1,128})$/.exec(rest);
      if (artMatch && (method === 'GET' || req.method === 'HEAD')) return handleArt(artMatch[1], req, res);
      return quiet404(res);
    }
    // Wrong secret or slug: answer like the upstream addon — a bare 404 that
    // reveals nothing about which segment failed.
    return quiet404(res);
  }
  quiet404(res);
}

const server = http.createServer((req, res) => {
  route(req, res).catch((e) => {
    try { sendError(res, e); } catch { /* already sent */ }
  });
});

// ---------------------------------------------------------------------------
// Keep-alive self ping (Render free tier idles after 15 min without traffic;
// same approach as Perzaan-AFK-Bot's "Self-Ping" for infinite uptime)
// ---------------------------------------------------------------------------

function startKeepAlive() {
  if (!KEEP_ALIVE) return;
  const target = (SELF_PING_URL || state.config.publicUrl || '').replace(/\/+$/, '');
  if (!target || /127\.0\.0\.1|localhost/.test(target)) {
    info('keep-alive skipped (no external URL)');
    return;
  }
  const tick = async () => {
    try {
      const res = await fetchUrl(`${target}/health`, { timeoutMs: 10000 });
      debug('self-ping', { status: res.status });
    } catch {
      debug('self-ping failed');
    }
  };
  const interval = KEEP_ALIVE_INTERVAL * 1000;
  setInterval(tick, interval).unref();
  info('keep-alive self-ping enabled', { target: redactUrl(target), intervalSec: KEEP_ALIVE_INTERVAL });
}

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------

function boot() {
  loadConfig();
  syncBackendRegistry();
  if (!state.config.tidoUrl && !state.config.hifiApiUrls.length) {
    warn('no backends configured yet — open /setup to add your Tido URL or hifi-api instances');
  }
  server.listen(PORT, '0.0.0.0', () => {
    info(`bitchord-tido-addon ${VERSION} listening`, { port: PORT, dataDir: DATA_DIR, slug: state.config.slug });
  });
  startKeepAlive();
}

const shuttingDown = () => {
  info('shutting down');
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
};
process.on('SIGTERM', shuttingDown);
process.on('SIGINT', shuttingDown);

boot();


