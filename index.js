
"use strict";

require("dotenv").config({ override: true });

const express = require("express");
const path = require("path");
const fs = require("fs");
const fsp = require("fs/promises");
const net = require("net");
const dns = require("dns").promises;
const os = require("os");
const { spawn } = require("child_process");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const { RedisStore: RateLimitRedisStore } = require("rate-limit-redis");
const cookieParser = require("cookie-parser");
const session = require("express-session");
const zlib = require("zlib");
let _multer; function getLazyMulter() { if (!_multer) _multer = require("multer"); return _multer; }
const hpp = require("hpp");
const crypto = require("crypto");
const https = require("https");
const httpMod = require("http");
const { URL } = require("url");
const { Server: SocketIOServer } = require("socket.io");
let _FileStore; function getLazyFileStore() { if (!_FileStore) _FileStore = require("session-file-store")(session); return _FileStore; }
let _FormData; function getLazyFormData() { if (!_FormData) _FormData = require("form-data"); return _FormData; }
const db = require("./db");
const mysql = require("mysql2/promise");
const DASHBOARD_CSS = path.join(process.cwd(), "public", "css", "dashboard.css");
const BOT_CSS = path.join(process.cwd(), "public", "css", "bot.css");
const { makeCssBackground } = require("./utils/background");
const {
  brandingMiddleware,
  loadBrandingConfig,
  saveBrandingConfig,
  sanitizeAppName,
  sanitizeLogoUrl,
  generateSafeLogoFilename,
  validateBase64Image,
  validateBase64Video,
  getValidatedExtension,
  getValidatedVideoExtension,
  getMimeTypeForExtension,
  getMediaTypeFromFilename,
  getMediaTypeFromUrl,
  getExtensionFromUrl,
  resolveLoginWatermarkAssetUrl,
  resolveLoginBackgroundAssetUrl,
  invalidateBrandingCache
} = require("./utils/branding");
const { scaleForMemory } = require("./utils/memory-scaling");
const dbProxy = require("./utils/db-proxy");
const { sanitizeDockerTemplatePayload } = require("./utils/server-template-payload");
const { deleteServerByName } = require("./utils/server-delete");
const { applyRemoteAssetToServer } = require("./utils/server-asset-apply");
const { ensureStealthConfig } = require("./utils/stealth");
const {
  BOT_GOOGLE_FUNCTION_DECLARATIONS,
  BOT_OPENAI_TOOL_DEFINITIONS,
  BOT_TOOL_DEFINITIONS,
  MAX_BOT_TOOL_LOOPS,
  buildBotAssistantSystemPrompt,
  buildBotReplyFromToolResult,
  parseBotAssistantToolPlan,
  resolveBackupFromList,
  resolveServerFromAccessibleList,
} = require("./utils/bot-ai-tools");

const nodesRouter = require("./nodes.js");
const subdomainsRouter = require("./routes/subdomains.js");
const createDashboardAssistantRouter = require("./routes/dashboard-assistant.js");
const { recordActivity } = nodesRouter;


const app = express();
app.locals.themeVersion = Date.now();
const GUEST_AVATAR_URL = "https://icon-library.com/images/guest-account-icon/guest-account-icon-1.jpg";
const ADMIN_AVATAR_URL = "https://cdn.jsdelivr.net/gh/antonndev/ADCDn/admin-avatar.webp";
const DEFAULT_USER_AVATAR_URLS = [
  "https://cdn.jsdelivr.net/gh/antonndev/ADCDn/normal-1.webp",
  "https://cdn.jsdelivr.net/gh/antonndev/ADCDn/normal-2.webp",
  "https://cdn.jsdelivr.net/gh/antonndev/ADCDn/normal-3.webp",
];

function getDefaultUserAvatar(seed) {
  const hash = crypto.createHash("sha256").update(String(seed || "adpanel-user")).digest();
  return DEFAULT_USER_AVATAR_URLS[hash[0] % DEFAULT_USER_AVATAR_URLS.length];
}

function resolveUserAvatarUrl(user) {
  if (user?.avatar_url && String(user.avatar_url).trim()) {
    return String(user.avatar_url).trim();
  }
  if (!user) {
    return GUEST_AVATAR_URL;
  }
  if (user.admin) {
    return ADMIN_AVATAR_URL;
  }
  return getDefaultUserAvatar(user.email || user.id || "adpanel-user");
}
app.disable("x-powered-by");

function isDisallowedResponseHeader(name) {
  const normalized = String(name || "").trim().toLowerCase();
  return normalized === "x-powered-by" || normalized === "server";
}

function hardenResponseHeaderWrites(res) {
  if (!res || res.__adpanelResponseHeadersHardened) return;
  res.__adpanelResponseHeadersHardened = true;

  const originalSetHeader = res.setHeader;
  res.setHeader = function patchedSetHeader(name, value) {
    if (isDisallowedResponseHeader(name)) return this;
    return originalSetHeader.call(this, name, value);
  };

  const originalWriteHead = res.writeHead;
  res.writeHead = function patchedWriteHead(statusCode, reasonPhrase, headers) {
    let finalReasonPhrase = reasonPhrase;
    let finalHeaders = headers;

    if (finalHeaders === undefined && finalReasonPhrase && typeof finalReasonPhrase === "object") {
      finalHeaders = finalReasonPhrase;
      finalReasonPhrase = undefined;
    }

    if (finalHeaders && typeof finalHeaders === "object") {
      for (const headerName of Object.keys(finalHeaders)) {
        if (isDisallowedResponseHeader(headerName)) delete finalHeaders[headerName];
      }
    }

    try {
      this.removeHeader("Server");
      this.removeHeader("X-Powered-By");
    } catch { }

    if (finalReasonPhrase === undefined && finalHeaders === undefined) {
      return originalWriteHead.call(this, statusCode);
    }
    if (finalReasonPhrase === undefined) {
      return originalWriteHead.call(this, statusCode, finalHeaders);
    }
    return originalWriteHead.call(this, statusCode, finalReasonPhrase, finalHeaders);
  };
}

app.use((req, res, next) => {
  hardenResponseHeaderWrites(res);
  try {
    res.removeHeader("Server");
    res.removeHeader("X-Powered-By");
  } catch { }
  next();
});
const ALLOWED_HTTP_METHODS = "GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS";
app.use((req, res, next) => {
  const method = String(req.method || "").toUpperCase();
  if (method === "TRACE" || method === "TRACK") {
    res.setHeader("Allow", ALLOWED_HTTP_METHODS);
    return res.status(405).type("text/plain").send("Method Not Allowed");
  }
  if (method === "OPTIONS") {
    res.setHeader("Allow", ALLOWED_HTTP_METHODS);
    return res.status(204).end();
  }
  return next();
});
const TRUST_PROXY = process.env.TRUST_PROXY || "loopback";
app.set("trust proxy", TRUST_PROXY);
const FREEZE_PROTOTYPES = ["1", "true", "yes", "on"].includes(String(process.env.FREEZE_PROTOTYPES ?? "1").trim().toLowerCase());
const SCHEDULER_ENABLED_EARLY = ["1", "true", "yes", "on"].includes(String(process.env.SCHEDULER_ENABLED ?? "1").trim().toLowerCase());
if (FREEZE_PROTOTYPES && !SCHEDULER_ENABLED_EARLY) {
  Object.freeze(Object.prototype);
  Object.freeze(Array.prototype);
} else if (FREEZE_PROTOTYPES && SCHEDULER_ENABLED_EARLY) {
  console.log("[security] FREEZE_PROTOTYPES skipped - BullMQ scheduler requires unfrozen prototypes");
}
const CSP_NONCE_BYTES = parseInt(process.env.CSP_NONCE_BYTES || "", 10) || 16;


const BCRYPT_ROUNDS = parseInt(process.env.BCRYPT_ROUNDS || "12", 10) || 12;

function parseBoolean(value, defaultValue = false) {
  if (value === undefined || value === null || value === "") return defaultValue;
  if (typeof value === "boolean") return value;
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return defaultValue;
}


function validatePort(port) {
  if (port === undefined || port === null) return null;
  const str = String(port).trim();
  if (!/^[1-9]\d{0,4}$/.test(str) && str !== '0') return null;
  const num = parseInt(str, 10);
  if (isNaN(num) || num < 1 || num > 65535) return null;
  return num;
}

function validatePortListInput(value) {
  if (value === undefined || value === null || value === "") return [];
  const rawEntries = Array.isArray(value) ? value : String(value).split(",");
  const parsed = [];
  const seen = new Set();
  for (const entry of rawEntries) {
    const port = validatePort(entry);
    if (!port) return null;
    if (seen.has(port)) continue;
    seen.add(port);
    parsed.push(port);
  }
  return parsed;
}

function validateIPv4(ip) {
  if (!ip || typeof ip !== 'string') return null;
  const trimmed = ip.trim();
  const ipv4Regex = /^(?:(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\.){3}(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)$/;
  return ipv4Regex.test(trimmed) ? trimmed : null;
}

function validateIPv6(ip) {
  if (!ip || typeof ip !== 'string') return null;
  const trimmed = ip.trim();
  if (/['"\\$`\s;|&<>(){}[\]!#]/.test(trimmed)) return null;
  const ipv6Regex = /^(?:[a-fA-F0-9]{1,4}:){0,7}[a-fA-F0-9]{0,4}$/;
  if (trimmed === '::' || trimmed === '::1' || trimmed === '::0') return trimmed;
  const parts = trimmed.split(':');
  if (parts.length < 2 || parts.length > 8) return null;
  for (const part of parts) {
    if (part !== '' && !/^[a-fA-F0-9]{1,4}$/.test(part)) return null;
  }
  return trimmed;
}

function validateHostname(host) {
  if (!host || typeof host !== 'string') return null;
  const trimmed = host.trim().toLowerCase();
  if (trimmed.length === 0 || trimmed.length > 253) return null;
  if (!/^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$/.test(trimmed)) return null;
  const labels = trimmed.split('.');
  for (const label of labels) {
    if (label.length === 0 || label.length > 63) return null;
    if (!/^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/.test(label)) return null;
    if (label.startsWith('-') || label.endsWith('-')) return null;
  }
  return trimmed;
}

function validateDbHost(host) {
  if (!host || typeof host !== 'string') return null;
  const trimmed = host.trim();

  if (trimmed === 'localhost') return 'localhost';

  if (trimmed === '0.0.0.0') return '0.0.0.0';

  const ipv4 = validateIPv4(trimmed);
  if (ipv4) return ipv4;

  const ipv6 = validateIPv6(trimmed);
  if (ipv6) return ipv6;

  const hostname = validateHostname(trimmed);
  if (hostname) return hostname;

  return null;
}

const AUTO_MANAGE_FIREWALL = parseBoolean(process.env.ADPANEL_AUTO_MANAGE_FIREWALL, false);

function isLocalOnlyDbHost(host) {
  const normalized = String(host || "").trim().toLowerCase();
  return normalized === "localhost" || normalized === "127.0.0.1" || normalized === "::1" || normalized === "[::1]";
}

async function maybeOpenFirewallPort(runCmd, listenPort, listenHost, serviceTag) {
  if (isLocalOnlyDbHost(listenHost)) {
    console.log(`[${serviceTag}] Service is configured for local-only access; skipping firewall changes`);
    return;
  }
  if (!AUTO_MANAGE_FIREWALL) {
    console.log(`[${serviceTag}] Skipping automatic firewall changes for port ${listenPort}. Set ADPANEL_AUTO_MANAGE_FIREWALL=1 to re-enable this behavior.`);
    return;
  }
  await runCmd(`ufw allow ${listenPort}/tcp 2>/dev/null || true`, 30000);
  await runCmd(`iptables -C INPUT -p tcp --dport ${listenPort} -j ACCEPT 2>/dev/null || iptables -A INPUT -p tcp --dport ${listenPort} -j ACCEPT 2>/dev/null || true`, 10000);
}

async function maybeRemoveFirewallPort(runCmd, listenPort, serviceTag) {
  if (!AUTO_MANAGE_FIREWALL) {
    console.log(`[${serviceTag}] Skipping automatic firewall cleanup for port ${listenPort}.`);
    return;
  }
  await runCmd(`ufw delete allow ${listenPort}/tcp 2>/dev/null || true`, 10000);
  await runCmd(`iptables -D INPUT -p tcp --dport ${listenPort} -j ACCEPT 2>/dev/null || true`, 10000);
}

function escapePhpSingleQuote(str) {
  if (!str || typeof str !== 'string') return '';
  return str.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
}

function escapeHtml(input) {
  const s = String(input ?? "");
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

const POLLUTION_KEYS = new Set(["__proto__", "constructor", "prototype"]);

const FILE_SIGNATURES = {
  'image/jpeg': [Buffer.from([0xFF, 0xD8, 0xFF])],
  'image/png': [Buffer.from([0x89, 0x50, 0x4E, 0x47])],
  'image/gif': [Buffer.from([0x47, 0x49, 0x46, 0x38])],
  'image/webp': [Buffer.from([0x52, 0x49, 0x46, 0x46])],
};

function validateFileMagicBytes(buffer, declaredMimeType) {
  if (!buffer || buffer.length < 4) return false;

  const signatures = FILE_SIGNATURES[declaredMimeType];
  if (!signatures) return false;

  for (const sig of signatures) {
    if (buffer.slice(0, sig.length).equals(sig)) {
      if (declaredMimeType === 'image/webp') {
        if (buffer.length < 12) return false;
        const webpSig = buffer.slice(8, 12).toString('ascii');
        return webpSig === 'WEBP';
      }
      return true;
    }
  }
  return false;
}

function checkFileForDangerousContent(buffer) {
  if (!buffer || buffer.length === 0) return false;

  const checkSize = Math.min(buffer.length, 8192);
  const content = buffer.slice(0, checkSize).toString('utf8', 0, checkSize);

  const dangerousPatterns = [
    /<%.*%>/i,
    /<\?php/i,
    /<\?=/i,
    /<script[\s>]/i,
    /eval\s*\(/i,
    /exec\s*\(/i,
    /system\s*\(/i,
    /passthru\s*\(/i,
    /shell_exec\s*\(/i,
    /\bimport\s+os\b/i,
    /\bsubprocess\b/i,
    /#!\/.*\/(bash|sh|python|perl|ruby)/i,
  ];

  for (const pattern of dangerousPatterns) {
    if (pattern.test(content)) {
      return false;
    }
  }

  return true;
}

// Unified TTL cache with auto-sweep and max-size eviction
class TTLCache {
  constructor({ name = "cache", ttlMs = 60000, maxSize = Infinity, sweepMs = 0 } = {}) {
    this._name = name;
    this._ttlMs = ttlMs;
    this._maxSize = maxSize;
    this._map = new Map();
    this._sweepTimer = null;
    if (sweepMs > 0) {
      this._sweepTimer = setInterval(() => this.sweep(), sweepMs);
      this._sweepTimer.unref();
    }
  }

  get size() {
    return this._map.size;
  }

  get(key) {
    const entry = this._map.get(key);
    if (!entry) return undefined;
    if (entry.expiresAt > 0 && Date.now() >= entry.expiresAt) {
      this._map.delete(key);
      return undefined;
    }
    return entry.value;
  }

  has(key) {
    return this.get(key) !== undefined;
  }

  set(key, value, ttlMs) {
    const ttl = ttlMs !== undefined ? ttlMs : this._ttlMs;
    if (this._map.size >= this._maxSize && !this._map.has(key)) {
      // Evict oldest entry
      const oldestKey = this._map.keys().next().value;
      if (oldestKey !== undefined) this._map.delete(oldestKey);
    }
    this._map.set(key, {
      value,
      expiresAt: ttl > 0 ? Date.now() + ttl : 0,
      updatedAt: Date.now()
    });
  }

  delete(key) {
    return this._map.delete(key);
  }

  clear(predicate) {
    if (!predicate) {
      this._map.clear();
      return;
    }
    for (const [key, entry] of this._map.entries()) {
      if (predicate(key, entry.value)) this._map.delete(key);
    }
  }

  // Iterate over non-expired entries
  *entries() {
    const now = Date.now();
    for (const [key, entry] of this._map.entries()) {
      if (entry.expiresAt > 0 && now >= entry.expiresAt) continue;
      yield [key, entry.value];
    }
  }

  *keys() {
    for (const [key] of this.entries()) yield key;
  }

  *values() {
    for (const [, value] of this.entries()) yield value;
  }

  sweep() {
    const now = Date.now();
    for (const [key, entry] of this._map.entries()) {
      if (entry.expiresAt > 0 && now >= entry.expiresAt) {
        this._map.delete(key);
      }
    }
  }

  // Get raw entry metadata (updatedAt) for consumers that need it
  getMeta(key) {
    const entry = this._map.get(key);
    if (!entry) return null;
    if (entry.expiresAt > 0 && Date.now() >= entry.expiresAt) {
      this._map.delete(key);
      return null;
    }
    return { value: entry.value, updatedAt: entry.updatedAt, expiresAt: entry.expiresAt };
  }

  destroy() {
    if (this._sweepTimer) {
      clearInterval(this._sweepTimer);
      this._sweepTimer = null;
    }
    this._map.clear();
  }
}

const serverCreateLocks = new Set();
setInterval(() => { serverCreateLocks.clear(); }, 600_000).unref();

let mongodbOperationLock = false;

const transferJobs = new Map();

const TRANSFER_JOB_TTL_SEC = parseInt(process.env.TRANSFER_JOB_TTL_SEC || "", 10) || 3600;

function transferJobRedisKey(name) {
  const key = String(name || "").trim();
  return `transferJob:${key}`;
}

async function getTransferJobFromRedis(name) {
  if (!redisClient) return null;
  const key = String(name || "").trim();
  if (!key) return null;
  try {
    const raw = await redisClient.get(transferJobRedisKey(key));
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object") {
      transferJobs.set(key, parsed);
      return parsed;
    }
  } catch { }
  return null;
}

function getTransferJob(name) {
  const key = String(name || "").trim();
  if (!key) return null;
  return transferJobs.get(key) || null;
}

function setTransferJob(name, patch) {
  const key = String(name || "").trim();
  if (!key) return;
  const existing = transferJobs.get(key) || { name: key };
  const updated = { ...existing, ...patch, updatedAt: Date.now() };
  transferJobs.set(key, updated);

  if (redisClient) {
    redisClient.set(transferJobRedisKey(key), JSON.stringify(updated), "EX", TRANSFER_JOB_TTL_SEC).catch(() => { });
  }
}

setInterval(() => {
  try {
    const now = Date.now();
    for (const [key, job] of transferJobs.entries()) {
      if (job.finishedAt && (now - job.finishedAt > TRANSFER_JOB_TTL_SEC * 1000)) {
        transferJobs.delete(key);
      } else if (job.updatedAt && (now - job.updatedAt > TRANSFER_JOB_TTL_SEC * 1000 * 2)) {
        transferJobs.delete(key);
      }
    }
  } catch (err) { console.debug("[transferJobs] sweep error:", err.message); }
}, 300_000).unref();


function cleanupTransferJobLater(name, ms = 10 * 60 * 1000) {
  const key = String(name || "").trim();
  if (!key) return;
  setTimeout(() => {
    const j = transferJobs.get(key);
    if (!j) return;
    if (j.finishedAt) {
      transferJobs.delete(key);
      if (redisClient) {
        try { redisClient.del(transferJobRedisKey(key)).catch(() => { }); } catch { }
      }
    }
  }, ms);
}

function httpStreamRequest(fullUrl, { method = "GET", headers = {}, bodyStream = null, timeoutMs = 120000 } = {}) {
  return new Promise((resolve, reject) => {
    try {
      const isHttps = fullUrl.startsWith("https:");
      const lib = isHttps ? https : httpMod;
      const requestOptions = { method, headers };
      if (isHttps && nodeMtlsAgent) requestOptions.agent = nodeMtlsAgent;

      const cleanup = () => {
        if (req) {
          req.removeListener("timeout", onTimeout);
          req.removeListener("error", onError);
        }
      };

      const onTimeout = () => {
        cleanup();
        try { req.destroy(new Error("timeout")); } catch { }
      };

      const onError = (e) => {
        cleanup();
        reject(e);
      };

      const req = lib.request(fullUrl, requestOptions, (res) => {
        cleanup();
        resolve({ req, res });
      });

      req.on("timeout", onTimeout);
      req.on("error", onError);
      req.setTimeout(timeoutMs);

      if (bodyStream) bodyStream.pipe(req);
      else req.end();
    } catch (e) {
      reject(e);
    }
  });
}

async function stopServerOnNode(node, name) {
  const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
  if (!baseUrl) return;
  const headers = nodeAuthHeadersFor(node, true);
  try {
    await httpRequestJson(`${baseUrl}/v1/servers/${encodeURIComponent(name)}/stop`, "POST", headers, null, 30_000);
  } catch { }
}

async function waitForServerStopped(node, name, { timeoutMs = 30_000, intervalMs = 1500 } = {}) {
  const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
  if (!baseUrl) return true;
  const headers = nodeAuthHeadersFor(node, true);
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const r = await httpRequestJson(
        `${baseUrl}/v1/servers/${encodeURIComponent(name)}`,
        "GET", headers, null, 10_000
      );
      if (r.status === 200 && r.json) {
        const st = String(r.json.status || "").toLowerCase();
        if (st === "stopped" || st === "exited" || st === "not running") return true;
      }
      if (r.status === 404) return true;
    } catch { }
    await new Promise(r => setTimeout(r, intervalMs));
  }
  return false;
}

async function waitForServerReady(node, name, { timeoutMs = 30_000, intervalMs = 2000 } = {}) {
  const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
  if (!baseUrl) return false;
  const headers = nodeAuthHeadersFor(node, true);
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const r = await httpRequestJson(
        `${baseUrl}/v1/servers/${encodeURIComponent(name)}`,
        "GET", headers, null, 10_000
      );
      if (r.status === 200 && r.json && r.json.ok) {
        return true;
      }
    } catch { }
    await new Promise(r => setTimeout(r, intervalMs));
  }
  return false;
}

async function deleteServerOnNode(node, name, { filesOnly = false } = {}) {
  const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
  if (!baseUrl) throw new Error("invalid node address");
  const headers = nodeAuthHeadersFor(node, true);
  const qs = filesOnly ? "?files_only=true" : "";
  const r = await httpRequestJson(`${baseUrl}/v1/servers/${encodeURIComponent(name)}${qs}`, "DELETE", headers, null, 120_000);
  if (r.status !== 200 || !(r.json && r.json.ok)) {
    const msg = (r.json && (r.json.error || r.json.detail)) ? (r.json.error || r.json.detail) : `remote delete failed (${r.status})`;
    throw new Error(msg);
  }
}

function computeTransferEffectiveResources(serverResources, sourceNode, destNode) {
  const inRes = (serverResources && typeof serverResources === "object") ? serverResources : {};
  const out = { ...inRes };

  const srcRam = Number(sourceNode?.ram_mb || 0);
  const dstRam = Number(destNode?.ram_mb || 0);
  const srcDisk = Number(sourceNode?.disk_gb || 0);
  const dstDisk = Number(destNode?.disk_gb || 0);

  const srcCores = Number(sourceNode?.buildConfig?.cpu_cores || sourceNode?.buildConfig?.cpuCores || 0);
  const dstCores = Number(destNode?.buildConfig?.cpu_cores || destNode?.buildConfig?.cpuCores || 0);

  const coerceInt = (v) => {
    const n = Number(v);
    return Number.isFinite(n) ? Math.trunc(n) : null;
  };
  const coerceFloat = (v) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  };

  const isUnlimitedOrNodeMaxInt = (val, srcMax) => {
    if (val == null) return true;
    const n = coerceInt(val);
    if (n == null) return true;
    if (n <= 0) return true;
    if (srcMax > 0 && n === Math.trunc(srcMax)) return true;
    return false;
  };

  const isUnlimitedOrNodeMaxFloat = (val, srcMax) => {
    if (val == null) return true;
    const n = coerceFloat(val);
    if (n == null) return true;
    if (n <= 0) return true;
    if (srcMax > 0 && Math.abs(n - srcMax) < 0.0001) return true;
    return false;
  };

  if (dstRam > 0) {
    const cur = coerceInt(inRes.ramMb);
    if (isUnlimitedOrNodeMaxInt(inRes.ramMb, srcRam)) out.ramMb = Math.trunc(dstRam);
    else if (cur != null && cur > Math.trunc(dstRam)) out.ramMb = Math.trunc(dstRam);
  }

  const srcDiskMb = srcDisk > 0 ? srcDisk * 1024 : 0;
  const dstDiskMb = dstDisk > 0 ? dstDisk * 1024 : 0;
  const storageField = inRes.storageMb != null ? 'storageMb' : 'storageGb';
  const storageSrcMax = storageField === 'storageMb' ? srcDiskMb : srcDisk;
  const storageDstMax = storageField === 'storageMb' ? dstDiskMb : dstDisk;
  if ((storageField === 'storageMb' ? dstDiskMb : dstDisk) > 0) {
    const cur = coerceInt(inRes[storageField]);
    if (isUnlimitedOrNodeMaxInt(inRes[storageField], storageSrcMax)) out[storageField] = Math.trunc(storageDstMax);
    else if (cur != null && cur > Math.trunc(storageDstMax)) out[storageField] = Math.trunc(storageDstMax);
  }

  if (dstCores > 0) {
    const cur = coerceFloat(inRes.cpuCores);
    if (isUnlimitedOrNodeMaxFloat(inRes.cpuCores, srcCores)) out.cpuCores = dstCores;
    else if (cur != null && cur > dstCores) out.cpuCores = dstCores;
  }

  return out;
}

function parseOptionalIntegerResource(value, label, min, max) {
  if (value === null || value === undefined || String(value).trim() === "") return null;
  const text = String(value).trim();
  if (!/^\d+$/.test(text)) {
    throw new Error(`${label} must be a whole number.`);
  }
  const parsed = Number(text);
  if (!Number.isSafeInteger(parsed) || parsed < min || parsed > max) {
    throw new Error(`${label} must be between ${min} and ${max}.`);
  }
  return parsed;
}

function collectResourcePerformanceOptions(input) {
  const source = (input && typeof input === "object") ? input : {};
  const options = {};
  const ioWeight = parseOptionalIntegerResource(source.ioWeight, "I/O Priority", 10, 1000);
  const cpuWeight = parseOptionalIntegerResource(source.cpuWeight, "CPU Priority", 1, 1000);
  const pidsLimit = parseOptionalIntegerResource(source.pidsLimit, "Process Limit", 64, 4096);
  const fileLimit = parseOptionalIntegerResource(source.fileLimit, "File Limit", 1024, 1048576);
  if (ioWeight != null) options.ioWeight = ioWeight;
  if (cpuWeight != null) options.cpuWeight = cpuWeight;
  if (pidsLimit != null) options.pidsLimit = pidsLimit;
  if (fileLimit != null) options.fileLimit = fileLimit;
  return options;
}

async function startServerOnNode(node, name, hostPort) {
  const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
  if (!baseUrl) throw new Error("invalid node address");
  const headers = nodeAuthHeadersFor(node, true);
  const payload = (hostPort != null && Number.isFinite(Number(hostPort))) ? { hostPort: Number(hostPort) } : {};
  const r = await httpRequestJson(`${baseUrl}/v1/servers/${encodeURIComponent(name)}/start`, "POST", headers, payload, 120_000);
  if (r.status !== 200 || !(r.json && (r.json.ok === true || r.json.ok === undefined))) {
    const msg = (r.json && (r.json.error || r.json.detail)) ? (r.json.error || r.json.detail) : `remote start failed (${r.status})`;
    throw new Error(msg);
  }
}

async function streamExportToImport({ sourceNode, destNode, serverName, onProgress }) {
  const sourceBase = buildNodeBaseUrl(sourceNode.address, sourceNode.api_port || 8080, sourceNode.ssl_enabled);
  const destBase = buildNodeBaseUrl(destNode.address, destNode.api_port || 8080, destNode.ssl_enabled);
  if (!sourceBase) throw new Error("invalid source node address");
  if (!destBase) throw new Error("invalid destination node address");

  const srcUrl = `${sourceBase}/v1/servers/${encodeURIComponent(serverName)}/export`;
  const dstUrl = `${destBase}/v1/servers/${encodeURIComponent(serverName)}/import-tar`;

  const srcHeaders = {
    Authorization: `Bearer ${sourceNode.token}`,
    "X-Node-Token": sourceNode.token || "",
    "X-Node-Token-Id": sourceNode.token_id || ""
  };
  const dstHeaders = {
    Authorization: `Bearer ${destNode.token}`,
    "X-Node-Token": destNode.token || "",
    "X-Node-Token-Id": destNode.token_id || "",
    "Content-Type": "application/x-tar"
  };

  const { res: srcRes } = await httpStreamRequest(srcUrl, { method: "GET", headers: srcHeaders, timeoutMs: 10 * 60 * 1000 });
  if (srcRes.statusCode !== 200) {
    const MAX_ERR_BYTES = 64 * 1024;
    const chunks = [];
    let srcTotal = 0;
    srcRes.on("data", (d) => {
      if (srcTotal < MAX_ERR_BYTES) { chunks.push(d); srcTotal += d.length; }
    });
    await new Promise((r) => srcRes.on("end", r));
    const msg = Buffer.concat(chunks).toString("utf8").slice(0, 500);
    throw new Error(`export failed (${srcRes.statusCode}): ${msg || "no body"}`);
  }

  const totalBytesHeader = srcRes.headers["x-adpanel-server-bytes"] || srcRes.headers["content-length"];
  const totalBytes = totalBytesHeader ? Number(totalBytesHeader) : 0;
  let transferred = 0;

  srcRes.on("data", (chunk) => {
    transferred += chunk.length;
    if (typeof onProgress === "function") {
      const pct = totalBytes > 0 ? Math.min(100, Math.floor((transferred / totalBytes) * 100)) : null;
      onProgress({ transferred, totalBytes, percent: pct });
    }
  });

  srcRes.once("end", () => {
    try {
      if (typeof onProgress === "function") onProgress({ transferred, totalBytes, percent: 100 });
    } catch { }
  });

  const { req: dstReq, res: dstRes } = await httpStreamRequest(dstUrl, { method: "POST", headers: dstHeaders, bodyStream: srcRes, timeoutMs: 30 * 60 * 1000 });
  const dstBody = await new Promise((resolve, reject) => {
    const MAX_ERR_BYTES = 64 * 1024;
    const chunks = [];
    let dstTotal = 0;
    dstRes.on("data", (d) => {
      if (dstTotal < MAX_ERR_BYTES) { chunks.push(d); dstTotal += d.length; }
    });
    dstRes.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    dstRes.on("error", reject);
    dstReq.on("error", reject);
  });

  if (dstRes.statusCode !== 200) {
    throw new Error(`import failed (${dstRes.statusCode}): ${String(dstBody || "").slice(0, 500) || "no body"}`);
  }

  if (dstBody) {
    try {
      const parsed = JSON.parse(dstBody);
      if (parsed && parsed.ok === false) {
        throw new Error(parsed.error || "import failed");
      }
    } catch { }
  }
}

function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function safeMerge(target, source) {
  const safeTarget = isPlainObject(target) ? target : Object.create(null);
  if (!isPlainObject(source)) return safeTarget;

  for (const key of Object.keys(source)) {
    if (key === "__proto__" || key === "constructor" || key === "prototype") continue;

    if (POLLUTION_KEYS.has(key)) continue;

    const val = source[key];

    if (isPlainObject(val)) {
      if (!safeTarget[key] || !isPlainObject(safeTarget[key])) {
        safeTarget[key] = Object.create(null);
      }
      safeTarget[key] = safeMerge(safeTarget[key], val);
    } else {
      safeTarget[key] = val;
    }
  }
  return safeTarget;
}

const ENABLE_WEEKLY_AUDIT_FIX = parseBoolean(process.env.ENABLE_WEEKLY_AUDIT_FIX, false);

function scheduleWeeklySecurityAudit() {
  if (!ENABLE_WEEKLY_AUDIT_FIX) {
    console.log("[ADSecurity] Weekly npm audit fix disabled (set ENABLE_WEEKLY_AUDIT_FIX=true to enable)");
    return;
  }
  const timestampFile = path.join(__dirname, ".last_audit_ts");
  const now = Date.now();
  const ONE_WEEK = 7 * 24 * 60 * 60 * 1000;

  if (fs.existsSync(timestampFile)) {
    const lastRun = parseInt(fs.readFileSync(timestampFile, "utf8"), 10);
    if (now - lastRun < ONE_WEEK) return;
  }

  console.log("[ADSecurity] Initialized the npm audit fix weekly");

  const child = spawn("bash", [path.join(__dirname, "audit-fix.sh")], {
    detached: true,
    stdio: "ignore",
    cwd: __dirname,
  });

  child.unref();

  fs.writeFileSync(timestampFile, now.toString());
}

scheduleWeeklySecurityAudit();

function safeCompare(a, b) {
  if (typeof a !== "string" || typeof b !== "string") return false;

  const bufA = Buffer.from(a);
  const bufB = Buffer.from(b);

  if (bufA.length !== bufB.length) {
    crypto.timingSafeEqual(bufA, bufA);
    return false;
  }

  return crypto.timingSafeEqual(bufA, bufB);
}

const ACTION_TOKEN_SECRET =
  process.env.ACTION_TOKEN_SECRET ||
  crypto.createHmac("sha256", process.env.SESSION_SECRET).update("adpanel-action-token-key-v1").digest("hex");

const SSH_TERM_TOKEN_SECRET =
  process.env.SSH_TERM_TOKEN_SECRET ||
  crypto.createHmac("sha256", process.env.SESSION_SECRET).update("adpanel-ssh-term-token-key-v1").digest("hex");
const SSH_TERM_TOKEN_TTL_MS = parseInt(process.env.SSH_TERM_TOKEN_TTL_MS || "", 10) || 20 * 60 * 1000;

function b64urlEncode(input) {
  const buf = Buffer.isBuffer(input) ? input : Buffer.from(String(input));
  return buf.toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}
function b64urlDecode(str) {
  const s = String(str).replace(/-/g, "+").replace(/_/g, "/");
  const pad = s.length % 4 ? "=".repeat(4 - (s.length % 4)) : "";
  return Buffer.from(s + pad, "base64");
}

function stableStringify(obj) {
  if (obj === null || obj === undefined) return "null";
  if (typeof obj !== "object") return JSON.stringify(obj);
  if (Array.isArray(obj)) return `[${obj.map(stableStringify).join(",")}]`;
  const keys = Object.keys(obj).sort();
  return `{${keys.map(k => `${JSON.stringify(k)}:${stableStringify(obj[k])}`).join(",")}}`;
}

function signActionPayload(payloadB64) {
  const sig = crypto.createHmac("sha256", ACTION_TOKEN_SECRET).update(payloadB64).digest();
  return b64urlEncode(sig);
}

function signSshTerminalPayload(payloadB64) {
  const sig = crypto.createHmac("sha256", SSH_TERM_TOKEN_SECRET).update(payloadB64).digest();
  return b64urlEncode(sig);
}

function issueSshTerminalToken(req) {
  const payload = {
    v: 1,
    uid: String(req?.session?.user || ""),
    ip: String(getRequestIp(req) || ""),
    exp: Date.now() + SSH_TERM_TOKEN_TTL_MS,
  };
  const payloadB64 = b64urlEncode(JSON.stringify(payload));
  const sig = signSshTerminalPayload(payloadB64);
  return `${payloadB64}.${sig}`;
}

function issueActionToken(req, action, resource = {}, opts = {}) {
  const ttlSeconds = Number(opts.ttlSeconds ?? 300);
  const oneTime = !!opts.oneTime;

  const now = Date.now();
  const payload = {
    v: 1,
    uid: String(req.session?.user || ""),
    sid: String(req.sessionID || ""),
    act: String(action),
    res: stableStringify(resource || {}),
    exp: now + ttlSeconds * 1000,
    n: oneTime ? crypto.randomBytes(16).toString("hex") : undefined
  };

  const payloadB64 = b64urlEncode(JSON.stringify(payload));
  const sig = signActionPayload(payloadB64);
  return `${payloadB64}.${sig}`;
}

function getActionTokenFromRequest(req) {
  return (
    req.get("x-action-token") ||
    req.body?.actionToken ||
    null
  );
}

function markNonceUsed(req, nonce, expMs) {
  if (!req.session) return false;
  if (!req.session.usedActionNonces) req.session.usedActionNonces = {};
  const used = req.session.usedActionNonces;

  const now = Date.now();
  for (const [k, v] of Object.entries(used)) {
    if (Number(v) < now) delete used[k];
  }

  if (used[nonce]) return false;
  used[nonce] = expMs;
  req.session.usedActionNonces = used;
  return true;
}

function verifyActionToken(req, token, expectedAction, expectedResource = {}) {
  try {
    if (!token || typeof token !== "string") return false;
    const parts = token.split(".");
    if (parts.length !== 2) return false;

    const [payloadB64, sig] = parts;
    const expectedSig = signActionPayload(payloadB64);
    if (!safeCompare(sig, expectedSig)) return false;

    const payload = JSON.parse(b64urlDecode(payloadB64).toString("utf8"));
    if (!payload || payload.v !== 1) return false;

    if (!req.session || !req.session.user) return false;
    if (String(payload.uid).toLowerCase() !== String(req.session.user).toLowerCase()) return false;
    if (String(payload.sid) !== String(req.sessionID)) return false;

    if (String(payload.act) !== String(expectedAction)) return false;

    const now = Date.now();
    if (!payload.exp || Number(payload.exp) < now) return false;

    const expectedRes = stableStringify(expectedResource || {});
    if (String(payload.res) !== expectedRes) return false;

    if (payload.n) {
      const ok = markNonceUsed(req, String(payload.n), Number(payload.exp));
      if (!ok) return false;
    }

    return true;
  } catch {
    return false;
  }
}

function requireActionTokenOr403(req, res, action, resource = {}) {
  const token = getActionTokenFromRequest(req);
  const ok = verifyActionToken(req, token, action, resource);
  if (!ok) {
    res.status(403).json({ error: "invalid or missing action token" });
    return false;
  }
  return true;
}

function readJson(file, fallback) {
  try {
    if (!fs.existsSync(file)) return fallback;
    const raw = fs.readFileSync(file, "utf8").trim();
    if (!raw) return fallback;
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

function safeWriteJson(file, obj) {
  try {
    fs.writeFileSync(file, JSON.stringify(obj, null, 2), "utf8");
    return true;
  } catch {
    return false;
  }
}

function parseDbJson(value, fallback = null) {
  if (value === null || value === undefined || value === "") return fallback;
  if (typeof value === "object") return value;
  try {
    return JSON.parse(String(value));
  } catch {
    return fallback;
  }
}

function toDbJson(value) {
  if (value === undefined || value === null) return null;
  if (typeof value === "string") {
    try {
      JSON.parse(value);
      return value;
    } catch {
      return JSON.stringify(value);
    }
  }
  return JSON.stringify(value);
}

const GLOBAL_ALERT_FILE = path.join(process.cwd(), "data", "global-alert.json");
let cachedAlerts = null;

const MAINTENANCE_FILE = path.join(process.cwd(), "data", "maintenance.json");
let maintenanceState = null;
const DASHBOARD_ASSISTANT_CONFIG_FILE = path.join(process.cwd(), "data", "dashboard-assistant-config.json");
let dashboardAssistantConfigCache = null;

function loadMaintenanceState() {
  try {
    if (fs.existsSync(MAINTENANCE_FILE)) {
      const data = JSON.parse(fs.readFileSync(MAINTENANCE_FILE, "utf-8"));
      if (data && typeof data === "object") {
        maintenanceState = data;
        return data;
      }
    }
  } catch (e) {
    console.error("[maintenance] Failed to read maintenance.json:", e);
  }
  maintenanceState = {
    enabled: false,
    scheduled_at: null,
    enabled_by: null,
    enabled_at: null,
    reason: null,
    pre_alert: { show: false, message: null, minutes_before: null },
    previous_alert_backup: null
  };
  return maintenanceState;
}

function getMaintenanceState() {
  if (maintenanceState === null) loadMaintenanceState();
  return maintenanceState;
}

function saveMaintenanceState(state) {
  try {
    maintenanceState = state;
    fs.writeFileSync(MAINTENANCE_FILE, JSON.stringify(state, null, 2), "utf-8");
    return true;
  } catch (e) {
    console.error("[maintenance] Failed to save maintenance.json:", e);
    return false;
  }
}

function isMaintenanceActive() {
  const state = getMaintenanceState();
  if (!state) return false;
  if (state.enabled) return true;
  if (state.scheduled_at) {
    const scheduledTime = new Date(state.scheduled_at).getTime();
    if (!isNaN(scheduledTime) && Date.now() >= scheduledTime) {
      state.enabled = true;
      state.enabled_at = new Date().toISOString();
      saveMaintenanceState(state);
      console.log("[maintenance] Scheduled maintenance activated automatically");
      return true;
    }
  }
  return false;
}

function normalizeDashboardAssistantConfig(value) {
  const source = value && typeof value === "object" ? value : {};
  return {
    allowNormalUsers: !!source.allowNormalUsers,
    updatedAt: source.updatedAt ? String(source.updatedAt) : null,
    updatedBy: source.updatedBy ? String(source.updatedBy) : null,
  };
}

function loadDashboardAssistantConfig() {
  dashboardAssistantConfigCache = normalizeDashboardAssistantConfig(
    readJson(DASHBOARD_ASSISTANT_CONFIG_FILE, null)
  );
  return dashboardAssistantConfigCache;
}

function getDashboardAssistantConfig() {
  if (!dashboardAssistantConfigCache) {
    return loadDashboardAssistantConfig();
  }
  return dashboardAssistantConfigCache;
}

function saveDashboardAssistantConfig(config) {
  const normalized = normalizeDashboardAssistantConfig(config);
  if (!safeWriteJson(DASHBOARD_ASSISTANT_CONFIG_FILE, normalized)) {
    return false;
  }
  dashboardAssistantConfigCache = normalized;
  return true;
}

function isDashboardAssistantEnabledForNormalUsers() {
  return !!getDashboardAssistantConfig().allowNormalUsers;
}

function canUserAccessDashboardAssistant(user) {
  return !!(user && (user.admin || isDashboardAssistantEnabledForNormalUsers()));
}

let maintenanceCheckerInterval = null;

function startMaintenanceChecker() {
  if (maintenanceCheckerInterval) return;
  maintenanceCheckerInterval = setInterval(() => {
    try {
      const state = getMaintenanceState();
      if (!state) return;

      if (!state.enabled && state.scheduled_at) {
        const scheduledTime = new Date(state.scheduled_at).getTime();
        if (!isNaN(scheduledTime) && Date.now() >= scheduledTime) {
          state.enabled = true;
          state.enabled_at = new Date().toISOString();
          saveMaintenanceState(state);
          console.log("[maintenance] Scheduled maintenance activated");
        }

        if (!state.enabled && state.pre_alert && state.pre_alert.show && state.pre_alert.message && state.pre_alert.minutes_before) {
          const alertTime = scheduledTime - (state.pre_alert.minutes_before * 60 * 1000);
          if (Date.now() >= alertTime && !state.pre_alert._alert_injected) {
            const currentAlert = getActiveGlobalAlert();
            if (currentAlert) {
              state.previous_alert_backup = currentAlert;
            }
            addGlobalAlert(state.pre_alert.message, new Date().toISOString());
            state.pre_alert._alert_injected = true;
            state.pre_alert._injected_alert_message = state.pre_alert.message;
            saveMaintenanceState(state);
            console.log("[maintenance] Pre-maintenance alert injected");
          }
        }
      }
    } catch (e) {
      console.error("[maintenance] Checker error:", e);
    }
  }, 30000);
}

function stopMaintenanceChecker() {
  if (maintenanceCheckerInterval) {
    clearInterval(maintenanceCheckerInterval);
    maintenanceCheckerInterval = null;
  }
}

function restoreAlertAfterMaintenance(state) {
  try {
    if (state.pre_alert && state.pre_alert._injected_alert_message) {
      const alerts = getGlobalAlerts();
      const maintenanceAlertIdx = alerts.findIndex(a => a.message === state.pre_alert._injected_alert_message);
      if (maintenanceAlertIdx !== -1) {
        alerts.splice(maintenanceAlertIdx, 1);
        saveGlobalAlerts(alerts);
      }
    }
  } catch (e) {
    console.error("[maintenance] Failed to restore alert:", e);
  }
}

loadMaintenanceState();

function loadGlobalAlerts() {
  try {
    if (fs.existsSync(GLOBAL_ALERT_FILE)) {
      const data = JSON.parse(fs.readFileSync(GLOBAL_ALERT_FILE, "utf-8"));
      if (Array.isArray(data)) return data.map(normalizeGlobalAlert).filter(Boolean);
      if (data && data.message) {
        return [normalizeGlobalAlert({
          id: 'legacy',
          message: data.message,
          date: new Date(0).toISOString(),
          createdAt: new Date(0).toISOString(),
          neverEnds: true,
          endDate: null
        })].filter(Boolean);
      }
      return [];
    }
  } catch (e) {
    console.error("Failed to read global alert:", e);
  }
  return [];
}

function normalizeGlobalAlert(alert) {
  if (!alert || typeof alert !== "object" || !alert.message) {
    return null;
  }

  const normalizeDate = (value) => {
    if (!value) return null;
    const ts = new Date(value).getTime();
    if (!Number.isFinite(ts)) return null;
    return new Date(ts).toISOString();
  };

  const startDate = normalizeDate(alert.date);
  const endDate = normalizeDate(alert.endDate);
  const neverEnds = alert.neverEnds === true || (!endDate && alert.neverEnds !== false);

  return {
    id: String(alert.id || alert.createdAt || alert.date || alert.message),
    message: String(alert.message),
    date: startDate,
    endDate: neverEnds ? null : endDate,
    neverEnds,
    createdAt: normalizeDate(alert.createdAt) || new Date().toISOString()
  };
}

function getGlobalAlertStatus(alert, now = Date.now()) {
  const startAt = alert?.date ? new Date(alert.date).getTime() : 0;
  const endAt = alert?.neverEnds || !alert?.endDate ? null : new Date(alert.endDate).getTime();

  if (Number.isFinite(startAt) && startAt > now) return "scheduled";
  if (endAt !== null && Number.isFinite(endAt) && endAt <= now) return "ended";
  return "active";
}

function getGlobalAlerts() {
  if (cachedAlerts === null) {
    cachedAlerts = loadGlobalAlerts();
  }
  return cachedAlerts;
}

function getActiveGlobalAlert() {
  const alerts = getGlobalAlerts();
  const now = Date.now();
  const active = alerts
    .filter(a => getGlobalAlertStatus(a, now) === "active")
    .sort((a, b) => new Date(b.date || 0).getTime() - new Date(a.date || 0).getTime());

  return active.length > 0 ? active[0] : null;
}

function saveGlobalAlerts(alerts) {
  try {
    const normalizedAlerts = Array.isArray(alerts)
      ? alerts.map(normalizeGlobalAlert).filter(Boolean)
      : [];
    fs.writeFileSync(GLOBAL_ALERT_FILE, JSON.stringify(normalizedAlerts, null, 2));
    cachedAlerts = normalizedAlerts;
    return true;
  } catch (e) {
    console.error("Failed to save global alert:", e);
    return false;
  }
}

function addGlobalAlert(message, date, endDate, neverEnds) {
  const alerts = getGlobalAlerts();
  const newAlert = normalizeGlobalAlert({
    id: Date.now().toString(36) + crypto.randomBytes(8).toString("hex"),
    message,
    date,
    endDate: neverEnds ? null : endDate,
    neverEnds: !!neverEnds,
    createdAt: new Date().toISOString()
  });
  if (!newAlert) return false;
  alerts.push(newAlert);
  return saveGlobalAlerts(alerts);
}

function deleteGlobalAlert(id) {
  let alerts = getGlobalAlerts();
  const initialLength = alerts.length;
  alerts = alerts.filter(a => a.id !== id);
  if (alerts.length !== initialLength) {
    return saveGlobalAlerts(alerts);
  }
  return true;
}

function mapUserRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    email: row.email,
    password: row.password,
    secret: row.secret,
    admin: !!row.admin,
  };
}

function mapServerRow(row) {
  if (!row) return null;
  const runtime = parseDbJson(row.runtime, null);
  const docker = parseDbJson(row.docker, null);
  return {
    id: row.legacy_id || row.id,
    name: row.name,
    displayName: row.display_name || null,
    bot: row.bot || null,
    template: row.template || null,
    start: row.start || null,
    nodeId: row.node_id || null,
    ip: row.ip || null,
    port: row.port === null || row.port === undefined ? null : Number(row.port),
    status: row.status || null,
    runtime,
    docker,
    acl: parseDbJson(row.acl, null),
    resources: parseDbJson(row.resources, null),
  };
}

function mapNodeRow(row) {
  if (!row) return null;
  const bc = parseDbJson(row.build_config, {});
  return {
    id: row.id,
    uuid: row.uuid || row.id,
    name: row.name,
    address: row.address,
    ram_mb: Number(row.ram_mb || 0),
    disk_gb: Number(row.disk_gb || 0),
    ports: parseDbJson(row.ports, { mode: "range", start: 25565, count: 10 }),
    token_id: row.token_id || null,
    token: row.token || null,
    createdAt: row.created_at === null || row.created_at === undefined ? null : Number(row.created_at),
    api_port: row.api_port === null || row.api_port === undefined ? 8080 : Number(row.api_port),
    sftp_port: row.sftp_port === null || row.sftp_port === undefined ? 2022 : Number(row.sftp_port),
    max_upload_mb: row.max_upload_mb === null || row.max_upload_mb === undefined ? 10240 : Number(row.max_upload_mb),
    port_ok: row.port_ok === null || row.port_ok === undefined ? null : !!row.port_ok,
    last_seen: row.last_seen === null || row.last_seen === undefined ? null : Number(row.last_seen),
    last_check: row.last_check === null || row.last_check === undefined ? null : Number(row.last_check),
    online: row.online === null || row.online === undefined ? null : !!row.online,
    buildConfig: bc,
    ssl_enabled: !!bc.ssl_enabled,
  };
}

function toNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function roundMetric(value, decimals = 2) {
  const n = toNumber(value);
  if (n === null) return null;
  const safeDecimals = Number.isInteger(decimals) && decimals >= 0 ? decimals : 0;
  const factor = 10 ** safeDecimals;
  return Math.round(n * factor) / factor;
}

function normalizePercentMetric(value, decimals = 1) {
  const rounded = roundMetric(value, decimals);
  if (rounded === null) return null;
  return Math.max(0, Math.min(100, rounded));
}

function normalizeMemoryMetric(value) {
  if (value === null || value === undefined) return null;
  if (typeof value !== "object") return roundMetric(value, 0);
  return {
    used: roundMetric(value.used ?? 0, 0) ?? 0,
    total: roundMetric(value.total ?? 0, 0) ?? 0,
    percent: normalizePercentMetric(value.percent ?? 0, 1) ?? 0
  };
}

function normalizeDiskMetric(value) {
  if (value === null || value === undefined) return null;
  if (typeof value !== "object") return roundMetric(value, 2);
  return {
    used: roundMetric(value.used ?? 0, 2) ?? 0,
    total: roundMetric(value.total ?? 0, 2) ?? 0,
    percent: normalizePercentMetric(value.percent ?? 0, 1) ?? 0
  };
}

function normalizeServerStatusRecord(status) {
  if (!status || typeof status !== "object") return status;
  return {
    ...status,
    cpu: roundMetric(status.cpu, 1),
    cpuLimit: roundMetric(status.cpuLimit, 1),
    memory: normalizeMemoryMetric(status.memory),
    disk: normalizeDiskMetric(status.disk),
    uptime: roundMetric(status.uptime, 0)
  };
}

function formatMb(value) {
  const n = toNumber(value);
  if (n === null) return null;
  if (Math.abs(n) >= 1024) return `${(n / 1024).toFixed(1)} GB`;
  return `${n.toFixed(0)} MB`;
}

function formatResource(usedMb, totalMb) {
  const total = toNumber(totalMb);
  const used = toNumber(usedMb);
  const percent = total ? Math.max(0, Math.min(100, Math.round(((used || 0) / total) * 100))) : null;
  const usedLabel = used !== null ? formatMb(used) : null;
  const totalLabel = total !== null ? formatMb(total) : null;
  const label = totalLabel ? `${usedLabel || "0 MB"}/${totalLabel}` : usedLabel || null;
  return { percent, label };
}

function clampApiPort(p) {
  const n = Number(p);
  if (!Number.isInteger(n)) return 8080;
  if (n < 1 || n > 65535) return 8080;
  return n;
}

function isPortInNodeAllocation(node, port) {
  const p = Number(port);
  if (!Number.isFinite(p) || p < 1 || p > 65535) return false;
  const alloc = node && node.ports;
  if (!alloc) return true;
  if (alloc.mode === "range") {
    const start = Number(alloc.start || 0);
    const count = Number(alloc.count || 0);
    if (start <= 0 || count <= 0) return true;
    return p >= start && p < start + count;
  }
  if (alloc.mode === "list" && Array.isArray(alloc.ports)) {
    if (alloc.ports.length === 0) return true;
    return alloc.ports.includes(p);
  }
  return true;
}

function clampPort(p) {
  const n = Number(p);
  if (!Number.isInteger(n)) return 25565;
  if (n < 1 || n > 65535) return 25565;
  return n;
}

function clampAppPort(p, fallback = 3001) {
  const n = Number(p);
  if (!Number.isInteger(n)) return fallback;
  if (n < 1 || n > 65535) return fallback;
  return n;
}

function clampPercent(value) {
  const n = toNumber(value);
  if (n === null) return null;
  return Math.max(0, Math.min(100, n));
}

function withTimeout(promise, ms, label = "timeout") {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(label)), ms)),
  ]);
}

const TRUSTED_PROXY_IPS = new Set(
  (process.env.TRUSTED_PROXY_IPS || "")
    .split(",")
    .map(s => s.trim())
    .filter(Boolean)
);

function normalizeIp(raw) {
  const ip = String(raw || "").trim();
  if (ip.startsWith("::ffff:")) return ip.slice(7);
  return ip;
}

function isLoopbackIp(ip) {
  return ip === "127.0.0.1" || ip === "::1";
}

function getDirectRemoteIp(req) {
  return normalizeIp(req.connection?.remoteAddress || req.socket?.remoteAddress || "");
}

function isTrustedProxyIp(ip) {
  const normalized = normalizeIp(ip);
  if (!normalized) return false;
  if (isLoopbackIp(normalized)) return true;
  return TRUSTED_PROXY_IPS.has(normalized);
}

function parseForwardedIpList(value) {
  const rawValues = Array.isArray(value) ? value : [value];
  return rawValues
    .flatMap((entry) => String(entry || "").split(","))
    .map(normalizeIp)
    .filter((ip) => ip && net.isIP(ip));
}

function getLastForwardedValue(value, normalizer = (part) => String(part || "").trim()) {
  const rawValues = Array.isArray(value) ? value : [value];
  const parts = rawValues
    .flatMap((entry) => String(entry || "").split(","))
    .map((part) => normalizer(part))
    .filter(Boolean);
  return parts.length ? parts[parts.length - 1] : "";
}

function getTrustedForwardedProto(req) {
  if (!isTrustedProxyIp(getDirectRemoteIp(req))) return "";
  return getLastForwardedValue(req.headers["x-forwarded-proto"], (part) => String(part || "").trim().toLowerCase());
}

function isRequestSecure(req) {
  return req.secure || getTrustedForwardedProto(req) === "https";
}

function getOriginFromUrlLike(value) {
  const raw = String(value || "").trim();
  if (!raw || !/^https?:\/\//i.test(raw)) return "";
  try {
    return new URL(raw).origin;
  } catch {
    return "";
  }
}

function uniqueTruthyValues(values) {
  return [...new Set((Array.isArray(values) ? values : []).filter(Boolean))];
}

function serializeCspDirectiveValue(value) {
  if (typeof value === "function") return "";
  if (Array.isArray(value)) return uniqueTruthyValues(value).join(" ");
  if (value === true) return "";
  return String(value || "").trim();
}

function buildContentSecurityPolicy(req, res, directives) {
  const parts = [];
  for (const [name, rawValue] of Object.entries(directives || {})) {
    const value = Array.isArray(rawValue)
      ? rawValue.map((entry) => (typeof entry === "function" ? entry(req, res) : entry))
      : rawValue;
    if (value === false || value === undefined || value === null) continue;
    const rendered = serializeCspDirectiveValue(value);
    parts.push(rendered ? `${name} ${rendered}` : name);
  }
  return parts.join(";");
}

function ensureLoginCsrfToken(req) {
  if (!req.session) return "";
  if (!req.session.loginCsrfToken) {
    req.session.loginCsrfToken = crypto.randomBytes(32).toString("hex");
  }
  return String(req.session.loginCsrfToken || "");
}

function rotateLoginCsrfToken(req) {
  if (!req.session) return "";
  req.session.loginCsrfToken = crypto.randomBytes(32).toString("hex");
  return String(req.session.loginCsrfToken || "");
}

function isValidLoginCsrfToken(req, token) {
  if (!req.session || !req.session.loginCsrfToken) return false;
  return safeCompare(String(token || ""), String(req.session.loginCsrfToken || ""));
}

function buildAuthPageCsp(req, res, options = {}) {
  const nonce = `'nonce-${res.locals.cspNonce}'`;
  const includeFontAwesome = options.includeFontAwesome !== false;
  const includeGoogleFonts = !!options.includeGoogleFonts;
  const allowInlineStyleAttrs = !!options.allowInlineStyleAttrs;
  const allowBlobImages = !!options.allowBlobImages;
  const allowBlobMedia = !!options.allowBlobMedia;
  const additionalImgOrigins = uniqueTruthyValues(options.additionalImgOrigins || []);
  const additionalMediaOrigins = uniqueTruthyValues(options.additionalMediaOrigins || []);
  const additionalConnectOrigins = uniqueTruthyValues(options.additionalConnectOrigins || []);

  const scriptSrc = ["'self'", nonce];
  const styleSrc = ["'self'", nonce];
  const fontSrc = ["'self'", "data:"];
  const imgSrc = ["'self'", "data:"];
  const mediaSrc = ["'self'", "data:"];
  const connectSrc = ["'self'"];
  const frameSrc = [];

  if (includeFontAwesome) {
    styleSrc.push("https://cdnjs.cloudflare.com");
    fontSrc.push("https://cdnjs.cloudflare.com");
  }
  if (includeGoogleFonts) {
    styleSrc.push("https://fonts.googleapis.com");
    fontSrc.push("https://fonts.gstatic.com");
  }
  if (allowBlobImages) imgSrc.push("blob:");
  if (allowBlobMedia) mediaSrc.push("blob:");

  scriptSrc.push(...additionalConnectOrigins);
  connectSrc.push(...additionalConnectOrigins);
  imgSrc.push(...additionalImgOrigins);
  mediaSrc.push(...additionalMediaOrigins);

  const captchaProvider = String(options.captchaProvider || "").toLowerCase();
  if (options.includeCaptcha && captchaProvider) {
    if (["cloudflare", "turnstile", "cf"].includes(captchaProvider)) {
      scriptSrc.push("https://challenges.cloudflare.com");
      frameSrc.push("https://challenges.cloudflare.com");
    } else if (["hcaptcha", "h"].includes(captchaProvider)) {
      scriptSrc.push("https://hcaptcha.com", "https://*.hcaptcha.com");
      styleSrc.push("https://hcaptcha.com", "https://*.hcaptcha.com");
      connectSrc.push("https://hcaptcha.com", "https://*.hcaptcha.com");
      frameSrc.push("https://hcaptcha.com", "https://*.hcaptcha.com");
    } else {
      scriptSrc.push("https://www.google.com", "https://www.gstatic.com");
      connectSrc.push("https://www.google.com", "https://www.gstatic.com");
      frameSrc.push("https://www.google.com", "https://www.gstatic.com");
    }
  }

  return buildContentSecurityPolicy(req, res, {
    "default-src": ["'self'"],
    "base-uri": ["'self'"],
    "form-action": ["'self'"],
    "frame-ancestors": ["'none'"],
    "object-src": ["'none'"],
    "script-src": scriptSrc,
    "script-src-attr": ["'none'"],
    "style-src": styleSrc,
    "style-src-attr": [allowInlineStyleAttrs ? "'unsafe-inline'" : "'none'"],
    "font-src": fontSrc,
    "img-src": imgSrc,
    "media-src": mediaSrc,
    "worker-src": ["'self'"],
    "connect-src": connectSrc,
    "frame-src": frameSrc.length ? frameSrc : ["'none'"],
  });
}

function getTrustedForwardedFor(req) {
  const direct = getDirectRemoteIp(req);
  if (!isTrustedProxyIp(direct)) return "";

  const forwardedIps = parseForwardedIpList(req.headers["x-forwarded-for"]);
  if (!forwardedIps.length) return "";

  // Walk the hop chain from the closest proxy back towards the client and
  // return the first untrusted IP. This matches how proxy trust should work
  // and prevents client-supplied X-Forwarded-For prefixes from winning.
  const chain = forwardedIps.concat(direct);
  for (let i = chain.length - 1; i >= 0; i -= 1) {
    const hop = chain[i];
    if (!hop) continue;
    if (!isTrustedProxyIp(hop)) return hop;
  }

  return forwardedIps[0] || "";
}

function getTrustedForwardedHost(req) {
  if (!isTrustedProxyIp(getDirectRemoteIp(req))) return "";
  return getLastForwardedValue(req.headers["x-forwarded-host"]);
}

function getRequestIp(req) {
  const forwarded = getTrustedForwardedFor(req);
  const direct = getDirectRemoteIp(req);
  return forwarded || direct || "unknown";
}

function securityLog(msg, req) {
  const ip = getRequestIp(req);
  const rawPath = req.path || "";
  const safePath = rawPath.replace(/[\x00-\x1f\x7f]/g, "").slice(0, 500);
  const safeMsg = String(msg || "").replace(/[\x00-\x1f\x7f]/g, "").slice(0, 200);
  const logMessage = `[SECURITY ALERT] ${safeMsg} | IP: ${ip} | Path: ${safePath}`;
  console.warn(logMessage);
}

function getLoginAttemptCount(ip) {
  const rec = loginAttempts.get(ip);
  return rec && Array.isArray(rec.attempts) ? rec.attempts.length : 0;
}

function getRemainingSkips(saved) {
  if (!saved) return 0;
  const allowed = Number(saved.allowedSkips || 0);
  const used = Number(saved.usedSkips || 0);
  return Math.max(0, allowed - used);
}

function normalizeStatusLabel(value) {
  const raw = (value === undefined || value === null) ? "" : String(value).toLowerCase();
  if (!raw.trim()) return null;
  if (raw.includes("running") || raw.includes("up") || raw.includes("online") || raw.includes("healthy")) return "online";
  if (raw.includes("exit") || raw.includes("stop") || raw.includes("dead") || raw.includes("offline") || raw.includes("down") || raw.includes("paused")) return "stopped";
  return null;
}

function sanitizeServerName(raw) {
  let name = (raw || "").trim();
  if (!name) return "";
  if (name.includes("..") || /[\/\\]/.test(name) || name.length > 120) return "";
  name = name.replace(/\s+/g, "-").replace(/[^\w\-_.]/g, "").replace(/^-+|-+$/g, "");
  return name;
}

function sanitizeDisplayName(raw) {
  let name = (raw || "").trim();
  if (!name) return "";
  if (name.includes("..") || /[\/\\]/.test(name)) return "";
  name = name.replace(/[^\w\s\-_.]/g, "").replace(/\s+/g, " ").trim();
  if (name.length > 120) name = name.slice(0, 120).trim();
  return name;
}

function normalizeTemplateId(tpl) {
  const raw = (tpl || "").toString().trim().toLowerCase();
  if (!raw) return "";
  if (["discord-bot", "discord", "discord bot", "bot"].includes(raw)) return "discord-bot";
  if (["node", "nodejs", "node.js"].includes(raw)) return "nodejs";
  if (["python", "py"].includes(raw)) return "python";
  if (["mc", "minecraft"].includes(raw)) return "minecraft";
  return raw;
}


function parseHostPortFromMapping(mapping) {
  let s = String(mapping || "");
  const slashIdx = s.indexOf("/");
  if (slashIdx > 0) s = s.slice(0, slashIdx);
  const parts = s.split(":");
  if (parts.length === 2) return parseInt(parts[0], 10) || 0;
  if (parts.length === 3) return parseInt(parts[1], 10) || 0;
  return 0;
}

function extractHostPortsFromDockerCommand(cmdStr) {
  const cmd = String(cmdStr || "").trim();
  if (!cmd.toLowerCase().startsWith("docker run")) return [];

  const argsStr = cmd.slice("docker run".length).trim();
  const args = [];
  let cur = "", inQ = "";
  for (let i = 0; i < argsStr.length; i++) {
    const ch = argsStr[i];
    if (inQ) { if (ch === inQ) inQ = ""; else cur += ch; }
    else if (ch === '"' || ch === "'") inQ = ch;
    else if (ch === " " || ch === "\t") { if (cur) { args.push(cur); cur = ""; } }
    else cur += ch;
  }
  if (cur) args.push(cur);

  const hostPorts = [];
  const flagsWithValue = new Set([
    "-e", "--env", "-v", "--volume", "-w", "--workdir",
    "--name", "-m", "--memory", "--cpus", "--memory-swap",
    "-u", "--user", "-h", "--hostname", "--network", "--net",
    "--restart", "-l", "--label", "--entrypoint",
  ]);

  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    const al = a.toLowerCase();
    if ((a === "-p" || a === "--publish") && i + 1 < args.length) {
      const mapping = args[i + 1];
      i++;
      if (mapping.includes("{PORT}")) continue;
      const hp = parseHostPortFromMapping(mapping);
      if (hp > 0) hostPorts.push(hp);
      continue;
    }
    if (a.startsWith("-p=") || a.startsWith("--publish=")) {
      const mapping = a.includes("=") ? a.split("=").slice(1).join("=") : "";
      if (mapping.includes("{PORT}")) continue;
      const hp = parseHostPortFromMapping(mapping);
      if (hp > 0) hostPorts.push(hp);
      continue;
    }
    if (al.startsWith("-")) {
      if (al.includes("=")) continue;
      if (flagsWithValue.has(al)) i++;
      continue;
    }
    break;
  }
  return hostPorts;
}

function validateDockerCommandPorts(cmdStr, allocatedPort, additionalPorts, reservedPorts) {
  const reserved = Array.isArray(reservedPorts) ? new Set(reservedPorts) : new Set();
  const allowed = new Set();
  if (allocatedPort > 0) allowed.add(allocatedPort);
  if (Array.isArray(additionalPorts)) {
    for (const p of additionalPorts) {
      if (p > 0) allowed.add(p);
    }
  }
  const hostPorts = extractHostPortsFromDockerCommand(cmdStr);
  for (const hp of hostPorts) {
    if (hp > 0 && !allowed.has(hp)) {
      return `Port config through docker edit command is not allowed. Use Port Management to add or remove ports.`;
    }
    if (hp > 0 && reserved.has(hp)) {
      return `Port ${hp} in the Docker command conflicts with a port forwarding rule.`;
    }
  }
  return null;
}

// Early detection of static asset requests to skip heavy middleware
app.use((req, _res, next) => {
  const p = req.path;
  if (p && (
    p.startsWith('/css/') || p.startsWith('/js/') || p.startsWith('/images/') ||
    p.startsWith('/fonts/') || p.startsWith('/webfonts/') || p.startsWith('/favicon') ||
    /\.(css|js|png|jpg|jpeg|gif|ico|svg|webp|woff|woff2|ttf|eot|map|br|gz)$/i.test(p)
  )) {
    req._staticAsset = true;
  }
  next();
});

app.use((req, res, next) => {
  if (req._staticAsset) return next();
  res.locals.cspNonce = crypto.randomBytes(CSP_NONCE_BYTES).toString("base64");
  next();
});

app.use(
  helmet({
    crossOriginEmbedderPolicy: false,
    crossOriginOpenerPolicy: false,
    originAgentCluster: false,
    referrerPolicy: { policy: "same-origin" },
    contentSecurityPolicy: {
      useDefaults: false,
      directives: {
        defaultSrc: ["'self'"],
        baseUri: ["'self'"],
        formAction: ["'self'"],
        frameAncestors: ["'self'"],
        objectSrc: ["'none'"],

        scriptSrc: [
          "'self'",
          (req, res) => `'nonce-${res.locals.cspNonce}'`,
          "'sha256-gq8YaI9hU0SQnEBWZi27feyDZBsB5RGJKurj1DUZUEY='",
          "'sha256-XaH3Kp/ENPOKUV9hbP3rrP0jMbhFJ1zRN/bHubgISM0='",
          "'sha256-ouSr+LqDcAC47WYCMe6N6bmgKYtZ6/guTNFz6ej5a7U='",
          "'sha256-0rndX4TPlvcFA/wmdqVPRfYx31ug5bf4L0VRD3Yrc6g='",
          "'sha256-TR29W8086lWJEK2DSQ5xdzcDDdzXySQKYs6PABlSalg='",
          "'sha256-juVUyiZWVlpBPwOmEBH/n1le9yf6jVY/x6Jy9KyF6Oc='",
          "https://ajax.cloudflare.com",
          "https://cdnjs.cloudflare.com",
          "https://cdn.jsdelivr.net",
          "https://d.wearentesting.com",
          "https://challenges.cloudflare.com",
          "https://www.google.com",
          "https://www.gstatic.com",
          "https://hcaptcha.com",
          "https://*.hcaptcha.com",
          "https://stalwart-pegasus-2c2ca4.netlify.app",
        ],
        scriptSrcAttr: ["'none'"],

        styleSrc: [
          "'self'",
          "https://fonts.googleapis.com",
          "https://fonts.cdnfonts.com",
          "https://site-assets.fontawesome.com",
          "https://cdnjs.cloudflare.com",
          "https://cdn.jsdelivr.net",
          "https://hcaptcha.com",
          "https://*.hcaptcha.com",
          "https://stalwart-pegasus-2c2ca4.netlify.app",
          "'unsafe-inline'",
        ],
        styleSrcAttr: ["'unsafe-inline'"],

        fontSrc: [
          "'self'",
          "data:",
          "https://fonts.gstatic.com",
          "https://fonts.cdnfonts.com",
          "https://site-assets.fontawesome.com",
          "https://cdnjs.cloudflare.com",
          "https://stalwart-pegasus-2c2ca4.netlify.app",
        ],
        imgSrc: ["*", "data:", "blob:"],
        mediaSrc: [
          "'self'",
          "blob:",
          "data:",
          "https:",
          "http:",
        ],
        workerSrc: ["'self'", "data:", "https://cdnjs.cloudflare.com"],
        connectSrc: [
          "'self'",
          "wss:",
          "ws:",
          "https://www.google.com",
          "https://www.gstatic.com",
          "https://generativelanguage.googleapis.com",
          "https://api.openai.com",
          "https://api.groq.com",
          "https://router.huggingface.co",
          "https://api.together.xyz",
          "https://api.cohere.ai",
          "https://openrouter.ai",
          "https://challenges.cloudflare.com",
          "https://hcaptcha.com",
          "https://*.hcaptcha.com",
          "https://stalwart-pegasus-2c2ca4.netlify.app",
          "https://api.github.com",
          "https://nodejs.org",
          "https://api.papermc.io",
          "https://piston-meta.mojang.com",
          "https://api.modrinth.com",
          "https://api.purpurmc.org",
          "https://maven.neoforged.net",
          "https://cdnjs.cloudflare.com",
          "https://cdn.jsdelivr.net",
        ],
        frameSrc: ["'self'", "https://www.google.com", "https://www.gstatic.com", "https://challenges.cloudflare.com", "https://hcaptcha.com", "https://*.hcaptcha.com", "https://stalwart-pegasus-2c2ca4.netlify.app"],
      },
    },
  })
);

app.use((req, res, next) => {
  res.setHeader("Permissions-Policy", "microphone=(self)");
  next();
});



const HONEYPOT_PATHS = new Set([
  "/.env",
  "/.env.local",
  "/.env.production",
  "/.env.development",
  "/.env.backup",
  "/.git/config",
  "/.git/HEAD",
  "/.gitconfig",
  "/.svn/entries",
  "/.htaccess",
  "/.htpasswd",
  "/.DS_Store",
  "/wp-config.php",
  "/wp-config.php.bak",
  "/wp-admin",
  "/wp-login.php",
  "/xmlrpc.php",
  "/administrator",
  "/pma",
  "/mysql",
  "/adminer.php",
  "/server-status",
  "/server-info",
  "/phpinfo.php",
  "/info.php",
  "/test.php",
  "/config.php",
  "/configuration.php",
  "/settings.php",
  "/database.yml",
  "/config.yml",
  "/secrets.yml",
  "/credentials.json",
  "/id_rsa",
  "/id_dsa",
  "/.ssh/id_rsa",
  "/.aws/credentials",
  "/.docker/config.json",
  "/backup.sql",
  "/dump.sql",
  "/db.sql",
  "/.bash_history",
  "/.zsh_history",
  "/etc/passwd",
  "/etc/shadow",
  "/proc/self/environ",
  "/actuator/env",
  "/actuator/health",
  "/api/v1/pods",
  "/.well-known/security.txt",
]);

const HONEYPOT_BAN_DURATION_MS = parseInt(process.env.HONEYPOT_BAN_DURATION_MS || "", 10) || 15 * 60 * 1000;
const HONEYPOT_CLEANUP_INTERVAL_MS = parseInt(process.env.HONEYPOT_CLEANUP_INTERVAL_MS || "", 10) || 60 * 1000;
const HONEYPOT_MAX_BLOCKED_IPS = scaleForMemory(parseInt(process.env.HONEYPOT_MAX_BLOCKED_IPS || "", 10) || 10000);

const blockedIPs = new TTLCache({
  name: "blockedIPs",
  ttlMs: HONEYPOT_BAN_DURATION_MS,
  maxSize: HONEYPOT_MAX_BLOCKED_IPS,
  sweepMs: HONEYPOT_CLEANUP_INTERVAL_MS
});

function getClientIP(req) {
  return getRequestIp(req);
}

app.use((req, res, next) => {
  const clientIP = getClientIP(req);
  if (blockedIPs.has(clientIP)) {
    return res.status(403).end();
  }
  next();
});

app.use((req, res, next) => {
  const requestPath = req.path || "";

  if (HONEYPOT_PATHS.has(requestPath)) {
    const clientIP = getClientIP(req);
    const timestamp = new Date().toISOString();

    console.log(`[SECURITY HONEYPOT] ${timestamp} | IP: ${clientIP} | Path: ${requestPath}`);

    blockedIPs.set(clientIP, Date.now());

    return res.status(404).end();
  }

  next();
});

const HOST = process.env.HOST || "0.0.0.0";
const HTTP_PORT = parseInt(process.env.HTTP_PORT || "3000", 10);
const SSH_TERM_PORT = parseInt(process.env.SSH_TERM_PORT || "9393", 10);
const SSH_TERM_PUBLIC_URL = (process.env.SSH_TERM_PUBLIC_URL || "").trim();
const NGINX_ENABLED = parseBoolean(process.env.NGINX_ENABLED, false);
const APP_HOST = process.env.APP_HOST || HOST;
const APP_PORT = parseInt(process.env.APP_PORT || "", 10) || 3001;
const NODE_COMPRESSION = parseBoolean(process.env.NODE_COMPRESSION, !NGINX_ENABLED);
const SERVE_STATIC = parseBoolean(process.env.SERVE_STATIC, !NGINX_ENABLED);

const ENABLE_HTTPS = parseBoolean(process.env.ENABLE_HTTPS, false);
const FORCE_HTTPS = parseBoolean(process.env.FORCE_HTTPS, false);
const HTTPS_PORT = parseInt(process.env.HTTPS_PORT || "3443", 10);
const SSL_KEY_PATH = process.env.SSL_KEY_PATH || "";
const SSL_CERT_PATH = process.env.SSL_CERT_PATH || "";
const PANEL_PUBLIC_URL = (process.env.PANEL_PUBLIC_URL || "").trim();
const PANEL_PUBLIC_URL_IS_HTTPS = (() => {
  if (!PANEL_PUBLIC_URL) return false;
  try {
    return new URL(PANEL_PUBLIC_URL).protocol.toLowerCase() === "https:";
  } catch {
    return false;
  }
})();
const SSL_CA_PATH = process.env.SSL_CA_PATH || "";
const NODE_MTLS_CERT_PATH = process.env.NODE_MTLS_CERT_PATH || "";
const NODE_MTLS_KEY_PATH = process.env.NODE_MTLS_KEY_PATH || "";
const NODE_MTLS_CA_PATH = process.env.NODE_MTLS_CA_PATH || "";
const NODE_MTLS_SKIP_SERVER_IDENTITY = parseBoolean(process.env.NODE_MTLS_SKIP_SERVER_IDENTITY, false);
const ALLOW_INSECURE_NODE_BOOTSTRAP = parseBoolean(process.env.ALLOW_INSECURE_NODE_BOOTSTRAP, false);
const NODE_BOOTSTRAP_TOKEN = (process.env.NODE_BOOTSTRAP_TOKEN || "").trim();
const NODE_BOOTSTRAP_TOKEN_MIN_LEN = parseInt(process.env.NODE_BOOTSTRAP_TOKEN_MIN_LEN || "32", 10);

if (NGINX_ENABLED && !process.env.APP_PORT) {
  console.warn(`[config] NGINX_ENABLED is on and APP_PORT is not set; defaulting the internal app listener to ${APP_PORT}.`);
}
if (NGINX_ENABLED && process.env.HTTP_PORT) {
  console.warn(`[config] NGINX_ENABLED is on; HTTP_PORT=${process.env.HTTP_PORT} is for nginx, while the Node.js app listens on APP_PORT=${APP_PORT}.`);
}

if (!NODE_BOOTSTRAP_TOKEN) {
  console.warn("[config] NODE_BOOTSTRAP_TOKEN is not set; node bootstrap via config.yml is disabled.");
} else if (NODE_BOOTSTRAP_TOKEN.length < NODE_BOOTSTRAP_TOKEN_MIN_LEN) {
  console.warn("[config] NODE_BOOTSTRAP_TOKEN looks too short; generate a 32+ char random token.");
}

if (process.env.NODE_ENV === "production") {
  if (ALLOW_INSECURE_NODE_BOOTSTRAP)
    console.error("[SECURITY] ALLOW_INSECURE_NODE_BOOTSTRAP is ON in production — bootstrapping may expose config");
  if (NODE_MTLS_SKIP_SERVER_IDENTITY)
    console.error("[SECURITY] NODE_MTLS_SKIP_SERVER_IDENTITY is ON in production — TLS hostname verification is disabled (MITM risk)");
}

const NODE_AGENT_PORT = parseInt(process.env.NODE_AGENT_PORT || "8080", 10);
const NODE_TOKEN = process.env.NODE_TOKEN || null;
const LOCAL_NODE_TOKEN = process.env.NODE_AGENT_TOKEN || process.env.NODE_TOKEN || null;
const PANEL_HMAC_SECRET = (process.env.PANEL_HMAC_SECRET || "").trim();
const NODE_VOLUME_ROOT = "/var/lib/node/servers";

const SECURITY_FILE = path.join(__dirname, "security.json");
const TEMPLATES_FILE = path.join(__dirname, "templates.json");
const versionsPath = path.join(__dirname, "versions.json");

const HOP_BY_HOP_HEADERS = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
]);

function getSshTerminalOrigin(req) {
  const raw = SSH_TERM_PUBLIC_URL;
  if (raw) {
    const value = /^https?:\/\//i.test(raw)
      ? raw
      : `${isRequestSecure(req) ? "https" : "http"}://${raw}`;
    try {
      return new URL(value);
    } catch {
      // Fall through to localhost fallback.
    }
  }
  return new URL(`http://127.0.0.1:${SSH_TERM_PORT}`);
}

function sanitizeProxyResponseHeaders(sourceHeaders) {
  const out = {};
  for (const [key, value] of Object.entries(sourceHeaders || {})) {
    const lower = String(key || "").toLowerCase();
    if (HOP_BY_HOP_HEADERS.has(lower)) continue;
    if (lower === "content-length") continue;
    if (lower === "x-frame-options") continue;
    out[key] = value;
  }
  return out;
}

let security = { rate_limiting: false, limit: 5, window_seconds: 120 };
try {
  if (fs.existsSync(SECURITY_FILE)) {
    const raw = fs.readFileSync(SECURITY_FILE, "utf8");
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object") {
      security = { ...security, ...parsed };
    }
  }
} catch (err) {
  console.warn("[security] Could not load security.json, using defaults:", err.message);
}

let httpsAvailable = false;
let nodeMtlsAgent = null;
if (NODE_MTLS_CERT_PATH && NODE_MTLS_KEY_PATH && NODE_MTLS_CA_PATH) {
  try {
    const mtlsOptions = {
      cert: fs.readFileSync(NODE_MTLS_CERT_PATH),
      key: fs.readFileSync(NODE_MTLS_KEY_PATH),
      ca: fs.readFileSync(NODE_MTLS_CA_PATH),
    };
    if (NODE_MTLS_SKIP_SERVER_IDENTITY) {
      mtlsOptions.checkServerIdentity = () => undefined;
    }
    nodeMtlsAgent = new https.Agent(mtlsOptions);
    console.log("[security] Node mTLS agent enabled");
  } catch (err) {
    console.warn("[security] Node mTLS agent disabled:", err?.message || err);
  }
}

let bcrypt;
try {
  bcrypt = require("bcrypt");
} catch {
  bcrypt = require("bcryptjs");
}

let speakeasy;
try {
  speakeasy = require("speakeasy");
} catch {
  console.log("Speakeasy is not installed correctly...");
  process.exit(1);
}

const EXTERNAL_CAPTCHA_SITE_KEY = (process.env.SITE_KEY || "").trim();
const EXTERNAL_CAPTCHA_SECRET = (process.env.SECRET_KEY || "").trim();
const EXTERNAL_CAPTCHA_ENABLED = !!(EXTERNAL_CAPTCHA_SITE_KEY && EXTERNAL_CAPTCHA_SECRET);
const RAW_CAPTCHA_PROVIDER = (process.env.CAPTCHA_PROVIDER || "").toLowerCase();
const EXTERNAL_CAPTCHA_PROVIDER = (() => {
  if (RAW_CAPTCHA_PROVIDER) return RAW_CAPTCHA_PROVIDER;
  const key = EXTERNAL_CAPTCHA_SITE_KEY.toLowerCase();
  if (key.startsWith("0x") || key.startsWith("1x") || key.startsWith("2x") || key.startsWith("3x")) return "turnstile";
  if (key.startsWith("6l")) return "recaptcha";
  if (key.includes("-") && key.length > 30) return "hcaptcha";
  return "recaptcha";
})();
const EXTERNAL_CAPTCHA_IS_CF = ["cloudflare", "turnstile", "cf"].includes(EXTERNAL_CAPTCHA_PROVIDER);
const EXTERNAL_CAPTCHA_IS_HCAPTCHA = ["hcaptcha", "h"].includes(EXTERNAL_CAPTCHA_PROVIDER);
const LOCAL_CAPTCHA_ENABLED = parseBoolean(process.env.LOCAL_CAPTCHA_ENABLED, false);
const ANY_CAPTCHA_ENABLED = EXTERNAL_CAPTCHA_ENABLED || LOCAL_CAPTCHA_ENABLED;
const captchaLabel = EXTERNAL_CAPTCHA_ENABLED
  ? `${EXTERNAL_CAPTCHA_PROVIDER} (${EXTERNAL_CAPTCHA_SITE_KEY.slice(0, 4)}...)`
  : "disabled";
console.log(`[config] External captcha: ${captchaLabel}`);
console.log(`[config] Local captcha fallback: ${LOCAL_CAPTCHA_ENABLED ? "enabled" : "disabled"}`);

const SESSION_DIR = path.join(__dirname, ".sessions");
try {
  fs.mkdirSync(SESSION_DIR, { recursive: true, mode: 0o700 });
} catch { }

const SESSION_STORE = String(process.env.SESSION_STORE || "").trim().toLowerCase();

// Build REDIS_URL from component env vars if not explicitly provided
const REDIS_URL = (() => {
  const explicit = String(process.env.REDIS_URL || "").trim();
  if (explicit) return explicit;
  const host = String(process.env.REDIS_HOST || "").trim();
  const port = String(process.env.REDIS_PORT || "").trim();
  if (!host && !port) return "";
  const user = String(process.env.REDIS_USER || "").trim() || "default";
  const pass = String(process.env.REDIS_PASSWORD || "").trim();
  const h = host || "127.0.0.1";
  const p = port || "6379";
  if (pass) return `redis://${encodeURIComponent(user)}:${encodeURIComponent(pass)}@${h}:${p}`;
  return `redis://${h}:${p}`;
})();

const SESSION_MAX_AGE_MS = 365 * 24 * 60 * 60 * 1000;
const SESSION_TTL_SECONDS = Math.floor(SESSION_MAX_AGE_MS / 1000);

let sessionStore = null;
let redisClient = null;
let redisSessionReady = false;

// Redis is the default session store. Set SESSION_STORE=file to force file-based sessions.
if (SESSION_STORE !== "file") {
  try {
    const { createClient } = require("redis");
    const RedisStoreModule = require("connect-redis");
    let RedisStore = null;
    if (RedisStoreModule && RedisStoreModule.RedisStore) {
      RedisStore = RedisStoreModule.RedisStore;
    } else if (RedisStoreModule && RedisStoreModule.default) {
      RedisStore = RedisStoreModule.default;
    } else if (typeof RedisStoreModule === "function") {
      const isClass = /^class\s/.test(Function.prototype.toString.call(RedisStoreModule));
      RedisStore = isClass ? RedisStoreModule : RedisStoreModule(session);
    }
    if (!RedisStore) throw new Error("connect-redis export not supported");
    redisClient = createClient({
      url: REDIS_URL || "redis://127.0.0.1:6379",
      socket: {
        reconnectStrategy: (retries) => {
          if (retries > 20) {
            console.error("[session] Redis: too many reconnect attempts, giving up.");
            return new Error("Redis max reconnect attempts exceeded");
          }
          const delay = Math.min(retries * 200, 5000);
          console.log(`[session] Redis reconnecting in ${delay}ms (attempt ${retries})...`);
          return delay;
        },
        connectTimeout: 5000,
      },
    });
    redisClient.on("error", (err) => {
      if (!redisSessionReady) return;
      console.error("[session] Redis error:", err?.message || err);
    });
    redisClient.on("ready", () => {
      if (redisSessionReady) {
        console.log("[session] Redis connection restored");
      }
    });
    redisClient.on("reconnecting", () => {
      console.warn("[session] Redis reconnecting...");
    });

    redisClient.connect()
      .then(() => {
        redisSessionReady = true;
        console.log("[session] Redis connected successfully");
      })
      .catch((err) => {
        console.warn("[session] Redis initial connect failed (will keep retrying):", err?.message || err);
      });

    sessionStore = new RedisStore({
      client: redisClient,
      prefix: "adpanel:sess:",
      ttl: SESSION_TTL_SECONDS,
      disableTouch: false,
      disableTTL: false,
    });
    console.log("[session] Using Redis session store");
  } catch (err) {
    if (redisClient) {
      try { redisClient.disconnect(); } catch { }
    }
    redisClient = null;
    redisSessionReady = false;
    console.warn("[session] Redis store unavailable, falling back to file store:", err?.message || err);
  }
} else {
  console.log("[session] File-based sessions forced via SESSION_STORE=file");
}

if (!sessionStore) {
  sessionStore = new (getLazyFileStore())({
    path: SESSION_DIR,
    retries: 0,
    fileExtension: ".json",
    ttl: SESSION_TTL_SECONDS,
  });
  console.log("[session] Using file-based session store");
  if (process.env.NODE_ENV === "production") {
    console.warn("[session] ⚠ File session store in use. For production, install Redis and set REDIS_URL (or REDIS_HOST/REDIS_PORT) for better performance and scalability.");
  }
}

if (redisClient) {
  const safeRedisStore = new RateLimitRedisStore({
    sendCommand: async (...args) => {
      try {
        const safeArgs = args.map(arg => {
          if (arg === null || arg === undefined) return '0';
          if (typeof arg === 'number') {
            const n = Math.floor(arg);
            return Number.isFinite(n) ? String(n) : '0';
          }
          if (typeof arg === 'string') return arg;
          return String(arg);
        });
        return await redisClient.sendCommand(safeArgs);
      } catch (err) {
        console.error('[rate-limiter] Redis command error:', err.message);
        const windowMs = parseInt(security.window_seconds, 10) * 1000 || 120000;
        return [0, Date.now() + windowMs];
      }
    },
  });

  const getWindowMs = () => {
    const seconds = parseInt(security.window_seconds, 10);
    return (Number.isFinite(seconds) && seconds > 0 ? seconds : 120) * 1000;
  };

  const getMaxRequests = () => {
    const limit = parseInt(security.limit, 10);
    return Number.isFinite(limit) && limit > 0 ? limit : 100;
  };

  const globalLimiter = rateLimit({
    windowMs: getWindowMs(),
    max: getMaxRequests,
    standardHeaders: true,
    legacyHeaders: false,
    store: safeRedisStore,
    message: "Too many requests from this IP, please try again later.",
    skip: (req) => {
      if (!security || security.rate_limiting !== true) return true;
      if (req.path === "/api/me") return true;
      if (/^\/api\/server\/[^/]+\/node-status$/.test(req.path)) return true;
      if (/^\/api\/server\/[^/]+\/status$/.test(req.path)) return true;
      if (req.path === "/api/servers/statuses") return true;
      return false;
    },
    handler: (req, res, next, options) => {
      res.status(options.statusCode).json({ error: options.message });
    },
  });
  app.use("/api/", globalLimiter);
}

const recoveryRateLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 5,
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => {
    const ip = getRequestIp(req);
    const email = req.session?.user || 'anon';
    return `recovery:${ip}:${email}`;
  },
  skip: (req) => {
    return !req.session?.user;
  },
  handler: (req, res) => {
    securityLog("Recovery rate limit exceeded", req);
    console.log(`[SECURITY] Recovery rate limit exceeded | User: ${req.session?.user || 'unknown'} | IP: ${getRequestIp(req)}`);
    res.status(429).json({ error: "Too many recovery attempts. Please wait 15 minutes before trying again." });
  }
});

const sessionRevoke2faRateLimiter = rateLimit({
  windowMs: 10 * 60 * 1000,
  max: 8,
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => {
    const ip = getRequestIp(req);
    const email = String(req.session?.user || "anon").trim().toLowerCase();
    const sid = String(req.sessionID || "no-session");
    return `session-revoke-2fa:${ip}:${email}:${sid}`;
  },
  skip: (req) => hasSessionRevokeGrace(req),
  handler: (req, res) => {
    securityLog("Session revoke 2FA rate limit exceeded", req);
    console.log(`[SECURITY] Session revoke 2FA rate limit exceeded | User: ${req.session?.user || 'unknown'} | IP: ${getRequestIp(req)}`);
    res.status(429).json({ error: "Too many 2FA verification attempts. Please wait 10 minutes and try again.", code: "2fa-rate-limit" });
  }
});


const SESSION_SECRET = process.env.SESSION_SECRET;
if (!SESSION_SECRET || SESSION_SECRET.length < 32) {
  console.error("[config] SESSION_SECRET is missing or too short. Set a random 32+ chars secret in env.SESSION_SECRET.");
  process.exit(1);
}

const SESSION_COOKIE_SECURE = parseBoolean(
  process.env.SESSION_COOKIE_SECURE,
  ENABLE_HTTPS || (NGINX_ENABLED && PANEL_PUBLIC_URL_IS_HTTPS)
);

if (NGINX_ENABLED && PANEL_PUBLIC_URL_IS_HTTPS && !SESSION_COOKIE_SECURE) {
  console.warn("[config] PANEL_PUBLIC_URL is HTTPS behind nginx, but SESSION_COOKIE_SECURE is disabled.");
}

const REMEMBER_LOGIN_COOKIE_NAME = "adpanel.remember";
const REMEMBER_LOGIN_SECRET =
  process.env.REMEMBER_LOGIN_SECRET ||
  crypto.createHmac("sha256", SESSION_SECRET).update("adpanel-remember-login-key-v1").digest("hex");
const REMEMBER_LOGIN_MAX_AGE_MS = SESSION_MAX_AGE_MS;
const REMEMBER_LOGIN_REFRESH_WINDOW_MS = 30 * 24 * 60 * 60 * 1000;
const REMEMBER_LOGIN_VALIDATE_INTERVAL_MS = 6 * 60 * 60 * 1000;
const REMEMBER_LOGIN_REGISTRY_FILE = path.join(__dirname, ".remember-logins.json");
const REMEMBER_LOGIN_LEGACY_CUTOFF_FILE = path.join(__dirname, ".remember-login-legacy-cutoffs.json");

let rememberLoginRegistry = readJson(REMEMBER_LOGIN_REGISTRY_FILE, {});
if (!rememberLoginRegistry || typeof rememberLoginRegistry !== "object" || Array.isArray(rememberLoginRegistry)) {
  rememberLoginRegistry = {};
}

let rememberLoginLegacyCutoffs = readJson(REMEMBER_LOGIN_LEGACY_CUTOFF_FILE, {});
if (!rememberLoginLegacyCutoffs || typeof rememberLoginLegacyCutoffs !== "object" || Array.isArray(rememberLoginLegacyCutoffs)) {
  rememberLoginLegacyCutoffs = {};
}

function normalizeRememberLoginRegistry(raw) {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return {};

  const now = Date.now();
  const normalized = {};
  for (const [rememberId, value] of Object.entries(raw)) {
    const id = String(rememberId || "").trim();
    if (!id) continue;
    const entry = value && typeof value === "object" ? value : {};
    const expiresAt = Number(entry.expiresAt || 0);
    if (!Number.isFinite(expiresAt) || expiresAt <= now) continue;

    normalized[id] = {
      uid: String(entry.uid || "").trim(),
      sid: String(entry.sid || "").trim(),
      email: String(entry.email || "").trim().toLowerCase(),
      issuedAt: Number(entry.issuedAt || 0) || 0,
      expiresAt,
    };
  }

  return normalized;
}

rememberLoginRegistry = normalizeRememberLoginRegistry(rememberLoginRegistry);

function persistRememberLoginRegistry() {
  rememberLoginRegistry = normalizeRememberLoginRegistry(rememberLoginRegistry);
  safeWriteJson(REMEMBER_LOGIN_REGISTRY_FILE, rememberLoginRegistry);
}

function normalizeRememberLoginLegacyCutoffs(raw) {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return {};

  const normalized = {};
  for (const [userId, value] of Object.entries(raw)) {
    const id = String(userId || "").trim();
    const cutoff = Number(value || 0);
    if (!id || !Number.isFinite(cutoff) || cutoff <= 0) continue;
    normalized[id] = cutoff;
  }
  return normalized;
}

rememberLoginLegacyCutoffs = normalizeRememberLoginLegacyCutoffs(rememberLoginLegacyCutoffs);

function persistRememberLoginLegacyCutoffs() {
  rememberLoginLegacyCutoffs = normalizeRememberLoginLegacyCutoffs(rememberLoginLegacyCutoffs);
  safeWriteJson(REMEMBER_LOGIN_LEGACY_CUTOFF_FILE, rememberLoginLegacyCutoffs);
}

function getRememberLoginLegacyCutoff(userId) {
  const id = String(userId || "").trim();
  if (!id) return 0;
  return Number(rememberLoginLegacyCutoffs[id] || 0) || 0;
}

function setRememberLoginLegacyCutoff(userId) {
  const id = String(userId || "").trim();
  if (!id) return 0;
  const cutoff = Date.now();
  rememberLoginLegacyCutoffs[id] = cutoff;
  persistRememberLoginLegacyCutoffs();
  return cutoff;
}

function getRememberLoginRegistryEntry(rememberId) {
  const id = String(rememberId || "").trim();
  if (!id) return null;

  const entry = rememberLoginRegistry[id];
  if (!entry) return null;

  if (!Number.isFinite(Number(entry.expiresAt)) || Number(entry.expiresAt) <= Date.now()) {
    delete rememberLoginRegistry[id];
    persistRememberLoginRegistry();
    return null;
  }

  return entry;
}

function ensureRememberLoginSessionId(req) {
  if (!req?.session) return "";
  const current = String(req.session.rememberLoginId || "").trim();
  if (current) return current;

  const rememberId = crypto.randomBytes(18).toString("hex");
  req.session.rememberLoginId = rememberId;
  return rememberId;
}

function upsertRememberLoginRegistryEntry(req, user, expiresAt, issuedAt = Date.now()) {
  if (!req?.session || !user?.id) return "";

  const rememberId = ensureRememberLoginSessionId(req);
  if (!rememberId) return "";

  rememberLoginRegistry[rememberId] = {
    uid: String(user.id),
    sid: String(req.sessionID || ""),
    email: String(user.email || req.session.user || "").trim().toLowerCase(),
    issuedAt: Number(issuedAt) || Date.now(),
    expiresAt: Number(expiresAt) || (Date.now() + REMEMBER_LOGIN_MAX_AGE_MS),
  };
  persistRememberLoginRegistry();
  return rememberId;
}

function revokeRememberLoginRegistryEntry(rememberId) {
  const id = String(rememberId || "").trim();
  if (!id || !rememberLoginRegistry[id]) return false;
  delete rememberLoginRegistry[id];
  persistRememberLoginRegistry();
  return true;
}

function isManagedRememberLoginToken(parsedToken, user = null) {
  if (!parsedToken?.jti) return false;
  const entry = getRememberLoginRegistryEntry(parsedToken.jti);
  if (!entry) return false;
  if (user?.id && String(entry.uid || "") !== String(user.id)) return false;
  if (user?.email && String(entry.email || "").toLowerCase() !== String(user.email || "").toLowerCase()) return false;
  return true;
}

function getRememberLoginCookieOptions() {
  return {
    maxAge: REMEMBER_LOGIN_MAX_AGE_MS,
    sameSite: "lax",
    secure: SESSION_COOKIE_SECURE,
    httpOnly: true,
    path: "/",
  };
}

function clearRememberLoginCookie(res) {
  if (!res) return;
  res.clearCookie(REMEMBER_LOGIN_COOKIE_NAME, {
    httpOnly: true,
    sameSite: "lax",
    secure: SESSION_COOKIE_SECURE,
    path: "/",
  });
}

function clearRememberLoginSessionState(req) {
  if (!req?.session) return;
  delete req.session.rememberLoginCheckedAt;
  delete req.session.rememberLoginExpiresAt;
}

function markRememberLoginSessionFresh(req, expiresAt) {
  if (!req?.session) return;
  req.session.rememberLoginCheckedAt = Date.now();
  req.session.rememberLoginExpiresAt = Number(expiresAt) || 0;
}

function canSkipRememberLoginValidation(req) {
  if (!req?.session?.user) return false;

  const checkedAt = Number(req.session.rememberLoginCheckedAt || 0);
  const expiresAt = Number(req.session.rememberLoginExpiresAt || 0);
  if (!checkedAt || !expiresAt) return false;

  if (Date.now() - checkedAt >= REMEMBER_LOGIN_VALIDATE_INTERVAL_MS) return false;
  if (expiresAt - Date.now() <= REMEMBER_LOGIN_REFRESH_WINDOW_MS) return false;

  return true;
}

function getRememberLoginFingerprint(user) {
  if (!user || !user.password) return "";
  return crypto
    .createHash("sha256")
    .update(String(user.password || ""))
    .update("\n")
    .update(String(user.secret || ""))
    .digest("hex");
}

function signRememberLoginPayload(encodedPayload) {
  return crypto.createHmac("sha256", REMEMBER_LOGIN_SECRET).update(String(encodedPayload || "")).digest("base64url");
}

function readRememberLoginToken(token) {
  const raw = String(token || "").trim();
  if (!raw) return null;

  const parts = raw.split(".");
  if (parts.length !== 2) return null;

  const [encodedPayload, signature] = parts;
  if (!encodedPayload || !signature) return null;

  const expectedSignature = signRememberLoginPayload(encodedPayload);
  if (!safeCompare(signature, expectedSignature)) return null;

  try {
    const payload = JSON.parse(Buffer.from(encodedPayload, "base64url").toString("utf8"));
    if (!payload || ![1, 2].includes(Number(payload.v))) return null;

    const uid = String(payload.uid || "").trim();
    const iat = Number(payload.iat);
    const exp = Number(payload.exp);
    const fp = String(payload.fp || "").trim();
    const jti = String(payload.jti || "").trim();

    if (!uid || !Number.isFinite(iat) || !Number.isFinite(exp) || !fp) return null;
    if (iat > Date.now() + 5 * 60 * 1000) return null;
    if (exp <= Date.now()) return null;

    return { v: Number(payload.v), uid, iat, exp, fp, jti };
  } catch {
    return null;
  }
}

function issueRememberLoginToken(req, user, now = Date.now()) {
  if (!req?.session || !user || !user.id || !user.password) return "";

  const expiresAt = now + REMEMBER_LOGIN_MAX_AGE_MS;
  const rememberId = upsertRememberLoginRegistryEntry(req, user, expiresAt, now);
  if (!rememberId) return "";

  const payload = {
    v: 2,
    uid: String(user.id),
    iat: now,
    exp: expiresAt,
    fp: getRememberLoginFingerprint(user),
    jti: rememberId,
  };
  const encodedPayload = Buffer.from(JSON.stringify(payload), "utf8").toString("base64url");
  const signature = signRememberLoginPayload(encodedPayload);
  return `${encodedPayload}.${signature}`;
}

function setRememberLoginCookie(req, res, user) {
  if (!req?.session || !res) return;
  const now = Date.now();
  const token = issueRememberLoginToken(req, user, now);
  if (!token) return;
  res.cookie(REMEMBER_LOGIN_COOKIE_NAME, token, getRememberLoginCookieOptions());
  return now + REMEMBER_LOGIN_MAX_AGE_MS;
}

function rememberLoginNeedsRefresh(tokenPayload) {
  if (!tokenPayload) return true;
  return tokenPayload.exp - Date.now() <= REMEMBER_LOGIN_REFRESH_WINDOW_MS;
}

async function ensureRememberLoginCookie(req, res, user = null) {
  const parsed = readRememberLoginToken(req.cookies?.[REMEMBER_LOGIN_COOKIE_NAME]);
  const sessionRememberId = ensureRememberLoginSessionId(req);

  if (
    parsed &&
    parsed.jti &&
    parsed.jti === sessionRememberId &&
    isManagedRememberLoginToken(parsed) &&
    !rememberLoginNeedsRefresh(parsed) &&
    canSkipRememberLoginValidation(req)
  ) {
    markRememberLoginSessionFresh(req, parsed.exp);
    return;
  }

  const resolvedUser = user || (req.session?.user ? await findUserByEmail(req.session.user) : null);
  if (!resolvedUser || !resolvedUser.id || !resolvedUser.password) {
    clearRememberLoginSessionState(req);
    clearRememberLoginCookie(res);
    return;
  }

  const expiresAt = setRememberLoginCookie(req, res, resolvedUser);
  markRememberLoginSessionFresh(req, expiresAt);
}

async function restoreRememberedLogin(req, res) {
  const rawToken = String(req.cookies?.[REMEMBER_LOGIN_COOKIE_NAME] || "").trim();
  if (!rawToken) return false;

  const parsed = readRememberLoginToken(rawToken);
  if (!parsed) {
    clearRememberLoginSessionState(req);
    clearRememberLoginCookie(res);
    return false;
  }

  const user = await findUserById(parsed.uid);
  if (!user || !user.password) {
    clearRememberLoginSessionState(req);
    clearRememberLoginCookie(res);
    return false;
  }

  if (!safeCompare(parsed.fp, getRememberLoginFingerprint(user))) {
    clearRememberLoginSessionState(req);
    clearRememberLoginCookie(res);
    return false;
  }

  if (parsed.v === 1) {
    const legacyCutoff = getRememberLoginLegacyCutoff(user.id);
    if (legacyCutoff && Number(parsed.iat || 0) <= legacyCutoff) {
      clearRememberLoginSessionState(req);
      clearRememberLoginCookie(res);
      return false;
    }
  }

  if (parsed.jti && !isManagedRememberLoginToken(parsed, user)) {
    clearRememberLoginSessionState(req);
    clearRememberLoginCookie(res);
    return false;
  }

  await new Promise((resolve, reject) => req.session.regenerate((err) => (err ? reject(err) : resolve())));
  req.session.user = user.email;
  req.session.rememberLoginId = parsed.jti || crypto.randomBytes(18).toString("hex");
  req.currentUser = user;
  clearRememberLoginSessionState(req);
  await new Promise((resolve, reject) => req.session.save((err) => (err ? reject(err) : resolve())));
  const expiresAt = setRememberLoginCookie(req, res, user);
  markRememberLoginSessionFresh(req, expiresAt);
  return true;
}

const sessionMiddleware = session({
  store: sessionStore,
  secret: SESSION_SECRET,
  name: "adpanel.sid",
  resave: false,
  saveUninitialized: false,
  rolling: true,
  cookie: {
    maxAge: SESSION_MAX_AGE_MS,
    sameSite: "lax",
    secure: SESSION_COOKIE_SECURE,
    httpOnly: true,
  },
});

app.use(cookieParser());
app.use(sessionMiddleware);

app.use(async (req, res, next) => {
  try {
    if (req._staticAsset) return next();

    if (req.session?.user) {
      await ensureRememberLoginCookie(req, res);
      return next();
    }

    await restoreRememberedLogin(req, res);
    return next();
  } catch (err) {
    clearRememberLoginSessionState(req);
    clearRememberLoginCookie(res);
    console.warn("[auth] Persistent login restore failed:", err?.message || err);
    return next();
  }
});

// Middleware to ensure session TTL is refreshed on every authenticated request
app.use((req, res, next) => {
  if (req.session && req.session.user && req.session.touch) {
    // Force session touch to refresh Redis TTL
    req.session.touch();
  }
  next();
});

const BROWSER_SESSION_AUDIT_TOUCH_INTERVAL_MS = 60 * 1000;
const ACTIVE_BROWSER_SESSION_SCAN_TTL_MS = 15 * 1000;
const ACTIVE_BROWSER_SESSION_SCAN_CONCURRENCY = 25;
const SESSION_REVOKE_2FA_GRACE_MS = 10 * 60 * 1000;
const ACTIVE_BROWSER_SESSION_PAGE_SIZE = 25;

const activeBrowserSessionCache = {
  scannedAt: 0,
  sessions: new Map(),
  promise: null,
};

function normalizeBrowserSessionAudit(raw, fallback = {}) {
  const audit = raw && typeof raw === "object" ? raw : {};

  const createdAt = Number(audit.createdAt || fallback.createdAt || 0) || 0;
  const lastSeenAt = Number(audit.lastSeenAt || audit.updatedAt || fallback.lastSeenAt || 0) || 0;

  return {
    email: String(audit.email || fallback.email || "").trim().toLowerCase(),
    createdAt: createdAt || lastSeenAt || 0,
    lastSeenAt: lastSeenAt || createdAt || 0,
    userAgent: String(audit.userAgent || fallback.userAgent || "").trim().slice(0, 255),
    firstIp: String(audit.firstIp || fallback.firstIp || "").trim().slice(0, 64),
    lastIp: String(audit.lastIp || fallback.lastIp || "").trim().slice(0, 64),
    rememberLoginId: String(audit.rememberLoginId || fallback.rememberLoginId || "").trim(),
  };
}

function buildActiveBrowserSessionRecord(sessionId, sess) {
  const sid = String(sessionId || sess?.id || "").trim();
  if (!sid || !sess || typeof sess !== "object") return null;

  const email = String(sess.user || "").trim().toLowerCase();
  if (!email) return null;

  const cookieExpiresAt = sess?.cookie?.expires ? new Date(sess.cookie.expires).getTime() : 0;
  if (Number.isFinite(cookieExpiresAt) && cookieExpiresAt > 0 && cookieExpiresAt <= Date.now()) {
    return null;
  }

  const audit = normalizeBrowserSessionAudit(sess.browserSessionAudit, {
    email,
    lastSeenAt: Number(sess.__lastAccess || 0) || 0,
    rememberLoginId: String(sess.rememberLoginId || "").trim(),
  });

  return {
    sessionId: sid,
    email,
    createdAt: audit.createdAt,
    lastSeenAt: audit.lastSeenAt,
    expiresAt: Number.isFinite(cookieExpiresAt) && cookieExpiresAt > 0 ? cookieExpiresAt : 0,
    userAgent: audit.userAgent,
    ip: audit.lastIp || audit.firstIp || "",
    rememberLoginId: audit.rememberLoginId,
  };
}

function upsertActiveBrowserSessionCacheEntry(sessionId, sess) {
  const record = buildActiveBrowserSessionRecord(sessionId, sess);
  const sid = String(sessionId || sess?.id || "").trim();
  if (!sid) return;
  if (!record) {
    activeBrowserSessionCache.sessions.delete(sid);
    return;
  }
  activeBrowserSessionCache.sessions.set(record.sessionId, record);
}

function removeActiveBrowserSessionCacheEntry(sessionId) {
  const sid = String(sessionId || "").trim();
  if (!sid) return;
  activeBrowserSessionCache.sessions.delete(sid);
}

function invalidateActiveBrowserSessionCache() {
  activeBrowserSessionCache.scannedAt = 0;
}

function touchBrowserSessionAudit(req) {
  if (req?._staticAsset || !req?.session?.user) return;

  const now = Date.now();
  const currentIp = String(getRequestIp(req) || "").trim().slice(0, 64);
  const currentUserAgent = String(req.get("user-agent") || "").trim().slice(0, 255);
  const rememberLoginId = String(req.session.rememberLoginId || "").trim();

  const currentAudit = normalizeBrowserSessionAudit(req.session.browserSessionAudit, {
    email: String(req.session.user || "").trim().toLowerCase(),
    rememberLoginId,
  });

  let changed = false;
  if (!currentAudit.createdAt) {
    currentAudit.createdAt = now;
    changed = true;
  }
  if (!currentAudit.lastSeenAt || now - currentAudit.lastSeenAt >= BROWSER_SESSION_AUDIT_TOUCH_INTERVAL_MS) {
    currentAudit.lastSeenAt = now;
    changed = true;
  }
  if (String(currentAudit.email || "").toLowerCase() !== String(req.session.user || "").toLowerCase()) {
    currentAudit.email = String(req.session.user || "").trim().toLowerCase();
    changed = true;
  }
  if (currentUserAgent && currentAudit.userAgent !== currentUserAgent) {
    currentAudit.userAgent = currentUserAgent;
    changed = true;
  }
  if (currentIp && !currentAudit.firstIp) {
    currentAudit.firstIp = currentIp;
    changed = true;
  }
  if (currentIp && currentAudit.lastIp !== currentIp) {
    currentAudit.lastIp = currentIp;
    changed = true;
  }
  if (rememberLoginId && currentAudit.rememberLoginId !== rememberLoginId) {
    currentAudit.rememberLoginId = rememberLoginId;
    changed = true;
  }

  if (changed) {
    req.session.browserSessionAudit = currentAudit;
  }

  upsertActiveBrowserSessionCacheEntry(req.sessionID, {
    ...req.session,
    browserSessionAudit: currentAudit,
  });
}

app.use((req, _res, next) => {
  try {
    touchBrowserSessionAudit(req);
  } catch (err) {
    console.warn("[session] Failed to refresh browser session audit:", err?.message || err);
  }
  next();
});

function callSessionStore(methodName, ...args) {
  return new Promise((resolve, reject) => {
    if (!sessionStore || typeof sessionStore[methodName] !== "function") {
      return reject(new Error(`session store method "${methodName}" is not available`));
    }
    sessionStore[methodName](...args, (err, result) => {
      if (err) return reject(err);
      resolve(result);
    });
  });
}

async function mapWithConcurrency(items, limit, worker) {
  const allItems = Array.isArray(items) ? items : [];
  const concurrency = Math.max(1, Number(limit || 1));
  const results = new Array(allItems.length);
  let cursor = 0;

  async function consume() {
    while (cursor < allItems.length) {
      const index = cursor++;
      results[index] = await worker(allItems[index], index);
    }
  }

  await Promise.all(Array.from({ length: Math.min(concurrency, allItems.length || 1) }, () => consume()));
  return results;
}

function getFileStoreSessionIdFromFilename(fileName) {
  const raw = String(fileName || "").trim();
  if (!raw) return "";
  const extension = String(sessionStore?.options?.fileExtension || ".json");
  if (!extension) return raw;
  return raw.endsWith(extension) ? raw.slice(0, -extension.length) : raw;
}

async function scanActiveBrowserSessionsFromStore() {
  if (typeof sessionStore?.all === "function") {
    const sessions = await callSessionStore("all");
    return (Array.isArray(sessions) ? sessions : [])
      .map((sess) => buildActiveBrowserSessionRecord(sess?.id, sess))
      .filter(Boolean);
  }

  if (typeof sessionStore?.list === "function") {
    const files = await callSessionStore("list");
    const sessionIds = (Array.isArray(files) ? files : [])
      .map(getFileStoreSessionIdFromFilename)
      .filter(Boolean);

    const sessions = await mapWithConcurrency(sessionIds, ACTIVE_BROWSER_SESSION_SCAN_CONCURRENCY, async (sid) => {
      try {
        const sess = await callSessionStore("get", sid);
        return buildActiveBrowserSessionRecord(sid, sess);
      } catch {
        return null;
      }
    });

    return sessions.filter(Boolean);
  }

  throw new Error("session store does not support browser session inventory");
}

async function getCachedActiveBrowserSessions({ force = false } = {}) {
  const now = Date.now();
  if (!force && activeBrowserSessionCache.scannedAt && now - activeBrowserSessionCache.scannedAt < ACTIVE_BROWSER_SESSION_SCAN_TTL_MS) {
    return Array.from(activeBrowserSessionCache.sessions.values());
  }

  if (activeBrowserSessionCache.promise) {
    return activeBrowserSessionCache.promise;
  }

  activeBrowserSessionCache.promise = (async () => {
    const scannedSessions = await scanActiveBrowserSessionsFromStore();
    activeBrowserSessionCache.sessions = new Map(scannedSessions.map((record) => [record.sessionId, record]));
    activeBrowserSessionCache.scannedAt = Date.now();
    return scannedSessions;
  })()
    .catch((err) => {
      if (activeBrowserSessionCache.sessions.size > 0) {
        return Array.from(activeBrowserSessionCache.sessions.values());
      }
      throw err;
    })
    .finally(() => {
      activeBrowserSessionCache.promise = null;
    });

  return activeBrowserSessionCache.promise;
}

function getSessionRevokeGraceState(req) {
  if (!req?.session) return null;

  const state = req.session.sessionRevoke2fa;
  if (!state || typeof state !== "object") return null;

  const email = String(state.email || "").trim().toLowerCase();
  const verifiedAt = Number(state.verifiedAt || 0);
  if (!email || !Number.isFinite(verifiedAt) || verifiedAt <= 0) {
    delete req.session.sessionRevoke2fa;
    return null;
  }

  if (Date.now() - verifiedAt > SESSION_REVOKE_2FA_GRACE_MS) {
    delete req.session.sessionRevoke2fa;
    return null;
  }

  if (String(req.session.user || "").trim().toLowerCase() !== email) {
    delete req.session.sessionRevoke2fa;
    return null;
  }

  const stateIp = String(state.ip || "").trim();
  const currentIp = String(getRequestIp(req) || "").trim();
  if (stateIp && currentIp && stateIp !== currentIp) {
    delete req.session.sessionRevoke2fa;
    return null;
  }

  const stateUaHash = String(state.uaHash || "").trim();
  if (stateUaHash && stateUaHash !== getSessionRevokeGraceUaHash(req)) {
    delete req.session.sessionRevoke2fa;
    return null;
  }

  return {
    email,
    verifiedAt,
    expiresAt: verifiedAt + SESSION_REVOKE_2FA_GRACE_MS,
  };
}

function hasSessionRevokeGrace(req) {
  return !!getSessionRevokeGraceState(req);
}

function getSessionRevokeGraceUaHash(req) {
  return crypto
    .createHash("sha256")
    .update(String(req?.get?.("user-agent") || "").trim().slice(0, 255))
    .digest("hex");
}

function setSessionRevokeGrace(req) {
  if (!req?.session?.user) return 0;
  const verifiedAt = Date.now();
  req.session.sessionRevoke2fa = {
    email: String(req.session.user || "").trim().toLowerCase(),
    verifiedAt,
    ip: String(getRequestIp(req) || "").trim().slice(0, 64),
    uaHash: getSessionRevokeGraceUaHash(req),
  };
  return verifiedAt + SESSION_REVOKE_2FA_GRACE_MS;
}

const STEALTH_MODE = parseBoolean(process.env.STEALTH_MODE, true);
const STEALTH_ALLOW_HEALTHCHECK = parseBoolean(process.env.STEALTH_ALLOW_HEALTHCHECK, false);
const STEALTH_RESPONSE_FLOOR_MS = Math.max(0, parseInt(process.env.STEALTH_RESPONSE_FLOOR_MS || "90", 10) || 90);
const STEALTH_BOOTSTRAP_TTL_MS = Math.max(1000, parseInt(process.env.STEALTH_BOOTSTRAP_TTL_MS || "120000", 10) || 120000);
const STEALTH_COOKIE_TTL_DAYS = Math.max(1, parseInt(process.env.STEALTH_COOKIE_TTL_DAYS || "30", 10) || 30);
const STEALTH_CONFIG = ensureStealthConfig({
  enabled: STEALTH_MODE,
  cookieTtlDays: STEALTH_COOKIE_TTL_DAYS,
});
const STEALTH_ACTIVE = STEALTH_MODE && STEALTH_CONFIG.enabled !== false;
const STEALTH_COOKIE_NAME = String(STEALTH_CONFIG.cookieName || "adpanel_gate");
const STEALTH_LOGIN_PATHS = new Set(["/login", "/forgot-password", "/register"]);
const STEALTH_ASSET_PATHS = new Set([
  "/favicon.ico",
  "/login.css",
  "/images/adpanel-dark.webp",
  "/images/ADPanel-christmas.png",
  "/images/ADPanel-christmas.webp",
  "/images/bgvid.webm",
  "/branding-media/login-watermark",
  "/branding-media/login-background",
]);

function isStealthProtectedAssetPath(pathname) {
  const p = String(pathname || "").trim();
  return p.startsWith("/auth-assets/") || STEALTH_ASSET_PATHS.has(p);
}

function isStealthProtectedLoginPath(pathname) {
  return STEALTH_LOGIN_PATHS.has(String(pathname || "").trim());
}

function sanitizeStealthReturnTo(value) {
  const raw = String(value || "").trim();
  if (!raw.startsWith("/")) return "/login";
  if (raw.startsWith("//") || raw.startsWith("/_stealth/")) return "/login";
  return raw;
}

function getStealthGateCookieMaxAgeMs() {
  return (STEALTH_CONFIG.cookieTtlDays || STEALTH_COOKIE_TTL_DAYS || 30) * 24 * 60 * 60 * 1000;
}

function signStealthGatePayload(payload) {
  return crypto
    .createHmac("sha256", String(STEALTH_CONFIG.cookieSecret || ""))
    .update(String(payload || ""))
    .digest("base64url");
}

function createStealthGateToken() {
  const expiresAt = Date.now() + getStealthGateCookieMaxAgeMs();
  const nonce = crypto.randomBytes(18).toString("base64url");
  const payload = `${expiresAt}.${nonce}`;
  return `${payload}.${signStealthGatePayload(payload)}`;
}

function hasValidStealthGateToken(token) {
  const raw = String(token || "").trim();
  if (!raw) return false;
  const parts = raw.split(".");
  if (parts.length !== 3) return false;
  const [expiresAtRaw, nonce, signature] = parts;
  const expiresAt = parseInt(expiresAtRaw, 10);
  if (!Number.isFinite(expiresAt) || expiresAt <= Date.now()) return false;
  if (!nonce || !signature) return false;
  const payload = `${expiresAt}.${nonce}`;
  return safeCompare(signature, signStealthGatePayload(payload));
}

function getStealthCookieOptions() {
  return {
    httpOnly: true,
    sameSite: "lax",
    secure: SESSION_COOKIE_SECURE,
    path: "/",
    maxAge: getStealthGateCookieMaxAgeMs(),
  };
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function applyStealthResponseFloor(startedAt) {
  const elapsed = Date.now() - Number(startedAt || 0);
  const remaining = STEALTH_RESPONSE_FLOOR_MS - elapsed;
  if (remaining > 0) {
    await delay(remaining);
  }
}

function getStealthBootstrapState(req) {
  if (!req.session || !req.session.stealthBootstrap || typeof req.session.stealthBootstrap !== "object") {
    return null;
  }
  return req.session.stealthBootstrap;
}

function setStealthBootstrapState(req, returnTo) {
  if (!req.session) return null;
  const state = {
    id: crypto.randomBytes(18).toString("base64url"),
    returnTo: sanitizeStealthReturnTo(returnTo),
    issuedAt: Date.now(),
  };
  req.session.stealthBootstrap = state;
  return state;
}

function clearStealthBootstrapState(req) {
  if (!req.session) return;
  delete req.session.stealthBootstrap;
}

function buildStealthBootstrapHtml(res, authorizeUrl, stateId) {
  const nonce = res.locals.cspNonce || crypto.randomBytes(16).toString("base64");
  const variant = crypto
    .createHash("sha256")
    .update(`${STEALTH_CONFIG.htmlVariantSalt || "stealth"}:${stateId || ""}`)
    .digest("hex")
    .slice(0, 16);
  const rawAuthorizeUrl = String(authorizeUrl || "/login");
  const metaAuthorizeUrl = rawAuthorizeUrl
    .replace(/&/g, "&amp;")
    .replace(/"/g, "&quot;");
  const safeAuthorizeUrl = JSON.stringify(rawAuthorizeUrl);

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="referrer" content="no-referrer">
  <meta http-equiv="refresh" content="0;url=${metaAuthorizeUrl}">
  <title>Please wait</title>
  <style nonce="${nonce}">
    :root { color-scheme: light; }
    html, body { margin: 0; min-height: 100%; background: #f5f5f2; color: #161616; font: 400 14px/1.4 -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }
    body { display: grid; place-items: center; }
    .card { display: inline-flex; align-items: center; gap: 10px; padding: 14px 16px; border-radius: 999px; background: rgba(255,255,255,0.88); box-shadow: 0 12px 30px rgba(0,0,0,0.08); }
    .dot { width: 10px; height: 10px; border-radius: 999px; background: #111827; animation: pulse 1s ease-in-out infinite; }
    .msg { white-space: nowrap; letter-spacing: 0.02em; }
    @keyframes pulse { 0%, 100% { transform: scale(0.75); opacity: 0.45; } 50% { transform: scale(1); opacity: 1; } }
  </style>
  <script nonce="${nonce}">
    (function () {
      var target = ${safeAuthorizeUrl};
      window.location.replace(target);
    })();
  </script>
</head>
<body data-v="${variant}">
  <div class="card" aria-live="polite" aria-busy="true">
    <div class="dot" aria-hidden="true"></div>
    <div class="msg">Preparing secure access...</div>
  </div>
</body>
</html>`;
}

function applyStealthPageHeaders(res, nonce) {
  const currentNonce = nonce || res.locals.cspNonce || crypto.randomBytes(16).toString("base64");
  res.setHeader("Cache-Control", "no-store, private");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Vary", "Accept, Cookie");
  res.setHeader(
    "Content-Security-Policy",
    `default-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'; object-src 'none'; img-src 'self' data:; style-src 'nonce-${currentNonce}'; script-src 'nonce-${currentNonce}'; connect-src 'self';`
  );
  return currentNonce;
}

async function sendStealthBootstrap(req, res, returnTo) {
  const startedAt = Date.now();
  const state = setStealthBootstrapState(req, returnTo);
  const finalize = async () => {
    const nonce = applyStealthPageHeaders(res, res.locals.cspNonce);
    const target = sanitizeStealthReturnTo(returnTo);
    const authorizeUrl = `/_stealth/authorize?id=${encodeURIComponent(state?.id || "")}&r=${encodeURIComponent(target)}`;
    await applyStealthResponseFloor(startedAt);
    res.status(200).type("html").send(buildStealthBootstrapHtml({ ...res, locals: { ...res.locals, cspNonce: nonce } }, authorizeUrl, state?.id || ""));
  };

  if (req.session?.save) {
    return req.session.save((err) => {
      if (err) return res.status(500).type("text/plain").send("Service unavailable");
      finalize().catch(() => {
        if (!res.headersSent) res.status(500).type("text/plain").send("Service unavailable");
      });
    });
  }
  return finalize();
}

async function sendStealthNotFound(res) {
  const startedAt = Date.now();
  res.setHeader("Cache-Control", "no-store, private");
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("Content-Security-Policy", "default-src 'none'; base-uri 'none'; frame-ancestors 'none'; object-src 'none';");
  await applyStealthResponseFloor(startedAt);
  return res.status(404).type("text/plain").send("");
}

async function sendStealthEmptyFavicon(res) {
  const startedAt = Date.now();
  res.setHeader("Cache-Control", "no-store, private");
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("Content-Security-Policy", "default-src 'none'; base-uri 'none'; frame-ancestors 'none'; object-src 'none';");
  await applyStealthResponseFloor(startedAt);
  return res.status(204).end();
}

async function hasStealthAccess(req) {
  if (!STEALTH_ACTIVE) return true;
  if (hasValidStealthGateToken(req.cookies?.[STEALTH_COOKIE_NAME])) return true;
  try {
    return await isAuthenticated(req);
  } catch {
    return false;
  }
}

app.get("/_stealth/authorize", (req, res) => {
  const startedAt = Date.now();
  const returnTo = sanitizeStealthReturnTo(req.query?.r || "/login");
  if (!STEALTH_ACTIVE) {
    return res.redirect(303, returnTo);
  }

  const stateId = String(req.query?.id || "").trim();
  const currentState = getStealthBootstrapState(req);
  const validState = !!(
    currentState &&
    stateId &&
    safeCompare(stateId, String(currentState.id || "")) &&
    sanitizeStealthReturnTo(currentState.returnTo) === returnTo &&
    (Date.now() - Number(currentState.issuedAt || 0)) <= STEALTH_BOOTSTRAP_TTL_MS
  );

  clearStealthBootstrapState(req);

  const finalize = async () => {
    if (validState) {
      res.cookie(STEALTH_COOKIE_NAME, createStealthGateToken(), getStealthCookieOptions());
    } else {
      res.clearCookie(STEALTH_COOKIE_NAME, { path: "/" });
    }
    await applyStealthResponseFloor(startedAt);
    return res.redirect(303, validState ? returnTo : "/login");
  };

  if (req.session?.save) {
    return req.session.save((err) => {
      if (err) return res.status(500).type("text/plain").send("Service unavailable");
      finalize().catch(() => {
        if (!res.headersSent) res.status(500).type("text/plain").send("Service unavailable");
      });
    });
  }
  return finalize();
});

app.all("/_stealth/nginx-auth", async (req, res) => {
  if (!STEALTH_ACTIVE) return res.status(204).end();
  if (await hasStealthAccess(req)) return res.status(204).end();
  return res.status(401).end();
});

app.use(async (req, res, next) => {
  if (!STEALTH_ACTIVE) return next();
  if (STEALTH_ALLOW_HEALTHCHECK && req.path === "/healthz") return next();
  if (req.path.startsWith("/_stealth/")) return next();
  if (req.path.startsWith("/.well-known/acme-challenge/")) return next();

  const pathName = String(req.path || "").trim();
  const isProtectedLoginPath = isStealthProtectedLoginPath(pathName);
  const isProtectedAssetPath = isStealthProtectedAssetPath(pathName);
  if (!isProtectedLoginPath && !isProtectedAssetPath) return next();

  try {
    if (await hasStealthAccess(req)) return next();
  } catch (err) {
    return next(err);
  }

  if (pathName === "/favicon.ico") {
    return sendStealthEmptyFavicon(res).catch(next);
  }

  if (isProtectedAssetPath) {
    return sendStealthNotFound(res).catch(next);
  }

  if (req.method !== "GET" && req.method !== "HEAD") {
    res.setHeader("Cache-Control", "no-store, private");
    return res.redirect(303, "/login");
  }

  return sendStealthBootstrap(req, res, req.originalUrl || pathName).catch(next);
});

// ── Database Proxy Middleware (before body parsers to enable streaming) ──
app.use(dbProxy.createProxyMiddleware());

const JSON_BODY_LIMIT = process.env.JSON_BODY_LIMIT || "50mb";
const FILE_WRITE_JSON_LIMIT = process.env.FILE_WRITE_JSON_LIMIT || "50mb";
const AI_CHAT_JSON_LIMIT = process.env.AI_CHAT_JSON_LIMIT || "50mb";
const BRANDING_MEDIA_JSON_LIMIT = process.env.BRANDING_MEDIA_JSON_LIMIT || "80mb";
const URLENCODED_BODY_LIMIT = process.env.URLENCODED_BODY_LIMIT || "50mb";
const jsonParserDefault = express.json({ limit: JSON_BODY_LIMIT });
const jsonParserFileWrite = express.json({ limit: FILE_WRITE_JSON_LIMIT });
const jsonParserAiChat = express.json({ limit: AI_CHAT_JSON_LIMIT });
const jsonParserBrandingMedia = express.json({ limit: BRANDING_MEDIA_JSON_LIMIT });

app.use((req, res, next) => {
  if (req._staticAsset || req.method === "GET" || req.method === "HEAD" || req.method === "OPTIONS") return next();
  if (req.method === "PUT" && /^\/api\/servers\/[^/]+\/files\/write$/.test(req.path || "")) {
    return jsonParserFileWrite(req, res, next);
  }
  if (req.method === "POST" && /^\/api\/admin\/(branding\/update|login-background)$/.test(req.path || "")) {
    return jsonParserBrandingMedia(req, res, next);
  }
  if (req.method === "POST" && /^\/api\/ai\/(chat|chats\/\d+\/messages)/.test(req.path || "")) {
    return jsonParserAiChat(req, res, next);
  }
  return jsonParserDefault(req, res, next);
});
const _urlencodedParser = express.urlencoded({ extended: true, limit: URLENCODED_BODY_LIMIT });
app.use((req, res, next) => {
  if (req._staticAsset || req.method === "GET" || req.method === "HEAD" || req.method === "OPTIONS") return next();
  return _urlencodedParser(req, res, next);
});
app.use(hpp());
const SUSPICIOUS_PATH_PATTERNS = [/etc\/passwd/, /proc\/self/, /\.env/];
app.use((req, res, next) => {
  const pathToCheck = req.path || "";
  if (SUSPICIOUS_PATH_PATTERNS.some((pattern) => pattern.test(pathToCheck))) {
    securityLog("Potential Path Traversal Attempt", req);
    return res.status(403).send("Forbidden");
  }
  return next();
});
app.use((req, _res, next) => {
  if (req._staticAsset) return next();
  const seen = new Set();
  const maxDepth = 10;

  const scrub = (obj, depth = 0) => {
    if (!obj || typeof obj !== "object") return;
    if (seen.has(obj)) return;
    if (depth > maxDepth) return;
    if (Array.isArray(obj)) {
      seen.add(obj);
      for (const item of obj) scrub(item, depth + 1);
      return;
    }
    if (!isPlainObject(obj)) return;
    seen.add(obj);
    for (const key of Object.keys(obj)) {
      if (POLLUTION_KEYS.has(key)) {
        console.warn(`[SECURITY] Prototype pollution attempt from IP: ${getRequestIp(req)}`);
        delete obj[key];
        continue;
      }
      scrub(obj[key], depth + 1);
    }
  };

  scrub(req.body);
  scrub(req.query);
  scrub(req.params);
  return next();
});
if (NODE_COMPRESSION) {
  const COMPRESSION_THRESHOLD = 512;
  const BROTLI_QUALITY = Math.min(11, Math.max(0, parseInt(process.env.BROTLI_QUALITY || "4", 10)));
  const GZIP_LEVEL = 4;
  const COMPRESSIBLE_RE = /^text\/|^application\/(json|javascript|xml|xhtml\+xml|x-javascript|ld\+json|manifest\+json|vnd\.api\+json)|image\/svg\+xml/i;

  function isCompressible(contentType) {
    if (!contentType) return true;
    return COMPRESSIBLE_RE.test(contentType);
  }

  app.use((req, res, next) => {
    const acceptEncoding = req.headers["accept-encoding"] || "";
    const supportsBrotli = /\bbr\b/.test(acceptEncoding);
    const supportsGzip = /\bgzip\b/.test(acceptEncoding);

    if (!supportsBrotli && !supportsGzip) return next();

    const origWriteHead = res.writeHead;
    const origWrite = res.write;
    const origEnd = res.end;
    let compressStream = null;
    let decided = false;
    let bypassed = false;

    // Intercept writeHead to detect proxy/pipe scenarios
    res.writeHead = function(statusCode, ...args) {
      if (!decided) {
        decided = true;
        bypassed = true;
      }
      return origWriteHead.call(res, statusCode, ...args);
    };

    function decide() {
      if (decided) return;
      decided = true;

      const ct = String(res.getHeader("Content-Type") || "");
      if (ct === "text/event-stream" || !isCompressible(ct)) {
        bypassed = true;
        return;
      }

      res.removeHeader("Content-Length");
      res.setHeader("Vary", "Accept-Encoding");

      if (supportsBrotli) {
        compressStream = zlib.createBrotliCompress({
          params: {
            [zlib.constants.BROTLI_PARAM_QUALITY]: BROTLI_QUALITY,
            [zlib.constants.BROTLI_PARAM_MODE]: zlib.constants.BROTLI_MODE_TEXT,
          }
        });
        res.setHeader("Content-Encoding", "br");
      } else {
        compressStream = zlib.createGzip({ level: GZIP_LEVEL, threshold: COMPRESSION_THRESHOLD });
        res.setHeader("Content-Encoding", "gzip");
      }

      compressStream.on("data", (chunk) => origWrite.call(res, chunk));
      compressStream.on("end", () => origEnd.call(res));
      compressStream.on("error", () => {
        // On compression error, try to end gracefully
        try { origEnd.call(res); } catch {}
      });
    }

    res.write = function(chunk, encoding, cb) {
      if (!decided) decide();
      if (bypassed) return origWrite.call(res, chunk, encoding, cb);
      if (compressStream) return compressStream.write(chunk, encoding, cb);
      return origWrite.call(res, chunk, encoding, cb);
    };

    res.end = function(chunk, encoding, cb) {
      if (typeof chunk === "function") { cb = chunk; chunk = undefined; encoding = undefined; }
      if (typeof encoding === "function") { cb = encoding; encoding = undefined; }
      if (!decided) decide();
      if (bypassed) return origEnd.call(res, chunk, encoding, cb);
      if (compressStream) {
        if (cb) compressStream.once("end", cb);
        if (chunk) compressStream.end(chunk, encoding);
        else compressStream.end();
      } else {
        return origEnd.call(res, chunk, encoding, cb);
      }
    };

    next();
  });
}

app.set("views", path.join(__dirname, "views"));
app.set("view engine", "ejs");
app.set("view cache", false);

app.use((req, res, next) => {
  if (req._staticAsset) return next();
  return brandingMiddleware(req, res, next);
});
app.use((req, _res, next) => {
  if (req._staticAsset) return next();
  maybeScheduleLoginBackgroundMirror();
  next();
});

const BRANDING_REMOTE_ASSET_CACHE_TTL_MS = parseInt(process.env.BRANDING_REMOTE_ASSET_CACHE_TTL_MS || "300000", 10);
const BRANDING_REMOTE_ASSET_CACHE = new TTLCache({
  name: "brandingRemoteAssets",
  ttlMs: BRANDING_REMOTE_ASSET_CACHE_TTL_MS,
  maxSize: scaleForMemory(100),
  sweepMs: 60_000
});
const LOGIN_BACKGROUND_IMAGE_MAX_BYTES = 5 * 1024 * 1024;
const LOGIN_BACKGROUND_VIDEO_MAX_BYTES = 40 * 1024 * 1024;
const FIXED_FAVICON_REMOTE_URL = "https://cdn.jsdelivr.net/gh/antonndev/ADCDn/adpanel-dark.webp";
let loginBackgroundMirrorPromise = null;

function getBrandingRemoteAssetCacheKey(kind, sourceUrl) {
  return `${String(kind || "").trim().toLowerCase()}|${String(sourceUrl || "").trim()}`;
}

function getCachedBrandingRemoteAsset(cacheKey) {
  const cached = BRANDING_REMOTE_ASSET_CACHE.get(cacheKey);
  return cached || null;
}

function setCachedBrandingRemoteAsset(cacheKey, asset) {
  BRANDING_REMOTE_ASSET_CACHE.set(cacheKey, asset);
}

function clearBrandingRemoteAssetCache(kind = "") {
  const normalizedKind = String(kind || "").trim().toLowerCase();
  if (!normalizedKind) {
    BRANDING_REMOTE_ASSET_CACHE.clear();
    return;
  }
  BRANDING_REMOTE_ASSET_CACHE.clear((key) => key.startsWith(`${normalizedKind}|`));
}

function getBrandingRemoteAssetConfig(kind, branding) {
  const normalizedKind = String(kind || "").trim().toLowerCase();
  if (normalizedKind === "login-watermark") {
    const sourceUrl = String(branding?.loginWatermarkUrl || "").trim();
    const ext = getExtensionFromUrl(sourceUrl);
    if (!sourceUrl || !ext) return null;
    return {
      kind: normalizedKind,
      sourceUrl,
      ext,
      mediaType: "image",
      mimeType: getMimeTypeForExtension(ext) || "image/webp",
      maxBytes: 5 * 1024 * 1024,
    };
  }

  if (normalizedKind === "login-background") {
    const sourceUrl = String(branding?.loginBackgroundExternalUrl || "").trim();
    const ext = getExtensionFromUrl(sourceUrl);
    const mediaType = branding?.loginBackgroundType || getMediaTypeFromUrl(sourceUrl);
    if (!sourceUrl || !ext || !mediaType) return null;
    return {
      kind: normalizedKind,
      sourceUrl,
      ext,
      mediaType,
      mimeType: getMimeTypeForExtension(ext) || (mediaType === "image" ? "image/webp" : "video/webm"),
      maxBytes: mediaType === "image" ? 5 * 1024 * 1024 : 40 * 1024 * 1024,
    };
  }

  return null;
}

async function loadBrandingRemoteAsset(assetConfig) {
  const cacheKey = getBrandingRemoteAssetCacheKey(assetConfig.kind, assetConfig.sourceUrl);
  const cached = getCachedBrandingRemoteAsset(cacheKey);
  if (cached) {
    return cached;
  }

  const rawBuffer = await httpGetRaw(assetConfig.sourceUrl, { maxBytes: assetConfig.maxBytes });
  const base64 = rawBuffer.toString("base64");
  const validation = assetConfig.mediaType === "video"
    ? validateBase64Video(base64, assetConfig.ext)
    : validateBase64Image(base64);

  if (!validation?.valid) {
    throw new Error(validation?.error || "Invalid branding media");
  }

  const asset = {
    buffer: validation.data || rawBuffer,
    mimeType: assetConfig.mimeType,
  };
  setCachedBrandingRemoteAsset(cacheKey, asset);
  return asset;
}

app.get("/branding-media/:kind", async (req, res) => {
  try {
    const branding = loadBrandingConfig();
    const assetConfig = getBrandingRemoteAssetConfig(req.params.kind, branding);
    if (!assetConfig) {
      return res.status(404).end("not found");
    }

    const asset = await loadBrandingRemoteAsset(assetConfig);
    res.setHeader("Content-Type", asset.mimeType);
    res.setHeader("Cache-Control", "public, max-age=300");
    res.setHeader("X-Content-Type-Options", "nosniff");
    return res.send(asset.buffer);
  } catch (err) {
    console.error("[branding-media] Failed to serve remote asset:", err.message);
    return res.status(502).end("branding media unavailable");
  }
});

function resolvePublicImagePath(filename) {
  const safeName = path.basename(String(filename || "").trim());
  if (!safeName || safeName === "." || safeName === "..") return null;
  const filePath = path.join(__dirname, "public", "images", safeName);
  return fs.existsSync(filePath) ? filePath : null;
}

function sendPrivateAssetFile(res, filePath, fallbackType = "application/octet-stream") {
  const ext = path.extname(String(filePath || "")).slice(1).toLowerCase();
  const mimeType = getMimeTypeForExtension(ext) || fallbackType;
  res.setHeader("Cache-Control", "no-store, private");
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.type(mimeType);
  return res.sendFile(filePath);
}

async function sendPrivateRemoteAsset(res, assetConfig) {
  const asset = await loadBrandingRemoteAsset(assetConfig);
  res.setHeader("Cache-Control", "no-store, private");
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.type(asset.mimeType || "application/octet-stream");
  return res.send(asset.buffer);
}

function getDefaultLogoPath() {
  return resolvePublicImagePath("adpanel-dark.webp") || resolvePublicImagePath("logo.webp");
}

function getDefaultChristmasLogoPath() {
  return resolvePublicImagePath("ADPanel-christmas.webp") || resolvePublicImagePath("ADPanel-christmas.png");
}

function getDefaultFaviconPath() {
  return resolvePublicImagePath("favicon.ico") || getDefaultLogoPath();
}

app.get("/auth-assets/login.css", (req, res) => {
  res.setHeader("Cache-Control", "no-store, private");
  res.setHeader("X-Content-Type-Options", "nosniff");
  return res.type("text/css").send(getInlineLoginCss());
});

app.get("/auth-assets/logo", async (req, res) => {
  try {
    const branding = loadBrandingConfig();
    const localPath = resolvePublicImagePath(branding.localLogoPath) || getDefaultLogoPath();
    if (localPath) {
      return sendPrivateAssetFile(res, localPath, "image/webp");
    }

    const remoteLogoUrl = String(branding.logoUrl || "").trim();
    const ext = getExtensionFromUrl(remoteLogoUrl);
    if (remoteLogoUrl && ext) {
      return await sendPrivateRemoteAsset(res, {
        kind: "logo",
        sourceUrl: remoteLogoUrl,
        ext,
        mediaType: "image",
        mimeType: getMimeTypeForExtension(ext) || "image/webp",
        maxBytes: 5 * 1024 * 1024,
      });
    }

    return res.status(404).end("not found");
  } catch (err) {
    console.error("[auth-assets] Failed to serve logo:", err.message);
    return res.status(502).end("branding media unavailable");
  }
});

app.get("/auth-assets/christmas-logo", (req, res) => {
  const filePath = getDefaultChristmasLogoPath();
  if (!filePath) return res.status(404).end("not found");
  return sendPrivateAssetFile(res, filePath, "image/webp");
});

app.get("/auth-assets/watermark", async (req, res) => {
  try {
    const branding = loadBrandingConfig();
    const localPath = resolvePublicImagePath(branding.localLoginWatermarkPath);
    if (localPath) {
      return sendPrivateAssetFile(res, localPath, "image/webp");
    }

    const assetConfig = getBrandingRemoteAssetConfig("login-watermark", branding);
    if (!assetConfig) return res.status(404).end("not found");
    return await sendPrivateRemoteAsset(res, assetConfig);
  } catch (err) {
    console.error("[auth-assets] Failed to serve watermark:", err.message);
    return res.status(502).end("branding media unavailable");
  }
});

app.get("/auth-assets/background", async (req, res) => {
  try {
    const branding = loadBrandingConfig();
    const localPath = resolvePublicImagePath(branding.localLoginBackgroundPath);
    if (localPath) {
      return sendPrivateAssetFile(
        res,
        localPath,
        branding.loginBackgroundType === "image" ? "image/webp" : (branding.loginBackgroundMimeType || "video/webm")
      );
    }

    const assetConfig = getBrandingRemoteAssetConfig("login-background", branding);
    if (assetConfig) {
      return await sendPrivateRemoteAsset(res, assetConfig);
    }

    const fallbackPath = resolvePublicImagePath("bgvid.webm");
    if (fallbackPath) {
      return sendPrivateAssetFile(res, fallbackPath, "video/webm");
    }

    return res.status(404).end("not found");
  } catch (err) {
    console.error("[auth-assets] Failed to serve background:", err.message);
    return res.status(502).end("branding media unavailable");
  }
});

app.get("/auth-assets/favicon", async (req, res) => {
  try {
    const remoteFaviconExt = getExtensionFromUrl(FIXED_FAVICON_REMOTE_URL);
    if (remoteFaviconExt) {
      return await sendPrivateRemoteAsset(res, {
        kind: "favicon",
        sourceUrl: FIXED_FAVICON_REMOTE_URL,
        ext: remoteFaviconExt,
        mediaType: "image",
        mimeType: getMimeTypeForExtension(remoteFaviconExt) || "image/webp",
        maxBytes: 5 * 1024 * 1024,
      });
    }

    const localPath = resolvePublicImagePath("adpanel-dark.webp") || getDefaultLogoPath() || getDefaultFaviconPath();
    if (localPath) {
      return sendPrivateAssetFile(res, localPath, "image/webp");
    }

    return res.status(404).end("not found");
  } catch (err) {
    console.error("[auth-assets] Failed to serve favicon:", err.message);
    const localPath = resolvePublicImagePath("adpanel-dark.webp") || getDefaultLogoPath() || getDefaultFaviconPath();
    if (localPath) {
      return sendPrivateAssetFile(res, localPath, "image/webp");
    }
    return res.status(502).end("branding media unavailable");
  }
});

app.get("/favicon.ico", async (req, res) => {
  if (STEALTH_ACTIVE && !(await hasStealthAccess(req))) {
    return sendStealthEmptyFavicon(res);
  }
  return res.redirect(302, "/auth-assets/favicon");
});

if (SERVE_STATIC) {
  app.use(express.static(path.join(__dirname, "public"), {
    maxAge: "30d",
    etag: true,
    lastModified: true,
    immutable: true,
  }));
}


const STATUS_CACHE_TTL_MS = parseInt(process.env.STATUS_CACHE_TTL_MS || "10000", 10);
const NODE_POLL_INTERVAL_MS = parseInt(process.env.NODE_POLL_INTERVAL_MS || "15000", 10);
const NODE_POLL_TIMEOUT_MS = parseInt(process.env.NODE_POLL_TIMEOUT_MS || "5000", 10);
const NODE_POLL_CONCURRENCY = parseInt(process.env.NODE_POLL_CONCURRENCY || "10", 10);

const statusCache = {
  servers: new TTLCache({ name: "statusServers", ttlMs: 5 * 60 * 1000, sweepMs: 60_000 }),

  nodes: new TTLCache({ name: "statusNodes", ttlMs: 5 * 60 * 1000, sweepMs: 60_000 }),

  lastFullRefresh: 0,

  refreshing: false
};

const REDIS_KEYS = {
  serverStatus: (name) => `adpanel:status:server:${name}`,
  nodeStatus: (id) => `adpanel:status:node:${id}`,
  allServers: 'adpanel:status:servers:all',
  allNodes: 'adpanel:status:nodes:all'
};


async function getCachedServerStatus(serverName) {
  const name = String(serverName || '').trim();
  if (!name) return null;

  if (redisClient) {
    try {
      const cached = await redisClient.get(REDIS_KEYS.serverStatus(name));
      if (cached) {
        const parsed = normalizeServerStatusRecord(JSON.parse(cached));
        if (Date.now() - parsed.updatedAt < STATUS_CACHE_TTL_MS) {
          return parsed;
        }
      }
    } catch { }
  }

  const memCached = statusCache.servers.get(name);
  if (memCached && Date.now() - memCached.updatedAt < STATUS_CACHE_TTL_MS) {
    return normalizeServerStatusRecord(memCached);
  }

  return null;
}

const SSR_CACHE_LENIENT_MS = 60000;
async function getCachedServerStatusLenient(serverName) {
  const name = String(serverName || '').trim();
  if (!name) return null;

  if (redisClient) {
    try {
      const cached = await redisClient.get(REDIS_KEYS.serverStatus(name));
      if (cached) {
        const parsed = normalizeServerStatusRecord(JSON.parse(cached));
        if (Date.now() - parsed.updatedAt < SSR_CACHE_LENIENT_MS) {
          return parsed;
        }
      }
    } catch { }
  }

  const memCached = statusCache.servers.get(name);
  if (memCached && Date.now() - memCached.updatedAt < SSR_CACHE_LENIENT_MS) {
    return normalizeServerStatusRecord(memCached);
  }

  return null;
}

async function getCachedNodeStatus(nodeId) {
  const id = String(nodeId || '').trim();
  if (!id) return null;

  if (redisClient) {
    try {
      const cached = await redisClient.get(REDIS_KEYS.nodeStatus(id));
      if (cached) {
        const parsed = JSON.parse(cached);
        if (Date.now() - parsed.updatedAt < STATUS_CACHE_TTL_MS * 3) {
          return parsed;
        }
      }
    } catch { }
  }

  const memCached = statusCache.nodes.get(id);
  if (memCached && Date.now() - memCached.updatedAt < STATUS_CACHE_TTL_MS * 3) {
    return memCached;
  }

  return null;
}

async function getAllCachedServerStatuses(serverNames) {
  const results = new Map();
  const missing = [];

  for (const name of serverNames) {
    const cached = await getCachedServerStatus(name);
    if (cached) {
      results.set(name, cached);
    } else {
      missing.push(name);
    }
  }

  return { results, missing };
}

async function getAllCachedServerStatusesLenient(serverNames) {
  const results = new Map();
  const BATCH = 200;
  for (let i = 0; i < serverNames.length; i += BATCH) {
    const batch = serverNames.slice(i, i + BATCH);
    const entries = await Promise.all(batch.map(async (name) => {
      const cached = await getCachedServerStatusLenient(name);
      return cached ? [name, cached] : null;
    }));
    for (const entry of entries) {
      if (entry) results.set(entry[0], entry[1]);
    }
  }
  return { results };
}


let broadcastDashboardUpdate = null;
let broadcastNodeUpdate = null;

async function setCachedServerStatus(serverName, status) {
  const name = String(serverName || '').trim();
  if (!name) return;

  // Keep metrics human-scale; long floats add no UI value and can trigger false-positive PII scanners.
  const data = normalizeServerStatusRecord({
    name,
    status: status.status || 'unknown',
    cpu: status.cpu ?? null,
    cpuLimit: status.cpuLimit ?? null,
    memory: status.memory ?? null,
    disk: status.disk ?? null,
    uptime: status.uptime ?? null,
    nodeOnline: status.nodeOnline !== false,
    nodeId: status.nodeId || null,
    updatedAt: Date.now()
  });

  const previous = statusCache.servers.get(name);
  const statusChanged = !previous || previous.status !== data.status ||
    previous.nodeOnline !== data.nodeOnline;

  statusCache.servers.set(name, data);

  if (redisClient) {
    try {
      const ttlSeconds = Math.max(1, Math.floor(STATUS_CACHE_TTL_MS / 1000) * 2);
      await redisClient.setEx(
        REDIS_KEYS.serverStatus(name),
        ttlSeconds,
        JSON.stringify(data)
      );
    } catch (err) {
    }
  }

  if (statusChanged && broadcastDashboardUpdate) {
    broadcastDashboardUpdate(name, data);
  }
}

async function setCachedNodeStatus(nodeId, status) {
  const id = String(nodeId || '').trim();
  if (!id) return;

  const data = {
    id,
    online: status.online !== false,
    latency: status.latency ?? null,
    lastSeen: status.online ? Date.now() : (status.lastSeen || null),
    serverCount: status.serverCount ?? 0,
    resources: status.resources || null,
    updatedAt: Date.now()
  };

  const previous = statusCache.nodes.get(id);
  const statusChanged = !previous || previous.online !== data.online;

  statusCache.nodes.set(id, data);

  if (redisClient) {
    try {
      const ttlSeconds = Math.max(1, Math.floor(STATUS_CACHE_TTL_MS / 1000) * 6);
      await redisClient.setEx(
        REDIS_KEYS.nodeStatus(id),
        ttlSeconds,
        JSON.stringify(data)
      );
    } catch (err) {
    }
  }

  if (statusChanged && broadcastNodeUpdate) {
    broadcastNodeUpdate(id, data);
  }

  if (statusChanged) {
    db.query(
      'UPDATE nodes SET online = ?, port_ok = ?, last_seen = ?, last_check = ? WHERE id = ? OR uuid = ?',
      [data.online ? 1 : 0, data.online ? 1 : 0, data.online ? Date.now() : null, Date.now(), id, id]
    ).catch(() => { });
  }
}


async function fetchNodeServerStatuses(node) {
  const nodeId = node.uuid || node.id || node.name;
  const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);

  if (!baseUrl) {
    await setCachedNodeStatus(nodeId, { online: false });
    return { nodeId, online: false, servers: [] };
  }

  const headers = nodeAuthHeadersFor(node, true);
  const startTime = Date.now();

  try {
    const { status, json } = await httpRequestJson(
      `${baseUrl}/v1/servers`,
      'GET',
      headers,
      null,
      NODE_POLL_TIMEOUT_MS
    );

    const latency = Date.now() - startTime;

    if (status === 0) {
      await setCachedNodeStatus(nodeId, { online: false, latency: null });
      return { nodeId, online: false, servers: [], latency: null };
    }

    if (status !== 200 || !json) {
      await setCachedNodeStatus(nodeId, { online: true, latency });
      return { nodeId, online: true, servers: [], latency };
    }

    const servers = Array.isArray(json.servers) ? json.servers :
      Array.isArray(json) ? json : [];

    await setCachedNodeStatus(nodeId, {
      online: true,
      latency,
      serverCount: servers.length,
      resources: json.resources || null
    });

    for (const server of servers) {
      if (!server.name) continue;
      await setCachedServerStatus(server.name, {
        status: normalizeStatusLabel(server.status || server.state) || 'unknown',
        cpu: server.cpu ?? server.cpuPercent ?? null,
        cpuLimit: server.cpuLimit ?? null,
        memory: server.memory ?? server.memoryMb ?? null,
        disk: server.disk ?? server.diskMb ?? null,
        uptime: server.uptime ?? null,
        nodeOnline: true,
        nodeId
      });
    }

    return { nodeId, online: true, servers, latency };

  } catch (err) {
    await setCachedNodeStatus(nodeId, { online: false });
    return { nodeId, online: false, servers: [], error: err.message };
  }
}

async function pollAllNodes() {
  if (statusCache.refreshing) {
    return;
  }

  statusCache.refreshing = true;

  try {
    const nodes = await loadNodes();

    if (!nodes.length) {
      statusCache.lastFullRefresh = Date.now();
      return;
    }

    const results = [];
    for (let i = 0; i < nodes.length; i += NODE_POLL_CONCURRENCY) {
      const batch = nodes.slice(i, i + NODE_POLL_CONCURRENCY);
      const batchResults = await Promise.all(
        batch.map(node => fetchNodeServerStatuses(node).catch(err => ({
          nodeId: node.uuid || node.id,
          online: false,
          error: err.message
        })))
      );
      results.push(...batchResults);
    }

    const seenServers = new Set();
    for (const result of results) {
      if (result.servers) {
        for (const s of result.servers) {
          if (s.name) seenServers.add(s.name);
        }
      }
    }

    const serverIndex = await loadServersIndex();
    const nodeToServers = new Map();
    for (const entry of serverIndex) {
      if (!entry?.name || seenServers.has(entry.name) || !entry.nodeId) continue;
      const list = nodeToServers.get(entry.nodeId) || [];
      list.push(entry.name);
      nodeToServers.set(entry.nodeId, list);
    }
    const nodeIds = [...nodeToServers.keys()];
    const BATCH = 50;
    for (let i = 0; i < nodeIds.length; i += BATCH) {
      const batch = nodeIds.slice(i, i + BATCH);
      const nodeStatuses = await Promise.all(batch.map(nid => getCachedNodeStatus(nid).then(s => [nid, s])));
      const offlineUpdates = [];
      for (const [nid, nodeStatus] of nodeStatuses) {
        if (nodeStatus && !nodeStatus.online) {
          for (const serverName of nodeToServers.get(nid)) {
            offlineUpdates.push(setCachedServerStatus(serverName, {
              status: 'unknown',
              nodeOnline: false,
              nodeId: nid
            }));
          }
        }
      }
      if (offlineUpdates.length) await Promise.all(offlineUpdates);
    }

    statusCache.lastFullRefresh = Date.now();

    const onlineNodes = results.filter(r => r.online).length;
    const totalServers = results.reduce((sum, r) => sum + (r.servers?.length || 0), 0);
    console.log(`[StatusCache] Refreshed: ${onlineNodes}/${nodes.length} nodes online, ${totalServers} servers`);

  } catch (err) {
    console.error('[StatusCache] Poll failed:', err);
  } finally {
    statusCache.refreshing = false;
  }
}


let _WebSocket; function getLazyWebSocket() { if (!_WebSocket) _WebSocket = require('ws'); return _WebSocket; }

const WS_RECONNECT_BASE_MS = 2000;
const WS_RECONNECT_MAX_MS = 60000;
const WS_FALLBACK_POLL_MS = 60000;

const nodeWsConnections = new Map();

function getNodeWsUrl(node) {
  const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
  if (!baseUrl) return null;
  return baseUrl.replace(/^http/, 'ws') + '/v1/ws';
}

function connectNodeWebSocket(node) {
  const nodeId = node.uuid || node.id || node.name;
  const wsUrl = getNodeWsUrl(node);
  if (!wsUrl) {
    startNodeFallbackPolling(node);
    return;
  }

  const existing = nodeWsConnections.get(nodeId);
  if (existing && existing.ws && existing.ws.readyState === getLazyWebSocket().OPEN) return;

  const token = node.token || node.secret || node.api_key || '';
  const state = existing || {
    ws: null,
    reconnectTimer: null,
    reconnectDelay: WS_RECONNECT_BASE_MS,
    fallbackInterval: null,
    node
  };
  nodeWsConnections.set(nodeId, state);

  try {
    const ws = new (getLazyWebSocket())(wsUrl, {
      headers: {
        'Authorization': `Bearer ${token}`,
        'X-Node-Token': token
      },
      handshakeTimeout: 5000,
      maxPayload: 4 * 1024 * 1024
    });

    state.ws = ws;

    ws.on('open', () => {
      console.log(`[NodeWS] Connected to ${node.name || nodeId}`);
      state.reconnectDelay = WS_RECONNECT_BASE_MS;
      stopNodeFallbackPolling(nodeId);
    });

    ws.on('message', async (data) => {
      try {
        const msg = JSON.parse(data.toString());

        if (msg.type === 'status' && msg.ok) {
          const latency = null;
          const servers = Array.isArray(msg.servers) ? msg.servers : [];

          await setCachedNodeStatus(nodeId, {
            online: true,
            latency,
            serverCount: servers.length,
            resources: msg.resources || null
          });

          for (const server of servers) {
            if (!server.name) continue;
            await setCachedServerStatus(server.name, {
              status: normalizeStatusLabel(server.status || server.state) || 'unknown',
              cpu: server.cpu ?? server.cpuPercent ?? null,
              cpuLimit: server.cpuLimit ?? null,
              memory: server.memory ?? server.memoryMb ?? null,
              disk: server.disk ?? server.diskMb ?? null,
              uptime: server.uptime ?? null,
              nodeOnline: true,
              nodeId
            });
          }
        } else if (msg.type === 'event') {
          if (msg.server) {
            await setCachedServerStatus(msg.server, {
              status: normalizeStatusLabel(msg.status) || 'unknown',
              nodeOnline: true,
              nodeId
            });
          }
        }
      } catch (err) {
      }
    });

    ws.on('close', (code) => {
      if (isShuttingDown) return;
      console.log(`[NodeWS] Disconnected from ${node.name || nodeId} (code: ${code})`);
      scheduleReconnect(node);
    });

    ws.on('error', (err) => {
      if (isShuttingDown) return;
    });
  } catch (err) {
    console.error(`[NodeWS] Failed to connect to ${node.name || nodeId}:`, err.message);
    scheduleReconnect(node);
  }
}

function scheduleReconnect(node) {
  const nodeId = node.uuid || node.id || node.name;
  const state = nodeWsConnections.get(nodeId);
  if (!state || isShuttingDown) return;

  setCachedNodeStatus(nodeId, { online: false }).catch(() => { });

  startNodeFallbackPolling(node);

  if (state.reconnectTimer) clearTimeout(state.reconnectTimer);
  const jitter = Math.random() * 1000;
  const delay = Math.min(state.reconnectDelay + jitter, WS_RECONNECT_MAX_MS);

  state.reconnectTimer = setTimeout(async () => {
    const freshNode = await findNodeByIdOrName(nodeId).catch(() => null);
    if (freshNode) {
      connectNodeWebSocket(freshNode);
    } else {
      cleanupNodeWs(nodeId);
    }
  }, delay);

  state.reconnectDelay = Math.min(state.reconnectDelay * 2, WS_RECONNECT_MAX_MS);
}

function startNodeFallbackPolling(node) {
  const nodeId = node.uuid || node.id || node.name;
  const state = nodeWsConnections.get(nodeId);
  if (!state) return;
  if (state.fallbackInterval) return;

  state.fallbackInterval = setInterval(async () => {
    try {
      await fetchNodeServerStatuses(node);
    } catch { }
  }, WS_FALLBACK_POLL_MS);
  fetchNodeServerStatuses(node).catch(() => { });
}

function stopNodeFallbackPolling(nodeId) {
  const state = nodeWsConnections.get(nodeId);
  if (!state || !state.fallbackInterval) return;
  clearInterval(state.fallbackInterval);
  state.fallbackInterval = null;
}

function cleanupNodeWs(nodeId) {
  const state = nodeWsConnections.get(nodeId);
  if (!state) return;
  if (state.ws) {
    try { state.ws.close(1000); } catch { }
  }
  if (state.reconnectTimer) clearTimeout(state.reconnectTimer);
  if (state.fallbackInterval) clearInterval(state.fallbackInterval);
  nodeWsConnections.delete(nodeId);
}

function cleanupAllNodeWs() {
  for (const [nodeId] of nodeWsConnections) {
    cleanupNodeWs(nodeId);
  }
}


let pollInterval = null;

async function initNodeConnections() {
  const nodes = await loadNodes();
  if (!nodes.length) return;

  await pollAllNodes().catch(console.error);

  for (const node of nodes) {
    connectNodeWebSocket(node);
  }

  console.log(`[NodeWS] Initialized WebSocket connections to ${nodes.length} node(s)`);
}

function startStatusPolling() {
  if (pollInterval) return;

  setTimeout(() => {
    initNodeConnections().catch(console.error);
  }, 500);

  pollInterval = setInterval(async () => {
    try {
      const nodes = await loadNodes();
      const currentIds = new Set(nodes.map(n => n.uuid || n.id || n.name));

      for (const node of nodes) {
        const nodeId = node.uuid || node.id || node.name;
        const existing = nodeWsConnections.get(nodeId);
        if (!existing || !existing.ws || existing.ws.readyState !== getLazyWebSocket().OPEN) {
          connectNodeWebSocket(node);
        }
      }

      for (const [nodeId] of nodeWsConnections) {
        if (!currentIds.has(nodeId)) {
          console.log(`[NodeWS] Node ${nodeId} removed, cleaning up`);
          cleanupNodeWs(nodeId);
        }
      }
    } catch (err) {
      console.error('[NodeWS] Node sync error:', err.message);
    }
  }, 60_000);

  console.log(`[NodeWS] WebSocket-first status system started`);
}

function stopStatusPolling() {
  if (pollInterval) {
    clearInterval(pollInterval);
    pollInterval = null;
  }
  cleanupAllNodeWs();
}


let isShuttingDown = false;
async function gracefulShutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  console.log(`\n[ADPanel] Received ${signal}, shutting down gracefully...`);

  stopStatusPolling();

  if (typeof stopStatusCleanup === 'function') {
    try { stopStatusCleanup(); } catch { }
  }

  if (typeof shutdownScheduler === 'function') {
    try { await shutdownScheduler(); } catch { }
  }

  if (httpServer) {
    httpServer.close(() => console.log('[ADPanel] HTTP server closed'));
  }

  if (httpsServer) {
    httpsServer.close(() => console.log('[ADPanel] HTTPS server closed'));
  }

  if (io) {
    io.close(() => console.log('[ADPanel] Socket.IO closed'));
  }

  try {
    await db.close();
    console.log('[ADPanel] Database pool closed');
  } catch (err) {
    console.error('[ADPanel] Error closing database:', err?.message || err);
  }

  if (redisClient) {
    try {
      await redisClient.quit();
      console.log('[ADPanel] Redis connection closed');
    } catch (err) {
      console.error('[ADPanel] Error closing Redis:', err?.message || err);
    }
  }

  console.log('[ADPanel] Shutdown complete');
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

const STATUS_CLEANUP_INTERVAL_MS = parseInt(process.env.STATUS_CLEANUP_INTERVAL_MS || "", 10) || 60 * 1000;
let statusCleanupInterval = null;

async function cleanupExpiredStatuses() {
  try {
    const now = Date.now();
    await db.query("DELETE FROM user_status WHERE expires_at IS NOT NULL AND expires_at <= ?", [now]);
  } catch (err) {
  }
}

function startStatusCleanup() {
  if (statusCleanupInterval) return;
  statusCleanupInterval = setInterval(cleanupExpiredStatuses, STATUS_CLEANUP_INTERVAL_MS);
  statusCleanupInterval.unref();
}

function stopStatusCleanup() {
  if (statusCleanupInterval) {
    clearInterval(statusCleanupInterval);
    statusCleanupInterval = null;
  }
}

startStatusCleanup();

const SCHEDULER_ENABLED = parseBoolean(process.env.SCHEDULER_ENABLED, true);
let schedulerQueue = null;
let schedulerWorker = null;
let schedulerQueueEvents = null;

function parseRedisUrlForBullMQ(redisUrl) {
  if (!redisUrl) {
    return { host: "127.0.0.1", port: 6379 };
  }
  try {
    const url = new URL(redisUrl);
    const opts = {
      host: url.hostname || "127.0.0.1",
      port: parseInt(url.port, 10) || 6379
    };
    if (url.password) opts.password = decodeURIComponent(url.password);
    if (url.username && url.username !== "default") opts.username = decodeURIComponent(url.username);
    if (url.pathname && url.pathname.length > 1) {
      const dbNum = parseInt(url.pathname.slice(1), 10);
      if (!isNaN(dbNum)) opts.db = dbNum;
    }
    return opts;
  } catch {
    return { host: "127.0.0.1", port: 6379 };
  }
}

const SCHEDULER_REDIS_URL = REDIS_URL || process.env.SCHEDULER_REDIS_URL || "";

async function initializeScheduler() {
  if (!SCHEDULER_ENABLED || !SCHEDULER_REDIS_URL) {
    if (!SCHEDULER_REDIS_URL) {
      console.log("[scheduler] Scheduler disabled - REDIS_URL not configured");
    }
    return;
  }

  try {
    const { Queue, Worker, QueueEvents } = require("bullmq");

    const bullmqConnectionOpts = parseRedisUrlForBullMQ(SCHEDULER_REDIS_URL);
    const bullmqConnection = { connection: bullmqConnectionOpts };

    schedulerQueue = new Queue("adpanel-scheduler", bullmqConnection);

    schedulerQueueEvents = new QueueEvents("adpanel-scheduler", bullmqConnection);

    schedulerWorker = new Worker("adpanel-scheduler", async (job) => {
      const { actionType, serverName, payload, userId, userName } = job.data;
      console.log(`[scheduler] Processing job ${job.id}: ${actionType} for server ${serverName}`);

      try {
        switch (actionType) {
          case "console_command": {
            const command = payload?.command;
            if (!command) throw new Error("No command specified");

            const nodesModule = require("./nodes.js");
            const ctx = await nodesModule.remoteContext(serverName);
            if (!ctx.exists) throw new Error("Server not found");
            if (!ctx.remote || !ctx.node) throw new Error("Server is not remote");

            const result = await nodesModule.httpJson(
              nodesModule.nodeUrl(ctx.node, `/v1/servers/${encodeURIComponent(serverName)}/command`),
              {
                method: "POST",
                headers: Object.assign({ "Content-Type": "application/json" }, nodesModule.nodeHeaders(ctx.node)),
                body: { command },
                timeoutMs: 15000
              }
            );

            if (result.status !== 200 || !result.json?.ok) {
              throw new Error(result.json?.error || `Command failed: ${result.status}`);
            }

            console.log(`[scheduler] Console command executed: ${command}`);
            return { success: true, action: "console_command", command };
          }

          case "server_start": {
            const nodesModule = require("./nodes.js");
            const ctx = await nodesModule.remoteContext(serverName);
            if (!ctx.exists) throw new Error("Server not found");
            if (!ctx.remote || !ctx.node) throw new Error("Server is not remote");

            const result = await nodesModule.httpJson(
              nodesModule.nodeUrl(ctx.node, `/v1/servers/${encodeURIComponent(serverName)}/start`),
              {
                method: "POST",
                headers: Object.assign({ "Content-Type": "application/json" }, nodesModule.nodeHeaders(ctx.node)),
                body: {},
                timeoutMs: 60000
              }
            );

            if (result.status !== 200 || !result.json?.ok) {
              throw new Error(result.json?.error || `Start failed: ${result.status}`);
            }

            console.log(`[scheduler] Server started: ${serverName}`);
            return { success: true, action: "server_start", server: serverName };
          }

          case "server_stop": {
            const nodesModule = require("./nodes.js");
            const ctx = await nodesModule.remoteContext(serverName);
            if (!ctx.exists) throw new Error("Server not found");
            if (!ctx.remote || !ctx.node) throw new Error("Server is not remote");

            const result = await nodesModule.httpJson(
              nodesModule.nodeUrl(ctx.node, `/v1/servers/${encodeURIComponent(serverName)}/stop`),
              {
                method: "POST",
                headers: Object.assign({ "Content-Type": "application/json" }, nodesModule.nodeHeaders(ctx.node)),
                timeoutMs: 30000
              }
            );

            if (result.status !== 200 || !result.json?.ok) {
              throw new Error(result.json?.error || `Stop failed: ${result.status}`);
            }

            console.log(`[scheduler] Server stopped: ${serverName}`);
            return { success: true, action: "server_stop", server: serverName };
          }

          case "create_file": {
            const { filePath: targetPath, content } = payload || {};
            if (!targetPath) throw new Error("No file path specified");

            const nodesModule = require("./nodes.js");
            const ctx = await nodesModule.remoteContext(serverName);
            if (!ctx.exists) throw new Error("Server not found");
            if (!ctx.remote || !ctx.node) throw new Error("Server is not remote");

            const result = await nodesModule.httpJson(
              nodesModule.nodeUrl(ctx.node, `/v1/servers/${encodeURIComponent(serverName)}/files/${encodeURIComponent(targetPath)}`),
              {
                method: "PUT",
                headers: Object.assign({ "Content-Type": "application/json" }, nodesModule.nodeHeaders(ctx.node)),
                body: { content: content || "" },
                timeoutMs: 30000
              }
            );

            if (result.status !== 200 || !result.json?.ok) {
              throw new Error(result.json?.error || `File create failed: ${result.status}`);
            }

            console.log(`[scheduler] File created: ${targetPath}`);
            return { success: true, action: "create_file", path: targetPath };
          }

          case "modify_file": {
            const { filePath: targetPath, content } = payload || {};
            if (!targetPath) throw new Error("No file path specified");

            const nodesModule = require("./nodes.js");
            const ctx = await nodesModule.remoteContext(serverName);
            if (!ctx.exists) throw new Error("Server not found");
            if (!ctx.remote || !ctx.node) throw new Error("Server is not remote");

            const result = await nodesModule.httpJson(
              nodesModule.nodeUrl(ctx.node, `/v1/servers/${encodeURIComponent(serverName)}/files/${encodeURIComponent(targetPath)}`),
              {
                method: "PUT",
                headers: Object.assign({ "Content-Type": "application/json" }, nodesModule.nodeHeaders(ctx.node)),
                body: { content: content || "" },
                timeoutMs: 30000
              }
            );

            if (result.status !== 200 || !result.json?.ok) {
              throw new Error(result.json?.error || `File modify failed: ${result.status}`);
            }

            console.log(`[scheduler] File modified: ${targetPath}`);
            return { success: true, action: "modify_file", path: targetPath };
          }

          case "backup": {
            const { backupName, description } = payload || {};

            const nodesModule = require("./nodes.js");
            const ctx = await nodesModule.remoteContext(serverName);
            if (!ctx.exists) throw new Error("Server not found");
            if (!ctx.remote || !ctx.node) throw new Error("Server is not remote");

            const result = await nodesModule.httpJson(
              nodesModule.nodeUrl(ctx.node, `/v1/servers/${encodeURIComponent(serverName)}/backups`),
              {
                method: "POST",
                headers: Object.assign({ "Content-Type": "application/json" }, nodesModule.nodeHeaders(ctx.node)),
                body: { name: backupName || `Scheduled Backup ${new Date().toISOString()}`, description: description || "Created by scheduler" },
                timeoutMs: 120000
              }
            );

            if (result.status !== 200 || !result.json?.ok) {
              throw new Error(result.json?.error || `Backup failed: ${result.status}`);
            }

            console.log(`[scheduler] Backup created: ${backupName || "auto"}`);
            return { success: true, action: "backup", backupId: result.json?.backup?.id };
          }

          default:
            throw new Error(`Unknown action type: ${actionType}`);
        }
      } catch (err) {
        console.error(`[scheduler] Job ${job.id} failed:`, err?.message || err);
        throw err;
      }
    }, {
      connection: bullmqConnectionOpts,
      concurrency: 5,
      removeOnComplete: { count: 100 },
      removeOnFail: { count: 50 }
    });

    schedulerWorker.on("completed", (job, result) => {
      console.log(`[scheduler] Job ${job.id} completed:`, result);
    });

    schedulerWorker.on("failed", (job, err) => {
      console.error(`[scheduler] Job ${job?.id} failed:`, err?.message || err);
    });

    console.log("[scheduler] BullMQ scheduler initialized successfully");
  } catch (err) {
    console.warn("[scheduler] Failed to initialize BullMQ scheduler:", err?.message || err);
    schedulerQueue = null;
    schedulerWorker = null;
  }
}

function scheduleToCron(scheduleType, scheduleValue, scheduleTime) {
  if (scheduleType === "seconds") {
    const seconds = parseInt(scheduleValue, 10) || 30;
    return { type: "interval", ms: seconds * 1000 };
  }
  if (scheduleType === "minutes") {
    const minutes = parseInt(scheduleValue, 10) || 5;
    return `*/${minutes} * * * *`;
  }
  if (scheduleType === "hourly") {
    const hours = parseInt(scheduleValue, 10) || 1;
    return `0 */${hours} * * *`;
  }
  if (scheduleType === "weekly") {
    const day = parseInt(scheduleValue, 10) || 0;
    const [hour, minute] = (scheduleTime || "00:00").split(":").map(n => parseInt(n, 10) || 0);
    return `${minute} ${hour} * * ${day}`;
  }
  if (scheduleType === "daily") {
    const [hour, minute] = (scheduleTime || "00:00").split(":").map(n => parseInt(n, 10) || 0);
    return `${minute} ${hour} * * *`;
  }
  return null;
}

async function shutdownScheduler() {
  if (schedulerWorker) {
    try {
      await schedulerWorker.close();
      console.log("[scheduler] Worker closed");
    } catch (err) {
      console.error("[scheduler] Error closing worker:", err?.message || err);
    }
  }
  if (schedulerQueueEvents) {
    try {
      await schedulerQueueEvents.close();
      console.log("[scheduler] QueueEvents closed");
    } catch (err) {
      console.error("[scheduler] Error closing queue events:", err?.message || err);
    }
  }
  if (schedulerQueue) {
    try {
      await schedulerQueue.close();
      console.log("[scheduler] Queue closed");
    } catch (err) {
      console.error("[scheduler] Error closing queue:", err?.message || err);
    }
  }
}

function parseOriginHost(value) {
  if (!value) return "";
  try {
    const u = new URL(value);
    return extractHostnameFromHeader(u.host || u.hostname || "");
  } catch {
    return extractHostnameFromHeader(value);
  }
}

function normalizeHostLower(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw || raw === "null" || raw === "undefined") return "";
  const h = extractHostnameFromHeader(raw);
  if (!h) return "";
  return h.toLowerCase().replace(/\.+$/, "");
}

function isLocalHost(name) {
  const h = String(name || "").toLowerCase();
  return h === "localhost" || h === "127.0.0.1" || h === "::1" || h === "[::1]";
}

function hostAllowed(headerHost, allowedHosts) {
  if (!headerHost) return false;
  if (!allowedHosts || allowedHosts.size === 0) return false;
  if (allowedHosts.has(headerHost)) return true;
  for (const allowed of allowedHosts) {
    if (!allowed) continue;
    if (headerHost === allowed) return true;
    if (allowed.startsWith("*.") || allowed.startsWith(".")) {
      const base = allowed.replace(/^\*\./, "").replace(/^\./, "");
      if (!base) continue;
      if (headerHost === base || headerHost.endsWith(`.${base}`)) return true;
    }
  }
  return false;
}

const LOCAL_CAPTCHA_EXP_MS = 75 * 1000;
const LOCAL_CAPTCHA_GRID = 9;
const LOCAL_CAPTCHA_CORRECT_MAX = 2;
const LOCAL_CAPTCHA_MAX_STEPS = 3;
const LOCAL_CAPTCHA_SKIPS_SAFE = 2;
const LOCAL_CAPTCHA_SKIPS_SUS = 1;
const LOCAL_CAPTCHA_REQUIRED_SAFE = 2;
const LOCAL_CAPTCHA_REQUIRED_SUS = 3;
const LOCAL_CAPTCHA_MAX_QUESTIONS_SAFE = 5;
const LOCAL_CAPTCHA_MAX_QUESTIONS_SUS = 4;
const LOCAL_CAPTCHA_MAX_WRONG = 3;

const LOCAL_CAPTCHA_TYPES = [
  { id: "bridges", prompt: "Select all images with bridges", keyword: "bridge" },
  { id: "bikes", prompt: "Select all images with bicycles", keyword: "bicycle" },
  { id: "crosswalks", prompt: "Select all images with crosswalks", keyword: "crosswalk" },
  { id: "trafficlights", prompt: "Select all images with traffic lights", keyword: "traffic light" },
  { id: "mountains", prompt: "Select all images with mountains", keyword: "mountain" },
  { id: "beaches", prompt: "Select all images with beaches", keyword: "beach" },
  { id: "cats", prompt: "Select all images with cats", keyword: "cat" },
  { id: "dogs", prompt: "Select all images with dogs", keyword: "dog" },
  { id: "planes", prompt: "Select all images with airplanes", keyword: "airplane" },
  { id: "boats", prompt: "Select all images with boats", keyword: "boat" },
  { id: "buses", prompt: "Select all images with buses", keyword: "bus" },
  { id: "trains", prompt: "Select all images with trains", keyword: "train" },
  { id: "statues", prompt: "Select all images with statues", keyword: "statue" },
  { id: "stadiums", prompt: "Select all images with stadiums", keyword: "stadium" },
  { id: "flowers", prompt: "Select all images with flowers", keyword: "flower" },
  { id: "coffee", prompt: "Select all images with coffee cups", keyword: "coffee" },
  { id: "laptops", prompt: "Select all images with laptops", keyword: "laptop" },
  { id: "books", prompt: "Select all images with books", keyword: "book" },
  { id: "skylines", prompt: "Select all images with city skylines", keyword: "city skyline" },
  { id: "umbrellas", prompt: "Select all images with umbrellas", keyword: "umbrella" },
];

function imageUrl(keyword, seed) {
  const tag = encodeURIComponent(keyword.replace(/\s+/g, ","));
  return `https://loremflickr.com/360/360/${tag}?lock=${encodeURIComponent(seed)}`;
}

const LOCAL_CAPTCHA_POOL = LOCAL_CAPTCHA_TYPES;

function randomPick(arr, count) {
  const copy = [...arr];
  for (let i = copy.length - 1; i > 0; i--) {
    const j = crypto.randomInt(i + 1);
    [copy[i], copy[j]] = [copy[j], copy[i]];
  }
  return copy.slice(0, count);
}

function assignLocalCaptcha(req) {
  if (!req.session) return null;
  const suspicious = getLoginAttemptCount(getRequestIp(req)) >= 6;
  const required = suspicious ? LOCAL_CAPTCHA_REQUIRED_SUS : LOCAL_CAPTCHA_REQUIRED_SAFE;
  const allowedSkips = suspicious ? LOCAL_CAPTCHA_SKIPS_SUS : LOCAL_CAPTCHA_SKIPS_SAFE;
  const maxQuestions = suspicious ? LOCAL_CAPTCHA_MAX_QUESTIONS_SUS : LOCAL_CAPTCHA_MAX_QUESTIONS_SAFE;
  const steps = Math.min(LOCAL_CAPTCHA_MAX_STEPS, maxQuestions);

  const buildQuestion = () => {
    const type = LOCAL_CAPTCHA_POOL[crypto.randomInt(LOCAL_CAPTCHA_POOL.length)];
    const decoyType = randomPick(LOCAL_CAPTCHA_POOL.filter((t) => t.id !== type.id), 1)[0] || type;

    const correctCount = Math.min(LOCAL_CAPTCHA_CORRECT_MAX, Math.max(2, LOCAL_CAPTCHA_GRID - 1));
    const decoyCount = Math.max(0, LOCAL_CAPTCHA_GRID - correctCount);

    const nowSeed = Date.now();
    const correctImgs = Array.from({ length: correctCount }, (_, i) => imageUrl(type.keyword, `${nowSeed}-${i}`));
    const decoyImgs = Array.from({ length: decoyCount }, (_, i) => imageUrl(decoyType.keyword, `${nowSeed}-d-${i}`));

    const grid = randomPick(
      correctImgs.map((u) => ({ url: u, correct: true }))
        .concat(decoyImgs.map((u) => ({ url: u, correct: false }))),
      LOCAL_CAPTCHA_GRID
    );

    const correct = [];
    grid.forEach((g, idx) => {
      if (g.correct) correct.push(idx);
    });

    return {
      id: type.id,
      prompt: type.prompt,
      images: grid.map((g) => g.url),
      correct,
      prompt: `${type.prompt} (${correctCount} images)`,
      token: crypto.randomBytes(12).toString("hex"),
      expiresAt: Date.now() + LOCAL_CAPTCHA_EXP_MS,
      used: false,
    };
  };

  const questions = [];
  for (let i = 0; i < maxQuestions; i++) questions.push(buildQuestion());

  req.session.captchaRequired = true;
  req.session.localCaptcha = {
    idx: 0,
    total: steps,
    required,
    maxQuestions,
    questions,
    allowedSkips,
    usedSkips: 0,
    incorrectClicks: 0,
    maxIncorrect: LOCAL_CAPTCHA_MAX_WRONG,
    answered: 0,
  };

  const current = questions[0];
  if (!current) return null;
  return {
    id: current.id,
    prompt: current.prompt,
    images: current.images,
    token: current.token,
    step: 1,
    total: steps,
    remainingSkips: getRemainingSkips(req.session.localCaptcha),
  };
}

function clearLocalCaptcha(req) {
  if (req.session) {
    req.session.captchaRequired = false;
    req.session.localCaptcha = null;
  }
}

function validateLocalCaptcha(req, selectedIndexes, token) {
  const saved = req.session?.localCaptcha;
  if (!saved || !Array.isArray(saved.questions)) return { ok: false, done: false };
  const current = saved.questions[saved.idx];
  if (!current || !Array.isArray(current.correct) || !current.token) return { ok: false, done: false };
  if (!token || !safeCompare(String(token), String(current.token))) return { ok: false, done: false };
  if (current.used) return { ok: false, done: false };
  if (current.expiresAt && current.expiresAt < Date.now()) return { ok: false, done: false, expired: true };

  const expected = Array.from(new Set(current.correct.map(Number).filter(Number.isInteger))).sort();
  const provided = Array.from(new Set((selectedIndexes || []).map(Number).filter(Number.isInteger))).sort();
  if (provided.length === 0) return { ok: false, done: false };
  if (provided.length !== expected.length) return { ok: false, done: false };
  for (let i = 0; i < expected.length; i++) {
    if (expected[i] !== provided[i]) return { ok: false, done: false };
  }

  current.used = true;
  saved.idx += 1;
  saved.answered = (saved.answered || 0) + 1;

  let done = false;
  if (saved.answered >= saved.required) {
    done = true;
  } else if (saved.idx >= saved.maxQuestions) {
    return { ok: false, done: false, error: "not-enough-answers" };
  }

  if (done) {
    req.session.captchaRequired = false;
    req.session.captchaSolved = true;
    req.session.localCaptcha = null;
  } else {
    req.session.captchaRequired = true;
    req.session.localCaptcha = saved;
  }
  return { ok: true, done, next: saved.idx + 1, total: saved.total, remainingSkips: getRemainingSkips(saved) };
}

function skipLocalCaptcha(req) {
  const saved = req.session?.localCaptcha;
  if (!saved || !Array.isArray(saved.questions)) return { ok: false, done: false, error: "no-captcha" };
  if (getRemainingSkips(saved) <= 0) return { ok: false, done: false, error: "no-skips-left" };

  saved.usedSkips = (saved.usedSkips || 0) + 1;
  saved.idx += 1;
  let done = false;
  if (saved.answered >= saved.required) {
    done = true;
  } else if (saved.idx >= saved.maxQuestions && saved.answered >= saved.required) {
    done = true;
  } else if (saved.idx >= saved.maxQuestions && saved.answered < saved.required) {
    return { ok: false, done: false, error: "not-enough-answers" };
  }

  if (done) {
    req.session.captchaRequired = false;
    req.session.captchaSolved = true;
    req.session.localCaptcha = null;
  } else {
    req.session.captchaRequired = true;
    req.session.localCaptcha = saved;
  }

  return { ok: true, done, next: saved.idx + 1, total: saved.total, remainingSkips: getRemainingSkips(saved) };
}

function getCurrentCaptchaQuestion(req) {
  const saved = req.session?.localCaptcha;
  if (!saved || !Array.isArray(saved.questions)) return null;
  const current = saved.questions[saved.idx];
  if (!current) return null;
  if (current.expiresAt && current.expiresAt < Date.now()) return null;
  return {
    id: current.id,
    prompt: current.prompt,
    images: current.images,
    token: current.token,
    step: (saved.idx || 0) + 1,
    total: saved.total || saved.questions.length || 1,
    remainingSkips: getRemainingSkips(saved),
    required: saved.required || 1,
    answered: saved.answered || 0,
  };
}

const _CSRF_EXTRA_ALLOWED = (process.env.CSRF_ALLOWED_HOSTS || "")
  .split(",")
  .map(s => normalizeHostLower(s.trim()))
  .filter(Boolean);
const _CSRF_PANEL_PUBLIC_HOST = normalizeHostLower(parseOriginHost(process.env.PANEL_PUBLIC_URL || ""));

async function sameOriginProtection(req, res, next) {
  try {
    const method = (req.method || "").toUpperCase();
    if (method === "GET" || method === "HEAD" || method === "OPTIONS") return next();
    if (await isOpenNodeRoute(req)) return next();

    const originHost = normalizeHostLower(parseOriginHost(req.headers.origin || req.headers.Origin || ""));
    const refererHost = normalizeHostLower(parseOriginHost(req.headers.referer || req.headers.referrer || ""));
    const forwardedHost = normalizeHostLower(getTrustedForwardedHost(req) || "");
    const hostHeader = normalizeHostLower(req.headers.host || "");
    const requestHost = forwardedHost || hostHeader;

    const fetchSite = String(req.headers["sec-fetch-site"] || "").toLowerCase();
    const sameSiteFetch = fetchSite === "same-origin" || fetchSite === "same-site";

    const allowedHosts = new Set(
      [requestHost, forwardedHost, hostHeader, _CSRF_PANEL_PUBLIC_HOST, ..._CSRF_EXTRA_ALLOWED].filter(Boolean)
    );
    if (isLocalHost(originHost) || isLocalHost(refererHost)) {
      allowedHosts.add("localhost");
      allowedHosts.add("127.0.0.1");
      allowedHosts.add("::1");
    }



    if (req.headers.origin === "null" || req.headers.referer === "null") {
      if (req.headers.cookie && !sameSiteFetch) return res.status(403).send("CSRF blocked");
    } else if (!originHost && !refererHost) {
      if (req.headers.cookie && !sameSiteFetch) return res.status(403).send("CSRF blocked");
      return next();
    }

    if ((originHost || refererHost) && (requestHost || allowedHosts.size > 0)) {
      const headerHost = originHost || refererHost;
      if (headerHost && !hostAllowed(headerHost, allowedHosts) && !sameSiteFetch) {
        return res.status(403).send("CSRF blocked");
      }
    }

    return next();
  } catch (err) {
    return next(err);
  }
}

app.use(sameOriginProtection);


try {
  if (!fs.existsSync(SECURITY_FILE)) {
    safeWriteJson(SECURITY_FILE, security);
  } else {
    const raw = fs.readFileSync(SECURITY_FILE, "utf8");
    security = safeMerge(security, JSON.parse(raw) || {});
  }
} catch (err) {
  console.error("[rate-limiter] Error ensuring security.json:", err);
}

try {
  fs.watch(SECURITY_FILE, (evtType) => {
    if (evtType === "change" || evtType === "rename") {
      try {
        const raw = fs.readFileSync(SECURITY_FILE, "utf8");
        security = safeMerge(security, JSON.parse(raw) || {});
      } catch { }
    }
  });
} catch { }

const rateRequests = new Map();
const RATE_LIMIT_EXEMPT_PATHS = new Set([
  "/api/me",
  "/health",
]);
const RATE_LIMIT_EXEMPT_PATTERNS = [
  /^\/api\/server\/[^/]+\/node-status$/,
];
function isRateLimitExempt(path) {
  if (RATE_LIMIT_EXEMPT_PATHS.has(path)) return true;
  for (const pattern of RATE_LIMIT_EXEMPT_PATTERNS) {
    if (pattern.test(path)) return true;
  }
  return false;
}
const RATE_MAX_TRACKED_IPS = scaleForMemory(50000);
function rateLimiterMiddleware(req, res, next) {
  try {
    if (!security || security.rate_limiting !== true) return next();
    if (redisClient && req.path.startsWith("/api/")) return next();
    if (isRateLimitExempt(req.path)) return next();

    const ip = getRequestIp(req);
    const now = Date.now();
    const windowMs = (security.window_seconds || 120) * 1000;
    const limit = security.limit || 5;

    if (!rateRequests.has(ip) && rateRequests.size >= RATE_MAX_TRACKED_IPS) {
      return res.status(503).send("503 Service Busy");
    }

    let arr = rateRequests.get(ip) || [];
    arr = arr.filter(ts => (now - ts) <= windowMs);

    if (arr.length >= limit) {
      const oldest = arr[0] || now;
      const retryAfter = Math.ceil((oldest + windowMs - now) / 1000);
      res.setHeader("Retry-After", String(retryAfter));
      return res.status(429).send("429 Too Many Requests");
    }

    arr.push(now);
    if (arr.length > limit) arr = arr.slice(-limit);
    rateRequests.set(ip, arr);
    return next();
  } catch {
    return next();
  }
}

setInterval(() => {
  try {
    const now = Date.now();
    const windowMs = (security.window_seconds || 120) * 1000;
    for (const [ip, arr] of rateRequests.entries()) {
      const kept = arr.filter(ts => (now - ts) <= windowMs);
      if (kept.length > 0) rateRequests.set(ip, kept);
      else rateRequests.delete(ip);
    }
  } catch { }
}, 30_000).unref();

app.use(rateLimiterMiddleware);

async function loadUsers() {
  const rows = await db.query(
    "SELECT id, email, admin, agent_access FROM users"
  );
  return rows.map(r => ({
    id: r.id,
    email: r.email,
    admin: !!r.admin,
    agent_access: !!(r && r.agent_access)
  }));
}

async function findUserByEmail(email) {
  if (!email) return null;

  let rows;
  try {
    rows = await db.query(
      "SELECT id, email, password, secret, admin, agent_access, avatar_url, preferences FROM users WHERE LOWER(email) = LOWER(?) LIMIT 1",
      [email]
    );
  } catch (err) {
    if (err.code === 'ER_BAD_FIELD_ERROR' && err.message?.includes('avatar_url')) {
      rows = await db.query(
        "SELECT id, email, password, secret, admin, agent_access FROM users WHERE LOWER(email) = LOWER(?) LIMIT 1",
        [email]
      );
    } else {
      throw err;
    }
  }

  const row = rows[0];
  if (!row) return null;
  const user = mapUserRow(row);
  if (user) {
    user.agent_access = !!(row && row.agent_access);
    user.avatar_url = row.avatar_url || null;
    try {
      user.preferences = row.preferences ? JSON.parse(row.preferences) : {};
    } catch (_) {
      user.preferences = {};
    }
  }
  return user;
}

async function findUserById(id) {
  if (!id) return null;

  let rows;
  try {
    rows = await db.query(
      "SELECT id, email, password, secret, admin, agent_access, avatar_url, preferences FROM users WHERE id = ? LIMIT 1",
      [id]
    );
  } catch (err) {
    if (err.code === 'ER_BAD_FIELD_ERROR' && err.message?.includes('avatar_url')) {
      rows = await db.query(
        "SELECT id, email, password, secret, admin, agent_access FROM users WHERE id = ? LIMIT 1",
        [id]
      );
    } else {
      throw err;
    }
  }

  const row = rows[0];
  if (!row) return null;
  const user = mapUserRow(row);
  if (user) {
    user.agent_access = !!(row && row.agent_access);
    user.avatar_url = row.avatar_url || null;
    try {
      user.preferences = row.preferences ? (typeof row.preferences === "string" ? JSON.parse(row.preferences) : row.preferences) : null;
    } catch {
      user.preferences = null;
    }
  }
  return user;
}

async function getUserIdByEmail(email) {
  if (!email) return null;
  const rows = await db.query(
    "SELECT id FROM users WHERE LOWER(email) = LOWER(?) LIMIT 1",
    [email]
  );
  return rows[0] ? rows[0].id : null;
}

async function loadUserAccess() {
  const rows = await db.query(
    "SELECT u.email, ua.server_name FROM user_access ua JOIN users u ON u.id = ua.user_id ORDER BY u.email"
  );
  const grouped = new Map();
  for (const row of rows) {
    const email = String(row.email || "").toLowerCase();
    if (!email) continue;
    if (!grouped.has(email)) grouped.set(email, { email: row.email, servers: [] });
    grouped.get(email).servers.push(row.server_name);
  }
  return Array.from(grouped.values());
}

async function saveUserAccess(arr) {
  const list = Array.isArray(arr) ? arr : [];
  const conn = await db.pool.getConnection();
  try {
    await conn.beginTransaction();
    for (const rec of list) {
      const email = String(rec?.email || "").trim();
      if (!email) continue;
      const [userRows] = await conn.execute(
        "SELECT id FROM users WHERE LOWER(email) = LOWER(?) LIMIT 1",
        [email]
      );
      if (!userRows[0]) continue;
      const userId = userRows[0].id;
      await conn.execute("DELETE FROM user_access WHERE user_id = ?", [userId]);
      const servers = Array.isArray(rec.servers) ? rec.servers : [];
      for (const server of servers) {
        await conn.execute(
          "INSERT IGNORE INTO user_access (user_id, server_name) VALUES (?, ?)",
          [userId, server]
        );
      }
    }
    await conn.commit();
    return true;
  } catch (err) {
    try { await conn.rollback(); } catch { }
    console.error("[db] saveUserAccess failed:", err);
    return false;
  } finally {
    conn.release();
  }
}

async function getAccessListForEmail(email) {
  if (!email) return [];
  const rows = await db.query(
    "SELECT ua.server_name FROM user_access ua JOIN users u ON u.id = ua.user_id WHERE LOWER(u.email) = LOWER(?)",
    [email]
  );
  return rows.map(r => r.server_name);
}

async function addAccessForEmail(email, server) {
  if (!email || !server) return false;
  const userId = await getUserIdByEmail(email);
  if (!userId) return false;
  await db.query(
    "INSERT IGNORE INTO user_access (user_id, server_name) VALUES (?, ?)",
    [userId, server]
  );
  return true;
}

async function removeAccessForEmail(email, server) {
  if (!email || !server) return false;
  const userId = await getUserIdByEmail(email);
  if (!userId) return false;
  await db.query(
    "DELETE FROM user_access WHERE user_id = ? AND server_name = ?",
    [userId, server]
  );
  return true;
}

async function removeAccessForServerName(server) {
  if (!server) return false;
  await db.query("DELETE FROM user_access WHERE server_name = ?", [server]);
  return true;
}

async function userHasAccessToServer(email, botName) {
  if (!email) return false;
  const u = await findUserByEmail(email);
  if (u && u.admin) return true;
  const access = await getAccessListForEmail(email);
  if (!access || access.length === 0) return false;

  const lowerAccess = access.map(s => String(s).toLowerCase());
  const lowerBot = String(botName).toLowerCase();

  if (lowerAccess.includes("all")) return true;
  return lowerAccess.includes(lowerBot);
}

async function getLoggedInUser(req) {
  if (!req) return null;
  if (Object.prototype.hasOwnProperty.call(req, "currentUser")) {
    return req.currentUser;
  }
  const email = req.session && req.session.user;
  if (!email) {
    req.currentUser = null;
    return null;
  }
  const user = await findUserByEmail(email);
  req.currentUser = user || null;
  return req.currentUser;
}

async function isAuthenticated(req) {
  return !!(await getLoggedInUser(req));
}

async function isAdmin(req) {
  const u = await getLoggedInUser(req);
  return !!(u && u.admin);
}

app.use(async (req, _res, next) => {
  if (req._staticAsset) return next();
  try {
    req.isAdmin = await isAdmin(req);
    next();
  } catch (err) {
    next(err);
  }
});

const MAINTENANCE_BYPASS_PATHS = new Set([
  "/login",
  "/logout",
  "/api/maintenance/status",
  "/favicon.ico",
  "/register",
  "/forgot-password"
]);

app.use(async (req, res, next) => {
  try {
    if (MAINTENANCE_BYPASS_PATHS.has(req.path) ||
      req.path.startsWith("/css") ||
      req.path.startsWith("/js") ||
      req.path.startsWith("/images") ||
      req.path.startsWith("/css-ext") ||
      req.path.endsWith(".css") ||
      req.path.endsWith(".js") ||
      req.path.endsWith(".webp") ||
      req.path.endsWith(".png") ||
      req.path.endsWith(".avif") ||
      req.path.endsWith(".ico") ||
      req.path.endsWith(".woff2")) {
      return next();
    }

    if (!isMaintenanceActive()) return next();

    if (req.isAdmin) return next();

    if (req.path.startsWith("/api/")) {
      return res.status(503).json({
        error: "maintenance",
        message: "The panel is currently under maintenance. Please try again later."
      });
    }

    const state = getMaintenanceState();
    return res.status(503).render("maintenance", {
      reason: state.reason || null,
      cspNonce: res.locals.cspNonce || ""
    });
  } catch (err) {
    return next(err);
  }
});

let _siCache = null;
let _siCacheTs = 0;
let _siInflight = null;
const SI_CACHE_TTL = parseInt(process.env.SERVER_INDEX_CACHE_TTL_MS || "5000", 10) || 5000;

async function loadServersIndex() {
  const now = Date.now();
  if (_siCache && (now - _siCacheTs) < SI_CACHE_TTL) return _siCache.slice();
  if (_siInflight) return (await _siInflight).slice();
  _siInflight = db.query(
    "SELECT id, name, display_name, legacy_id, bot, template, start, node_id, ip, port, status, runtime, docker, acl, resources FROM servers"
  ).then(rows => {
    const mapped = rows.map(mapServerRow);
    _siCache = mapped;
    _siCacheTs = Date.now();
    return mapped;
  }).finally(() => { _siInflight = null; });
  return (await _siInflight).slice();
}

function invalidateServerIndexCache() {
  _siCache = null;
  _siCacheTs = 0;
}

async function saveServersIndex(list) {
  const safeList = Array.isArray(list) ? list : [];
  const names = safeList.map(s => s && s.name).filter(Boolean);
  const nameSet = new Set(names);
  const conn = await db.pool.getConnection();
  try {
    await conn.beginTransaction();
    const [existingRows] = await conn.execute("SELECT name FROM servers");
    const existing = new Set(existingRows.map(r => r.name));

    const toDelete = [...existing].filter(n => !nameSet.has(n));
    if (toDelete.length > 0) {
      const DBATCH = 500;
      for (let i = 0; i < toDelete.length; i += DBATCH) {
        const batch = toDelete.slice(i, i + DBATCH);
        const placeholders = batch.map(() => '?').join(',');
        await conn.execute(`DELETE FROM servers WHERE name IN (${placeholders})`, batch);
      }
    }

    for (const entry of safeList) {
      if (!entry || !entry.name) continue;
      const cleanEntry = { ...entry };
      delete cleanEntry.startupCommand;
      if (cleanEntry.runtime && typeof cleanEntry.runtime === "object") {
        cleanEntry.runtime = { ...cleanEntry.runtime };
        delete cleanEntry.runtime.startupCommand;
      }
      if (cleanEntry.docker && typeof cleanEntry.docker === "object") {
        cleanEntry.docker = { ...cleanEntry.docker };
        delete cleanEntry.docker.startupCommand;
      }
      const legacyId = cleanEntry.id ? String(cleanEntry.id) : null;
      const runtime = toDbJson(cleanEntry.runtime);
      const docker = toDbJson(cleanEntry.docker);
      const acl = toDbJson(cleanEntry.acl);
      const resources = toDbJson(cleanEntry.resources);
      await conn.execute(
        `INSERT INTO servers
          (name, display_name, legacy_id, bot, template, start, node_id, ip, port, status, runtime, docker, acl, resources)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
           display_name = COALESCE(VALUES(display_name), display_name),
           legacy_id = VALUES(legacy_id),
           bot = VALUES(bot),
           template = VALUES(template),
           start = VALUES(start),
           node_id = VALUES(node_id),
           ip = VALUES(ip),
           port = VALUES(port),
           status = VALUES(status),
           runtime = VALUES(runtime),
           docker = VALUES(docker),
           acl = VALUES(acl),
           resources = VALUES(resources)`,
        [
          cleanEntry.name,
          cleanEntry.displayName || null,
          legacyId,
          cleanEntry.bot || null,
          cleanEntry.template || null,
          cleanEntry.start || null,
          cleanEntry.nodeId || null,
          cleanEntry.ip || null,
          cleanEntry.port == null ? null : Number(cleanEntry.port),
          cleanEntry.status || null,
          runtime,
          docker,
          acl,
          resources,
        ]
      );
    }
    await conn.commit();
    invalidateServerIndexCache();
    return true;
  } catch (err) {
    try { await conn.rollback(); } catch { }
    console.error("[db] saveServersIndex failed:", err);
    return false;
  } finally {
    conn.release();
  }
}

async function upsertServerIndexEntry(entry) {
  if (!entry || !entry.name) return false;
  const existing = await findServer(entry.name);
  const merged = existing ? safeMerge(Object.assign({}, existing), entry) : entry;
  if (merged && typeof merged === "object") {
    delete merged.startupCommand;
    if (merged.runtime && typeof merged.runtime === "object") {
      delete merged.runtime.startupCommand;
    }
    if (merged.docker && typeof merged.docker === "object") {
      delete merged.docker.startupCommand;
    }
  }
  const legacyId = merged.id ? String(merged.id) : null;
  await db.query(
    `INSERT INTO servers
      (name, display_name, legacy_id, bot, template, start, node_id, ip, port, status, runtime, docker, acl, resources)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
       display_name = COALESCE(VALUES(display_name), display_name),
       legacy_id = VALUES(legacy_id),
       bot = VALUES(bot),
       template = VALUES(template),
       start = VALUES(start),
       node_id = VALUES(node_id),
       ip = VALUES(ip),
       port = VALUES(port),
       status = VALUES(status),
       runtime = VALUES(runtime),
       docker = VALUES(docker),
       acl = VALUES(acl),
       resources = VALUES(resources)`,
    [
      merged.name,
      merged.displayName || null,
      legacyId,
      merged.bot || null,
      merged.template || null,
      merged.start || null,
      merged.nodeId || null,
      merged.ip || null,
      merged.port == null ? null : Number(merged.port),
      merged.status || null,
      toDbJson(merged.runtime),
      toDbJson(merged.docker),
      toDbJson(merged.acl),
      toDbJson(merged.resources),
    ]
  );
  invalidateServerIndexCache();
  return true;
}

async function removeServerIndexEntry(name) {
  if (!name) return false;
  await db.query("DELETE FROM servers WHERE name = ?", [name]);
  invalidateServerIndexCache();
  return true;
}

async function findServer(botName) {
  const needle = String(botName || "").trim().toLowerCase();
  if (!needle) return null;
  const rows = await db.query(
    `SELECT id, name, legacy_id, bot, template, start, node_id, ip, port, status, runtime, docker, acl, resources
     FROM servers
     WHERE LOWER(name) = ? OR LOWER(legacy_id) = ? OR LOWER(bot) = ?
     LIMIT 1`,
    [needle, needle, needle]
  );
  return mapServerRow(rows[0]) || null;
}

function findServerIndex(list, botName) {
  const needle = String(botName || "").trim().toLowerCase();
  if (!needle) return -1;
  return list.findIndex(s => {
    if (!s) return false;
    if (s.name && String(s.name).toLowerCase() === needle) return true;
    if (s.id && String(s.id).toLowerCase() === needle) return true;
    if (s.bot && String(s.bot).toLowerCase() === needle) return true;
    return false;
  });
}

function isRemoteEntry(entry) {
  return !!(entry && entry.nodeId && entry.nodeId !== "local");
}

const ALLOWED_PERM_KEYS = [
  "files_read",
  "files_delete",
  "files_rename",
  "files_archive",
  "console_read",
  "console_write",
  "server_stop",
  "server_start",
  "files_upload",
  "files_create",
  "activity_logs",
  "backups_view",
  "backups_create",
  "backups_delete",
  "scheduler_access",
  "scheduler_create",
  "scheduler_delete",
  "store_access",
  "server_reinstall",
];

async function getEffectivePermsForUserOnServer(email, serverName) {
  const permsTemplate = {
    files_read: false,
    files_delete: false,
    files_rename: false,
    files_archive: false,
    console_read: false,
    console_write: false,
    server_stop: false,
    server_start: false,
    files_upload: false,
    files_create: false,
    activity_logs: false,
    backups_view: false,
    backups_create: false,
    backups_delete: false,
    scheduler_access: false,
    scheduler_create: false,
    scheduler_delete: false,
    store_access: false,
    server_reinstall: false,
  };

  if (!email || !serverName) return permsTemplate;

  const u = await findUserByEmail(email);

  const needle = String(serverName).trim().toLowerCase();
  const entry = await findServer(needle);
  const acl = entry && entry.acl && typeof entry.acl === "object" ? entry.acl : null;
  const rec = acl ? acl[String(email).toLowerCase()] : null;

  if (u && u.admin) {
    const allTrue = {};
    for (const k of Object.keys(permsTemplate)) allTrue[k] = true;
    return allTrue;
  }

  if (rec && typeof rec === "object") {
    const merged = { ...permsTemplate };
    for (const k of Object.keys(merged)) merged[k] = !!rec[k];
    if (!Object.prototype.hasOwnProperty.call(rec, "console_read") && rec.console_write) {
      merged.console_read = true;
    }
    if (rec.backups_manage) {
      merged.backups_view = true;
      merged.backups_create = true;
      merged.backups_delete = true;
    }
    return merged;
  }

  return permsTemplate;
}

nodesRouter.setPermissionCheckers(getEffectivePermsForUserOnServer, isAdmin, userHasAccessToServer);

async function loadNodes() {
  const rows = await db.query(
    "SELECT id, uuid, name, address, ram_mb, disk_gb, ports, token_id, token, created_at, api_port, sftp_port, max_upload_mb, port_ok, last_seen, last_check, online, build_config FROM nodes"
  );
  return rows.map(mapNodeRow);
}

async function findNodeByIdOrName(idOrName) {
  const key = String(idOrName || "").trim().toLowerCase();
  if (!key) return null;
  const rows = await db.query(
    `SELECT id, uuid, name, address, ram_mb, disk_gb, ports, token_id, token, created_at, api_port, sftp_port, max_upload_mb, port_ok, last_seen, last_check, online, build_config
     FROM nodes
     WHERE LOWER(id) = ? OR LOWER(uuid) = ? OR LOWER(name) = ?
     LIMIT 1`,
    [key, key, key]
  );
  return mapNodeRow(rows[0]) || null;
}

function buildNodeBaseUrl(address, port, sslEnabled) {
  let base = String(address || "").trim();
  if (!base) return null;
  if (/^https?:\/\//i.test(base)) {
    try {
      const u = new URL(base);
      if (u.username || u.password) return null;
      u.hash = "";
      u.search = "";
      u.pathname = "";
      if (!u.port) u.port = String(clampApiPort(port || 8080));
      return u.origin;
    } catch { }
  }
  const defaultScheme = sslEnabled ? "https" : (process.env.NODE_DEFAULT_SCHEME || "http");
  return `${defaultScheme}://${base}:${clampApiPort(port || 8080)}`;
}

function nodeAuthHeadersFor(node, isRemote) {
  const h = { "Content-Type": "application/json" };
  const remoteToken = node && (node.token || node.secret || node.api_key);
  if (isRemote && remoteToken) {
    h.Authorization = `Bearer ${remoteToken}`;
    h["X-Node-Token"] = remoteToken;
    if (node.token_id) h["X-Node-Token-Id"] = node.token_id;
    return h;
  }
  if (!isRemote && LOCAL_NODE_TOKEN) {
    h.Authorization = `Bearer ${LOCAL_NODE_TOKEN}`;
    h["X-Node-Token"] = LOCAL_NODE_TOKEN;
  }
  return h;
}

function signPanelAdminReinstallHeaders(headers, node, serverName, templateId) {
  if (!headers || typeof headers !== "object") return false;
  const role = "admin";
  const ts = String(Date.now());
  const name = String(serverName || "").trim();
  const template = String(templateId || "").trim().toLowerCase();
  if (!name || !template) return false;

  const nodeToken = String((node && (node.token || node.secret || node.api_key)) || "").trim();
  const signingSecret = PANEL_HMAC_SECRET || nodeToken;
  if (!signingSecret) return false;

  const base = `${name}|reinstall|${template}|${role}|${ts}`;
  const sig = crypto.createHmac("sha256", signingSecret).update(base).digest("hex");

  headers["x-panel-role"] = role;
  headers["x-panel-ts"] = ts;
  headers["x-panel-sign"] = sig;
  return true;
}


async function verifyExternalCaptcha(token, remoteIp) {
  if (!EXTERNAL_CAPTCHA_ENABLED) return { ok: true };
  if (!token) return { ok: false, error: "missing-captcha-token" };

  if (EXTERNAL_CAPTCHA_IS_CF) {
    const body = JSON.stringify({
      secret: EXTERNAL_CAPTCHA_SECRET,
      response: token,
      remoteip: remoteIp || undefined,
    });

    const res = await new Promise((resolve) => {
      const req = https.request(
        "https://challenges.cloudflare.com/turnstile/v0/siteverify",
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Content-Length": Buffer.byteLength(body),
          },
          timeout: 6000,
        },
        (r) => {
          const chunks = [];
          r.on("data", (d) => chunks.push(d));
          r.on("end", () => {
            try {
              const parsed = JSON.parse(Buffer.concat(chunks).toString("utf8"));
              resolve({ status: r.statusCode || 0, json: parsed });
            } catch {
              resolve({ status: r.statusCode || 0, json: null });
            }
          });
        }
      );

      req.on("error", () => resolve({ status: 0, json: null }));
      req.on("timeout", () => {
        try { req.destroy(); } catch { }
        resolve({ status: 0, json: null });
      });

      req.write(body);
      req.end();
    });

    if (res.status !== 200 || !(res.json && res.json.success)) {
      return { ok: false, error: "captcha-validation-failed" };
    }
    return { ok: true };
  }

  if (EXTERNAL_CAPTCHA_IS_HCAPTCHA) {
    const payload = `secret=${encodeURIComponent(EXTERNAL_CAPTCHA_SECRET)}&response=${encodeURIComponent(token)}&remoteip=${encodeURIComponent(remoteIp || "")}&sitekey=${encodeURIComponent(EXTERNAL_CAPTCHA_SITE_KEY)}`;

    const res = await new Promise((resolve) => {
      const req = https.request(
        "https://api.hcaptcha.com/siteverify",
        {
          method: "POST",
          headers: {
            "Content-Type": "application/x-www-form-urlencoded",
            "Content-Length": Buffer.byteLength(payload),
          },
          timeout: 6000,
        },
        (r) => {
          const chunks = [];
          r.on("data", (d) => chunks.push(d));
          r.on("end", () => {
            try {
              const parsed = JSON.parse(Buffer.concat(chunks).toString("utf8"));
              resolve({ status: r.statusCode || 0, json: parsed });
            } catch {
              resolve({ status: r.statusCode || 0, json: null });
            }
          });
        }
      );

      req.on("error", () => resolve({ status: 0, json: null }));
      req.on("timeout", () => {
        try { req.destroy(); } catch { }
        resolve({ status: 0, json: null });
      });

      req.write(payload);
      req.end();
    });

    if (res.status !== 200 || !(res.json && res.json.success)) {
      return { ok: false, error: "captcha-validation-failed" };
    }
    return { ok: true };
  }

  const payload = `secret=${encodeURIComponent(EXTERNAL_CAPTCHA_SECRET)}&response=${encodeURIComponent(token)}${remoteIp ? `&remoteip=${encodeURIComponent(remoteIp)}` : ""}`;

  const res = await new Promise((resolve) => {
    const req = https.request(
      "https://www.google.com/recaptcha/api/siteverify",
      {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          "Content-Length": Buffer.byteLength(payload),
        },
        timeout: 6000,
      },
      (r) => {
        const chunks = [];
        r.on("data", (d) => chunks.push(d));
        r.on("end", () => {
          try {
            const parsed = JSON.parse(Buffer.concat(chunks).toString("utf8"));
            resolve({ status: r.statusCode || 0, json: parsed });
          } catch {
            resolve({ status: r.statusCode || 0, json: null });
          }
        });
      }
    );

    req.on("error", () => resolve({ status: 0, json: null }));
    req.on("timeout", () => {
      try { req.destroy(); } catch { }
      resolve({ status: 0, json: null });
    });

    req.write(payload);
    req.end();
  });

  if (res.status !== 200 || !(res.json && res.json.success)) {
    return { ok: false, error: "captcha-validation-failed" };
  }

  return { ok: true };
}

async function createOnRemoteNode(node, payload) {
  const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
  if (!baseUrl) throw new Error("invalid node address");

  const info = await httpRequestJson(`${baseUrl}/v1/info`, "GET", { Authorization: `Bearer ${node.token}` }, null, 5000);
  if (info.status !== 200 || !(info.json && info.json.ok)) throw new Error("node not reachable");

  const headers = { Authorization: `Bearer ${node.token}`, "Content-Type": "application/json" };

  let res = await httpRequestJson(
    `${baseUrl}/v1/servers/create`,
    "POST",
    headers,
    payload,
    300_000
  );

  if (res.status === 0) {
    console.warn("[createOnRemoteNode] transport failure (status 0) — verifying server on node...");
    const check = await httpRequestJson(
      `${baseUrl}/v1/servers/${encodeURIComponent(payload.name)}/info`,
      "GET",
      headers,
      null,
      10_000
    );
    if (check.status === 200 && check.json) {
      return { ok: true, name: payload.name, recovered: true };
    }
    res = await httpRequestJson(
      `${baseUrl}/v1/servers/create`,
      "POST",
      headers,
      payload,
      300_000
    );
  }

  if (res.status !== 200 || !(res.json && res.json.ok)) {
    const errorText = String(res.json?.error || "").trim();
    const detailText = String(res.json?.detail || "").trim();
    const msg = detailText
      ? (errorText ? `${errorText}: ${detailText}` : detailText)
      : (errorText || `remote create failed (${res.status})`);
    throw new Error(msg);
  }

  return res.json;
}

function configuredNodeCapacity(node) {
  return {
    ramMb: Math.max(0, Math.trunc(Number(node?.ram_mb || node?.buildConfig?.ram_mb || 0))),
    cpuCores: Math.max(0, Number(node?.buildConfig?.cpu_cores || node?.buildConfig?.cpuCores || node?.cpu_cores || 0)),
    diskGb: Math.max(0, Number(node?.disk_gb || node?.buildConfig?.disk_gb || node?.buildConfig?.diskGb || 0)),
  };
}

function mergeNodeCapacityValue(configured, live, round = false) {
  const cfg = Number(configured);
  const detected = Number(live);
  let value = 0;
  if (Number.isFinite(cfg) && cfg > 0 && Number.isFinite(detected) && detected > 0) value = Math.min(cfg, detected);
  else if (Number.isFinite(detected) && detected > 0) value = detected;
  else if (Number.isFinite(cfg) && cfg > 0) value = cfg;
  return round ? Math.trunc(value) : value;
}

function resolveEffectiveNodeCapacity(node, liveCapacity = null) {
  const configured = configuredNodeCapacity(node);
  const live = (liveCapacity && typeof liveCapacity === "object") ? liveCapacity : {};
  return {
    ramMb: mergeNodeCapacityValue(configured.ramMb, live.ramMb, true),
    cpuCores: mergeNodeCapacityValue(configured.cpuCores, live.cpuCores, false),
    diskGb: mergeNodeCapacityValue(configured.diskGb, live.diskGb, false),
  };
}

async function fetchLiveNodeCapacity(node) {
  if (!node) return null;
  const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
  if (!baseUrl) return null;

  const headers = nodeAuthHeadersFor(node, !isLocalHost(node.address));
  const result = await httpRequestJson(`${baseUrl}/api/system/stats`, "GET", headers, null, 8000);
  if (!result || result.status !== 200 || !result.json || !result.json.ok) return null;

  const data = result.json;
  return {
    ramMb: Math.max(0, Math.trunc(Number(data.ram_total_mb || data.memory_total_mb || 0))),
    cpuCores: Math.max(0, Number(data.cpu_cores || data.cores || 0)),
    diskGb: Math.max(0, Number(data.disk_total_gb || 0)),
  };
}

async function getEffectiveNodeCapacity(node) {
  let liveCapacity = null;
  try {
    liveCapacity = await fetchLiveNodeCapacity(node);
  } catch { }
  return resolveEffectiveNodeCapacity(node, liveCapacity);
}

function safeJoinUnix(baseDir, rel) {
  const base = path.posix.normalize(String(baseDir ?? "")).replace(/\0/g, "");
  const raw = String(rel ?? "").replace(/\0/g, "");
  if (path.posix.isAbsolute(raw)) throw new Error("Absolute path not allowed");

  const normalized = path.posix.normalize("/" + raw).replace(/^\/+/, "");
  if (normalized === ".." || normalized.startsWith("../") || normalized.includes("/../")) {
    throw new Error("Unix path traversal detected");
  }
  return path.posix.join(base, normalized);
}

const PATH_TRAVERSAL_ERRORS = new Set(["Absolute path not allowed", "Unix path traversal detected"]);

function maskPathErrorMessage(err, traversalFallback, defaultFallback) {
  const msg = String(err?.message || "");
  if (PATH_TRAVERSAL_ERRORS.has(msg)) return traversalFallback;
  return msg || defaultFallback;
}

function sanitizeUploadFilename(name) {
  const raw = String(name ?? "").replace(/\0/g, "").trim();
  if (!raw) return null;
  const base = path.posix.basename(raw);
  const withoutSlashes = base.replace(/[\\/]/g, "");
  const cleaned = withoutSlashes.replace(/^\.+/, "").replace(/[^\w.\- ]+/g, "_").trim();
  if (!cleaned || cleaned === "." || cleaned === "..") return null;
  return cleaned.slice(0, 255);
}

function normalizeExtensionList(input) {
  return new Set(
    String(input || "")
      .split(",")
      .map(s => s.trim().toLowerCase())
      .filter(Boolean)
      .map(ext => (ext.startsWith(".") ? ext : `.${ext}`))
  );
}

const UPLOAD_ALLOWED_EXTENSIONS = normalizeExtensionList(process.env.UPLOAD_ALLOWED_EXTENSIONS);
const UPLOAD_BLOCKED_EXTENSIONS = normalizeExtensionList(process.env.UPLOAD_BLOCKED_EXTENSIONS);
const UPLOAD_MULTIPART_EXTENSIONS = [".tar.gz", ".tar.bz2", ".tar.xz", ".tar.zst"];

function getUploadExtension(filename) {
  const lower = String(filename || "").toLowerCase();
  for (const ext of UPLOAD_MULTIPART_EXTENSIONS) {
    if (lower.endsWith(ext)) return ext;
  }
  const single = path.posix.extname(lower);
  return single || "";
}

function isUploadExtensionAllowed(filename) {
  const ext = getUploadExtension(filename);
  if (ext && UPLOAD_BLOCKED_EXTENSIONS.has(ext)) return false;
  if (UPLOAD_ALLOWED_EXTENSIONS.size === 0) return true;
  return !!(ext && UPLOAD_ALLOWED_EXTENSIONS.has(ext));
}

async function nodeFsPost(node, endpoint, payload, timeoutMs = 20000) {
  const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
  if (!baseUrl) return { status: 0, json: null };
  const headers = nodeAuthHeadersFor(node, true);
  return httpRequestJson(`${baseUrl}${endpoint}`, "POST", headers, payload, timeoutMs);
}

async function resolveRemoteFsContext(botName) {
  const entry = await findServer(botName);
  if (!isRemoteEntry(entry)) return { remote: false };
  const node = await findNodeByIdOrName(entry.nodeId);
  if (!node) return { remote: false };
  const safeName = sanitizeServerName(entry.name || botName);
  if (!safeName) return { remote: false };
  const baseDir = `${NODE_VOLUME_ROOT}/${safeName}`;
  return { remote: true, node, baseDir };
}

app.use("/api/dashboard-assistant", createDashboardAssistantRouter({
  db,
  isAuthenticated,
  isAdmin,
  getDashboardAssistantConfig,
  saveDashboardAssistantConfig,
  isDashboardAssistantEnabledForNormalUsers,
  findUserByEmail,
  getAccessListForEmail,
  loadServersIndex,
  loadNodes,
  loadTemplatesFile,
  getEffectivePermsForUserOnServer,
  readEnvFile,
  writeEnvFileBatch,
  issueActionToken,
  requireActionTokenOr403,
  recordActivity,
  findNodeByIdOrName,
  buildNodeBaseUrl,
  nodeAuthHeadersFor,
  httpRequestJson,
  nodeFsPost,
  resolveRemoteFsContext,
  upsertServerIndexEntry,
  createOnRemoteNode,
  isPortInNodeAllocation,
  validateDockerCommandPorts,
  assertSafeRemoteUrl,
  isValidArchiveUrl,
  normalizeTemplateId,
  sanitizeServerName,
  sanitizeDisplayName,
  clampPort,
  clampAppPort,
  deleteServerByName: (name) => deleteServerByName({
    findServer,
    isRemoteEntry,
    findNodeByIdOrName,
    buildNodeBaseUrl,
    nodeAuthHeadersFor,
    httpRequestJson,
    deleteServerSchedules,
    removeServerIndexEntry,
    removeAccessForServerName,
  }, name),
  applyRemoteAssetToServer: (params) => applyRemoteAssetToServer({
    findServer,
    isRemoteEntry,
    findNodeByIdOrName,
    buildNodeBaseUrl,
    nodeAuthHeadersFor,
    httpRequestJson,
    assertSafeRemoteUrl,
    resolveRemoteFsContext,
    nodeFsPost,
    httpGetRaw,
    remoteApplyProxyDownload: REMOTE_APPLY_PROXY_DOWNLOAD,
    remoteApplyMaxBytes: REMOTE_APPLY_MAX_BYTES,
    remoteApplyTimeoutMs: REMOTE_APPLY_TIMEOUT_MS,
    remoteFetchMaxRedirects: REMOTE_FETCH_MAX_REDIRECTS,
  }, params),
}));

const DEFAULT_TEMPLATES = [
  {
    id: "minecraft",
    name: "Minecraft",
    description: "You can create minecraft servers on this platform",
    templateImage: "https://images.unsplash.com/photo-1501446529957-6226bd447c46?auto=format&fit=crop&w=1200&q=80",
    docker: {
      image: "eclipse-temurin",
      tag: "21-jre",
      ports: [],
      env: {},
      volumes: ["{BOT_DIR}:/data"],
      workdir: "/data",
      command: "java -Xms128M -Xmx{RAM_MB}M -jar /data/server.jar nogui",
      restart: "unless-stopped",
    },
  },
  {
    id: "nodejs",
    name: "Node.js",
    description: "Run Node.JS applications",
    templateImage: "https://images.unsplash.com/photo-1518770660439-4636190af475?auto=format&fit=crop&w=1200&q=80",
    docker: {
      image: "node",
      tag: "20-alpine",
      ports: [],
      env: { NODE_ENV: "production" },
      volumes: ["{BOT_DIR}:/app"],
      workdir: "/app",
      command: "node /app/index.js",
      restart: "unless-stopped",
    },
  },
  {
    id: "vanilla",
    name: "Vanilla",
    description: "Choose what platform you want",
    templateImage: "https://images.unsplash.com/photo-1498050108023-c5249f4df085?auto=format&fit=crop&w=1200&q=80",
    docker: { image: "alpine", tag: "latest", ports: [], env: {}, volumes: [], workdir: "/data", command: "sleep 3600", restart: "no" },
  },
];

const DEFAULT_MINECRAFT_PROCESS_IMAGE = "eclipse-temurin";
const DEFAULT_MINECRAFT_PROCESS_TAG = "21-jre";
const DEFAULT_MINECRAFT_PROCESS_COMMAND = "java -Xms128M -Xmx{RAM_MB}M -jar /data/server.jar nogui";

function sanitizeRuntimeCommandInput(value) {
  return String(value ?? "").replace(/\r\n?/g, "\n").trim().slice(0, 4000);
}

function normalizeRuntimeWorkdirInput(value) {
  let workdir = String(value ?? "").replace(/\0/g, "").trim();
  if (!workdir) return "";
  workdir = workdir.replace(/\\/g, "/");
  if (!workdir.startsWith("/")) return "";
  workdir = path.posix.normalize(workdir);
  if (!workdir || workdir === ".") return "";
  if (!workdir.startsWith("/")) workdir = `/${workdir}`;
  return workdir;
}

const RUNTIME_PROCESS_BLOCKED_EXECUTABLES = new Set([
  'sh', '/bin/sh', 'bash', '/bin/bash', 'ash', '/bin/ash', 'dash', '/bin/dash', 'zsh', '/bin/zsh',
]);
const RUNTIME_PROCESS_BLOCKED_CONTAINER_EXECUTABLES = new Set([
  'docker', 'docker-compose', 'podman', 'podman-compose', 'nerdctl', 'ctr',
]);
const RUNTIME_PROCESS_ENV_WRAPPERS = new Set(['env']);
const RUNTIME_PROCESS_BLOCKED_TOKENS = new Set([
  '&&', '||', '|', ';', '&', '>', '>>', '<', '<<', '2>', '2>>', '2>&1', '|&',
]);

function parseRuntimeProcessArgsInput(value) {
  const input = String(value || '');
  const args = [];
  let current = '';
  let quote = '';
  let escape = false;

  for (let i = 0; i < input.length; i += 1) {
    const ch = input[i];
    if (escape) {
      current += ch;
      escape = false;
      continue;
    }
    if (ch === '\\') {
      escape = true;
      continue;
    }
    if (quote) {
      if (ch === quote) quote = '';
      else current += ch;
      continue;
    }
    if (ch === '"' || ch === "'") {
      quote = ch;
      continue;
    }
    if (/\s/.test(ch)) {
      if (current) {
        args.push(current);
        current = '';
      }
      continue;
    }
    current += ch;
  }
  if (current) args.push(current);
  return args;
}

function getRuntimeProcessExecutableBaseNameInput(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  let candidate = raw;
  if (/\s/.test(candidate)) {
    const nestedArgs = parseRuntimeProcessArgsInput(candidate);
    if (nestedArgs.length) candidate = String(nestedArgs[0] || '').trim();
  }
  const normalized = candidate.toLowerCase().replace(/\\/g, '/');
  const parts = normalized.split('/').filter(Boolean);
  return parts.pop() || normalized;
}

function isRuntimeEnvAssignmentTokenInput(value) {
  const token = String(value || '').trim();
  if (!token || token.startsWith('-')) return false;
  const eqIdx = token.indexOf('=');
  if (eqIdx <= 0) return false;
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(token.slice(0, eqIdx));
}

function resolveRuntimeProcessExecutableArgsInput(args) {
  const parts = Array.isArray(args) ? args : [];
  if (!parts.length) return { execName: '', index: -1 };
  let index = 0;
  while (index < parts.length) {
    const token = String(parts[index] || '').trim();
    if (!token) {
      index += 1;
      continue;
    }
    const execName = getRuntimeProcessExecutableBaseNameInput(token);
    if (RUNTIME_PROCESS_ENV_WRAPPERS.has(execName)) {
      index += 1;
      while (index < parts.length) {
        const envToken = String(parts[index] || '').trim();
        if (!envToken) {
          index += 1;
          continue;
        }
        if (envToken === '--') {
          index += 1;
          break;
        }
        if (envToken.startsWith('-') || isRuntimeEnvAssignmentTokenInput(envToken)) {
          index += 1;
          continue;
        }
        break;
      }
      continue;
    }
    return { execName, index };
  }
  return { execName: '', index: -1 };
}

function validateRuntimeContainerLauncherInput(args) {
  const { execName } = resolveRuntimeProcessExecutableArgsInput(args);
  if (RUNTIME_PROCESS_BLOCKED_CONTAINER_EXECUTABLES.has(execName)) {
    return 'Container runtime CLI commands like "docker run" are not allowed here. Configure image, ports, volumes, env, and the in-container process separately.';
  }
  return null;
}

function isBlockedRuntimeShellExecutableInput(value) {
  const execName = getRuntimeProcessExecutableBaseNameInput(value);
  return RUNTIME_PROCESS_BLOCKED_EXECUTABLES.has(execName);
}

function unwrapRuntimeShellWrapperInput(value) {
  const args = parseRuntimeProcessArgsInput(value);
  if (args.length < 3) return null;
  const shellFlags = String(args[1] || '').trim().toLowerCase();
  if (!isBlockedRuntimeShellExecutableInput(args[0])) return null;
  if (!shellFlags.startsWith('-') || !shellFlags.includes('c')) return null;
  return String(args[2] || '').trim() || null;
}

function normalizeLegacyRuntimeProcessCommandInput(templateId, value) {
  let command = sanitizeRuntimeCommandInput(value);
  if (!command) return '';
  for (let i = 0; i < 2; i += 1) {
    const unwrapped = unwrapRuntimeShellWrapperInput(command);
    if (!unwrapped) break;
    command = sanitizeRuntimeCommandInput(unwrapped);
  }
  const normalizedTemplateId = normalizeTemplateId(templateId);
  if (normalizedTemplateId === 'nodejs' || normalizedTemplateId === 'discord-bot') {
    const lower = command.toLowerCase();
    const nodeIdx = lower.lastIndexOf('node ');
    if (nodeIdx >= 0 && (lower.includes('&&') || lower.startsWith('npm ') || lower.startsWith('cd '))) {
      command = sanitizeRuntimeCommandInput(command.slice(nodeIdx));
    }
  }
  return command;
}

function validateRuntimeProcessCommandInput(value) {
  const command = sanitizeRuntimeCommandInput(value);
  if (!command) return null;
  if (command.length > 4000) {
    return 'Process command is too long.';
  }
  if (command.includes('\n')) {
    return 'Process command must be a single line.';
  }
  if (command.includes('`') || command.includes('$(')) {
    return 'Shell expansions are not allowed in process commands.';
  }
  const args = parseRuntimeProcessArgsInput(command);
  if (!args.length) {
    return 'Process command is empty.';
  }
  const { execName: resolvedExecName } = resolveRuntimeProcessExecutableArgsInput(args);
  const containerRuntimeError = validateRuntimeContainerLauncherInput(args);
  if (containerRuntimeError) {
    return containerRuntimeError;
  }
  if (isBlockedRuntimeShellExecutableInput(resolvedExecName || args[0])) {
    return 'Shell wrappers like sh -c or bash -lc are not allowed. Provide the executable and arguments directly.';
  }
  for (const arg of args) {
    const token = String(arg || '').trim();
    if (RUNTIME_PROCESS_BLOCKED_TOKENS.has(token)) {
      return `Shell control operator "${token}" is not allowed in process commands.`;
    }
  }
  return null;
}

function looksLikeWrapperEntrypointOnlyCommand(value, entrypoint = null, cmd = null) {
  const args = parseRuntimeProcessArgsInput(value);
  if (args.length !== 1) return false;

  const token = String(args[0] || '').trim().toLowerCase();
  if (!token.startsWith('/')) return false;

  const baseName = token.split('/').filter(Boolean).pop() || token;
  const wrapperLike = baseName.includes('entrypoint');
  if (!wrapperLike) return false;

  const epList = Array.isArray(entrypoint)
    ? entrypoint.map((item) => String(item || '').trim()).filter(Boolean)
    : [];
  const cmdList = Array.isArray(cmd)
    ? cmd.map((item) => String(item || '').trim()).filter(Boolean)
    : [];

  if (cmdList.length > 0) return false;
  if (epList.length > 1) return false;
  return true;
}

function looksLikePathOnlyScriptCommand(value) {
  const args = parseRuntimeProcessArgsInput(value);
  if (args.length !== 1) return false;
  const token = String(args[0] || '').trim().toLowerCase();
  if (!token.startsWith('/')) return false;
  if (token.includes('entrypoint')) return true;
  if (token.endsWith('.sh') || token.endsWith('.bash')) return true;
  return false;
}

function defaultRuntimeCommandForTemplate(templateId, dockerConfig = null, startFile = null) {
  const normalized = normalizeTemplateId(templateId);
  const explicit = normalizeLegacyRuntimeProcessCommandInput(normalized, dockerConfig?.command);
  if (explicit) return explicit;
  const start = String(startFile || defaultStartFileForTemplate(normalized) || "").trim();
  const configuredWorkdir = normalizeRuntimeWorkdirInput(
    dockerConfig?.workdir || dockerConfig?.workingDir || dockerConfig?.working_dir
  );
  const appDir = configuredWorkdir || "/app";
  if (normalized === "minecraft") {
    return DEFAULT_MINECRAFT_PROCESS_COMMAND;
  }
  if (normalized === "python") {
    return `python ${appDir}/${start || "main.py"}`;
  }
  if (normalized === "nodejs" || normalized === "discord-bot") {
    return `node ${appDir}/${start || "index.js"}`;
  }
  return "";
}

function runtimeDataDirForTemplate(templateId, dockerConfig = null) {
  const explicitWorkdir = normalizeRuntimeWorkdirInput(
    dockerConfig?.workdir || dockerConfig?.workingDir || dockerConfig?.working_dir
  );
  if (explicitWorkdir) return explicitWorkdir;

  const normalized = normalizeTemplateId(templateId);
  const volumes = Array.isArray(dockerConfig?.volumes) ? dockerConfig.volumes : [];
  for (const volumeRaw of volumes) {
    const volume = String(volumeRaw || "").trim();
    if (!volume || !volume.includes(':')) continue;
    const parts = volume.split(':');
    if (parts.length < 2) continue;
    let containerPath = String(parts[1] || "").trim();
    if (!containerPath) continue;
    const optionsIdx = containerPath.lastIndexOf(':');
    if (optionsIdx > 0) containerPath = containerPath.slice(0, optionsIdx).trim();
    if (containerPath.startsWith('/')) return containerPath;
  }
  if (normalized === 'python' || normalized === 'nodejs' || normalized === 'discord-bot' || normalized === 'runtime') {
    return '/app';
  }
  return '/data';
}

function defaultTemplateBotDirVolumeForWorkdir(workdir) {
  const normalizedWorkdir = normalizeRuntimeWorkdirInput(workdir) || '/data';
  return `{BOT_DIR}:${normalizedWorkdir}`;
}

function shouldAutoAlignBotDirVolume(volumes) {
  if (!Array.isArray(volumes)) return true;
  const cleaned = volumes.map((v) => String(v || '').trim()).filter(Boolean);
  if (cleaned.length === 0) return true;
  if (cleaned.length !== 1) return false;
  return cleaned[0].startsWith('{BOT_DIR}:');
}

function normalizeTemplateDockerForRuntime(templateId, dockerConfig = null, startFile = null) {
  const normalized = normalizeTemplateId(templateId);
  let docker = sanitizeDockerTemplatePayload(dockerConfig || {}) || {};
  docker = {
    image: String(docker.image || "").trim(),
    tag: String(docker.tag || "").trim(),
    ports: Array.isArray(docker.ports) ? docker.ports.slice() : [],
    volumes: Array.isArray(docker.volumes) ? docker.volumes.slice() : [],
    workdir: normalizeRuntimeWorkdirInput(docker.workdir || docker.workingDir || docker.working_dir),
    env: docker.env && typeof docker.env === "object" && !Array.isArray(docker.env) ? { ...docker.env } : {},
    command: normalizeLegacyRuntimeProcessCommandInput(normalized, docker.command),
    restart: String(docker.restart || "unless-stopped").trim() || "unless-stopped",
    console: docker.console || { type: "stdin" },
  };
  if (!docker.workdir) {
    docker.workdir = runtimeDataDirForTemplate(normalized, docker);
  }
  if (!docker.volumes.length) {
    docker.volumes = [defaultTemplateBotDirVolumeForWorkdir(docker.workdir)];
  } else if (docker.workdir && shouldAutoAlignBotDirVolume(docker.volumes)) {
    docker.volumes = [defaultTemplateBotDirVolumeForWorkdir(docker.workdir)];
  }
  if (docker.command && validateRuntimeProcessCommandInput(docker.command)) {
    docker.command = "";
  }

  if (normalized === "minecraft") {
    docker.image = DEFAULT_MINECRAFT_PROCESS_IMAGE;
    docker.tag = DEFAULT_MINECRAFT_PROCESS_TAG;
    docker.volumes = docker.volumes.length ? docker.volumes : ["{BOT_DIR}:/data"];
    docker.workdir = docker.workdir || "/data";
    docker.env = {};
    docker.command = docker.command || defaultRuntimeCommandForTemplate(normalized, docker, startFile);
    return docker;
  }

  docker.command = docker.command || defaultRuntimeCommandForTemplate(normalized, docker, startFile);
  return docker;
}

function normalizeTemplateDefinition(template) {
  if (!template || typeof template !== "object") return template;
  const normalizedId = normalizeTemplateId(template.id);
  if (!template.docker || !normalizedId) return template;
  return {
    ...template,
    docker: normalizeTemplateDockerForRuntime(normalizedId, template.docker, template.start || defaultStartFileForTemplate(normalizedId)),
  };
}

function ensureTemplatesFile() {
  try {
    if (!fs.existsSync(TEMPLATES_FILE)) {
      fs.writeFileSync(TEMPLATES_FILE, JSON.stringify(DEFAULT_TEMPLATES, null, 2), "utf8");
    }
  } catch { }
}

let _templatesCache = null;
let _templatesCacheTs = 0;
const TEMPLATES_CACHE_TTL_MS = 30000;

function loadTemplatesFile() {
  const now = Date.now();
  if (_templatesCache && (now - _templatesCacheTs) < TEMPLATES_CACHE_TTL_MS) return _templatesCache;
  try {
    ensureTemplatesFile();
    const raw = fs.readFileSync(TEMPLATES_FILE, "utf8");
    if (!raw.trim()) {
      _templatesCache = DEFAULT_TEMPLATES;
      _templatesCacheTs = now;
      return DEFAULT_TEMPLATES;
    }
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      _templatesCache = parsed.map((template) => normalizeTemplateDefinition(template));
      _templatesCacheTs = now;
      return _templatesCache;
    }
  } catch { }
  _templatesCache = DEFAULT_TEMPLATES.map((template) => normalizeTemplateDefinition(template));
  _templatesCacheTs = now;
  return _templatesCache;
}

function saveTemplatesFile(list) {
  try {
    fs.writeFileSync(TEMPLATES_FILE, JSON.stringify(list, null, 2), "utf8");
    _templatesCache = null;
    _templatesCacheTs = 0;
    return true;
  } catch {
    return false;
  }
}

function findTemplateById(id) {
  const key = normalizeTemplateId(id);
  const fromFile = loadTemplatesFile().find(t => normalizeTemplateId(t?.id) === key);
  if (fromFile) return fromFile;
  const fromDefaults = DEFAULT_TEMPLATES.find(t => normalizeTemplateId(t?.id) === key);
  return fromDefaults ? normalizeTemplateDefinition(fromDefaults) : null;
}

function defaultStartFileForTemplate(templateId) {
  const normalized = normalizeTemplateId(templateId);
  if (normalized === "minecraft") return "server.jar";
  if (normalized === "nodejs" || normalized === "discord-bot") return "index.js";
  if (normalized === "python") return "main.py";
  return null;
}

ensureTemplatesFile();

let versionsConfig = { providers: [] };
try {
  const rawVersions = fs.readFileSync(versionsPath, "utf8");
  versionsConfig = JSON.parse(rawVersions);
} catch { }

function providerTemplates(provider) {
  if (!provider) return [];
  const base = [];
  if (provider.templates && Array.isArray(provider.templates)) base.push(...provider.templates.map(normalizeTemplateId));
  else if (provider.template) base.push(normalizeTemplateId(provider.template));
  else base.push("minecraft");
  if (base.includes("discord-bot")) ["nodejs", "python"].forEach(a => { if (!base.includes(a)) base.push(a); });
  return base;
}

function providerSupportsTemplate(provider, tpl) {
  const normalized = normalizeTemplateId(tpl);
  if (!normalized) return true;
  return providerTemplates(provider).includes(normalized);
}

function providersForTemplate(tpl) {
  const providers = Array.isArray(versionsConfig) ? versionsConfig : (versionsConfig.providers || []);
  return providers.filter(p => providerSupportsTemplate(p, tpl));
}

function findProviderConfig(providerId) {
  const providers = Array.isArray(versionsConfig) ? versionsConfig : (versionsConfig.providers || []);
  return providers.find(p => String(p.id) === String(providerId)) || null;
}

function findProviderVersionConfig(providerId, versionId) {
  const provider = findProviderConfig(providerId);
  if (!provider) return null;
  const versions = Array.isArray(provider.versions) ? provider.versions : [];
  return versions.find(v => String(v.id || v.name || v.version) === String(versionId)) || null;
}

function sanitizePythonVersionTag(version) {
  const raw = (version || "").toString().trim();
  if (!raw) return null;
  const clean = raw.replace(/^v/, "");
  if (!/^[A-Za-z0-9._-]+$/.test(clean)) return null;
  return clean;
}

function sanitizeNodeVersionTag(version) {
  const raw = (version || "").toString().trim();
  if (!raw) return null;
  const clean = raw.replace(/^v/, "");
  const len = clean.length;
  let i = 0;
  const isDigit = (ch) => ch >= 48 && ch <= 57;
  const isAlpha = (ch) => (ch >= 65 && ch <= 90) || (ch >= 97 && ch <= 122);
  const readDigits = () => {
    const start = i;
    while (i < len && isDigit(clean.charCodeAt(i))) i += 1;
    return i > start;
  };

  if (!readDigits()) return null;
  if (i >= len || clean[i] !== ".") return null;
  i += 1;
  if (!readDigits()) return null;
  if (i < len && clean[i] === ".") {
    i += 1;
    if (!readDigits()) return null;
  }

  for (; i < len; i += 1) {
    const ch = clean.charCodeAt(i);
    if (isDigit(ch) || isAlpha(ch) || ch === 45 || ch === 46) continue;
    return null;
  }
  return clean;
}

function inferPythonStart(entry) {
  const candidates = [entry?.start, "main.py"];
  for (const c of candidates) {
    if (!c) continue;
    const s = String(c).trim();
    if (s.toLowerCase().endsWith(".py") && /^[A-Za-z0-9._\/-]+$/.test(s) && !s.includes("..") && !s.startsWith("/")) return s;
  }
  return "main.py";
}

function inferNodeStart(entry) {
  const candidates = [entry?.start, "index.js"];
  for (const c of candidates) {
    if (!c) continue;
    const s = String(c).trim();
    if (s.toLowerCase().endsWith(".js") && /^[A-Za-z0-9._\/-]+$/.test(s) && !s.includes("..") && !s.startsWith("/")) return s;
  }
  return "index.js";
}

function buildPythonVersionConfig(versionId, entry) {
  const clean = sanitizePythonVersionTag(versionId);
  if (!clean) return null;
  const startFile = inferPythonStart(entry);
  return {
    id: versionId,
    name: clean,
    label: `Python ${clean}`,
    start: startFile,
    docker: { image: "python", tag: `${clean}-slim`, command: `python /app/${startFile}` },
  };
}

function buildNodeVersionConfig(versionId, entry) {
  const clean = sanitizeNodeVersionTag(versionId);
  if (!clean) return null;
  const startFile = inferNodeStart(entry);
  return {
    id: versionId,
    name: clean,
    label: `Node.js ${clean}`,
    start: startFile,
    docker: { image: "node", tag: `${clean}-alpine`, command: `node /app/${startFile}` },
  };
}

const REMOTE_FETCH_MAX_BYTES = parseInt(process.env.REMOTE_FETCH_MAX_BYTES || "", 10) || 25 * 1024 * 1024;
const REMOTE_FETCH_TIMEOUT_MS = parseInt(process.env.REMOTE_FETCH_TIMEOUT_MS || "", 10) || 20000;
const REMOTE_FETCH_MAX_REDIRECTS = parseInt(process.env.REMOTE_FETCH_MAX_REDIRECTS || "", 10) || 5;
const REMOTE_FETCH_ALLOW_ANY_PORT = process.env.REMOTE_FETCH_ALLOW_ANY_PORT === "1";
const REMOTE_APPLY_PROXY_DOWNLOAD = parseBoolean(process.env.REMOTE_APPLY_PROXY_DOWNLOAD, true);
const REMOTE_APPLY_MAX_BYTES = parseInt(process.env.REMOTE_APPLY_MAX_BYTES || "", 10) || 1024 * 1024 * 1024;
const REMOTE_APPLY_TIMEOUT_MS = parseInt(process.env.REMOTE_APPLY_TIMEOUT_MS || "", 10) || 300000;

function ipv4ToInt(ip) {
  const parts = ip.split(".");
  if (parts.length !== 4) return null;
  let n = 0;
  for (const p of parts) {
    const v = Number(p);
    if (!Number.isInteger(v) || v < 0 || v > 255) return null;
    n = (n << 8) + v;
  }
  return n >>> 0;
}

function isPrivateIPv4(ip) {
  const n = ipv4ToInt(ip);
  if (n === null) return true;
  const ranges = [
    [ipv4ToInt("0.0.0.0"), ipv4ToInt("0.255.255.255")],
    [ipv4ToInt("10.0.0.0"), ipv4ToInt("10.255.255.255")],
    [ipv4ToInt("100.64.0.0"), ipv4ToInt("100.127.255.255")],
    [ipv4ToInt("127.0.0.0"), ipv4ToInt("127.255.255.255")],
    [ipv4ToInt("169.254.0.0"), ipv4ToInt("169.254.255.255")],
    [ipv4ToInt("172.16.0.0"), ipv4ToInt("172.31.255.255")],
    [ipv4ToInt("192.168.0.0"), ipv4ToInt("192.168.255.255")],
    [ipv4ToInt("192.0.2.0"), ipv4ToInt("192.0.2.255")],
    [ipv4ToInt("198.18.0.0"), ipv4ToInt("198.19.255.255")],
    [ipv4ToInt("198.51.100.0"), ipv4ToInt("198.51.100.255")],
    [ipv4ToInt("203.0.113.0"), ipv4ToInt("203.0.113.255")],
    [ipv4ToInt("224.0.0.0"), ipv4ToInt("255.255.255.255")],
  ];
  return ranges.some(([a, b]) => n >= a && n <= b);
}

function isPrivateIPv6(ip) {
  const v = String(ip || "").toLowerCase();
  if (v === "::" || v === "::1") return true;
  if (v.startsWith("fc") || v.startsWith("fd")) return true;
  if (v.startsWith("fe8") || v.startsWith("fe9") || v.startsWith("fea") || v.startsWith("feb")) return true;
  if (v.startsWith("::ffff:")) {
    const tail = v.slice("::ffff:".length);
    if (net.isIP(tail) === 4) return isPrivateIPv4(tail);
  }
  return false;
}

function isPrivateAddress(ip) {
  const t = net.isIP(ip);
  if (t === 4) return isPrivateIPv4(ip);
  if (t === 6) return isPrivateIPv6(ip);
  return true;
}

async function assertSafeRemoteUrl(inputUrl) {
  let u;
  try {
    u = new URL(String(inputUrl));
  } catch {
    throw new Error("Invalid URL");
  }

  const proto = (u.protocol || "").toLowerCase();
  if (proto !== "http:" && proto !== "https:") throw new Error("Only http(s) URLs are allowed");
  if (u.username || u.password) throw new Error("Credentials in URL are not allowed");

  const hostname = u.hostname;
  if (!hostname) throw new Error("Invalid hostname");

  const hostLower = hostname.toLowerCase();
  if (hostLower === "localhost" || hostLower.endsWith(".localhost") || hostLower.endsWith(".local")) {
    throw new Error("Localhost URLs are not allowed");
  }

  const port = u.port ? Number(u.port) : (u.protocol === "https:" ? 443 : 80);
  if (!REMOTE_FETCH_ALLOW_ANY_PORT && port !== 80 && port !== 443) throw new Error("Only ports 80/443 are allowed");

  if (net.isIP(hostname)) {
    if (isPrivateAddress(hostname)) throw new Error("Private IP is not allowed");
  } else {
    const answers = await dns.lookup(hostname, { all: true, verbatim: true });
    if (!answers || answers.length === 0) throw new Error("DNS lookup failed");
    for (const a of answers) {
      if (a && a.address && isPrivateAddress(a.address)) throw new Error("Hostname resolves to a private/internal IP");
    }
  }

  return u;
}

function safeLookup(hostname, options, cb) {
  dns.lookup(hostname, { all: true, verbatim: true })
    .then((answers) => {
      if (!answers || answers.length === 0) return cb(new Error("DNS lookup failed"));
      for (const a of answers) {
        if (a && a.address && !isPrivateAddress(a.address)) return cb(null, a.address, a.family || net.isIP(a.address));
      }
      return cb(new Error("Hostname resolves to a private/internal IP"));
    })
    .catch((err) => cb(err));
}


const SSRF_SAFE_AGENTS = new Map();

setInterval(() => {
  try {
    const now = Date.now();
    const AGENT_TTL = 60 * 60 * 1000;
    for (const [key, agent] of SSRF_SAFE_AGENTS.entries()) {
      if (agent._lastUsed && (now - agent._lastUsed > AGENT_TTL)) {
        try { agent.destroy(); } catch { }
        SSRF_SAFE_AGENTS.delete(key);
      } else if (!agent._lastUsed) {
        agent._lastUsed = now;
      }
    }
  } catch (err) { console.debug("[ssrf-agents] sweep error:", err.message); }
}, 10 * 60 * 1000).unref();

function createSsrfSafeAgent(resolvedIp, protocol, originalHostname) {
  const key = `${protocol}://${resolvedIp}`;
  if (SSRF_SAFE_AGENTS.has(key)) {
    const agent = SSRF_SAFE_AGENTS.get(key);
    agent._lastUsed = Date.now();
    return agent;
  }

  const AgentClass = protocol === 'https:' ? https.Agent : httpMod.Agent;
  const agent = new AgentClass({
    lookup: (hostname, options, callback) => {
      const family = net.isIP(resolvedIp);
      callback(null, resolvedIp, family || 4);
    },
    keepAlive: false,
    maxSockets: 1,
  });

  agent._lastUsed = Date.now();
  if (SSRF_SAFE_AGENTS.size >= 500) {
    const oldestKey = SSRF_SAFE_AGENTS.keys().next().value;
    if (oldestKey !== undefined) { try { SSRF_SAFE_AGENTS.get(oldestKey).destroy(); } catch {} SSRF_SAFE_AGENTS.delete(oldestKey); }
  }
  SSRF_SAFE_AGENTS.set(key, agent);
  return agent;
}

async function assertSafeRemoteUrlWithResolvedIp(inputUrl) {
  let u;
  try {
    u = new URL(String(inputUrl));
  } catch {
    throw new Error("Invalid URL");
  }

  const proto = (u.protocol || "").toLowerCase();
  if (proto !== "http:" && proto !== "https:") throw new Error("Only http(s) URLs are allowed");
  if (u.username || u.password) throw new Error("Credentials in URL are not allowed");

  const hostname = u.hostname;
  if (!hostname) throw new Error("Invalid hostname");

  const hostLower = hostname.toLowerCase();
  if (hostLower === "localhost" || hostLower.endsWith(".localhost") || hostLower.endsWith(".local")) {
    throw new Error("Localhost URLs are not allowed");
  }

  const port = u.port ? Number(u.port) : (u.protocol === "https:" ? 443 : 80);
  if (!REMOTE_FETCH_ALLOW_ANY_PORT && port !== 80 && port !== 443) throw new Error("Only ports 80/443 are allowed");

  let resolvedIp = null;

  if (net.isIP(hostname)) {
    if (isPrivateAddress(hostname)) throw new Error("Private IP is not allowed");
    resolvedIp = hostname;
  } else {
    const answers = await dns.lookup(hostname, { all: true, verbatim: true });
    if (!answers || answers.length === 0) throw new Error("DNS lookup failed");

    for (const a of answers) {
      if (a && a.address && !isPrivateAddress(a.address)) {
        resolvedIp = a.address;
        break;
      }
    }

    if (!resolvedIp) throw new Error("Hostname resolves to a private/internal IP");

    for (const a of answers) {
      if (a && a.address && isPrivateAddress(a.address)) {
        throw new Error("Hostname resolves to a private/internal IP");
      }
    }
  }

  return { url: u, resolvedIp, originalHostname: hostname };
}

function httpGetRaw(url, opts = {}) {
  const maxBytes = Number.isFinite(opts.maxBytes) ? opts.maxBytes : REMOTE_FETCH_MAX_BYTES;
  const timeoutMs = Number.isFinite(opts.timeoutMs) ? opts.timeoutMs : REMOTE_FETCH_TIMEOUT_MS;
  const maxRedirects = Number.isFinite(opts.maxRedirects) ? opts.maxRedirects : REMOTE_FETCH_MAX_REDIRECTS;
  const redirects = Number.isFinite(opts._redirects) ? opts._redirects : 0;

  return new Promise(async (resolve, reject) => {
    let validated;
    try {
      validated = await assertSafeRemoteUrlWithResolvedIp(url);
    } catch (e) {
      return reject(e);
    }

    const { url: u, resolvedIp, originalHostname } = validated;
    const lib = u.protocol === "https:" ? https : httpMod;

    const agent = createSsrfSafeAgent(resolvedIp, u.protocol, originalHostname);

    const requestOptions = {
      method: "GET",
      hostname: resolvedIp,
      port: u.port || (u.protocol === "https:" ? 443 : 80),
      path: u.pathname + u.search,
      headers: {
        "User-Agent": "ADPanel/1.0",
        "Accept": "*/*",
        "Host": originalHostname,
      },
      agent: agent,
      timeout: timeoutMs,
      ...(u.protocol === "https:" ? { servername: originalHostname } : {}),
    };

    const req = lib.request(requestOptions, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        if (redirects >= maxRedirects) {
          res.resume();
          return reject(new Error("Too many redirects"));
        }
        const nextUrl = new URL(res.headers.location, u).toString();
        res.resume();
        return resolve(httpGetRaw(nextUrl, { maxBytes, timeoutMs, maxRedirects, _redirects: redirects + 1 }));
      }

      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode}`));
      }

      const chunks = [];
      let total = 0;

      res.on("data", (chunk) => {
        total += chunk.length;
        if (total > maxBytes) {
          req.destroy(new Error("Download too large"));
          return;
        }
        chunks.push(chunk);
      });

      res.on("end", () => resolve(Buffer.concat(chunks)));
    });

    req.on("error", reject);
    req.on("timeout", () => {
      req.destroy(new Error("Request timeout"));
    });
    req.end();
  });
}

function httpRequestJson(fullUrl, method = "GET", headers = {}, body = null, timeoutMs = 15000) {
  return new Promise(async (resolve) => {
    let settled = false;
    const finish = (out) => {
      if (settled) return;
      settled = true;
      try { clearTimeout(hardTimer); } catch { }
      resolve(out);
    };

    let hardTimer = null;

    try {
      const isInternalNodeRequest = headers && (headers["X-Node-Token"] || headers["Authorization"]);

      let requestOptions;
      let lib;

      if (isInternalNodeRequest) {
        const isHttps = fullUrl.startsWith("https:");
        lib = isHttps ? https : httpMod;
        requestOptions = { method, headers };
        if (isHttps && nodeMtlsAgent) requestOptions.agent = nodeMtlsAgent;
      } else {
        let validated;
        try {
          validated = await assertSafeRemoteUrlWithResolvedIp(fullUrl);
        } catch (e) {
          return finish({ status: 0, json: null });
        }

        const { url: u, resolvedIp, originalHostname } = validated;
        lib = u.protocol === "https:" ? https : httpMod;

        const agent = createSsrfSafeAgent(resolvedIp, u.protocol, originalHostname);

        requestOptions = {
          method,
          hostname: resolvedIp,
          port: u.port || (u.protocol === "https:" ? 443 : 80),
          path: u.pathname + u.search,
          headers: {
            ...headers,
            "Host": originalHostname,
          },
          agent: agent,
          ...(u.protocol === "https:" ? { servername: originalHostname } : {}),
        };

        fullUrl = null;
      }

      const req = fullUrl
        ? lib.request(fullUrl, requestOptions, handleResponse)
        : lib.request(requestOptions, handleResponse);

      const MAX_RESPONSE_BYTES = 5 * 1024 * 1024;
      function handleResponse(res) {
        const statusCode = res.statusCode || 0;
        const chunks = [];
        let totalBytes = 0;

        res.on("data", (d) => {
          totalBytes += d.length;
          if (totalBytes > MAX_RESPONSE_BYTES) {
            try { res.destroy(); } catch { }
            return finish({ status: 0, json: null });
          }
          chunks.push(d);
        });
        res.on("end", () => {
          const bodyStr = Buffer.concat(chunks).toString("utf8");
          try {
            const json = bodyStr ? JSON.parse(bodyStr) : null;
            finish({ status: statusCode, json });
          } catch {
            finish({ status: statusCode, json: null });
          }
        });
      }

      hardTimer = setTimeout(() => {
        try { req.destroy(); } catch { }
        finish({ status: 0, json: null });
      }, timeoutMs);

      req.on("timeout", () => {
        try { req.destroy(); } catch { }
        finish({ status: 0, json: null });
      });

      req.on("error", () => finish({ status: 0, json: null }));

      req.setTimeout(timeoutMs);

      if (body != null) req.write(typeof body === "string" ? body : JSON.stringify(body));
      req.end();
    } catch {
      finish({ status: 0, json: null });
    }
  });
}

async function fetchJson(u) {
  const buf = await httpGetRaw(u);
  return JSON.parse(buf.toString("utf8"));
}

app.post("/api/captcha/local", (req, res) => {
  if (!LOCAL_CAPTCHA_ENABLED) return res.status(400).json({ ok: false, error: "not-enabled" });
  if (!req.session || !req.session.localCaptcha) return res.status(400).json({ ok: false, error: "no-captcha" });

  const action = String(req.body?.action || "").toLowerCase();
  const token = req.body?.token || req.body?.localCaptchaToken;
  const rawSelection = req.body?.selection || req.body?.captchaSelection;

  let selection = [];
  if (Array.isArray(rawSelection)) selection = rawSelection.map(Number);
  else if (typeof rawSelection === "string") {
    selection = rawSelection
      .split(",")
      .map(s => Number(s.trim()))
      .filter(Number.isInteger);
  }

  if (action === "skip") {
    const r = skipLocalCaptcha(req);
    if (!r.ok) {
      if (r.error === "no-skips-left") return res.status(429).json({ ok: false, error: "no-skips-left" });
      if (r.error === "not-enough-answers") return res.status(400).json({ ok: false, error: "not-enough-answers" });
      return res.status(400).json({ ok: false, error: r.error || "skip-failed" });
    }
    if (r.done) {
      return req.session.save(() => res.json({ ok: true, done: true }));
    }
    const nextQ = getCurrentCaptchaQuestion(req);
    return res.json({ ok: true, done: false, question: nextQ, remainingSkips: r.remainingSkips });
  }

  const r = validateLocalCaptcha(req, selection, token);
  if (!r.ok) {
    if (r.fail) return res.status(400).json({ ok: false, error: "too-many-wrong" });
    if (r.expired) return res.status(410).json({ ok: false, error: "expired" });
    if (r.error === "not-enough-answers") return res.status(400).json({ ok: false, error: "not-enough-answers" });
    return res.status(400).json({ ok: false, error: "invalid" });
  }
  if (r.done) {
    return req.session.save(() => res.json({ ok: true, done: true }));
  }

  const nextQ = getCurrentCaptchaQuestion(req);
  return res.json({ ok: true, done: false, question: nextQ, remainingSkips: r.remainingSkips });
});

app.post("/api/captcha/verify", async (req, res) => {
  if (!req.session) return res.status(400).json({ ok: false });
  const token = req.body?.token || req.body?.["g-recaptcha-response"];
  if (!token) return res.status(400).json({ ok: false, error: "missing-token" });
  if (!EXTERNAL_CAPTCHA_ENABLED) return res.status(400).json({ ok: false, error: "not-enabled" });

  const clientIp = getRequestIp(req);
  const result = await verifyExternalCaptcha(token, clientIp);
  if (!result.ok) return res.status(400).json({ ok: false, error: "verification-failed" });

  req.session.captchaSolved = true;
  req.session.captchaRequired = false;
  clearLocalCaptcha(req);
  return req.session.save(() => res.json({ ok: true }));
});

async function fetchPythonTagsFromGitHub() {
  return await fetchJson("https://api.github.com/repos/python/cpython/tags");
}

function mapPythonTagsToVersions(tags) {
  const list = Array.isArray(tags) ? tags : [];
  return list.map(tag => {
    const raw = (tag && tag.name) ? String(tag.name) : "";
    const clean = sanitizePythonVersionTag(raw) || raw;
    return { id: raw, name: clean, label: `Python ${clean || raw || "unknown"}`, releaseDate: "", tags: ["PYTHON"] };
  });
}

async function fetchNodeVersionsIndex() {
  return await fetchJson("https://nodejs.org/dist/index.json");
}

function mapNodeVersionsToList(list) {
  const arr = Array.isArray(list) ? list : [];
  return arr
    .filter(v => v && typeof v.version === "string" && /^v?\d+\.\d+\.\d+/.test(v.version))
    .map(v => {
      const clean = sanitizeNodeVersionTag(v.version) || v.version;
      const tags = [];
      if (v.lts) tags.push("LTS");
      if (!v.lts) tags.push("LATEST");
      return { id: v.version, name: clean, label: `Node.js ${clean || v.version}`, releaseDate: v.date || "", tags };
    });
}

const REDIRECT_ALLOWED_HOSTS = new Set(
  (process.env.REDIRECT_ALLOWED_HOSTS || "")
    .split(",").map(s => s.trim().toLowerCase()).filter(Boolean)
);

app.use((req, res, next) => {
  if (httpsAvailable && FORCE_HTTPS && !req.secure) {
    const headerHost = req.headers.host || "";
    const requestedHostname = extractHostnameFromHeader(headerHost).toLowerCase();
    const hostname = (requestedHostname && (requestedHostname === HOST.toLowerCase() || REDIRECT_ALLOWED_HOSTS.has(requestedHostname)))
      ? requestedHostname
      : HOST;
    const portSegment = HTTPS_PORT === 443 ? "" : `:${HTTPS_PORT}`;
    return res.redirect(301, `https://${hostname}${portSegment}${req.originalUrl}`);
  }
  next();
});

function getBearerToken(req) {
  const authHeader = req.get("authorization") || "";
  if (!authHeader) return "";
  if (authHeader.toLowerCase().startsWith("bearer ")) return authHeader.slice(7).trim();
  return "";
}

const ALLOW_QUERY_TOKENS = parseBoolean(process.env.ALLOW_QUERY_TOKENS, false);
if (ALLOW_QUERY_TOKENS) console.warn("[SECURITY] ALLOW_QUERY_TOKENS is enabled — tokens may leak via URL query strings and logs");
if (ALLOW_QUERY_TOKENS && process.env.NODE_ENV === "production")
  console.error("[SECURITY] ALLOW_QUERY_TOKENS is ON in production — tokens may leak via URL query strings and referrer headers");

function getNodeTokenFromRequest(req) {
  const headerToken = req.get("x-node-token") || "";
  const queryToken = ALLOW_QUERY_TOKENS ? (req.query?.token || "") : "";
  const bodyToken = ALLOW_QUERY_TOKENS ? (req.body?.token || "") : "";
  return (getBearerToken(req) || headerToken || queryToken || bodyToken || "").toString().trim();
}

function getBootstrapTokenFromRequest(req) {
  const queryBootstrap = ALLOW_QUERY_TOKENS ? (req.query?.bootstrap || "") : "";
  return (req.get("x-node-bootstrap") || queryBootstrap || "").toString().trim();
}

function isAllowedNodeToken(token, node) {
  if (!token) return false;
  if (NODE_TOKEN && safeCompare(String(token), String(NODE_TOKEN))) return true;
  if (LOCAL_NODE_TOKEN && safeCompare(String(token), String(LOCAL_NODE_TOKEN))) return true;
  if (node) {
    if (node.token && safeCompare(String(token), String(node.token))) return true;
    if (node.secret && safeCompare(String(token), String(node.secret))) return true;
    if (node.api_key && safeCompare(String(token), String(node.api_key))) return true;
  }
  return false;
}

async function isOpenNodeRoute(req) {
  const p = req.path || "";
  const match = /^\/api\/nodes\/([^/]+)\/(heartbeat|config\.yml)$/.exec(p);
  if (!match) return false;

  const nodeId = match[1];
  const isConfigYml = match[2] === "config.yml";
  if (isConfigYml && !isRequestSecure(req) && !ALLOW_INSECURE_NODE_BOOTSTRAP) {
    return false;
  }

  if (isConfigYml && NODE_BOOTSTRAP_TOKEN) {
    const bootstrapToken = getBootstrapTokenFromRequest(req);
    if (!safeCompare(String(bootstrapToken), String(NODE_BOOTSTRAP_TOKEN))) return false;
    return true;
  }

  const node = await findNodeByIdOrName(nodeId);
  const token = getNodeTokenFromRequest(req);
  return isAllowedNodeToken(token, node);
}

const HTML_NO_STORE_CACHE_CONTROL = "no-store, no-cache, must-revalidate, private";

// Prevent browser/proxy caching of HTML documents and auth redirects while
// leaving long-lived caching intact for static assets.
app.use((req, res, next) => {
  if (req._staticAsset || req.path.startsWith("/api/")) return next();

  const method = String(req.method || "").toUpperCase();
  if (method !== "GET" && method !== "HEAD") return next();

  const accept = String(req.get("accept") || "").toLowerCase();
  const pathName = String(req.path || "");
  const looksLikeDocument = !/\.[a-z0-9]+$/i.test(pathName);
  const wantsHtml =
    accept.includes("text/html") ||
    accept.includes("application/xhtml+xml") ||
    accept.includes("*/*");

  if (looksLikeDocument && wantsHtml) {
    res.setHeader("Cache-Control", HTML_NO_STORE_CACHE_CONTROL);
    res.setHeader("Pragma", "no-cache");
    res.append("Vary", "Cookie");
    res.append("Vary", "Accept");
  }

  return next();
});

// Prevent CDN/proxy caching of authenticated API responses
app.use((req, res, next) => {
  if (req.path.startsWith("/api/")) {
    res.setHeader("Cache-Control", HTML_NO_STORE_CACHE_CONTROL);
    res.setHeader("Pragma", "no-cache");
    res.setHeader("Vary", "Cookie");
  }
  next();
});

app.get(["/healthz", "/healtz"], (req, res) => {
  res.setHeader("Cache-Control", "no-store");
  return res.status(200).json({
    ok: true,
    service: "adpanel",
    ts: Date.now(),
  });
});

app.use(async (req, res, next) => {
  try {
    if (
      req.path.startsWith("/login") ||
      req.path.startsWith("/register") ||
      req.path.startsWith("/forgot-password") ||
      req.path.startsWith("/db-access/") ||
      req.path === "/healthz" ||
      req.path === "/healtz" ||
      req.path === "/api/me" ||
      await isOpenNodeRoute(req)
    ) {
      return next();
    }

    if (!(await isAuthenticated(req))) {
      if (req.path.startsWith("/api/")) return res.status(401).json({ error: "not authenticated" });
      return res.redirect("/login");
    }

    return next();
  } catch (err) {
    return next(err);
  }
});

app.use(async (req, res, next) => {
  try {
    if (!req.path.startsWith("/api/nodes")) return next();
    if (await isOpenNodeRoute(req)) return next();
    if (req.path.startsWith("/api/nodes/server/")) return next();
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
    return next();
  } catch (err) {
    return next(err);
  }
});

app.use("/api/admin", async (req, res, next) => {
  try {
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
    return next();
  } catch (err) {
    return next(err);
  }
});

app.use("/api/settings/security", async (req, res, next) => {
  try {
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
    return next();
  } catch (err) {
    return next(err);
  }
});

function parseDbAccessTokenRequest(req) {
  let token;
  if (req.body && typeof req.body === "object") {
    token = req.body.token;
  }
  if (!token && typeof req.body === "string") {
    try {
      const parsed = JSON.parse(req.body);
      token = parsed.token;
    } catch { }
  }
  return { token };
}

// Token heartbeat/revocation (before admin guard — token is the secret, no auth needed)
app.post("/api/settings/database/access-heartbeat", (req, res) => {
  const { token } = parseDbAccessTokenRequest(req);
  if (!token || typeof token !== "string") return res.status(400).json({ error: "Missing token" });
  if (!dbProxy.touchToken(token)) return res.status(404).end();
  return res.status(204).end();
});

app.post("/api/settings/database/revoke-token", (req, res) => {
  const { token } = parseDbAccessTokenRequest(req);
  if (token && typeof token === "string") dbProxy.revokeToken(token);
  return res.status(204).end();
});

app.use("/api/settings/database", async (req, res, next) => {
  try {
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
    return next();
  } catch (err) {
    return next(err);
  }
});

app.use("/api/settings/templates", async (req, res, next) => {
  try {
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
    return next();
  } catch (err) {
    return next(err);
  }
});

app.use("/api/settings/accounts", async (req, res, next) => {
  try {
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
    return next();
  } catch (err) {
    return next(err);
  }
});

app.use("/api/settings/servers", async (req, res, next) => {
  try {
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
    return next();
  } catch (err) {
    return next(err);
  }
});

app.use("/api/settings/webhooks", async (req, res, next) => {
  try {
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
    return next();
  } catch (err) {
    return next(err);
  }
});

app.use("/api/settings/panel-update", async (req, res, next) => {
  try {
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
    return next();
  } catch (err) {
    return next(err);
  }
});

app.use("/api/settings/maintenance", async (req, res, next) => {
  try {
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
    return next();
  } catch (err) {
    return next(err);
  }
});

app.use(nodesRouter);
app.use("/api/subdomains", subdomainsRouter);

let _hostIpCache = { host: null, ip: null, ts: 0 };

function extractHostnameFromHeader(hostHeader) {
  if (!hostHeader) return null;
  const first = String(hostHeader).split(",")[0].trim();
  if (first.startsWith("[")) {
    const end = first.indexOf("]");
    if (end !== -1) return first.slice(1, end);
  }
  return first.split(":")[0];
}

async function resolvePublicIpFromHost(hostname) {
  if (!hostname) return null;
  if (net.isIP(hostname)) return hostname;

  try {
    const a = await dns.resolve4(hostname);
    if (Array.isArray(a) && a.length) return a[0];
  } catch { }

  try {
    const list = await dns.lookup(hostname, { all: true });
    const v4 = list.find(r => r && r.family === 4);
    if (v4) return v4.address;
    if (list[0] && list[0].address) return list[0].address;
  } catch { }

  try {
    const aaaa = await dns.resolve6(hostname);
    if (Array.isArray(aaaa) && aaaa.length) return aaaa[0];
  } catch { }

  return null;
}

app.get("/api/ssh-terminal/session", async (req, res) => {
  try {
    if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
    const token = issueSshTerminalToken(req);
    return res.json({ ok: true, launchUrl: `/ssh-terminal/?token=${encodeURIComponent(token)}` });
  } catch (err) {
    return res.status(500).json({ error: "failed to create terminal session" });
  }
});

async function proxySshTerminalIndex(req, res) {
  if (!(await isAuthenticated(req))) {
    return res.status(401).type("text/plain").send("not authenticated");
  }

  const incomingToken = String(req.query?.token || "").trim();
  const retryFlag = String(req.query?._retry || "") === "1";
  const token = incomingToken || issueSshTerminalToken(req);
  const upstreamOrigin = getSshTerminalOrigin(req);
  const upstreamUrl = new URL("/", upstreamOrigin);
  upstreamUrl.searchParams.set("token", token);

  let upstream;
  try {
    upstream = await fetch(upstreamUrl.toString(), {
      method: "GET",
      headers: {
        "x-forwarded-for": getRequestIp(req),
        "x-forwarded-proto": isRequestSecure(req) ? "https" : "http",
        "x-forwarded-host": req.get("host") || "",
      },
    });
  } catch {
    return res.status(502).type("text/plain").send("ssh terminal service unavailable");
  }

  if (upstream.status === 401 && !retryFlag) {
    const fresh = issueSshTerminalToken(req);
    return res.redirect(`/ssh-terminal/?token=${encodeURIComponent(fresh)}&_retry=1`);
  }

  const bodyText = await upstream.text();
  const headers = sanitizeProxyResponseHeaders(Object.fromEntries(upstream.headers.entries()));
  res.status(upstream.status);
  for (const [k, v] of Object.entries(headers)) {
    try { res.setHeader(k, v); } catch { }
  }
  return res.send(bodyText);
}

app.get("/ssh-terminal", proxySshTerminalIndex);
app.get("/ssh-terminal/", proxySshTerminalIndex);

app.get("/api/server-info", async (req, res, next) => {
  try {
    if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
    const hostname = HOST;
    const now = Date.now();

    if (_hostIpCache.host === hostname && _hostIpCache.ip && (now - _hostIpCache.ts) < 5 * 60 * 1000) {
      return res.json({ publicIp: _hostIpCache.ip });
    }

    const ip = await resolvePublicIpFromHost(hostname);
    _hostIpCache = { host: hostname, ip, ts: now };
    return res.json({ publicIp: ip });
  } catch (err) {
    return res.status(500).json({ error: "failed to resolve host ip" });
  }
});

app.get("/api/server-info/:name", async (req, res, next) => {
  try {
    if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });

    const raw = String(req.params.name || "").trim();
    const safeName = sanitizeServerName(raw) || raw;
    const entry = (await findServer(safeName)) || {};
    const serverName = entry.name || safeName;

    if (!(await isAdmin(req)) && !(await userHasAccessToServer(req.session.user, serverName))) {
      return res.status(403).json({ error: "no-access-to-server" });
    }

    const template = normalizeTemplateId(entry.template);

    const hostname = HOST;
    const now = Date.now();

    let ip = entry.ip || null;
    if (!ip) {
      if (_hostIpCache.host === hostname && _hostIpCache.ip && (now - _hostIpCache.ts) < 5 * 60 * 1000) ip = _hostIpCache.ip;
      else {
        ip = await resolvePublicIpFromHost(hostname);
        _hostIpCache = { host: hostname, ip, ts: now };
      }
    }

    const port = (entry.port !== undefined && entry.port !== null)
      ? entry.port
      : (template === "minecraft" ? 25565 : null);

    return res.json({
      name: entry.name || raw,
      start: entry.start || null,
      ip: ip || null,
      port,
      template: template || null,
      runtime: entry.runtime || null,
      nodeId: entry.nodeId || null,
    });
  } catch (err) {
    return res.status(500).json({ error: "failed to resolve host ip" });
  }
});

app.post("/api/servers/:bot/template", async (req, res, next) => {
  try {
    if (!(await isAuthenticated(req))) return res.status(401).json({ ok: false, error: "not-authenticated" });

    const raw = String(req.params.bot || "").trim();
    if (!requireActionTokenOr403(req, res, "POST /api/servers/:bot/template", { serverName: raw })) return;
    const entry = await findServer(raw);
    const isAdminUser = await isAdmin(req);

    if (!entry) return res.status(404).json({ ok: false, error: "server-not-found" });
    if (!isAdminUser && !(await userHasAccessToServer(req.session.user, entry.name || raw))) {
      return res.status(403).json({ ok: false, error: "no-access-to-server" });
    }
    if (!isAdminUser) {
      const perms = await getEffectivePermsForUserOnServer(req.session.user, entry.name || raw);
      if (!perms.store_access) {
        return res.status(403).json({ ok: false, error: "not-authorized", permission: "store_access" });
      }
    }

    const templateId = normalizeTemplateId(req.body?.template || req.body?.tpl || "");
    const allowedTemplates = new Set(["discord-bot", "minecraft", "nodejs", "python"]);

    if (!templateId) return res.status(400).json({ ok: false, error: "missing-template" });
    if (!allowedTemplates.has(templateId)) {
      return res.status(400).json({ ok: false, error: "unsupported-template" });
    }

    const list = await loadServersIndex();
    const idx = findServerIndex(list, entry.name || raw);
    if (idx === -1) return res.status(404).json({ ok: false, error: "server-not-found" });

    list[idx] = Object.assign({}, list[idx], { template: templateId });
    if (!(await saveServersIndex(list))) return res.status(500).json({ ok: false, error: "persist-failed" });

    return res.json({ ok: true, template: templateId });
  } catch (err) {
    return next(err);
  }
});

const LOGIN_WINDOW_MS = 2 * 60 * 1000;
const LOGIN_MAX_ATTEMPTS = 10;
const LOGIN_BLOCK_MS = 10 * 60 * 1000;
const LOGIN_DELAY_BASE_MS = 400;
const LOGIN_DELAY_JITTER_MS = 400;
const LOGIN_SUSPICIOUS_WINDOW_MS = parseInt(process.env.LOGIN_SUSPICIOUS_WINDOW_MS || String(LOGIN_WINDOW_MS), 10);
const LOGIN_SUSPICIOUS_ATTEMPTS = parseInt(process.env.LOGIN_SUSPICIOUS_ATTEMPTS || "3", 10);
const LOGIN_SUSPICIOUS_FAST_WINDOW_MS = parseInt(process.env.LOGIN_SUSPICIOUS_FAST_WINDOW_MS || "20000", 10);
const LOGIN_SUSPICIOUS_FAST_ATTEMPTS = parseInt(process.env.LOGIN_SUSPICIOUS_FAST_ATTEMPTS || "2", 10);
const LOGIN_SUSPICIOUS_SCORE_THRESHOLD = parseInt(process.env.LOGIN_SUSPICIOUS_SCORE_THRESHOLD || "3", 10);
const LOGIN_MAP_MAX_SIZE = scaleForMemory(10000);
const loginAttempts = new Map();

const LOGIN_ATTACK_WINDOW_MS = parseInt(process.env.LOGIN_ATTACK_WINDOW_MS || "1000", 10) || 1000;
const LOGIN_ATTACK_UNIQUE_IPS = parseInt(process.env.LOGIN_ATTACK_UNIQUE_IPS || "100", 10) || 100;
const LOGIN_ATTACK_HOLD_MS = parseInt(process.env.LOGIN_ATTACK_HOLD_MS || "60000", 10) || 60000;
const LOGIN_ATTACK_IP_WINDOW_MS = parseInt(process.env.LOGIN_ATTACK_IP_WINDOW_MS || "3000", 10) || 3000;
const LOGIN_ATTACK_IP_MAX = parseInt(process.env.LOGIN_ATTACK_IP_MAX || "25", 10) || 25;

const loginAttackIps = new Map();
const loginAttackIpHits = new Map();
let loginUnderAttackUntil = 0;

function cleanLoginAttempts() {
  const now = Date.now();
  for (const [ip, rec] of loginAttempts.entries()) {
    const attempts = (rec?.attempts || []).filter(ts => now - ts < LOGIN_WINDOW_MS);
    const blockedUntil = rec?.blockedUntil || 0;
    if (attempts.length === 0 && (!blockedUntil || blockedUntil < now)) {
      loginAttempts.delete(ip);
    } else {
      loginAttempts.set(ip, { attempts, blockedUntil });
    }
  }

  for (const [ip, hits] of loginAttackIpHits.entries()) {
    const valid = (hits || []).filter(ts => now - ts <= LOGIN_ATTACK_IP_WINDOW_MS);
    if (valid.length === 0) {
      loginAttackIpHits.delete(ip);
    } else {
      loginAttackIpHits.set(ip, valid);
    }
  }
}
setInterval(cleanLoginAttempts, 5 * 60 * 1000).unref();

function recordLoginAttempt(ip) {
  const now = Date.now();
  if (!loginAttempts.has(ip) && loginAttempts.size >= LOGIN_MAP_MAX_SIZE) {
    const oldest = loginAttempts.keys().next().value;
    if (oldest !== undefined) loginAttempts.delete(oldest);
  }
  const rec = loginAttempts.get(ip) || { attempts: [], blockedUntil: 0 };
  rec.attempts = (rec.attempts || []).filter(ts => now - ts < LOGIN_WINDOW_MS);
  rec.attempts.push(now);
  if (rec.attempts.length > LOGIN_MAX_ATTEMPTS) {
    rec.blockedUntil = now + LOGIN_BLOCK_MS;
  }
  loginAttempts.set(ip, rec);
  return rec;
}

function isLoginBlocked(ip) {
  const now = Date.now();
  const rec = loginAttempts.get(ip);
  return rec && rec.blockedUntil && rec.blockedUntil > now;
}

function resetLoginAttempts(ip) {
  if (!ip) return;
  loginAttempts.set(ip, { attempts: [], blockedUntil: 0 });
}

function loginDelay() {
  const jitter = Math.floor(Math.random() * LOGIN_DELAY_JITTER_MS);
  const ms = LOGIN_DELAY_BASE_MS + jitter;
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function markLoginTraffic(ip) {
  if (!ip || LOGIN_ATTACK_UNIQUE_IPS <= 0) return;
  const now = Date.now();
  for (const [key, ts] of loginAttackIps.entries()) {
    if (now - ts > LOGIN_ATTACK_WINDOW_MS) loginAttackIps.delete(key);
  }
  if (!loginAttackIps.has(ip) && loginAttackIps.size >= LOGIN_MAP_MAX_SIZE) {
    const oldest = loginAttackIps.keys().next().value;
    if (oldest !== undefined) loginAttackIps.delete(oldest);
  }
  loginAttackIps.set(ip, now);
  if (loginAttackIps.size >= LOGIN_ATTACK_UNIQUE_IPS) {
    loginUnderAttackUntil = Math.max(loginUnderAttackUntil, now + LOGIN_ATTACK_HOLD_MS);
  }
}

function isLoginUnderAttack() {
  return Date.now() < loginUnderAttackUntil;
}

function isAggressiveLoginIp(ip) {
  if (!ip || LOGIN_ATTACK_IP_MAX <= 0) return false;
  const now = Date.now();
  let arr = loginAttackIpHits.get(ip) || [];
  arr = arr.filter(ts => now - ts <= LOGIN_ATTACK_IP_WINDOW_MS);
  arr.push(now);

  if (arr.length > LOGIN_ATTACK_IP_MAX * 5) {
    arr = arr.slice(-LOGIN_ATTACK_IP_MAX * 2);
  }

  if (!loginAttackIpHits.has(ip) && loginAttackIpHits.size >= LOGIN_MAP_MAX_SIZE) {
    const oldest = loginAttackIpHits.keys().next().value;
    if (oldest !== undefined) loginAttackIpHits.delete(oldest);
  }
  loginAttackIpHits.set(ip, arr);
  return arr.length > LOGIN_ATTACK_IP_MAX;
}

const NON_BROWSER_UA_RE = /^(curl|wget|httpie|python-|go-http|java\/|libwww|axios\/|node-fetch|undici\/|got\/|got |superagent|postman|insomnia|rest-client|apache-httpclient|okhttp|dart|ruby|perl|php|powershell|winhttp|scrapy|mechanize|phantomjs|headlesschrome|selenium|puppeteer|playwright)/i;

function isNonBrowserRequest(req) {
  const ua = (req.get("user-agent") || "").trim();
  const secFetchDest = (req.get("sec-fetch-dest") || "").toLowerCase();

  if (NON_BROWSER_UA_RE.test(ua)) return true;

  if (!ua) return true;

  if (secFetchDest && secFetchDest !== "document" && secFetchDest !== "empty") return true;

  return false;
}

function countAttemptsWithin(rec, windowMs) {
  if (!rec || !Array.isArray(rec.attempts)) return 0;
  const now = Date.now();
  return rec.attempts.reduce((acc, ts) => acc + (now - ts <= windowMs ? 1 : 0), 0);
}

function suspiciousScoreForLogin(req, rec) {
  const ua = String(req.get("user-agent") || "").trim();
  const acceptLang = String(req.get("accept-language") || "").trim();
  const accept = String(req.get("accept") || "").trim();
  let score = 0;

  const fastAttempts = countAttemptsWithin(rec, LOGIN_SUSPICIOUS_FAST_WINDOW_MS);
  const windowAttempts = countAttemptsWithin(rec, LOGIN_SUSPICIOUS_WINDOW_MS);

  if (windowAttempts >= LOGIN_SUSPICIOUS_ATTEMPTS) score += 2;
  if (fastAttempts >= LOGIN_SUSPICIOUS_FAST_ATTEMPTS) score += 2;
  if (!ua || ua.length < 8) score += 2;
  if (!acceptLang) score += 1;
  if (!accept) score += 1;

  return score;
}

function isSuspiciousLogin(req, rec) {
  return suspiciousScoreForLogin(req, rec) >= LOGIN_SUSPICIOUS_SCORE_THRESHOLD;
}

const BOTPROOF_TTL_MS = parseInt(process.env.BOTPROOF_TTL_MS || "900000", 10);

function ensureBotProofSeed(req, res) {
  if (!req || !res || !req.session) return "";
  const now = Date.now();
  let seed = req.session.botProofSeed;
  const ts = req.session.botProofSeedTs || 0;
  if (!seed || !ts || (now - ts) > BOTPROOF_TTL_MS) {
    seed = crypto.randomBytes(18).toString("hex");
    req.session.botProofSeed = seed;
    req.session.botProofSeedTs = now;
  }
  try {
    res.cookie("adpanel.bp", seed, {
      httpOnly: true,
      sameSite: "lax",
      secure: isRequestSecure(req),
      maxAge: BOTPROOF_TTL_MS,
    });
  } catch { }
  return seed;
}

function isBotProofValid(req, value) {
  const expected = req?.session?.botProofSeed || "";
  if (!expected) return false;
  return !!value && value === expected;
}

setInterval(() => {
  try {
    const now = Date.now();
    for (const [ip, ts] of loginAttackIps.entries()) {
      if (now - ts > LOGIN_ATTACK_WINDOW_MS) loginAttackIps.delete(ip);
    }
    for (const [ip, arr] of loginAttackIpHits.entries()) {
      const kept = arr.filter(ts => now - ts <= LOGIN_ATTACK_IP_WINDOW_MS);
      if (kept.length) loginAttackIpHits.set(ip, kept);
      else loginAttackIpHits.delete(ip);
    }
  } catch (err) { console.debug("[loginAttack] sweep error:", err.message); }
}, 15_000);

function loginAttackMiddleware(req, res, next) {
  const ip = getRequestIp(req);
  markLoginTraffic(ip);
  const underAttack = isLoginUnderAttack();
  res.locals.underAttack = underAttack;
  if (underAttack && isAggressiveLoginIp(ip)) {
    res.setHeader("Retry-After", "5");
    return res.status(429).send("Too Many Requests");
  }
  return next();
}

app.use("/login", loginAttackMiddleware);

function getLoginPageAssetsForSecurity(res) {
  const branding = (res && res.locals && res.locals.branding) || {};
  return {
    loginBackgroundType: String(branding.loginBackgroundType || "video").toLowerCase(),
    loginBackgroundUrl: String(branding.loginBackgroundUrl || "").trim(),
    loginWatermarkUrl: String(branding.loginWatermarkUrl || "").trim(),
  };
}

function applyLoginPageSecurityHeaders(req, res, options = {}) {
  const assets = getLoginPageAssetsForSecurity(res);
  const imgOrigins = ["https://cdn.jsdelivr.net"];
  const mediaOrigins = [];

  const watermarkOrigin = getOriginFromUrlLike(assets.loginWatermarkUrl);
  if (watermarkOrigin) imgOrigins.push(watermarkOrigin);

  const backgroundOrigin = getOriginFromUrlLike(assets.loginBackgroundUrl);
  if (backgroundOrigin) {
    if (assets.loginBackgroundType === "image") imgOrigins.push(backgroundOrigin);
    else mediaOrigins.push(backgroundOrigin);
  }

  res.setHeader("Cache-Control", "no-store");
  res.setHeader("Content-Security-Policy", buildAuthPageCsp(req, res, {
    includeFontAwesome: true,
    includeGoogleFonts: false,
    includeCaptcha: !!options.useExternalCaptcha,
    captchaProvider: options.externalCaptchaProvider || "",
    allowInlineStyleAttrs: !!options.useExternalCaptcha,
    additionalImgOrigins: imgOrigins,
    additionalMediaOrigins: mediaOrigins,
  }));
}

function applyForgotPasswordPageSecurityHeaders(req, res) {
  res.setHeader("Cache-Control", "no-store");
  res.setHeader("Content-Security-Policy", buildAuthPageCsp(req, res, {
    includeFontAwesome: false,
    includeGoogleFonts: false,
    includeCaptcha: false,
    allowInlineStyleAttrs: false,
    additionalImgOrigins: ["https://cdn.jsdelivr.net"],
  }));
}

const LOGIN_CSS_PATH = path.join(__dirname, "public", "login.css");
let loginCssInlineCache = { mtimeMs: 0, content: "" };

function getInlineLoginCss() {
  try {
    const stat = fs.statSync(LOGIN_CSS_PATH);
    if (!loginCssInlineCache.content || loginCssInlineCache.mtimeMs !== stat.mtimeMs) {
      loginCssInlineCache = {
        mtimeMs: stat.mtimeMs,
        content: fs.readFileSync(LOGIN_CSS_PATH, "utf8"),
      };
    }
    return loginCssInlineCache.content;
  } catch (err) {
    console.error("[login] Failed to load inline CSS:", err.message);
    return "";
  }
}

app.get("/login", (req, res) => {
  if (req.session?.user) {
    return res.redirect(303, "/");
  }

  const botProofSeed = ensureBotProofSeed(req, res);
  const csrfToken = ensureLoginCsrfToken(req);

  const qErr = req.query?.error ? String(req.query.error) : null;
  const hasError = !!qErr;
  const underAttack = !!res.locals.underAttack || isLoginUnderAttack();

  if (req.session && req.session.captchaSolved) {
    clearLocalCaptcha(req);
    req.session.captchaRequired = false;
  }

  if (req.session && !req.session.captchaRequired && !hasError && !underAttack) {
    clearLocalCaptcha(req);
    req.session.captchaRequired = false;
    req.session.captchaSolved = false;
  }

  let captchaQuestion = null;
  const captchaRequired = ANY_CAPTCHA_ENABLED && (underAttack || (req.session?.captchaRequired && !req.session?.captchaSolved));
  const useExternalCaptcha = EXTERNAL_CAPTCHA_ENABLED && captchaRequired;
  const shouldShowLocal = LOCAL_CAPTCHA_ENABLED && !useExternalCaptcha && captchaRequired;
  const externalCaptchaAutoStart = useExternalCaptcha && captchaRequired;
  if (shouldShowLocal) {
    if (!req.session.localCaptcha) assignLocalCaptcha(req);
    captchaQuestion = getCurrentCaptchaQuestion(req);
    if (!captchaQuestion) captchaQuestion = assignLocalCaptcha(req);
  }

  applyLoginPageSecurityHeaders(req, res, {
    useExternalCaptcha,
    externalCaptchaProvider: EXTERNAL_CAPTCHA_PROVIDER,
  });

  return res.render("login", {
    error: qErr || null,
    botProofSeed,
    csrfToken,
    loginCss: getInlineLoginCss(),
    useExternalCaptcha,
    externalCaptchaAutoStart,
    externalCaptchaSiteKey: EXTERNAL_CAPTCHA_SITE_KEY,
    externalCaptchaProvider: EXTERNAL_CAPTCHA_PROVIDER,
    showLocalCaptcha: shouldShowLocal && !!captchaQuestion,
    captchaQuestion,
  });
});

app.get("/register", (req, res) => {
  const secret = speakeasy.generateSecret({ length: 20 });
  req.session.secret = secret.base32;
  res.render("register", { secret: req.session.secret });
});

app.get("/forgot-password", (req, res) => {
  applyForgotPasswordPageSecurityHeaders(req, res);
  res.render("forgot-password", { error: null, success: null });
});

app.post("/login", async (req, res) => {
  const accept = String(req.get("accept") || "");
  const contentType = String(req.get("content-type") || "");
  const wantsJson =
    accept.includes("application/json") ||
    contentType.includes("application/json") ||
    String(req.get("x-requested-with") || "") === "XMLHttpRequest";
  const externalCaptchaEnabled = EXTERNAL_CAPTCHA_ENABLED;
  const localCaptchaEnabled = LOCAL_CAPTCHA_ENABLED;
  const captchaAvailable = ANY_CAPTCHA_ENABLED;
  let justSolvedCaptcha = false;

  const sendError = (status, message, options = {}) => {
    const csrfToken = rotateLoginCsrfToken(req);
    const preserveCaptchaState = !!options.preserveCaptchaState;
    if (wantsJson) return res.status(status).json({ ok: false, error: message });
    try {
      if (req.session) {
        if (preserveCaptchaState || justSolvedCaptcha || req.session.captchaSolved) {
        } else {
          if (captchaAvailable) req.session.captchaRequired = true;
          req.session.captchaSolved = false;
          if (localCaptchaEnabled && !externalCaptchaEnabled && !req.session.localCaptcha) {
            assignLocalCaptcha(req);
          }
        }
      }
    } catch { }
    const botProofSeed = ensureBotProofSeed(req, res);
    const underAttack = !!res.locals.underAttack || isLoginUnderAttack();
    let captchaQuestion = null;
    const captchaRequired = captchaAvailable && (underAttack || (req.session?.captchaRequired && !req.session?.captchaSolved));
    const useExternalCaptcha = externalCaptchaEnabled && captchaRequired;
    const shouldShowLocal = localCaptchaEnabled && !useExternalCaptcha && captchaRequired;
    const externalCaptchaAutoStart = useExternalCaptcha && captchaRequired;
    if (shouldShowLocal) {
      if (!req.session.localCaptcha) assignLocalCaptcha(req);
      captchaQuestion = getCurrentCaptchaQuestion(req) || assignLocalCaptcha(req);
    }

    applyLoginPageSecurityHeaders(req, res, {
      useExternalCaptcha,
      externalCaptchaProvider: EXTERNAL_CAPTCHA_PROVIDER,
    });

    const renderData = {
      error: message,
      botProofSeed,
      csrfToken,
      loginCss: getInlineLoginCss(),
      useExternalCaptcha,
      externalCaptchaAutoStart,
      externalCaptchaSiteKey: EXTERNAL_CAPTCHA_SITE_KEY,
      externalCaptchaProvider: EXTERNAL_CAPTCHA_PROVIDER,
      showLocalCaptcha: shouldShowLocal && !!captchaQuestion,
      captchaQuestion,
    };

    if (req.session?.save) {
      return req.session.save(() => res.status(status).render("login", renderData));
    }
    return res.status(status).render("login", renderData);
  };

  const sendOk = () => {
    res.setHeader("Cache-Control", "no-store");
    if (wantsJson) return res.json({ ok: true, redirect: "/" });
    return res.redirect(303, "/");
  };

  if (!isValidLoginCsrfToken(req, req.body?._csrf)) {
    return sendError(403, "Security token expired. Please refresh and try again.", { preserveCaptchaState: true });
  }

  const withTimeout = (p, ms, label) =>
    Promise.race([
      p,
      new Promise((_, rej) => setTimeout(() => rej(new Error(label || "timeout")), ms)),
    ]);

  try {
    if (isNonBrowserRequest(req)) {
      return res.status(403).set("Content-Type", "text/plain").send("Forbidden");
    }

    const { email, password, code } = req.body || {};
    if (req.body?.company) {
      return sendError(400, "Invalid submission.");
    }
    const captchaToken = req.body?.captchaToken || req.body?.["g-recaptcha-response"];
    const botProof = String(req.body?.botProof || "").trim();
    const localCaptchaToken = req.body?.localCaptchaToken;
    const captchaSelectionRaw = req.body?.captchaSelection;

    const emailStr = String(email || "").trim().toLowerCase();
    const passStr = String(password || "");
    const codeStr = String(code || "").trim();

    const clientIp = getRequestIp(req);

    if (isLoginBlocked(clientIp)) {
      await loginDelay();
      return sendError(429, "Try again later.");
    }

    const rec = recordLoginAttempt(clientIp);
    await loginDelay();
    if (rec.blockedUntil && rec.blockedUntil > Date.now()) {
      return sendError(429, "Try again later.");
    }

    const underAttack = !!res.locals.underAttack || isLoginUnderAttack();
    if (underAttack && req.session && captchaAvailable) {
      req.session.captchaRequired = true;
      req.session.captchaSolved = false;
    }

    const suspicious = isSuspiciousLogin(req, rec);
    const captchaRequired = captchaAvailable && (underAttack || (req.session?.captchaRequired && !req.session?.captchaSolved));
    const needCaptcha = captchaAvailable && (captchaRequired || suspicious || underAttack);

    if (req.session?.captchaSolved) {
    } else if (needCaptcha && externalCaptchaEnabled) {
      if (req.session) req.session.captchaRequired = true;
      if (!captchaToken) {
        if (req.session) req.session.captchaSolved = false;
        return sendError(403, "");
      }
      if (req.session.lastExternalCaptchaToken && req.session.lastExternalCaptchaToken === captchaToken) {
        if (localCaptchaEnabled) assignLocalCaptcha(req);
        return sendError(400, "Captcha verification failed");
      }
      const verify = await verifyExternalCaptcha(captchaToken, clientIp);
      if (!verify.ok) return sendError(400, "Captcha verification failed");
      req.session.lastExternalCaptchaToken = captchaToken || "";
      req.session.captchaSolved = true;
      justSolvedCaptcha = true;
    } else if (needCaptcha && localCaptchaEnabled) {
      if (req.session) req.session.captchaRequired = true;
      if (!localCaptchaToken && !captchaSelectionRaw) {
        if (req.session) req.session.captchaSolved = false;
        return sendError(403, "");
      }
      let selected = [];
      if (Array.isArray(captchaSelectionRaw)) selected = captchaSelectionRaw.map(Number);
      else if (typeof captchaSelectionRaw === "string") {
        selected = captchaSelectionRaw
          .split(",")
          .map(s => Number(s.trim()))
          .filter(Number.isInteger);
      }
      const valid = validateLocalCaptcha(req, selected, localCaptchaToken);
      if (!valid.ok) {
        assignLocalCaptcha(req);
        return sendError(400, valid.expired ? "Captcha expired. Try again." : "Captcha incorrect");
      }
      if (!valid.done) {
        return sendError(400, "Captcha not finished yet.");
      }
      req.session.captchaSolved = true;
      justSolvedCaptcha = true;
    }

    if (needCaptcha && !justSolvedCaptcha && !req.session?.captchaSolved && !isBotProofValid(req, botProof)) {
      if (req.session) {
        req.session.captchaRequired = true;
        req.session.captchaSolved = false;
      }
      return sendError(403, "");
    }

    if (!emailStr || !passStr || !codeStr) {
      return sendError(400, "The fields are empty.");
    }

    const user = await findUserByEmail(emailStr);
    if (!user || !user.password) {
      return sendError(400, "Invalid credentials");
    }

    const match = await withTimeout(bcrypt.compare(passStr, user.password), 8000, "bcrypt-timeout");
    if (!match) {
      return sendError(400, "Invalid credentials");
    }

    const verified = speakeasy.totp.verify({
      secret: user.secret,
      encoding: "base32",
      token: codeStr,
      window: 1,
    });
    if (!verified) {
      return sendError(400, "Invalid credentials");
    }

    const isActuallySecure = isRequestSecure(req);
    if (SESSION_COOKIE_SECURE && !isActuallySecure) {
      return sendError(
        500,
        "Config problem: SESSION_COOKIE_SECURE is on, but the request is seen as HTTP. Check reverse proxy to send X-Forwarded-Proto: https or set proxy trust correctly."
      );
    }

    await withTimeout(
      new Promise((resolve, reject) => req.session.regenerate((err) => (err ? reject(err) : resolve()))),
      5000,
      "session-regenerate-timeout"
    );

    req.session.user = user.email;
    ensureRememberLoginSessionId(req);

    await withTimeout(
      new Promise((resolve, reject) => req.session.save((err) => (err ? reject(err) : resolve()))),
      5000,
      "session-save-timeout"
    );

    setRememberLoginCookie(req, res, user);
    clearLocalCaptcha(req);
    resetLoginAttempts(clientIp);
    return sendOk();
  } catch (err) {
    console.error("[login] error:", err);
    return sendError(500, "Internal login error.");
  }
});

app.post("/logout", (req, res) => {
  const clear = () => {
    res.clearCookie("adpanel.sid", {
      httpOnly: true,
      sameSite: "lax",
      secure: SESSION_COOKIE_SECURE,
      path: "/",
    });
    clearRememberLoginCookie(res);
  };

  if (req.session) {
    const rememberLoginId = String(req.session.rememberLoginId || req.session.browserSessionAudit?.rememberLoginId || "").trim();
    if (rememberLoginId) revokeRememberLoginRegistryEntry(rememberLoginId);
    removeActiveBrowserSessionCacheEntry(req.sessionID);
    invalidateActiveBrowserSessionCache();
    req.session.destroy(err => {
      if (err) return res.status(500).json({ error: "Failed to logout" });
      clear();
      return res.json({ success: true });
    });
  } else {
    clear();
    return res.json({ success: true });
  }
});

const ENV_FILE_PATH = path.join(__dirname, ".env");

function readEnvFile() {
  try {
    if (!fs.existsSync(ENV_FILE_PATH)) return {};
    const content = fs.readFileSync(ENV_FILE_PATH, "utf8");
    const env = {};
    for (const line of content.split("\n")) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) continue;
      const eqIdx = trimmed.indexOf("=");
      if (eqIdx === -1) continue;
      const key = trimmed.slice(0, eqIdx).trim();
      let val = trimmed.slice(eqIdx + 1).trim();
      if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
        val = val.slice(1, -1);
      }
      env[key] = val;
    }
    return env;
  } catch {
    return {};
  }
}

function writeEnvValue(key, value) {
  try {
    let content = "";
    if (fs.existsSync(ENV_FILE_PATH)) {
      content = fs.readFileSync(ENV_FILE_PATH, "utf8");
    }
    const lines = content.split("\n");
    let found = false;
    const cleaned = String(value ?? "").replace(/\0/g, "").replace(/[\r\n]+/g, "");
    const safeValue = cleaned.includes(" ") || cleaned.includes('"') ? `"${cleaned.replace(/"/g, '\\"')}"` : `"${cleaned}"`;

    for (let i = 0; i < lines.length; i++) {
      const trimmed = lines[i].trim();
      if (trimmed.startsWith("#")) continue;
      const eqIdx = trimmed.indexOf("=");
      if (eqIdx === -1) continue;
      const lineKey = trimmed.slice(0, eqIdx).trim();
      if (lineKey === key) {
        lines[i] = `${key}=${safeValue}`;
        found = true;
        break;
      }
    }

    if (!found) {
      lines.push(`${key}=${safeValue}`);
    }

    fs.writeFileSync(ENV_FILE_PATH, lines.join("\n"), "utf8");
    try { fs.chmodSync(ENV_FILE_PATH, 0o600); } catch { }
    return true;
  } catch (err) {
    console.error("[security] Failed to write .env:", err);
    return false;
  }
}

app.get("/api/settings/security", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });

  const envData = readEnvFile();
  const captchaEnabled = !!(envData.SITE_KEY && envData.SECRET_KEY && envData.SITE_KEY.trim() && envData.SECRET_KEY.trim());

  return res.json({
    rate_limiting: security.rate_limiting,
    limit: security.limit,
    window_seconds: security.window_seconds,
    trusted_subnets: security.trusted_subnets || [],
    captcha_enabled: captchaEnabled,
    actionTokens: {
      updateSecurity: issueActionToken(req, "POST /api/settings/security", {}, { ttlSeconds: 300 }),
      updateCaptcha: issueActionToken(req, "POST /api/settings/captcha", {}, { ttlSeconds: 300 })
    }
  });
});

app.post("/api/settings/security", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  if (!requireActionTokenOr403(req, res, "POST /api/settings/security")) return;

  const { rate_limiting, limit, window_seconds, trusted_subnets } = req.body || {};

  const newLimit = Math.max(1, Math.min(10000, parseInt(limit, 10) || 5));
  const newWindow = Math.max(1, Math.min(86400, parseInt(window_seconds, 10) || 120));

  security.rate_limiting = !!rate_limiting;
  security.limit = newLimit;
  security.window_seconds = newWindow;
  if (Array.isArray(trusted_subnets)) {
    security.trusted_subnets = trusted_subnets.filter(s => typeof s === "string" && s.length > 0).slice(0, 50);
  }

  try {
    safeWriteJson(SECURITY_FILE, security);
    return res.json({ ok: true });
  } catch (err) {
    console.error("[security] Failed to save security.json:", err);
    return res.status(500).json({ error: "failed to save" });
  }
});

app.post("/api/settings/captcha", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  if (!requireActionTokenOr403(req, res, "POST /api/settings/captcha")) return;

  const { site_key, secret_key, enabled } = req.body || {};

  if (enabled === false) {
    const siteOk = writeEnvValue("SITE_KEY", "");
    const secretOk = writeEnvValue("SECRET_KEY", "");
    if (!siteOk || !secretOk) {
      return res.status(500).json({ error: "failed to update configuration" });
    }
    return res.json({ ok: true, captcha_enabled: false });
  }

  const cleanSiteKey = String(site_key || "").trim();
  const cleanSecretKey = String(secret_key || "").trim();

  if (!cleanSiteKey || cleanSiteKey.length < 10) {
    return res.status(400).json({ error: "invalid site key" });
  }
  if (!cleanSecretKey || cleanSecretKey.length < 10) {
    return res.status(400).json({ error: "invalid secret key" });
  }

  const siteOk = writeEnvValue("SITE_KEY", cleanSiteKey);
  const secretOk = writeEnvValue("SECRET_KEY", cleanSecretKey);

  if (!siteOk || !secretOk) {
    return res.status(500).json({ error: "failed to update configuration" });
  }

  return res.json({ ok: true, captcha_enabled: true, note: "Restart server for changes to take effect" });
});


app.get("/api/maintenance/status", async (req, res) => {
  const state = getMaintenanceState();
  const active = isMaintenanceActive();
  return res.json({
    active,
    reason: active ? (state.reason || null) : null,
    scheduled_at: (!active && state.scheduled_at) ? state.scheduled_at : null
  });
});

app.get("/api/settings/maintenance", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  const state = getMaintenanceState();
  return res.json({
    ...state,
    is_active: isMaintenanceActive(),
    actionTokens: {
      enableMaintenance: issueActionToken(req, "POST /api/settings/maintenance", {}, { ttlSeconds: 300 }),
      disableMaintenance: issueActionToken(req, "DELETE /api/settings/maintenance", {}, { ttlSeconds: 300 })
    }
  });
});

app.post("/api/settings/maintenance", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  if (!requireActionTokenOr403(req, res, "POST /api/settings/maintenance")) return;

  const { reason, scheduled_at, immediate, pre_alert_show, pre_alert_message, pre_alert_minutes_before } = req.body || {};

  const user = await findUserByEmail(req.session.user);
  if (!user || !user.admin) return res.status(403).json({ error: "not authorized" });

  const state = getMaintenanceState();

  if (scheduled_at && !immediate) {
    const scheduledTime = new Date(scheduled_at).getTime();
    if (isNaN(scheduledTime)) {
      return res.status(400).json({ error: "Invalid scheduled date" });
    }
    if (scheduledTime <= Date.now()) {
      return res.status(400).json({ error: "Scheduled time must be in the future" });
    }
    state.scheduled_at = new Date(scheduledTime).toISOString();
    state.enabled = false;
  } else {
    state.enabled = true;
    state.enabled_at = new Date().toISOString();
    state.scheduled_at = null;
  }

  state.enabled_by = user.email;
  state.reason = reason ? String(reason).slice(0, 500) : null;

  state.pre_alert = {
    show: !!pre_alert_show,
    message: pre_alert_message ? String(pre_alert_message).slice(0, 300) : null,
    minutes_before: pre_alert_minutes_before ? Math.max(1, Math.min(525600, parseInt(pre_alert_minutes_before, 10) || 30)) : null,
    _alert_injected: false,
    _injected_alert_message: null
  };

  if (state.enabled && state.pre_alert.show && state.pre_alert.message) {
    const currentAlert = getActiveGlobalAlert();
    if (currentAlert) {
      state.previous_alert_backup = currentAlert;
    }
    addGlobalAlert(state.pre_alert.message, new Date().toISOString());
    state.pre_alert._alert_injected = true;
    state.pre_alert._injected_alert_message = state.pre_alert.message;
  }

  if (!saveMaintenanceState(state)) {
    return res.status(500).json({ error: "Failed to save maintenance state" });
  }

  startMaintenanceChecker();

  console.log(`[maintenance] ${state.enabled ? "Enabled" : "Scheduled"} by ${user.email}${state.scheduled_at ? " for " + state.scheduled_at : ""}`);
  return res.json({ ok: true, state: { enabled: state.enabled, scheduled_at: state.scheduled_at } });
});

app.delete("/api/settings/maintenance", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  if (!requireActionTokenOr403(req, res, "DELETE /api/settings/maintenance")) return;

  const user = await findUserByEmail(req.session.user);
  if (!user || !user.admin) return res.status(403).json({ error: "not authorized" });

  const state = getMaintenanceState();

  restoreAlertAfterMaintenance(state);

  const newState = {
    enabled: false,
    scheduled_at: null,
    enabled_by: null,
    enabled_at: null,
    reason: null,
    pre_alert: { show: false, message: null, minutes_before: null },
    previous_alert_backup: null
  };

  if (!saveMaintenanceState(newState)) {
    return res.status(500).json({ error: "Failed to save maintenance state" });
  }

  console.log(`[maintenance] Disabled by ${user.email}`);
  return res.json({ ok: true });
});

app.get("/api/settings/sessions", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  res.set("Cache-Control", "no-store");

  const currentUser = await findUserByEmail(req.session.user);
  if (!currentUser || !currentUser.admin) {
    return res.status(403).json({ error: "not authorized" });
  }

  const rawSearch = String(req.query?.search || "").trim().toLowerCase();
  const search = rawSearch.slice(0, 160);
  const searchActive = !!search;
  const requestedPage = Math.max(1, parseInt(req.query?.page, 10) || 1);

  try {
    const activeSessions = await getCachedActiveBrowserSessions();
    const sortedSessions = [...activeSessions].sort((a, b) => {
      const lastSeenDiff = Number(b.lastSeenAt || 0) - Number(a.lastSeenAt || 0);
      if (lastSeenDiff !== 0) return lastSeenDiff;
      return Number(b.createdAt || 0) - Number(a.createdAt || 0);
    });

    const filteredSessions = searchActive
      ? sortedSessions.filter((entry) => String(entry.email || "").toLowerCase().includes(search))
      : sortedSessions;

    const total = filteredSessions.length;
    const totalPages = searchActive ? 1 : Math.max(1, Math.ceil(total / ACTIVE_BROWSER_SESSION_PAGE_SIZE));
    const page = searchActive ? 1 : Math.min(requestedPage, totalPages);
    const startIndex = searchActive ? 0 : (page - 1) * ACTIVE_BROWSER_SESSION_PAGE_SIZE;
    const endIndex = searchActive ? total : Math.min(startIndex + ACTIVE_BROWSER_SESSION_PAGE_SIZE, total);
    const items = filteredSessions
      .slice(startIndex, searchActive ? undefined : endIndex)
      .map((entry) => ({
        ...entry,
        isCurrent: entry.sessionId === String(req.sessionID || ""),
      }));

    const revokeGrace = getSessionRevokeGraceState(req);

    return res.json({
      items,
      total,
      page,
      pageSize: ACTIVE_BROWSER_SESSION_PAGE_SIZE,
      totalPages,
      search,
      searchActive,
      from: total > 0 ? startIndex + 1 : 0,
      to: total > 0 ? (searchActive ? total : endIndex) : 0,
      revokeSecurity: {
        twoFactorEnabled: !!currentUser.secret,
        requiresTwoFactor: !revokeGrace,
        graceExpiresAt: revokeGrace?.expiresAt || 0,
      },
      actionTokens: {
        revokeSession: issueActionToken(req, "POST /api/settings/sessions/revoke", {}, { ttlSeconds: 600 }),
      },
    });
  } catch (err) {
    console.error("[security] Failed to load active browser sessions:", err);
    return res.status(500).json({ error: "failed to load sessions" });
  }
});

app.post("/api/settings/sessions/revoke", sessionRevoke2faRateLimiter, async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  res.set("Cache-Control", "no-store");
  if (!requireActionTokenOr403(req, res, "POST /api/settings/sessions/revoke")) return;

  const currentUser = await findUserByEmail(req.session.user);
  if (!currentUser || !currentUser.admin) {
    return res.status(403).json({ error: "not authorized" });
  }

  if (!currentUser.secret) {
    return res.status(403).json({
      error: "Enable 2FA before revoking sessions.",
      code: "2fa-not-enabled",
    });
  }

  const sessionId = String(req.body?.sessionId || "").trim();
  if (!sessionId || sessionId.length > 255 || !/^[A-Za-z0-9_-]+$/.test(sessionId)) {
    return res.status(400).json({ error: "Invalid session id", code: "invalid-session-id" });
  }

  let graceState = getSessionRevokeGraceState(req);
  let usedFreshTwoFactor = false;

  if (!graceState) {
    const twoFactorCode = String(req.body?.twoFactorCode || "").replace(/\s+/g, "");
    if (!/^\d{6}$/.test(twoFactorCode)) {
      return res.status(403).json({ error: "A valid 6-digit code is required", code: "2fa-required" });
    }

    const verified = speakeasy.totp.verify({
      secret: currentUser.secret,
      encoding: "base32",
      token: twoFactorCode,
      window: 1,
    });

    if (!verified) {
      console.log(`[SECURITY] Session revoke denied - invalid 2FA | User: ${req.session.user} | IP: ${getRequestIp(req)} | Session: ${sessionId}`);
      return res.status(403).json({ error: "Incorrect 2FA code", code: "invalid-2fa" });
    }

    usedFreshTwoFactor = true;
  }

  let targetSession = null;
  try {
    targetSession = await callSessionStore("get", sessionId);
  } catch (err) {
    console.error("[security] Failed to read target session:", err);
    return res.status(500).json({ error: "failed to read target session" });
  }

  if (!targetSession || !targetSession.user) {
    removeActiveBrowserSessionCacheEntry(sessionId);
    invalidateActiveBrowserSessionCache();
    return res.status(404).json({ error: "Session not found or already inactive", code: "session-not-active" });
  }

  const targetRecord = buildActiveBrowserSessionRecord(sessionId, targetSession) || {
    sessionId,
    email: String(targetSession.user || "").trim().toLowerCase(),
    rememberLoginId: String(targetSession.rememberLoginId || targetSession.browserSessionAudit?.rememberLoginId || "").trim(),
  };
  const targetEmail = String(targetRecord.email || "").trim().toLowerCase();
  const targetRememberLoginId = String(targetRecord.rememberLoginId || "").trim();
  const isCurrentSession = sessionId === String(req.sessionID || "");

  if (targetRememberLoginId) {
    revokeRememberLoginRegistryEntry(targetRememberLoginId);
  } else {
    try {
      const targetUser = targetEmail ? await findUserByEmail(targetEmail) : null;
      if (targetUser?.id) {
        setRememberLoginLegacyCutoff(targetUser.id);
      }
    } catch (legacyErr) {
      console.warn("[security] Failed to persist legacy remember-login cutoff:", legacyErr?.message || legacyErr);
    }
  }

  try {
    if (isCurrentSession) {
      await new Promise((resolve, reject) => {
        req.session.destroy((err) => (err ? reject(err) : resolve()));
      });
      removeActiveBrowserSessionCacheEntry(sessionId);
      invalidateActiveBrowserSessionCache();
      res.clearCookie("adpanel.sid", {
        httpOnly: true,
        sameSite: "lax",
        secure: SESSION_COOKIE_SECURE,
        path: "/",
      });
      clearRememberLoginCookie(res);

      console.log(`[SECURITY] Browser session revoked | Admin: ${currentUser.email} | Target: ${targetEmail} | Session: ${sessionId} | Self: yes | IP: ${getRequestIp(req)}`);
      return res.json({
        ok: true,
        revoked: true,
        sessionId,
        targetEmail,
        revokedCurrentSession: true,
        graceExpiresAt: 0,
      });
    }

    await callSessionStore("destroy", sessionId);
    removeActiveBrowserSessionCacheEntry(sessionId);
    invalidateActiveBrowserSessionCache();

    let graceExpiresAt = graceState?.expiresAt || 0;
    if (usedFreshTwoFactor) {
      graceExpiresAt = setSessionRevokeGrace(req);
      try {
        await new Promise((resolve, reject) => req.session.save((err) => (err ? reject(err) : resolve())));
        graceState = getSessionRevokeGraceState(req);
      } catch (saveErr) {
        console.warn("[security] Failed to persist session revoke 2FA grace window:", saveErr?.message || saveErr);
        graceExpiresAt = 0;
      }
    }

    console.log(`[SECURITY] Browser session revoked | Admin: ${currentUser.email} | Target: ${targetEmail} | Session: ${sessionId} | Self: no | IP: ${getRequestIp(req)}`);
    return res.json({
      ok: true,
      revoked: true,
      sessionId,
      targetEmail,
      revokedCurrentSession: false,
      graceExpiresAt: graceExpiresAt || graceState?.expiresAt || 0,
    });
  } catch (err) {
    console.error("[security] Failed to revoke target session:", err);
    return res.status(500).json({ error: "failed to revoke session" });
  }
});

const PANEL_INFO_FILE = path.join(__dirname, "panel-information.json");
const PANEL_UPDATE_GITHUB_API = "https://api.github.com/repos/antonndev/ADPanel/releases";
const PANEL_AUTO_UPDATE_INTERVAL_MS = 60 * 60 * 1000;

const PROTECTED_FILES = new Set([
  "security.json",
  "templates.json",
  "webhooks-config.json",
  "pgadmin-config.json",
  "quick-actions.json",
  "nodes.json",
  "mongodb-config.json",
  "database-config.json",
  "user.json",
  "user-access.json",
  ".env",
  "store-templates.json",
  "versions.json",
  "node_modules",
  ".git",
  ".sessions",
  ".activity-logs",
  "data",
  "package-lock.json",
  "go.mod",
  "go.sum",
  "venv",
]);

const UPDATABLE_PATHS = new Set([
  "index.js",
  "db.js",
  "nodes.js",
  "init_subdomains.js",
  "panel-information.json",
  "package.json",
  "start.sh",
]);
const UPDATABLE_DIRS = ["scripts", "public", "views", "middleware", "utils", "routes"];

function readPanelInfo() {
  const data = readJson(PANEL_INFO_FILE, [{}]);
  const info = Array.isArray(data) ? (data[0] || {}) : (data || {});
  return {
    version: String(info["panel-version"] || "unknown"),
    architecture: String(info["panel-architecture"] || "unknown"),
    autoUpdate: ["1", "true", "yes", "on"].includes(String(info["panel-auto-update"] ?? "false").trim().toLowerCase()),
  };
}

function readPanelInfoObject() {
  const data = readJson(PANEL_INFO_FILE, [{}]);
  const arr = Array.isArray(data) ? data : [data || {}];
  const info = (arr[0] && typeof arr[0] === "object") ? arr[0] : {};
  return { arr, info };
}

function setPanelAutoUpdateEnabled(enabled) {
  try {
    const { arr, info } = readPanelInfoObject();
    info["panel-auto-update"] = !!enabled;
    arr[0] = info;
    return safeWriteJson(PANEL_INFO_FILE, arr);
  } catch {
    return false;
  }
}

async function fetchPanelReleases() {
  const result = await httpRequestJson(PANEL_UPDATE_GITHUB_API, "GET", {
    "Accept": "application/vnd.github.v3+json",
    "User-Agent": "ADPanel-Updater",
  });
  if (!result || result.status !== 200 || !Array.isArray(result.json)) {
    const err = new Error("failed to fetch releases from GitHub");
    err.statusCode = 502;
    throw err;
  }
  return result.json;
}

function getLatestStableRelease(releases) {
  let latestRelease = null;
  for (const release of releases) {
    if (release.draft || release.prerelease) continue;
    const tagName = String(release.tag_name || "");
    if (!tagName) continue;
    if (!latestRelease || compareVersions(latestRelease.tag_name, tagName) > 0) {
      latestRelease = release;
    }
  }
  return latestRelease;
}

async function checkPanelUpdateInternal() {
  const currentInfo = readPanelInfo();
  const currentVersion = currentInfo.version;
  const releases = await fetchPanelReleases();

  if (releases.length === 0) {
    return { updateAvailable: false, currentVersion, latestVersion: currentVersion, latestRelease: null };
  }

  const latestRelease = getLatestStableRelease(releases);
  if (!latestRelease) {
    return { updateAvailable: false, currentVersion, latestVersion: currentVersion, latestRelease: null };
  }

  const latestVersion = String(latestRelease.tag_name);
  const updateAvailable = compareVersions(currentVersion, latestVersion) > 0;
  return { updateAvailable, currentVersion, latestVersion, latestRelease };
}

async function installPanelUpdateInternal(targetVersion, source = "manual") {
  const cleanTargetVersion = String(targetVersion || "").trim();
  if (!cleanTargetVersion || !/^v?\d+\.\d+\.\d+/.test(cleanTargetVersion)) {
    const err = new Error("invalid version format");
    err.statusCode = 400;
    throw err;
  }

  const currentInfo = readPanelInfo();
  if (compareVersions(currentInfo.version, cleanTargetVersion) <= 0) {
    console.log(`[panel-update] Installing version ${cleanTargetVersion} (current: ${currentInfo.version}) — same or older version requested (${source}).`);
  }

  const releases = await fetchPanelReleases();
  const targetRelease = releases.find(r => {
    const tag = String(r.tag_name || "");
    return tag === cleanTargetVersion || tag === `v${cleanTargetVersion.replace(/^v/, "")}`;
  });

  if (!targetRelease) {
    const err = new Error(`release ${cleanTargetVersion} not found`);
    err.statusCode = 404;
    throw err;
  }

  const zipballUrl = targetRelease.zipball_url;
  if (!zipballUrl) {
    const err = new Error("no download URL available for this release");
    err.statusCode = 502;
    throw err;
  }

  const MAX_DOWNLOAD_SIZE = 200 * 1024 * 1024;
  let zipBuffer;
  try {
    zipBuffer = await httpGetRaw(zipballUrl, {
      maxBytes: MAX_DOWNLOAD_SIZE,
      timeoutMs: 120000,
      maxRedirects: 5,
    });
  } catch (dlErr) {
    console.error("[panel-update] Download failed:", dlErr);
    const err = new Error("failed to download update archive");
    err.statusCode = 502;
    throw err;
  }

  if (!zipBuffer || zipBuffer.length < 100) {
    const err = new Error("downloaded archive is empty or too small");
    err.statusCode = 502;
    throw err;
  }

  const AdmZip = require("adm-zip");
  const tmpDir = path.join(os.tmpdir(), `adpanel-update-${Date.now()}`);

  try {
    const zip = new AdmZip(zipBuffer);
    const entries = zip.getEntries();

    if (entries.length === 0) {
      const err = new Error("update archive is empty");
      err.statusCode = 502;
      throw err;
    }

    let prefix = "";
    for (const entry of entries) {
      const name = entry.entryName || "";
      if (name.includes("/")) {
        const candidate = name.split("/")[0] + "/";
        if (!prefix) prefix = candidate;
        break;
      }
    }

    const panelRoot = __dirname;
    let filesUpdated = 0;
    let filesSkipped = 0;

    for (const entry of entries) {
      if (entry.isDirectory) continue;

      const fullEntryPath = entry.entryName || "";
      const relativePath = prefix && fullEntryPath.startsWith(prefix)
        ? fullEntryPath.slice(prefix.length)
        : fullEntryPath;

      if (!relativePath) continue;

      const normalizedRelative = path.normalize(relativePath);
      if (normalizedRelative.startsWith("..") || path.isAbsolute(normalizedRelative)) {
        filesSkipped++;
        continue;
      }

      const topLevel = normalizedRelative.split(path.sep)[0];
      if (PROTECTED_FILES.has(topLevel)) {
        filesSkipped++;
        continue;
      }
      if (PROTECTED_FILES.has(normalizedRelative)) {
        filesSkipped++;
        continue;
      }

      const isUpdatableFile = UPDATABLE_PATHS.has(normalizedRelative);
      const isUpdatableDir = UPDATABLE_DIRS.some(dir =>
        normalizedRelative === dir || normalizedRelative.startsWith(dir + path.sep)
      );

      if (!isUpdatableFile && !isUpdatableDir) {
        filesSkipped++;
        continue;
      }

      const targetPath = path.join(panelRoot, normalizedRelative);

      const resolvedTarget = path.resolve(targetPath);
      if (!resolvedTarget.startsWith(path.resolve(panelRoot) + path.sep) && resolvedTarget !== path.resolve(panelRoot)) {
        filesSkipped++;
        continue;
      }

      const parentDir = path.dirname(targetPath);
      if (!fs.existsSync(parentDir)) {
        fs.mkdirSync(parentDir, { recursive: true });
      }

      fs.writeFileSync(targetPath, entry.getData());
      filesUpdated++;
    }

    const panelInfoData = readJson(PANEL_INFO_FILE, [{}]);
    const infoObj = Array.isArray(panelInfoData) ? (panelInfoData[0] || {}) : (panelInfoData || {});
    infoObj["panel-version"] = cleanTargetVersion.startsWith("v") ? cleanTargetVersion : `v${cleanTargetVersion}`;
    const newInfoArray = [infoObj];
    safeWriteJson(PANEL_INFO_FILE, newInfoArray);

    console.log(`[panel-update] Update to ${cleanTargetVersion} completed. ${filesUpdated} files updated, ${filesSkipped} files skipped (protected/excluded). Source: ${source}.`);

    return {
      ok: true,
      version: cleanTargetVersion,
      filesUpdated,
      filesSkipped,
      message: "Update installed successfully. Please restart the panel for changes to take effect.",
    };
  } finally {
    try {
      if (fs.existsSync(tmpDir)) {
        fs.rmSync(tmpDir, { recursive: true, force: true });
      }
    } catch { }
  }
}

let panelAutoUpdateInterval = null;
let panelAutoUpdateRunning = false;

async function runPanelAutoUpdateCycle(trigger = "interval") {
  if (panelAutoUpdateRunning) return;

  const panelInfo = readPanelInfo();
  if (!panelInfo.autoUpdate) return;

  panelAutoUpdateRunning = true;
  try {
    const check = await checkPanelUpdateInternal();
    if (!check.updateAvailable) {
      console.log(`[panel-auto-update] No update available (trigger: ${trigger}, current: ${check.currentVersion}).`);
      return;
    }

    console.log(`[panel-auto-update] Update found ${check.currentVersion} -> ${check.latestVersion} (trigger: ${trigger}). Installing...`);
    await installPanelUpdateInternal(check.latestVersion, `auto:${trigger}`);
    console.log(`[panel-auto-update] Installed ${check.latestVersion}. Restart panel to apply all runtime changes.`);
  } catch (err) {
    console.error("[panel-auto-update] Cycle failed:", err?.message || err);
  } finally {
    panelAutoUpdateRunning = false;
  }
}

function startPanelAutoUpdateScheduler() {
  if (panelAutoUpdateInterval) return;

  panelAutoUpdateInterval = setInterval(() => {
    runPanelAutoUpdateCycle("interval").catch(() => { });
  }, PANEL_AUTO_UPDATE_INTERVAL_MS);
  panelAutoUpdateInterval.unref();

  setTimeout(() => {
    runPanelAutoUpdateCycle("startup").catch(() => { });
  }, 10000).unref();
}

function compareVersions(a, b) {
  const parse = (v) => {
    const m = String(v || "").replace(/^v/i, "").match(/^(\d+)\.(\d+)\.(\d+)/);
    return m ? [parseInt(m[1], 10), parseInt(m[2], 10), parseInt(m[3], 10)] : [0, 0, 0];
  };
  const [a1, a2, a3] = parse(a);
  const [b1, b2, b3] = parse(b);
  if (b1 !== a1) return b1 - a1;
  if (b2 !== a2) return b2 - a2;
  return b3 - a3;
}

app.get("/api/settings/panel-info", async (req, res) => {
  try {
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
    const info = readPanelInfo();
    return res.json({
      version: info.version,
      architecture: info.architecture,
      autoUpdateEnabled: !!info.autoUpdate,
      actionTokens: {
        checkUpdate: issueActionToken(req, "POST /api/settings/panel-update/check", {}, { ttlSeconds: 300 }),
        installUpdate: issueActionToken(req, "POST /api/settings/panel-update/install", {}, { ttlSeconds: 600, oneTime: true }),
        setAutoUpdate: issueActionToken(req, "POST /api/settings/panel-update/auto", {}, { ttlSeconds: 300 }),
      },
    });
  } catch (err) {
    console.error("[panel-info] Error reading panel info:", err);
    return res.status(500).json({ error: "failed to read panel information" });
  }
});

app.post("/api/settings/panel-update/check", async (req, res) => {
  try {
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
    if (!requireActionTokenOr403(req, res, "POST /api/settings/panel-update/check")) return;
    const checkResult = await checkPanelUpdateInternal();
    const { updateAvailable, currentVersion, latestVersion } = checkResult;

    return res.json({
      updateAvailable,
      currentVersion,
      latestVersion,
      ...(updateAvailable ? {
        actionTokens: {
          installUpdate: issueActionToken(req, "POST /api/settings/panel-update/install", {}, { ttlSeconds: 600, oneTime: true }),
        },
      } : {}),
    });
  } catch (err) {
    console.error("[panel-update] Error checking for updates:", err);
    return res.status(500).json({ error: "failed to check for updates" });
  }
});

app.post("/api/settings/panel-update/install", async (req, res) => {
  try {
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
    if (!requireActionTokenOr403(req, res, "POST /api/settings/panel-update/install")) return;

    const targetVersion = String(req.body?.version || "").trim();
    const installResult = await installPanelUpdateInternal(targetVersion, "manual");
    return res.json(installResult);
  } catch (err) {
    console.error("[panel-update] Error installing update:", err);
    const status = Number(err?.statusCode) || 500;
    const message = (status >= 500) ? "update installation failed" : (err?.message || "update installation failed");
    return res.status(status).json({ error: message });
  }
});

app.post("/api/settings/panel-update/auto", async (req, res) => {
  try {
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
    if (!requireActionTokenOr403(req, res, "POST /api/settings/panel-update/auto")) return;

    const enabled = !!req.body?.enabled;
    const ok = setPanelAutoUpdateEnabled(enabled);
    if (!ok) {
      return res.status(500).json({ error: "failed to save auto update setting" });
    }

    if (enabled) {
      setTimeout(() => {
        runPanelAutoUpdateCycle("enabled-toggle").catch(() => { });
      }, 1000).unref();
    }

    return res.json({ ok: true, autoUpdateEnabled: enabled });
  } catch (err) {
    console.error("[panel-auto-update] Failed to toggle:", err);
    return res.status(500).json({ error: "failed to update auto update setting" });
  }
});

const AI_PROVIDER_CONFIGS = {
  openai: {
    name: 'OpenAI',
    keyEnv: 'OPENAI_API_KEY',
    requiresKey: true,
    chatProtocol: 'openai-compatible',
    chatEndpoint: 'https://api.openai.com/v1/chat/completions',
    modelDiscovery: 'openai-style',
    modelListUrls: ['https://api.openai.com/v1/models']
  },
  anthropic: {
    name: 'Anthropic',
    keyEnv: 'ANTHROPIC_API_KEY',
    requiresKey: true,
    chatProtocol: 'anthropic',
    chatEndpoint: 'https://api.anthropic.com/v1/messages',
    modelDiscovery: 'anthropic',
    modelListUrls: ['https://api.anthropic.com/v1/models']
  },
  'openai-compatible': {
    name: 'OpenAI Compatible',
    keyEnv: 'OPENAI_COMPATIBLE_API_KEY',
    baseUrlEnv: 'OPENAI_COMPATIBLE_BASE_URL',
    requiresKey: false,
    requiresBaseUrl: true,
    chatProtocol: 'openai-compatible',
    modelDiscovery: 'openai-compatible'
  },
  google: {
    name: 'Google AI',
    keyEnv: 'GOOGLE_AI_KEY',
    requiresKey: true,
    chatProtocol: 'google',
    modelDiscovery: 'google'
  },
  groq: {
    name: 'Groq',
    keyEnv: 'GROQ_API_KEY',
    requiresKey: true,
    chatProtocol: 'openai-compatible',
    chatEndpoint: 'https://api.groq.com/openai/v1/chat/completions',
    modelDiscovery: 'openai-style',
    modelListUrls: ['https://api.groq.com/openai/v1/models']
  },
  huggingface: {
    name: 'HuggingFace',
    keyEnv: 'HUGGINGFACE_API_KEY',
    requiresKey: true,
    chatProtocol: 'huggingface',
    chatEndpoint: 'https://router.huggingface.co/v1/chat/completions',
    modelDiscovery: 'huggingface'
  },
  together: {
    name: 'Together AI',
    keyEnv: 'TOGETHER_API_KEY',
    requiresKey: true,
    chatProtocol: 'openai-compatible',
    chatEndpoint: 'https://api.together.ai/v1/chat/completions',
    modelDiscovery: 'together',
    modelListUrls: ['https://api.together.ai/v1/models']
  },
  cohere: {
    name: 'Cohere',
    keyEnv: 'COHERE_API_KEY',
    requiresKey: true,
    chatProtocol: 'cohere',
    modelDiscovery: 'cohere'
  },
  openrouter: {
    name: 'OpenRouter',
    keyEnv: 'OPENROUTER_API_KEY',
    requiresKey: true,
    chatProtocol: 'openai-compatible',
    chatEndpoint: 'https://openrouter.ai/api/v1/chat/completions',
    modelDiscovery: 'openrouter',
    modelListUrls: ['https://openrouter.ai/api/v1/models?output_modalities=text']
  }
};

const ANTHROPIC_API_VERSION = '2023-06-01';

const AI_KEY_ENV_MAPPING = Object.fromEntries(
  Object.entries(AI_PROVIDER_CONFIGS)
    .filter(([, config]) => !!config.keyEnv)
    .map(([provider, config]) => [provider, config.keyEnv])
);



function writeEnvFileBatch(updates) {
  try {
    let lines = [];
    if (fs.existsSync(ENV_FILE_PATH)) {
      lines = fs.readFileSync(ENV_FILE_PATH, 'utf8').split('\n');
    }

    const updatedKeys = new Set();
    const newLines = [];

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) {
        newLines.push(line);
        continue;
      }
      const eqIdx = trimmed.indexOf('=');
      if (eqIdx === -1) {
        newLines.push(line);
        continue;
      }
      const key = trimmed.slice(0, eqIdx).trim();
      if (updates.hasOwnProperty(key)) {
        const cleaned = String(updates[key] ?? "").replace(/\0/g, "").replace(/[\r\n]+/g, "");
        newLines.push(`${key}="${cleaned.replace(/"/g, '\\"')}"`);
        updatedKeys.add(key);
      } else {
        newLines.push(line);
      }
    }

    for (const [key, value] of Object.entries(updates)) {
      if (!updatedKeys.has(key)) {
        const cleaned = String(value ?? "").replace(/\0/g, "").replace(/[\r\n]+/g, "");
        newLines.push(`${key}="${cleaned.replace(/"/g, '\\"')}"`);
      }
    }

    fs.writeFileSync(ENV_FILE_PATH, newLines.join('\n'), 'utf8');
    try { fs.chmodSync(ENV_FILE_PATH, 0o600); } catch { }
    return true;
  } catch (err) {
    console.error('[ai-keys] Failed to write .env:', err);
    return false;
  }
}

async function hasAgentAccess(req) {
  if (await isAdmin(req)) return true;
  const email = req.session?.user;
  if (!email) return false;
  const user = await findUserByEmail(email);
  return !!(user && user.agent_access);
}

function getAiProviderConfig(provider) {
  return AI_PROVIDER_CONFIGS[String(provider || '').trim().toLowerCase()] || null;
}

function normalizeAiSecretValue(value) {
  return String(value == null ? '' : value).trim();
}

function maskAiSecretValue(value) {
  const normalized = normalizeAiSecretValue(value);
  if (!normalized) return '';
  return normalized.length > 8 ? `${normalized.slice(0, 4)}...${normalized.slice(-4)}` : '****';
}

function normalizeOpenAiCompatibleBaseUrl(input) {
  const raw = normalizeAiSecretValue(input);
  if (!raw) return '';

  let parsed;
  try {
    parsed = new URL(raw);
  } catch {
    throw new Error('Please enter a valid Base URL.');
  }

  if (!['http:', 'https:'].includes(parsed.protocol)) {
    throw new Error('Base URL must start with http:// or https://.');
  }

  parsed.hash = '';
  parsed.search = '';

  let pathname = parsed.pathname || '';
  pathname = pathname.replace(/\/+$/, '');
  pathname = pathname.replace(/\/(?:chat\/completions|completions|responses|models)$/i, '');
  parsed.pathname = pathname || '/';

  return parsed.toString().replace(/\/$/, '');
}

function buildOpenAiCompatibleBaseUrlCandidates(input) {
  const normalized = normalizeOpenAiCompatibleBaseUrl(input);
  const parsed = new URL(normalized);
  const pathname = parsed.pathname.replace(/\/+$/, '').toLowerCase();
  const candidates = [normalized];

  if (!/\/v\d+$/.test(pathname) && !/\/openai\/v\d+$/.test(pathname)) {
    candidates.push(`${normalized}/v1`);
  }

  return Array.from(new Set(candidates.map((value) => value.replace(/\/+$/, ''))));
}

function appendPathToBaseUrl(baseUrl, path) {
  return `${String(baseUrl || '').replace(/\/+$/, '')}/${String(path || '').replace(/^\/+/, '')}`;
}

function getStoredAiProviderSettings(provider, env = null) {
  const providerConfig = getAiProviderConfig(provider);
  if (!providerConfig) return { apiKey: '', baseUrl: '' };

  const source = env || readEnvFile();
  const apiKey = providerConfig.keyEnv
    ? normalizeAiSecretValue(source[providerConfig.keyEnv] || process.env[providerConfig.keyEnv] || '')
    : '';
  const baseUrl = providerConfig.baseUrlEnv
    ? normalizeAiSecretValue(source[providerConfig.baseUrlEnv] || process.env[providerConfig.baseUrlEnv] || '')
    : '';
  return { apiKey, baseUrl };
}

function isAiProviderConfigured(provider, settings) {
  const providerConfig = getAiProviderConfig(provider);
  if (!providerConfig) return false;
  const resolved = settings || getStoredAiProviderSettings(provider);
  if (providerConfig.requiresKey !== false && !resolved.apiKey) return false;
  if (providerConfig.requiresBaseUrl && !resolved.baseUrl) return false;
  return true;
}

function buildAiProviderState(provider, settings, { includeBaseUrl = false } = {}) {
  const providerConfig = getAiProviderConfig(provider);
  const resolved = settings || getStoredAiProviderSettings(provider);
  return {
    configured: isAiProviderConfigured(provider, resolved),
    keyConfigured: !!resolved.apiKey,
    maskedKey: maskAiSecretValue(resolved.apiKey),
    baseUrlConfigured: !!resolved.baseUrl,
    baseUrl: includeBaseUrl ? resolved.baseUrl : '',
    keyRequired: providerConfig?.requiresKey !== false,
    baseUrlRequired: !!providerConfig?.requiresBaseUrl
  };
}

function buildAiProviderAuthHeaders(provider, apiKey) {
  const headers = {};
  if (provider === 'anthropic') {
    if (apiKey) {
      headers['x-api-key'] = apiKey;
    }
    headers['anthropic-version'] = ANTHROPIC_API_VERSION;
  } else if (apiKey) {
    headers['Authorization'] = `Bearer ${apiKey}`;
  }

  if (provider === 'openrouter') {
    headers['HTTP-Referer'] = process.env.APP_URL || 'https://adpanel.local';
    headers['X-Title'] = 'ADPanel';
  }

  return headers;
}

function extractAiErrorMessage(response) {
  const payload = response?.data || {};
  return payload?.error?.message || payload?.error || payload?.message || payload?.raw || `API error: ${response?.status || 'request failed'}`;
}

function sortAndDedupeModelIds(modelIds) {
  const seen = new Set();
  const unique = [];
  for (const value of modelIds || []) {
    const modelId = normalizeAiSecretValue(value);
    const key = modelId.toLowerCase();
    if (!modelId || seen.has(key)) continue;
    seen.add(key);
    unique.push(modelId);
  }
  return unique.sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
}

function isLikelyChatCapableOpenAiStyleModel(modelId) {
  const lower = String(modelId || '').toLowerCase();
  if (!lower) return false;
  if (/(^|[-_/])(embedding|rerank|re-rank|moderation|whisper|transcrib|tts|speech|audio|image)([-_/]|$)/.test(lower)) {
    return false;
  }
  if (lower.includes('omni-moderation')) return false;
  return true;
}

function parseOpenAiStyleModels(payload) {
  const entries = Array.isArray(payload?.data)
    ? payload.data
    : Array.isArray(payload?.models)
      ? payload.models
      : Array.isArray(payload)
        ? payload
        : [];

  return sortAndDedupeModelIds(
    entries
      .map((entry) => {
        if (typeof entry === 'string') return entry;
        return entry?.id || entry?.name || entry?.model || '';
      })
      .filter(isLikelyChatCapableOpenAiStyleModel)
  );
}

function parseNextLinkHeader(linkHeader) {
  const match = String(linkHeader || '').match(/<([^>]+)>\s*;\s*rel="next"/i);
  return match ? match[1] : '';
}

async function fetchPaginatedJsonArray(url, headers = {}, { maxPages = 10, timeoutMs = 15000 } = {}) {
  const items = [];
  let nextUrl = url;
  let pageCount = 0;

  while (nextUrl && pageCount < maxPages) {
    const response = await proxyAiRequest(nextUrl, { method: 'GET', headers }, timeoutMs);
    if (response.status < 200 || response.status >= 300) {
      throw new Error(extractAiErrorMessage(response));
    }
    if (!Array.isArray(response.data)) break;
    items.push(...response.data);
    nextUrl = parseNextLinkHeader(response.headers?.link);
    pageCount += 1;
  }

  return items;
}

async function fetchOpenAiStyleProviderModels(provider, settings) {
  const providerConfig = getAiProviderConfig(provider);
  const headers = buildAiProviderAuthHeaders(provider, settings.apiKey);
  let lastError = null;

  for (const url of providerConfig?.modelListUrls || []) {
    const response = await proxyAiRequest(url, { method: 'GET', headers }, 15000);
    if (response.status >= 200 && response.status < 300) {
      const models = parseOpenAiStyleModels(response.data);
      if (models.length > 0) {
        return { models };
      }
      lastError = new Error(`No chat-capable models were returned by ${providerConfig.name}.`);
      continue;
    }
    lastError = new Error(extractAiErrorMessage(response));
    if (![404, 405].includes(response.status)) break;
  }

  throw lastError || new Error(`Failed to load models from ${providerConfig?.name || provider}.`);
}

async function fetchAnthropicProviderModels(settings) {
  const headers = buildAiProviderAuthHeaders('anthropic', settings.apiKey);
  const models = [];
  let afterId = '';
  let pageCount = 0;

  while (pageCount < 10) {
    const url = new URL('https://api.anthropic.com/v1/models');
    url.searchParams.set('limit', '1000');
    if (afterId) {
      url.searchParams.set('after_id', afterId);
    }

    const response = await proxyAiRequest(url.toString(), { method: 'GET', headers }, 15000);
    if (response.status < 200 || response.status >= 300) {
      throw new Error(extractAiErrorMessage(response));
    }

    const pageModels = Array.isArray(response.data?.data)
      ? response.data.data
      : [];

    models.push(...pageModels.map((entry) => entry?.id || '').filter(Boolean));

    if (!response.data?.has_more || !response.data?.last_id) {
      break;
    }

    afterId = normalizeAiSecretValue(response.data.last_id);
    if (!afterId) break;
    pageCount += 1;
  }

  const result = sortAndDedupeModelIds(models);
  if (!result.length) {
    throw new Error('No Anthropic models were returned.');
  }
  return { models: result };
}

async function fetchOpenAiCompatibleModels(settings) {
  const headers = buildAiProviderAuthHeaders('openai-compatible', settings.apiKey);
  let lastError = null;

  for (const rootUrl of buildOpenAiCompatibleBaseUrlCandidates(settings.baseUrl)) {
    const response = await proxyAiRequest(appendPathToBaseUrl(rootUrl, 'models'), { method: 'GET', headers }, 15000);
    if (response.status >= 200 && response.status < 300) {
      const models = parseOpenAiStyleModels(response.data);
      if (models.length > 0) {
        return { models, resolvedBaseUrl: rootUrl };
      }
      lastError = new Error('The OpenAI-compatible API did not return any chat-capable models.');
      continue;
    }
    lastError = new Error(extractAiErrorMessage(response));
    if (![404, 405].includes(response.status)) break;
  }

  throw lastError || new Error('Failed to load models from the OpenAI-compatible API.');
}

async function fetchGoogleProviderModels(settings) {
  const models = [];
  let pageToken = '';
  let pageCount = 0;

  do {
    const url = new URL('https://generativelanguage.googleapis.com/v1beta/models');
    url.searchParams.set('key', settings.apiKey);
    url.searchParams.set('pageSize', '1000');
    if (pageToken) {
      url.searchParams.set('pageToken', pageToken);
    }

    const response = await proxyAiRequest(url.toString(), { method: 'GET' }, 15000);
    if (response.status < 200 || response.status >= 300) {
      throw new Error(extractAiErrorMessage(response));
    }

    for (const entry of response.data?.models || []) {
      const methods = Array.isArray(entry?.supportedGenerationMethods)
        ? entry.supportedGenerationMethods
        : Array.isArray(entry?.supportedActions)
          ? entry.supportedActions
          : [];
      if (!methods.includes('generateContent')) continue;
      models.push(String(entry?.name || '').replace(/^models\//, ''));
    }

    pageToken = normalizeAiSecretValue(response.data?.nextPageToken);
    pageCount += 1;
  } while (pageToken && pageCount < 10);

  const result = sortAndDedupeModelIds(models);
  if (!result.length) {
    throw new Error('No Gemini models with generateContent support were returned.');
  }
  return { models: result };
}

async function fetchTogetherProviderModels(settings) {
  const headers = buildAiProviderAuthHeaders('together', settings.apiKey);
  const response = await proxyAiRequest('https://api.together.ai/v1/models', { method: 'GET', headers }, 15000);
  if (response.status < 200 || response.status >= 300) {
    throw new Error(extractAiErrorMessage(response));
  }

  const entries = Array.isArray(response.data)
    ? response.data
    : Array.isArray(response.data?.data)
      ? response.data.data
      : [];

  const result = sortAndDedupeModelIds(
    entries
      .filter((entry) => ['chat', 'language', 'code'].includes(String(entry?.type || '').toLowerCase()))
      .map((entry) => entry?.id || '')
  );

  if (!result.length) {
    throw new Error('No Together chat-capable models were returned.');
  }
  return { models: result };
}

async function fetchCohereProviderModels(settings) {
  const headers = buildAiProviderAuthHeaders('cohere', settings.apiKey);
  const response = await proxyAiRequest('https://api.cohere.ai/v1/models', { method: 'GET', headers }, 15000);
  if (response.status < 200 || response.status >= 300) {
    throw new Error(extractAiErrorMessage(response));
  }

  const result = sortAndDedupeModelIds(
    (response.data?.models || [])
      .filter((entry) => {
        if (entry?.is_deprecated) return false;
        const endpoints = Array.isArray(entry?.endpoints) ? entry.endpoints : [];
        const defaultEndpoints = Array.isArray(entry?.default_endpoints) ? entry.default_endpoints : [];
        const features = Array.isArray(entry?.features) ? entry.features : [];
        return endpoints.includes('chat') || defaultEndpoints.includes('chat') || features.includes('chat-completions');
      })
      .map((entry) => entry?.name || '')
  );

  if (!result.length) {
    throw new Error('No Cohere chat-capable models were returned.');
  }
  return { models: result };
}

async function fetchOpenRouterProviderModels(settings) {
  const headers = buildAiProviderAuthHeaders('openrouter', settings.apiKey);
  const response = await proxyAiRequest('https://openrouter.ai/api/v1/models?output_modalities=text', { method: 'GET', headers }, 15000);
  if (response.status < 200 || response.status >= 300) {
    throw new Error(extractAiErrorMessage(response));
  }

  const result = sortAndDedupeModelIds(
    (response.data?.data || [])
      .filter((entry) => {
        const architecture = entry?.architecture || {};
        const inputModalities = Array.isArray(architecture.input_modalities)
          ? architecture.input_modalities.map((value) => String(value).toLowerCase())
          : [];
        const outputModalities = Array.isArray(architecture.output_modalities)
          ? architecture.output_modalities.map((value) => String(value).toLowerCase())
          : [];
        const modality = String(architecture.modality || '').toLowerCase();
        const acceptsText = inputModalities.length === 0 || inputModalities.includes('text');
        const emitsText = outputModalities.length > 0 ? outputModalities.includes('text') : modality.endsWith('->text');
        return acceptsText && emitsText;
      })
      .map((entry) => entry?.id || '')
  );

  if (!result.length) {
    throw new Error('No OpenRouter text-output models were returned.');
  }
  return { models: result };
}

function isLikelyHuggingFaceChatModel(entry) {
  const pipelineTag = String(entry?.pipeline_tag || '').toLowerCase();
  const tags = Array.isArray(entry?.tags) ? entry.tags.map((tag) => String(tag).toLowerCase()) : [];
  const modelId = String(entry?.id || entry?.modelId || '').toLowerCase();

  if (pipelineTag === 'image-text-to-text') return true;
  if (pipelineTag !== 'text-generation') return false;
  if (tags.includes('conversational')) return true;
  return /(chat|instruct|assistant|vision)/.test(modelId);
}

async function fetchHuggingFaceProviderModels(settings) {
  const headers = buildAiProviderAuthHeaders('huggingface', settings.apiKey);
  const textModels = await fetchPaginatedJsonArray(
    'https://huggingface.co/api/models?inference_provider=all&pipeline_tag=text-generation&limit=200',
    headers,
    { maxPages: 10, timeoutMs: 15000 }
  );
  const visionModels = await fetchPaginatedJsonArray(
    'https://huggingface.co/api/models?inference_provider=all&pipeline_tag=image-text-to-text&limit=200',
    headers,
    { maxPages: 10, timeoutMs: 15000 }
  );

  const result = sortAndDedupeModelIds(
    [...textModels, ...visionModels]
      .filter(isLikelyHuggingFaceChatModel)
      .map((entry) => entry?.id || entry?.modelId || '')
  );

  if (!result.length) {
    throw new Error('No Hugging Face chat-capable models were returned.');
  }
  return { models: result };
}

async function fetchAvailableAiModels(provider, settings) {
  const providerConfig = getAiProviderConfig(provider);
  if (!providerConfig) {
    throw new Error('Unknown AI provider.');
  }

  switch (providerConfig.modelDiscovery) {
    case 'anthropic':
      return fetchAnthropicProviderModels(settings);
    case 'openai-compatible':
      return fetchOpenAiCompatibleModels(settings);
    case 'google':
      return fetchGoogleProviderModels(settings);
    case 'together':
      return fetchTogetherProviderModels(settings);
    case 'cohere':
      return fetchCohereProviderModels(settings);
    case 'openrouter':
      return fetchOpenRouterProviderModels(settings);
    case 'huggingface':
      return fetchHuggingFaceProviderModels(settings);
    case 'openai-style':
    default:
      return fetchOpenAiStyleProviderModels(provider, settings);
  }
}

app.get("/api/ai/keys", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  if (!(await hasAgentAccess(req))) return res.status(403).json({ error: "no agent access" });

  const env = readEnvFile();
  const configured = {};
  const canManageProviders = await isAdmin(req);

  for (const provider of Object.keys(AI_PROVIDER_CONFIGS)) {
    configured[provider] = buildAiProviderState(provider, getStoredAiProviderSettings(provider, env), {
      includeBaseUrl: canManageProviders
    });
  }

  const actionTokens = {
    setKey: issueActionToken(req, "POST /api/ai/keys", {}, { ttlSeconds: 300 })
  };
  for (const provider of Object.keys(AI_PROVIDER_CONFIGS)) {
    actionTokens[`deleteKey_${provider}`] = issueActionToken(req, "DELETE /api/ai/keys/:provider", { provider }, { ttlSeconds: 120, oneTime: true });
  }

  return res.json({ ok: true, providers: configured, actionTokens });
});



app.post("/api/ai/keys", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  if (!(await isAdmin(req))) return res.status(403).json({ error: "admin required to configure AI keys" });
  if (!requireActionTokenOr403(req, res, "POST /api/ai/keys")) return;

  const { provider, key, baseUrl } = req.body || {};
  const providerLower = String(provider || '').toLowerCase();
  const providerConfig = getAiProviderConfig(providerLower);

  if (!providerConfig) return res.status(400).json({ error: "invalid provider" });

  try {
    const env = readEnvFile();
    const storedSettings = getStoredAiProviderSettings(providerLower, env);
    const nextSettings = {
      apiKey: normalizeAiSecretValue(key) || storedSettings.apiKey,
      baseUrl: providerConfig.requiresBaseUrl
        ? (normalizeAiSecretValue(baseUrl) || storedSettings.baseUrl)
        : ''
    };

    if (providerConfig.requiresKey !== false && !nextSettings.apiKey) {
      return res.status(400).json({ error: `Please enter a valid ${providerConfig.name} API key.` });
    }

    if (providerConfig.requiresBaseUrl && !nextSettings.baseUrl) {
      return res.status(400).json({ error: 'Please enter a valid Base URL.' });
    }

    if (providerConfig.requiresBaseUrl) {
      nextSettings.baseUrl = normalizeOpenAiCompatibleBaseUrl(nextSettings.baseUrl);
    }

    const discovery = await fetchAvailableAiModels(providerLower, nextSettings);
    if (discovery?.resolvedBaseUrl) {
      nextSettings.baseUrl = discovery.resolvedBaseUrl;
    }
    if (!Array.isArray(discovery?.models) || discovery.models.length === 0) {
      return res.status(400).json({ error: 'No models were returned for this provider.' });
    }

    const envUpdates = {};
    if (providerConfig.keyEnv) envUpdates[providerConfig.keyEnv] = nextSettings.apiKey || '';
    if (providerConfig.baseUrlEnv) envUpdates[providerConfig.baseUrlEnv] = nextSettings.baseUrl || '';

    const success = writeEnvFileBatch(envUpdates);
    if (!success) {
      return res.status(500).json({ error: 'failed to save provider configuration' });
    }

    if (providerConfig.keyEnv) {
      if (nextSettings.apiKey) {
        process.env[providerConfig.keyEnv] = nextSettings.apiKey;
      } else {
        delete process.env[providerConfig.keyEnv];
      }
    }
    if (providerConfig.baseUrlEnv) {
      if (nextSettings.baseUrl) {
        process.env[providerConfig.baseUrlEnv] = nextSettings.baseUrl;
      } else {
        delete process.env[providerConfig.baseUrlEnv];
      }
    }

    return res.json({
      ok: true,
      provider: providerLower,
      models: discovery.models,
      providerState: buildAiProviderState(providerLower, nextSettings, { includeBaseUrl: true })
    });
  } catch (err) {
    console.error(`[ai-keys] Failed to save ${providerLower}:`, err.message);
    return res.status(400).json({ error: err.message || 'failed to save provider configuration' });
  }
});

app.delete("/api/ai/keys/:provider", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  if (!(await isAdmin(req))) return res.status(403).json({ error: "admin required to configure AI keys" });
  if (!requireActionTokenOr403(req, res, "DELETE /api/ai/keys/:provider", { provider: String(req.params.provider || '').toLowerCase() })) return;

  const provider = String(req.params.provider || '').toLowerCase();
  const providerConfig = getAiProviderConfig(provider);

  if (!providerConfig) return res.status(400).json({ error: "invalid provider" });

  const envUpdates = {};
  if (providerConfig.keyEnv) envUpdates[providerConfig.keyEnv] = '';
  if (providerConfig.baseUrlEnv) envUpdates[providerConfig.baseUrlEnv] = '';

  const success = writeEnvFileBatch(envUpdates);

  if (!success) {
    return res.status(500).json({ error: "failed to remove provider configuration" });
  }

  if (providerConfig.keyEnv) delete process.env[providerConfig.keyEnv];
  if (providerConfig.baseUrlEnv) delete process.env[providerConfig.baseUrlEnv];

  return res.json({ ok: true, provider });
});

async function proxyAiRequest(url, options, timeoutMs = 60000) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(url);
    const isHttps = parsedUrl.protocol === 'https:';
    const httpModule = isHttps ? https : httpMod;

    const reqOptions = {
      hostname: parsedUrl.hostname,
      port: parsedUrl.port || (isHttps ? 443 : 80),
      path: parsedUrl.pathname + parsedUrl.search,
      method: options.method || 'POST',
      headers: options.headers || {},
      timeout: timeoutMs
    };

    const req = httpModule.request(reqOptions, (res) => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          resolve({ status: res.statusCode, data: json, headers: res.headers });
        } catch {
          resolve({ status: res.statusCode, data: { raw: data }, headers: res.headers });
        }
      });
    });

    req.on('error', reject);
    req.on('timeout', () => {
      req.destroy();
      reject(new Error('Request timeout'));
    });

    if (options.body) {
      req.write(options.body);
    }
    req.end();
  });
}

const BOT_AI_TOOL_NAMES = new Set(BOT_TOOL_DEFINITIONS.map((tool) => String(tool?.name || "").trim()));
const BOT_AI_MAX_CONTEXT_MESSAGES = 18;
const BOT_AI_MAX_TOOL_RESULT_CHARS = 14000;
const BOT_AI_MAX_CONSOLE_LINES = 100;
const BOT_AI_DEFAULT_CONSOLE_LINES = 60;
const BOT_AI_MAX_BACKUPS = 50;

function normalizeAiConversationMessages(messages) {
  return (Array.isArray(messages) ? messages : [])
    .slice(-BOT_AI_MAX_CONTEXT_MESSAGES)
    .map((entry) => {
      const role = String(entry?.role || "").trim().toLowerCase();
      if (!["system", "user", "assistant"].includes(role)) return null;
      const content = typeof entry?.content === "string" ? entry.content : String(entry?.content || "");
      const message = {
        role,
        content: content.slice(0, 32000),
      };
      if (typeof entry?.image === "string" && entry.image.startsWith("data:image/")) {
        message.image = entry.image;
      }
      return message;
    })
    .filter((entry) => entry && (entry.content || entry.image));
}

function stripBotToolPlanMarkup(value) {
  return String(value || "").replace(/<adpanel_tool_plan>[\s\S]*?<\/adpanel_tool_plan>/gi, "").trim();
}

function isWeakBotAiReply(value) {
  const text = String(value || "").trim();
  if (!text) return true;
  if (/^[`'"“”‘’()[\]{}<>.,:;!?*_~\\/\-|+=\s]+$/.test(text)) return true;
  return /^(?:done|ok|okay|sure|completed|finished|all set|it is done|i did it)\.?$/i.test(text);
}

function normalizeBotPowerAction(value) {
  const action = String(value || "").trim().toLowerCase();
  if (action === "run" || action === "boot") return "start";
  if (["start", "stop", "restart", "kill"].includes(action)) return action;
  return "";
}

function looksLikeLeakedBotReasoning(value) {
  const normalized = String(value || "").replace(/\s+/g, " ").trim();
  if (!normalized) return false;
  const leakedInstructionTalk = [
    /\bwait,\s*the prompt says\b/i,
    /\bthe prompt says\b/i,
    /\bprevious reply was invalid\b/i,
    /\bentire reply must be one\b/i,
    /<adpanel_tool_plan>/i,
    /\bcurrent page server is\b/i,
    /\bcurrent page server:\b/i,
    /\bthe user wants to\b/i,
    /\bi have the\b/i,
    /\bi can use\b/i,
    /\bi should use\b/i,
    /\bi need to use\b/i,
    /\blet'?s refine\b/i,
    /\brespond with exactly one\b/i,
    /\bi need to check\b/i,
    /\btool which can be used\b/i,
  ].some((pattern) => pattern.test(normalized));
  const leakedToolReference = /\b(?:power_server|inspect_server|query_console|send_console_command|list_backups|create_backup|restore_backup|delete_backup)\b/i.test(normalized);
  const truncatedReasoning = /(?:i should use the|i can use the|i need to use the)\s*`?$/i.test(normalized);
  return leakedInstructionTalk || (leakedToolReference && /\b(?:the user wants to|i have the|i can use|i should use|i need to use|let'?s refine|i need to check|tool which can be used)\b/i.test(normalized)) || truncatedReasoning;
}

function sanitizeBotVisibleReplyText(value) {
  const cleaned = String(value || "")
    .replace(/<think>[\s\S]*?<\/think>/gi, " ")
    .replace(/<thinking>[\s\S]*?<\/thinking>/gi, " ")
    .replace(/^\s*(?:thought|thinking|reasoning|analysis)\s*:\s*[\s\S]*$/gim, " ")
    .trim();
  if (!cleaned) return "";
  if (/^[`'"“”‘’()[\]{}<>.,:;!?*_~\\/\-|+=\s]+$/.test(cleaned)) return "";
  if (looksLikeLeakedBotReasoning(cleaned)) return "";
  return cleaned;
}

function finalizeBotAssistantReply(candidateReply, lastToolName = "", lastToolResult = null) {
  let finalReply = sanitizeBotVisibleReplyText(candidateReply);
  if (isWeakBotAiReply(finalReply) && lastToolResult) {
    finalReply = sanitizeBotVisibleReplyText(buildBotReplyFromToolResult(lastToolName, lastToolResult));
  }
  if (!finalReply && lastToolResult) {
    finalReply = sanitizeBotVisibleReplyText(buildBotReplyFromToolResult(lastToolName, lastToolResult));
  }
  return finalReply;
}

function needsBotToolPlanRepair(value) {
  const raw = String(value || "").trim();
  if (!raw) return false;
  if (looksLikeLeakedBotReasoning(raw)) return true;
  return /\b(?:power_server|inspect_server|query_console|send_console_command|list_backups|create_backup|restore_backup|delete_backup)\b/i.test(raw);
}

function extractBotAssistantServerCandidate(value, currentServerName) {
  const raw = String(value || "").trim();
  const candidates = [
    raw.match(/\bcurrent page server\s*:\s*([a-zA-Z0-9._-]+)/i)?.[1],
    raw.match(/\bserver\s+(?:is|:)\s*([a-zA-Z0-9._-]+)/i)?.[1],
    raw.match(/\bon\s+([a-zA-Z0-9._-]+)\b/i)?.[1],
  ];

  for (const candidate of candidates) {
    const normalized = String(candidate || "").trim();
    if (normalized) return normalized;
  }
  return String(currentServerName || "").trim();
}

function extractBotAssistantQuotedFragment(value) {
  const raw = String(value || "");
  const match = raw.match(/[`"']([^`"']+)[`"']/);
  return String(match?.[1] || "").trim();
}

function decodeBotAssistantJsonStringFragment(value) {
  const raw = String(value || "");
  if (!raw) return "";
  try {
    return String(JSON.parse(`"${raw}"`) || "").trim();
  } catch {
    return raw
      .replace(/\\n/g, "\n")
      .replace(/\\r/g, "\r")
      .replace(/\\t/g, "\t")
      .replace(/\\(["'`\\/])/g, "$1")
      .trim();
  }
}

function extractBotAssistantNamedStringArg(value, key) {
  const raw = String(value || "");
  const safeKey = String(key || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  if (!raw || !safeKey) return "";

  const doubleQuoted = raw.match(new RegExp(`["']?${safeKey}["']?\\s*:\\s*"((?:\\\\.|[^"\\\\]){1,1024})"`, "i"));
  if (doubleQuoted?.[1]) return decodeBotAssistantJsonStringFragment(doubleQuoted[1]);

  const singleQuoted = raw.match(new RegExp(`["']?${safeKey}["']?\\s*:\\s*'([^'\\r\\n]{1,1024})'`, "i"));
  if (singleQuoted?.[1]) return String(singleQuoted[1]).trim();

  const backticked = raw.match(new RegExp(`["']?${safeKey}["']?\\s*:\\s*\`([^\`\\r\\n]{1,1024})\``, "i"));
  if (backticked?.[1]) return String(backticked[1]).trim();

  return "";
}

function detectBotAssistantPowerAction(value) {
  const text = String(value || "").trim().toLowerCase();
  if (!text) return "";
  if (/\bforce\s*stop|hard\s*stop|kill|terminate\b/.test(text)) return "kill";
  if (/\brestart|reboot\b/.test(text)) return "restart";
  if (/\bstop|shutdown\b/.test(text)) return "stop";
  if (/\bstart|run|boot\b/.test(text)) return "start";
  return "";
}

function extractBotAssistantRequestedConsoleLimit(value) {
  const raw = String(value || "");
  const match = raw.match(/\b(\d{1,3})\s*(?:console\s*)?(?:lines?|entries?)\b/i);
  if (!match) return BOT_AI_DEFAULT_CONSOLE_LINES;
  const parsed = parseInt(match[1], 10);
  if (!Number.isFinite(parsed)) return BOT_AI_DEFAULT_CONSOLE_LINES;
  return Math.min(Math.max(parsed, 10), BOT_AI_MAX_CONSOLE_LINES);
}

function getLatestBotUserMessageContent(messages) {
  const list = Array.isArray(messages) ? messages : [];
  for (let i = list.length - 1; i >= 0; i -= 1) {
    const entry = list[i];
    if (String(entry?.role || "").trim().toLowerCase() === "user") {
      return String(entry?.content || "").trim();
    }
  }
  return "";
}

function extractExplicitBotConsoleCommandRequest(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";

  const explicitIntent = /\b(?:run|send|execute|type|write|issue|dispatch|trimite|ruleaza|executa|scrie|baga|da)\b/i.test(raw)
    && /\b(?:console|consola|command|comanda|cmd)\b/i.test(raw);
  if (!explicitIntent) return "";

  const quoted = raw.match(/(?:run|send|execute|type|write|issue|dispatch|trimite|ruleaza|executa|scrie|baga|da)[\s\S]{0,120}?(?:console|consola|command|comanda|cmd)[\s\S]{0,80}?[`"']([^`"'\r\n]{1,512})[`"']/i)
    || raw.match(/(?:run|send|execute|type|write|issue|dispatch|trimite|ruleaza|executa|scrie|baga|da)[\s\S]{0,80}?[`"']([^`"'\r\n]{1,512})[`"'][\s\S]{0,120}?(?:console|consola|command|comanda|cmd)/i)
    || raw.match(/(?:console|consola|command|comanda|cmd)[\s\S]{0,80}?[`"']([^`"'\r\n]{1,512})[`"']/i);
  if (quoted?.[1]) return quoted[1].trim();

  const colon = raw.match(/(?:console\s*command|command|cmd|comanda(?:\s+(?:in|la|pe)\s+consola)?|(?:in|la|pe)\s+consola)\s*[:=-]\s*([^\r\n]{1,512})/i);
  if (colon?.[1]) return colon[1].trim();

  return "";
}

function buildBotToolCallFromAssistantToolName(toolName, rawValue, currentServerName) {
  const raw = String(rawValue || "").trim();
  if (!raw) return null;
  const server = extractBotAssistantServerCandidate(raw, currentServerName);
  const quoted = extractBotAssistantQuotedFragment(raw);
  const normalizedToolName = String(toolName || "").trim();

  switch (normalizedToolName) {
    case "inspect_server":
      return { name: "inspect_server", args: { server } };
    case "power_server": {
      const action = detectBotAssistantPowerAction(raw);
      if (!action) return null;
      return { name: "power_server", args: { server, action } };
    }
    case "query_console":
      return {
        name: "query_console",
        args: {
          server,
          limit: extractBotAssistantRequestedConsoleLimit(raw),
        },
      };
    case "list_backups":
      return { name: "list_backups", args: { server } };
    case "create_backup":
      return { name: "create_backup", args: { server } };
    case "send_console_command": {
      const command = extractBotAssistantNamedStringArg(raw, "command") || quoted;
      return command ? { name: "send_console_command", args: { server, command } } : null;
    }
    case "restore_backup":
      return quoted ? { name: "restore_backup", args: { server, backup: quoted } } : null;
    case "delete_backup":
      return quoted ? { name: "delete_backup", args: { server, backup: quoted } } : null;
    default:
      return null;
  }
}

function salvageBotToolCallsFromAssistantText(value, currentServerName) {
  const raw = String(value || "").trim();
  if (!raw) return [];

  const parsedPlan = parseBotAssistantToolPlan(raw);
  if (parsedPlan && Array.isArray(parsedPlan.tool_calls) && parsedPlan.tool_calls.length > 0) {
    const seenPlanCalls = new Set();
    return parsedPlan.tool_calls
      .filter((entry) => entry && BOT_AI_TOOL_NAMES.has(String(entry.name || "").trim()))
      .map((entry) => ({
        name: String(entry.name || "").trim(),
        args: entry.args && typeof entry.args === "object" && !Array.isArray(entry.args) ? entry.args : {},
      }))
      .filter((entry) => {
        const key = `${entry.name}:${JSON.stringify(entry.args || {})}`;
        if (seenPlanCalls.has(key)) return false;
        seenPlanCalls.add(key);
        return true;
      });
  }

  const toolMatches = [];
  const patterns = [
    { name: "inspect_server", regex: /\binspect_server\b/gi },
    { name: "power_server", regex: /\bpower_server\b/gi },
    { name: "query_console", regex: /\bquery_console\b/gi },
    { name: "send_console_command", regex: /\bsend_console_command\b/gi },
    { name: "list_backups", regex: /\blist_backups\b/gi },
    { name: "create_backup", regex: /\bcreate_backup\b/gi },
    { name: "restore_backup", regex: /\brestore_backup\b/gi },
    { name: "delete_backup", regex: /\bdelete_backup\b/gi },
  ];

  for (const pattern of patterns) {
    pattern.regex.lastIndex = 0;
    let match;
    while ((match = pattern.regex.exec(raw)) !== null) {
      toolMatches.push({ index: match.index, name: pattern.name });
    }
  }

  if (!toolMatches.length) {
    return [];
  }

  toolMatches.sort((left, right) => left.index - right.index);

  const seen = new Set();
  const toolCalls = [];
  for (const match of toolMatches) {
    const toolCall = buildBotToolCallFromAssistantToolName(match.name, raw, currentServerName);
    if (!toolCall) continue;
    const dedupeKey = `${toolCall.name}:${JSON.stringify(toolCall.args || {})}`;
    if (seen.has(dedupeKey)) continue;
    seen.add(dedupeKey);
    toolCalls.push(toolCall);
  }

  return toolCalls;
}

function salvageBotToolCallFromAssistantText(value, currentServerName) {
  return salvageBotToolCallsFromAssistantText(value, currentServerName)[0] || null;
}

function describeBotPermissionKey(permKey) {
  const labels = {
    server_start: "start this server",
    server_stop: "stop this server",
    console_read: "read the console",
    console_write: "send console commands",
    backups_view: "view backups",
    backups_create: "create or restore backups",
    backups_delete: "delete backups",
  };
  return labels[permKey] || permKey.replace(/_/g, " ");
}

function buildBotServerSummary(entry, perms = null) {
  return {
    name: entry?.name || null,
    displayName: entry?.displayName || entry?.name || null,
    status: entry?.status || "unknown",
    template: entry?.template || "custom",
    permissions: perms || undefined,
  };
}

async function getAccessibleServerEntriesForAi(userEmail, isAdminUser = false) {
  const allServers = (await loadServersIndex()) || [];
  if (isAdminUser) {
    return allServers.filter((entry) => entry?.name);
  }

  const accessList = await getAccessListForEmail(userEmail);
  const accessSet = new Set((Array.isArray(accessList) ? accessList : []).map((entry) => String(entry || "").trim().toLowerCase()));
  if (!accessSet.size) return [];
  if (accessSet.has("all")) {
    return allServers.filter((entry) => entry?.name);
  }

  return allServers.filter((entry) => accessSet.has(String(entry?.name || "").trim().toLowerCase()));
}

function sanitizeBotToolArgs(toolName, rawArgs) {
  const args = rawArgs && typeof rawArgs === "object" && !Array.isArray(rawArgs) ? rawArgs : {};

  switch (String(toolName || "").trim()) {
    case "inspect_server":
    case "list_backups":
      return { server: String(args.server || args.query || "").trim() };
    case "power_server":
      return {
        server: String(args.server || args.query || "").trim(),
        action: normalizeBotPowerAction(args.action),
      };
    case "query_console":
      return {
        server: String(args.server || args.query || "").trim(),
        question: String(args.question || "").trim().slice(0, 300),
        limit: Math.min(Math.max(parseInt(args.limit, 10) || BOT_AI_DEFAULT_CONSOLE_LINES, 10), BOT_AI_MAX_CONSOLE_LINES),
      };
    case "send_console_command":
      return {
        server: String(args.server || args.query || "").trim(),
        command: String(args.command || "").trim(),
      };
    case "create_backup":
      return {
        server: String(args.server || args.query || "").trim(),
        name: String(args.name || "").trim().slice(0, 120),
        description: String(args.description || "").trim().slice(0, 280),
      };
    case "restore_backup":
      return {
        server: String(args.server || args.query || "").trim(),
        backup: String(args.backup || args.backupId || args.name || "").trim(),
        deleteOldFiles: !!args.deleteOldFiles,
      };
    case "delete_backup":
      return {
        server: String(args.server || args.query || "").trim(),
        backup: String(args.backup || args.backupId || args.name || "").trim(),
      };
    default:
      return {};
  }
}

function isSameBotServer(left, right) {
  return String(left || "").trim().toLowerCase() === String(right || "").trim().toLowerCase();
}

async function getBotConsoleHistorySnapshot(serverName, limit = BOT_AI_DEFAULT_CONSOLE_LINES) {
  const safeLimit = Math.min(Math.max(parseInt(limit, 10) || BOT_AI_DEFAULT_CONSOLE_LINES, 10), BOT_AI_MAX_CONSOLE_LINES);
  const snapshot = await getConsoleHistorySnapshot(serverName, safeLimit, { includeCommands: true });
  return snapshot.lines.map((item) => ({
    line: String(item?.line || "").slice(0, 400),
    ts: Number(item?.ts || 0) || Date.now(),
    source: String(item?.source || "output"),
  }));
}

function summarizeBackupsForBot(backups) {
  return (Array.isArray(backups) ? backups : []).slice(0, BOT_AI_MAX_BACKUPS).map((backup) => ({
    id: backup?.id || backup?.uuid || null,
    name: backup?.name || backup?.archive_name || backup?.archiveName || null,
    size: backup?.size ?? null,
    completedAt: backup?.completed_at || backup?.completedAt || backup?.created_at || backup?.createdAt || null,
    locked: !!backup?.is_locked,
  }));
}

function buildBotToolResultContextMessage(toolResults) {
  const compactResults = (Array.isArray(toolResults) ? toolResults : []).map((entry) => {
    const result = entry?.result && typeof entry.result === "object" ? { ...entry.result } : {};
    if (Array.isArray(result.lines)) {
      result.lines = result.lines.slice(-BOT_AI_MAX_CONSOLE_LINES);
    }
    if (Array.isArray(result.backups)) {
      result.backups = result.backups.slice(0, BOT_AI_MAX_BACKUPS);
    }
    if (Array.isArray(result.servers)) {
      result.servers = result.servers.slice(0, 50);
    }
    return {
      name: String(entry?.name || "").trim(),
      result,
    };
  });

  const serialized = JSON.stringify(compactResults);
  const payload = serialized.length > BOT_AI_MAX_TOOL_RESULT_CHARS
    ? `${serialized.slice(0, BOT_AI_MAX_TOOL_RESULT_CHARS)}...[truncated]`
    : serialized;

  return [
    "Verified ADPanel native tool results are below.",
    "Use them as the source of truth.",
    "If a tool returned ok false, explain the failure briefly and do not claim success.",
    "If a tool returned raw console lines, analyze them directly and answer the user's question.",
    "If more verified information or action is still needed, return another tool plan immediately instead of narrating a plan.",
    payload,
  ].join(" ");
}

function buildBotContinueAfterToolInstruction() {
  return [
    "Continue solving the user's request.",
    "If you need another native action or more verified data, reply with exactly one <adpanel_tool_plan> JSON block and nothing else.",
    "Otherwise reply with one short user-facing answer only.",
    "Do not narrate planning, tool selection, or hidden reasoning.",
  ].join(" ");
}

function buildSyntheticNativeToolCalls(toolCalls, prefix = "synthetic_tool_call") {
  return (Array.isArray(toolCalls) ? toolCalls : [])
    .filter((entry) => entry && typeof entry === "object" && String(entry.name || "").trim())
    .map((entry, index) => ({
      id: `${prefix}_${index + 1}`,
      type: "function",
      function: {
        name: String(entry.name || "").trim(),
        arguments: JSON.stringify(entry.args && typeof entry.args === "object" ? entry.args : {}),
      },
    }));
}

function summarizeBotConsoleLinesForFallback(lines) {
  const normalizedLines = (Array.isArray(lines) ? lines : [])
    .map((entry) => String(entry?.line || "").trim())
    .filter(Boolean);
  if (!normalizedLines.length) return "";

  const interestingLines = normalizedLines.filter((line) => (
    /\b(?:error|exception|failed|invalid|warning|warn|terminated|crash|denied|refused|unable)\b/i.test(line)
  ));
  const selectedLines = (interestingLines.length ? interestingLines : normalizedLines).slice(-3);
  if (!selectedLines.length) return "";

  const snippet = selectedLines
    .map((line) => (line.length > 180 ? `${line.slice(0, 177)}...` : line))
    .join(" | ");
  return `Recent console output: ${snippet}`;
}

function buildBotReplyFromToolHistory(toolHistory) {
  const history = Array.isArray(toolHistory) ? toolHistory : [];
  if (!history.length) return "";

  const latestSuccessfulInspect = [...history].reverse().find((entry) => (
    entry?.name === "inspect_server" && entry?.result && entry.result.ok !== false
  ));
  const latestSuccessfulConsole = [...history].reverse().find((entry) => (
    entry?.name === "query_console" && entry?.result && entry.result.ok !== false
  ));

  const parts = [];
  if (latestSuccessfulInspect?.result?.server?.name) {
    const server = latestSuccessfulInspect.result.server;
    parts.push(`${server.displayName || server.name} is ${server.status || "unknown"}.`);
  }

  if (latestSuccessfulConsole?.result?.lines) {
    const consoleSummary = summarizeBotConsoleLinesForFallback(latestSuccessfulConsole.result.lines);
    if (consoleSummary) parts.push(consoleSummary);
  }

  if (parts.length) {
    return parts.join(" ");
  }

  const lastEntry = [...history].reverse().find((entry) => entry?.result);
  if (!lastEntry) return "";
  return buildBotReplyFromToolResult(lastEntry.name, lastEntry.result);
}

const OPENAI_STYLE_NATIVE_BOT_TOOL_PROVIDERS = new Set([
  "openai",
  "openai-compatible",
  "groq",
  "together",
  "openrouter",
  "huggingface",
]);
const ANTHROPIC_NATIVE_BOT_TOOL_PROVIDERS = new Set(["anthropic"]);
const COHERE_NATIVE_BOT_TOOL_PROVIDERS = new Set(["cohere"]);
const GOOGLE_NATIVE_BOT_TOOL_PROVIDERS = new Set(["google"]);
const BOT_ANTHROPIC_TOOL_DEFINITIONS = Object.freeze(
  BOT_OPENAI_TOOL_DEFINITIONS
    .map((tool) => ({
      name: String(tool?.function?.name || "").trim(),
      description: String(tool?.function?.description || "").trim(),
      input_schema: tool?.function?.parameters || { type: "object", properties: {}, required: [] },
    }))
    .filter((tool) => tool.name)
);

function buildAiProxyError(message, status = 500, responseData = null) {
  const error = new Error(message || "AI request failed.");
  error.status = Number(status || 500) || 500;
  if (responseData !== undefined) {
    error.responseData = responseData;
  }
  return error;
}

function isNativeBotToolFallbackError(error) {
  const status = Number(error?.status || 0) || 0;
  const message = String(error?.message || "").toLowerCase();
  const responseString = JSON.stringify(error?.responseData || {}).toLowerCase();
  const combined = `${message}\n${responseString}`;

  if (status && ![400, 404, 405, 406, 409, 415, 422, 429, 500, 501, 503].includes(status)) {
    return false;
  }

  return [
    "tool",
    "tool_calls",
    "tool_use",
    "tool_result",
    "tool_choice",
    "parallel_tool_calls",
    "function call",
    "function_call",
    "functiondeclarations",
    "function declarations",
    "functionresponse",
    "generic fallback",
    "needs generic fallback",
    "planning text",
    "unsupported",
    "not support",
    "not supported",
    "unknown field",
    "unknown name",
    "invalid argument",
    "invalid_request_error",
    "response schema",
  ].some((fragment) => combined.includes(fragment));
}

function normalizeOpenAiNativeTextContent(content) {
  if (Array.isArray(content)) {
    return content
      .map((part) => {
        if (typeof part === "string") return part;
        if (part && typeof part.text === "string") return part.text;
        if (part && part.type === "text" && typeof part.text === "string") return part.text;
        if (part && part.type === "output_text" && typeof part.text === "string") return part.text;
        return "";
      })
      .join("")
      .trim();
  }
  return String(content || "").trim();
}

function normalizeCohereNativeTextContent(content) {
  if (Array.isArray(content)) {
    return content
      .map((part) => {
        if (typeof part === "string") return part;
        if (typeof part?.text === "string") return part.text;
        if (part?.type === "text" && typeof part?.text === "string") return part.text;
        return "";
      })
      .join("")
      .trim();
  }
  return String(content || "").trim();
}

function normalizeAnthropicTextContent(content) {
  if (Array.isArray(content)) {
    return content
      .map((part) => {
        if (typeof part === "string") return part;
        if (part?.type === "text" && typeof part?.text === "string") return part.text;
        if (typeof part?.text === "string") return part.text;
        return "";
      })
      .join("")
      .trim();
  }
  return String(content || "").trim();
}

function appendThinkingPart(parts, value) {
  const text = String(value || "").trim();
  if (!text) return;
  const normalized = text.replace(/\s+/g, " ").trim();
  if (!normalized) return;
  if (!parts.some((entry) => entry.replace(/\s+/g, " ").trim() === normalized)) {
    parts.push(text);
  }
}

function extractThinkingSectionsFromTaggedText(rawText) {
  const thinkingParts = [];
  let visible = String(rawText || "");

  const patterns = [
    /<think(?:ing)?>([\s\S]*?)<\/think(?:ing)?>/gi,
    /\[thinking\]([\s\S]*?)\[\/thinking\]/gi,
  ];

  for (const pattern of patterns) {
    visible = visible.replace(pattern, (_, inner) => {
      appendThinkingPart(thinkingParts, inner);
      return " ";
    });
  }

  return {
    content: visible.trim(),
    thinking_content: thinkingParts.join("\n\n").trim(),
  };
}

function collectThinkingFromReasoningDetails(details) {
  const parts = [];

  const visit = (node) => {
    if (node == null) return;

    if (typeof node === "string") {
      appendThinkingPart(parts, node);
      return;
    }

    if (Array.isArray(node)) {
      for (const entry of node) visit(entry);
      return;
    }

    if (typeof node !== "object") return;

    const type = String(node.type || "").trim().toLowerCase();
    const isReasoningLike = /reason|think|summary|analysis/.test(type);

    if (typeof node.thinking === "string") appendThinkingPart(parts, node.thinking);
    if (typeof node.reasoning === "string") appendThinkingPart(parts, node.reasoning);
    if (typeof node.summary === "string") appendThinkingPart(parts, node.summary);

    if (isReasoningLike && typeof node.text === "string") {
      appendThinkingPart(parts, node.text);
    }
    if (isReasoningLike && typeof node.content === "string") {
      appendThinkingPart(parts, node.content);
    }

    if (Array.isArray(node.summary)) visit(node.summary);
    if (Array.isArray(node.parts)) visit(node.parts);
    if (Array.isArray(node.reasoning_details)) visit(node.reasoning_details);
  };

  visit(details);
  return parts.join("\n\n").trim();
}

function parseOpenAiCompatibleResponsePayload(payload) {
  const data = payload && typeof payload === "object" ? payload : {};
  const thinkingParts = [];
  const visibleParts = [];

  const choice = data?.choices?.[0] || {};
  const message = choice?.message || {};
  const rawContent = message?.content;

  if (Array.isArray(rawContent)) {
    for (const part of rawContent) {
      if (typeof part === "string") {
        visibleParts.push(part);
        continue;
      }
      if (!part || typeof part !== "object") continue;

      const partType = String(part.type || "").trim().toLowerCase();
      const text = typeof part.text === "string" ? part.text : "";

      if (!partType || partType === "text" || partType === "output_text") {
        if (text) visibleParts.push(text);
        continue;
      }

      if (/reason|think|summary|analysis/.test(partType)) {
        appendThinkingPart(thinkingParts, text);
        appendThinkingPart(thinkingParts, part.thinking);
        appendThinkingPart(thinkingParts, part.reasoning);
        appendThinkingPart(thinkingParts, part.summary);
        continue;
      }

      if (typeof part.thinking === "string") appendThinkingPart(thinkingParts, part.thinking);
      if (typeof part.reasoning === "string") appendThinkingPart(thinkingParts, part.reasoning);
      if (text) visibleParts.push(text);
    }
  } else if (typeof rawContent === "string") {
    visibleParts.push(rawContent);
  } else {
    const fallback = normalizeOpenAiNativeTextContent(rawContent);
    if (fallback) visibleParts.push(fallback);
  }

  appendThinkingPart(thinkingParts, message?.reasoning);
  appendThinkingPart(thinkingParts, message?.reasoning_content);
  appendThinkingPart(thinkingParts, message?.thinking);
  appendThinkingPart(thinkingParts, collectThinkingFromReasoningDetails(message?.reasoning_details));

  if (Array.isArray(data?.output)) {
    for (const item of data.output) {
      if (!item || typeof item !== "object") continue;
      const itemType = String(item.type || "").trim().toLowerCase();

      if (itemType === "reasoning") {
        appendThinkingPart(thinkingParts, item?.text);
        appendThinkingPart(thinkingParts, collectThinkingFromReasoningDetails(item?.summary));
        appendThinkingPart(thinkingParts, collectThinkingFromReasoningDetails(item?.reasoning_details));
        continue;
      }

      if (itemType === "message" && Array.isArray(item.content)) {
        for (const part of item.content) {
          if (!part || typeof part !== "object") continue;
          const partType = String(part.type || "").trim().toLowerCase();
          if ((partType === "output_text" || partType === "text") && typeof part.text === "string") {
            visibleParts.push(part.text);
          } else if (/reason|think|summary|analysis/.test(partType)) {
            appendThinkingPart(thinkingParts, part.text);
            appendThinkingPart(thinkingParts, part.reasoning);
            appendThinkingPart(thinkingParts, part.thinking);
            appendThinkingPart(thinkingParts, part.summary);
          }
        }
      }
    }
  }

  const extracted = extractThinkingSectionsFromTaggedText(visibleParts.join("").trim());
  appendThinkingPart(thinkingParts, extracted.thinking_content);

  return {
    content: extracted.content,
    thinking_content: thinkingParts.join("\n\n").trim(),
  };
}

function parseGoogleResponsePayload(payload) {
  const data = payload && typeof payload === "object" ? payload : {};
  const thinkingParts = [];
  const visibleParts = [];

  const candidate = data?.candidates?.[0] || {};
  const parts = Array.isArray(candidate?.content?.parts) ? candidate.content.parts : [];

  for (const part of parts) {
    if (!part || typeof part !== "object") continue;
    const partType = String(part.type || "").trim().toLowerCase();
    const isThought = part?.thought === true || /think|reason|summary|analysis/.test(partType);

    if (typeof part.thinking === "string") appendThinkingPart(thinkingParts, part.thinking);
    if (typeof part.reasoning === "string") appendThinkingPart(thinkingParts, part.reasoning);

    const text = typeof part.text === "string" ? part.text : "";
    if (text) {
      if (isThought) appendThinkingPart(thinkingParts, text);
      else visibleParts.push(text);
    }
  }

  appendThinkingPart(thinkingParts, candidate?.reasoning);
  appendThinkingPart(thinkingParts, collectThinkingFromReasoningDetails(candidate?.reasoning_details));

  const extracted = extractThinkingSectionsFromTaggedText(visibleParts.join("").trim());
  appendThinkingPart(thinkingParts, extracted.thinking_content);

  return {
    content: extracted.content,
    thinking_content: thinkingParts.join("\n\n").trim(),
  };
}

function parseCohereResponsePayload(payload) {
  const data = payload && typeof payload === "object" ? payload : {};
  const thinkingParts = [];
  const visibleParts = [];

  const messageContent = data?.message?.content;
  if (Array.isArray(messageContent)) {
    for (const part of messageContent) {
      if (typeof part === "string") {
        visibleParts.push(part);
        continue;
      }
      if (!part || typeof part !== "object") continue;

      const partType = String(part.type || "").trim().toLowerCase();
      const text = typeof part.text === "string" ? part.text : "";

      if (!partType || partType === "text") {
        if (text) visibleParts.push(text);
        continue;
      }

      if (/think|reason|summary|analysis/.test(partType)) {
        appendThinkingPart(thinkingParts, text);
        appendThinkingPart(thinkingParts, part.thinking);
        appendThinkingPart(thinkingParts, part.reasoning);
        appendThinkingPart(thinkingParts, part.summary);
        continue;
      }

      if (text) visibleParts.push(text);
    }
  } else {
    const fallback = normalizeCohereNativeTextContent(messageContent) || String(data?.text || "").trim();
    if (fallback) visibleParts.push(fallback);
  }

  appendThinkingPart(thinkingParts, data?.message?.reasoning);
  appendThinkingPart(thinkingParts, data?.message?.thinking);
  appendThinkingPart(thinkingParts, collectThinkingFromReasoningDetails(data?.message?.reasoning_details));

  const extracted = extractThinkingSectionsFromTaggedText(visibleParts.join("").trim());
  appendThinkingPart(thinkingParts, extracted.thinking_content);

  return {
    content: extracted.content,
    thinking_content: thinkingParts.join("\n\n").trim(),
  };
}

function parseAnthropicResponsePayload(payload) {
  const data = payload && typeof payload === "object" ? payload : {};
  const thinkingParts = [];
  const visibleParts = [];
  const contentBlocks = Array.isArray(data?.content) ? data.content : [];

  for (const block of contentBlocks) {
    if (!block || typeof block !== "object") continue;
    const blockType = String(block.type || "").trim().toLowerCase();

    if (blockType === "text") {
      if (typeof block.text === "string") {
        visibleParts.push(block.text);
      }
      continue;
    }

    if (blockType === "thinking") {
      appendThinkingPart(thinkingParts, block.thinking);
      appendThinkingPart(thinkingParts, block.text);
      continue;
    }

    if (blockType === "redacted_thinking") {
      continue;
    }

    if (/think|reason|summary|analysis/.test(blockType)) {
      appendThinkingPart(thinkingParts, block.thinking);
      appendThinkingPart(thinkingParts, block.text);
      appendThinkingPart(thinkingParts, block.summary);
      continue;
    }
  }

  const extracted = extractThinkingSectionsFromTaggedText(visibleParts.join("").trim());
  appendThinkingPart(thinkingParts, extracted.thinking_content);

  return {
    content: extracted.content,
    thinking_content: thinkingParts.join("\n\n").trim(),
  };
}

function normalizeOpenAiNativeToolCalls(rawToolCalls) {
  return (Array.isArray(rawToolCalls) ? rawToolCalls : [])
    .map((call, index) => {
      const functionName = String(call?.function?.name || "").trim();
      if (!functionName) return null;
      let argsRaw = call?.function?.arguments;
      if (typeof argsRaw !== "string") {
        try {
          argsRaw = JSON.stringify(argsRaw || {});
        } catch {
          argsRaw = "{}";
        }
      }
      return {
        id: String(call?.id || `tool_call_${index + 1}`).trim(),
        type: "function",
        function: {
          name: functionName,
          arguments: argsRaw || "{}",
        },
      };
    })
    .filter(Boolean);
}

function normalizeLegacyOpenAiFunctionCall(rawFunctionCall) {
  const functionName = String(rawFunctionCall?.name || "").trim();
  if (!functionName) return [];
  let argsRaw = rawFunctionCall?.arguments;
  if (typeof argsRaw !== "string") {
    try {
      argsRaw = JSON.stringify(argsRaw || {});
    } catch {
      argsRaw = "{}";
    }
  }
  return [{
    id: "legacy_function_call_1",
    type: "function",
    function: {
      name: functionName,
      arguments: argsRaw || "{}",
    },
  }];
}

function buildOpenAiCompatibleNativeMessages(messages) {
  return (Array.isArray(messages) ? messages : [])
    .map((message) => {
      const role = String(message?.role || "").trim().toLowerCase();
      if (!role) return null;

      if (role === "tool") {
        const toolCallId = String(message?.tool_call_id || "").trim();
        if (!toolCallId) return null;
        return {
          role: "tool",
          tool_call_id: toolCallId,
          content: typeof message?.content === "string"
            ? message.content
            : JSON.stringify(message?.content || {}),
        };
      }

      if (role === "assistant") {
        const payload = { role: "assistant" };
        const textContent = normalizeOpenAiNativeTextContent(message?.content);
        const toolCalls = normalizeOpenAiNativeToolCalls(message?.tool_calls);
        if (textContent) {
          payload.content = textContent;
        }
        if (toolCalls.length > 0) {
          payload.tool_calls = toolCalls;
        }
        if (!("content" in payload) && !payload.tool_calls) {
          payload.content = "";
        }
        return payload;
      }

      if (message?.image && typeof message.image === "string" && message.image.startsWith("data:image/")) {
        return {
          role,
          content: [
            { type: "text", text: message.content || "" },
            { type: "image_url", image_url: { url: message.image } },
          ],
        };
      }

      return {
        role,
        content: typeof message?.content === "string" ? message.content : String(message?.content || ""),
      };
    })
    .filter(Boolean);
}

function buildCohereNativeMessages(messages) {
  return (Array.isArray(messages) ? messages : [])
    .map((message) => {
      const role = String(message?.role || "").trim().toLowerCase();
      if (!role) return null;

      if (role === "tool") {
        const toolCallId = String(message?.tool_call_id || "").trim();
        if (!toolCallId) return null;
        let data;
        try {
          data = JSON.stringify(message?.content ?? {});
        } catch {
          data = JSON.stringify({ ok: false, error: "Failed to serialize tool result." });
        }
        return {
          role: "tool",
          tool_call_id: toolCallId,
          content: [
            {
              type: "document",
              document: { data },
            },
          ],
        };
      }

      if (role === "assistant") {
        const payload = { role: "assistant" };
        const textContent = normalizeCohereNativeTextContent(message?.content);
        const toolCalls = normalizeOpenAiNativeToolCalls(message?.tool_calls);
        const toolPlan = String(message?.tool_plan || "").trim();
        if (textContent) {
          payload.content = textContent;
        }
        if (toolPlan) {
          payload.tool_plan = toolPlan;
        }
        if (toolCalls.length > 0) {
          payload.tool_calls = toolCalls;
        }
        if (!("content" in payload) && !payload.tool_calls) {
          payload.content = "";
        }
        return payload;
      }

      return {
        role,
        content: typeof message?.content === "string" ? message.content : String(message?.content || ""),
      };
    })
    .filter(Boolean);
}

function buildAnthropicSystemMessage(messages) {
  return (Array.isArray(messages) ? messages : [])
    .filter((message) => String(message?.role || "").trim().toLowerCase() === "system")
    .map((message) => String(message?.content || "").trim())
    .filter(Boolean)
    .join("\n\n");
}

function buildAnthropicContentBlocks(message) {
  if (Array.isArray(message?.anthropicContent)) {
    return message.anthropicContent;
  }

  const role = String(message?.role || "").trim().toLowerCase();
  const blocks = [];
  const textContent = normalizeAnthropicTextContent(message?.content);

  if (textContent) {
    blocks.push({ type: "text", text: textContent });
  }

  if (role === "user" && message?.image && typeof message.image === "string" && message.image.startsWith("data:image/")) {
    const match = message.image.match(/^data:(image\/[^;]+);base64,(.+)$/);
    if (match) {
      blocks.push({
        type: "image",
        source: {
          type: "base64",
          media_type: match[1],
          data: match[2],
        },
      });
    }
  }

  return blocks;
}

function buildAnthropicInitialMessages(messages) {
  return (Array.isArray(messages) ? messages : [])
    .filter((message) => {
      const role = String(message?.role || "").trim().toLowerCase();
      return role === "user" || role === "assistant";
    })
    .map((message) => ({
      role: String(message?.role || "").trim().toLowerCase(),
      content: buildAnthropicContentBlocks(message),
    }))
    .filter((message) => Array.isArray(message.content) && message.content.length > 0);
}

function extractAnthropicNativeToolCalls(contentBlocks) {
  return (Array.isArray(contentBlocks) ? contentBlocks : [])
    .map((block, index) => {
      if (!block || typeof block !== "object") return null;
      if (String(block.type || "").trim().toLowerCase() !== "tool_use") return null;
      const name = String(block.name || "").trim();
      if (!name) return null;
      return {
        id: String(block.id || `anthropic_tool_call_${index + 1}`).trim(),
        type: "function",
        function: {
          name,
          arguments: JSON.stringify(block.input && typeof block.input === "object" ? block.input : {}),
        },
      };
    })
    .filter(Boolean);
}

function buildAnthropicToolResultUserMessage(toolCalls, toolResults) {
  const resultById = new Map((Array.isArray(toolResults) ? toolResults : []).map((entry) => [String(entry?.id || "").trim(), entry]));
  const content = [];

  for (const toolCall of Array.isArray(toolCalls) ? toolCalls : []) {
    const match = resultById.get(String(toolCall?.id || "").trim());
    const toolResult = match?.result ?? { ok: false, error: "Missing tool result." };
    content.push({
      type: "tool_result",
      tool_use_id: String(toolCall?.id || "").trim(),
      is_error: toolResult?.ok === false,
      content: JSON.stringify(toolResult || {}),
    });
  }

  return {
    role: "user",
    content,
  };
}

function buildOpenAiNativeEndpoint(provider, settings) {
  const providerConfig = getAiProviderConfig(provider);
  if (provider === "openai-compatible") {
    return appendPathToBaseUrl(settings.baseUrl, "chat/completions");
  }
  return providerConfig?.chatEndpoint || "";
}

async function requestOpenAiNativeBotToolTurn(provider, settings, model, messages, options = {}) {
  const endpoint = buildOpenAiNativeEndpoint(provider, settings);
  if (!endpoint) {
    throw buildAiProxyError("Unknown provider.", 400);
  }

  const headers = {
    "Content-Type": "application/json",
    ...buildAiProviderAuthHeaders(provider, settings.apiKey),
  };

  const body = JSON.stringify({
    model,
    messages: buildOpenAiCompatibleNativeMessages(messages),
    max_tokens: Number.isFinite(options.maxTokens) ? options.maxTokens : 1800,
    temperature: Number.isFinite(options.temperature) ? options.temperature : 0.2,
    tools: BOT_OPENAI_TOOL_DEFINITIONS,
    tool_choice: "auto",
  });

  const response = await proxyAiRequest(endpoint, { method: "POST", headers, body });
  if (response.status !== 200) {
    throw buildAiProxyError(extractAiErrorMessage(response), response.status, response.data);
  }

  const choice = response.data?.choices?.[0] || {};
  const responseMessage = choice?.message || {};
  const parsed = parseOpenAiCompatibleResponsePayload(response.data);
  const toolCalls = normalizeOpenAiNativeToolCalls(responseMessage?.tool_calls);
  const finalToolCalls = toolCalls.length > 0
    ? toolCalls
    : normalizeLegacyOpenAiFunctionCall(responseMessage?.function_call);
  const content = parsed.content || normalizeOpenAiNativeTextContent(responseMessage?.content);

  return {
    content,
    thinking_content: parsed.thinking_content || "",
    toolCalls: finalToolCalls,
    assistantMessage: {
      role: "assistant",
      content: content || "",
      tool_calls: finalToolCalls,
    },
    finishReason: String(choice?.finish_reason || "").trim().toLowerCase(),
  };
}

async function requestCohereNativeBotToolTurn(apiKey, model, messages, options = {}) {
  const endpoint = "https://api.cohere.com/v2/chat";
  const body = JSON.stringify({
    stream: false,
    model,
    messages: buildCohereNativeMessages(messages),
    max_tokens: Number.isFinite(options.maxTokens) ? options.maxTokens : 1800,
    temperature: Number.isFinite(options.temperature) ? options.temperature : 0.2,
    tools: BOT_OPENAI_TOOL_DEFINITIONS,
  });

  const response = await proxyAiRequest(endpoint, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body,
  });

  if (response.status !== 200) {
    if ([404, 405].includes(response.status)) {
      throw buildAiProxyError(
        "Cohere native tool calling is not available on this deployment.",
        response.status,
        response.data
      );
    }
    throw buildAiProxyError(
      response.data?.message || response.data?.error || `API error: ${response.status}`,
      response.status,
      response.data
    );
  }

  const responseMessage = response.data?.message || {};
  const parsed = parseCohereResponsePayload(response.data);
  const toolCalls = normalizeOpenAiNativeToolCalls(responseMessage?.tool_calls);
  const content = parsed.content || normalizeCohereNativeTextContent(responseMessage?.content) || String(response.data?.text || "").trim();
  const toolPlan = String(responseMessage?.tool_plan || "").trim();

  return {
    content,
    thinking_content: parsed.thinking_content || "",
    toolCalls,
    assistantMessage: {
      role: "assistant",
      content: content || "",
      tool_plan: toolPlan,
      tool_calls: toolCalls,
    },
    finishReason: String(response.data?.finish_reason || "").trim().toLowerCase(),
  };
}

function buildGoogleNativeSystemInstruction(messages) {
  const systemText = (Array.isArray(messages) ? messages : [])
    .filter((message) => String(message?.role || "").trim().toLowerCase() === "system")
    .map((message) => String(message?.content || "").trim())
    .filter(Boolean)
    .join("\n\n");

  return systemText ? { parts: [{ text: systemText }] } : null;
}

function buildGoogleNativeInitialContents(messages) {
  return (Array.isArray(messages) ? messages : [])
    .filter((message) => String(message?.role || "").trim().toLowerCase() !== "system")
    .map((message) => {
      const role = String(message?.role || "").trim().toLowerCase();
      const parts = [];

      if (typeof message?.content === "string" && message.content) {
        parts.push({ text: message.content });
      }

      if (message?.image && typeof message.image === "string" && message.image.startsWith("data:image/")) {
        const match = message.image.match(/^data:(image\/[^;]+);base64,(.+)$/);
        if (match) {
          parts.push({
            inline_data: {
              mime_type: match[1],
              data: match[2],
            },
          });
        }
      }

      return {
        role: role === "assistant" ? "model" : "user",
        parts: parts.length ? parts : [{ text: "" }],
      };
    });
}

function extractGoogleNativeToolCalls(candidateContent) {
  return (Array.isArray(candidateContent?.parts) ? candidateContent.parts : [])
    .map((part, index) => {
      const functionCall = part?.functionCall || part?.function_call;
      const functionName = String(functionCall?.name || "").trim();
      if (!functionName) return null;
      return {
        id: String(functionCall?.id || `google_tool_call_${index + 1}`).trim(),
        type: "function",
        function: {
          name: functionName,
          arguments: JSON.stringify(functionCall?.args || {}),
        },
      };
    })
    .filter(Boolean);
}

function extractGoogleNativeText(candidateContent) {
  return (Array.isArray(candidateContent?.parts) ? candidateContent.parts : [])
    .map((part) => (typeof part?.text === "string" ? part.text : ""))
    .join("")
    .trim();
}

function buildGoogleFunctionResponseContent(toolCalls, toolResults) {
  const parts = [];
  const byId = new Map((Array.isArray(toolResults) ? toolResults : []).map((entry) => [String(entry?.id || "").trim(), entry]));

  for (const toolCall of Array.isArray(toolCalls) ? toolCalls : []) {
    const match = byId.get(String(toolCall?.id || "").trim());
    const toolResult = match?.result ?? { ok: false, error: "Missing tool result." };
    parts.push({
      functionResponse: {
        id: String(toolCall?.id || "").trim() || undefined,
        name: String(toolCall?.function?.name || "").trim(),
        response: { result: toolResult },
      },
    });
  }

  return {
    role: "user",
    parts,
  };
}

async function requestGoogleNativeBotToolTurn(apiKey, model, contents, systemInstruction, options = {}) {
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(apiKey)}`;
  const requestBody = {
    contents,
    generationConfig: {
      maxOutputTokens: Number.isFinite(options.maxTokens) ? options.maxTokens : 1800,
      temperature: Number.isFinite(options.temperature) ? options.temperature : 0.2,
    },
    tools: [{
      functionDeclarations: BOT_GOOGLE_FUNCTION_DECLARATIONS,
    }],
  };

  if (systemInstruction) {
    requestBody.systemInstruction = systemInstruction;
  }

  const response = await proxyAiRequest(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(requestBody),
  });

  if (response.status !== 200) {
    throw buildAiProxyError(response.data?.error?.message || `API error: ${response.status}`, response.status, response.data);
  }

  const candidateContent = response.data?.candidates?.[0]?.content || { role: "model", parts: [] };
  const parsed = parseGoogleResponsePayload(response.data);
  return {
    content: parsed.content || extractGoogleNativeText(candidateContent),
    thinking_content: parsed.thinking_content || "",
    toolCalls: extractGoogleNativeToolCalls(candidateContent),
    candidateContent,
    finishReason: String(response.data?.candidates?.[0]?.finishReason || "").trim().toLowerCase(),
  };
}

async function requestAnthropicNativeBotToolTurn(apiKey, model, messages, systemInstruction, options = {}) {
  const endpoint = 'https://api.anthropic.com/v1/messages';
  const body = {
    model,
    max_tokens: Number.isFinite(options.maxTokens) ? options.maxTokens : 1800,
    messages,
    tools: BOT_ANTHROPIC_TOOL_DEFINITIONS,
  };

  if (systemInstruction) {
    body.system = systemInstruction;
  }
  if (Number.isFinite(options.temperature)) {
    body.temperature = options.temperature;
  }

  const response = await proxyAiRequest(endpoint, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...buildAiProviderAuthHeaders('anthropic', apiKey),
    },
    body: JSON.stringify(body),
  });

  if (response.status !== 200) {
    throw buildAiProxyError(extractAiErrorMessage(response), response.status, response.data);
  }

  const contentBlocks = Array.isArray(response.data?.content) ? response.data.content : [];
  const parsed = parseAnthropicResponsePayload(response.data);

  return {
    content: parsed.content || normalizeAnthropicTextContent(contentBlocks),
    thinking_content: parsed.thinking_content || '',
    toolCalls: extractAnthropicNativeToolCalls(contentBlocks),
    assistantMessage: {
      role: 'assistant',
      anthropicContent: contentBlocks,
    },
    finishReason: String(response.data?.stop_reason || '').trim().toLowerCase(),
  };
}

async function callAiChatProvider(provider, settings, model, messages, options = {}) {
  switch (getAiProviderConfig(provider)?.chatProtocol) {
    case "anthropic":
      return proxyAnthropicAi(settings.apiKey, model, messages, options);
    case "google":
      return proxyGoogleAi(settings.apiKey, model, messages, options);
    case "cohere":
      return proxyCohereAi(settings.apiKey, model, messages, options);
    case "huggingface":
      return proxyHuggingFaceAi(settings.apiKey, model, messages, options);
    default:
      return proxyOpenAiCompatible(provider, settings, model, messages, options);
  }
}

function extractAiPanelResultError(result, fallbackMessage) {
  const body = result && typeof result.body === "object" && result.body ? result.body : {};
  const detail = body.detail || body.error || body.message || fallbackMessage;
  return String(detail || fallbackMessage || "That action failed.").trim();
}

async function performAiServerPowerAction(req, serverName, action) {
  const result = await nodesRouter.performPanelServerAction(req, serverName, action);
  if (!result || result.status !== 200) {
    throw new Error(extractAiPanelResultError(result, "Failed to perform that server action."));
  }
  return { ok: true, response: result.body || { ok: true } };
}

async function performAiConsoleCommand(req, serverName, command) {
  const commandCheck = typeof nodesRouter.sanitizeConsoleCommand === "function"
    ? nodesRouter.sanitizeConsoleCommand(command)
    : { ok: true, command: String(command || "").trim() };
  if (!commandCheck.ok) {
    throw new Error(String(commandCheck.detail || commandCheck.error || "Invalid console command."));
  }

  const result = await nodesRouter.performPanelConsoleCommand(req, serverName, commandCheck.command);
  if (!result || result.status !== 200) {
    throw new Error(extractAiPanelResultError(result, "Failed to send that console command."));
  }

  rememberConsoleCommand(serverName, commandCheck.command);
  return { ok: true, command: commandCheck.command };
}

async function listAiServerBackups(req, serverName) {
  const result = await nodesRouter.performPanelListBackups(req, serverName);
  if (!result || result.status !== 200) {
    throw new Error(extractAiPanelResultError(result, "Failed to load backups."));
  }
  return Array.isArray(result.body?.backups) ? result.body.backups : [];
}

async function createAiServerBackup(req, serverName, backupName, description) {
  const result = await nodesRouter.performPanelCreateBackup(req, serverName, backupName, description);
  if (!result || result.status !== 200) {
    throw new Error(extractAiPanelResultError(result, "Failed to create the backup."));
  }
  return result.body?.backup || null;
}

async function restoreAiServerBackup(req, serverName, backupId, deleteOldFiles) {
  const result = await nodesRouter.performPanelRestoreBackup(req, serverName, backupId, deleteOldFiles);
  if (!result || result.status !== 200) {
    throw new Error(extractAiPanelResultError(result, "Failed to restore that backup."));
  }
  return true;
}

async function deleteAiServerBackup(req, serverName, backupId) {
  const result = await nodesRouter.performPanelDeleteBackup(req, serverName, backupId);
  if (!result || result.status !== 200) {
    throw new Error(extractAiPanelResultError(result, "Failed to delete that backup."));
  }
  return true;
}

async function resolveBotToolServerOrError({ accessibleServers, requestedServer, currentServerName, userEmail, requiredPerms = [] }) {
  const resolved = resolveServerFromAccessibleList(accessibleServers, requestedServer, currentServerName);
  if (!resolved.entry) {
    return {
      error: {
        ok: false,
        error: resolved.error || "Server not found.",
        candidates: resolved.candidates || undefined,
      },
    };
  }

  const perms = await getEffectivePermsForUserOnServer(userEmail, resolved.entry.name);
  const missingPerms = requiredPerms.filter((permKey) => !perms?.[permKey]);
  if (missingPerms.length) {
    return {
      error: {
        ok: false,
        error: missingPerms.length === 1
          ? `You do not have permission to ${describeBotPermissionKey(missingPerms[0])} on ${resolved.entry.displayName || resolved.entry.name}.`
          : `You do not have all required permissions on ${resolved.entry.displayName || resolved.entry.name}.`,
        permissions: missingPerms,
      },
      entry: resolved.entry,
      perms,
    };
  }

  return { entry: resolved.entry, perms };
}

function queueBotClientAction(clientActions, nextAction) {
  if (!Array.isArray(clientActions) || !nextAction || typeof nextAction !== "object") return;

  const type = String(nextAction.type || "").trim().toLowerCase();
  const server = String(nextAction.server || "").trim().toLowerCase();
  if (!type) return;

  if (type === "refresh_power_state" || type === "backups_updated") {
    const existingIndex = clientActions.findIndex((entry) => (
      String(entry?.type || "").trim().toLowerCase() === type &&
      String(entry?.server || "").trim().toLowerCase() === server
    ));
    if (existingIndex !== -1) {
      clientActions[existingIndex] = { ...clientActions[existingIndex], ...nextAction };
      return;
    }
  }

  if (type === "console_command_executed") {
    const command = String(nextAction.command || "").trim();
    const sentAt = Number(nextAction.sentAt) || 0;
    const duplicate = clientActions.some((entry) => (
      String(entry?.type || "").trim().toLowerCase() === type &&
      String(entry?.server || "").trim().toLowerCase() === server &&
      String(entry?.command || "").trim() === command &&
      (Number(entry?.sentAt) || 0) === sentAt
    ));
    if (duplicate) return;
  }

  clientActions.push(nextAction);
}

function normalizeBotToolCacheValue(value) {
  if (Array.isArray(value)) {
    return value.map((entry) => normalizeBotToolCacheValue(entry));
  }
  if (value && typeof value === "object") {
    return Object.keys(value).sort().reduce((accumulator, key) => {
      accumulator[key] = normalizeBotToolCacheValue(value[key]);
      return accumulator;
    }, {});
  }
  return value;
}

function buildBotToolExecutionCacheKey(name, args, currentServerName) {
  return JSON.stringify({
    name: String(name || "").trim(),
    currentServerName: String(currentServerName || "").trim().toLowerCase(),
    args: normalizeBotToolCacheValue(args && typeof args === "object" ? args : {}),
  });
}

async function maybeExecuteDirectBotConsoleCommand({
  req,
  safeMessages,
  currentServerName,
  accessibleServers,
  userEmail,
  userIp,
  clientActions,
}) {
  const latestUserMessage = getLatestBotUserMessageContent(safeMessages);
  const command = extractExplicitBotConsoleCommandRequest(latestUserMessage);
  if (!command) return null;

  const requestedServer = extractBotAssistantServerCandidate(latestUserMessage, currentServerName) || currentServerName;
  const toolResult = await runBotAssistantTool({
    req,
    name: "send_console_command",
    args: { server: requestedServer, command },
    currentServerName,
    accessibleServers,
    userEmail,
    userIp,
    clientActions,
  });

  return {
    content: buildBotReplyFromToolResult("send_console_command", toolResult) || "Done.",
    thinking_time_ms: 0,
    thinking_content: "",
    clientActions,
    nativeToolMode: "direct-console-command",
  };
}

async function executeBotAssistantToolWithCache({
  toolExecutionCache,
  name,
  args,
  currentServerName,
  ...rest
}) {
  const cacheKey = buildBotToolExecutionCacheKey(name, args, currentServerName);
  if (toolExecutionCache instanceof Map && toolExecutionCache.has(cacheKey)) {
    return toolExecutionCache.get(cacheKey);
  }

  const result = await runBotAssistantTool({
    ...rest,
    name,
    args,
    currentServerName,
  });

  if (toolExecutionCache instanceof Map) {
    toolExecutionCache.set(cacheKey, result);
  }
  return result;
}

async function runBotAssistantTool({ req, name, args, currentServerName, accessibleServers, userEmail, userIp, clientActions }) {
  const toolName = String(name || "").trim();
  if (!BOT_AI_TOOL_NAMES.has(toolName)) {
    return { ok: false, error: "Unknown tool." };
  }

  switch (toolName) {
    case "list_accessible_servers":
      return {
        ok: true,
        servers: accessibleServers.slice(0, 100).map((entry) => buildBotServerSummary(entry)),
      };

    case "inspect_server": {
      const { entry, perms, error } = await resolveBotToolServerOrError({
        accessibleServers,
        requestedServer: args.server,
        currentServerName,
        userEmail,
      });
      if (error) return error;
      return {
        ok: true,
        server: buildBotServerSummary(entry, perms),
      };
    }

    case "power_server": {
      const action = normalizeBotPowerAction(args.action);
      if (!action) return { ok: false, error: "Invalid power action." };
      const requiredPerms = action === "start"
        ? ["server_start"]
        : action === "restart"
          ? ["server_stop", "server_start"]
          : ["server_stop"];

      const { entry, perms, error } = await resolveBotToolServerOrError({
        accessibleServers,
        requestedServer: args.server,
        currentServerName,
        userEmail,
        requiredPerms,
      });
      if (error) return error;

      await performAiServerPowerAction(req, entry.name, action);
      if (Array.isArray(clientActions) && isSameBotServer(entry.name, currentServerName)) {
        queueBotClientAction(clientActions, { type: "refresh_power_state", server: entry.name, action });
      }

      return {
        ok: true,
        action,
        server: buildBotServerSummary(entry, perms),
      };
    }

    case "query_console": {
      const { entry, perms, error } = await resolveBotToolServerOrError({
        accessibleServers,
        requestedServer: args.server,
        currentServerName,
        userEmail,
        requiredPerms: ["console_read"],
      });
      if (error) return error;

      const lines = await getBotConsoleHistorySnapshot(entry.name, args.limit);
      return {
        ok: true,
        server: buildBotServerSummary(entry, perms),
        question: String(args.question || "").trim(),
        total: lines.length,
        lines,
      };
    }

    case "send_console_command": {
      const { entry, perms, error } = await resolveBotToolServerOrError({
        accessibleServers,
        requestedServer: args.server,
        currentServerName,
        userEmail,
        requiredPerms: ["console_write"],
      });
      if (error) return error;

      if (!String(args.command || "").trim()) {
        return { ok: false, error: "A console command is required." };
      }

      let commandResult;
      try {
        commandResult = await performAiConsoleCommand(req, entry.name, args.command);
      } catch (error) {
        return {
          ok: false,
          error: String(error?.message || "Failed to send that console command.").trim(),
        };
      }

      if (Array.isArray(clientActions) && isSameBotServer(entry.name, currentServerName)) {
        queueBotClientAction(clientActions, {
          type: "console_command_executed",
          server: entry.name,
          command: commandResult.command,
          sentAt: Date.now(),
        });
      }

      return {
        ok: true,
        server: buildBotServerSummary(entry, perms),
        command: commandResult.command,
      };
    }

    case "list_backups": {
      const { entry, perms, error } = await resolveBotToolServerOrError({
        accessibleServers,
        requestedServer: args.server,
        currentServerName,
        userEmail,
        requiredPerms: ["backups_view"],
      });
      if (error) return error;

      const backups = await listAiServerBackups(req, entry.name);
      return {
        ok: true,
        server: buildBotServerSummary(entry, perms),
        total: backups.length,
        backups: summarizeBackupsForBot(backups),
      };
    }

    case "create_backup": {
      const { entry, perms, error } = await resolveBotToolServerOrError({
        accessibleServers,
        requestedServer: args.server,
        currentServerName,
        userEmail,
        requiredPerms: ["backups_create"],
      });
      if (error) return error;

      const backup = await createAiServerBackup(req, entry.name, args.name, args.description);
      if (Array.isArray(clientActions) && isSameBotServer(entry.name, currentServerName)) {
        queueBotClientAction(clientActions, { type: "backups_updated", server: entry.name, reason: "create" });
      }

      return {
        ok: true,
        server: buildBotServerSummary(entry, perms),
        backup: backup ? summarizeBackupsForBot([backup])[0] : null,
      };
    }

    case "restore_backup":
    case "delete_backup": {
      const requiredPerms = toolName === "restore_backup" ? ["backups_create"] : ["backups_delete"];
      const { entry, perms, error } = await resolveBotToolServerOrError({
        accessibleServers,
        requestedServer: args.server,
        currentServerName,
        userEmail,
        requiredPerms,
      });
      if (error) return error;

      const backups = await listAiServerBackups(req, entry.name);
      const backupResolution = resolveBackupFromList(backups, args.backup);
      if (!backupResolution.backup) {
        return {
          ok: false,
          error: backupResolution.error || "Backup not found.",
          candidates: backupResolution.candidates || summarizeBackupsForBot(backups).map((backup) => backup.name || backup.id).filter(Boolean).slice(0, 5),
        };
      }

      const backupId = backupResolution.backup.id || backupResolution.backup.uuid;
      if (!backupId) {
        return { ok: false, error: "That backup does not expose a usable identifier." };
      }

      if (toolName === "restore_backup") {
        await restoreAiServerBackup(req, entry.name, backupId, !!args.deleteOldFiles);
      } else {
        await deleteAiServerBackup(req, entry.name, backupId);
      }

      if (Array.isArray(clientActions) && isSameBotServer(entry.name, currentServerName)) {
        queueBotClientAction(clientActions, {
          type: "backups_updated",
          server: entry.name,
          reason: toolName === "restore_backup" ? "restore" : "delete",
        });
      }

      return {
        ok: true,
        server: buildBotServerSummary(entry, perms),
        backup: summarizeBackupsForBot([backupResolution.backup])[0] || {
          id: backupId,
          name: backupResolution.backup.name || null,
        },
      };
    }

    default:
      return { ok: false, error: "Unknown tool." };
  }
}

function parseNativeBotToolArguments(rawArguments) {
  if (!rawArguments) return {};
  if (typeof rawArguments === "object" && !Array.isArray(rawArguments)) {
    return rawArguments;
  }
  try {
    const parsed = JSON.parse(String(rawArguments));
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

async function requestBotToolPlanRepair(provider, settings, model, workingMessages, assistantContent) {
  let currentAssistantContent = String(assistantContent || "").slice(0, 12000);

  for (let attempt = 0; attempt < 3; attempt += 1) {
    const repairMessages = [
      ...(Array.isArray(workingMessages) ? workingMessages : []),
      ...((currentAssistantContent && !looksLikeLeakedBotReasoning(currentAssistantContent) && !isWeakBotAiReply(currentAssistantContent))
        ? [{ role: "assistant", content: currentAssistantContent }]
        : []),
      {
        role: "user",
        content: [
          "Your previous reply was invalid.",
          "If a native server action is needed, your entire reply must be one <adpanel_tool_plan> JSON block with no extra text.",
          "If no native action is needed, output one short user-facing sentence only.",
          "If you need more than one tool, include multiple tool calls in the same JSON block.",
          "Do not mention tools unless they are inside the JSON block.",
          "Do not expose reasoning, thinking, or planning.",
          'Examples: <adpanel_tool_plan>{"reply":"Starting it now.","tool_calls":[{"name":"power_server","args":{"server":"current server","action":"start"}}]}</adpanel_tool_plan>',
          '<adpanel_tool_plan>{"reply":"Checking it now.","tool_calls":[{"name":"inspect_server","args":{"server":"current server"}}]}</adpanel_tool_plan>',
          '<adpanel_tool_plan>{"reply":"Checking that now.","tool_calls":[{"name":"inspect_server","args":{"server":"current server"}},{"name":"query_console","args":{"server":"current server","limit":60}}]}</adpanel_tool_plan>',
          '<adpanel_tool_plan>{"reply":"Sending it now.","tool_calls":[{"name":"send_console_command","args":{"server":"current server","command":"say hello"}}]}</adpanel_tool_plan>',
        ].join(" "),
      },
    ];

    const repairResponse = await callAiChatProvider(provider, settings, model, repairMessages, {
      maxTokens: 900,
      temperature: 0,
    });

    currentAssistantContent = String(repairResponse?.content || "").trim();
    if (!currentAssistantContent) break;
    if (parseBotAssistantToolPlan(currentAssistantContent)) break;
    if (!needsBotToolPlanRepair(currentAssistantContent)) break;
  }

  return currentAssistantContent;
}

async function completeAiChatConversationWithOpenAiNativeTools({
  req,
  provider,
  settings,
  model,
  conversationMessages,
  currentServerName,
  accessibleServers,
  userEmail,
  userIp,
  clientActions,
}) {
  let workingMessages = Array.isArray(conversationMessages) ? conversationMessages.slice() : [];
  const toolExecutionCache = new Map();
  const thinkingParts = [];
  let thinkingTimeMs = 0;
  let finalContent = "";
  let lastToolName = "";
  let lastToolResult = null;
  const toolHistory = [];
  let executedAnyTool = false;

  try {
    for (let attempt = 0; attempt < MAX_BOT_TOOL_LOOPS; attempt += 1) {
      const startedAt = Date.now();
      const turn = await requestOpenAiNativeBotToolTurn(provider, settings, model, workingMessages, {
        maxTokens: 2200,
        temperature: 0.2,
      });
      thinkingTimeMs += Date.now() - startedAt;
      appendThinkingPart(thinkingParts, turn.thinking_content);

      let effectiveToolCalls = Array.isArray(turn.toolCalls) ? turn.toolCalls.slice() : [];
      let assistantMessage = turn.assistantMessage;
      if (!effectiveToolCalls.length && needsBotToolPlanRepair(turn.content || "")) {
        const salvagedToolCalls = salvageBotToolCallsFromAssistantText(turn.content || "", currentServerName);
        if (salvagedToolCalls.length > 0) {
          effectiveToolCalls = buildSyntheticNativeToolCalls(salvagedToolCalls, `openai_salvaged_${attempt + 1}`);
          assistantMessage = {
            role: "assistant",
            content: "",
            tool_calls: effectiveToolCalls,
          };
        } else if (!executedAnyTool) {
          throw buildAiProxyError("Native tool response needs generic fallback.", 422, { content: turn.content || "" });
        }
      }

      if (!effectiveToolCalls.length) {
        finalContent = stripBotToolPlanMarkup(turn.content || "");
        break;
      }

      workingMessages.push(assistantMessage);

      for (const toolCall of effectiveToolCalls) {
        const toolName = String(toolCall?.function?.name || "").trim();
        const toolArgs = sanitizeBotToolArgs(toolName, parseNativeBotToolArguments(toolCall?.function?.arguments));
        const toolResult = await executeBotAssistantToolWithCache({
          toolExecutionCache,
          req,
          name: toolName,
          args: toolArgs,
          currentServerName,
          accessibleServers,
          userEmail,
          userIp,
          clientActions,
        });
        executedAnyTool = true;
        lastToolName = toolName;
        lastToolResult = toolResult;
        toolHistory.push({ name: toolName, result: toolResult });
        workingMessages.push({
          role: "tool",
          tool_call_id: String(toolCall?.id || "").trim(),
          content: JSON.stringify(toolResult || {}),
        });
      }
    }
  } catch (error) {
    if (executedAnyTool) {
      error.nativeToolProgressStarted = true;
    }
    throw error;
  }

  if (!finalContent) {
    finalContent = buildBotReplyFromToolHistory(toolHistory) || buildBotReplyFromToolResult(lastToolName, lastToolResult) || "Done.";
  }
  finalContent = finalizeBotAssistantReply(finalContent, lastToolName, lastToolResult);

  return {
    content: finalContent,
    thinking_time_ms: thinkingTimeMs,
    thinking_content: thinkingParts.join("\n\n").trim(),
    clientActions,
    nativeToolMode: "openai-style",
  };
}

async function completeAiChatConversationWithCohereNativeTools({
  req,
  settings,
  model,
  conversationMessages,
  currentServerName,
  accessibleServers,
  userEmail,
  userIp,
  clientActions,
}) {
  let workingMessages = Array.isArray(conversationMessages) ? conversationMessages.slice() : [];
  const toolExecutionCache = new Map();
  const thinkingParts = [];
  let thinkingTimeMs = 0;
  let finalContent = "";
  let lastToolName = "";
  let lastToolResult = null;
  const toolHistory = [];
  let executedAnyTool = false;

  try {
    for (let attempt = 0; attempt < MAX_BOT_TOOL_LOOPS; attempt += 1) {
      const startedAt = Date.now();
      const turn = await requestCohereNativeBotToolTurn(settings.apiKey, model, workingMessages, {
        maxTokens: 2200,
        temperature: 0.2,
      });
      thinkingTimeMs += Date.now() - startedAt;
      appendThinkingPart(thinkingParts, turn.thinking_content);

      let effectiveToolCalls = Array.isArray(turn.toolCalls) ? turn.toolCalls.slice() : [];
      let assistantMessage = turn.assistantMessage;
      if (!effectiveToolCalls.length && needsBotToolPlanRepair(turn.content || "")) {
        const salvagedToolCalls = salvageBotToolCallsFromAssistantText(turn.content || "", currentServerName);
        if (salvagedToolCalls.length > 0) {
          effectiveToolCalls = buildSyntheticNativeToolCalls(salvagedToolCalls, `cohere_salvaged_${attempt + 1}`);
          assistantMessage = {
            role: "assistant",
            content: "",
            tool_calls: effectiveToolCalls,
          };
        } else if (!executedAnyTool) {
          throw buildAiProxyError("Native tool response needs generic fallback.", 422, { content: turn.content || "" });
        }
      }

      if (!effectiveToolCalls.length) {
        finalContent = stripBotToolPlanMarkup(turn.content || "");
        break;
      }

      workingMessages.push(assistantMessage);

      for (const toolCall of effectiveToolCalls) {
        const toolName = String(toolCall?.function?.name || "").trim();
        const toolArgs = sanitizeBotToolArgs(toolName, parseNativeBotToolArguments(toolCall?.function?.arguments));
        const toolResult = await executeBotAssistantToolWithCache({
          toolExecutionCache,
          req,
          name: toolName,
          args: toolArgs,
          currentServerName,
          accessibleServers,
          userEmail,
          userIp,
          clientActions,
        });
        executedAnyTool = true;
        lastToolName = toolName;
        lastToolResult = toolResult;
        toolHistory.push({ name: toolName, result: toolResult });
        workingMessages.push({
          role: "tool",
          tool_call_id: String(toolCall?.id || "").trim(),
          content: toolResult || {},
        });
      }
    }
  } catch (error) {
    if (executedAnyTool) {
      error.nativeToolProgressStarted = true;
    }
    throw error;
  }

  if (!finalContent) {
    finalContent = buildBotReplyFromToolHistory(toolHistory) || buildBotReplyFromToolResult(lastToolName, lastToolResult) || "Done.";
  }
  finalContent = finalizeBotAssistantReply(finalContent, lastToolName, lastToolResult);

  return {
    content: finalContent,
    thinking_time_ms: thinkingTimeMs,
    thinking_content: thinkingParts.join("\n\n").trim(),
    clientActions,
    nativeToolMode: "cohere",
  };
}

async function completeAiChatConversationWithGoogleNativeTools({
  req,
  settings,
  model,
  conversationMessages,
  currentServerName,
  accessibleServers,
  userEmail,
  userIp,
  clientActions,
}) {
  const systemInstruction = buildGoogleNativeSystemInstruction(conversationMessages);
  const contents = buildGoogleNativeInitialContents(conversationMessages);
  const toolExecutionCache = new Map();
  const thinkingParts = [];
  let thinkingTimeMs = 0;
  let finalContent = "";
  let lastToolName = "";
  let lastToolResult = null;
  const toolHistory = [];
  let executedAnyTool = false;

  try {
    for (let attempt = 0; attempt < MAX_BOT_TOOL_LOOPS; attempt += 1) {
      const startedAt = Date.now();
      const turn = await requestGoogleNativeBotToolTurn(settings.apiKey, model, contents, systemInstruction, {
        maxTokens: 2200,
        temperature: 0.2,
      });
      thinkingTimeMs += Date.now() - startedAt;
      appendThinkingPart(thinkingParts, turn.thinking_content);

      let effectiveToolCalls = Array.isArray(turn.toolCalls) ? turn.toolCalls.slice() : [];
      let candidateContent = turn.candidateContent;
      if (!effectiveToolCalls.length && needsBotToolPlanRepair(turn.content || "")) {
        const salvagedToolCalls = salvageBotToolCallsFromAssistantText(turn.content || "", currentServerName);
        if (salvagedToolCalls.length > 0) {
          effectiveToolCalls = buildSyntheticNativeToolCalls(salvagedToolCalls, `google_salvaged_${attempt + 1}`);
          candidateContent = {
            role: "model",
            parts: effectiveToolCalls.map((toolCall) => ({
              functionCall: {
                id: String(toolCall?.id || "").trim(),
                name: String(toolCall?.function?.name || "").trim(),
                args: parseNativeBotToolArguments(toolCall?.function?.arguments),
              },
            })),
          };
        } else if (!executedAnyTool) {
          throw buildAiProxyError("Native tool response needs generic fallback.", 422, { content: turn.content || "" });
        }
      }

      if (!effectiveToolCalls.length) {
        finalContent = stripBotToolPlanMarkup(turn.content || "");
        break;
      }

      contents.push(candidateContent);
      const toolResults = [];

      for (const toolCall of effectiveToolCalls) {
        const toolName = String(toolCall?.function?.name || "").trim();
        const toolArgs = sanitizeBotToolArgs(toolName, parseNativeBotToolArguments(toolCall?.function?.arguments));
        const toolResult = await executeBotAssistantToolWithCache({
          toolExecutionCache,
          req,
          name: toolName,
          args: toolArgs,
          currentServerName,
          accessibleServers,
          userEmail,
          userIp,
          clientActions,
        });
        executedAnyTool = true;
        lastToolName = toolName;
        lastToolResult = toolResult;
        toolHistory.push({ name: toolName, result: toolResult });
        toolResults.push({
          id: String(toolCall?.id || "").trim(),
          name: toolName,
          result: toolResult,
        });
      }

      contents.push(buildGoogleFunctionResponseContent(effectiveToolCalls, toolResults));
    }
  } catch (error) {
    if (executedAnyTool) {
      error.nativeToolProgressStarted = true;
    }
    throw error;
  }

  if (!finalContent) {
    finalContent = buildBotReplyFromToolHistory(toolHistory) || buildBotReplyFromToolResult(lastToolName, lastToolResult) || "Done.";
  }
  finalContent = finalizeBotAssistantReply(finalContent, lastToolName, lastToolResult);

  return {
    content: finalContent,
    thinking_time_ms: thinkingTimeMs,
    thinking_content: thinkingParts.join("\n\n").trim(),
    clientActions,
    nativeToolMode: "google",
  };
}

async function completeAiChatConversationWithAnthropicNativeTools({
  req,
  settings,
  model,
  conversationMessages,
  currentServerName,
  accessibleServers,
  userEmail,
  userIp,
  clientActions,
}) {
  const systemInstruction = buildAnthropicSystemMessage(conversationMessages);
  const workingMessages = buildAnthropicInitialMessages(conversationMessages);
  const toolExecutionCache = new Map();
  const thinkingParts = [];
  let thinkingTimeMs = 0;
  let finalContent = "";
  let lastToolName = "";
  let lastToolResult = null;
  const toolHistory = [];
  let executedAnyTool = false;

  try {
    for (let attempt = 0; attempt < MAX_BOT_TOOL_LOOPS; attempt += 1) {
      const startedAt = Date.now();
      const turn = await requestAnthropicNativeBotToolTurn(settings.apiKey, model, workingMessages, systemInstruction, {
        maxTokens: 2200,
        temperature: 0.2,
      });
      thinkingTimeMs += Date.now() - startedAt;
      appendThinkingPart(thinkingParts, turn.thinking_content);

      let effectiveToolCalls = Array.isArray(turn.toolCalls) ? turn.toolCalls.slice() : [];
      let assistantMessage = turn.assistantMessage;
      if (!effectiveToolCalls.length && needsBotToolPlanRepair(turn.content || "")) {
        const salvagedToolCalls = salvageBotToolCallsFromAssistantText(turn.content || "", currentServerName);
        if (salvagedToolCalls.length > 0) {
          effectiveToolCalls = buildSyntheticNativeToolCalls(salvagedToolCalls, `anthropic_salvaged_${attempt + 1}`);
          assistantMessage = {
            role: "assistant",
            anthropicContent: effectiveToolCalls.map((toolCall) => ({
              type: "tool_use",
              id: String(toolCall?.id || "").trim(),
              name: String(toolCall?.function?.name || "").trim(),
              input: parseNativeBotToolArguments(toolCall?.function?.arguments),
            })),
          };
        } else if (!executedAnyTool) {
          throw buildAiProxyError("Native tool response needs generic fallback.", 422, { content: turn.content || "" });
        }
      }

      if (!effectiveToolCalls.length) {
        finalContent = stripBotToolPlanMarkup(turn.content || "");
        break;
      }

      workingMessages.push({
        role: "assistant",
        content: Array.isArray(assistantMessage?.anthropicContent) ? assistantMessage.anthropicContent : [],
      });

      const toolResults = [];
      for (const toolCall of effectiveToolCalls) {
        const toolName = String(toolCall?.function?.name || "").trim();
        const toolArgs = sanitizeBotToolArgs(toolName, parseNativeBotToolArguments(toolCall?.function?.arguments));
        const toolResult = await executeBotAssistantToolWithCache({
          toolExecutionCache,
          req,
          name: toolName,
          args: toolArgs,
          currentServerName,
          accessibleServers,
          userEmail,
          userIp,
          clientActions,
        });
        executedAnyTool = true;
        lastToolName = toolName;
        lastToolResult = toolResult;
        toolHistory.push({ name: toolName, result: toolResult });
        toolResults.push({
          id: String(toolCall?.id || "").trim(),
          name: toolName,
          result: toolResult,
        });
      }

      workingMessages.push(buildAnthropicToolResultUserMessage(effectiveToolCalls, toolResults));
    }
  } catch (error) {
    if (executedAnyTool) {
      error.nativeToolProgressStarted = true;
    }
    throw error;
  }

  if (!finalContent) {
    finalContent = buildBotReplyFromToolHistory(toolHistory) || buildBotReplyFromToolResult(lastToolName, lastToolResult) || "Done.";
  }
  finalContent = finalizeBotAssistantReply(finalContent, lastToolName, lastToolResult);

  return {
    content: finalContent,
    thinking_time_ms: thinkingTimeMs,
    thinking_content: thinkingParts.join("\n\n").trim(),
    clientActions,
    nativeToolMode: "anthropic",
  };
}

async function maybeCompleteAiChatConversationNatively({
  req,
  provider,
  settings,
  model,
  conversationMessages,
  currentServerName,
  accessibleServers,
  userEmail,
  userIp,
  clientActions,
}) {
  try {
    if (ANTHROPIC_NATIVE_BOT_TOOL_PROVIDERS.has(provider)) {
      return await completeAiChatConversationWithAnthropicNativeTools({
        req,
        settings,
        model,
        conversationMessages,
        currentServerName,
        accessibleServers,
        userEmail,
        userIp,
        clientActions,
      });
    }

    if (OPENAI_STYLE_NATIVE_BOT_TOOL_PROVIDERS.has(provider)) {
      return await completeAiChatConversationWithOpenAiNativeTools({
        req,
        provider,
        settings,
        model,
        conversationMessages,
        currentServerName,
        accessibleServers,
        userEmail,
        userIp,
        clientActions,
      });
    }

    if (COHERE_NATIVE_BOT_TOOL_PROVIDERS.has(provider)) {
      return await completeAiChatConversationWithCohereNativeTools({
        req,
        settings,
        model,
        conversationMessages,
        currentServerName,
        accessibleServers,
        userEmail,
        userIp,
        clientActions,
      });
    }

    if (GOOGLE_NATIVE_BOT_TOOL_PROVIDERS.has(provider)) {
      return await completeAiChatConversationWithGoogleNativeTools({
        req,
        settings,
        model,
        conversationMessages,
        currentServerName,
        accessibleServers,
        userEmail,
        userIp,
        clientActions,
      });
    }
  } catch (error) {
    if (!error?.nativeToolProgressStarted && isNativeBotToolFallbackError(error)) {
      console.warn(`[ai-chat] Native tool mode unsupported for ${provider}, falling back to generic tool plan: ${error.message}`);
      return null;
    }
    throw error;
  }

  return null;
}

async function completeAiChatConversation({ req, provider, settings, model, messages, serverContext }) {
  const safeMessages = normalizeAiConversationMessages(messages);
  const safeServerContext = serverContext && typeof serverContext === "object" ? serverContext : {};
  const currentServerName = String(safeServerContext.currentServer || safeServerContext.server || "").trim();
  const currentUser = await getLoggedInUser(req);
  const userEmail = String(currentUser?.email || req.session?.user || "").trim().toLowerCase();
  const isAdminUser = !!currentUser?.admin;
  const userIp = getRequestIp(req);
  const clientActions = [];

  if (!currentServerName) {
    const startedAt = Date.now();
    const result = await callAiChatProvider(provider, settings, model, safeMessages, {});
    return {
      content: finalizeBotAssistantReply(result.content || "", "", null) || "I could not produce a clean answer for that request.",
      thinking_time_ms: Date.now() - startedAt,
      thinking_content: String(result?.thinking_content || "").trim(),
      clientActions,
    };
  }

  const accessibleServers = await getAccessibleServerEntriesForAi(userEmail, isAdminUser);
  const currentServerResolution = resolveServerFromAccessibleList(accessibleServers, currentServerName, currentServerName);
  const currentServerEntry = currentServerResolution.entry || null;
  const currentServerPerms = currentServerEntry
    ? await getEffectivePermsForUserOnServer(userEmail, currentServerEntry.name)
    : null;

  const conversationMessages = [
    {
      role: "system",
      content: buildBotAssistantSystemPrompt({
        currentServerName,
        accessibleServers,
        currentServerPerms,
      }),
    },
    ...safeMessages,
  ];

  const directConsoleCommandResult = await maybeExecuteDirectBotConsoleCommand({
    req,
    safeMessages,
    currentServerName,
    accessibleServers,
    userEmail,
    userIp,
    clientActions,
  });
  if (directConsoleCommandResult) {
    return directConsoleCommandResult;
  }

  const nativeResult = await maybeCompleteAiChatConversationNatively({
    req,
    provider,
    settings,
    model,
    conversationMessages,
    currentServerName,
    accessibleServers,
    userEmail,
    userIp,
    clientActions,
  });
  if (nativeResult) {
    const cleanedNativeContent = finalizeBotAssistantReply(nativeResult.content || "", "", null);
    const hasNativeActions = Array.isArray(nativeResult.clientActions) && nativeResult.clientActions.length > 0;
    if (cleanedNativeContent || hasNativeActions) {
      return {
        ...nativeResult,
        content: cleanedNativeContent || nativeResult.content || "Done.",
      };
    }
  }

  let workingMessages = conversationMessages.slice();
  const toolExecutionCache = new Map();
  const thinkingParts = [];

  let thinkingTimeMs = 0;
  let finalContent = "";
  let lastToolName = "";
  let lastToolResult = null;
  const toolHistory = [];

  for (let attempt = 0; attempt < MAX_BOT_TOOL_LOOPS; attempt += 1) {
    const startedAt = Date.now();
    const response = await callAiChatProvider(provider, settings, model, workingMessages, {
      maxTokens: 2200,
      temperature: 0.2,
    });
    thinkingTimeMs += Date.now() - startedAt;
    appendThinkingPart(thinkingParts, response?.thinking_content);

    let content = String(response?.content || "").trim();
    let plan = parseBotAssistantToolPlan(content);

    if ((!plan || !Array.isArray(plan.tool_calls) || plan.tool_calls.length === 0) && needsBotToolPlanRepair(content)) {
      const repairedContent = await requestBotToolPlanRepair(provider, settings, model, workingMessages, content);
      if (repairedContent) {
        content = repairedContent;
        plan = parseBotAssistantToolPlan(repairedContent);
      }
    }

    if ((!plan || !Array.isArray(plan.tool_calls) || plan.tool_calls.length === 0) && needsBotToolPlanRepair(content)) {
      const salvagedToolCalls = salvageBotToolCallsFromAssistantText(content, currentServerName);
      if (salvagedToolCalls.length > 0) {
        plan = { reply: "", tool_calls: salvagedToolCalls };
      }
    }

    if (!plan || !Array.isArray(plan.tool_calls) || plan.tool_calls.length === 0) {
      if (needsBotToolPlanRepair(content) && attempt < (MAX_BOT_TOOL_LOOPS - 1)) {
        workingMessages = [
          ...workingMessages,
          {
            role: "system",
            content: "The previous reply was invalid. The next reply must be either one <adpanel_tool_plan> JSON block with no extra text or one short final user-facing sentence with no prompt talk and no reasoning.",
          },
        ];
        continue;
      }
      finalContent = stripBotToolPlanMarkup(plan?.reply || content) || plan?.reply || content;
      break;
    }

    const toolResults = [];
    for (const toolCall of plan.tool_calls) {
      const toolName = String(toolCall?.name || "").trim();
      const toolArgs = sanitizeBotToolArgs(toolName, toolCall?.args);
      const toolResult = await executeBotAssistantToolWithCache({
        toolExecutionCache,
        req,
        name: toolName,
        args: toolArgs,
        currentServerName,
        accessibleServers,
        userEmail,
        userIp,
        clientActions,
      });
      lastToolName = toolName;
      lastToolResult = toolResult;
      toolHistory.push({ name: toolName, result: toolResult });
      toolResults.push({ name: toolName, result: toolResult });
    }

    const assistantContinuationMessage = looksLikeLeakedBotReasoning(content)
      ? null
      : { role: "assistant", content: content.slice(0, 12000) };

    workingMessages = [
      ...workingMessages,
      ...(assistantContinuationMessage ? [assistantContinuationMessage] : []),
      { role: "system", content: buildBotToolResultContextMessage(toolResults) },
      { role: "user", content: buildBotContinueAfterToolInstruction() },
    ];
  }

  if (!finalContent) {
    finalContent = buildBotReplyFromToolHistory(toolHistory) || buildBotReplyFromToolResult(lastToolName, lastToolResult) || "Done.";
  }
  finalContent = finalizeBotAssistantReply(finalContent, lastToolName, lastToolResult) || "I could not produce a clean answer for that request.";

  return {
    content: finalContent,
    thinking_time_ms: thinkingTimeMs,
    thinking_content: thinkingParts.join("\n\n").trim(),
    clientActions,
  };
}

app.post("/api/ai/chat", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  if (!(await hasAgentAccess(req))) return res.status(403).json({ error: "no agent access" });

  const { provider, model, messages, serverContext } = req.body || {};

  if (!provider || !model || !Array.isArray(messages)) {
    return res.status(400).json({ error: "provider, model, and messages are required" });
  }

  const providerLower = String(provider).toLowerCase();
  const providerConfig = getAiProviderConfig(providerLower);

  if (!providerConfig) {
    return res.status(400).json({ error: "invalid provider" });
  }

  const env = readEnvFile();
  const settings = getStoredAiProviderSettings(providerLower, env);

  if (providerConfig.requiresKey !== false && !settings.apiKey) {
    return res.status(400).json({ error: "API key not configured for this provider" });
  }
  if (providerConfig.requiresBaseUrl && !settings.baseUrl) {
    return res.status(400).json({ error: "Base URL not configured for this provider" });
  }

  try {
    const result = await completeAiChatConversation({
      req,
      provider: providerLower,
      settings,
      model,
      messages,
      serverContext,
    });
    return res.json({ ok: true, ...result });
  } catch (err) {
    console.error(`[ai-chat] Error calling ${providerLower}:`, err.message);
    return res.status(500).json({ error: err.message || "AI request failed" });
  }
});

async function proxyOpenAiCompatible(provider, settings, model, messages, options = {}) {
  const providerConfig = getAiProviderConfig(provider);
  const endpoint = provider === 'openai-compatible'
    ? appendPathToBaseUrl(settings.baseUrl, 'chat/completions')
    : providerConfig?.chatEndpoint;
  if (!endpoint) throw new Error('Unknown provider');

  const headers = {
    'Content-Type': 'application/json',
    ...buildAiProviderAuthHeaders(provider, settings.apiKey)
  };

  const transformedMessages = messages.map(m => {
    if (m.image && typeof m.image === 'string' && m.image.startsWith('data:image/')) {
      return {
        role: m.role,
        content: [
          { type: 'text', text: m.content || '' },
          { type: 'image_url', image_url: { url: m.image } }
        ]
      };
    }
    return { role: m.role, content: m.content };
  });

  const body = JSON.stringify({
    model,
    messages: transformedMessages,
    max_tokens: Number.isFinite(options.maxTokens) ? options.maxTokens : 2048,
    temperature: Number.isFinite(options.temperature) ? options.temperature : 0.7
  });

  const response = await proxyAiRequest(endpoint, { method: 'POST', headers, body });

  if (response.status !== 200) {
    throw new Error(extractAiErrorMessage(response));
  }

  const parsed = parseOpenAiCompatibleResponsePayload(response.data);
  return {
    content: parsed.content || response.data?.choices?.[0]?.message?.content || '',
    thinking_content: parsed.thinking_content || '',
  };
}

async function proxyAnthropicAi(apiKey, model, messages, options = {}) {
  const endpoint = 'https://api.anthropic.com/v1/messages';
  const systemInstruction = buildAnthropicSystemMessage(messages);
  const body = {
    model,
    max_tokens: Number.isFinite(options.maxTokens) ? options.maxTokens : 2048,
    messages: buildAnthropicInitialMessages(messages),
  };

  if (systemInstruction) {
    body.system = systemInstruction;
  }
  if (Number.isFinite(options.temperature)) {
    body.temperature = options.temperature;
  }

  const response = await proxyAiRequest(endpoint, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...buildAiProviderAuthHeaders('anthropic', apiKey),
    },
    body: JSON.stringify(body),
  });

  if (response.status !== 200) {
    throw new Error(extractAiErrorMessage(response));
  }

  const parsed = parseAnthropicResponsePayload(response.data);
  return {
    content: parsed.content || normalizeAnthropicTextContent(response.data?.content),
    thinking_content: parsed.thinking_content || '',
  };
}

async function proxyGoogleAi(apiKey, model, messages, options = {}) {
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(apiKey)}`;

  const systemMessage = messages.find(m => m.role === 'system');
  const contents = messages.filter(m => m.role !== 'system').map(m => {
    const parts = [{ text: m.content || '' }];

    if (m.image && typeof m.image === 'string' && m.image.startsWith('data:image/')) {
      const match = m.image.match(/^data:(image\/[^;]+);base64,(.+)$/);
      if (match) {
        parts.push({
          inline_data: {
            mime_type: match[1],
            data: match[2]
          }
        });
      }
    }

    return {
      role: m.role === 'assistant' ? 'model' : 'user',
      parts
    };
  });

  const requestBody = {
    contents,
    generationConfig: {
      maxOutputTokens: Number.isFinite(options.maxTokens) ? options.maxTokens : 2048,
      temperature: Number.isFinite(options.temperature) ? options.temperature : 0.7
    }
  };

  if (systemMessage) {
    requestBody.systemInstruction = { parts: [{ text: systemMessage.content }] };
  }

  const body = JSON.stringify(requestBody);
  const headers = { 'Content-Type': 'application/json' };

  const response = await proxyAiRequest(endpoint, { method: 'POST', headers, body });

  if (response.status !== 200) {
    const errMsg = response.data?.error?.message || `API error: ${response.status}`;
    throw new Error(errMsg);
  }

  const parsed = parseGoogleResponsePayload(response.data);
  return {
    content: parsed.content || response.data?.candidates?.[0]?.content?.parts?.[0]?.text || '',
    thinking_content: parsed.thinking_content || '',
  };
}

async function proxyCohereAiLegacyV1(apiKey, model, messages, options = {}) {
  const endpoint = 'https://api.cohere.ai/v1/chat';
  const systemMessage = messages.find(m => m.role === 'system');
  const lastMessage = messages[messages.length - 1];
  const chatHistory = messages.slice(0, -1).filter(m => m.role !== 'system').map(m => ({
    role: m.role === 'assistant' ? 'CHATBOT' : 'USER',
    message: m.content
  }));

  const requestBody = {
    model,
    message: lastMessage?.content || '',
    chat_history: chatHistory
  };

  if (systemMessage) {
    requestBody.preamble = systemMessage.content;
  }
  if (Number.isFinite(options.temperature)) {
    requestBody.temperature = options.temperature;
  }
  if (Number.isFinite(options.maxTokens)) {
    requestBody.max_tokens = options.maxTokens;
  }

  const body = JSON.stringify(requestBody);
  const headers = {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${apiKey}`
  };

  const response = await proxyAiRequest(endpoint, { method: 'POST', headers, body });

  if (response.status !== 200) {
    const errMsg = response.data?.message || response.data?.error || `API error: ${response.status}`;
    throw new Error(errMsg);
  }

  const parsed = parseCohereResponsePayload(response.data);
  return {
    content: parsed.content || response.data?.text || '',
    thinking_content: parsed.thinking_content || '',
  };
}

function buildCohereStandardMessages(messages) {
  return (Array.isArray(messages) ? messages : [])
    .map((message) => {
      const role = String(message?.role || '').trim().toLowerCase();
      if (!['system', 'user', 'assistant'].includes(role)) return null;
      return {
        role,
        content: typeof message?.content === 'string' ? message.content : String(message?.content || ''),
      };
    })
    .filter(Boolean);
}

async function proxyCohereAi(apiKey, model, messages, options = {}) {
  const endpoint = 'https://api.cohere.com/v2/chat';
  const body = JSON.stringify({
    stream: false,
    model,
    messages: buildCohereStandardMessages(messages),
    max_tokens: Number.isFinite(options.maxTokens) ? options.maxTokens : 2048,
    temperature: Number.isFinite(options.temperature) ? options.temperature : 0.7,
  });

  const headers = {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${apiKey}`
  };

  const response = await proxyAiRequest(endpoint, { method: 'POST', headers, body });
  if (response.status === 200) {
    const parsed = parseCohereResponsePayload(response.data);
    return {
      content: parsed.content || normalizeCohereNativeTextContent(response.data?.message?.content) || String(response.data?.text || '').trim(),
      thinking_content: parsed.thinking_content || '',
    };
  }

  if ([404, 405].includes(response.status)) {
    return proxyCohereAiLegacyV1(apiKey, model, messages, options);
  }

  const errMsg = response.data?.message || response.data?.error || `API error: ${response.status}`;
  throw new Error(errMsg);
}

async function proxyHuggingFaceAi(apiKey, model, messages, options = {}) {
  const chatEndpoint = 'https://router.huggingface.co/v1/chat/completions';

  const chatBody = JSON.stringify({
    model,
    messages,
    max_tokens: Number.isFinite(options.maxTokens) ? options.maxTokens : 2048,
    temperature: Number.isFinite(options.temperature) ? options.temperature : 0.7
  });

  const headers = {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${apiKey}`
  };

  let response = await proxyAiRequest(chatEndpoint, { method: 'POST', headers, body: chatBody });

  if (response.status !== 200) {
    const errStr = JSON.stringify(response.data || {}).toLowerCase();
    const isUnsupported = errStr.includes('no endpoints') || errStr.includes('not supported') || errStr.includes('not found');

    if (isUnsupported) {
      console.log(`[ai-hf] Chat API not supported for ${model}, trying inference API`);
      const inferenceEndpoint = `https://router.huggingface.co/hf-inference/models/${encodeURIComponent(model)}/v1/chat/completions`;

      response = await proxyAiRequest(inferenceEndpoint, { method: 'POST', headers, body: chatBody });

      if (response.status !== 200) {
        const textEndpoint = `https://router.huggingface.co/hf-inference/models/${encodeURIComponent(model)}`;

        const systemMsg = messages.find(m => m.role === 'system');
        let prompt = '';
        if (systemMsg) prompt += `System: ${systemMsg.content}\n\n`;
        for (const msg of messages.filter(m => m.role !== 'system')) {
          prompt += `${msg.role === 'user' ? 'User' : 'Assistant'}: ${msg.content}\n`;
        }
        prompt += 'Assistant:';

        const textBody = JSON.stringify({
          inputs: prompt,
          parameters: {
            max_new_tokens: Number.isFinite(options.maxTokens) ? options.maxTokens : 2048,
            temperature: Number.isFinite(options.temperature) ? options.temperature : 0.7,
            return_full_text: false
          }
        });

        response = await proxyAiRequest(textEndpoint, { method: 'POST', headers, body: textBody });

        if (response.status === 200) {
          let content = '';
          if (Array.isArray(response.data)) {
            content = response.data[0]?.generated_text || '';
          } else if (response.data?.generated_text) {
            content = response.data.generated_text;
          }
          const extracted = extractThinkingSectionsFromTaggedText(content);
          return {
            content: extracted.content || content,
            thinking_content: extracted.thinking_content || '',
          };
        }
      }
    }

    if (response.status !== 200) {
      const errMsg = response.data?.error?.message || response.data?.error || response.data?.message || `API error: ${response.status}`;
      throw new Error(errMsg);
    }
  }

  const parsed = parseOpenAiCompatibleResponsePayload(response.data);
  return {
    content: parsed.content || response.data?.choices?.[0]?.message?.content || '',
    thinking_content: parsed.thinking_content || '',
  };
}

app.get("/api/ai/models/:provider", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  if (!(await hasAgentAccess(req))) return res.status(403).json({ error: "no agent access" });

  const provider = String(req.params.provider || '').toLowerCase();
  const providerConfig = getAiProviderConfig(provider);

  if (!providerConfig) return res.status(400).json({ error: "invalid provider" });

  const env = readEnvFile();
  const settings = getStoredAiProviderSettings(provider, env);

  if (providerConfig.requiresKey !== false && !settings.apiKey) {
    return res.status(400).json({ error: "API key not configured" });
  }
  if (providerConfig.requiresBaseUrl && !settings.baseUrl) {
    return res.status(400).json({ error: "Base URL not configured" });
  }

  try {
    const discovery = await fetchAvailableAiModels(provider, settings);
    const finalSettings = {
      ...settings,
      ...(discovery?.resolvedBaseUrl ? { baseUrl: discovery.resolvedBaseUrl } : {})
    };

    return res.json({
      ok: true,
      models: discovery.models,
      providerState: buildAiProviderState(provider, finalSettings, { includeBaseUrl: await isAdmin(req) })
    });
  } catch (err) {
    console.error(`[ai-models] Error fetching models for ${provider}:`, err.message);
    return res.status(502).json({ error: err.message || 'failed to load models' });
  }
});


async function initAiChatTables() {
  if (!db || !db.query) {
    console.warn('[ai-chat] Database not available, skipping table initialization');
    return;
  }

  try {
    await db.query(`
      CREATE TABLE IF NOT EXISTS ai_chats (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        user_email VARCHAR(255) NOT NULL,
        title VARCHAR(255) NOT NULL DEFAULT 'New Chat',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        KEY idx_ai_chats_user (user_email),
        KEY idx_ai_chats_created (created_at)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);
    await db.query(`
      CREATE TABLE IF NOT EXISTS ai_messages (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        chat_id BIGINT UNSIGNED NOT NULL,
        role ENUM('user', 'assistant', 'system') NOT NULL,
        content LONGTEXT NOT NULL,
        image_data LONGTEXT NULL,
        thinking_time_ms INT UNSIGNED NULL,
        thinking_content LONGTEXT NULL,
        model VARCHAR(255) NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        KEY idx_ai_messages_chat (chat_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    try {
      await db.query(`ALTER TABLE ai_chats ADD COLUMN user_email VARCHAR(255) NOT NULL DEFAULT '' AFTER id`);
      console.log('[ai-chat] Added user_email column to ai_chats');
    } catch (alterErr) {
      if (!alterErr.message.includes('Duplicate column')) {
      }
    }

    try {
      await db.query(`CREATE INDEX idx_ai_chats_user ON ai_chats(user_email)`);
    } catch (idxErr) {
    }

    try {
      await db.query(`ALTER TABLE ai_messages ADD COLUMN image_data LONGTEXT NULL AFTER content`);
      console.log('[ai-chat] Added image_data column to ai_messages');
    } catch (alterErr) {
      if (!alterErr.message.includes('Duplicate column')) {
        console.log('[ai-chat] image_data column already exists or error:', alterErr.message);
      }
    }

    try {
      await db.query(`ALTER TABLE ai_messages ADD COLUMN thinking_content LONGTEXT NULL AFTER thinking_time_ms`);
      console.log('[ai-chat] Added thinking_content column to ai_messages');
    } catch (alterErr) {
      if (!alterErr.message.includes('Duplicate column')) {
        console.log('[ai-chat] thinking_content column already exists or error:', alterErr.message);
      }
    }

    console.log('[ai-chat] Tables initialized successfully');
  } catch (err) {
    console.error('[ai-chat] Failed to init tables:', err.message);
  }
}

let plannerTablesReady = false;

async function initPlannerTables() {
  if (!db || !db.query) {
    console.warn('[planner] Database not available, skipping table initialization');
    return;
  }

  try {
    await db.query(`
      CREATE TABLE IF NOT EXISTS server_planner_items (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        server_name VARCHAR(120) NOT NULL,
        title VARCHAR(255) NOT NULL,
        prompt LONGTEXT NOT NULL,
        is_done TINYINT(1) NOT NULL DEFAULT 0,
        sort_order INT NOT NULL DEFAULT 0,
        created_by_email VARCHAR(255) NOT NULL,
        updated_by_email VARCHAR(255) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        KEY idx_server_planner_server (server_name),
        KEY idx_server_planner_sort (server_name, is_done, sort_order, updated_at)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);
    plannerTablesReady = true;
    console.log('[planner] Tables initialized successfully');
  } catch (err) {
    console.error('[planner] Failed to init tables:', err.message);
  }
}

setTimeout(() => initAiChatTables(), 3000);
setTimeout(() => initPlannerTables(), 3200);

app.get("/api/ai/chats", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  if (!(await hasAgentAccess(req))) return res.status(403).json({ error: "no agent access" });

  if (!db || !db.query) {
    return res.json({ ok: true, chats: [] });
  }

  const userEmail = String(req.session.user || '').toLowerCase();
  if (!userEmail) return res.status(401).json({ error: 'not authenticated' });

  try {
    const chats = await db.query(`
      SELECT c.id, c.title, c.created_at, c.updated_at,
             (SELECT COUNT(*) FROM ai_messages WHERE chat_id = c.id) as message_count
      FROM ai_chats c
      WHERE c.user_email = ?
      ORDER BY c.updated_at DESC
      LIMIT 100
    `, [userEmail]);
    return res.json({ ok: true, chats: chats || [] });
  } catch (err) {
    console.error('[ai-chats] Error:', err.message);
    return res.json({ ok: true, chats: [] });
  }
});

app.post("/api/ai/chats", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  if (!(await hasAgentAccess(req))) return res.status(403).json({ error: "no agent access" });

  if (!db || !db.query) {
    return res.status(503).json({ error: 'Database not available' });
  }

  const userEmail = String(req.session.user || '').toLowerCase();
  if (!userEmail) return res.status(401).json({ error: 'not authenticated' });

  const { title } = req.body || {};
  const chatTitle = String(title || 'New Chat').trim().slice(0, 255);

  try {
    const result = await db.query(
      'INSERT INTO ai_chats (user_email, title) VALUES (?, ?)',
      [userEmail, chatTitle]
    );
    return res.json({ ok: true, chatId: result.insertId, title: chatTitle });
  } catch (err) {
    console.error('[ai-chats] Create error:', err.message);
    if (err.message.includes("doesn't exist")) {
      await initAiChatTables();
      try {
        const result = await db.query(
          'INSERT INTO ai_chats (title) VALUES (?)',
          [chatTitle]
        );
        return res.json({ ok: true, chatId: result.insertId, title: chatTitle });
      } catch (retryErr) {
        console.error('[ai-chats] Retry failed:', retryErr.message);
      }
    }
    return res.status(500).json({ error: 'Failed to create chat' });
  }
});

app.get("/api/ai/chats/:id", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  if (!(await hasAgentAccess(req))) return res.status(403).json({ error: "no agent access" });

  if (!db || !db.query) {
    return res.status(503).json({ error: 'Database not available' });
  }

  const chatId = parseInt(req.params.id, 10);
  if (!chatId || isNaN(chatId)) return res.status(400).json({ error: 'Invalid chat ID' });

  const userEmail = String(req.session.user || '').toLowerCase();
  if (!userEmail) return res.status(401).json({ error: 'not authenticated' });

  try {
    const chatRows = await db.query('SELECT * FROM ai_chats WHERE id = ? AND user_email = ?', [chatId, userEmail]);
    const chat = chatRows[0];
    if (!chat) return res.status(404).json({ error: 'Chat not found' });

    const messages = await db.query(
      'SELECT id, role, content, image_data, thinking_time_ms, thinking_content, model, created_at FROM ai_messages WHERE chat_id = ? ORDER BY created_at ASC',
      [chatId]
    );

    return res.json({ ok: true, chat, messages: messages || [] });
  } catch (err) {
    console.error('[ai-chats] Get error:', err.message);
    return res.status(500).json({ error: 'Failed to load chat' });
  }
});

app.put("/api/ai/chats/:id", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  if (!(await hasAgentAccess(req))) return res.status(403).json({ error: "no agent access" });

  if (!db || !db.query) {
    return res.status(503).json({ error: 'Database not available' });
  }

  const chatId = parseInt(req.params.id, 10);
  if (!chatId || isNaN(chatId)) return res.status(400).json({ error: 'Invalid chat ID' });

  const userEmail = String(req.session.user || '').toLowerCase();
  if (!userEmail) return res.status(401).json({ error: 'not authenticated' });

  const { title } = req.body || {};
  const chatTitle = String(title || 'New Chat').trim().slice(0, 255);

  try {
    const result = await db.query('UPDATE ai_chats SET title = ? WHERE id = ? AND user_email = ?', [chatTitle, chatId, userEmail]);
    if (result.affectedRows === 0) return res.status(404).json({ error: 'Chat not found' });
    return res.json({ ok: true });
  } catch (err) {
    console.error('[ai-chats] Update error:', err.message);
    return res.status(500).json({ error: 'Failed to update chat' });
  }
});

app.delete("/api/ai/chats/:id", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  if (!(await hasAgentAccess(req))) return res.status(403).json({ error: "no agent access" });

  if (!db || !db.query) {
    return res.status(503).json({ error: 'Database not available' });
  }

  const chatId = parseInt(req.params.id, 10);
  if (!chatId || isNaN(chatId)) return res.status(400).json({ error: 'Invalid chat ID' });

  const userEmail = String(req.session.user || '').toLowerCase();
  if (!userEmail) return res.status(401).json({ error: 'not authenticated' });

  try {
    const result = await db.query('DELETE FROM ai_chats WHERE id = ? AND user_email = ?', [chatId, userEmail]);
    if (result.affectedRows === 0) return res.status(404).json({ error: 'Chat not found' });
    await db.query('DELETE FROM ai_messages WHERE chat_id = ?', [chatId]);
    return res.json({ ok: true });
  } catch (err) {
    console.error('[ai-chats] Delete error:', err.message);
    return res.status(500).json({ error: 'Failed to delete chat' });
  }
});

app.post("/api/ai/chats/:id/messages", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  if (!(await hasAgentAccess(req))) return res.status(403).json({ error: "no agent access" });

  if (!db || !db.query) {
    return res.status(503).json({ error: 'Database not available' });
  }

  const chatId = parseInt(req.params.id, 10);
  if (!chatId || isNaN(chatId)) return res.status(400).json({ error: 'Invalid chat ID' });

  const userEmail = String(req.session.user || '').toLowerCase();
  if (!userEmail) return res.status(401).json({ error: 'not authenticated' });

  try {
    const chatRows = await db.query('SELECT id FROM ai_chats WHERE id = ? AND user_email = ?', [chatId, userEmail]);
    if (!chatRows || !chatRows[0]) return res.status(404).json({ error: 'Chat not found' });
  } catch (err) {
    console.error('[ai-chats] Ownership check error:', err.message);
    return res.status(500).json({ error: 'Failed to verify chat ownership' });
  }

  const { role, content, thinking_time_ms, thinking_content, model, image_data } = req.body || {};

  if (!role || !content) {
    return res.status(400).json({ error: 'role and content are required' });
  }

  const validRoles = ['user', 'assistant', 'system'];
  if (!validRoles.includes(role)) {
    return res.status(400).json({ error: 'Invalid role' });
  }

  let sanitizedImageData = null;
  if (image_data && typeof image_data === 'string') {
    if (image_data.startsWith('data:image/')) {
      if (image_data.length <= 10 * 1024 * 1024) {
        sanitizedImageData = image_data;
      }
    }
  }

  try {
    const result = await db.query(
      'INSERT INTO ai_messages (chat_id, role, content, image_data, thinking_time_ms, thinking_content, model) VALUES (?, ?, ?, ?, ?, ?, ?)',
      [chatId, role, content, sanitizedImageData, thinking_time_ms || null, String(thinking_content || '').trim() || null, model || null]
    );

    await db.query('UPDATE ai_chats SET updated_at = NOW() WHERE id = ?', [chatId]);

    if (role === 'user') {
      const chatRows = await db.query('SELECT title FROM ai_chats WHERE id = ?', [chatId]);
      const chat = chatRows[0];
      if (chat && chat.title === 'New Chat') {
        const autoTitle = String(content).slice(0, 50).trim() + (content.length > 50 ? '...' : '');
        await db.query('UPDATE ai_chats SET title = ? WHERE id = ?', [autoTitle, chatId]);
      }
    }

    return res.json({ ok: true, messageId: result.insertId });
  } catch (err) {
    console.error('[ai-messages] Add error:', err.message);
    return res.status(500).json({ error: 'Failed to add message' });
  }
});

function derivePlannerTitleFromPrompt(prompt) {
  const clean = String(prompt || '').trim().replace(/\s+/g, ' ');
  if (!clean) return 'Untitled Prompt';
  return clean.length > 72 ? `${clean.slice(0, 69)}...` : clean;
}

function sanitizePlannerTitle(value) {
  return String(value || '').trim().replace(/\s+/g, ' ').slice(0, 255);
}

function sanitizePlannerPrompt(value) {
  return String(value || '').trim().slice(0, 4000);
}

async function fetchPlannerItemsForServer(serverName) {
  return db.query(
    `SELECT id, server_name, title, prompt, is_done, sort_order, created_by_email, updated_by_email, created_at, updated_at
     FROM server_planner_items
     WHERE server_name = ?
     ORDER BY is_done ASC, sort_order ASC, updated_at DESC, id ASC`,
    [serverName]
  );
}

async function getNextPlannerSortOrder(serverName, isDone) {
  const rows = await db.query(
    'SELECT COALESCE(MAX(sort_order), -1) AS max_sort_order FROM server_planner_items WHERE server_name = ? AND is_done = ?',
    [serverName, isDone ? 1 : 0]
  );
  return Number(rows?.[0]?.max_sort_order ?? -1) + 1;
}

async function resolvePlannerContext(req, res, rawServerName) {
  if (!(await isAuthenticated(req))) {
    res.status(401).json({ error: 'not authenticated' });
    return null;
  }
  if (!db || !db.query) {
    res.status(503).json({ error: 'Database not available' });
    return null;
  }
  if (!plannerTablesReady) {
    await initPlannerTables();
  }

  const requestedServerName = String(rawServerName || '').trim();
  if (!requestedServerName) {
    res.status(400).json({ error: 'Server name is required' });
    return null;
  }

  const entry = await findServer(requestedServerName);
  if (!entry) {
    res.status(404).json({ error: 'Server not found' });
    return null;
  }

  const serverName = String(entry.name || requestedServerName).trim();
  const userEmail = String(req.session?.user || '').trim().toLowerCase();
  if (!userEmail) {
    res.status(401).json({ error: 'not authenticated' });
    return null;
  }

  if (!(await isAdmin(req)) && !(await userHasAccessToServer(userEmail, serverName))) {
    res.status(403).json({ error: 'access denied' });
    return null;
  }

  return { serverName, userEmail };
}

app.get("/api/servers/:name/planner", async (req, res) => {
  const ctx = await resolvePlannerContext(req, res, req.params.name);
  if (!ctx) return;

  try {
    const items = await fetchPlannerItemsForServer(ctx.serverName);
    return res.json({ ok: true, items });
  } catch (err) {
    console.error('[planner] List error:', err.message);
    return res.status(500).json({ error: 'Failed to load planner items' });
  }
});

app.post("/api/servers/:name/planner", async (req, res) => {
  const ctx = await resolvePlannerContext(req, res, req.params.name);
  if (!ctx) return;

  const prompt = sanitizePlannerPrompt(req.body?.prompt);
  const title = sanitizePlannerTitle(req.body?.title) || derivePlannerTitleFromPrompt(prompt);
  if (!prompt) {
    return res.status(400).json({ error: 'Prompt is required' });
  }

  try {
    const sortOrder = await getNextPlannerSortOrder(ctx.serverName, false);
    const result = await db.query(
      `INSERT INTO server_planner_items
       (server_name, title, prompt, is_done, sort_order, created_by_email, updated_by_email)
       VALUES (?, ?, ?, 0, ?, ?, ?)`,
      [ctx.serverName, title, prompt, sortOrder, ctx.userEmail, ctx.userEmail]
    );
    const items = await fetchPlannerItemsForServer(ctx.serverName);
    return res.json({ ok: true, itemId: result.insertId, items });
  } catch (err) {
    console.error('[planner] Create error:', err.message);
    return res.status(500).json({ error: 'Failed to create planner item' });
  }
});

app.post("/api/servers/:name/planner/reorder", async (req, res) => {
  const ctx = await resolvePlannerContext(req, res, req.params.name);
  if (!ctx) return;

  const requestedIds = Array.isArray(req.body?.itemIds)
    ? req.body.itemIds.map((value) => parseInt(value, 10)).filter((value) => Number.isFinite(value))
    : [];
  if (!requestedIds.length) {
    return res.status(400).json({ error: 'itemIds are required' });
  }

  try {
    const existingActive = await db.query(
      'SELECT id FROM server_planner_items WHERE server_name = ? AND is_done = 0 ORDER BY sort_order ASC, id ASC',
      [ctx.serverName]
    );
    const existingIds = existingActive.map((row) => Number(row.id));
    const normalizedRequested = requestedIds.filter((id) => existingIds.includes(id));
    const remainingIds = existingIds.filter((id) => !normalizedRequested.includes(id));
    const finalIds = [...normalizedRequested, ...remainingIds];

    for (let index = 0; index < finalIds.length; index += 1) {
      await db.query(
        'UPDATE server_planner_items SET sort_order = ?, updated_by_email = ? WHERE id = ? AND server_name = ?',
        [index, ctx.userEmail, finalIds[index], ctx.serverName]
      );
    }

    const items = await fetchPlannerItemsForServer(ctx.serverName);
    return res.json({ ok: true, items });
  } catch (err) {
    console.error('[planner] Reorder error:', err.message);
    return res.status(500).json({ error: 'Failed to reorder planner items' });
  }
});

app.put("/api/servers/:name/planner/:id", async (req, res) => {
  const ctx = await resolvePlannerContext(req, res, req.params.name);
  if (!ctx) return;

  const itemId = parseInt(req.params.id, 10);
  if (!Number.isFinite(itemId)) {
    return res.status(400).json({ error: 'Invalid planner item ID' });
  }

  try {
    const rows = await db.query(
      'SELECT id, title, prompt, is_done, sort_order FROM server_planner_items WHERE id = ? AND server_name = ? LIMIT 1',
      [itemId, ctx.serverName]
    );
    const current = rows?.[0];
    if (!current) {
      return res.status(404).json({ error: 'Planner item not found' });
    }

    const body = req.body || {};
    const hasTitle = Object.prototype.hasOwnProperty.call(body, 'title');
    const hasPrompt = Object.prototype.hasOwnProperty.call(body, 'prompt');
    const hasDone = Object.prototype.hasOwnProperty.call(body, 'isDone');

    const nextPrompt = hasPrompt ? sanitizePlannerPrompt(body.prompt) : String(current.prompt || '');
    if (!nextPrompt) {
      return res.status(400).json({ error: 'Prompt is required' });
    }

    const nextTitle = (hasTitle ? sanitizePlannerTitle(body.title) : String(current.title || '').trim()) || derivePlannerTitleFromPrompt(nextPrompt);
    const nextIsDone = hasDone ? !!body.isDone : !!current.is_done;
    let nextSortOrder = Number(current.sort_order || 0);

    if (nextIsDone !== !!current.is_done) {
      nextSortOrder = await getNextPlannerSortOrder(ctx.serverName, nextIsDone);
    }

    await db.query(
      `UPDATE server_planner_items
       SET title = ?, prompt = ?, is_done = ?, sort_order = ?, updated_by_email = ?
       WHERE id = ? AND server_name = ?`,
      [nextTitle, nextPrompt, nextIsDone ? 1 : 0, nextSortOrder, ctx.userEmail, itemId, ctx.serverName]
    );

    const items = await fetchPlannerItemsForServer(ctx.serverName);
    return res.json({ ok: true, items });
  } catch (err) {
    console.error('[planner] Update error:', err.message);
    return res.status(500).json({ error: 'Failed to update planner item' });
  }
});

app.delete("/api/servers/:name/planner/:id", async (req, res) => {
  const ctx = await resolvePlannerContext(req, res, req.params.name);
  if (!ctx) return;

  const itemId = parseInt(req.params.id, 10);
  if (!Number.isFinite(itemId)) {
    return res.status(400).json({ error: 'Invalid planner item ID' });
  }

  try {
    const result = await db.query(
      'DELETE FROM server_planner_items WHERE id = ? AND server_name = ?',
      [itemId, ctx.serverName]
    );
    if (!result.affectedRows) {
      return res.status(404).json({ error: 'Planner item not found' });
    }
    const items = await fetchPlannerItemsForServer(ctx.serverName);
    return res.json({ ok: true, items });
  } catch (err) {
    console.error('[planner] Delete error:', err.message);
    return res.status(500).json({ error: 'Failed to delete planner item' });
  }
});

const QUICK_ACTIONS_FILE = path.join(__dirname, "quick-actions.json");

const DEFAULT_ADMIN_ACTIONS = [
  { id: 'default-admin-1', title: 'Manage account', icon: 'fa-solid fa-list-check', link: '/settings?panel=account', animation: 'default', newTab: false },
  { id: 'default-admin-2', title: 'Manage servers', icon: 'fa-solid fa-bars-progress', link: '/settings?panel=servers', animation: 'default', newTab: false },
  { id: 'default-admin-3', title: 'ADPanel Site', icon: 'fa-solid fa-globe', link: 'https://ad-panel.com', animation: 'default', newTab: true },
];

const DEFAULT_USER_ACTIONS = [
  { id: 'default-user-1', title: 'Manage account', icon: 'fa-solid fa-list-check', link: '/settings?panel=account', animation: 'default', newTab: false },
];

let _quickActionsCache = null;
let _quickActionsCacheTs = 0;
const QUICK_ACTIONS_CACHE_TTL_MS = 30000;

function loadQuickActions() {
  const now = Date.now();
  if (_quickActionsCache && (now - _quickActionsCacheTs) < QUICK_ACTIONS_CACHE_TTL_MS) return _quickActionsCache;
  try {
    if (fs.existsSync(QUICK_ACTIONS_FILE)) {
      const data = fs.readFileSync(QUICK_ACTIONS_FILE, "utf8");
      const parsed = JSON.parse(data);

      if (Array.isArray(parsed.actions) && !parsed.admin && !parsed.user) {
        const result = {
          admin: parsed.actions,
          user: DEFAULT_USER_ACTIONS
        };
        _quickActionsCache = result;
        _quickActionsCacheTs = now;
        return result;
      }

      const result = {
        admin: Array.isArray(parsed.admin) ? parsed.admin : DEFAULT_ADMIN_ACTIONS,
        user: Array.isArray(parsed.user) ? parsed.user : DEFAULT_USER_ACTIONS
      };
      _quickActionsCache = result;
      _quickActionsCacheTs = now;
      return result;
    }
  } catch (err) {
    console.error("[quick-actions] Failed to load:", err);
  }
  const result = {
    admin: DEFAULT_ADMIN_ACTIONS,
    user: DEFAULT_USER_ACTIONS
  };
  _quickActionsCache = result;
  _quickActionsCacheTs = now;
  return result;
}

function saveQuickActions(actions) {
  try {
    safeWriteJson(QUICK_ACTIONS_FILE, { ...actions, updatedAt: Date.now() });
    _quickActionsCache = null;
    _quickActionsCacheTs = 0;
    return true;
  } catch (err) {
    console.error("[quick-actions] Failed to save:", err);
    return false;
  }
}

app.get("/api/settings/quick-actions", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  const actions = loadQuickActions();
  return res.json({ ok: true, actions });
});

app.post("/api/settings/quick-actions", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });

  const { admin, user } = req.body || {};

  const validateList = (list, name) => {
    if (!Array.isArray(list)) return { valid: false, error: `${name} actions must be an array` };
    if (list.length > 6) return { valid: false, error: `Maximum of 6 ${name} quick actions allowed` };

    const validItems = [];
    for (const action of list) {
      const title = String(action.title || "").trim();
      const link = String(action.link || "").trim();
      const icon = String(action.icon || "fa-solid fa-star").trim();
      const animation = String(action.animation || "default").trim();
      const newTab = Boolean(action.newTab);
      const id = String(action.id || `qa-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`).trim();

      if (!title || title.length > 30) return { valid: false, error: `Invalid title for action: ${title || 'empty'}` };
      if (!link) return { valid: false, error: `Link is required for action: ${title}` };
      if (!link.startsWith('/') && !link.startsWith('http://') && !link.startsWith('https://')) {
        return { valid: false, error: `Invalid link format for action: ${title}` };
      }

      const cleanIcon = icon.replace(/[^a-zA-Z0-9\-\s]/g, '');
      const validAnimations = ['default', 'pulse', 'shake', 'bounce', 'glow', 'scale', 'rotate', 'none'];
      const cleanAnimation = validAnimations.includes(animation) ? animation : 'default';

      let scaleValue = parseFloat(action.scaleValue);
      if (isNaN(scaleValue) || scaleValue < 0.1 || scaleValue > 5) scaleValue = 1.05;

      validItems.push({ id, title, link, icon: cleanIcon, animation: cleanAnimation, newTab, scaleValue });
    }
    return { valid: true, items: validItems };
  };

  const adminResult = validateList(admin || [], 'admin');
  if (!adminResult.valid) return res.status(400).json({ error: adminResult.error });

  const userResult = validateList(user || [], 'user');
  if (!userResult.valid) return res.status(400).json({ error: userResult.error });

  const success = saveQuickActions({
    admin: adminResult.items,
    user: userResult.items
  });

  if (success) {
    return res.json({ ok: true });
  } else {
    return res.status(500).json({ error: "Failed to save quick actions" });
  }
});

const DATABASE_CONFIG_FILE = path.join(__dirname, "database-config.json");

const _processIsRoot = (typeof process.getuid === 'function') ? process.getuid() === 0 : false;

function _privBashSpawn(cmd, opts) {
  if (_processIsRoot) return spawn("bash", ["-c", cmd], opts);
  return spawn("sudo", ["-n", "bash", "-c", cmd], opts);
}

function _privSpawn(command, args, opts) {
  if (_processIsRoot) return spawn(command, args, opts);
  return spawn("sudo", ["-n", command, ...args], opts);
}

function _userSpawn(user, command, args, opts) {
  if (_processIsRoot) return spawn("runuser", ["-u", user, "--", command, ...args], opts);
  return spawn("sudo", ["-n", "-u", user, command, ...args], opts);
}

function _privExecSync(cmd, opts) {
  const { execSync } = require('child_process');
  if (_processIsRoot) return execSync(cmd, opts);
  return execSync(`sudo -n bash -c ${JSON.stringify(cmd)}`, opts);
}

let _dbConfigCache = null;
let _dbConfigCacheTs = 0;
const DB_CONFIG_CACHE_TTL_MS = 30000;

function loadDatabaseConfig() {
  const now = Date.now();
  if (_dbConfigCache && (now - _dbConfigCacheTs) < DB_CONFIG_CACHE_TTL_MS) return _dbConfigCache;
  try {
    if (!fs.existsSync(DATABASE_CONFIG_FILE)) {
      const result = { enabled: false, config: null, users: [] };
      _dbConfigCache = result;
      _dbConfigCacheTs = now;
      return result;
    }
    const raw = fs.readFileSync(DATABASE_CONFIG_FILE, "utf8").trim();
    if (!raw) {
      const result = { enabled: false, config: null, users: [] };
      _dbConfigCache = result;
      _dbConfigCacheTs = now;
      return result;
    }
    const data = JSON.parse(raw);
    const result = {
      enabled: !!data.enabled,
      config: data.config || null,
      users: Array.isArray(data.users) ? data.users : []
    };
    _dbConfigCache = result;
    _dbConfigCacheTs = now;
    return result;
  } catch {
    const result = { enabled: false, config: null, users: [] };
    _dbConfigCache = result;
    _dbConfigCacheTs = now;
    return result;
  }
}

function saveDatabaseConfig(data) {
  try {
    fs.writeFileSync(DATABASE_CONFIG_FILE, JSON.stringify(data, null, 2), { encoding: "utf8", mode: 0o600 });
    try { fs.chmodSync(DATABASE_CONFIG_FILE, 0o600); } catch (e) { }
    _dbConfigCache = null;
    _dbConfigCacheTs = 0;
    return true;
  } catch (err) {
    console.error("[database] Failed to save config:", err);
    return false;
  }
}

async function checkPackageInstalled(packageName) {
  return new Promise((resolve) => {
    const child = spawn("which", [packageName], { stdio: "ignore" });
    child.on("close", (code) => {
      resolve(code === 0);
    });
    child.on("error", () => resolve(false));
  });
}


// ─── Nginx Cleanup Helpers for Secure DB Proxy ──────────────────
function removeDbSnippetIncludes(snippetName) {
  const dirs = ['/etc/nginx/sites-enabled', '/etc/nginx/sites-available'];
  for (const dir of dirs) {
    try {
      if (!fs.existsSync(dir)) continue;
      for (const f of fs.readdirSync(dir)) {
        const full = path.join(dir, f);
        try {
          const content = fs.readFileSync(full, 'utf8');
          if (content.includes(snippetName)) {
            const cleaned = content.replace(new RegExp(`\\n?\\s*include\\s+/etc/nginx/snippets/${snippetName};`, 'g'), '');
            if (cleaned !== content) {
              fs.writeFileSync(full, cleaned);
              console.log(`[db-proxy] Removed ${snippetName} include from ${full}`);
            }
          }
        } catch { }
      }
    } catch { }
  }
}

function migrateDbToolsNginxOnStartup() {
  const { execSync } = require('child_process');
  let changed = false;

  try {
    // phpMyAdmin: migrate from public snippet to internal-only server
    const phpmyadminInstalled = fs.existsSync("/usr/share/phpmyadmin/index.php");
    const internalConf = '/etc/nginx/conf.d/adpanel-phpmyadmin-internal.conf';

    if (phpmyadminInstalled && !fs.existsSync(internalConf)) {
      console.log('[db-proxy] Migrating phpMyAdmin to internal-only nginx server...');
      configurePhpMyAdminWebServer();
      changed = true;
    } else if (phpmyadminInstalled) {
      // Still ensure public snippet is empty and includes are removed
      const snippet = '/etc/nginx/snippets/phpmyadmin.conf';
      if (fs.existsSync(snippet)) {
        const content = fs.readFileSync(snippet, 'utf8');
        if (content.includes('location /phpmyadmin') || content.includes('fastcgi_pass')) {
          fs.writeFileSync(snippet, '# phpMyAdmin - served via secure token proxy only\n', { mode: 0o644 });
          removeDbSnippetIncludes('phpmyadmin.conf');
          changed = true;
          console.log('[db-proxy] Replaced old public phpMyAdmin snippet with placeholder');
        }
      }
    }

    // pgAdmin: ensure public snippet is empty
    const pgadminSnippet = '/etc/nginx/snippets/pgadmin4.conf';
    if (fs.existsSync(pgadminSnippet)) {
      const content = fs.readFileSync(pgadminSnippet, 'utf8');
      if (content.includes('proxy_pass') || content.includes('location')) {
        fs.writeFileSync(pgadminSnippet, '# pgAdmin4 - served via secure token proxy only\n', { mode: 0o644 });
        removeDbSnippetIncludes('pgadmin4.conf');
        changed = true;
        console.log('[db-proxy] Replaced old public pgAdmin4 snippet with placeholder');
      }
    }

    if (changed) {
      try {
        execSync('nginx -t && systemctl reload nginx', { stdio: 'pipe', timeout: 15000 });
        console.log('[db-proxy] Nginx reloaded after migration');
      } catch (e) {
        console.warn('[db-proxy] Nginx reload failed:', e.message);
        try { execSync('systemctl restart nginx', { stdio: 'pipe', timeout: 15000 }); } catch { }
      }
    }
  } catch (e) {
    console.warn('[db-proxy] Startup migration warning:', e.message);
  }
}

function detectPhpFpmSocket() {
  try {
    const files = fs.readdirSync('/run/php/');
    const versioned = files
      .filter(f => /^php\d+\.\d+-fpm\.sock$/.test(f))
      .sort()
      .reverse();
    if (versioned.length > 0) return `/run/php/${versioned[0]}`;
  } catch { }
  if (fs.existsSync('/run/php/php-fpm.sock')) return '/run/php/php-fpm.sock';
  for (const p of ['/var/run/php-fpm.sock', '/run/php-fpm/www.sock']) {
    if (fs.existsSync(p)) return p;
  }
  return '/run/php/php-fpm.sock';
}

function writeSystemFileWithPrivilegeFallback(filePath, content, mode = 0o644) {
  try {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.writeFileSync(filePath, content, { mode });
    return true;
  } catch (err) {
    try {
      const encoded = Buffer.from(String(content), 'utf8').toString('base64');
      const modeStr = Number(mode).toString(8).padStart(4, '0');
      _privExecSync(
        `mkdir -p ${JSON.stringify(path.dirname(filePath))} && printf %s ${JSON.stringify(encoded)} | base64 -d > ${JSON.stringify(filePath)} && chmod ${modeStr} ${JSON.stringify(filePath)}`,
        { stdio: 'pipe', timeout: 20000 }
      );
      return true;
    } catch (privErr) {
      console.error(`[file-write] Failed to write ${filePath}:`, privErr?.message || err?.message || String(err));
      return false;
    }
  }
}

async function installPhpMyAdmin() {
  return new Promise((resolve) => {
    console.log("[database] Checking phpMyAdmin installation...");

    const phpmyadminInstalled = fs.existsSync("/usr/share/phpmyadmin/index.php");
    const phpFpmAvailable = fs.existsSync(detectPhpFpmSocket());
    if (phpmyadminInstalled && phpFpmAvailable) {
      console.log("[database] phpMyAdmin and PHP-FPM are already installed");
      resolve(true);
      return;
    }

    console.log("[database] Installing PHP-FPM and phpMyAdmin...");

    const installCmd = `
      set -e
      export DEBIAN_FRONTEND=noninteractive

      apt-get update -qq

      # Install PHP-FPM and required extensions first
      apt-get install -y --no-install-recommends \\
        php-fpm php-mysql php-mbstring php-zip php-gd php-curl php-xml php-cli

      # Install phpMyAdmin — skip Apache recommends
      apt-get install -y --no-install-recommends phpmyadmin

      # If Apache was pulled in anyway, stop and disable it — Nginx handles web traffic
      if systemctl is-active apache2 >/dev/null 2>&1; then
        systemctl stop apache2 2>/dev/null || true
      fi
      systemctl disable apache2 2>/dev/null || true

      # Ensure PHP-FPM is running
      PHP_FPM_SVC=$(systemctl list-units --type=service --state=running,exited --no-legend | grep -oP 'php[\\d.]+-fpm\\.service' | sort -rV | head -1)
      if [ -z "$PHP_FPM_SVC" ]; then
        # Not running yet — find the newest installed version
        PHP_FPM_SVC=$(systemctl list-unit-files --type=service --no-legend | grep -oP 'php[\\d.]+-fpm\\.service' | sort -rV | head -1)
      fi
      if [ -n "$PHP_FPM_SVC" ]; then
        systemctl enable "$PHP_FPM_SVC" 2>/dev/null || true
        systemctl restart "$PHP_FPM_SVC" 2>/dev/null || true
      fi
    `.trim();

    const installChild = _privBashSpawn(installCmd, {
      shell: false,
      env: { ...process.env, DEBIAN_FRONTEND: "noninteractive" }
    });

    installChild.stdout.on("data", (data) => {
      const line = data.toString().trim();
      if (line) console.log("[database] " + line);
    });

    installChild.stderr.on("data", (data) => {
      const line = data.toString().trim();
      if (line && !line.includes("debconf")) console.error("[database] " + line);
    });

    installChild.on("close", async (installCode) => {
      if (installCode === 0 || fs.existsSync("/usr/share/phpmyadmin/index.php")) {
        console.log("[database] phpMyAdmin and PHP-FPM installed successfully");
        await ensureMySqlRunning();
        resolve(true);
      } else {
        console.error("[database] Installation failed with code:", installCode);
        resolve(false);
      }
    });

    installChild.on("error", (err) => {
      console.error("[database] Installation error:", err);
      resolve(false);
    });
  });
}

function configurePhpMyAdminWebServer() {
  try {
    try {
      _privExecSync('systemctl stop apache2 2>/dev/null; systemctl disable apache2 2>/dev/null', { stdio: 'pipe', timeout: 10000 });
    } catch { }

    const fpmSocket = detectPhpFpmSocket();
    console.log(`[phpmyadmin] Using PHP-FPM socket: ${fpmSocket}`);

    const internalPort = dbProxy.PHPMYADMIN_INTERNAL_PORT;

    // Internal-only nginx server — only reachable from Node.js proxy on 127.0.0.1
    const internalConf = `# phpMyAdmin internal server - ADPanel managed
# Only accessible from the Node.js token-based proxy on 127.0.0.1:${internalPort}
server {
    listen 127.0.0.1:${internalPort};
    server_name _;

    location /phpmyadmin {
        root /usr/share/;
        index index.php index.html index.htm;

        location ~ ^/phpmyadmin/(.+\\.php)$ {
            try_files $uri =404;
            root /usr/share/;
            fastcgi_pass unix:${fpmSocket};
            fastcgi_index index.php;
            fastcgi_param SCRIPT_FILENAME $document_root$fastcgi_script_name;
            include /etc/nginx/fastcgi_params;
            fastcgi_read_timeout 300;
            fastcgi_buffers 16 16k;
            fastcgi_buffer_size 32k;
        }

        location ~* ^/phpmyadmin/(.+\\.(jpg|jpeg|gif|css|png|js|ico|html|xml|txt|svg|woff|woff2|ttf))$ {
            root /usr/share/;
            expires 30d;
            access_log off;
        }
    }
}
`;

    // Write internal server config
    if (!writeSystemFileWithPrivilegeFallback('/etc/nginx/conf.d/adpanel-phpmyadmin-internal.conf', internalConf, 0o644)) {
      throw new Error('Could not write internal phpMyAdmin nginx server config');
    }
    console.log('[phpmyadmin] Internal nginx server written to /etc/nginx/conf.d/adpanel-phpmyadmin-internal.conf');

    // Replace public snippet with empty placeholder (keeps include lines valid but serves nothing)
    if (!writeSystemFileWithPrivilegeFallback('/etc/nginx/snippets/phpmyadmin.conf', '# phpMyAdmin - served via secure token proxy only\n', 0o644)) {
      throw new Error('Could not write phpMyAdmin placeholder snippet');
    }
    console.log('[phpmyadmin] Public snippet replaced with placeholder');

    // Remove old /phpmyadmin include lines from site configs to keep them clean
    removeDbSnippetIncludes('phpmyadmin.conf');

    const snippetsDir = '/etc/nginx/snippets';
    if (!fs.existsSync(path.join(snippetsDir, 'pgadmin4.conf'))) {
      writeSystemFileWithPrivilegeFallback(path.join(snippetsDir, 'pgadmin4.conf'), '# pgAdmin4 - served via secure token proxy only\n', 0o644);
    }
    if (!fs.existsSync(path.join(snippetsDir, 'mongodb.conf'))) {
      writeSystemFileWithPrivilegeFallback(path.join(snippetsDir, 'mongodb.conf'), '# Placeholder - MongoDB not installed\n', 0o644);
    }

    try {
      _privExecSync('nginx -t && systemctl reload nginx', { stdio: 'pipe', timeout: 15000 });
    } catch (e) {
      console.error('[phpmyadmin] Nginx reload error:', e.message);
      try { _privExecSync('systemctl restart nginx', { stdio: 'pipe', timeout: 15000 }); } catch { }
    }

    console.log('[phpmyadmin] Web server configuration complete (internal proxy only)');
    return true;
  } catch (e) {
    console.error('[phpmyadmin] Web server config error:', e.message);
    return false;
  }
}

function disablePhpMyAdminWebServer() {
  try {
    const placeholderContent = '# phpMyAdmin disabled - placeholder\n';
    writeSystemFileWithPrivilegeFallback('/etc/nginx/snippets/phpmyadmin.conf', placeholderContent, 0o644);

    // Remove internal server config
    try {
      const internalConf = '/etc/nginx/conf.d/adpanel-phpmyadmin-internal.conf';
      _privExecSync(`rm -f ${JSON.stringify(internalConf)}`, { stdio: 'pipe', timeout: 10000 });
    } catch { }

    // Remove old include lines from site configs
    removeDbSnippetIncludes('phpmyadmin.conf');

    try {
      _privExecSync('nginx -t && systemctl reload nginx', { stdio: 'pipe', timeout: 15000 });
    } catch (e) { console.error('[phpmyadmin] Nginx reload error:', e.message); }

    console.log('[phpmyadmin] Web server configuration disabled');
    return true;
  } catch (e) {
    console.error('[phpmyadmin] Web server disable error:', e.message);
    return false;
  }
}

function configurePhpMyAdmin(dbConfig) {
  if (!dbConfig) return false;

  const validatedHost = validateDbHost(dbConfig.host);
  if (!validatedHost) {
    console.error("[database] Invalid host for phpMyAdmin config:", dbConfig.host);
    return false;
  }

  const validatedPort = validatePort(dbConfig.port);
  if (!validatedPort) {
    console.error("[database] Invalid port for phpMyAdmin config:", dbConfig.port);
    return false;
  }

  const blowfishSecret = crypto.randomBytes(32).toString("hex");

  const safeHost = escapePhpSingleQuote(validatedHost);
  const safePort = String(validatedPort);

  // Protect the panel database from phpMyAdmin access
  const panelDbName = String(process.env.MYSQL_DATABASE || process.env.MYSQL_DB || "adpanel").trim();
  const safeDbRegex = escapePhpSingleQuote(panelDbName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));

  const configContent = `<?php
/* Servers configuration */
$i = 0;

/* Server: ${safeHost} */
$i++;
$cfg['Servers'][$i]['auth_type'] = 'cookie';
$cfg['Servers'][$i]['host'] = '${safeHost}';
$cfg['Servers'][$i]['port'] = '${safePort}';
$cfg['Servers'][$i]['compress'] = false;
$cfg['Servers'][$i]['AllowNoPassword'] = false;

/* ADPanel Enterprise Security: hide panel database from phpMyAdmin UI */
$cfg['Servers'][$i]['hide_db'] = '^(${safeDbRegex})$';
$cfg['Servers'][$i]['only_db'] = '';

/* End of servers configuration */

$cfg['blowfish_secret'] = '${blowfishSecret}';
$cfg['DefaultLang'] = 'en';
$cfg['ServerDefault'] = 1;
$cfg['UploadDir'] = '';
$cfg['SaveDir'] = '';
$cfg['TempDir'] = '/tmp';

/* Theme */
$cfg['ThemeDefault'] = 'pmahomme';

/* Security */
$cfg['LoginCookieValidity'] = 3600;
$cfg['AllowUserDropDatabase'] = false;
$cfg['ShowCreateDb'] = true;
$cfg['SuhosinDisableWarning'] = true;
$cfg['CaptchaLoginPublicKey'] = '';
$cfg['CaptchaLoginPrivateKey'] = '';
?>`;

  const configPaths = [
    "/etc/phpmyadmin/config.inc.php",
    "/usr/share/phpmyadmin/config.inc.php"
  ];

  for (const configPath of configPaths) {
    if (writeSystemFileWithPrivilegeFallback(configPath, configContent, 0o644)) {
      console.log("[database] phpMyAdmin config written to", configPath);
      return true;
    } else {
      console.warn("[database] Could not write to", configPath);
    }
  }

  console.error("[database] Failed to write phpMyAdmin config to any location");
  return false;
}

const PGADMIN_CONFIG_FILE = path.join(__dirname, "pgadmin-config.json");

function loadPgAdminConfig() {
  try {
    if (fs.existsSync(PGADMIN_CONFIG_FILE)) {
      return JSON.parse(fs.readFileSync(PGADMIN_CONFIG_FILE, "utf8"));
    }
  } catch { }
  return { enabled: false, users: [] };
}

function savePgAdminConfig(data) {
  try {
    fs.writeFileSync(PGADMIN_CONFIG_FILE, JSON.stringify(data, null, 2), { mode: 0o600 });
    return true;
  } catch (err) {
    console.error("[pgadmin] Failed to save config:", err);
    return false;
  }
}

const installationJobs = new Map();

function createInstallJob(type) {
  const jobId = `${type}-${Date.now()}`;
  installationJobs.set(jobId, {
    type,
    status: 'starting',
    progress: 0,
    message: 'Starting installation...',
    startedAt: Date.now(),
    completedAt: null,
    error: null,
    result: null
  });
  return jobId;
}

function updateInstallJob(jobId, updates) {
  const job = installationJobs.get(jobId);
  if (job) {
    Object.assign(job, updates);
  }
}

function getInstallJob(jobId) {
  return installationJobs.get(jobId) || null;
}

setInterval(() => {
  try {
    const now = Date.now();
    for (const [id, job] of installationJobs.entries()) {
      if (job.completedAt) {
        if (now - job.completedAt > 30 * 60 * 1000) installationJobs.delete(id);
      } else if (job.startedAt && (now - job.startedAt > 60 * 60 * 1000)) {
        installationJobs.delete(id);
      }
    }
  } catch (err) { console.debug("[installJobs] sweep error:", err.message); }
}, 300_000).unref();

app.get("/api/settings/database/install-progress/:jobId", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });

  const job = getInstallJob(req.params.jobId);
  if (!job) {
    const jobId = req.params.jobId;
    if (jobId.startsWith('mongodb-')) {
      const config = loadMongoDBConfig();
      if (config.enabled) {
        return res.json({ status: 'completed', progress: 100, message: 'Installation complete!' });
      }
    } else if (jobId.startsWith('pgadmin-')) {
      const config = loadPgAdminConfig();
      if (config.enabled) {
        return res.json({ status: 'completed', progress: 100, message: 'Installation complete!' });
      }
    }
    return res.status(404).json({ error: "Job not found" });
  }
  return res.json(job);
});

async function installPostgreSQLAndPgAdmin(email, password, jobId, listenHost = '127.0.0.1', listenPort = '5432') {
  const runCmd = (cmd, timeoutMs = 120000) => new Promise((resolve) => {
    console.log("[pgadmin] Running install step...");
    const child = _privBashSpawn(cmd, {
      shell: false,
      env: { ...process.env, DEBIAN_FRONTEND: "noninteractive" }
    });
    let stdout = "", stderr = "";
    child.stdout.on("data", (d) => { stdout += d.toString(); });
    child.stderr.on("data", (d) => { stderr += d.toString(); });
    const timeout = setTimeout(() => {
      console.log(`[pgadmin] Command timed out after ${timeoutMs}ms`);
      child.kill();
      resolve({ code: -1, stdout, stderr: "Command timed out" });
    }, timeoutMs);
    child.on("close", (code) => {
      clearTimeout(timeout);
      if (code !== 0) console.log(`[pgadmin] Command exited with code ${code}`);
      resolve({ code, stdout, stderr });
    });
    child.on("error", (err) => { clearTimeout(timeout); resolve({ code: 1, stdout, stderr: err.message }); });
  });

  const validatedHost = validateDbHost(listenHost);
  if (!validatedHost) {
    if (jobId) updateInstallJob(jobId, { status: 'failed', error: 'Invalid listen host address', completedAt: Date.now() });
    return;
  }
  const validatedPort = validatePort(listenPort);
  if (!validatedPort) {
    if (jobId) updateInstallJob(jobId, { status: 'failed', error: 'Invalid listen port', completedAt: Date.now() });
    return;
  }
  listenHost = validatedHost;
  listenPort = String(validatedPort);
  const postgresListenHost = isLocalOnlyDbHost(listenHost) ? 'localhost' : listenHost;

  try {
    console.log("[pgadmin] Starting pgAdmin4 installation (handles fresh install and reinstall)...");

    if (jobId) updateInstallJob(jobId, { status: 'running', progress: 5, message: 'Preparing installation environment...' });
    console.log("[pgadmin] Step 0: Pre-installation cleanup...");
    await runCmd(`
      # Stop any existing services to avoid conflicts during reinstall
      systemctl stop apache2 2>/dev/null || true

      # Clean up any stale Apache pgAdmin configs from previous installs
      rm -f /etc/apache2/sites-enabled/pgadmin4.conf 2>/dev/null || true
      rm -f /etc/apache2/sites-available/pgadmin4.conf 2>/dev/null || true
      rm -f /etc/apache2/conf-enabled/pgadmin4.conf 2>/dev/null || true

      # Clean up stale pgAdmin data directories that might cause issues
      rm -rf /var/lib/pgadmin/sessions/* 2>/dev/null || true

      # Remove stale GPG keys and sources to ensure fresh fetch
      rm -f /usr/share/keyrings/packages-pgadmin-org.gpg 2>/dev/null || true
      rm -f /etc/apt/sources.list.d/pgadmin4.list 2>/dev/null || true

      true
    `, 30000);

    if (jobId) updateInstallJob(jobId, { status: 'running', progress: 10, message: 'Adding PostgreSQL repository...' });
    await runCmd(`
      if [ ! -f /etc/apt/sources.list.d/pgdg.list ]; then
        echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
        wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - 2>/dev/null
      fi
    `);

    if (jobId) updateInstallJob(jobId, { progress: 20, message: 'Adding pgAdmin4 repository...' });
    console.log("[pgadmin] Step 2: Adding pgAdmin4 repository...");
    await runCmd(`
      # Always download fresh GPG key and create sources list
      curl -fsS https://www.pgadmin.org/static/packages_pgadmin_org.pub | gpg --batch --yes --dearmor -o /usr/share/keyrings/packages-pgadmin-org.gpg 2>/dev/null || true
      echo "deb [signed-by=/usr/share/keyrings/packages-pgadmin-org.gpg] https://ftp.postgresql.org/pub/pgadmin/pgadmin4/apt/$(lsb_release -cs) pgadmin4 main" > /etc/apt/sources.list.d/pgadmin4.list
    `);

    if (jobId) updateInstallJob(jobId, { progress: 30, message: 'Updating package lists...' });
    console.log("[pgadmin] Step 3: Updating package lists...");
    const updateRes = await runCmd("DEBIAN_FRONTEND=noninteractive apt-get update -qq", 120000);
    if (updateRes.code !== 0) throw new Error("Failed to update packages: " + updateRes.stderr);

    if (jobId) updateInstallJob(jobId, { progress: 40, message: 'Installing PostgreSQL...' });
    console.log("[pgadmin] Step 4: Installing PostgreSQL...");
    const pgRes = await runCmd("DEBIAN_FRONTEND=noninteractive apt-get install -y postgresql postgresql-contrib", 300000);
    if (pgRes.code !== 0) throw new Error("Failed to install PostgreSQL: " + pgRes.stderr);

    if (jobId) updateInstallJob(jobId, { progress: 55, message: 'Installing pgAdmin4 web interface...' });
    console.log("[pgadmin] Step 5: Installing pgAdmin4 and Apache...");

    await runCmd("DEBIAN_FRONTEND=noninteractive apt-get install -y apache2 libapache2-mod-wsgi-py3", 300000);

    const pgaRes = await runCmd("DEBIAN_FRONTEND=noninteractive apt-get install -y pgadmin4-web", 300000);
    if (pgaRes.code !== 0) throw new Error("Failed to install pgAdmin4: " + pgaRes.stderr);

    if (jobId) updateInstallJob(jobId, { progress: 62, message: 'Configuring Apache for pgAdmin4...' });
    console.log("[pgadmin] Step 5b: Configuring Apache...");

    const apachePortsConf = `# Apache ports configuration - managed by ADPanel
# Only listen on localhost:5050 for pgAdmin4 (accessed via secure token proxy only)
Listen 127.0.0.1:5050
`;
    try {
      fs.writeFileSync('/etc/apache2/ports.conf', apachePortsConf, { mode: 0o644 });
      console.log("[pgadmin] Apache ports.conf configured for port 5050 only");
    } catch (e) {
      console.warn("[pgadmin] Could not write ports.conf:", e.message);
      await runCmd("sed -i '/^Listen/d' /etc/apache2/ports.conf 2>/dev/null || true", 10000);
      await runCmd("echo 'Listen 127.0.0.1:5050' >> /etc/apache2/ports.conf", 10000);
    }

    await runCmd("a2enmod wsgi </dev/null 2>/dev/null || true", 15000);
    await runCmd("a2enmod headers </dev/null 2>/dev/null || true", 15000);
    console.log("[pgadmin] Apache modules configured");

    const pgadminApacheConf = `
ServerTokens Prod
ServerSignature Off
Header always unset "X-Powered-By"
Header always set "Server" "webserver"

<VirtualHost 127.0.0.1:5050>
    ServerAdmin webmaster@localhost
    DocumentRoot /usr/pgadmin4/web

    WSGIDaemonProcess pgadmin processes=1 threads=25 python-home=/usr/pgadmin4/venv
    WSGIScriptAlias / /usr/pgadmin4/web/pgAdmin4.wsgi

    <Directory /usr/pgadmin4/web/>
        WSGIProcessGroup pgadmin
        WSGIApplicationGroup %{GLOBAL}
        Require all granted
    </Directory>

    ErrorLog \${APACHE_LOG_DIR}/pgadmin4_error.log
    CustomLog \${APACHE_LOG_DIR}/pgadmin4_access.log combined
</VirtualHost>
`.trim();

    try {
      fs.mkdirSync('/etc/apache2/sites-available', { recursive: true });
      fs.mkdirSync('/etc/apache2/sites-enabled', { recursive: true });
      fs.writeFileSync('/etc/apache2/sites-available/pgadmin4.conf', pgadminApacheConf, { mode: 0o644 });

      const symlinkTarget = '/etc/apache2/sites-enabled/pgadmin4.conf';
      const symlinkSource = '/etc/apache2/sites-available/pgadmin4.conf';
      try {
        if (fs.existsSync(symlinkTarget)) {
          fs.unlinkSync(symlinkTarget);
        }
        fs.symlinkSync(symlinkSource, symlinkTarget);
        console.log("[pgadmin] Created pgadmin4 site symlink");
      } catch (symErr) {
        await runCmd(`ln -sf ${symlinkSource} ${symlinkTarget} </dev/null`, 5000);
      }
      console.log("[pgadmin] Created Apache pgadmin4 VirtualHost on port 5050");
    } catch (e) {
      console.warn("[pgadmin] Could not create Apache config:", e.message);
    }

    console.log("[pgadmin] Cleaning up conflicting Apache sites...");
    try {
      const sitesToRemove = [
        '/etc/apache2/sites-enabled/000-default.conf',
        '/etc/apache2/sites-enabled/default-ssl.conf',
        '/etc/apache2/conf-enabled/phpmyadmin.conf',
        '/etc/apache2/conf-enabled/pgadmin4.conf'
      ];
      for (const site of sitesToRemove) {
        try {
          if (fs.existsSync(site)) {
            fs.unlinkSync(site);
            console.log(`[pgadmin] Removed ${path.basename(site)}`);
          }
        } catch (rmErr) { }
      }
    } catch (e) { }

    await runCmd("grep -l ':80>' /etc/apache2/sites-enabled/*.conf 2>/dev/null | xargs -r rm -f </dev/null", 5000);
    await runCmd("grep -l 'Listen 80' /etc/apache2/sites-enabled/*.conf 2>/dev/null | xargs -r rm -f </dev/null", 5000);

    if (jobId) updateInstallJob(jobId, { progress: 65, message: 'Configuring PostgreSQL network access...' });
    await runCmd(`find /etc/postgresql -name postgresql.conf -exec sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '${postgresListenHost}'/" {} \\;`, 30000);
    await runCmd(`find /etc/postgresql -name postgresql.conf -exec sed -i "s/listen_addresses = 'localhost'/listen_addresses = '${postgresListenHost}'/" {} \\;`, 30000);
    await runCmd(`find /etc/postgresql -name postgresql.conf -exec sed -i "s/#port = 5432/port = ${listenPort}/" {} \\;`, 30000);
    if (isLocalOnlyDbHost(listenHost)) {
      console.log("[pgadmin] PostgreSQL will remain local-only; skipping remote pg_hba.conf rules");
    } else {
      await runCmd(`find /etc/postgresql -name pg_hba.conf -exec sh -c 'grep -q "host all all 0.0.0.0/0" "\\$1" || echo "host all all 0.0.0.0/0 scram-sha-256" >> "\\$1"' _ {} \\;`, 30000);
      await runCmd(`find /etc/postgresql -name pg_hba.conf -exec sh -c 'grep -q "host all all ::0/0" "\\$1" || echo "host all all ::0/0 scram-sha-256" >> "\\$1"' _ {} \\;`, 30000);
    }

    if (jobId) updateInstallJob(jobId, { progress: 68, message: 'Starting PostgreSQL service...' });
    await runCmd("systemctl enable postgresql 2>/dev/null; systemctl restart postgresql 2>/dev/null || systemctl reload postgresql 2>/dev/null || pg_ctlcluster --skip-systemctl-redirect $(pg_lsclusters -h | head -1 | awk '{print $1, $2}') restart 2>/dev/null || true", 60000);

    if (jobId) updateInstallJob(jobId, { progress: 72, message: 'Checking firewall policy...' });
    await maybeOpenFirewallPort(runCmd, listenPort, listenHost, 'pgadmin');

    if (jobId) updateInstallJob(jobId, { progress: 80, message: 'Configuring pgAdmin4 web setup...' });

    const setupScriptPath = '/usr/pgadmin4/bin/setup-web.sh';
    const altSetupPath = '/usr/share/pgadmin4/bin/setup-web.sh';
    let setupPath = fs.existsSync(setupScriptPath) ? setupScriptPath : (fs.existsSync(altSetupPath) ? altSetupPath : null);

    await runCmd(`
      mkdir -p /var/lib/pgadmin /var/lib/pgadmin/sessions /var/lib/pgadmin/storage /var/log/pgadmin /etc/pgadmin
      chown -R www-data:www-data /var/lib/pgadmin /var/log/pgadmin 2>/dev/null || true
      chmod 750 /var/lib/pgadmin
    `, 15000);

    if (setupPath) {
      const setupCmd = [
        `export PGADMIN_SETUP_EMAIL=${JSON.stringify(email)}`,
        `export PGADMIN_SETUP_PASSWORD=${JSON.stringify(password)}`,
        `${setupPath} --yes`
      ].join('\n');

      const setupResult = await runCmd(setupCmd, 120000);
      console.log("[pgadmin] Setup script result:", setupResult.code, (setupResult.stdout || '').substring(0, 300), (setupResult.stderr || '').substring(0, 200));

      if (setupResult.code !== 0) {
        console.warn("[pgadmin] setup-web.sh did not complete successfully. pgAdmin may require additional setup.");
      }
    } else {
      console.warn("[pgadmin] setup-web.sh not found, skipping initial setup. You may need to configure pgAdmin manually.");
    }

    if (jobId) updateInstallJob(jobId, { progress: 88, message: 'Applying performance optimizations...' });
    const pgadminConfigPath = "/etc/pgadmin/config_local.py";
    const perfConfig = `
# Performance optimizations
SESSION_DB_PATH = '/var/lib/pgadmin/sessions'
STORAGE_DIR = '/var/lib/pgadmin/storage'
LOG_FILE = '/var/log/pgadmin/pgadmin.log'
MAX_SESSION_IDLE_TIME = 3600
ALLOW_SAVE_PASSWORD = True
SERVER_MODE = True
`.trim();
    try {
      fs.mkdirSync(path.dirname(pgadminConfigPath), { recursive: true });
      fs.appendFileSync(pgadminConfigPath, "\n" + perfConfig);
    } catch { }

    if (jobId) updateInstallJob(jobId, { progress: 92, message: 'Configuring nginx proxy...' });
    console.log("[pgadmin] Step 10: Configuring nginx (secure token proxy mode)...");

    try {
      // pgAdmin is accessed ONLY through Node.js token proxy → Apache:5050
      // Write empty placeholder snippet to avoid nginx include errors
      const snippetPath = "/etc/nginx/snippets/pgadmin4.conf";
      fs.mkdirSync("/etc/nginx/snippets", { recursive: true });
      fs.writeFileSync(snippetPath, "# pgAdmin4 - served via secure token proxy only\n", { mode: 0o644 });
      console.log("[pgadmin] Nginx snippet set to placeholder (proxy-only mode)");

      // Remove any old public includes from site configs
      removeDbSnippetIncludes('pgadmin4.conf');
      console.log("[pgadmin] Site config check complete");
    } catch (err) {
      console.warn("[pgadmin] Could not write nginx config:", err.message);
    }

    console.log("[pgadmin] Nginx config section done, proceeding to Step 11...");

    if (jobId) updateInstallJob(jobId, { progress: 95, message: 'Reloading web server...' });
    console.log("[pgadmin] Step 11: Reloading web servers...");

    const { execSync } = require('child_process');

    try {
      const conflictingConfigs = [
        '/etc/apache2/sites-enabled/000-default.conf',
        '/etc/apache2/sites-enabled/default-ssl.conf',
        '/etc/apache2/conf-enabled/phpmyadmin.conf',
        '/etc/apache2/conf-enabled/serve-cgi-bin.conf',
        '/etc/apache2/conf-enabled/pgadmin4.conf'
      ];
      for (const conf of conflictingConfigs) {
        try { if (fs.existsSync(conf)) fs.unlinkSync(conf); } catch (e) { }
      }
      console.log("[pgadmin] Removed conflicting Apache configs");
    } catch (e) { console.log("[pgadmin] Conflicting configs cleanup done"); }

    try {
      execSync("a2enmod wsgi </dev/null 2>/dev/null || true", { stdio: 'pipe', timeout: 10000, input: '' });
      execSync("a2enmod headers </dev/null 2>/dev/null || true", { stdio: 'pipe', timeout: 10000, input: '' });
      console.log("[pgadmin] Enabled wsgi and headers modules");
    } catch (e) { console.log("[pgadmin] wsgi module enable:", e.message); }

    try {
      const symlinkTarget = '/etc/apache2/sites-enabled/pgadmin4.conf';
      const symlinkSource = '/etc/apache2/sites-available/pgadmin4.conf';
      if (!fs.existsSync(symlinkTarget) && fs.existsSync(symlinkSource)) {
        try { fs.unlinkSync(symlinkTarget); } catch (e) { }
        fs.symlinkSync(symlinkSource, symlinkTarget);
      }
      console.log("[pgadmin] Enabled pgadmin4 site");
    } catch (e) {
      try {
        execSync("ln -sf /etc/apache2/sites-available/pgadmin4.conf /etc/apache2/sites-enabled/pgadmin4.conf </dev/null", { stdio: 'pipe', timeout: 5000 });
      } catch (lnErr) { }
      console.log("[pgadmin] pgadmin4 site enable via shell");
    }

    try {
      execSync("apache2ctl configtest 2>&1", { stdio: 'pipe', timeout: 15000 });
      console.log("[pgadmin] Apache config test passed");
    } catch (e) {
      console.warn("[pgadmin] Apache config test failed:", e.message);
      try {
        try { fs.unlinkSync('/etc/apache2/conf-enabled/pgadmin4.conf'); } catch (e) { }
        if (!fs.existsSync('/etc/apache2/sites-enabled/pgadmin4.conf')) {
          fs.symlinkSync('/etc/apache2/sites-available/pgadmin4.conf', '/etc/apache2/sites-enabled/pgadmin4.conf');
        }
      } catch (fixErr) { }
    }

    try {
      execSync("systemctl restart apache2 2>&1 || service apache2 restart 2>&1", { stdio: 'pipe', timeout: 60000 });
      console.log("[pgadmin] Apache restarted");

      try {
        const verifyResult = execSync("curl -sI http://127.0.0.1:5050/ 2>&1 | head -5", { stdio: 'pipe', timeout: 10000 });
        const verifyStr = verifyResult.toString();
        if (verifyStr.includes('302') || verifyStr.includes('200')) {
          console.log("[pgadmin] Apache pgAdmin verification passed - service responding on port 5050");
        } else if (verifyStr.includes('It works') || verifyStr.includes('Apache')) {
          console.warn("[pgadmin] Apache is serving default page instead of pgAdmin - retrying site enable");
          execSync("ln -sf /etc/apache2/sites-available/pgadmin4.conf /etc/apache2/sites-enabled/pgadmin4.conf && systemctl reload apache2", { stdio: 'pipe', timeout: 15000 });
        }
      } catch (verifyErr) {
        console.debug("[pgadmin] Verification check:", verifyErr.message);
      }
    } catch (e) {
      console.error("[pgadmin] Apache restart error:", e.message);
      try {
        const status = execSync("systemctl status apache2 2>&1 | tail -20", { stdio: 'pipe', timeout: 10000 });
        console.error("[pgadmin] Apache status:", status.toString());
      } catch { }
    }

    try {
      const snippetsDir = "/etc/nginx/snippets";
      fs.mkdirSync(snippetsDir, { recursive: true });
      if (!fs.existsSync(path.join(snippetsDir, "phpmyadmin.conf"))) {
        fs.writeFileSync(path.join(snippetsDir, "phpmyadmin.conf"), "# Placeholder - phpMyAdmin not installed\n", { mode: 0o644 });
      }
      if (!fs.existsSync(path.join(snippetsDir, "mongodb.conf"))) {
        fs.writeFileSync(path.join(snippetsDir, "mongodb.conf"), "# Placeholder - MongoDB not installed\n", { mode: 0o644 });
      }
    } catch (e) { console.debug("[pgadmin] Could not create placeholder snippets:", e.message); }

    try {
      execSync("nginx -t && systemctl restart nginx", { stdio: 'pipe', timeout: 15000 });
      execSync("sleep 2", { stdio: 'pipe' });
      console.log("[pgadmin] Nginx restarted");
    } catch (e) { console.error("[pgadmin] Nginx restart error:", e.message); }

    try {
      const finalCheck = execSync("curl -sI http://127.0.0.1:5050/login 2>&1 | head -3", { stdio: 'pipe', timeout: 10000 });
      if (finalCheck.toString().includes('200') || finalCheck.toString().includes('302')) {
        console.log("[pgadmin] Final verification passed - pgAdmin4 is ready");
      }
    } catch { }

    if (jobId) updateInstallJob(jobId, { status: 'completed', progress: 100, message: 'Installation complete!', completedAt: Date.now() });

    console.log("[pgadmin] pgAdmin4 installed successfully");
    return { ok: true };

  } catch (err) {
    console.error("[pgadmin] Installation error:", err);
    if (jobId) updateInstallJob(jobId, { status: 'failed', error: err.message, completedAt: Date.now() });
    return { ok: false, error: err.message };
  }
}

async function createPostgresDbUser(username, password, host = '127.0.0.1', port = '5432') {

  const safeUser = username.replace(/[^a-zA-Z0-9_]/g, '').slice(0, 63);
  if (!safeUser || safeUser.length < 1) {
    console.error("[pgadmin] Invalid username after sanitization");
    return { ok: false, error: "Invalid username" };
  }

  if (/^[0-9]/.test(safeUser)) {
    console.error("[pgadmin] Username cannot start with a number");
    return { ok: false, error: "Username cannot start with a number" };
  }

  const reservedNames = ['postgres', 'pg_', 'admin', 'root', 'public'];
  if (reservedNames.some(r => safeUser.toLowerCase().startsWith(r))) {
    console.error("[pgadmin] Reserved username attempted:", safeUser);
    return { ok: false, error: "Reserved username not allowed" };
  }

  const safePass = password.replace(/'/g, "''").slice(0, 128);

  console.log("[pgadmin] Creating PostgreSQL user:", safeUser);

  const runPsqlCommand = (sql) => new Promise((resolve) => {
    const child = _userSpawn("postgres", "psql", [], {
      shell: false,
      stdio: ['pipe', 'pipe', 'pipe']
    });
    let stdout = "", stderr = "";
    child.stdout.on("data", (d) => { stdout += d.toString(); });
    child.stderr.on("data", (d) => { stderr += d.toString(); });
    child.on("close", (code) => resolve({ code, stdout, stderr }));
    child.on("error", (err) => resolve({ code: 1, stdout, stderr: err.message }));
    const timeout = setTimeout(() => { child.kill(); resolve({ code: -1, stdout, stderr: "timeout" }); }, 10000);
    child.on("close", () => clearTimeout(timeout));
    child.stdin.write(sql);
    child.stdin.end();
  });

  try {
    await runPsqlCommand(`DROP ROLE IF EXISTS ${safeUser};`);

    const createResult = await runPsqlCommand(`CREATE ROLE ${safeUser} LOGIN PASSWORD '${safePass}' SUPERUSER CREATEDB CREATEROLE;`);
    if (createResult.code !== 0 && !createResult.stderr.includes('already exists')) {
      throw new Error(createResult.stderr || "Failed to create role");
    }
    console.log(`[pgadmin] PostgreSQL user '${safeUser}' created`);
    if (isLocalOnlyDbHost(host)) {
      console.log("[pgadmin] PostgreSQL user is scoped to the local-only server configuration");
    }

    await new Promise((resolve) => {
      const child = _privSpawn("systemctl", ["reload", "postgresql"], { shell: false, stdio: 'pipe' });
      child.on("close", resolve);
      child.on("error", resolve);
      setTimeout(() => { child.kill(); resolve(); }, 10000);
    });
    console.log(`[pgadmin] PostgreSQL user '${safeUser}' configured`);

    return { ok: true };
  } catch (err) {
    console.error(`[pgadmin] Failed to create user:`, err.message);
    return { ok: false, error: err.message };
  }
}

const MONGODB_CONFIG_FILE = path.join(__dirname, "mongodb-config.json");

function loadMongoDBConfig() {
  try {
    if (fs.existsSync(MONGODB_CONFIG_FILE)) {
      const config = JSON.parse(fs.readFileSync(MONGODB_CONFIG_FILE, "utf8"));

      let migrated = false;
      if (Array.isArray(config.users)) {
        for (const u of config.users) {
          if (u.password && !u.passwordHash) {
            u.passwordHash = require('bcrypt').hashSync(u.password, BCRYPT_ROUNDS);
            delete u.password;
            migrated = true;
          }
        }
      }
      if (migrated) {
        try {
          fs.writeFileSync(MONGODB_CONFIG_FILE, JSON.stringify(config, null, 2), { mode: 0o600 });
          console.log("[mongodb] Migrated legacy plaintext passwords to bcrypt hashes");
        } catch (e) {
          console.error("[mongodb] Failed to save migrated config:", e.message);
        }
      }

      return config;
    }
  } catch { }
  return { enabled: false, users: [] };
}

function saveMongoDBConfig(data) {
  try {
    fs.writeFileSync(MONGODB_CONFIG_FILE, JSON.stringify(data, null, 2), { mode: 0o600 });
    return true;
  } catch (err) {
    console.error("[mongodb] Failed to save config:", err);
    return false;
  }
}

async function installMongoDB(adminUser, adminPassword, jobId, listenHost = '127.0.0.1', listenPort = '27017') {
  const runCmd = (cmd, timeoutMs = 120000) => new Promise((resolve) => {
    console.log("[mongodb] Running install step...");
    const child = _privBashSpawn(cmd, {
      shell: false,
      stdio: ['pipe', 'pipe', 'pipe'],
      env: { ...process.env, DEBIAN_FRONTEND: "noninteractive" }
    });
    let stdout = "", stderr = "";
    child.stdout.on("data", (d) => { stdout += d.toString(); });
    child.stderr.on("data", (d) => { stderr += d.toString(); });
    const timeout = setTimeout(() => { child.kill('SIGKILL'); resolve({ code: -1, stdout, stderr: "Command timed out" }); }, timeoutMs);
    child.on("close", (code) => { clearTimeout(timeout); resolve({ code, stdout, stderr }); });
    child.on("error", (err) => { clearTimeout(timeout); resolve({ code: 1, stdout, stderr: err.message }); });
  });

  const waitFor = async (checkFn, maxAttempts = 30, delayMs = 2000) => {
    for (let i = 0; i < maxAttempts; i++) {
      if (await checkFn()) return true;
      await new Promise(r => setTimeout(r, delayMs));
    }
    return false;
  };

  const validatedHost = validateDbHost(listenHost);
  if (!validatedHost) {
    if (jobId) updateInstallJob(jobId, { status: 'failed', error: 'Invalid listen host address', completedAt: Date.now() });
    return;
  }
  const validatedPort = validatePort(listenPort);
  if (!validatedPort) {
    if (jobId) updateInstallJob(jobId, { status: 'failed', error: 'Invalid listen port', completedAt: Date.now() });
    return;
  }
  listenHost = validatedHost;
  listenPort = String(validatedPort);
  const mongoBindHost = isLocalOnlyDbHost(listenHost) ? '127.0.0.1' : listenHost;

  try {
    console.log(`[mongodb] Starting MongoDB installation (host: ${listenHost}, port: ${listenPort})...`);

    if (jobId) updateInstallJob(jobId, { status: 'running', progress: 5, message: 'Installing prerequisites...' });
    await runCmd("apt-get install -y gnupg curl 2>&1", 60000);

    if (jobId) updateInstallJob(jobId, { progress: 10, message: 'Adding MongoDB repository...' });

    const lsbRes = await runCmd("lsb_release -cs 2>/dev/null || cat /etc/os-release | grep VERSION_CODENAME | cut -d= -f2", 5000);
    let codename = (lsbRes.stdout || '').trim().split('\n')[0] || 'jammy';

    const supportedCodenames = ['focal', 'jammy', 'noble'];
    if (!supportedCodenames.includes(codename)) {
      console.log(`[mongodb] Codename '${codename}' not directly supported, using 'jammy'`);
      codename = 'jammy';
    }
    console.log(`[mongodb] Using codename: ${codename}`);

    await runCmd("rm -f /etc/apt/sources.list.d/mongodb*.list /usr/share/keyrings/mongodb*.gpg 2>/dev/null || true", 10000);

    let gpgRes = { code: -1, stderr: 'Not attempted' };
    for (let attempt = 1; attempt <= 3; attempt++) {
      console.log(`[mongodb] Importing GPG key (attempt ${attempt}/3)...`);
      gpgRes = await runCmd("curl -fsSL --retry 3 --retry-delay 5 https://www.mongodb.org/static/pgp/server-8.0.asc | gpg --dearmor -o /usr/share/keyrings/mongodb-server-8.0.gpg 2>&1", 120000);
      if (gpgRes.code === 0) break;
      console.warn(`[mongodb] GPG key import attempt ${attempt} failed:`, gpgRes.stderr);
      if (attempt < 3) await new Promise(r => setTimeout(r, 5000));
    }
    if (gpgRes.code !== 0) {
      console.error("[mongodb] Failed to import GPG key after 3 attempts:", gpgRes.stderr);
      throw new Error("Failed to import MongoDB GPG key");
    }

    const repoLine = `deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu ${codename}/mongodb-org/8.0 multiverse`;
    fs.writeFileSync('/etc/apt/sources.list.d/mongodb-org-8.0.list', repoLine + '\n', { mode: 0o644 });
    console.log("[mongodb] Repository configured:", repoLine);

    if (jobId) updateInstallJob(jobId, { progress: 20, message: 'Updating package lists...' });
    const updateRes = await runCmd("apt-get update 2>&1", 120000);
    if (updateRes.code !== 0) {
      console.error("[mongodb] apt-get update failed:", updateRes.stderr);
      throw new Error("Failed to update packages: " + (updateRes.stderr || updateRes.stdout).substring(0, 200));
    }
    console.log("[mongodb] Package lists updated");

    if (jobId) updateInstallJob(jobId, { progress: 35, message: 'Installing MongoDB packages...' });
    console.log("[mongodb] Installing mongodb-org package...");

    const installRes = await runCmd("DEBIAN_FRONTEND=noninteractive apt-get install -y mongodb-org 2>&1", 300000);
    if (installRes.code !== 0) {
      console.error("[mongodb] Installation failed:", installRes.stdout, installRes.stderr);
      throw new Error("MongoDB installation failed: " + (installRes.stderr || installRes.stdout).substring(0, 200));
    }
    console.log("[mongodb] MongoDB packages installed");

    const mongodCheck = await runCmd("which mongod && mongod --version | head -1", 5000);
    if (mongodCheck.code !== 0) {
      throw new Error("MongoDB installation failed - mongod binary not found");
    }
    console.log("[mongodb] MongoDB version:", mongodCheck.stdout.trim());

    const mongoshCheck = await runCmd("which mongosh", 5000);
    const mongoShell = mongoshCheck.code === 0 ? mongoshCheck.stdout.trim() : null;
    if (!mongoShell) {
      console.warn("[mongodb] mongosh not found, will try to install separately");
      await runCmd("DEBIAN_FRONTEND=noninteractive apt-get install -y mongodb-mongosh 2>&1", 60000);
    }
    const finalShellCheck = await runCmd("which mongosh", 5000);
    const finalMongoShell = finalShellCheck.code === 0 ? finalShellCheck.stdout.trim() : null;
    console.log(`[mongodb] Using shell: ${finalMongoShell || 'none'}`);

    if (jobId) updateInstallJob(jobId, { progress: 50, message: 'Configuring MongoDB...' });
    const mongoConfPath = "/etc/mongod.conf";

    await runCmd("mkdir -p /var/lib/mongodb /var/log/mongodb");
    await runCmd("chown -R mongodb:mongodb /var/lib/mongodb /var/log/mongodb");
    await runCmd("chmod 755 /var/lib/mongodb /var/log/mongodb");

    const mongoConfig = `# MongoDB 8.0 configuration file
storage:
  dbPath: /var/lib/mongodb

systemLog:
  destination: file
  logAppend: true
  path: /var/log/mongodb/mongod.log

net:
  port: ${listenPort}
  bindIp: 127.0.0.1

processManagement:
  timeZoneInfo: /usr/share/zoneinfo
`;
    fs.writeFileSync(mongoConfPath, mongoConfig, { mode: 0o644 });
    console.log("[mongodb] Configuration written (auth disabled for user creation)");

    if (jobId) updateInstallJob(jobId, { progress: 60, message: 'Starting MongoDB service...' });

    await runCmd("systemctl stop mongod 2>/dev/null || true");
    await runCmd("rm -f /tmp/mongodb-*.sock /var/lib/mongodb/mongod.lock 2>/dev/null || true");
    await new Promise(r => setTimeout(r, 1000));

    await runCmd("systemctl daemon-reload");
    await runCmd("systemctl enable mongod 2>/dev/null || true");

    const startRes = await runCmd("systemctl start mongod 2>&1", 30000);
    if (startRes.code !== 0) {
      const journalRes = await runCmd("journalctl -u mongod --no-pager -n 20 2>&1", 10000);
      console.error("[mongodb] Failed to start service:", journalRes.stdout);
      throw new Error("Failed to start MongoDB service");
    }

    if (jobId) updateInstallJob(jobId, { progress: 65, message: 'Waiting for MongoDB to be ready...' });
    console.log("[mongodb] Waiting for MongoDB to accept connections...");

    const mongoReady = await waitFor(async () => {
      const statusRes = await runCmd("systemctl is-active mongod 2>/dev/null", 5000);
      if (statusRes.stdout.trim() !== 'active') return false;

      if (finalMongoShell) {
        const pingRes = await runCmd(`${finalMongoShell} --host 127.0.0.1 --port ${listenPort} --quiet --eval 'db.runCommand({ping:1}).ok' 2>&1`, 10000);
        return pingRes.stdout.trim() === '1';
      } else {
        const portRes = await runCmd(`ss -tlnp | grep -q ':${listenPort}' && echo ok`, 5000);
        return portRes.stdout.includes('ok');
      }
    }, 30, 2000);

    if (!mongoReady) {
      const journalRes = await runCmd("journalctl -u mongod --no-pager -n 30 2>&1", 10000);
      console.error("[mongodb] Service did not become ready. Logs:", journalRes.stdout);
      throw new Error("MongoDB service failed to start properly. Check system logs.");
    }
    console.log("[mongodb] MongoDB is running and accepting connections");

    if (jobId) updateInstallJob(jobId, { progress: 75, message: 'Creating admin user...' });

    if (!finalMongoShell) {
      throw new Error("MongoDB shell (mongosh) not available - cannot create admin user");
    }

    const safeUser = JSON.stringify(adminUser);
    const safePass = JSON.stringify(adminPassword);

    const createUserScript = `
      db = db.getSiblingDB('admin');
      try { db.dropAllUsers(); } catch(e) {}
      db.createUser({
        user: ${safeUser},
        pwd: ${safePass},
        roles: [{ role: 'root', db: 'admin' }]
      });
      print('USER_CREATED_OK');
    `;

    let userCreated = false;
    for (let attempt = 1; attempt <= 3; attempt++) {
      console.log(`[mongodb] Creating admin user, attempt ${attempt}/3...`);
      const createRes = await new Promise((resolve) => {
        const child = spawn(finalMongoShell, [
          '--host', '127.0.0.1', '--port', String(listenPort),
          '--quiet', '--eval', createUserScript.replace(/\n/g, ' ')
        ], { shell: false, stdio: ['pipe', 'pipe', 'pipe'] });
        let stdout = '', stderr = '';
        child.stdout.on('data', d => { stdout += d.toString(); });
        child.stderr.on('data', d => { stderr += d.toString(); });
        child.on('close', code => resolve({ stdout, stderr, code }));
        child.on('error', err => resolve({ stdout: '', stderr: err.message, code: 1 }));
      });

      if (createRes.stdout.includes('USER_CREATED_OK') || createRes.stdout.includes('already exists')) {
        console.log("[mongodb] Admin user created successfully");
        userCreated = true;
        break;
      }

      console.warn(`[mongodb] Attempt ${attempt} failed:`, createRes.stdout, createRes.stderr);
      await new Promise(r => setTimeout(r, 2000));
    }

    if (!userCreated) {
      throw new Error("Failed to create MongoDB admin user after 3 attempts");
    }

    if (jobId) updateInstallJob(jobId, { progress: 85, message: 'Enabling authentication...' });

    const secureConfig = `# MongoDB 8.0 configuration file
storage:
  dbPath: /var/lib/mongodb

systemLog:
  destination: file
  logAppend: true
  path: /var/log/mongodb/mongod.log
  quiet: true

net:
  port: ${listenPort}
  bindIp: ${mongoBindHost}

processManagement:
  timeZoneInfo: /usr/share/zoneinfo

security:
  authorization: enabled
`;
    fs.writeFileSync(mongoConfPath, secureConfig, { mode: 0o644 });
    console.log("[mongodb] Configuration updated with authentication enabled");

    await runCmd("systemctl restart mongod 2>&1", 30000);
    await new Promise(r => setTimeout(r, 3000));

    const authReady = await waitFor(async () => {
      const statusRes = await runCmd("systemctl is-active mongod 2>/dev/null", 5000);
      return statusRes.stdout.trim() === 'active';
    }, 15, 2000);

    if (!authReady) {
      console.error("[mongodb] Service failed to restart with authentication");
      throw new Error("MongoDB failed to restart after enabling authentication");
    }

    if (jobId) updateInstallJob(jobId, { progress: 95, message: 'Verifying installation...' });

    let verified = false;
    for (let attempt = 1; attempt <= 5; attempt++) {
      const verifyRes = await new Promise((resolve) => {
        const child = spawn(finalMongoShell, [
          '--host', '127.0.0.1', '--port', String(listenPort),
          '-u', adminUser, '-p', adminPassword,
          '--authenticationDatabase', 'admin',
          '--quiet', '--eval', 'db.runCommand({ping:1}).ok'
        ], { shell: false, stdio: ['pipe', 'pipe', 'pipe'] });
        let stdout = '', stderr = '';
        child.stdout.on('data', d => { stdout += d.toString(); });
        child.stderr.on('data', d => { stderr += d.toString(); });
        child.on('close', code => resolve({ stdout, stderr, code }));
        child.on('error', err => resolve({ stdout: '', stderr: err.message, code: 1 }));
      });

      if (verifyRes.stdout.trim() === '1') {
        console.log("[mongodb] Authentication verified successfully");
        verified = true;
        break;
      }

      console.log(`[mongodb] Verification attempt ${attempt}/5 failed:`, verifyRes.stdout.substring(0, 100));
      await new Promise(r => setTimeout(r, 2000));
    }

    if (!verified) {
      throw new Error("MongoDB authentication verification failed");
    }

    await maybeOpenFirewallPort(runCmd, listenPort, listenHost, 'mongodb');

    if (jobId) updateInstallJob(jobId, { status: 'completed', progress: 100, message: 'Installation complete!', completedAt: Date.now() });
    console.log("[mongodb] MongoDB installation completed successfully");

    return { ok: true };

  } catch (err) {
    console.error("[mongodb] Installation failed:", err.message);
    if (jobId) updateInstallJob(jobId, { status: 'failed', error: err.message, completedAt: Date.now() });
    return { ok: false, error: err.message };
  }
}

async function checkServiceRunning(serviceName) {
  return new Promise((resolve) => {
    const child = spawn("systemctl", ["is-active", serviceName]);
    let output = "";
    child.stdout.on("data", (d) => { output += d.toString(); });
    child.on("close", (code) => {
      resolve(output.trim() === "active");
    });
    child.on("error", () => resolve(false));
  });
}

async function checkDatabaseConnection(type, host, port, username, password) {
  if (type !== 'mysql') {
    console.log(`[database] Skipping connection check for ${type} (only mysql supported for now)`);
    return true;
  }

  console.log(`[database] Testing ${type} connection to ${host}:${port} as ${username}`);
  let connection;
  try {
    connection = await mysql.createConnection({
      host: host,
      port: port,
      user: username,
      password: password,
      connectTimeout: 5000
    });
    console.log(`[database] Connection successful!`);
    return true;
  } catch (err) {
    console.error(`[database] Connection failed: ${err.message}`);
    return false;
  } finally {
    if (connection) await connection.end();
  }
}

let _cachedMySqlAdminCmd = null;

function _detectMySqlServiceName() {
  const { execSync } = require('child_process');
  for (const svc of ['mariadb', 'mysql', 'mysqld']) {
    try {
      const out = execSync(`systemctl list-unit-files ${svc}.service 2>/dev/null | grep -c ${svc}`, { encoding: 'utf8', timeout: 5000 }).trim();
      if (parseInt(out, 10) > 0) return svc;
    } catch { }
  }
  return null;
}

async function ensureMySqlRunning() {
  const { execSync } = require('child_process');
  const svc = _detectMySqlServiceName();
  if (!svc) {
    console.warn('[database] No MySQL/MariaDB service detected on this system');
    return false;
  }

  try {
    const status = execSync(`systemctl is-active ${svc} 2>/dev/null`, { encoding: 'utf8', timeout: 5000 }).trim();
    if (status === 'active') return true;
  } catch { }

  console.log(`[database] MySQL/MariaDB service '${svc}' is not running. Starting it...`);
  try {
    try { execSync('pkill -9 mariadbd 2>/dev/null; pkill -9 mysqld 2>/dev/null', { timeout: 5000 }); } catch { }
    await new Promise(r => setTimeout(r, 1000));
    execSync(`systemctl start ${svc}`, { timeout: 30000 });
  } catch (e) {
    console.error(`[database] Failed to start ${svc}:`, e.message);
    return false;
  }

  for (let i = 0; i < 20; i++) {
    await new Promise(r => setTimeout(r, 500));
    try {
      const status = execSync(`systemctl is-active ${svc} 2>/dev/null`, { encoding: 'utf8', timeout: 3000 }).trim();
      if (status === 'active') {
        console.log(`[database] Service '${svc}' is now running`);
        return true;
      }
    } catch { }
  }
  console.error(`[database] Service '${svc}' did not start in time`);
  return false;
}

function _probeMySqlCmd(command, args, timeoutMs = 5000) {
  return new Promise((resolve) => {
    try {
      const child = spawn(command, args, {
        shell: false,
        stdio: ['ignore', 'pipe', 'pipe']
      });
      let stderr = '';
      child.stderr.on('data', d => { stderr += d.toString(); });
      const timer = setTimeout(() => { child.kill('SIGKILL'); resolve(false); }, timeoutMs);
      child.on('close', (code) => {
        clearTimeout(timer);
        resolve(code === 0 && !stderr.includes('Access denied'));
      });
      child.on('error', () => { clearTimeout(timer); resolve(false); });
    } catch { resolve(false); }
  });
}

async function getMySqlAdminCommand() {
  if (_cachedMySqlAdminCmd) return _cachedMySqlAdminCmd;

  const running = await ensureMySqlRunning();
  if (!running) {
    console.error('[database] MySQL/MariaDB is not running and could not be started. Cannot probe auth.');
    return null;
  }

  console.log('[database] Probing MySQL/MariaDB admin auth methods...');

  const envPassword = (process.env.MYSQL_PASSWORD || '').trim();
  if (envPassword) {
    if (await _probeMySqlCmd('mysql', ['-u', 'root', `--password=${envPassword}`, '--connect-timeout=3', '-e', 'SELECT 1'])) {
      console.log('[database] Auth method: mysql -u root with MYSQL_PASSWORD from env');
      _cachedMySqlAdminCmd = { command: 'mysql', args: ['-u', 'root', `--password=${envPassword}`] };
      return _cachedMySqlAdminCmd;
    }
  }

  if (await _probeMySqlCmd('mysql', ['--connect-timeout=3', '-e', 'SELECT 1'])) {
    console.log('[database] Auth method: mysql (unix_socket)');
    _cachedMySqlAdminCmd = { command: 'mysql', args: [] };
    return _cachedMySqlAdminCmd;
  }

  if (await _probeMySqlCmd('mariadb', ['--connect-timeout=3', '-e', 'SELECT 1'])) {
    console.log('[database] Auth method: mariadb (unix_socket)');
    _cachedMySqlAdminCmd = { command: 'mariadb', args: [] };
    return _cachedMySqlAdminCmd;
  }

  if (!_processIsRoot) {
    if (await _probeMySqlCmd('sudo', ['-n', 'mysql', '--connect-timeout=3', '-e', 'SELECT 1'])) {
      console.log('[database] Auth method: sudo mysql (unix_socket)');
      _cachedMySqlAdminCmd = { command: 'sudo', args: ['-n', 'mysql'] };
      return _cachedMySqlAdminCmd;
    }

    if (await _probeMySqlCmd('sudo', ['-n', 'mariadb', '--connect-timeout=3', '-e', 'SELECT 1'])) {
      console.log('[database] Auth method: sudo mariadb (unix_socket)');
      _cachedMySqlAdminCmd = { command: 'sudo', args: ['-n', 'mariadb'] };
      return _cachedMySqlAdminCmd;
    }
  }

  if (fs.existsSync('/etc/mysql/debian.cnf')) {
    if (await _probeMySqlCmd('mysql', ['--defaults-file=/etc/mysql/debian.cnf', '--connect-timeout=3', '-e', 'SELECT 1'])) {
      console.log('[database] Auth method: mysql --defaults-file=/etc/mysql/debian.cnf');
      _cachedMySqlAdminCmd = { command: 'mysql', args: ['--defaults-file=/etc/mysql/debian.cnf'] };
      return _cachedMySqlAdminCmd;
    }
  }

  if (await _probeMySqlCmd('mysql', ['-u', 'root', '--skip-password', '--connect-timeout=3', '-e', 'SELECT 1'])) {
    console.log('[database] Auth method: mysql -u root --skip-password');
    _cachedMySqlAdminCmd = { command: 'mysql', args: ['-u', 'root', '--skip-password'] };
    return _cachedMySqlAdminCmd;
  }

  console.warn('[database] No standard MySQL admin auth method worked. Will use skip-grant-tables fallback.');
  return null;
}

async function runMySqlAdmin(sqlCommands) {
  const adminCmd = await getMySqlAdminCommand();

  if (adminCmd) {
    return new Promise((resolve) => {
      const child = spawn(adminCmd.command, adminCmd.args, {
        shell: false,
        stdio: ['pipe', 'pipe', 'pipe']
      });
      let stderr = '', stdout = '';
      child.stdout.on('data', d => { stdout += d.toString(); });
      child.stderr.on('data', d => { stderr += d.toString(); });
      child.on('close', (code) => {
        if (code === 0) {
          resolve({ ok: true });
        } else {
          if (stderr.includes('Access denied') || stderr.includes('1045')) {
            console.warn('[database] Cached auth method failed, clearing cache for next attempt');
            _cachedMySqlAdminCmd = null;
          }
          resolve({ ok: false, error: stderr || 'Unknown error' });
        }
      });
      child.on('error', (err) => {
        _cachedMySqlAdminCmd = null;
        resolve({ ok: false, error: err.message });
      });
      child.stdin.write(sqlCommands);
      child.stdin.end();
    });
  }

  console.log('[database] Using skip-grant-tables fallback...');
  return new Promise((resolve) => {
    const heredocDelim = `___ADPANEL_SQL_${crypto.randomBytes(8).toString('hex')}___`;
    const script = `
set -e

SOCK_DIR="/var/run/mysqld"
SOCK_PATH="$SOCK_DIR/mysqld.sock"

# Detect service name (mysql or mariadb)
SVC=""
for s in mariadb mysql mysqld; do
  if systemctl list-unit-files "$s.service" >/dev/null 2>&1; then
    SVC="$s"
    break
  fi
done

if [ -z "$SVC" ]; then
  echo "ERROR: Could not detect MySQL/MariaDB service name" >&2
  exit 1
fi

# Detect the server binary (mariadbd or mysqld)
SERVER_BIN=""
for bin in mariadbd mysqld; do
  if command -v "$bin" >/dev/null 2>&1; then
    SERVER_BIN="$bin"
    break
  fi
  # Check common paths
  for p in /usr/sbin /usr/bin /usr/libexec; do
    if [ -x "$p/$bin" ]; then
      SERVER_BIN="$p/$bin"
      break 2
    fi
  done
done

if [ -z "$SERVER_BIN" ]; then
  echo "ERROR: Could not find mariadbd or mysqld binary" >&2
  exit 1
fi

# Stop the service and any stale processes
systemctl stop "$SVC" 2>/dev/null || true
pkill -9 mariadbd 2>/dev/null || true
pkill -9 mysqld 2>/dev/null || true
sleep 2

# Ensure socket directory exists with correct perms
mkdir -p "$SOCK_DIR"
chown mysql:mysql "$SOCK_DIR"
chmod 755 "$SOCK_DIR"
rm -f "$SOCK_PATH" 2>/dev/null || true

# Start server in skip-grant-tables mode (no TCP, socket only)
"$SERVER_BIN" --skip-grant-tables --skip-networking --user=mysql --socket="$SOCK_PATH" &
SAFE_PID=$!

# Wait for the socket to appear (max 20 seconds)
READY=0
for i in $(seq 1 40); do
  if [ -S "$SOCK_PATH" ]; then
    # Also verify we can actually connect
    if mysql --socket="$SOCK_PATH" -u root -e "SELECT 1" >/dev/null 2>&1; then
      READY=1
      break
    fi
  fi
  sleep 0.5
done

if [ "$READY" -ne 1 ]; then
  echo "ERROR: skip-grant-tables server did not become ready" >&2
  kill $SAFE_PID 2>/dev/null || true
  pkill -9 mariadbd 2>/dev/null || true
  pkill -9 mysqld 2>/dev/null || true
  systemctl start "$SVC" 2>/dev/null || true
  exit 1
fi

# Execute the SQL commands via socket
mysql --socket="$SOCK_PATH" -u root <<'${heredocDelim}'
FLUSH PRIVILEGES;
${sqlCommands}
${heredocDelim}
SQL_EXIT=$?

# Kill the safe-mode instance
kill $SAFE_PID 2>/dev/null || true
pkill -9 mariadbd 2>/dev/null || true
pkill -9 mysqld 2>/dev/null || true
sleep 2

# Restart the service normally
systemctl start "$SVC" 2>/dev/null || true

# Wait for normal restart (max 15 seconds)
for i in $(seq 1 30); do
  if systemctl is-active "$SVC" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

exit $SQL_EXIT
`;

    const child = _privBashSpawn(script, {
      shell: false,
      stdio: ['pipe', 'pipe', 'pipe'],
      env: { ...process.env, DEBIAN_FRONTEND: 'noninteractive' }
    });
    let stderr = '', stdout = '';
    child.stdout.on('data', d => { stdout += d.toString(); });
    child.stderr.on('data', d => { stderr += d.toString(); });
    const timer = setTimeout(() => {
      child.kill('SIGKILL');
      _privBashSpawn('killall -9 mysqld mariadbd 2>/dev/null; sleep 1; systemctl start mariadb 2>/dev/null || systemctl start mysql 2>/dev/null || systemctl start mysqld 2>/dev/null', { stdio: 'ignore' });
      resolve({ ok: false, error: 'skip-grant-tables fallback timed out' });
    }, 45000);
    child.on('close', (code) => {
      clearTimeout(timer);
      if (code === 0) {
        console.log('[database] skip-grant-tables fallback succeeded');
        _cachedMySqlAdminCmd = null;
        resolve({ ok: true });
      } else {
        console.error('[database] skip-grant-tables fallback failed:', stderr);
        resolve({ ok: false, error: stderr || 'skip-grant-tables fallback failed' });
      }
    });
    child.on('error', (err) => {
      clearTimeout(timer);
      resolve({ ok: false, error: err.message });
    });
  });
}

async function createMySqlUser(username, password, host = '%', forceRecreate = false) {
  const safeUser = username.replace(/\\/g, '\\\\').replace(/'/g, "''");
  const safeHost = host.replace(/\\/g, '\\\\').replace(/'/g, "''");
  const safePass = password.replace(/\\/g, '\\\\').replace(/'/g, "''");

  // Grant DML/DDL privileges only — no SUPER, FILE, PROCESS, GRANT OPTION, or other admin privileges.
  // Panel DB isolation is enforced at the phpMyAdmin config level (hide_db + AllowDeny rules)
  // because MariaDB does not support partial revokes on global grants.
  const sqlCommands = `
${forceRecreate ? `DROP USER IF EXISTS '${safeUser}'@'${safeHost}';` : ''}
CREATE USER IF NOT EXISTS '${safeUser}'@'${safeHost}' IDENTIFIED BY '${safePass}';
ALTER USER '${safeUser}'@'${safeHost}' IDENTIFIED BY '${safePass}';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, EVENT, TRIGGER ON *.* TO '${safeUser}'@'${safeHost}';
FLUSH PRIVILEGES;
`;

  const result = await runMySqlAdmin(sqlCommands);
  if (result.ok) {
    console.log(`[database] MySQL user '${username}' created successfully`);
  } else {
    console.error(`[database] Failed to create MySQL user: ${result.error}`);
  }
  return result;
}

async function updateMySqlUserPassword(username, password, host = '%') {
  const tryUpdate = async (h) => {
    const safeUser = username.replace(/\\/g, '\\\\').replace(/'/g, "''");
    const safeHost = h.replace(/\\/g, '\\\\').replace(/'/g, "''");
    const safePass = password.replace(/\\/g, '\\\\').replace(/'/g, "''");
    const sqlCommands = `
ALTER USER '${safeUser}'@'${safeHost}' IDENTIFIED BY '${safePass}';
FLUSH PRIVILEGES;
`;
    const result = await runMySqlAdmin(sqlCommands);
    if (result.ok) {
      console.log(`[database] MySQL password updated for '${username}'@'${h}'`);
    }
    return result;
  };

  let result = await tryUpdate(host);

  if (!result.ok && (result.error.includes("1396") || result.error.includes("Operation ALTER USER failed")) && host === '%') {
    console.log(`[database] Failed to update password for '${username}'@'%', retrying with '${username}'@'localhost'...`);
    const retryResult = await tryUpdate('localhost');
    if (retryResult.ok) return retryResult;
  }

  if (!result.ok) {
    console.error(`[database] Failed to update MySQL password: ${result.error}`);
  }
  return result;
}

async function deleteMySqlUser(username, host = '%') {
  const safeUser = username.replace(/\\/g, '\\\\').replace(/'/g, "''");
  const safeHost = host.replace(/\\/g, '\\\\').replace(/'/g, "''");

  const sqlCommands = `
DROP USER IF EXISTS '${safeUser}'@'${safeHost}';
FLUSH PRIVILEGES;
`;

  const result = await runMySqlAdmin(sqlCommands);
  if (result.ok) {
    console.log(`[database] MySQL user '${username}' deleted`);
  } else {
    console.error(`[database] Failed to delete MySQL user: ${result.error}`);
  }
  return result;
}

async function createPostgresUser(username, password) {
  return new Promise((resolve) => {
    const safeUsername = username.replace(/'/g, "''");
    const safePassword = password.replace(/'/g, "''");
    const dqTag = `$adp_${crypto.randomBytes(4).toString('hex')}$`;

    const sqlCommands = `
DO ${dqTag}
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${safeUsername}') THEN
    CREATE ROLE "${username.replace(/"/g, '""')}" WITH LOGIN PASSWORD '${safePassword}' SUPERUSER CREATEDB CREATEROLE;
  ELSE
    ALTER ROLE "${username.replace(/"/g, '""')}" WITH PASSWORD '${safePassword}';
  END IF;
END
${dqTag};
`;

    const child = _userSpawn("postgres", "psql", [], {
      shell: false,
      stdio: ['pipe', 'pipe', 'pipe']
    });

    let stderr = "";

    child.stderr.on("data", (data) => {
      stderr += data.toString();
    });

    child.on("close", (code) => {
      if (code === 0) {
        console.log(`[database] PostgreSQL user '${username}' created successfully`);
        resolve({ ok: true });
      } else {
        console.error(`[database] Failed to create PostgreSQL user: ${stderr}`);
        resolve({ ok: false, error: stderr || "Unknown error" });
      }
    });

    child.on("error", (err) => {
      console.error("[database] PostgreSQL command error:", err);
      resolve({ ok: false, error: err.message });
    });

    child.stdin.write(sqlCommands);
    child.stdin.end();
  });
}

async function updatePostgresUserPassword(username, password) {
  return new Promise((resolve) => {
    const safePassword = password.replace(/'/g, "''");
    const sqlCommands = `ALTER ROLE "${username.replace(/"/g, '""')}" WITH PASSWORD '${safePassword}';`;

    const child = _userSpawn("postgres", "psql", [], {
      shell: false,
      stdio: ['pipe', 'pipe', 'pipe']
    });

    let stderr = "";

    child.stderr.on("data", (data) => {
      stderr += data.toString();
    });

    child.on("close", (code) => {
      if (code === 0) {
        console.log(`[database] PostgreSQL password updated for '${username}'`);
        resolve({ ok: true });
      } else {
        console.error(`[database] Failed to update PostgreSQL password: ${stderr}`);
        resolve({ ok: false, error: stderr || "Unknown error" });
      }
    });

    child.on("error", (err) => {
      console.error("[database] PostgreSQL command error:", err);
      resolve({ ok: false, error: err.message });
    });

    child.stdin.write(sqlCommands);
    child.stdin.end();
  });
}

async function deletePostgresUser(username) {
  return new Promise((resolve) => {
    const sqlCommands = `DROP ROLE IF EXISTS "${username.replace(/"/g, '""')}";`;

    const child = _userSpawn("postgres", "psql", [], {
      shell: false,
      stdio: ['pipe', 'pipe', 'pipe']
    });

    let stderr = "";

    child.stderr.on("data", (data) => {
      stderr += data.toString();
    });

    child.on("close", (code) => {
      if (code === 0) {
        console.log(`[database] PostgreSQL user '${username}' deleted`);
        resolve({ ok: true });
      } else {
        console.error(`[database] Failed to delete PostgreSQL user: ${stderr}`);
        resolve({ ok: false, error: stderr || "Unknown error" });
      }
    });

    child.on("error", (err) => {
      console.error("[database] PostgreSQL command error:", err);
      resolve({ ok: false, error: err.message });
    });

    child.stdin.write(sqlCommands);
    child.stdin.end();
  });
}

async function createMongoUser(username, password) {
  return new Promise((resolve) => {
    const safeUsername = JSON.stringify(username);
    const safePassword = JSON.stringify(password);

    const mongoCommands = `
use admin
db.createUser({
  user: ${safeUsername},
  pwd: ${safePassword},
  roles: [
    { role: "root", db: "admin" },
    { role: "userAdminAnyDatabase", db: "admin" },
    { role: "dbAdminAnyDatabase", db: "admin" },
    { role: "readWriteAnyDatabase", db: "admin" }
  ]
})
`;

    const child = spawn("mongosh", ["--quiet", "--eval", mongoCommands], {
      shell: false,
      stdio: ['pipe', 'pipe', 'pipe']
    });

    let stderr = "";
    let stdout = "";

    child.stdout.on("data", (data) => {
      stdout += data.toString();
    });

    child.stderr.on("data", (data) => {
      stderr += data.toString();
    });

    child.on("close", (code) => {
      if (code === 0 || stdout.includes("already exists")) {
        console.log(`[database] MongoDB user '${username}' created successfully`);
        resolve({ ok: true });
      } else {
        console.error(`[database] Failed to create MongoDB user: ${stderr || stdout}`);
        resolve({ ok: false, error: stderr || stdout || "Unknown error" });
      }
    });

    child.on("error", (err) => {
      if (err.code === 'ENOENT') {
        createMongoUserLegacy(username, password).then(resolve);
      } else {
        console.error("[database] MongoDB command error:", err);
        resolve({ ok: false, error: err.message });
      }
    });
  });
}

async function createMongoUserLegacy(username, password) {
  return new Promise((resolve) => {
    const safeUsername = JSON.stringify(username);
    const safePassword = JSON.stringify(password);
    const mongoCommands = `db.getSiblingDB('admin').createUser({user:${safeUsername},pwd:${safePassword},roles:[{role:"root",db:"admin"}]})`;

    const child = spawn("mongo", ["admin", "--eval", mongoCommands], {
      shell: false,
      stdio: ['pipe', 'pipe', 'pipe']
    });

    let stderr = "";
    let stdout = "";

    child.stdout.on("data", (data) => {
      stdout += data.toString();
    });

    child.stderr.on("data", (data) => {
      stderr += data.toString();
    });

    child.on("close", (code) => {
      if (code === 0 || stdout.includes("already exists")) {
        console.log(`[database] MongoDB user '${username}' created (legacy)`);
        resolve({ ok: true });
      } else {
        console.error(`[database] Failed to create MongoDB user (legacy): ${stderr || stdout}`);
        resolve({ ok: false, error: stderr || stdout || "Unknown error" });
      }
    });

    child.on("error", (err) => {
      console.error("[database] MongoDB legacy command error:", err);
      resolve({ ok: false, error: "MongoDB not installed or not accessible" });
    });
  });
}

async function updateMongoUserPassword(username, password) {
  return new Promise((resolve) => {
    const safeUsername = JSON.stringify(username);
    const safePassword = JSON.stringify(password);
    const mongoCommands = `db.getSiblingDB('admin').changeUserPassword(${safeUsername}, ${safePassword})`;

    const child = spawn("mongosh", ["--quiet", "--eval", mongoCommands], {
      shell: false,
      stdio: ['pipe', 'pipe', 'pipe']
    });

    let stderr = "";

    child.stderr.on("data", (data) => {
      stderr += data.toString();
    });

    child.on("close", (code) => {
      if (code === 0) {
        console.log(`[database] MongoDB password updated for '${username}'`);
        resolve({ ok: true });
      } else {
        console.error(`[database] Failed to update MongoDB password: ${stderr}`);
        resolve({ ok: false, error: stderr || "Unknown error" });
      }
    });

    child.on("error", (err) => {
      if (err.code === 'ENOENT') {
        updateMongoUserPasswordLegacy(username, password).then(resolve);
      } else {
        console.error("[database] MongoDB command error:", err);
        resolve({ ok: false, error: err.message });
      }
    });
  });
}

async function updateMongoUserPasswordLegacy(username, password) {
  return new Promise((resolve) => {
    const safeUsername = JSON.stringify(username);
    const safePassword = JSON.stringify(password);
    const mongoCommands = `db.getSiblingDB('admin').changeUserPassword(${safeUsername}, ${safePassword})`;

    const child = spawn("mongo", ["admin", "--eval", mongoCommands], {
      shell: false,
      stdio: ['pipe', 'pipe', 'pipe']
    });

    let stderr = "";

    child.stderr.on("data", (data) => {
      stderr += data.toString();
    });

    child.on("close", (code) => {
      if (code === 0) {
        console.log(`[database] MongoDB password updated (legacy) for '${username}'`);
        resolve({ ok: true });
      } else {
        console.error(`[database] Failed to update MongoDB password (legacy): ${stderr}`);
        resolve({ ok: false, error: stderr || "Unknown error" });
      }
    });

    child.on("error", (err) => {
      console.error("[database] MongoDB legacy command error:", err);
      resolve({ ok: false, error: "MongoDB not installed or not accessible" });
    });
  });
}

async function deleteMongoUser(username) {
  return new Promise((resolve) => {
    const safeUsername = JSON.stringify(username);
    const mongoCommands = `db.getSiblingDB('admin').dropUser(${safeUsername})`;

    const child = spawn("mongosh", ["--quiet", "--eval", mongoCommands], {
      shell: false,
      stdio: ['pipe', 'pipe', 'pipe']
    });

    let stderr = "";

    child.stderr.on("data", (data) => {
      stderr += data.toString();
    });

    child.on("close", (code) => {
      if (code === 0) {
        console.log(`[database] MongoDB user '${username}' deleted`);
        resolve({ ok: true });
      } else {
        console.error(`[database] Failed to delete MongoDB user: ${stderr}`);
        resolve({ ok: false, error: stderr || "Unknown error" });
      }
    });

    child.on("error", (err) => {
      if (err.code === 'ENOENT') {
        deleteMongoUserLegacy(username).then(resolve);
      } else {
        console.error("[database] MongoDB command error:", err);
        resolve({ ok: false, error: err.message });
      }
    });
  });
}

async function deleteMongoUserLegacy(username) {
  return new Promise((resolve) => {
    const safeUsername = JSON.stringify(username);
    const mongoCommands = `db.getSiblingDB('admin').dropUser(${safeUsername})`;

    const child = spawn("mongo", ["admin", "--eval", mongoCommands], {
      shell: false,
      stdio: ['pipe', 'pipe', 'pipe']
    });

    let stderr = "";

    child.stderr.on("data", (data) => {
      stderr += data.toString();
    });

    child.on("close", (code) => {
      if (code === 0) {
        console.log(`[database] MongoDB user '${username}' deleted (legacy)`);
        resolve({ ok: true });
      } else {
        console.error(`[database] Failed to delete MongoDB user (legacy): ${stderr}`);
        resolve({ ok: false, error: stderr || "Unknown error" });
      }
    });

    child.on("error", (err) => {
      console.error("[database] MongoDB legacy command error:", err);
      resolve({ ok: false, error: "MongoDB not installed or not accessible" });
    });
  });
}

function hashDbPassword(password) {
  const secret = process.env.SESSION_SECRET;
  return crypto.createHash("sha256").update(String(password) + secret).digest("hex");
}

function runPrivilegedBash(cmd, timeoutMs = 30000) {
  return new Promise((resolve) => {
    const child = _privBashSpawn(cmd, {
      shell: false,
      env: { ...process.env, DEBIAN_FRONTEND: "noninteractive" }
    });
    let stdout = "";
    let stderr = "";
    const timeout = setTimeout(() => {
      child.kill("SIGKILL");
      resolve({ code: -1, stdout, stderr: "Command timed out" });
    }, timeoutMs);
    child.stdout.on("data", (d) => { stdout += d.toString(); });
    child.stderr.on("data", (d) => { stderr += d.toString(); });
    child.on("close", (code) => {
      clearTimeout(timeout);
      resolve({ code, stdout, stderr });
    });
    child.on("error", (err) => {
      clearTimeout(timeout);
      resolve({ code: 1, stdout, stderr: err.message });
    });
  });
}

function isLocalPortOpen(port, host = "127.0.0.1", timeoutMs = 1500) {
  return new Promise((resolve) => {
    const socket = net.createConnection({ host, port: Number(port) });
    let finished = false;

    const done = (ok) => {
      if (finished) return;
      finished = true;
      try { socket.destroy(); } catch { }
      resolve(ok);
    };

    socket.setTimeout(timeoutMs);
    socket.once("connect", () => done(true));
    socket.once("timeout", () => done(false));
    socket.once("error", () => done(false));
  });
}

async function waitForLocalPort(port, host = "127.0.0.1", attempts = 12, delayMs = 500) {
  for (let i = 0; i < attempts; i++) {
    if (await isLocalPortOpen(port, host)) return true;
    await new Promise((r) => setTimeout(r, delayMs));
  }
  return false;
}

async function ensureDbToolBackendReady(service, opts = {}) {
  const autoInstall = !!opts.autoInstall;
  try {
    if (service === "pgadmin") {
      const pgConfig = loadPgAdminConfig();
      if (!pgConfig.enabled) {
        return { ok: false, error: "pgAdmin4 is not enabled. Install it from Settings first." };
      }

      const pgadminApacheConf = `
ServerTokens Prod
ServerSignature Off
Header always unset "X-Powered-By"
Header always set "Server" "webserver"

<VirtualHost 127.0.0.1:5050>
    ServerAdmin webmaster@localhost
    DocumentRoot /usr/pgadmin4/web

    WSGIDaemonProcess pgadmin processes=1 threads=25 python-home=/usr/pgadmin4/venv
    WSGIScriptAlias /pgadmin4 /usr/pgadmin4/web/pgAdmin4.wsgi

    <Directory /usr/pgadmin4/web/>
        WSGIProcessGroup pgadmin
        WSGIApplicationGroup %{GLOBAL}
        Require all granted
    </Directory>

    ErrorLog \${APACHE_LOG_DIR}/pgadmin4_error.log
    CustomLog \${APACHE_LOG_DIR}/pgadmin4_access.log combined
</VirtualHost>
`.trim();
      const pgadminApacheConfB64 = Buffer.from(pgadminApacheConf, 'utf8').toString('base64');

      const prepRes = await runPrivilegedBash(`
        set -e
        systemctl daemon-reload 2>/dev/null || true
        systemctl start postgresql 2>/dev/null || true

        mkdir -p /etc/apache2/sites-available /etc/apache2/sites-enabled
        printf %s ${JSON.stringify(pgadminApacheConfB64)} | base64 -d > /etc/apache2/sites-available/pgadmin4.conf
        chmod 0644 /etc/apache2/sites-available/pgadmin4.conf
        ln -sf /etc/apache2/sites-available/pgadmin4.conf /etc/apache2/sites-enabled/pgadmin4.conf

        rm -f /etc/apache2/sites-enabled/000-default.conf 2>/dev/null || true
        rm -f /etc/apache2/sites-enabled/default-ssl.conf 2>/dev/null || true
        rm -f /etc/apache2/conf-enabled/phpmyadmin.conf 2>/dev/null || true
        rm -f /etc/apache2/conf-enabled/pgadmin4.conf 2>/dev/null || true

        cat > /etc/apache2/ports.conf <<'EOF'
# Apache ports configuration - managed by ADPanel
Listen 127.0.0.1:5050
EOF

        a2enmod wsgi >/dev/null 2>&1 || true
        a2enmod headers >/dev/null 2>&1 || true
        apache2ctl -t >/dev/null 2>&1 || true
        systemctl restart apache2 2>/dev/null || systemctl start apache2 2>/dev/null || true
      `, 45000);

      if (prepRes.code !== 0) {
        return { ok: false, error: "Failed to prepare pgAdmin backend" };
      }

      const pgReady = await waitForLocalPort(5050, "127.0.0.1", 20, 500);
      if (!pgReady) {
        const logs = await runPrivilegedBash("journalctl -u apache2 --no-pager -n 20 2>/dev/null || true", 8000);
        return {
          ok: false,
          error: "pgAdmin backend is unreachable on 127.0.0.1:5050" + (logs.stdout ? ` | apache2: ${logs.stdout.split("\n").slice(-3).join(" | ")}` : "")
        };
      }

      const pgProbe = await runPrivilegedBash("curl -fsS http://127.0.0.1:5050/pgadmin4/ 2>/dev/null | head -c 6000 || true", 10000);
      let pgProbeText = (pgProbe.stdout || '').toLowerCase();
      if (!pgProbeText.includes('pgadmin')) {
        const recoveryEmail = String(pgConfig?.users?.[0]?.email || process.env.PGADMIN_SETUP_EMAIL || 'admin@localhost').trim();
        const recoveryPassword = String(
          process.env.PGADMIN_SETUP_PASSWORD ||
          process.env.MYSQL_PASSWORD ||
          (`AdPanel-${crypto.createHash('sha256').update(String(process.env.SESSION_SECRET || recoveryEmail)).digest('hex').slice(0, 20)}!`)
        );

        const setupRecoverRes = await runPrivilegedBash(`
          SETUP_SCRIPT=""
          if [ -x /usr/pgadmin4/bin/setup-web.sh ]; then
            SETUP_SCRIPT="/usr/pgadmin4/bin/setup-web.sh"
          elif [ -x /usr/share/pgadmin4/bin/setup-web.sh ]; then
            SETUP_SCRIPT="/usr/share/pgadmin4/bin/setup-web.sh"
          fi

          if [ -n "$SETUP_SCRIPT" ]; then
            export PGADMIN_SETUP_EMAIL=${JSON.stringify(recoveryEmail)}
            export PGADMIN_SETUP_PASSWORD=${JSON.stringify(recoveryPassword)}
            "$SETUP_SCRIPT" --yes >/dev/null 2>&1 || true
            rm -f /etc/apache2/conf-enabled/pgadmin4.conf 2>/dev/null || true
            systemctl restart apache2 2>/dev/null || systemctl start apache2 2>/dev/null || true
          fi
        `, 120000);

        if (setupRecoverRes.code === 0) {
          const probeAfterRecover = await runPrivilegedBash("curl -fsS http://127.0.0.1:5050/pgadmin4/ 2>/dev/null | head -c 6000 || true", 10000);
          pgProbeText = (probeAfterRecover.stdout || '').toLowerCase();
        }
      }

      if (!pgProbeText.includes('pgadmin')) {
        return {
          ok: false,
          error: "pgAdmin backend responded, but pgAdmin content was not detected on /pgadmin4/."
        };
      }

      return { ok: true };
    }

    if (service === "phpmyadmin") {
      const dbConfig = loadDatabaseConfig();
      if (!dbConfig.enabled || !dbConfig.config) {
        return { ok: false, error: "phpMyAdmin is not enabled. Configure it from Settings first." };
      }

      if (!fs.existsSync("/usr/share/phpmyadmin/index.php")) {
        if (!autoInstall) {
          return { ok: false, error: "phpMyAdmin is not installed." };
        }
        const installed = await installPhpMyAdmin();
        if (!installed) {
          return { ok: false, error: "phpMyAdmin is not installed and auto-install failed." };
        }
      }

      if (!configurePhpMyAdminWebServer()) {
        return { ok: false, error: "Failed to configure phpMyAdmin internal web server." };
      }

      const prepRes = await runPrivilegedBash(`
        PHP_FPM_SVC=$(systemctl list-unit-files --type=service --no-legend | awk '{print $1}' | grep -E '^php[0-9.]+-fpm\\.service$' | sort -rV | head -1)
        if [ -n "$PHP_FPM_SVC" ]; then
          systemctl enable "$PHP_FPM_SVC" 2>/dev/null || true
          systemctl restart "$PHP_FPM_SVC" 2>/dev/null || true
        fi

        systemctl start nginx 2>/dev/null || true
        nginx -t >/dev/null 2>&1 && systemctl reload nginx 2>/dev/null || systemctl restart nginx 2>/dev/null || true
      `, 45000);

      if (prepRes.code !== 0) {
        return { ok: false, error: "Failed to prepare phpMyAdmin backend" };
      }

      const phpReady = await waitForLocalPort(dbProxy.PHPMYADMIN_INTERNAL_PORT, "127.0.0.1", 20, 500);
      if (!phpReady) {
        return { ok: false, error: `phpMyAdmin backend is unreachable on 127.0.0.1:${dbProxy.PHPMYADMIN_INTERNAL_PORT}` };
      }

      const phpProbe = await runPrivilegedBash(`curl -fsS http://127.0.0.1:${dbProxy.PHPMYADMIN_INTERNAL_PORT}/phpmyadmin/ 2>/dev/null | head -c 6000 || true`, 10000);
      const phpProbeText = (phpProbe.stdout || '').toLowerCase();
      if (!phpProbeText.includes('phpmyadmin')) {
        return {
          ok: false,
          error: "phpMyAdmin backend responded, but phpMyAdmin content was not detected on /phpmyadmin/."
        };
      }

      return { ok: true };
    }

    return { ok: false, error: "Unsupported database service" };
  } catch (err) {
    return { ok: false, error: err?.message || "Database backend readiness check failed" };
  }
}

// ── Database Access Token API ────────────────────────────────────
app.post("/api/settings/database/access-token", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(404).end();
  const { service } = req.body || {};
  const validServices = ["phpmyadmin", "pgadmin"];
  if (!validServices.includes(service)) {
    return res.status(400).json({ error: "Invalid service" });
  }

  const readiness = await ensureDbToolBackendReady(service, { autoInstall: true });
  if (!readiness.ok) {
    return res.status(503).json({ error: readiness.error || "Database tool backend is not ready" });
  }

  const userIp = getRequestIp(req) || req.ip || req.connection?.remoteAddress || "";
  const username = req.session?.user || "unknown";
  const result = dbProxy.generateToken(service, userIp, username, req.sessionID || null);
  if (!result) return res.status(500).json({ error: "Failed to generate token" });
  return res.json({
    url: `/db-access/${result.token}/`,
    token: result.token,
    expiresAt: result.expiresAt,
    ttl: dbProxy.TOKEN_TTL_MS
  });
});

// Open DB tools in a real browser tab (non-popup flow) via normal navigation.
app.get("/db-open/:service", async (req, res) => {
  if (!(await isAdmin(req))) {
    return res.status(403).send("Access denied");
  }

  const service = String(req.params.service || "").toLowerCase();
  const validServices = ["phpmyadmin", "pgadmin"];
  if (!validServices.includes(service)) {
    return res.status(404).send("Not found");
  }
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, private");
  return res.render("db-open", {
    service,
    serviceLabel: service === "pgadmin" ? "pgAdmin4" : "phpMyAdmin"
  });
});

app.get("/api/settings/database", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });

  const data = loadDatabaseConfig();
  const safeUsers = (data.users || []).map(u => ({
    username: u.username,
    createdAt: u.createdAt
  }));

  const actionTokens = {
    setup: issueActionToken(req, "POST /api/settings/database/setup", {}, { ttlSeconds: 120, oneTime: true }),
    disable: issueActionToken(req, "POST /api/settings/database/disable", {}, { ttlSeconds: 120, oneTime: true }),
    createUser: issueActionToken(req, "POST /api/settings/database/users", {}, { ttlSeconds: 120, oneTime: true }),
    changeType: issueActionToken(req, "POST /api/settings/database/change-type", {}, { ttlSeconds: 120, oneTime: true }),
    pgadminSetup: issueActionToken(req, "POST /api/settings/database/pgadmin/setup", {}, { ttlSeconds: 120, oneTime: true }),
    pgadminDisable: issueActionToken(req, "POST /api/settings/database/pgadmin/disable", {}, { ttlSeconds: 120, oneTime: true }),
    mongodbSetup: issueActionToken(req, "POST /api/settings/database/mongodb/setup", {}, { ttlSeconds: 120, oneTime: true }),
    mongodbDisable: issueActionToken(req, "POST /api/settings/database/mongodb/disable", {}, { ttlSeconds: 120, oneTime: true }),
  };
  for (const u of safeUsers) {
    actionTokens[`deleteUser_${u.username}`] = issueActionToken(req, "DELETE /api/settings/database/users/:username", { username: u.username }, { ttlSeconds: 120, oneTime: true });
    actionTokens[`changePassword_${u.username}`] = issueActionToken(req, "POST /api/settings/database/users/:username/password", { username: u.username }, { ttlSeconds: 120, oneTime: true });
  }

  return res.json({
    enabled: data.enabled,
    config: data.config ? {
      type: data.config.type,
      host: data.config.host,
      port: data.config.port
    } : null,
    users: safeUsers,
    actionTokens
  });
});

app.post("/api/settings/database/setup", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  if (!requireActionTokenOr403(req, res, "POST /api/settings/database/setup")) return;

  const { type, host, port, username, password } = req.body || {};

  const validTypes = ["mysql", "postgresql", "mongodb"];
  const cleanType = String(type || "mysql").toLowerCase();
  if (!validTypes.includes(cleanType)) {
    return res.status(400).json({ error: "Invalid database type" });
  }

  const rawHost = String(host || "localhost").trim();
  const cleanHost = validateDbHost(rawHost);
  if (!cleanHost) {
    return res.status(400).json({ error: "Invalid host. Must be localhost, a valid IPv4/IPv6 address, or a valid hostname (a-z, 0-9, dots, hyphens only)" });
  }

  const cleanPort = validatePort(port);
  if (!cleanPort) {
    return res.status(400).json({ error: "Invalid port. Must be a number between 1 and 65535" });
  }

  const cleanUsername = String(username || "").trim();
  if (!cleanUsername || cleanUsername.length > 64 || !/^[a-zA-Z0-9_-]+$/.test(cleanUsername)) {
    return res.status(400).json({ error: "Invalid username (alphanumeric, _, - only)" });
  }

  if (!password || password.length < 8) {
    return res.status(400).json({ error: "Password must be at least 8 characters" });
  }

  try {
    const isInstalled = await checkPackageInstalled("phpmyadmin");
    if (!isInstalled) {
      console.log("[database] phpMyAdmin not installed, attempting installation...");
      const installed = await installPhpMyAdmin();
      if (!installed) {
        return res.status(500).json({ error: "Failed to install phpMyAdmin. Please install it manually." });
      }
    }

    const isLocal = ['localhost', '127.0.0.1', '%', '0.0.0.0'].includes(cleanHost);
    const connectionHost = (cleanHost === '%' || cleanHost === 'localhost' || cleanHost === '0.0.0.0') ? '127.0.0.1' : cleanHost;

    if (cleanType === 'mysql' && isLocal) {
      const mysqlRunning = await ensureMySqlRunning();
      if (!mysqlRunning) {
        return res.status(500).json({ error: "MySQL/MariaDB service is not running and could not be started. Please start it manually and try again." });
      }

      console.log(`[database] Ensuring MySQL user '${cleanUsername}' exists (Local Setup)...`);

      const resPerc = await createMySqlUser(cleanUsername, password, '%', true);
      if (!resPerc.ok) {
        console.error(`[database] Failed to create user@%: ${resPerc.error}`);
        return res.status(500).json({ error: "Failed to create MySQL user: " + resPerc.error });
      }

      console.log(`[database] Ensuring '${cleanUsername}'@'localhost' also exists...`);
      const resLocal = await createMySqlUser(cleanUsername, password, 'localhost', true);
      if (!resLocal.ok) {
        console.warn(`[database] Warning: Failed to create user@localhost: ${resLocal.error}`);
      }
    } else if (cleanType === 'mysql') {
      console.log(`[database] Skipping local user creation for remote host: ${cleanHost}`);
    }

    let connectionOk = await checkDatabaseConnection(cleanType, connectionHost, cleanPort, cleanUsername, password);

    if (!connectionOk) {
      return res.status(400).json({ error: `Connection failed: Access denied for user '${cleanUsername}'@'${connectionHost}' (or user does not exist on target server).` });
    }

    const data = {
      enabled: true,
      config: {
        type: cleanType,
        host: connectionHost,
        port: cleanPort
      },
      users: [{
        username: cleanUsername,
        passwordHash: hashDbPassword(password),
        createdAt: Date.now()
      }],
      setupAt: Date.now()
    };

    if (!saveDatabaseConfig(data)) {
      return res.status(500).json({ error: "Failed to save configuration" });
    }

    configurePhpMyAdmin(data.config);

    setTimeout(() => {
      configurePhpMyAdminWebServer();
    }, 500);

    console.log(`[phpmyadmin] phpMyAdmin setup complete for ${cleanType} at ${cleanHost}:${cleanPort}`);

    return res.json({ ok: true, message: "phpMyAdmin setup complete" });

  } catch (err) {
    console.error("[database] Setup error:", err);
    return res.status(500).json({ error: "Setup failed: " + (err.message || "unknown error") });
  }
});

app.post("/api/settings/database/disable", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  if (!requireActionTokenOr403(req, res, "POST /api/settings/database/disable")) return;

  try {
    console.log("[phpmyadmin] Disabling phpMyAdmin...");

    disablePhpMyAdminWebServer();

    const data = { enabled: false, config: null, users: [], disabledAt: Date.now() };

    if (!saveDatabaseConfig(data)) {
      return res.status(500).json({ error: "Failed to save configuration" });
    }

    console.log("[phpmyadmin] phpMyAdmin disabled - web access blocked");

    return res.json({ ok: true, message: "phpMyAdmin disabled" });

  } catch (err) {
    console.error("[phpmyadmin] Disable error:", err);
    return res.status(500).json({ error: "Failed to disable" });
  }
});

app.post("/api/settings/database/users", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  if (!requireActionTokenOr403(req, res, "POST /api/settings/database/users")) return;

  const { username, password, host } = req.body || {};

  const cleanUsername = String(username || "").trim();
  if (!cleanUsername || cleanUsername.length > 64 || !/^[a-zA-Z0-9_-]+$/.test(cleanUsername)) {
    return res.status(400).json({ error: "Invalid username (alphanumeric, _, - only)" });
  }

  if (!password || password.length < 8) {
    return res.status(400).json({ error: "Password must be at least 8 characters" });
  }

  const cleanHost = String(host || "%").trim();
  if (cleanHost.length > 255 || /[;'"\\]/.test(cleanHost)) {
    return res.status(400).json({ error: "Invalid host" });
  }

  try {
    const data = loadDatabaseConfig();

    if (!data.enabled) {
      return res.status(400).json({ error: "Database management is not enabled" });
    }

    const exists = data.users.find(u => u.username.toLowerCase() === cleanUsername.toLowerCase() && u.host === cleanHost);
    if (exists) {
      return res.status(400).json({ error: "User already exists" });
    }

    const dbType = data.config?.type || 'mysql';
    let result;
    let dbName = 'MySQL';

    if (dbType === 'mysql') {
      result = await createMySqlUser(cleanUsername, password, cleanHost);
      dbName = 'MySQL';
    } else if (dbType === 'postgresql') {
      result = await createPostgresUser(cleanUsername, password);
      dbName = 'PostgreSQL';
    } else if (dbType === 'mongodb') {
      result = await createMongoUser(cleanUsername, password);
      dbName = 'MongoDB';
    } else {
      return res.status(400).json({ error: `Unsupported database type: ${dbType}` });
    }

    if (!result.ok) {
      return res.status(500).json({ error: `Failed to create ${dbName} user: ${result.error || 'Unknown error'}` });
    }

    data.users.push({
      username: cleanUsername,
      host: cleanHost,
      passwordHash: hashDbPassword(password),
      createdAt: Date.now()
    });

    if (!saveDatabaseConfig(data)) {
      return res.status(500).json({ error: "Failed to save user" });
    }

    console.log(`[database] Created ${dbType} user: ${cleanUsername}@${cleanHost}`);

    return res.json({ ok: true, message: `User created successfully in ${dbName}` });

  } catch (err) {
    console.error("[database] Create user error:", err);
    return res.status(500).json({ error: "Failed to create user" });
  }
});

app.post("/api/settings/database/users/:username/password", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  if (!requireActionTokenOr403(req, res, "POST /api/settings/database/users/:username/password", { username: String(req.params.username || "").trim() })) return;

  const { username } = req.params;
  const { password, host } = req.body || {};

  const cleanUsername = String(username || "").trim();

  if (!password || password.length < 8) {
    return res.status(400).json({ error: "Password must be at least 8 characters" });
  }

  const cleanHost = String(host || "%").trim();
  if (cleanHost.length > 255 || /[;'"\\]/.test(cleanHost)) {
    return res.status(400).json({ error: "Invalid host" });
  }

  try {
    const data = loadDatabaseConfig();

    if (!data.enabled) {
      return res.status(400).json({ error: "Database management is not enabled" });
    }

    const userIndex = data.users.findIndex(u => u.username.toLowerCase() === cleanUsername.toLowerCase());
    if (userIndex === -1) {
      return res.status(404).json({ error: "User not found" });
    }

    const userHost = data.users[userIndex].host || cleanHost;

    const dbType = data.config?.type || 'mysql';
    let result;
    let dbName = 'MySQL';

    if (dbType === 'mysql') {
      result = await updateMySqlUserPassword(cleanUsername, password, userHost);
      dbName = 'MySQL';
    } else if (dbType === 'postgresql') {
      result = await updatePostgresUserPassword(cleanUsername, password);
      dbName = 'PostgreSQL';
    } else if (dbType === 'mongodb') {
      result = await updateMongoUserPassword(cleanUsername, password);
      dbName = 'MongoDB';
    } else {
      return res.status(400).json({ error: `Unsupported database type: ${dbType}` });
    }

    if (!result.ok) {
      return res.status(500).json({ error: `Failed to update ${dbName} password: ${result.error || 'Unknown error'}` });
    }

    data.users[userIndex].passwordHash = hashDbPassword(password);
    data.users[userIndex].updatedAt = Date.now();

    if (!saveDatabaseConfig(data)) {
      return res.status(500).json({ error: "Failed to update password" });
    }

    console.log(`[database] Password updated for ${dbType} user: ${cleanUsername}`);

    return res.json({ ok: true, message: `Password updated successfully in ${dbName}` });

  } catch (err) {
    console.error("[database] Update password error:", err);
    return res.status(500).json({ error: "Failed to update password" });
  }
});

app.delete("/api/settings/database/users/:username", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  if (!requireActionTokenOr403(req, res, "DELETE /api/settings/database/users/:username", { username: String(req.params.username || "").trim() })) return;

  const { username } = req.params;
  const { host } = req.query || {};
  const cleanUsername = String(username || "").trim();

  const cleanHost = String(host || "%").trim();
  if (cleanHost.length > 255 || /[;'"\\]/.test(cleanHost)) {
    return res.status(400).json({ error: "Invalid host" });
  }

  try {
    const data = loadDatabaseConfig();

    if (!data.enabled) {
      return res.status(400).json({ error: "Database management is not enabled" });
    }

    const userIndex = data.users.findIndex(u => u.username.toLowerCase() === cleanUsername.toLowerCase());
    if (userIndex === -1) {
      return res.status(404).json({ error: "User not found" });
    }

    const userHost = data.users[userIndex].host || cleanHost;

    const dbType = data.config?.type || 'mysql';
    let result;
    let dbName = 'MySQL';

    if (dbType === 'mysql') {
      result = await deleteMySqlUser(cleanUsername, userHost);
      dbName = 'MySQL';
    } else if (dbType === 'postgresql') {
      result = await deletePostgresUser(cleanUsername);
      dbName = 'PostgreSQL';
    } else if (dbType === 'mongodb') {
      result = await deleteMongoUser(cleanUsername);
      dbName = 'MongoDB';
    } else {
      result = { ok: true };
      dbName = dbType;
    }

    if (!result.ok) {
      console.warn(`[database] Could not delete ${dbType} user (may not exist): ${result.error}`);
    }

    data.users.splice(userIndex, 1);

    if (!saveDatabaseConfig(data)) {
      return res.status(500).json({ error: "Failed to delete user" });
    }

    console.log(`[database] Deleted ${dbType} user: ${cleanUsername}`);

    return res.json({ ok: true, message: `User deleted from ${dbName}` });

  } catch (err) {
    console.error("[database] Delete user error:", err);
    return res.status(500).json({ error: "Failed to delete user" });
  }
});

app.post("/api/settings/database/change-type", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  if (!requireActionTokenOr403(req, res, "POST /api/settings/database/change-type")) return;

  const { type, host, port, username, password } = req.body || {};

  const validTypes = ["mysql", "postgresql", "mongodb"];
  const cleanType = String(type || "").toLowerCase();
  if (!validTypes.includes(cleanType)) {
    return res.status(400).json({ error: "Invalid database type" });
  }

  const cleanHost = validateDbHost(String(host || "localhost").trim());
  if (!cleanHost) {
    return res.status(400).json({ error: "Invalid host. Must be localhost, a valid IPv4/IPv6 address, or a valid hostname." });
  }

  const cleanPort = parseInt(port, 10);
  if (isNaN(cleanPort) || cleanPort < 1 || cleanPort > 65535) {
    return res.status(400).json({ error: "Invalid port" });
  }

  const cleanUsername = String(username || "").trim();
  if (!cleanUsername || cleanUsername.length > 64 || !/^[a-zA-Z0-9_-]+$/.test(cleanUsername)) {
    return res.status(400).json({ error: "Invalid username (alphanumeric, _, - only)" });
  }

  if (!password || password.length < 8) {
    return res.status(400).json({ error: "Password must be at least 8 characters" });
  }

  try {
    const connectionOk = await checkDatabaseConnection(cleanType, cleanHost, cleanPort, cleanUsername, password);
    if (!connectionOk) {
      return res.status(400).json({ error: "Failed to connect to database. Check credentials." });
    }

    const data = {
      enabled: true,
      config: {
        type: cleanType,
        host: cleanHost,
        port: cleanPort
      },
      users: [{
        username: cleanUsername,
        passwordHash: hashDbPassword(password),
        createdAt: Date.now()
      }],
      changedAt: Date.now()
    };

    if (!saveDatabaseConfig(data)) {
      return res.status(500).json({ error: "Failed to save configuration" });
    }

    console.log(`[database] Database type changed to ${cleanType} at ${cleanHost}:${cleanPort}`);

    return res.json({ ok: true, message: "Database type changed successfully" });

  } catch (err) {
    console.error("[database] Change type error:", err);
    return res.status(500).json({ error: "Failed to change database type: " + (err.message || "unknown error") });
  }
});


app.use(['/pgadmin4', '/pgadmin'], async (req, res, next) => {
  if (!(await isAdmin(req))) {
    return res.status(403).send('Access denied - Admin authentication required');
  }

  const proxyReq = httpMod.request({
    hostname: '127.0.0.1',
    port: 5050,
    path: req.originalUrl.replace(/^\/(pgadmin4?)\/?/, '/') || '/',
    method: req.method,
    headers: {
      ...req.headers,
      host: '127.0.0.1:5050',
      'X-Script-Name': req.originalUrl.startsWith('/pgadmin4') ? '/pgadmin4' : '/pgadmin',
      'X-Forwarded-For': req.ip,
      'X-Forwarded-Proto': req.protocol,
      'X-Real-IP': req.ip
    }
  }, (proxyRes) => {
    if (proxyRes.headers.location) {
      let location = proxyRes.headers.location;
      if (location.startsWith('/') && !location.startsWith('/pgadmin')) {
        const prefix = req.originalUrl.startsWith('/pgadmin4') ? '/pgadmin4' : '/pgadmin';
        proxyRes.headers.location = prefix + location;
      }
    }
    res.writeHead(proxyRes.statusCode, proxyRes.headers);
    proxyRes.pipe(res);
  });

  proxyReq.on('error', (err) => {
    console.error('[pgadmin-proxy] Error:', err.message);
    res.status(502).send(`
      <html>
        <head><title>pgAdmin4 Unavailable</title></head>
        <body style="font-family: sans-serif; padding: 40px; text-align: center;">
          <h1>pgAdmin4 is not running</h1>
          <p>The pgAdmin4 service (Apache on port 5050) is not available.</p>
          <p>Please install PostgreSQL &amp; pgAdmin4 from the panel settings first.</p>
          <p style="color: #666; margin-top: 20px;">Error: ${escapeHtml(err.message)}</p>
          <a href="/settings" style="color: #007bff;">Go to Settings</a>
        </body>
      </html>
    `);
  });

  if (['POST', 'PUT', 'PATCH'].includes(req.method)) {
    req.pipe(proxyReq);
  } else {
    proxyReq.end();
  }
});

app.use('/phpmyadmin', async (req, res, next) => {
  if (!(await isAdmin(req))) {
    return res.status(403).send('Access denied - Admin authentication required');
  }

  if (!fs.existsSync('/usr/share/phpmyadmin/index.php')) {
    return res.status(503).send(`
      <html>
        <head><title>phpMyAdmin Not Installed</title></head>
        <body style="font-family: sans-serif; padding: 40px; text-align: center;">
          <h1>phpMyAdmin is not installed</h1>
          <p>Please enable and configure a database from the panel settings first.</p>
          <a href="/settings" style="color: #007bff;">Go to Settings</a>
        </body>
      </html>
    `);
  }

  return res.status(503).send(`
    <html>
      <head><title>phpMyAdmin Configuration Required</title></head>
      <body style="font-family: sans-serif; padding: 40px; text-align: center;">
        <h1>phpMyAdmin nginx configuration needed</h1>
        <p>phpMyAdmin is installed but nginx is not configured to serve it.</p>
        <p>Please re-enable the database from settings to configure nginx, or add this to your nginx server block:</p>
        <pre style="background: #f5f5f5; padding: 15px; text-align: left; display: inline-block;">include /etc/nginx/snippets/phpmyadmin.conf;</pre>
        <p style="margin-top: 20px;"><a href="/settings" style="color: #007bff;">Go to Settings</a></p>
      </body>
    </html>
  `);
});

app.get("/api/settings/database/pgadmin/status", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });

  const config = loadPgAdminConfig();
  const postgresRunning = await checkServiceRunning("postgresql");

  return res.json({
    enabled: config.enabled,
    installed: postgresRunning,
    postgresRunning,
    users: (config.users || []).map(u => ({ email: u.email, createdAt: u.createdAt }))
  });
});

app.post("/api/settings/database/pgadmin/setup", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  if (!requireActionTokenOr403(req, res, "POST /api/settings/database/pgadmin/setup")) return;

  const { email, password, dbUser, dbHost, dbPort } = req.body || {};

  const emailRegex = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
  if (!email || !emailRegex.test(email) || email.length > 255) {
    return res.status(400).json({ error: "Valid email required for pgAdmin login" });
  }

  if (!password || password.length < 8) {
    return res.status(400).json({ error: "Password must be at least 8 characters" });
  }

  const postgresUser = dbUser || email.split("@")[0].replace(/[^a-zA-Z0-9_]/g, "") || "admin";

  const rawHost = dbHost || "127.0.0.1";
  const rawPort = dbPort || "5432";

  const postgresHost = validateDbHost(rawHost);
  if (!postgresHost) {
    return res.status(400).json({ error: "Invalid host. Must be localhost, a valid IPv4/IPv6 address, or a valid hostname (a-z, 0-9, dots, hyphens only)" });
  }

  const postgresPortNum = validatePort(rawPort);
  if (!postgresPortNum) {
    return res.status(400).json({ error: "Invalid port. Must be a number between 1 and 65535" });
  }
  const postgresPort = String(postgresPortNum);

  const jobId = createInstallJob('pgadmin');
  const host = req.headers.host || "localhost";
  const protocol = req.protocol || "http";
  const accessUrl = `${protocol}://${host} (via secure token access)`;

  res.json({ ok: true, jobId, message: "Installation started", accessUrl });

  (async () => {
    try {
      console.log("[pgadmin] Starting pgAdmin4 setup...");

      const result = await installPostgreSQLAndPgAdmin(email, password, jobId, postgresHost, postgresPort);
      if (!result.ok) {
        updateInstallJob(jobId, { status: 'failed', error: result.error || "Installation failed", completedAt: Date.now() });
        return;
      }

      updateInstallJob(jobId, { progress: 92, message: 'Creating PostgreSQL user...' });
      await createPostgresDbUser(postgresUser, password, postgresHost, postgresPort);

      const config = {
        enabled: true,
        host: postgresHost,
        port: postgresPort,
        users: [{
          email,
          pgUser: postgresUser,
          createdAt: Date.now()
        }],
        setupAt: Date.now()
      };

      if (!savePgAdminConfig(config)) {
        updateInstallJob(jobId, { status: 'failed', error: "Failed to save configuration", completedAt: Date.now() });
        return;
      }

      updateInstallJob(jobId, {
        status: 'completed',
        progress: 100,
        message: 'Installation complete!',
        completedAt: Date.now(),
        result: { accessUrl, pgUser: postgresUser }
      });

      console.log("[pgadmin] Setup complete");

    } catch (err) {
      console.error("[pgadmin] Setup error:", err);
      updateInstallJob(jobId, { status: 'failed', error: err.message || "unknown error", completedAt: Date.now() });
    }
  })();
});

app.post("/api/settings/database/pgadmin/disable", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  if (!requireActionTokenOr403(req, res, "POST /api/settings/database/pgadmin/disable")) return;

  try {
    console.log("[pgadmin] Disabling pgAdmin4 and PostgreSQL - full cleanup for clean reinstall...");

    const runCmd = (cmd, timeoutMs = 60000) => new Promise((resolve) => {
      const child = _privBashSpawn(cmd, { shell: false });
      let output = "";
      child.stdout.on("data", (d) => { output += d.toString(); });
      child.stderr.on("data", (d) => { output += d.toString(); });
      child.on("close", (code) => resolve({ code, output }));
      child.on("error", () => resolve({ code: 1, output: "spawn error" }));
      setTimeout(() => { child.kill(); resolve({ code: -1, output: "timeout" }); }, timeoutMs);
    });

    await runCmd("systemctl stop postgresql 2>/dev/null; systemctl disable postgresql 2>/dev/null; killall -9 postgres 2>/dev/null; true");
    console.log("[pgadmin] PostgreSQL stopped and disabled");

    await runCmd("systemctl stop apache2 2>/dev/null; systemctl disable apache2 2>/dev/null; killall -9 apache2 2>/dev/null; true");
    console.log("[pgadmin] Apache stopped and disabled");

    console.log("[pgadmin] Removing Apache pgAdmin4 configuration...");
    try {
      const configsToRemove = [
        '/etc/apache2/sites-enabled/pgadmin4.conf',
        '/etc/apache2/sites-available/pgadmin4.conf',
        '/etc/apache2/conf-enabled/pgadmin4.conf',
        '/etc/apache2/conf-available/pgadmin4.conf'
      ];
      for (const conf of configsToRemove) {
        try { if (fs.existsSync(conf)) fs.unlinkSync(conf); } catch (e) { }
      }
    } catch (e) { }

    await runCmd(`
      # Remove Listen 5050 from ports.conf (clean slate for reinstall)
      sed -i '/Listen 5050/d' /etc/apache2/ports.conf 2>/dev/null || true

      # If ports.conf is empty or only has comments, add a placeholder
      # Don't add Listen 80 as nginx owns that port
      if ! grep -q '^Listen' /etc/apache2/ports.conf 2>/dev/null; then
        echo '# Apache ports - no active listeners (nginx handles web traffic)' > /etc/apache2/ports.conf
      fi

      true
    `);
    console.log("[pgadmin] Apache pgAdmin4 config completely removed");

    await runCmd(`
      # Write placeholder to prevent nginx include errors (don't delete, just empty it)
      echo '# pgAdmin4 disabled - placeholder to prevent nginx include errors' > /etc/nginx/snippets/pgadmin4.conf 2>/dev/null || true

      # Remove any include lines for pgadmin from nginx configs
      for conf in /etc/nginx/sites-available/*.conf /etc/nginx/sites-enabled/*.conf /etc/nginx/conf.d/*.conf; do
        if [ -f "\$conf" ]; then
          sed -i '/include.*pgadmin4.conf/d' "\$conf" 2>/dev/null || true
        fi
      done

      # Test and reload nginx
      nginx -t 2>/dev/null && systemctl reload nginx 2>/dev/null || true

      true
    `);
    console.log("[pgadmin] Nginx pgAdmin config removed");

    await runCmd(`
      DEBIAN_FRONTEND=noninteractive apt-get remove --purge -y pgadmin4-web pgadmin4-desktop pgadmin4 2>/dev/null || true
      DEBIAN_FRONTEND=noninteractive apt-get autoremove -y 2>/dev/null || true
    `, 120000);
    console.log("[pgadmin] pgAdmin4 packages removed");

    await runCmd(`
      rm -rf /var/lib/pgadmin 2>/dev/null || true
      rm -rf /var/log/pgadmin 2>/dev/null || true
      rm -rf /etc/pgadmin 2>/dev/null || true
      rm -f /usr/pgadmin4 2>/dev/null || true

      true
    `);
    console.log("[pgadmin] pgAdmin4 data directories cleaned");

    await runCmd(`
      rm -f /etc/apt/sources.list.d/pgadmin4.list 2>/dev/null || true
      rm -f /usr/share/keyrings/packages-pgadmin-org.gpg 2>/dev/null || true

      # Update apt to remove stale references
      apt-get update -qq 2>/dev/null || true

      true
    `);
    console.log("[pgadmin] pgAdmin4 apt sources removed for clean reinstall");

    await runCmd(`
      true
    `);
    const existingConfig = loadPgAdminConfig();
    const savedPgPort = validatePort(existingConfig.port || '5432');
    await maybeRemoveFirewallPort(runCmd, String(savedPgPort || 5432), 'pgadmin');
    console.log("[pgadmin] Firewall cleanup check completed");

    const config = { enabled: false, users: [], disabledAt: Date.now() };
    if (!savePgAdminConfig(config)) {
      return res.status(500).json({ error: "Failed to save configuration" });
    }

    console.log("[pgadmin] pgAdmin4 fully disabled and uninstalled - ready for clean reinstall");
    return res.json({ ok: true, message: "pgAdmin4 disabled and uninstalled. You can reinstall fresh at any time." });
  } catch (err) {
    console.error("[pgadmin] Disable error:", err);
    return res.status(500).json({ error: "Failed to disable: " + err.message });
  }
});

app.get("/api/settings/database/mongodb/status", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });

  const config = loadMongoDBConfig();
  const mongoRunning = await checkServiceRunning("mongod");

  return res.json({
    enabled: config.enabled,
    installed: mongoRunning,
    mongoRunning,
    host: config.host || '127.0.0.1',
    port: config.port || '27017',
    users: (config.users || []).map(u => ({ username: u.username, createdAt: u.createdAt }))
  });
});

app.post("/api/settings/database/mongodb/setup", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  if (!requireActionTokenOr403(req, res, "POST /api/settings/database/mongodb/setup")) return;

  const { username, password, host, port } = req.body || {};

  const cleanUsername = String(username || "admin").trim();
  if (!cleanUsername || cleanUsername.length > 64 || !/^[a-zA-Z0-9_-]+$/.test(cleanUsername)) {
    return res.status(400).json({ error: "Invalid username (alphanumeric, _, - only)" });
  }

  if (!password || password.length < 8) {
    return res.status(400).json({ error: "Password must be at least 8 characters" });
  }

  const rawHost = String(host || "127.0.0.1").trim();
  const rawPort = String(port || "27017").trim();

  const cleanHost = validateDbHost(rawHost);
  if (!cleanHost) {
    return res.status(400).json({ error: "Invalid host. Must be localhost, a valid IPv4/IPv6 address, or a valid hostname" });
  }

  const cleanPortNum = validatePort(rawPort);
  if (!cleanPortNum) {
    return res.status(400).json({ error: "Invalid port. Must be a number between 1 and 65535" });
  }
  const cleanPort = String(cleanPortNum);

  const jobId = createInstallJob('mongodb');
  const connectionString = `mongodb://${cleanUsername}:****@localhost:${cleanPort}/admin?authSource=admin`;

  res.json({ ok: true, jobId, message: "Installation started", connectionString });

  (async () => {
    try {
      console.log("[mongodb] Starting MongoDB setup...");

      const result = await installMongoDB(cleanUsername, password, jobId, cleanHost, cleanPort);
      if (!result.ok) {
        updateInstallJob(jobId, { status: 'failed', error: result.error || "Installation failed", completedAt: Date.now() });
        return;
      }

      updateInstallJob(jobId, { progress: 95, message: 'Saving configuration...' });
      const passwordHash = await bcrypt.hash(password, BCRYPT_ROUNDS);
      const config = {
        enabled: true,
        host: cleanHost,
        port: cleanPort,
        users: [{
          username: cleanUsername,
          passwordHash,
          createdAt: Date.now()
        }],
        setupAt: Date.now()
      };

      if (!saveMongoDBConfig(config)) {
        updateInstallJob(jobId, { status: 'failed', error: "Failed to save configuration", completedAt: Date.now() });
        return;
      }

      updateInstallJob(jobId, {
        status: 'completed',
        progress: 100,
        message: 'Installation complete!',
        completedAt: Date.now(),
        result: { connectionString }
      });

      console.log("[mongodb] Setup complete");

    } catch (err) {
      console.error("[mongodb] Setup error:", err);
      updateInstallJob(jobId, { status: 'failed', error: err.message || "unknown error", completedAt: Date.now() });
    }
  })();
});

app.post("/api/settings/database/mongodb/disable", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  if (!requireActionTokenOr403(req, res, "POST /api/settings/database/mongodb/disable")) return;

  try {
    console.log("[mongodb] Disabling MongoDB - full cleanup...");

    const runCmd = (cmd, timeoutMs = 60000) => new Promise((resolve) => {
      const child = _privBashSpawn(cmd, {
        shell: false,
        env: { ...process.env, DEBIAN_FRONTEND: "noninteractive" }
      });
      let output = "";
      child.stdout.on("data", (d) => { output += d.toString(); });
      child.stderr.on("data", (d) => { output += d.toString(); });
      child.on("close", (code) => resolve({ code, output }));
      child.on("error", () => resolve({ code: 1, output: "spawn error" }));
      setTimeout(() => { child.kill(); resolve({ code: -1, output: "timeout" }); }, timeoutMs);
    });

    console.log("[mongodb] Stopping MongoDB service...");
    await runCmd("systemctl stop mongod 2>/dev/null || true");
    await runCmd("systemctl disable mongod 2>/dev/null || true");
    await runCmd("pkill -9 mongod 2>/dev/null || true");
    console.log("[mongodb] MongoDB service stopped and disabled");

    console.log("[mongodb] Removing MongoDB packages...");
    await runCmd("DEBIAN_FRONTEND=noninteractive apt-get purge -y mongodb-org mongodb-org-server mongodb-org-mongos mongodb-org-shell mongodb-org-tools mongodb-mongosh 2>/dev/null || true", 120000);
    await runCmd("DEBIAN_FRONTEND=noninteractive apt-get autoremove -y 2>/dev/null || true", 60000);
    console.log("[mongodb] MongoDB packages removed");

    console.log("[mongodb] Removing MongoDB data and configuration...");
    await runCmd(`
      # Remove data directories
      rm -rf /var/lib/mongodb 2>/dev/null || true
      rm -rf /var/log/mongodb 2>/dev/null || true

      # Remove config files
      rm -f /etc/mongod.conf 2>/dev/null || true

      # Remove lock files
      rm -f /tmp/mongodb-*.sock 2>/dev/null || true

      true
    `);
    const existingConfig = loadMongoDBConfig();
    const savedMongoPort = validatePort(existingConfig.port || '27017');
    await maybeRemoveFirewallPort(runCmd, String(savedMongoPort || 27017), 'mongodb');
    console.log("[mongodb] MongoDB data and config removed");

    console.log("[mongodb] Removing MongoDB apt sources...");
    await runCmd(`
      rm -f /etc/apt/sources.list.d/mongodb*.list 2>/dev/null || true
      rm -f /usr/share/keyrings/mongodb*.gpg 2>/dev/null || true
      apt-get update 2>/dev/null || true
    `, 60000);
    console.log("[mongodb] MongoDB apt sources removed");


    await runCmd("systemctl daemon-reload");

    const config = { enabled: false, users: [], disabledAt: Date.now() };
    if (!saveMongoDBConfig(config)) {
      return res.status(500).json({ error: "Failed to save configuration" });
    }

    console.log("[mongodb] MongoDB fully disabled and removed - ready for clean reinstall");
    return res.json({ ok: true, message: "MongoDB disabled and fully removed" });
  } catch (err) {
    console.error("[mongodb] Disable error:", err);
    return res.status(500).json({ error: "Failed to disable: " + err.message });
  }
});

app.post("/api/settings/database/mongodb/users", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  if (!requireActionTokenOr403(req, res, "POST /api/settings/database/mongodb/users")) return;

  if (mongodbOperationLock) {
    return res.status(429).json({ error: "Another MongoDB operation is in progress. Please wait." });
  }
  mongodbOperationLock = true;

  try {
    const { username, password } = req.body || {};
    const cleanUsername = String(username || "").trim();

    if (!cleanUsername || !/^[a-zA-Z0-9_-]+$/.test(cleanUsername) || cleanUsername.length > 64) {
      return res.status(400).json({ error: "Invalid username" });
    }
    if (!password || password.length < 8) {
      return res.status(400).json({ error: "Password must be at least 8 characters" });
    }

    try {
      const config = loadMongoDBConfig();
      if (!config.enabled) {
        return res.status(400).json({ error: "MongoDB is not enabled" });
      }

      if (config.users?.some(u => u.username === cleanUsername)) {
        return res.status(400).json({ error: "User already exists" });
      }

      const portNum = validatePort(config.port || '27017');
      if (!portNum) {
        return res.status(500).json({ error: "Invalid MongoDB port in config" });
      }
      const port = String(portNum);
      const mongoConfPath = '/etc/mongod.conf';

      let originalBindIp = null;

      const runMongosh = (evalScript) => new Promise((resolve) => {
        const child = spawn("mongosh", ["--port", port, "admin", "--quiet", "--eval", evalScript], {
          shell: false,
          stdio: ['pipe', 'pipe', 'pipe']
        });
        let stdout = "", stderr = "";
        child.stdout.on("data", (d) => { stdout += d.toString(); });
        child.stderr.on("data", (d) => { stderr += d.toString(); });
        child.on("close", (code) => resolve({ code, stdout, stderr }));
        child.on("error", (err) => resolve({ code: 1, stdout, stderr: err.message }));
        setTimeout(() => { child.kill(); resolve({ code: -1, stdout, stderr: "timeout" }); }, 15000);
      });

      const runSystemctl = (action) => new Promise((resolve) => {
        const child = spawn("systemctl", [action, "mongod"], { shell: false, stdio: 'pipe' });
        child.on("close", (code) => resolve(code === 0));
        child.on("error", () => resolve(false));
        setTimeout(() => { child.kill(); resolve(false); }, 30000);
      });

      try {
        let conf = fs.readFileSync(mongoConfPath, 'utf8');
        const hadAuth = conf.includes('authorization: enabled');

        if (hadAuth) {
          const bindMatch = conf.match(/bindIp:\s*([\d.,]+)/);
          originalBindIp = bindMatch ? bindMatch[1] : '127.0.0.1';

          conf = conf.replace(/bindIp:\s*[\d.,]+/g, 'bindIp: 127.0.0.1');
          conf = conf.replace(/security:\s*\n\s*authorization:\s*enabled\s*/g, '');
          fs.writeFileSync(mongoConfPath, conf);
          await runSystemctl('restart');
          await new Promise(r => setTimeout(r, 2000));
        }

        const safePassword = JSON.stringify(password);
        const safeUsername = JSON.stringify(cleanUsername);
        const createScript = `db.createUser({user: ${safeUsername}, pwd: ${safePassword}, roles: [{role: "readWriteAnyDatabase", db: "admin"}]})`;
        const result = await runMongosh(createScript);

        if (result.code !== 0 && !result.stderr.includes('already exists')) {
          throw new Error(result.stderr || result.stdout || "Failed to create user");
        }

        if (hadAuth) {
          conf = fs.readFileSync(mongoConfPath, 'utf8');
          if (originalBindIp) {
            conf = conf.replace(/bindIp:\s*[\d.,]+/g, `bindIp: ${originalBindIp}`);
          }
          if (!conf.includes('authorization: enabled')) {
            conf += '\nsecurity:\n  authorization: enabled\n';
          }
          fs.writeFileSync(mongoConfPath, conf);
          await runSystemctl('restart');
        }

        config.users = config.users || [];
        const passwordHash = await bcrypt.hash(password, BCRYPT_ROUNDS);
        config.users.push({ username: cleanUsername, passwordHash, createdAt: Date.now() });
        saveMongoDBConfig(config);

        console.log(`[mongodb] User '${cleanUsername}' created`);
        return res.json({ ok: true, message: "User created" });
      } catch (cmdErr) {
        try {
          let conf = fs.readFileSync(mongoConfPath, 'utf8');
          if (originalBindIp) {
            conf = conf.replace(/bindIp:\s*[\d.,]+/g, `bindIp: ${originalBindIp}`);
          }
          if (!conf.includes('authorization: enabled')) {
            conf += '\nsecurity:\n  authorization: enabled\n';
          }
          fs.writeFileSync(mongoConfPath, conf);
          await runSystemctl('restart');
        } catch (e) { console.error("[mongodb] Failed to restore config:", e.message); }
        throw cmdErr;
      }
    } catch (err) {
      console.error("[mongodb] Create user error:", err.message);
      return res.status(500).json({ error: "Failed to create user: " + err.message });
    }
  } finally {
    mongodbOperationLock = false;
  }
});

app.delete("/api/settings/database/mongodb/users/:username", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  if (!requireActionTokenOr403(req, res, "DELETE /api/settings/database/mongodb/users/:username", { username: String(req.params.username || "").trim() })) return;

  if (mongodbOperationLock) {
    return res.status(429).json({ error: "Another MongoDB operation is in progress. Please wait." });
  }
  mongodbOperationLock = true;

  try {
    const { username } = req.params;
    const cleanUsername = String(username || "").trim();

    if (!cleanUsername || !/^[a-zA-Z0-9_-]+$/.test(cleanUsername)) {
      return res.status(400).json({ error: "Invalid username" });
    }

    try {
      const config = loadMongoDBConfig();
      if (!config.enabled) {
        return res.status(400).json({ error: "MongoDB is not enabled" });
      }

      if (config.users?.[0]?.username === cleanUsername) {
        return res.status(400).json({ error: "Cannot delete the admin user" });
      }

      const portNum = validatePort(config.port || '27017');
      if (!portNum) {
        return res.status(500).json({ error: "Invalid MongoDB port in config" });
      }
      const port = String(portNum);
      const mongoConfPath = '/etc/mongod.conf';
      let originalBindIp = null;

      const runMongosh = (evalScript) => new Promise((resolve) => {
        const child = spawn("mongosh", ["--port", port, "admin", "--quiet", "--eval", evalScript], {
          shell: false,
          stdio: ['pipe', 'pipe', 'pipe']
        });
        let stdout = "", stderr = "";
        child.stdout.on("data", (d) => { stdout += d.toString(); });
        child.stderr.on("data", (d) => { stderr += d.toString(); });
        child.on("close", (code) => resolve({ code, stdout, stderr }));
        child.on("error", (err) => resolve({ code: 1, stdout, stderr: err.message }));
        setTimeout(() => { child.kill(); resolve({ code: -1, stdout, stderr: "timeout" }); }, 15000);
      });

      const runSystemctl = (action) => new Promise((resolve) => {
        const child = spawn("systemctl", [action, "mongod"], { shell: false, stdio: 'pipe' });
        child.on("close", (code) => resolve(code === 0));
        child.on("error", () => resolve(false));
        setTimeout(() => { child.kill(); resolve(false); }, 30000);
      });

      try {
        let conf = fs.readFileSync(mongoConfPath, 'utf8');
        const hadAuth = conf.includes('authorization: enabled');
        if (hadAuth) {
          const bindMatch = conf.match(/bindIp:\s*([\d.,]+)/);
          originalBindIp = bindMatch ? bindMatch[1] : '127.0.0.1';

          conf = conf.replace(/bindIp:\s*[\d.,]+/g, 'bindIp: 127.0.0.1');
          conf = conf.replace(/security:\s*\n\s*authorization:\s*enabled\s*/g, '');
          fs.writeFileSync(mongoConfPath, conf);
          await runSystemctl('restart');
          await new Promise(r => setTimeout(r, 2000));
        }

        const safeUsername = JSON.stringify(cleanUsername);
        const deleteScript = `db.dropUser(${safeUsername})`;
        await runMongosh(deleteScript);

        if (hadAuth) {
          conf = fs.readFileSync(mongoConfPath, 'utf8');
          if (originalBindIp) {
            conf = conf.replace(/bindIp:\s*[\d.,]+/g, `bindIp: ${originalBindIp}`);
          }
          if (!conf.includes('authorization: enabled')) {
            conf += '\nsecurity:\n  authorization: enabled\n';
          }
          fs.writeFileSync(mongoConfPath, conf);
          await runSystemctl('restart');
        }
      } catch (cmdErr) {
        try {
          let conf = fs.readFileSync(mongoConfPath, 'utf8');
          if (originalBindIp) {
            conf = conf.replace(/bindIp:\s*[\d.,]+/g, `bindIp: ${originalBindIp}`);
          }
          if (!conf.includes('authorization: enabled')) {
            conf += '\nsecurity:\n  authorization: enabled\n';
          }
          fs.writeFileSync(mongoConfPath, conf);
          await runSystemctl('restart');
        } catch (e) { console.error("[mongodb] Failed to restore config:", e.message); }
      }

      config.users = (config.users || []).filter(u => u.username !== cleanUsername);
      saveMongoDBConfig(config);

      console.log(`[mongodb] User '${cleanUsername}' deleted`);
      return res.json({ ok: true, message: "User deleted" });
    } catch (err) {
      console.error("[mongodb] Delete user error:", err.message);
      return res.status(500).json({ error: "Failed to delete user" });
    }
  } finally {
    mongodbOperationLock = false;
  }
});

app.post("/api/settings/database/mongodb/users/:username/password", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  if (!requireActionTokenOr403(req, res, "POST /api/settings/database/mongodb/users/:username/password", { username: String(req.params.username || "").trim() })) return;

  if (mongodbOperationLock) {
    return res.status(429).json({ error: "Another MongoDB operation is in progress. Please wait." });
  }
  mongodbOperationLock = true;

  try {
    const { username } = req.params;
    const { password } = req.body || {};
    const cleanUsername = String(username || "").trim();

    if (!cleanUsername) {
      return res.status(400).json({ error: "Invalid username" });
    }
    if (!password || password.length < 8) {
      return res.status(400).json({ error: "Password must be at least 8 characters" });
    }

    try {
      const config = loadMongoDBConfig();
      if (!config.enabled) {
        return res.status(400).json({ error: "MongoDB is not enabled" });
      }

      const portNum = validatePort(config.port || '27017');
      if (!portNum) {
        return res.status(500).json({ error: "Invalid MongoDB port in config" });
      }
      const port = String(portNum);
      const mongoConfPath = '/etc/mongod.conf';
      let originalBindIp = null;

      const runMongosh = (evalScript) => new Promise((resolve) => {
        const child = spawn("mongosh", ["--port", port, "admin", "--quiet", "--eval", evalScript], {
          shell: false,
          stdio: ['pipe', 'pipe', 'pipe']
        });
        let stdout = "", stderr = "";
        child.stdout.on("data", (d) => { stdout += d.toString(); });
        child.stderr.on("data", (d) => { stderr += d.toString(); });
        child.on("close", (code) => resolve({ code, stdout, stderr }));
        child.on("error", (err) => resolve({ code: 1, stdout, stderr: err.message }));
        setTimeout(() => { child.kill(); resolve({ code: -1, stdout, stderr: "timeout" }); }, 15000);
      });

      const runSystemctl = (action) => new Promise((resolve) => {
        const child = spawn("systemctl", [action, "mongod"], { shell: false, stdio: 'pipe' });
        child.on("close", (code) => resolve(code === 0));
        child.on("error", () => resolve(false));
        setTimeout(() => { child.kill(); resolve(false); }, 30000);
      });

      try {
        let conf = fs.readFileSync(mongoConfPath, 'utf8');
        const hadAuth = conf.includes('authorization: enabled');
        if (hadAuth) {
          const bindMatch = conf.match(/bindIp:\s*([\d.,]+)/);
          originalBindIp = bindMatch ? bindMatch[1] : '127.0.0.1';

          conf = conf.replace(/bindIp:\s*[\d.,]+/g, 'bindIp: 127.0.0.1');
          conf = conf.replace(/security:\s*\n\s*authorization:\s*enabled\s*/g, '');
          fs.writeFileSync(mongoConfPath, conf);
          await runSystemctl('restart');
          await new Promise(r => setTimeout(r, 2000));
        }

        const safeUsername = JSON.stringify(cleanUsername);
        const safePassword = JSON.stringify(password);
        const changeScript = `db.changeUserPassword(${safeUsername}, ${safePassword})`;
        const result = await runMongosh(changeScript);

        if (result.code !== 0) {
          throw new Error(result.stderr || result.stdout || "Failed to change password");
        }

        if (hadAuth) {
          conf = fs.readFileSync(mongoConfPath, 'utf8');
          if (originalBindIp) {
            conf = conf.replace(/bindIp:\s*[\d.,]+/g, `bindIp: ${originalBindIp}`);
          }
          if (!conf.includes('authorization: enabled')) {
            conf += '\nsecurity:\n  authorization: enabled\n';
          }
          fs.writeFileSync(mongoConfPath, conf);
          await runSystemctl('restart');
        }

        const userEntry = config.users?.find(u => u.username === cleanUsername);
        if (userEntry) {
          userEntry.passwordHash = await bcrypt.hash(password, BCRYPT_ROUNDS);
          delete userEntry.password;
          saveMongoDBConfig(config);
        }

        console.log(`[mongodb] Password changed for '${cleanUsername}'`);
        return res.json({ ok: true, message: "Password changed" });
      } catch (cmdErr) {
        try {
          let conf = fs.readFileSync(mongoConfPath, 'utf8');
          if (originalBindIp) {
            conf = conf.replace(/bindIp:\s*[\d.,]+/g, `bindIp: ${originalBindIp}`);
          }
          if (!conf.includes('authorization: enabled')) {
            conf += '\nsecurity:\n  authorization: enabled\n';
          }
          fs.writeFileSync(mongoConfPath, conf);
          await runSystemctl('restart');
        } catch (e) { console.error("[mongodb] Failed to restore config:", e.message); }
        throw cmdErr;
      }
    } catch (err) {
      console.error("[mongodb] Change password error:", err.message);
      return res.status(500).json({ error: "Failed to change password" });
    }
  } finally {
    mongodbOperationLock = false;
  }
});

app.get("/api/settings/database/mongodb/users", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });

  try {
    const config = loadMongoDBConfig();

    const safeUsers = (config.users || []).map(u => ({ username: u.username, createdAt: u.createdAt }));

    const actionTokens = {
      createUser: issueActionToken(req, "POST /api/settings/database/mongodb/users", {}, { ttlSeconds: 120, oneTime: true }),
    };
    for (const u of safeUsers) {
      actionTokens[`deleteUser_${u.username}`] = issueActionToken(req, "DELETE /api/settings/database/mongodb/users/:username", { username: u.username }, { ttlSeconds: 120, oneTime: true });
      actionTokens[`changePassword_${u.username}`] = issueActionToken(req, "POST /api/settings/database/mongodb/users/:username/password", { username: u.username }, { ttlSeconds: 120, oneTime: true });
    }

    return res.json({
      ok: true,
      enabled: config.enabled || false,
      host: config.host || '127.0.0.1',
      port: config.port || '27017',
      users: safeUsers,
      actionTokens
    });
  } catch (err) {
    return res.status(500).json({ error: "Failed to load users" });
  }
});

app.get("/api/templates", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  return res.json({ templates: loadTemplatesFile() });
});

app.post("/api/templates/startup-command", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });

  const templateId = normalizeTemplateId(req.body?.templateId || "");
  if (!templateId) return res.status(400).json({ error: "missing templateId" });

  const targetNodeId = String(req.body?.nodeId || "").trim();

  const template = findTemplateById(templateId);
  if (!template) return res.status(404).json({ error: "template not found" });

  const startFile = String(template.start || defaultStartFileForTemplate(templateId) || "").trim();
  const docker = normalizeTemplateDockerForRuntime(templateId, template.docker, startFile);
  const fallback = normalizeLegacyRuntimeProcessCommandInput(
    templateId,
    docker?.command || defaultRuntimeCommandForTemplate(templateId, docker, startFile)
  );
  const fallbackError = validateRuntimeProcessCommandInput(fallback);
  const fallbackCommand = fallbackError ? "" : fallback;

  const image = String(docker?.image || "").trim();
  const tag = String(docker?.tag || "latest").trim() || "latest";
  if (!image) {
    if (fallbackCommand) {
      return res.json({ ok: true, command: fallbackCommand, source: "template-default", imageRef: "" });
    }
    return res.status(400).json({ error: "template has no docker image" });
  }

  let node = null;
  if (targetNodeId && targetNodeId.toLowerCase() !== "local") {
    node = await findNodeByIdOrName(targetNodeId);
  }
  if (!node) {
    const allNodes = await loadNodes();
    node = allNodes.find(n => n && n.online) || allNodes[0] || null;
  }
  if (!node) {
    if (fallbackCommand) {
      return res.json({ ok: true, command: fallbackCommand, source: "template-default", imageRef: `${image}:${tag}`, warning: "no node available for image inspect" });
    }
    return res.status(503).json({ error: "no node available for image inspect" });
  }

  const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
  if (!baseUrl) {
    if (fallbackCommand) {
      return res.json({ ok: true, command: fallbackCommand, source: "template-default", imageRef: `${image}:${tag}`, warning: "invalid node address for image inspect" });
    }
    return res.status(400).json({ error: "invalid node address" });
  }

  const payload = {
    templateId,
    image,
    tag,
    startFile,
    dataDir: runtimeDataDirForTemplate(templateId, docker),
    fallbackCommand,
  };

  const headers = nodeAuthHeadersFor(node, true);
  const upstream = await httpRequestJson(`${baseUrl}/v1/runtime/startup-command`, "POST", headers, payload, 45_000);

  let command = "";
  let source = "template-default";
  let warning = null;
  let inspectEntrypoint = [];
  let inspectCmd = [];
  let inspectWorkingDir = "";
  let inspectEnv = {};

  if (upstream.status === 200 && upstream.json && upstream.json.ok) {
    command = normalizeLegacyRuntimeProcessCommandInput(templateId, upstream.json.command);
    source = String(upstream.json.source || "image-inspect").trim() || "image-inspect";
    inspectEntrypoint = Array.isArray(upstream.json.entrypoint) ? upstream.json.entrypoint : [];
    inspectCmd = Array.isArray(upstream.json.cmd) ? upstream.json.cmd : [];
    inspectWorkingDir = String(upstream.json.workingDir || "").trim();
    inspectEnv = (upstream.json.env && typeof upstream.json.env === 'object' && !Array.isArray(upstream.json.env))
      ? upstream.json.env
      : {};
  } else {
    const detail = String(upstream.json?.detail || upstream.json?.error || `inspect failed (${upstream.status})`).trim();
    warning = detail || null;
  }

  if (
    source === 'image-inspect' &&
    command &&
    (looksLikeWrapperEntrypointOnlyCommand(command, inspectEntrypoint, inspectCmd) || looksLikePathOnlyScriptCommand(command))
  ) {
    if (fallbackCommand) {
      command = fallbackCommand;
      source = 'template-default';
      warning = warning
        ? `${warning}; inspect returned wrapper/path-only command`
        : 'inspect returned wrapper/path-only command';
    }
  }

  if (!command) {
    command = fallbackCommand;
    if (command) source = "template-default";
  }

  const commandError = validateRuntimeProcessCommandInput(command);
  if (!command || commandError) {
    return res.status(502).json({
      error: commandError || "failed to infer startup command",
      detail: warning || "Image inspection did not produce a valid process command.",
    });
  }

  return res.json({
    ok: true,
    command,
    source,
    imageRef: `${image}:${tag}`,
    warning,
    inspect: {
      entrypoint: inspectEntrypoint,
      cmd: inspectCmd,
      workingDir: inspectWorkingDir || '/',
      env: inspectEnv,
    },
  });
});

app.get("/api/settings/templates", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  let templates = loadTemplatesFile() || [];

  const search = String(req.query.search || '').trim().toLowerCase();
  if (search) {
    templates = templates.filter(t => t && ((t.name && t.name.toLowerCase().includes(search)) || (t.id && t.id.toLowerCase().includes(search))));
  }

  const total = templates.length;
  const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 50, 1), 200);
  const totalPages = Math.max(1, Math.ceil(total / limit));
  const page = Math.min(Math.max(parseInt(req.query.page, 10) || 1, 1), totalPages);
  const offset = (page - 1) * limit;
  templates = templates.slice(offset, offset + limit);

  return res.json({ templates, total, page, totalPages });
});

app.post("/api/settings/templates", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });

  const {
    id,
    name,
    description,
    dockerImage,
    dockerTag,
    templateImage,
    defaultPort,
    ports,
    volumes,
    env,
    command,
    startup,
    startupCommand,
    dockerWorkdir,
    workdir,
  } = req.body || {};
  const cleanId = String(id || "").trim().toLowerCase();
  const cleanName = String(name || "").trim();
  const cleanDesc = String(description || "").trim();
  const cleanImage = String(dockerImage || "").trim();
  const cleanTag = String(dockerTag || "latest").trim() || "latest";
  const cleanTemplateImage = String(templateImage || "").trim();
  const cleanCommand = normalizeLegacyRuntimeProcessCommandInput(cleanId, command || startup);
  const cleanStartupCommand = String(startupCommand || "").trim();
  const rawWorkdir = String(dockerWorkdir ?? workdir ?? "").trim();
  const cleanWorkdir = normalizeRuntimeWorkdirInput(rawWorkdir);

  if (!cleanId || !/^[a-z0-9_-]{2,60}$/.test(cleanId)) return res.status(400).json({ error: "invalid id" });
  if (!cleanName) return res.status(400).json({ error: "missing name" });
  if (!cleanImage) return res.status(400).json({ error: "missing docker image" });
  if (!cleanCommand) return res.status(400).json({ error: "missing startup command" });
  if (!rawWorkdir) return res.status(400).json({ error: "missing workdir" });
  if (!cleanWorkdir) return res.status(400).json({ error: "invalid workdir (must be an absolute path like /app)" });
  if (cleanStartupCommand) {
    return res.status(400).json({ error: "Raw Docker startup commands are no longer supported. Configure image, ports, volumes, env, and the in-container command instead." });
  }
  const commandError = validateRuntimeProcessCommandInput(cleanCommand);
  if (commandError) return res.status(400).json({ error: commandError });

  const list = loadTemplatesFile();
  const exists = list.find(t => String(t?.id || "").toLowerCase() === cleanId);
  if (exists) return res.status(400).json({ error: "template id already exists" });

  const cleanPorts = validatePortListInput(ports);
  if (cleanPorts === null) return res.status(400).json({ error: "invalid ports" });

  let cleanVolumes = [defaultTemplateBotDirVolumeForWorkdir(cleanWorkdir)];
  if (Array.isArray(volumes) && volumes.length > 0) {
    cleanVolumes = volumes.map(v => String(v).trim()).filter(Boolean);
  } else if (typeof volumes === "string" && volumes.trim()) {
    cleanVolumes = [volumes.trim()];
  }

  let cleanEnv = {};
  if (env && typeof env === "object" && !Array.isArray(env)) {
    for (const [k, v] of Object.entries(env)) {
      if (k && typeof k === "string") {
        cleanEnv[k.trim()] = String(v ?? "");
      }
    }
  }

  const cleanDefaultPort = validatePort(defaultPort) || (cleanPorts.length > 0 ? cleanPorts[0] : 8080);
  const tpl = {
    id: cleanId,
    name: cleanName,
    description: cleanDesc,
    defaultPort: cleanDefaultPort,
    ...(cleanTemplateImage ? { templateImage: cleanTemplateImage } : {}),
    docker: {
      image: cleanImage,
      tag: cleanTag,
      ports: cleanPorts,
      volumes: cleanVolumes,
      workdir: cleanWorkdir,
      env: cleanEnv,
      command: cleanCommand,
      restart: "unless-stopped",
      console: { type: "stdin" },
    },
  };

  list.push(tpl);
  if (!saveTemplatesFile(list)) return res.status(500).json({ error: "failed to save templates" });
  return res.json({ ok: true, template: tpl });
});

app.put("/api/settings/templates/:id", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });

  const targetId = normalizeTemplateId(req.params.id || "");
  if (!targetId) return res.status(400).json({ error: "invalid template id" });

  const list = loadTemplatesFile();
  const idx = list.findIndex(t => normalizeTemplateId(t?.id) === targetId);
  if (idx < 0) return res.status(404).json({ error: "template not found" });

  const current = list[idx] || {};
  const currentDocker = (current && typeof current.docker === "object" && !Array.isArray(current.docker)) ? current.docker : {};
  const body = req.body && typeof req.body === "object" ? req.body : {};

  const {
    id,
    name,
    description,
    dockerImage,
    dockerTag,
    templateImage,
    defaultPort,
    ports,
    volumes,
    env,
    command,
    startup,
    startupCommand,
    dockerWorkdir,
    workdir,
  } = body;

  const cleanId = String(id || current.id || "").trim().toLowerCase();
  if (!cleanId || !/^[a-z0-9_-]{2,60}$/.test(cleanId)) return res.status(400).json({ error: "invalid id" });
  if (normalizeTemplateId(cleanId) !== targetId) return res.status(400).json({ error: "template id cannot be changed" });

  const cleanName = String(name || "").trim();
  const cleanDesc = String(description || "").trim();
  const cleanImage = String(dockerImage || "").trim();
  const cleanTag = String(dockerTag || currentDocker.tag || "latest").trim() || "latest";

  const hasTemplateImage = Object.prototype.hasOwnProperty.call(body, "templateImage");
  const cleanTemplateImage = hasTemplateImage
    ? String(templateImage || "").trim()
    : String(current.templateImage || "").trim();

  const cleanCommand = normalizeLegacyRuntimeProcessCommandInput(cleanId, command || startup || currentDocker.command || "");
  const cleanStartupCommand = String(startupCommand || "").trim();
  const rawWorkdir = String(dockerWorkdir ?? workdir ?? currentDocker.workdir ?? "").trim();
  const cleanWorkdir = normalizeRuntimeWorkdirInput(rawWorkdir);

  if (!cleanName) return res.status(400).json({ error: "missing name" });
  if (!cleanImage) return res.status(400).json({ error: "missing docker image" });
  if (!cleanCommand) return res.status(400).json({ error: "missing startup command" });
  if (!rawWorkdir) return res.status(400).json({ error: "missing workdir" });
  if (!cleanWorkdir) return res.status(400).json({ error: "invalid workdir (must be an absolute path like /app)" });
  if (cleanStartupCommand) {
    return res.status(400).json({ error: "Raw Docker startup commands are no longer supported. Configure image, ports, volumes, env, and the in-container command instead." });
  }
  const commandError = validateRuntimeProcessCommandInput(cleanCommand);
  if (commandError) return res.status(400).json({ error: commandError });

  const hasPortsInput = Object.prototype.hasOwnProperty.call(body, "ports");
  const cleanPorts = validatePortListInput(hasPortsInput ? ports : currentDocker.ports);
  if (cleanPorts === null) return res.status(400).json({ error: "invalid ports" });

  let cleanVolumes = [defaultTemplateBotDirVolumeForWorkdir(cleanWorkdir)];
  const workdirChanged = normalizeRuntimeWorkdirInput(currentDocker.workdir || "") !== cleanWorkdir;
  const autoAlignCurrentBotDirVolume = shouldAutoAlignBotDirVolume(currentDocker.volumes);
  if (Object.prototype.hasOwnProperty.call(body, "volumes")) {
    if (Array.isArray(volumes) && volumes.length > 0) {
      cleanVolumes = volumes.map(v => String(v).trim()).filter(Boolean);
    } else if (typeof volumes === "string" && volumes.trim()) {
      cleanVolumes = [volumes.trim()];
    }
  } else if (Array.isArray(currentDocker.volumes) && currentDocker.volumes.length > 0) {
    cleanVolumes = currentDocker.volumes.map(v => String(v || "").trim()).filter(Boolean);
    if (!cleanVolumes.length) {
      cleanVolumes = [defaultTemplateBotDirVolumeForWorkdir(cleanWorkdir)];
    } else if (workdirChanged && autoAlignCurrentBotDirVolume) {
      cleanVolumes = [defaultTemplateBotDirVolumeForWorkdir(cleanWorkdir)];
    }
  }

  let cleanEnv = {};
  if (Object.prototype.hasOwnProperty.call(body, "env")) {
    if (env && typeof env === "object" && !Array.isArray(env)) {
      for (const [k, v] of Object.entries(env)) {
        if (k && typeof k === "string") {
          cleanEnv[k.trim()] = String(v ?? "");
        }
      }
    }
  } else if (currentDocker.env && typeof currentDocker.env === "object" && !Array.isArray(currentDocker.env)) {
    for (const [k, v] of Object.entries(currentDocker.env)) {
      if (k && typeof k === "string") {
        cleanEnv[k.trim()] = String(v ?? "");
      }
    }
  }

  const existingDefaultPort = validatePort(current.defaultPort);
  const cleanDefaultPort = validatePort(defaultPort)
    || existingDefaultPort
    || (cleanPorts.length > 0 ? cleanPorts[0] : 8080);

  const tpl = {
    id: cleanId,
    name: cleanName,
    description: cleanDesc,
    defaultPort: cleanDefaultPort,
    ...(cleanTemplateImage ? { templateImage: cleanTemplateImage } : {}),
    docker: {
      image: cleanImage,
      tag: cleanTag,
      ports: cleanPorts,
      volumes: cleanVolumes,
      workdir: cleanWorkdir,
      env: cleanEnv,
      command: cleanCommand,
      restart: "unless-stopped",
      console: { type: "stdin" },
    },
  };

  list[idx] = tpl;
  if (!saveTemplatesFile(list)) return res.status(500).json({ error: "failed to save templates" });
  return res.json({ ok: true, template: tpl });
});

app.delete("/api/settings/templates/:id", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });

  const cleanId = normalizeTemplateId(req.params.id || "");
  if (!cleanId) return res.status(400).json({ error: "invalid template id" });

  const list = loadTemplatesFile();
  const idx = list.findIndex(t => normalizeTemplateId(t?.id) === cleanId);
  if (idx < 0) return res.status(404).json({ error: "template not found" });

  const used = (await loadServersIndex())
    .filter(s => normalizeTemplateId(s?.template) === cleanId)
    .map(s => s.name)
    .filter(Boolean);
  if (used.length) return res.status(400).json({ error: "template in use", servers: used });

  list.splice(idx, 1);
  if (!saveTemplatesFile(list)) return res.status(500).json({ error: "failed to save templates" });
  return res.json({ ok: true, removed: cleanId });
});

const DASH_STATUS_TTL_MS = parseInt(process.env.DASH_STATUS_TTL_MS || "5000", 10);
const SERVER_START = Date.now();

app.get("/", async (req, res) => {
  try {
    const serverIndex = await loadServersIndex();
    const email = req.session?.user || null;
    const userObj = email ? await findUserByEmail(email) : null;
    const safeUser = userObj ? { email: userObj.email, admin: !!userObj.admin, avatar_url: userObj.avatar_url || null } : null;

    const allNames = serverIndex.map(e => e && e.name).filter(Boolean);

    let botsToShow;
    if (safeUser && safeUser.admin) {
      botsToShow = allNames;
    } else {
      const access = email ? await getAccessListForEmail(email) : [];
      botsToShow = access.includes("all") ? allNames : allNames.filter(n => access.includes(n));
    }

    const dashSearch = String(req.query.search || '').trim().toLowerCase();
    if (dashSearch) {
      const sMap = new Map(serverIndex.filter(e => e && e.name).map(e => [e.name, e]));
      botsToShow = botsToShow.filter(name => {
        if (name.toLowerCase().includes(dashSearch)) return true;
        const e = sMap.get(name);
        return e && e.displayName && e.displayName.toLowerCase().includes(dashSearch);
      });
    }
    const totalServers = botsToShow.length;
    const dashLimit = Math.min(Math.max(parseInt(req.query.limit, 10) || 50, 1), 200);
    const dashTotalPages = Math.max(1, Math.ceil(totalServers / dashLimit));
    const dashPage = Math.min(Math.max(parseInt(req.query.page, 10) || 1, 1), dashTotalPages);
    botsToShow = botsToShow.slice((dashPage - 1) * dashLimit, dashPage * dashLimit);

    const { results: cachedStatuses } = await getAllCachedServerStatusesLenient(botsToShow);

    const templates = loadTemplatesFile() || [];
    const templateMap = new Map(templates.filter(Boolean).map(t => [normalizeTemplateId(t.id), t]));
    const nodes = await loadNodes();
    const nodeMap = new Map(nodes.map(n => [n.uuid || n.id || n.name, n]));
    const serverMap = new Map(serverIndex.filter(e => e && e.name).map(e => [e.name, e]));

    const botCards = botsToShow.map((name) => {
      const entry = serverMap.get(name) || {};
      const cached = cachedStatuses.get(name);
      const template = templateMap.get(normalizeTemplateId(entry.template)) || null;
      const node = entry.nodeId ? nodeMap.get(entry.nodeId) : null;

      const status = cached?.status || normalizeStatusLabel(entry.status) || 'unknown';
      const nodeOnline = cached?.nodeOnline !== false;

      const cpuPercent = cached?.cpu ?? null;
      const memoryUsed = cached?.memory ?? null;
      const diskUsed = cached?.disk ?? null;

      const docker = entry.docker || {};
      const res = entry.resources || {};
      const memory = formatResource(memoryUsed ?? docker.memoryUsedMb ?? docker.memoryMbUsed, res.ramMb ?? docker.memoryMb);
      const disk = formatResource(diskUsed ?? docker.storageUsedMb ?? docker.storageMbUsed, res.storageMb ?? docker.storageMb);
      return {
        name,
        displayName: entry.displayName || null,
        templateId: entry.template || null,
        templateName: template?.name || entry.template || "Custom template",
        templateImage: template?.templateImage || null,
        description: template?.description || "Manage configuration, logs and deployments.",
        nodeName: node?.name || (entry.nodeId ? entry.nodeId : "Node"),
        nodeId: entry.nodeId || null,
        ip: entry.ip || null,
        port: entry.port || null,
        cpu: cpuPercent != null ? `${Number(cpuPercent).toFixed(1)}%` : null,
        cpuPercent,
        cpuLimit: cached?.cpuLimit ?? null,
        memory,
        disk,
        status,
        nodeOnline,
        statusUpdatedAt: cached?.updatedAt || null
      };
    });

    return res.render("index", {
      bots: botCards,
      pagination: { page: dashPage, totalPages: dashTotalPages, total: totalServers, limit: dashLimit, search: dashSearch },
      isAdmin: safeUser ? safeUser.admin : false,
      showDashboardAssistant: canUserAccessDashboardAssistant(safeUser),
      user: safeUser,
      serverStartTime: SERVER_START,
      globalAlert: getActiveGlobalAlert(),
      quickActions: (function () {
        const all = loadQuickActions();
        return (safeUser && safeUser.admin) ? all.admin : all.user;
      })(),
      avatarUrl: resolveUserAvatarUrl(safeUser),
      cacheInfo: {
        lastRefresh: statusCache.lastFullRefresh,
        ttlMs: STATUS_CACHE_TTL_MS
      }
    });
  } catch (err) {
    console.error("Error in GET /", err);
    return res.status(500).send("Internal error");
  }
});

app.get("/settings", async (req, res) => {
  if (!(await isAdmin(req))) return res.redirect("/");
  const user = await findUserByEmail(req.session.user);
  res.render("settings", { user, cspNonce: res.locals.cspNonce });
});

app.get("/account", async (req, res) => {
  if (!req.session || !req.session.user) {
    return res.redirect("/login");
  }

  const user = await findUserByEmail(req.session.user);
  if (!user) {
    return res.redirect("/login");
  }

  const adminCheck = await isAdmin(req);

  res.render("account", {
    username: user.username || user.email.split('@')[0],
    email: user.email,
    isAdmin: adminCheck,
    odbc_id: user.id || 'N/A',
    cspNonce: res.locals.cspNonce
  });
});

app.get("/api/account", async (req, res) => {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ error: "Not authenticated" });
  }

  const user = await findUserByEmail(req.session.user);
  if (!user) {
    return res.status(401).json({ error: "User not found" });
  }

  const adminCheck = await isAdmin(req);

  let statusText = "Available";
  let statusExpiresAt = null;
  try {
    const rows = await db.query(
      "SELECT status_text, expires_at FROM user_status WHERE user_id = ?",
      [user.id]
    );
    if (rows && rows[0]) {
      const now = Date.now();
      if (!rows[0].expires_at || Number(rows[0].expires_at) > now) {
        statusText = rows[0].status_text || "Available";
        statusExpiresAt = rows[0].expires_at ? Number(rows[0].expires_at) : null;
      }
    }
  } catch {
  }

  let usernameChangeCooldown = null;
  if (!adminCheck && user.username_changed_at) {
    const lastChange = Number(user.username_changed_at);
    const sevenDaysMs = 7 * 24 * 60 * 60 * 1000;
    const timeSinceChange = Date.now() - lastChange;
    if (timeSinceChange < sevenDaysMs) {
      usernameChangeCooldown = sevenDaysMs - timeSinceChange;
    }
  }

  return res.json({
    ok: true,
    username: user.username || user.email.split('@')[0],
    email: user.email,
    isAdmin: adminCheck,
    userId: user.id || null,
    createdAt: user.created_at || null,
    avatarUrl: resolveUserAvatarUrl({ ...user, admin: adminCheck }),
    status: statusText,
    statusExpiresAt,
    usernameChangeCooldown
  });
});

app.post("/api/account/avatar", async (req, res) => {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ error: "Not authenticated" });
  }

  const user = await findUserByEmail(req.session.user);
  if (!user) {
    return res.status(401).json({ error: "User not found" });
  }

  const { avatarUrl } = req.body || {};

  if (!avatarUrl || typeof avatarUrl !== 'string') {
    return res.status(400).json({ error: "Avatar URL is required" });
  }

  const cleanUrl = avatarUrl.trim();
  if (cleanUrl.length > 2048) {
    return res.status(400).json({ error: "URL too long" });
  }

  try {
    new URL(cleanUrl);
  } catch {
    return res.status(400).json({ error: "Invalid URL format" });
  }

  if (!cleanUrl.startsWith('http://') && !cleanUrl.startsWith('https://')) {
    return res.status(400).json({ error: "URL must use http or https protocol" });
  }

  try {
    await db.query(
      "UPDATE users SET avatar_url = ? WHERE LOWER(email) = LOWER(?)",
      [cleanUrl, req.session.user]
    );

    return res.json({ ok: true, avatarUrl: cleanUrl });
  } catch (err) {
    console.error("[account] Avatar update error:", err);
    return res.status(500).json({ error: "Failed to update avatar" });
  }
});

app.post("/api/account/username", async (req, res) => {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ error: "Not authenticated" });
  }

  const user = await findUserByEmail(req.session.user);
  if (!user) {
    return res.status(401).json({ error: "User not found" });
  }

  const { username } = req.body || {};

  if (!username || typeof username !== 'string') {
    return res.status(400).json({ error: "Username is required" });
  }

  const cleanUsername = username.trim();
  if (cleanUsername.length < 3 || cleanUsername.length > 30) {
    return res.status(400).json({ error: "Username must be 3-30 characters" });
  }

  if (!/^[a-zA-Z0-9_-]+$/.test(cleanUsername)) {
    return res.status(400).json({ error: "Username can only contain letters, numbers, underscore and hyphen" });
  }

  const isUserAdmin = !!user.admin;
  if (!isUserAdmin && user.username_changed_at) {
    const lastChange = Number(user.username_changed_at);
    const sevenDaysMs = 7 * 24 * 60 * 60 * 1000;
    const now = Date.now();
    const timeSinceChange = now - lastChange;

    if (timeSinceChange < sevenDaysMs) {
      const remainingMs = sevenDaysMs - timeSinceChange;
      const remainingDays = Math.ceil(remainingMs / (24 * 60 * 60 * 1000));
      return res.status(429).json({
        error: `You can change your username again in ${remainingDays} day${remainingDays !== 1 ? 's' : ''}`,
        cooldownRemaining: remainingMs
      });
    }
  }

  try {
    await db.query(
      "UPDATE users SET username = ?, username_changed_at = ? WHERE LOWER(email) = LOWER(?)",
      [cleanUsername, Date.now(), req.session.user]
    );

    return res.json({ ok: true, username: cleanUsername });
  } catch (err) {
    console.error("[account] Username update error:", err);
    return res.status(500).json({ error: "Failed to update username" });
  }
});

let _avatarUpload;
function getAvatarUpload() {
  if (!_avatarUpload) {
    const multer = getLazyMulter();
    const avatarStorage = multer.diskStorage({
      destination: function (req, file, cb) {
        const uploadDir = path.join(__dirname, 'public', 'uploads', 'avatars');
        fs.mkdirSync(uploadDir, { recursive: true });
        cb(null, uploadDir);
      },
      filename: function (req, file, cb) {
        const mimeToExt = {
          "image/jpeg": ".jpg",
          "image/png": ".png",
          "image/gif": ".gif",
          "image/webp": ".webp"
        };
        const ext = mimeToExt[file.mimetype] || ".jpg";
        const uniqueName = `${Date.now()}-${crypto.randomBytes(8).toString('hex')}${ext}`;
        cb(null, uniqueName);
      }
    });
    _avatarUpload = multer({
      storage: avatarStorage,
      limits: { fileSize: 5 * 1024 * 1024 },
      fileFilter: function (req, file, cb) {
        const allowedTypes = ['image/jpeg', 'image/png', 'image/gif', 'image/webp'];
        if (allowedTypes.includes(file.mimetype)) {
          cb(null, true);
        } else {
          cb(new Error('Only image files are allowed (JPEG, PNG, GIF, WebP)'));
        }
      }
    });
  }
  return _avatarUpload;
}

app.post("/api/account/avatar/upload", (req, res, next) => getAvatarUpload().single('avatar')(req, res, next), async (req, res) => {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ error: "Not authenticated" });
  }

  const user = await findUserByEmail(req.session.user);
  if (!user) {
    return res.status(401).json({ error: "User not found" });
  }

  if (!req.file) {
    return res.status(400).json({ error: "No file uploaded" });
  }

  try {
    const fileBuffer = await fsp.readFile(req.file.path);

    if (!validateFileMagicBytes(fileBuffer, req.file.mimetype)) {
      await fsp.unlink(req.file.path).catch(() => { });
      console.warn(`[security] Rejected file upload: magic bytes don't match declared type ${req.file.mimetype}`);
      return res.status(400).json({ error: "Invalid file format: file content doesn't match declared type" });
    }

    if (!checkFileForDangerousContent(fileBuffer)) {
      await fsp.unlink(req.file.path).catch(() => { });
      console.warn(`[security] Rejected file upload: potentially dangerous content detected`);
      return res.status(400).json({ error: "File rejected: potentially dangerous content detected" });
    }
  } catch (err) {
    await fsp.unlink(req.file.path).catch(() => { });
    console.error("[security] File validation error:", err);
    return res.status(500).json({ error: "Failed to validate uploaded file" });
  }

  const avatarUrl = `/uploads/avatars/${req.file.filename}`;

  try {
    if (user.avatar_url && user.avatar_url.startsWith('/uploads/avatars/')) {
      try {
        const oldFile = path.basename(user.avatar_url);
        const oldPath = path.join(__dirname, 'public', 'uploads', 'avatars', oldFile);
        await fsp.unlink(oldPath);
      } catch {
      }
    }

    await db.query(
      "UPDATE users SET avatar_url = ? WHERE LOWER(email) = LOWER(?)",
      [avatarUrl, req.session.user]
    );

    return res.json({ ok: true, avatarUrl });
  } catch (err) {
    console.error("[account] Avatar upload error:", err);
    return res.status(500).json({ error: "Failed to update avatar" });
  }
});

app.post("/api/account/2fa/generate", async (req, res) => {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ error: "Not authenticated" });
  }

  const user = await findUserByEmail(req.session.user);
  if (!user) {
    return res.status(401).json({ error: "User not found" });
  }

  const { currentCode } = req.body || {};

  if (!currentCode || typeof currentCode !== 'string') {
    return res.status(400).json({ error: "Current 2FA code is required" });
  }

  const codeStr = currentCode.replace(/\s+/g, "");
  if (!/^\d{6}$/.test(codeStr)) {
    return res.status(400).json({ error: "Invalid 2FA code format" });
  }

  if (!user.secret) {
    return res.status(400).json({ error: "2FA is not enabled for this account" });
  }

  const verified = speakeasy.totp.verify({
    secret: user.secret,
    encoding: "base32",
    token: codeStr,
    window: 1,
  });

  if (!verified) {
    return res.status(403).json({ error: "Incorrect current 2FA code" });
  }

  const newSecret = speakeasy.generateSecret({ length: 20 });
  const appName = process.env.APP_NAME || "ADPanel";
  const otpauthUrl = speakeasy.otpauthURL({
    secret: newSecret.base32,
    label: user.email,
    issuer: appName,
    encoding: "base32",
  });

  const qrcode = require("qrcode");
  try {
    const qrCodeUrl = await qrcode.toDataURL(otpauthUrl);
    req.session.pending2faSecret = newSecret.base32;
    req.session.pending2faFrom = 'standard';
    return res.json({
      ok: true,
      newSecret: newSecret.base32,
      qrCodeUrl: qrCodeUrl
    });
  } catch (err) {
    console.error("[account] QR code generation error:", err);
    return res.status(500).json({ error: "Failed to generate QR code" });
  }
});

app.post("/api/account/2fa/confirm", async (req, res) => {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ error: "Not authenticated" });
  }

  const user = await findUserByEmail(req.session.user);
  if (!user) {
    return res.status(401).json({ error: "User not found" });
  }

  const { newCode, newSecret } = req.body || {};

  if (!newCode || typeof newCode !== 'string') {
    return res.status(400).json({ error: "New 2FA code is required" });
  }

  if (!newSecret || typeof newSecret !== 'string') {
    return res.status(400).json({ error: "New secret is required" });
  }

  const codeStr = newCode.replace(/\s+/g, "");
  if (!/^\d{6}$/.test(codeStr)) {
    return res.status(400).json({ error: "Invalid 2FA code format" });
  }

  if (!/^[A-Z2-7]+=*$/i.test(newSecret) || newSecret.length < 16) {
    return res.status(400).json({ error: "Invalid secret format" });
  }

  const pendingFlow = req.session.pending2faFrom || 'none';
  if (!req.session.pending2faSecret || req.session.pending2faSecret !== newSecret) {
    delete req.session.pending2faSecret;
    delete req.session.pending2faFrom;
    console.log(`[SECURITY] 2FA confirm denied - secret mismatch | User: ${req.session.user} | IP: ${getRequestIp(req)} | Flow: ${pendingFlow}`);
    return res.status(403).json({ error: "Invalid or tampered secret. Please generate a new 2FA secret first." });
  }

  const verified = speakeasy.totp.verify({
    secret: newSecret,
    encoding: "base32",
    token: codeStr,
    window: 1,
  });

  if (!verified) {
    return res.status(403).json({ error: "Incorrect code from new authenticator" });
  }

  try {
    await db.query(
      "UPDATE users SET secret = ? WHERE LOWER(email) = LOWER(?)",
      [newSecret, req.session.user]
    );

    const wasRecoveryFlow = req.session.pending2faFrom === 'recovery';
    delete req.session.pending2faSecret;
    delete req.session.pending2faFrom;

    console.log(`[SECURITY] 2FA secret updated | User: ${req.session.user} | IP: ${getRequestIp(req)} | Flow: ${wasRecoveryFlow ? 'recovery' : 'standard'}`);

    setRememberLoginCookie(req, res, { ...user, secret: newSecret });
    return res.json({ ok: true });
  } catch (err) {
    console.error("[account] 2FA update error:", err);
    return res.status(500).json({ error: "Failed to update 2FA" });
  }
});


app.post("/api/admin/user/reset-2fa", async (req, res) => {
  if (!(await isAdmin(req))) {
    return res.status(403).json({ error: "Admin access required" });
  }
  if (!requireActionTokenOr403(req, res, "POST /api/admin/user/reset-2fa")) return;

  const { email } = req.body || {};
  if (!email || typeof email !== "string") {
    return res.status(400).json({ error: "Target email required" });
  }

  const targetUser = await findUserByEmail(email);
  if (!targetUser) {
    return res.status(404).json({ error: "User not found" });
  }

  if (targetUser.email.toLowerCase() === req.session.user.toLowerCase()) {
    return res.status(400).json({ error: "Cannot reset your own 2FA through admin panel. Use account settings." });
  }

  const speakeasy = require("speakeasy");
  const newSecret = speakeasy.generateSecret({ length: 20 });
  const appName = process.env.APP_NAME || "ADPanel";
  const otpauthUrl = speakeasy.otpauthURL({
    secret: newSecret.base32,
    label: targetUser.email,
    issuer: appName,
    encoding: "base32",
  });

  const qrcode = require("qrcode");
  try {
    const qrCodeUrl = await qrcode.toDataURL(otpauthUrl);
    return res.json({
      ok: true,
      email: targetUser.email,
      newSecret: newSecret.base32,
      qrCodeUrl: qrCodeUrl
    });
  } catch (err) {
    console.error("[admin] 2FA reset QR generation error:", err);
    return res.status(500).json({ error: "Failed to generate QR code" });
  }
});

app.post("/api/admin/user/confirm-2fa-reset", async (req, res) => {
  if (!(await isAdmin(req))) {
    return res.status(403).json({ error: "Admin access required" });
  }
  if (!requireActionTokenOr403(req, res, "POST /api/admin/user/confirm-2fa-reset")) return;

  const { email, newSecret } = req.body || {};

  if (!email || typeof email !== "string") {
    return res.status(400).json({ error: "Target email required" });
  }

  if (!newSecret || typeof newSecret !== "string") {
    return res.status(400).json({ error: "New secret required" });
  }

  if (!/^[A-Z2-7]+=*$/i.test(newSecret) || newSecret.length < 16) {
    return res.status(400).json({ error: "Invalid secret format" });
  }

  const targetUser = await findUserByEmail(email);
  if (!targetUser) {
    return res.status(404).json({ error: "User not found" });
  }

  try {
    await db.query(
      "UPDATE users SET secret = ? WHERE LOWER(email) = LOWER(?)",
      [newSecret, email]
    );

    console.log(`[admin] 2FA reset for user ${email} by admin ${req.session.user}`);
    return res.json({ ok: true });
  } catch (err) {
    console.error("[admin] 2FA reset save error:", err);
    return res.status(500).json({ error: "Failed to update 2FA secret" });
  }
});

app.post("/api/admin/user/reset-recovery-codes", async (req, res) => {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ error: "Not authenticated" });
  }

  if (!(await isAdmin(req))) {
    return res.status(403).json({ error: "Admin access required" });
  }
  if (!requireActionTokenOr403(req, res, "POST /api/admin/user/reset-recovery-codes")) return;

  const { email } = req.body || {};
  if (!email || typeof email !== "string") {
    return res.status(400).json({ error: "Target email required" });
  }

  const targetUser = await findUserByEmail(email);
  if (!targetUser) {
    return res.status(404).json({ error: "User not found" });
  }

  const RECOVERY_CODE_COUNT = 7;
  const RECOVERY_CODE_LENGTH = 8;
  const RECOVERY_CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

  const RECOVERY_REJECT_THRESHOLD = RECOVERY_CODE_CHARS.length * Math.floor(256 / RECOVERY_CODE_CHARS.length);
  function generateRecoveryCode() {
    let code = '';
    for (let i = 0; i < RECOVERY_CODE_LENGTH; i++) {
      let byte;
      do { byte = crypto.randomBytes(1)[0]; } while (byte >= RECOVERY_REJECT_THRESHOLD);
      code += RECOVERY_CODE_CHARS.charAt(byte % RECOVERY_CODE_CHARS.length);
    }
    return code;
  }

  let bcrypt;
  try {
    bcrypt = require("bcrypt");
  } catch {
    bcrypt = require("bcryptjs");
  }

  const plainRecoveryCodes = [];
  const hashedRecoveryCodes = [];
  for (let i = 0; i < RECOVERY_CODE_COUNT; i++) {
    const code = generateRecoveryCode();
    plainRecoveryCodes.push(code);
    hashedRecoveryCodes.push(await bcrypt.hash(code, BCRYPT_ROUNDS));
  }

  const recoveryCodesJson = JSON.stringify(hashedRecoveryCodes);

  try {
    await db.query(
      "UPDATE users SET recovery_codes = ? WHERE LOWER(email) = LOWER(?)",
      [recoveryCodesJson, email]
    );

    console.log(`[admin] Recovery codes reset for user ${email} by admin ${req.session.user}`);
    return res.json({
      ok: true,
      email: targetUser.email,
      recoveryCodes: plainRecoveryCodes
    });
  } catch (err) {
    console.error("[admin] Recovery codes reset error:", err);
    return res.status(500).json({ error: "Failed to reset recovery codes" });
  }
});

app.post("/api/admin/user/create", async (req, res) => {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ error: "Not authenticated" });
  }

  if (!(await isAdmin(req))) {
    return res.status(403).json({ error: "Admin access required" });
  }

  const { email, password } = req.body || {};

  if (!email || typeof email !== "string") {
    return res.status(400).json({ error: "Email required" });
  }
  const emailLower = email.trim().toLowerCase();
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(emailLower)) {
    return res.status(400).json({ error: "Invalid email format" });
  }

  if (!password || typeof password !== "string" || password.length < 8) {
    return res.status(400).json({ error: "Password must be at least 8 characters" });
  }

  const existingUser = await findUserByEmail(emailLower);
  if (existingUser) {
    return res.status(409).json({ error: "User already exists" });
  }

  let bcrypt;
  try {
    bcrypt = require("bcrypt");
  } catch {
    bcrypt = require("bcryptjs");
  }

  const passwordHash = await bcrypt.hash(password, BCRYPT_ROUNDS);

  const speakeasy = require("speakeasy");
  const secret = speakeasy.generateSecret({ length: 20 });
  const appName = process.env.APP_NAME || "ADPanel";
  const otpauthUrl = speakeasy.otpauthURL({
    secret: secret.base32,
    label: emailLower,
    issuer: appName,
    encoding: "base32",
  });

  const qrcode = require("qrcode");
  let qrCodeUrl;
  try {
    qrCodeUrl = await qrcode.toDataURL(otpauthUrl);
  } catch (err) {
    console.error("[admin] QR code generation error:", err);
    return res.status(500).json({ error: "Failed to generate QR code" });
  }

  const RECOVERY_CODE_COUNT = 7;
  const RECOVERY_CODE_LENGTH = 8;
  const RECOVERY_CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

  const RECOVERY_REJECT_THRESHOLD = RECOVERY_CODE_CHARS.length * Math.floor(256 / RECOVERY_CODE_CHARS.length);
  function generateRecoveryCode() {
    let code = '';
    for (let i = 0; i < RECOVERY_CODE_LENGTH; i++) {
      let byte;
      do { byte = crypto.randomBytes(1)[0]; } while (byte >= RECOVERY_REJECT_THRESHOLD);
      code += RECOVERY_CODE_CHARS.charAt(byte % RECOVERY_CODE_CHARS.length);
    }
    return code;
  }

  const plainRecoveryCodes = [];
  const hashedRecoveryCodes = [];
  for (let i = 0; i < RECOVERY_CODE_COUNT; i++) {
    const code = generateRecoveryCode();
    plainRecoveryCodes.push(code);
    hashedRecoveryCodes.push(await bcrypt.hash(code, BCRYPT_ROUNDS));
  }

  const recoveryCodesJson = JSON.stringify(hashedRecoveryCodes);
  const avatarUrl = getDefaultUserAvatar(emailLower);

  try {
    await db.query(
      `INSERT INTO users (email, password, secret, admin, recovery_codes, avatar_url) VALUES (?, ?, ?, 0, ?, ?)`,
      [emailLower, passwordHash, secret.base32, recoveryCodesJson, avatarUrl]
    );

    console.log(`[admin] User created: ${emailLower} by admin ${req.session.user}`);

    return res.json({
      ok: true,
      email: emailLower,
      secret: secret.base32,
      qrCodeUrl: qrCodeUrl,
      recoveryCodes: plainRecoveryCodes
    });
  } catch (err) {
    console.error("[admin] User creation error:", err);
    if (err.code === 'ER_DUP_ENTRY') {
      return res.status(409).json({ error: "User already exists" });
    }
    return res.status(500).json({ error: "Failed to create user" });
  }
});

app.post("/api/admin/branding/update", async (req, res) => {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ error: "Not authenticated" });
  }

  if (!(await isAdmin(req))) {
    return res.status(403).json({ error: "Admin access required" });
  }

  const { appName, logoUrl, logoBase64, logoFilename } = req.body || {};

  const sanitizedAppName = sanitizeAppName(appName);
  if (!sanitizedAppName) {
    return res.status(400).json({
      error: "Invalid app name. Must be 1-50 characters, alphanumeric with basic punctuation only."
    });
  }

  const currentAppName = loadBrandingConfig().appName;

  try {
    let localLogoPath = null;
    let finalLogoUrl = null;

    if (logoBase64 && logoFilename) {
      const validation = validateBase64Image(logoBase64);
      if (!validation.valid) {
        return res.status(400).json({ error: validation.error || "Invalid image data" });
      }

      const ext = getValidatedExtension(logoFilename);
      if (!ext) {
        return res.status(400).json({
          error: "Invalid file type. Allowed: png, jpg, jpeg, webp, ico, svg"
        });
      }

      const safeFilename = generateSafeLogoFilename(ext);
      const logoPath = path.join(__dirname, "public", "images", safeFilename);

      fs.writeFileSync(logoPath, validation.data);

      const faviconPath = path.join(__dirname, "public", "images", "favicon.ico");
      try {
        fs.copyFileSync(logoPath, faviconPath);
      } catch (e) {
        console.log("[branding] Could not update favicon:", e.message);
      }

      localLogoPath = safeFilename;
      console.log(`[branding] Logo saved securely as: ${safeFilename}`);
    } else if (logoUrl) {
      const sanitizedUrl = sanitizeLogoUrl(logoUrl);
      if (!sanitizedUrl) {
        return res.status(400).json({
          error: "Invalid logo URL. Must be a valid HTTP/HTTPS URL."
        });
      }
      finalLogoUrl = sanitizedUrl;
    }

    const saved = saveBrandingConfig({
      appName: sanitizedAppName,
      logoUrl: finalLogoUrl,
      localLogoPath: localLogoPath
    });

    if (!saved) {
      return res.status(500).json({ error: "Failed to save branding configuration" });
    }

    invalidateBrandingCache();

    console.log(`[admin] Branding updated securely: "${currentAppName}" -> "${sanitizedAppName}" by ${req.session.user}`);

    return res.json({
      ok: true,
      message: "Branding updated successfully. Refresh the page to see changes.",
      newAppName: sanitizedAppName
    });
  } catch (err) {
    console.error("[admin] Branding update error:", err);
    return res.status(500).json({ error: "Failed to update branding" });
  }
});

app.get("/api/admin/branding", async (req, res) => {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ error: "Not authenticated" });
  }

  if (!(await isAdmin(req))) {
    return res.status(403).json({ error: "Admin access required" });
  }

  const branding = loadBrandingConfig();

  return res.json({
    appName: branding.appName,
    logoUrl: branding.logoUrl
  });
});

app.get("/api/admin/login-watermark", async (req, res) => {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ error: "Not authenticated" });
  }

  if (!(await isAdmin(req))) {
    return res.status(403).json({ error: "Admin access required" });
  }

  const branding = loadBrandingConfig();
  const watermarkUrl = resolveLoginWatermarkAssetUrl(branding);
  const watermarkMode = branding.loginWatermarkUrl ? "url" : "upload";

  return res.json({
    watermarkUrl,
    externalUrl: branding.loginWatermarkUrl || "",
    mode: watermarkMode
  });
});

app.post("/api/admin/login-watermark", async (req, res) => {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ error: "Not authenticated" });
  }

  if (!(await isAdmin(req))) {
    return res.status(403).json({ error: "Admin access required" });
  }

  const { watermarkUrl, watermarkBase64, watermarkFilename } = req.body || {};
  const currentBranding = loadBrandingConfig();

  try {
    let localLoginWatermarkPath = currentBranding.localLoginWatermarkPath || null;
    let finalLoginWatermarkUrl = currentBranding.loginWatermarkUrl || null;

    if (watermarkBase64 && watermarkFilename) {
      const validation = validateBase64Image(watermarkBase64);
      if (!validation.valid) {
        return res.status(400).json({ error: validation.error || "Invalid image data" });
      }

      const ext = getValidatedExtension(watermarkFilename);
      if (!ext) {
        return res.status(400).json({
          error: "Invalid file type. Allowed: png, jpg, jpeg, webp, ico, svg"
        });
      }

      const safeFilename = `login-watermark-${crypto.randomBytes(8).toString("hex")}.${ext}`;
      const targetPath = path.join(__dirname, "public", "images", safeFilename);
      fs.writeFileSync(targetPath, validation.data);

      localLoginWatermarkPath = safeFilename;
      finalLoginWatermarkUrl = null;
    } else if (watermarkUrl) {
      const sanitizedUrl = sanitizeLogoUrl(watermarkUrl);
      if (!sanitizedUrl) {
        return res.status(400).json({
          error: "Invalid image URL. Must be a valid HTTP/HTTPS URL."
        });
      }

      finalLoginWatermarkUrl = sanitizedUrl;
      localLoginWatermarkPath = null;
    }

    const saved = saveBrandingConfig({
      loginWatermarkUrl: finalLoginWatermarkUrl,
      localLoginWatermarkPath
    });

    if (!saved) {
      return res.status(500).json({ error: "Failed to save login watermark" });
    }

    invalidateBrandingCache();
    clearBrandingRemoteAssetCache("login-watermark");

    const updatedBranding = loadBrandingConfig();
    const resolvedWatermarkUrl = resolveLoginWatermarkAssetUrl(updatedBranding);
    const watermarkMode = updatedBranding.loginWatermarkUrl ? "url" : "upload";

    console.log(`[admin] Login watermark updated by ${req.session.user}`);

    return res.json({
      ok: true,
      message: "Login watermark updated successfully.",
      watermarkUrl: resolvedWatermarkUrl,
      externalUrl: updatedBranding.loginWatermarkUrl || "",
      mode: watermarkMode
    });
  } catch (err) {
    console.error("[admin] Login watermark update error:", err);
    return res.status(500).json({ error: "Failed to update login watermark" });
  }
});

function removeOldLoginBackgroundFile(filenameToRemove, replacementFilename = null) {
  if (!filenameToRemove || filenameToRemove === replacementFilename) {
    return;
  }

  const safeFilename = path.basename(String(filenameToRemove));
  if (!safeFilename || safeFilename === "." || safeFilename === "..") return;
  const targetPath = path.join(__dirname, "public", "images", safeFilename);
  try {
    if (fs.existsSync(targetPath)) {
      fs.unlinkSync(targetPath);
    }
  } catch (err) {
    console.log("[branding] Could not remove previous login background:", err.message);
  }
}

function getLoginBackgroundStoragePath(filename) {
  const safeFilename = path.basename(String(filename || ""));
  if (!safeFilename || safeFilename === "." || safeFilename === "..") {
    return null;
  }
  return path.join(__dirname, "public", "images", safeFilename);
}

function getMirroredLoginBackgroundFilename(sourceUrl, extension) {
  const hash = crypto
    .createHash("sha256")
    .update(String(sourceUrl || "").trim())
    .digest("hex")
    .slice(0, 16);
  return `login-background-remote-${hash}.${extension}`;
}

function hasStoredLoginBackground(filename) {
  const targetPath = getLoginBackgroundStoragePath(filename);
  return !!(targetPath && fs.existsSync(targetPath));
}

function validateLoginBackgroundBuffer(rawBuffer, mediaType, extension) {
  const base64 = rawBuffer.toString("base64");
  return mediaType === "video"
    ? validateBase64Video(base64, extension)
    : validateBase64Image(base64);
}

async function downloadLoginBackgroundToLocal(sourceUrl, mediaType, extension, preferredFilename = null) {
  const maxBytes = mediaType === "video" ? LOGIN_BACKGROUND_VIDEO_MAX_BYTES : LOGIN_BACKGROUND_IMAGE_MAX_BYTES;
  const rawBuffer = await httpGetRaw(sourceUrl, { maxBytes });
  const validation = validateLoginBackgroundBuffer(rawBuffer, mediaType, extension);
  if (!validation?.valid) {
    throw new Error(validation?.error || "Invalid media data");
  }

  const filename = preferredFilename || `login-background-${crypto.randomBytes(8).toString("hex")}.${extension}`;
  const targetPath = getLoginBackgroundStoragePath(filename);
  if (!targetPath) {
    throw new Error("Invalid local background filename");
  }

  fs.writeFileSync(targetPath, validation.data || rawBuffer);
  return filename;
}

async function ensureLocalLoginBackgroundMirror() {
  const branding = loadBrandingConfig();
  const sourceUrl = String(branding.loginBackgroundExternalUrl || "").trim();
  if (!sourceUrl) {
    return null;
  }

  const mediaType = branding.loginBackgroundType || getMediaTypeFromUrl(sourceUrl);
  const extension = getExtensionFromUrl(sourceUrl);
  if (!mediaType || !extension) {
    return null;
  }

  const nextFilename = getMirroredLoginBackgroundFilename(sourceUrl, extension);
  const currentFilename = branding.localLoginBackgroundPath ? path.basename(String(branding.localLoginBackgroundPath)) : null;

  if (currentFilename === nextFilename && hasStoredLoginBackground(nextFilename)) {
    return nextFilename;
  }

  if (hasStoredLoginBackground(nextFilename)) {
    const saved = saveBrandingConfig({
      loginBackgroundExternalUrl: sourceUrl,
      localLoginBackgroundPath: nextFilename,
      loginBackgroundType: mediaType,
      loginBackgroundMimeType: getMimeTypeForExtension(extension) || (mediaType === "video" ? "video/webm" : "image/webp")
    });

    if (!saved) {
      throw new Error("Failed to update local login background mirror");
    }

    removeOldLoginBackgroundFile(currentFilename, nextFilename);
    invalidateBrandingCache();
    clearBrandingRemoteAssetCache("login-background");
    return nextFilename;
  }

  const mirroredFilename = await downloadLoginBackgroundToLocal(sourceUrl, mediaType, extension, nextFilename);
  const saved = saveBrandingConfig({
    loginBackgroundExternalUrl: sourceUrl,
    localLoginBackgroundPath: mirroredFilename,
    loginBackgroundType: mediaType,
    loginBackgroundMimeType: getMimeTypeForExtension(extension) || (mediaType === "video" ? "video/webm" : "image/webp")
  });

  if (!saved) {
    removeOldLoginBackgroundFile(mirroredFilename);
    throw new Error("Failed to persist local login background mirror");
  }

  removeOldLoginBackgroundFile(currentFilename, mirroredFilename);
  invalidateBrandingCache();
  clearBrandingRemoteAssetCache("login-background");
  return mirroredFilename;
}

function maybeScheduleLoginBackgroundMirror() {
  const branding = loadBrandingConfig();
  const sourceUrl = String(branding.loginBackgroundExternalUrl || "").trim();
  if (!sourceUrl) {
    return;
  }

  const extension = getExtensionFromUrl(sourceUrl);
  const mediaType = branding.loginBackgroundType || getMediaTypeFromUrl(sourceUrl);
  if (!extension || !mediaType) {
    return;
  }

  const expectedFilename = getMirroredLoginBackgroundFilename(sourceUrl, extension);
  if (branding.localLoginBackgroundPath === expectedFilename && hasStoredLoginBackground(expectedFilename)) {
    return;
  }

  if (loginBackgroundMirrorPromise) {
    return;
  }

  loginBackgroundMirrorPromise = ensureLocalLoginBackgroundMirror()
    .catch((err) => {
      console.error("[branding] Failed to mirror login background locally:", err.message);
      return null;
    })
    .finally(() => {
      loginBackgroundMirrorPromise = null;
    });
}

setImmediate(() => {
  maybeScheduleLoginBackgroundMirror();
});

app.get("/api/admin/login-background", async (req, res) => {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ error: "Not authenticated" });
  }

  if (!(await isAdmin(req))) {
    return res.status(403).json({ error: "Admin access required" });
  }

  const branding = loadBrandingConfig();
  const backgroundUrl = resolveLoginBackgroundAssetUrl(branding);
  const backgroundMode = branding.loginBackgroundExternalUrl ? "url" : (branding.localLoginBackgroundPath ? "upload" : "upload");

  return res.json({
    backgroundUrl,
    externalUrl: branding.loginBackgroundExternalUrl || "",
    mediaType: branding.loginBackgroundType || "video",
    mimeType: branding.loginBackgroundMimeType || "video/webm",
    mode: backgroundMode
  });
});

app.post("/api/admin/login-background", async (req, res) => {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ error: "Not authenticated" });
  }

  if (!(await isAdmin(req))) {
    return res.status(403).json({ error: "Admin access required" });
  }

  const { backgroundUrl, backgroundBase64, backgroundFilename } = req.body || {};
  const currentBranding = loadBrandingConfig();

  try {
    let localLoginBackgroundPath = currentBranding.localLoginBackgroundPath || null;
    let loginBackgroundExternalUrl = currentBranding.loginBackgroundExternalUrl || null;
    let loginBackgroundType = currentBranding.loginBackgroundType || "video";
    let loginBackgroundMimeType = currentBranding.loginBackgroundMimeType || "video/webm";
    const previousLocalPath = currentBranding.localLoginBackgroundPath || null;
    let cleanupReplacementFilename = null;

    if (backgroundBase64 && backgroundFilename) {
      const mediaType = getMediaTypeFromFilename(backgroundFilename);
      if (!mediaType) {
        return res.status(400).json({
          error: "Invalid file type. Allowed images: png, jpg, jpeg, webp, ico, svg. Allowed videos: webm, mp4, ogg"
        });
      }

      let ext = null;
      let validation = null;
      if (mediaType === "image") {
        ext = getValidatedExtension(backgroundFilename);
        validation = validateBase64Image(backgroundBase64);
      } else {
        ext = getValidatedVideoExtension(backgroundFilename);
        validation = validateBase64Video(backgroundBase64, ext);
      }

      if (!ext) {
        return res.status(400).json({ error: "Could not detect the selected media type" });
      }
      if (!validation?.valid) {
        return res.status(400).json({ error: validation?.error || "Invalid media data" });
      }

      const safeFilename = `login-background-${crypto.randomBytes(8).toString("hex")}.${ext}`;
      const targetPath = path.join(__dirname, "public", "images", safeFilename);
      fs.writeFileSync(targetPath, validation.data);

      localLoginBackgroundPath = safeFilename;
      loginBackgroundExternalUrl = null;
      loginBackgroundType = mediaType;
      loginBackgroundMimeType = getMimeTypeForExtension(ext) || (mediaType === "video" ? "video/webm" : "image/webp");
      cleanupReplacementFilename = safeFilename;
    } else if (backgroundUrl) {
      const sanitizedUrl = sanitizeLogoUrl(backgroundUrl);
      if (!sanitizedUrl) {
        return res.status(400).json({
          error: "Invalid media URL. Must be a valid HTTP/HTTPS URL."
        });
      }

      const mediaType = getMediaTypeFromUrl(sanitizedUrl);
      const ext = getExtensionFromUrl(sanitizedUrl);
      if (!mediaType || !ext) {
        return res.status(400).json({
          error: "URL must point directly to an image or video file."
        });
      }

      try {
        cleanupReplacementFilename = await downloadLoginBackgroundToLocal(
          sanitizedUrl,
          mediaType,
          ext,
          getMirroredLoginBackgroundFilename(sanitizedUrl, ext)
        );
      } catch (err) {
        return res.status(400).json({
          error: `Could not download that media URL: ${err.message}`
        });
      }

      loginBackgroundExternalUrl = sanitizedUrl;
      localLoginBackgroundPath = cleanupReplacementFilename;
      loginBackgroundType = mediaType;
      loginBackgroundMimeType = getMimeTypeForExtension(ext) || (mediaType === "video" ? "video/webm" : "image/webp");
    } else {
      return res.status(400).json({ error: "Choose an image or video first" });
    }

    const saved = saveBrandingConfig({
      loginBackgroundType,
      loginBackgroundExternalUrl,
      localLoginBackgroundPath,
      loginBackgroundMimeType
    });

    if (!saved) {
      removeOldLoginBackgroundFile(cleanupReplacementFilename, previousLocalPath);
      return res.status(500).json({ error: "Failed to save login background" });
    }

    removeOldLoginBackgroundFile(previousLocalPath, cleanupReplacementFilename);
    invalidateBrandingCache();
    clearBrandingRemoteAssetCache("login-background");

    const updatedBranding = loadBrandingConfig();
    const resolvedBackgroundUrl = resolveLoginBackgroundAssetUrl(updatedBranding);
    const backgroundMode = updatedBranding.loginBackgroundExternalUrl ? "url" : (updatedBranding.localLoginBackgroundPath ? "upload" : "upload");

    console.log(`[admin] Login background updated by ${req.session.user}`);

    return res.json({
      ok: true,
      message: "Login background updated successfully.",
      backgroundUrl: resolvedBackgroundUrl,
      externalUrl: updatedBranding.loginBackgroundExternalUrl || "",
      mediaType: updatedBranding.loginBackgroundType || loginBackgroundType,
      mimeType: updatedBranding.loginBackgroundMimeType || loginBackgroundMimeType,
      mode: backgroundMode
    });
  } catch (err) {
    console.error("[admin] Login background update error:", err);
    return res.status(500).json({ error: "Failed to update login background" });
  }
});

app.post("/api/account/recovery/verify", recoveryRateLimiter, async (req, res) => {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ error: "Not authenticated" });
  }

  const user = await findUserByEmail(req.session.user);
  if (!user) {
    return res.status(401).json({ error: "User not found" });
  }

  const { recoveryCode } = req.body || {};

  if (!recoveryCode || typeof recoveryCode !== 'string') {
    return res.status(400).json({ error: "Recovery code is required" });
  }

  const codeStr = recoveryCode.trim().toUpperCase();
  if (!/^[A-Z0-9]{8}$/.test(codeStr)) {
    return res.status(400).json({ error: "Invalid recovery code format" });
  }

  let recoveryCodes = [];
  try {
    const rows = await db.query(
      "SELECT id, recovery_codes FROM users WHERE LOWER(email) = LOWER(?) LIMIT 1",
      [req.session.user]
    );
    const row = rows[0];
    if (row && row.recovery_codes) {
      recoveryCodes = JSON.parse(row.recovery_codes);
    }
  } catch (err) {
    console.error("[account] Failed to get recovery codes:", err);
    return res.status(500).json({ error: "Failed to verify recovery code" });
  }

  if (!Array.isArray(recoveryCodes) || recoveryCodes.length === 0) {
    return res.status(403).json({ error: "No recovery codes available" });
  }

  let matchedIndex = -1;
  for (let i = 0; i < recoveryCodes.length; i++) {
    try {
      const isMatch = await bcrypt.compare(codeStr, recoveryCodes[i]);
      if (isMatch && matchedIndex === -1) {
        matchedIndex = i;
      }
    } catch (err) {
    }
  }

  if (matchedIndex === -1) {
    console.log(`[SECURITY] Recovery code verification failed | User: ${req.session.user} | IP: ${getRequestIp(req)}`);
    return res.status(403).json({ error: "Invalid recovery code" });
  }

  try {
    recoveryCodes.splice(matchedIndex, 1);
    await db.query(
      "UPDATE users SET recovery_codes = ? WHERE LOWER(email) = LOWER(?)",
      [JSON.stringify(recoveryCodes), req.session.user]
    );
  } catch (err) {
    console.error("[account] Failed to remove used recovery code:", err);
  }

  req.session.recoveryVerified = {
    userId: user.id,
    email: req.session.user,
    nonce: crypto.randomBytes(16).toString('hex'),
    verifiedAt: Date.now()
  };

  console.log(`[SECURITY] Recovery code verified successfully | User: ${req.session.user} | IP: ${getRequestIp(req)} | RemainingCodes: ${recoveryCodes.length}`);

  return res.json({ ok: true, verified: true, remainingCodes: recoveryCodes.length });
});

app.post("/api/account/2fa/generate-recovery", recoveryRateLimiter, async (req, res) => {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ error: "Not authenticated" });
  }

  const user = await findUserByEmail(req.session.user);
  if (!user) {
    return res.status(401).json({ error: "User not found" });
  }

  const RECOVERY_EXPIRY_MS = 5 * 60 * 1000;
  const rv = req.session.recoveryVerified;

  if (!rv) {
    console.log(`[SECURITY] Recovery 2FA generation denied - no verification | User: ${req.session.user} | IP: ${getRequestIp(req)}`);
    return res.status(403).json({ error: "Recovery verification required" });
  }

  if (rv.email !== req.session.user) {
    delete req.session.recoveryVerified;
    console.log(`[SECURITY] Recovery 2FA generation denied - user mismatch | User: ${req.session.user} | IP: ${getRequestIp(req)}`);
    return res.status(403).json({ error: "Recovery verification required" });
  }

  if (Date.now() - rv.verifiedAt > RECOVERY_EXPIRY_MS) {
    delete req.session.recoveryVerified;
    console.log(`[SECURITY] Recovery 2FA generation denied - expired | User: ${req.session.user} | IP: ${getRequestIp(req)}`);
    return res.status(403).json({ error: "Recovery verification expired. Please verify a recovery code again." });
  }

  try {
    const newSecret = speakeasy.generateSecret({ length: 20 });
    const appName = process.env.APP_NAME || "ADPanel";
    const otpauthUrl = speakeasy.otpauthURL({
      secret: newSecret.base32,
      label: user.email,
      issuer: appName,
      encoding: "base32",
    });

    const qrcode = require("qrcode");
    const qrCodeUrl = await qrcode.toDataURL(otpauthUrl);

    req.session.pending2faSecret = newSecret.base32;
    req.session.pending2faFrom = 'recovery';

    delete req.session.recoveryVerified;

    console.log(`[SECURITY] Recovery 2FA secret generated | User: ${req.session.user} | IP: ${getRequestIp(req)}`);

    return res.json({
      ok: true,
      newSecret: newSecret.base32,
      qrCodeUrl: qrCodeUrl,
    });
  } catch (err) {
    console.error("[account] 2FA generation error:", err);
    return res.status(500).json({ error: "Failed to generate new 2FA" });
  }
});

app.post("/api/account/password/verify", async (req, res) => {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ error: "Not authenticated" });
  }

  const user = await findUserByEmail(req.session.user);
  if (!user) {
    return res.status(401).json({ error: "User not found" });
  }

  const { currentPassword, twoFactorCode } = req.body || {};

  if (!currentPassword || typeof currentPassword !== 'string') {
    return res.status(400).json({ error: "Current password is required" });
  }

  try {
    if (process.env.DEBUG_AUTH === "true") {
      console.log("[account] Password verify attempt for:", req.session.user);
    }

    if (!user.password) {
      console.error("[account] User has no password hash in database");
      return res.status(500).json({ error: "Account configuration error" });
    }

    const isValid = await withTimeout(bcrypt.compare(currentPassword, user.password), 8000, "bcrypt-timeout");

    if (!isValid) {
      return res.status(403).json({ error: "Incorrect current password" });
    }
  } catch (err) {
    console.error("[account] Password verify error:", err);
    return res.status(500).json({ error: "Verification failed" });
  }

  if (user.secret) {
    if (!twoFactorCode || typeof twoFactorCode !== 'string') {
      return res.status(400).json({ error: "2FA code is required" });
    }

    const codeStr = twoFactorCode.replace(/\s+/g, "");
    if (!/^\d{6}$/.test(codeStr)) {
      return res.status(400).json({ error: "Invalid 2FA code format" });
    }

    const verified = speakeasy.totp.verify({
      secret: user.secret,
      encoding: "base32",
      token: codeStr,
      window: 1,
    });

    if (!verified) {
      return res.status(403).json({ error: "Incorrect 2FA code" });
    }
  }

  return res.json({ ok: true, verified: true });
});

app.post("/api/account/password/change", async (req, res) => {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ error: "Not authenticated" });
  }

  const user = await findUserByEmail(req.session.user);
  if (!user) {
    return res.status(401).json({ error: "User not found" });
  }

  const { currentPassword, twoFactorCode, newPassword } = req.body || {};

  if (!currentPassword || typeof currentPassword !== 'string') {
    return res.status(400).json({ error: "Current password is required" });
  }
  if (!newPassword || typeof newPassword !== 'string') {
    return res.status(400).json({ error: "New password is required" });
  }
  if (newPassword.length < 8) {
    return res.status(400).json({ error: "Password must be at least 8 characters" });
  }

  try {
    const isValid = await withTimeout(bcrypt.compare(currentPassword, user.password), 8000, "bcrypt-timeout");
    if (!isValid) {
      return res.status(403).json({ error: "Incorrect current password" });
    }
  } catch (err) {
    console.error("[account] Password change verify error:", err);
    return res.status(500).json({ error: "Verification failed" });
  }

  if (user.secret) {
    if (!twoFactorCode || typeof twoFactorCode !== 'string') {
      return res.status(400).json({ error: "2FA code is required" });
    }

    const codeStr = twoFactorCode.replace(/\s+/g, "");
    if (!/^\d{6}$/.test(codeStr)) {
      return res.status(400).json({ error: "Invalid 2FA code format" });
    }

    const verified = speakeasy.totp.verify({
      secret: user.secret,
      encoding: "base32",
      token: codeStr,
      window: 1,
    });

    if (!verified) {
      return res.status(403).json({ error: "Incorrect 2FA code" });
    }
  }

  try {
    const hashedPassword = await bcrypt.hash(newPassword, BCRYPT_ROUNDS);

    await db.query(
      "UPDATE users SET password = ? WHERE LOWER(email) = LOWER(?)",
      [hashedPassword, req.session.user]
    );

    setRememberLoginCookie(req, res, { ...user, password: hashedPassword });
    return res.json({ ok: true });
  } catch (err) {
    console.error("[account] Password update error:", err);
    return res.status(500).json({ error: "Failed to update password" });
  }
});

app.post("/api/account/email/verify", async (req, res) => {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ error: "Not authenticated" });
  }

  const user = await findUserByEmail(req.session.user);
  if (!user) {
    return res.status(401).json({ error: "User not found" });
  }

  const { currentPassword, twoFactorCode } = req.body || {};

  if (!currentPassword || typeof currentPassword !== 'string') {
    return res.status(400).json({ error: "Password is required" });
  }

  try {
    if (!user.password) {
      return res.status(500).json({ error: "Account configuration error" });
    }

    const isValid = await withTimeout(bcrypt.compare(currentPassword, user.password), 8000, "bcrypt-timeout");
    if (!isValid) {
      return res.status(403).json({ error: "Incorrect password" });
    }
  } catch (err) {
    console.error("[account] Email verify error:", err);
    return res.status(500).json({ error: "Verification failed" });
  }

  if (user.secret) {
    if (!twoFactorCode || typeof twoFactorCode !== 'string') {
      return res.status(400).json({ error: "2FA code is required" });
    }

    const codeStr = twoFactorCode.replace(/\s+/g, "");
    if (!/^\d{6}$/.test(codeStr)) {
      return res.status(400).json({ error: "Invalid 2FA code format" });
    }

    const verified = speakeasy.totp.verify({
      secret: user.secret,
      encoding: "base32",
      token: codeStr,
      window: 1,
    });

    if (!verified) {
      return res.status(403).json({ error: "Incorrect 2FA code" });
    }
  }

  return res.json({ ok: true, verified: true });
});

app.post("/api/account/email/change", async (req, res) => {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ error: "Not authenticated" });
  }

  const user = await findUserByEmail(req.session.user);
  if (!user) {
    return res.status(401).json({ error: "User not found" });
  }

  const { currentPassword, twoFactorCode, newEmail } = req.body || {};

  if (!currentPassword || typeof currentPassword !== 'string') {
    return res.status(400).json({ error: "Password is required" });
  }
  if (!newEmail || typeof newEmail !== 'string') {
    return res.status(400).json({ error: "New email is required" });
  }

  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(newEmail)) {
    return res.status(400).json({ error: "Invalid email format" });
  }

  const existingUser = await findUserByEmail(newEmail);
  if (existingUser && existingUser.id !== user.id) {
    return res.status(400).json({ error: "Email already in use" });
  }

  try {
    if (!user.password) {
      return res.status(500).json({ error: "Account configuration error" });
    }

    const isValid = await withTimeout(bcrypt.compare(currentPassword, user.password), 8000, "bcrypt-timeout");
    if (!isValid) {
      return res.status(403).json({ error: "Incorrect password" });
    }
  } catch (err) {
    console.error("[account] Email change verify error:", err);
    return res.status(500).json({ error: "Verification failed" });
  }

  if (user.secret) {
    if (!twoFactorCode || typeof twoFactorCode !== 'string') {
      return res.status(400).json({ error: "2FA code is required" });
    }

    const codeStr = twoFactorCode.replace(/\s+/g, "");
    if (!/^\d{6}$/.test(codeStr)) {
      return res.status(400).json({ error: "Invalid 2FA code format" });
    }

    const verified = speakeasy.totp.verify({
      secret: user.secret,
      encoding: "base32",
      token: codeStr,
      window: 1,
    });

    if (!verified) {
      return res.status(403).json({ error: "Incorrect 2FA code" });
    }
  }

  try {
    await db.query(
      "UPDATE users SET email = ? WHERE LOWER(email) = LOWER(?)",
      [newEmail.toLowerCase(), req.session.user]
    );

    req.session.user = newEmail.toLowerCase();
    setRememberLoginCookie(req, res, { ...user, email: newEmail.toLowerCase() });

    return res.json({ ok: true });
  } catch (err) {
    console.error("[account] Email update error:", err);
    return res.status(500).json({ error: "Failed to update email" });
  }
});

app.get("/settings/servers", async (req, res) => {
  if (!(await isAdmin(req))) return res.redirect("/");
  const user = await findUserByEmail(req.session.user);
  res.render("server", { user });
});

let USER_COUNT_CACHE = 0;
async function loadUserCount() {
  try {
    const rows = await db.query("SELECT COUNT(*) AS count FROM users");
    const count = rows && rows[0] ? Number(rows[0].count) : 0;
    return Number.isFinite(count) ? count : 0;
  } catch {
    return USER_COUNT_CACHE;
  }
}
async function refreshUserCount() {
  USER_COUNT_CACHE = await loadUserCount();
}
refreshUserCount().catch(() => { });
setInterval(() => {
  refreshUserCount().catch(() => { });
}, 10_000);
app.get("/api/usercount", (req, res) => res.json({ userCount: USER_COUNT_CACHE }));

app.get("/api/me", async (req, res, next) => {
  try {
    const email = req.session?.user;
    if (!email) return res.status(401).json({ ok: false, user: null });
    const user = await findUserByEmail(email);
    if (!user) return res.status(401).json({ ok: false, user: null });
    return res.json({
      ok: true,
      user: {
        email: user.email,
        admin: !!user.admin,
        has2FA: !!user.secret,
        preferences: user.preferences || {}
      }
    });
  } catch (err) {
    return next(err);
  }
});

app.post("/api/me/preferences", async (req, res, next) => {
  try {
    const email = req.session?.user;
    if (!email) return res.status(401).json({ error: "Unauthorized" });
    const { preferences } = req.body;
    if (!preferences || typeof preferences !== 'object') {
      return res.status(400).json({ error: "Invalid preferences" });
    }

    const user = await findUserByEmail(email);
    if (!user) return res.status(404).json({ error: "User not found" });

    const currentPrefs = user.preferences || {};
    const newPrefs = { ...currentPrefs, ...preferences };

    await db.query("UPDATE users SET preferences = ? WHERE id = ?", [JSON.stringify(newPrefs), user.id]);

    return res.json({ ok: true, preferences: newPrefs });
  } catch (err) {
    next(err);
  }
});


app.get("/api/me/status", async (req, res, next) => {
  try {
    const email = req.session?.user;
    if (!email) return res.status(401).json({ ok: false, error: "not authenticated" });
    const user = await findUserByEmail(email);
    if (!user) return res.status(401).json({ ok: false, error: "user not found" });

    const rows = await db.query(
      "SELECT status_text, expires_at FROM user_status WHERE user_id = ?",
      [user.id]
    );

    if (!rows || rows.length === 0) {
      return res.json({ ok: true, status: { text: "Available", expiresAt: null } });
    }

    const row = rows[0];
    const now = Date.now();

    if (row.expires_at && Number(row.expires_at) <= now) {
      await db.query("DELETE FROM user_status WHERE user_id = ?", [user.id]);
      return res.json({ ok: true, status: { text: "Available", expiresAt: null } });
    }

    return res.json({
      ok: true,
      status: {
        text: row.status_text || "Available",
        expiresAt: row.expires_at ? Number(row.expires_at) : null
      }
    });
  } catch (err) {
    return next(err);
  }
});

app.post("/api/me/status", async (req, res, next) => {
  try {
    const email = req.session?.user;
    if (!email) return res.status(401).json({ ok: false, error: "not authenticated" });
    const user = await findUserByEmail(email);
    if (!user) return res.status(401).json({ ok: false, error: "user not found" });

    const { text, expiresAt } = req.body || {};
    const statusText = String(text || "Available").slice(0, 80);
    const expiresAtNum = expiresAt ? Number(expiresAt) : null;

    await db.query(
      `INSERT INTO user_status (user_id, status_text, expires_at)
       VALUES (?, ?, ?)
       ON DUPLICATE KEY UPDATE status_text = VALUES(status_text), expires_at = VALUES(expires_at)`,
      [user.id, statusText, expiresAtNum]
    );

    return res.json({ ok: true });
  } catch (err) {
    return next(err);
  }
});


async function deleteServerSchedules(serverName) {
  if (!schedulerQueue) return;

  try {
    const [waiting, active, delayed] = await Promise.all([
      schedulerQueue.getJobs(["waiting"], 0, 1000),
      schedulerQueue.getJobs(["active"], 0, 1000),
      schedulerQueue.getJobs(["delayed"], 0, 1000)
    ]);

    const allJobs = [...waiting, ...active, ...delayed];
    let deletedCount = 0;

    for (const job of allJobs) {
      if (job?.data?.serverName === serverName) {
        try {
          await job.remove();
          deletedCount++;
        } catch (e) {
          console.log(`[deleteServerSchedules] Could not remove job ${job.id}: ${e.message}`);
        }
      }
    }

    const repeatableJobs = await schedulerQueue.getRepeatableJobs();
    for (const job of repeatableJobs) {
      try {
        const nameData = JSON.parse(job.name || "{}");
        if (nameData.serverName === serverName) {
          await schedulerQueue.removeRepeatableByKey(job.key);
          deletedCount++;
        }
      } catch (e) {
        console.log(`[deleteServerSchedules] Could not parse/remove repeatable job: ${e.message}`);
      }
    }

    if (deletedCount > 0) {
      console.log(`[deleteServerSchedules] Deleted ${deletedCount} schedules for server ${serverName}`);
    }
  } catch (err) {
    console.error(`[deleteServerSchedules] Error deleting schedules for ${serverName}:`, err);
  }
}

app.get("/api/scheduler/:serverName/tasks", async (req, res) => {
  try {
    if (!(await isAuthenticated(req))) {
      return res.status(401).json({ ok: false, error: "not authenticated" });
    }

    const serverName = sanitizeServerName(req.params.serverName);
    if (!serverName) {
      return res.status(400).json({ ok: false, error: "invalid server name" });
    }

    const email = req.session?.user;
    if (!email) return res.status(401).json({ ok: false, error: "not authenticated" });

    const hasAccess = await userHasAccessToServer(email, serverName);
    if (!hasAccess) {
      return res.status(403).json({ ok: false, error: "access denied" });
    }

    const perms = await getEffectivePermsForUserOnServer(email, serverName);
    if (!perms.scheduler_access) {
      return res.status(403).json({ ok: false, error: "no scheduler access permission" });
    }

    if (!schedulerQueue) {
      return res.status(503).json({ ok: false, error: "scheduler not available", detail: "Redis not configured" });
    }

    const [waiting, active, delayed, completed, failed] = await Promise.all([
      schedulerQueue.getJobs(["waiting"], 0, 100),
      schedulerQueue.getJobs(["active"], 0, 50),
      schedulerQueue.getJobs(["delayed"], 0, 100),
      schedulerQueue.getJobs(["completed"], 0, 50),
      schedulerQueue.getJobs(["failed"], 0, 50)
    ]);

    const allJobs = [...waiting, ...active, ...delayed, ...completed, ...failed];
    const serverJobs = allJobs.filter(job => job?.data?.serverName === serverName);

    const repeatableJobs = await schedulerQueue.getRepeatableJobs();
    const serverRepeatables = repeatableJobs.filter(job => {
      try {
        const nameData = JSON.parse(job.name || "{}");
        return nameData.serverName === serverName;
      } catch {
        return false;
      }
    });

    const tasks = serverJobs.map(job => ({
      id: job.id,
      name: job.data?.name || "Unnamed Task",
      actionType: job.data?.actionType,
      payload: job.data?.payload,
      scheduleType: job.data?.scheduleType,
      scheduledFor: job.timestamp + (job.delay || 0),
      status: job.finishedOn ? (job.failedReason ? "failed" : "completed") : (job.processedOn ? "running" : "pending"),
      createdAt: job.timestamp,
      failedReason: job.failedReason || null,
      returnValue: job.returnvalue || null
    }));

    const recurring = serverRepeatables.map(job => {
      try {
        const nameData = JSON.parse(job.name || "{}");
        return {
          id: job.key,
          name: nameData.name || "Recurring Task",
          actionType: nameData.actionType,
          payload: nameData.payload,
          scheduleType: nameData.scheduleType,
          cron: job.pattern,
          next: job.next,
          status: "recurring"
        };
      } catch {
        return null;
      }
    }).filter(Boolean);

    return res.json({ ok: true, tasks, recurring });
  } catch (err) {
    console.error("[scheduler] List tasks error:", err);
    return res.status(500).json({ ok: false, error: "internal error" });
  }
});

app.post("/api/scheduler/:serverName/tasks", async (req, res) => {
  try {
    if (!(await isAuthenticated(req))) {
      return res.status(401).json({ ok: false, error: "not authenticated" });
    }

    const serverName = sanitizeServerName(req.params.serverName);
    if (!serverName) {
      return res.status(400).json({ ok: false, error: "invalid server name" });
    }

    const email = req.session?.user;
    if (!email) return res.status(401).json({ ok: false, error: "not authenticated" });

    const hasAccess = await userHasAccessToServer(email, serverName);
    if (!hasAccess) {
      return res.status(403).json({ ok: false, error: "access denied" });
    }

    const perms = await getEffectivePermsForUserOnServer(email, serverName);
    if (!perms.scheduler_create) {
      return res.status(403).json({ ok: false, error: "no permission to create schedules" });
    }

    if (!schedulerQueue) {
      return res.status(503).json({ ok: false, error: "scheduler not available", detail: "Redis not configured" });
    }

    const serverEntry = await findServer(serverName);
    const maxSchedules = serverEntry?.resources?.maxSchedules || serverEntry?.docker?.maxSchedules;
    if (maxSchedules && maxSchedules > 0) {
      const [waiting, active, delayed] = await Promise.all([
        schedulerQueue.getJobs(["waiting"], 0, 1000),
        schedulerQueue.getJobs(["active"], 0, 1000),
        schedulerQueue.getJobs(["delayed"], 0, 1000)
      ]);
      const allActiveJobs = [...waiting, ...active, ...delayed];
      const serverSchedules = allActiveJobs.filter(job => job?.data?.serverName === serverName).length;

      const repeatableJobs = await schedulerQueue.getRepeatableJobs();
      const serverRecurring = repeatableJobs.filter(job => {
        try {
          const nameData = JSON.parse(job.name || "{}");
          return nameData.serverName === serverName;
        } catch {
          return false;
        }
      }).length;

      const totalSchedules = serverSchedules + serverRecurring;
      if (totalSchedules >= maxSchedules) {
        return res.status(400).json({
          ok: false,
          error: `Maximum schedule limit reached (${maxSchedules})`,
          current: totalSchedules,
          max: maxSchedules
        });
      }
    }

    const { name, actionType, payload, scheduleType, scheduleValue, scheduleTime, scheduledTimestamp } = req.body || {};

    if (!actionType) {
      return res.status(400).json({ ok: false, error: "actionType is required" });
    }

    if (!scheduleType) {
      return res.status(400).json({ ok: false, error: "scheduleType is required" });
    }

    const validActions = ["console_command", "server_start", "server_stop", "create_file", "modify_file", "backup"];
    if (!validActions.includes(actionType)) {
      return res.status(400).json({ ok: false, error: "invalid actionType" });
    }

    const validScheduleTypes = ["once", "seconds", "minutes", "hourly", "daily", "weekly"];
    if (!validScheduleTypes.includes(scheduleType)) {
      return res.status(400).json({ ok: false, error: "invalid scheduleType" });
    }

    if (actionType === "console_command" && !payload?.command) {
      return res.status(400).json({ ok: false, error: "command is required for console_command action" });
    }
    if ((actionType === "create_file" || actionType === "modify_file") && !payload?.filePath) {
      return res.status(400).json({ ok: false, error: "filePath is required for file operations" });
    }

    const user = await findUserByEmail(email);
    const jobData = {
      name: name || `${actionType} task`,
      serverName,
      actionType,
      payload: payload || {},
      scheduleType,
      scheduleValue,
      scheduleTime,
      userId: user?.id,
      userName: email
    };

    let job;

    if (scheduleType === "once") {
      const runAt = scheduledTimestamp ? new Date(scheduledTimestamp).getTime() : Date.now();
      const delay = Math.max(0, runAt - Date.now());

      job = await schedulerQueue.add("scheduled-task", jobData, {
        delay,
        removeOnComplete: { count: 50 },
        removeOnFail: { count: 25 }
      });

      console.log(`[scheduler] Created one-time task ${job.id} for ${serverName}, runs in ${delay}ms`);
    } else {
      const schedule = scheduleToCron(scheduleType, scheduleValue, scheduleTime);

      const repeatJobName = JSON.stringify({
        serverName,
        name: name || `${actionType} task`,
        actionType,
        payload,
        scheduleType
      });

      if (schedule && typeof schedule === "object" && schedule.type === "interval") {
        job = await schedulerQueue.add(repeatJobName, jobData, {
          repeat: { every: schedule.ms },
          removeOnComplete: { count: 20 },
          removeOnFail: { count: 10 }
        });
        console.log(`[scheduler] Created interval task for ${serverName}, every ${schedule.ms}ms`);
      } else if (schedule) {
        job = await schedulerQueue.add(repeatJobName, jobData, {
          repeat: { pattern: schedule },
          removeOnComplete: { count: 20 },
          removeOnFail: { count: 10 }
        });
        console.log(`[scheduler] Created recurring task for ${serverName}, cron: ${schedule}`);
      } else {
        return res.status(400).json({ ok: false, error: "invalid schedule configuration" });
      }
    }

    const userIp = getRequestIp(req);
    recordActivity(serverName, "scheduler_create", {
      taskId: job.id,
      taskName: jobData.name,
      actionType,
      scheduleType,
      scheduleValue,
      scheduleTime
    }, email, userIp);

    return res.json({
      ok: true,
      task: {
        id: job.id,
        name: jobData.name,
        actionType,
        scheduleType,
        createdAt: Date.now()
      }
    });
  } catch (err) {
    console.error("[scheduler] Create task error:", err);
    return res.status(500).json({ ok: false, error: "internal error", detail: err?.message });
  }
});

app.delete("/api/scheduler/:serverName/tasks/:taskId", async (req, res) => {
  try {
    if (!(await isAuthenticated(req))) {
      return res.status(401).json({ ok: false, error: "not authenticated" });
    }

    const serverName = sanitizeServerName(req.params.serverName);
    if (!serverName) {
      return res.status(400).json({ ok: false, error: "invalid server name" });
    }

    const email = req.session?.user;
    if (!email) return res.status(401).json({ ok: false, error: "not authenticated" });

    const hasAccess = await userHasAccessToServer(email, serverName);
    if (!hasAccess) {
      return res.status(403).json({ ok: false, error: "access denied" });
    }

    const perms = await getEffectivePermsForUserOnServer(email, serverName);
    if (!perms.scheduler_delete) {
      return res.status(403).json({ ok: false, error: "no permission to delete schedules" });
    }

    if (!schedulerQueue) {
      return res.status(503).json({ ok: false, error: "scheduler not available" });
    }

    const taskId = req.params.taskId;
    const userIp = getRequestIp(req);

    const job = await schedulerQueue.getJob(taskId);
    if (job && job.data?.serverName === serverName) {
      const taskName = job.data?.name || "Unknown task";
      const actionType = job.data?.actionType || "unknown";
      await job.remove();
      console.log(`[scheduler] Removed task ${taskId}`);

      recordActivity(serverName, "scheduler_delete", {
        taskId,
        taskName,
        actionType
      }, email, userIp);

      return res.json({ ok: true });
    }

    const repeatables = await schedulerQueue.getRepeatableJobs();
    const repeatable = repeatables.find(r => r.key === taskId);
    if (repeatable) {
      try {
        const nameData = JSON.parse(repeatable.name || "{}");
        if (nameData.serverName === serverName) {
          await schedulerQueue.removeRepeatableByKey(taskId);
          console.log(`[scheduler] Removed recurring task ${taskId}`);

          recordActivity(serverName, "scheduler_delete", {
            taskId,
            taskName: nameData.name || "Recurring task",
            actionType: nameData.actionType || "unknown",
            recurring: true
          }, email, userIp);

          return res.json({ ok: true });
        }
      } catch { }
    }

    return res.status(404).json({ ok: false, error: "task not found" });
  } catch (err) {
    console.error("[scheduler] Delete task error:", err);
    return res.status(500).json({ ok: false, error: "internal error" });
  }
});

app.get("/api/scheduler/status", async (req, res) => {
  try {
    if (!(await isAuthenticated(req))) {
      return res.status(401).json({ ok: false, error: "not authenticated" });
    }

    const available = !!schedulerQueue;
    let queueInfo = null;

    if (available) {
      try {
        const [waiting, active, delayed, completed, failed] = await Promise.all([
          schedulerQueue.getWaitingCount(),
          schedulerQueue.getActiveCount(),
          schedulerQueue.getDelayedCount(),
          schedulerQueue.getCompletedCount(),
          schedulerQueue.getFailedCount()
        ]);

        queueInfo = { waiting, active, delayed, completed, failed };
      } catch (qErr) {
        console.error("[scheduler] Queue info error:", qErr?.message);
      }
    }

    return res.json({
      ok: true,
      available,
      redisConfigured: !!SCHEDULER_REDIS_URL,
      queue: queueInfo
    });
  } catch (err) {
    console.error("[scheduler] Status error:", err);
    return res.status(500).json({ ok: false, error: "internal error" });
  }
});

app.get("/api/server/:name/node-status", async (req, res, next) => {
  try {
    if (!(await isAuthenticated(req))) {
      return res.status(401).json({ error: "not authenticated" });
    }
    const serverName = sanitizeServerName(req.params.name);
    if (!serverName) {
      return res.status(400).json({ error: "invalid server name" });
    }

    const email = req.session?.user;
    if (!email) return res.status(401).json({ error: "not authenticated" });

    const hasAccess = await userHasAccessToServer(email, serverName);
    if (!hasAccess) {
      return res.status(403).json({ error: "access denied" });
    }

    const serverIndex = await loadServersIndex();
    const entry = serverIndex.find(e => e && e.name && e.name.toLowerCase() === serverName.toLowerCase());
    if (!entry) {
      return res.json({ ok: false, nodeOnline: false, status: "unknown" });
    }

    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, private");

    const cachedStatus = await getCachedServerStatusLenient(entry.name);

    let nodeStats = null;
    if (isRemoteEntry(entry)) {
      const node = await findNodeByIdOrName(entry.nodeId);
      if (node) {
        const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
        if (baseUrl) {
          const headers = nodeAuthHeadersFor(node, true);
          try {
            const { status, json } = await httpRequestJson(
              `${baseUrl}/v1/servers/${encodeURIComponent(serverName)}`,
              "GET",
              headers,
              null,
              5000
            );
            if (status === 200 && json && json.ok) {
              nodeStats = json;
            }
          } catch { }
        }
      }
    }

    let result;
    if (nodeStats) {
      const nodeStatusLabel =
        normalizeStatusLabel(nodeStats.status || nodeStats.state) ||
        normalizeStatusLabel(cachedStatus?.status) ||
        'unknown';

      if (nodeStats.stats) {
        const stats = nodeStats.stats;
        result = normalizeServerStatusRecord({
          ok: true,
          status: nodeStatusLabel,
          nodeOnline: true,
          cpu: stats.cpu?.percent ?? cachedStatus?.cpu ?? null,
          cpuLimit: stats.cpu?.limit ?? cachedStatus?.cpuLimit ?? null,
          memory: {
            used: stats.memory?.usedMb ?? 0,
            total: stats.memory?.limitMb ?? 1,
            percent: stats.memory?.percent ?? 0
          },
          disk: {
            used: (stats.disk?.usedMb ?? 0) / 1024,
            total: (stats.disk?.limitMb ?? 0) / 1024,
            percent: stats.disk?.percent ?? 0
          },
          uptime: nodeStats.uptime ?? cachedStatus?.uptime ?? null
        });
      } else {
        result = normalizeServerStatusRecord({
          ok: true,
          status: nodeStatusLabel,
          nodeOnline: true,
          cpu: cachedStatus?.cpu ?? null,
          cpuLimit: cachedStatus?.cpuLimit ?? null,
          memory: cachedStatus?.memory ?? null,
          disk: cachedStatus?.disk ?? null,
          uptime: nodeStats.uptime ?? cachedStatus?.uptime ?? null
        });
      }
    } else {
      let nodeOnline = false;
      let serverStatus = normalizeStatusLabel(cachedStatus?.status) || 'unknown';

      if (isRemoteEntry(entry)) {
        const node = await findNodeByIdOrName(entry.nodeId);
        if (node) {
          const cachedNode = await getCachedNodeStatus(node.uuid || node.id || node.name);
          nodeOnline = cachedNode?.online ?? false;
        }
      } else {
        nodeOnline = true;
      }

      result = normalizeServerStatusRecord({
        ok: true,
        status: serverStatus,
        nodeOnline: nodeOnline,
        cpu: cachedStatus?.cpu ?? null,
        cpuLimit: cachedStatus?.cpuLimit ?? null,
        memory: cachedStatus?.memory ?? null,
        disk: cachedStatus?.disk ?? null,
        uptime: cachedStatus?.uptime ?? null
      });
    }

    return res.json(result);
  } catch (err) {
    console.error("[node-status] Error:", err);
    return res.status(500).json({ error: "internal error" });
  }
});

const ACTIVITY_LOGS_DIR = path.join(__dirname, ".activity-logs");

app.get("/api/server/:name/activity", async (req, res) => {
  try {
    if (!(await isAuthenticated(req))) {
      return res.status(401).json({ error: "not authenticated" });
    }

    const serverName = sanitizeServerName(req.params.name);
    if (!serverName) {
      return res.status(400).json({ error: "invalid server name" });
    }

    const email = req.session?.user;
    if (!email) return res.status(401).json({ error: "not authenticated" });

    const isAdminUser = await isAdmin(req);
    if (!isAdminUser) {
      const hasAccess = await userHasAccessToServer(email, serverName);
      if (!hasAccess) {
        return res.status(403).json({ error: "access denied" });
      }
      const perms = await getEffectivePermsForUserOnServer(email, serverName);
      if (!perms || !perms.activity_logs) {
        return res.status(403).json({ error: "access denied" });
      }
    }

    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, private");

    const offset = Math.max(0, parseInt(req.query.offset, 10) || 0);
    const limit = Math.min(200, Math.max(1, parseInt(req.query.limit, 10) || 50));

    const safeName = String(serverName).replace(/[^a-zA-Z0-9_-]/g, "_");
    const logPath = path.join(ACTIVITY_LOGS_DIR, `${safeName}.log`);

    let entries = [];
    try {
      const st = await fsp.stat(logPath).catch(() => null);
      if (st && st.size > 10 * 1024 * 1024) {
        return res.status(413).json({ error: "activity log too large; rotate logs" });
      }
      const content = await fsp.readFile(logPath, "utf8");
      const lines = content.trim().split("\n").filter(Boolean);

      for (let i = lines.length - 1; i >= 0; i--) {
        try {
          const entry = JSON.parse(lines[i]);
          entries.push(entry);
        } catch { }
      }
    } catch (err) {
      if (err.code !== "ENOENT") {
        console.error("[activity] Error reading log:", err);
      }
    }

    const paginated = entries.slice(offset, offset + limit);

    return res.json({
      ok: true,
      entries: paginated,
      total: entries.length,
      offset,
      limit
    });
  } catch (err) {
    console.error("[activity] Error:", err);
    return res.status(500).json({ error: "failed to load activity logs" });
  }
});


app.post("/api/servers/statuses", async (req, res) => {
  if (!(await isAuthenticated(req))) {
    return res.status(401).json({ error: "not authenticated" });
  }

  const email = req.session?.user;
  const rawNames = Array.isArray(req.body?.names) ? req.body.names : [];
  const requestedNames = [...new Set(rawNames.slice(0, 200))];

  const safeNames = requestedNames.map(n => String(n || '').trim()).filter(Boolean);
  const user = email ? await findUserByEmail(email) : null;
  let accessibleNames;
  if (user && user.admin) {
    accessibleNames = safeNames;
  } else {
    const access = email ? await getAccessListForEmail(email) : [];
    const accessLower = new Set(access.map(a => String(a).toLowerCase()));
    const hasAll = accessLower.has('all');
    accessibleNames = hasAll ? safeNames : safeNames.filter(n => accessLower.has(n.toLowerCase()));
  }

  const statuses = {};
  const BATCH = 200;
  for (let i = 0; i < accessibleNames.length; i += BATCH) {
    const batch = accessibleNames.slice(i, i + BATCH);
    const entries = await Promise.all(batch.map(async (name) => {
      const cached = await getCachedServerStatus(name);
      return [name, cached];
    }));
    for (const [name, cached] of entries) {
      if (cached) {
        statuses[name] = {
          status: cached.status,
          cpu: cached.cpu,
          memory: cached.memory,
          disk: cached.disk,
          nodeOnline: cached.nodeOnline,
          updatedAt: cached.updatedAt
        };
      } else {
        statuses[name] = {
          status: 'unknown',
          nodeOnline: true,
          updatedAt: null
        };
      }
    }
  }

  return res.json({
    ok: true,
    statuses,
    cacheAge: Date.now() - statusCache.lastFullRefresh
  });
});

app.get("/api/server/:name/status", async (req, res) => {
  if (!(await isAuthenticated(req))) {
    return res.status(401).json({ error: "not authenticated" });
  }

  const name = sanitizeServerName(req.params.name);
  const email = req.session?.user;

  if (!name || !email || !(await userHasAccessToServer(email, name))) {
    return res.status(403).json({ error: "access denied" });
  }

  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, private");

  const cached = await getCachedServerStatus(name);

  if (cached) {
    const normalizedCached = normalizeServerStatusRecord(cached);
    return res.json({
      ok: true,
      name,
      status: normalizedCached.status,
      cpu: normalizedCached.cpu,
      cpuLimit: normalizedCached.cpuLimit ?? null,
      memory: normalizedCached.memory,
      disk: normalizedCached.disk,
      nodeOnline: normalizedCached.nodeOnline,
      updatedAt: normalizedCached.updatedAt,
      fresh: Date.now() - normalizedCached.updatedAt < STATUS_CACHE_TTL_MS
    });
  }

  return res.json({
    ok: true,
    name,
    status: 'unknown',
    cpuLimit: null,
    nodeOnline: true,
    updatedAt: null,
    fresh: false
  });
});

app.get("/api/nodes/statuses", async (req, res) => {
  if (!(await isAdmin(req))) {
    return res.status(403).json({ error: "not authorized" });
  }

  const nodes = await loadNodes();
  const statuses = {};

  for (const node of nodes) {
    const id = node.uuid || node.id || node.name;
    const cached = await getCachedNodeStatus(id);

    statuses[id] = cached ? {
      online: cached.online,
      latency: cached.latency,
      lastSeen: cached.lastSeen,
      serverCount: cached.serverCount,
      updatedAt: cached.updatedAt
    } : {
      online: null,
      lastSeen: null,
      updatedAt: null
    };
  }

  return res.json({
    ok: true,
    nodes: statuses,
    lastRefresh: statusCache.lastFullRefresh
  });
});

app.get("/api/admin/nodes/:id/stats", async (req, res) => {
  if (!(await isAdmin(req))) {
    return res.status(403).json({ error: "Admin access required" });
  }

  const nodeId = req.params.id;
  if (!nodeId) {
    return res.status(400).json({ error: "Node ID required" });
  }

  const node = await findNodeByIdOrName(nodeId);
  if (!node) {
    return res.status(404).json({ error: "Node not found" });
  }

  const isLocal = isLocalHost(node.address);
  const baseUrl = buildNodeBaseUrl(node.address, node.api_port, node.ssl_enabled);

  if (!baseUrl) {
    return res.json({
      ok: false,
      online: false,
      error: "Node address not configured"
    });
  }

  const headers = nodeAuthHeadersFor(node, !isLocal);

  try {
    const statsUrl = `${baseUrl}/api/system/stats`;
    const result = await httpRequestJson(statsUrl, "GET", headers, null, 8000);

    if (!result || result.status !== 200 || !result.json || !result.json.ok) {
      return res.json({
        ok: false,
        online: false,
        nodeId: node.id || node.uuid,
        nodeName: node.name,
        error: result?.json?.error || "Failed to reach node"
      });
    }

    const data = result.json;

    const ramUsedMb = Number(data.ram_used_mb || data.memory_used_mb || 0);
    const ramTotalMb = Number(data.ram_total_mb || data.memory_total_mb || 1);
    const ramFreeMb = ramTotalMb - ramUsedMb;
    const ramPercent = ramTotalMb > 0 ? Math.round((ramUsedMb / ramTotalMb) * 100) : 0;

    const diskUsedGb = Number(data.disk_used_gb || 0);
    const diskTotalGb = Number(data.disk_total_gb || 1);
    const diskFreeGb = diskTotalGb - diskUsedGb;
    const diskPercent = diskTotalGb > 0 ? Math.round((diskUsedGb / diskTotalGb) * 100) : 0;

    const cpuPercent = Number(data.cpu_percent || data.cpu_usage || 0);

    const warnings = [];
    if (ramFreeMb < 100) warnings.push({ type: "ram", level: "critical", message: `Only ${Math.round(ramFreeMb)} MB RAM remaining` });
    else if (ramPercent > 90) warnings.push({ type: "ram", level: "warning", message: "RAM usage above 90%" });

    if (cpuPercent > 90) warnings.push({ type: "cpu", level: "warning", message: "CPU usage above 90%" });

    if (diskPercent > 90) warnings.push({ type: "disk", level: "warning", message: "Disk usage above 90%" });
    else if (diskFreeGb < 1) warnings.push({ type: "disk", level: "critical", message: `Only ${(diskFreeGb * 1024).toFixed(0)} MB disk space remaining` });

    return res.json({
      ok: true,
      online: true,
      nodeId: node.id || node.uuid,
      nodeName: node.name,
      stats: {
        cpu: {
          percent: Math.round(cpuPercent),
          cores: Number(data.cpu_cores || data.cores || 0)
        },
        ram: {
          usedMb: Math.round(ramUsedMb),
          totalMb: Math.round(ramTotalMb),
          freeMb: Math.round(ramFreeMb),
          percent: ramPercent
        },
        disk: {
          usedGb: Math.round(diskUsedGb * 10) / 10,
          totalGb: Math.round(diskTotalGb * 10) / 10,
          freeGb: Math.round(diskFreeGb * 10) / 10,
          percent: diskPercent
        },
        uptime: data.uptime || data.uptime_seconds || 0,
        hostname: data.hostname || node.name,
        os: data.os || data.platform || "Unknown"
      },
      warnings,
      timestamp: Date.now()
    });
  } catch (err) {
    console.error(`[stats] Error fetching stats from node ${nodeId}:`, err.message);
    return res.json({
      ok: false,
      online: false,
      nodeId: node.id || node.uuid,
      nodeName: node.name,
      error: "Failed to connect to node"
    });
  }
});

app.post("/api/admin/refresh-statuses", async (req, res) => {
  if (!(await isAdmin(req))) {
    return res.status(403).json({ error: "not authorized" });
  }

  if (statusCache.refreshing) {
    return res.json({ ok: true, message: "Refresh already in progress" });
  }

  pollAllNodes().catch(console.error);

  return res.json({ ok: true, message: "Refresh started" });
});

function fileInfo(p) {
  const exists = fs.existsSync(p);
  let writable = false;
  if (exists) {
    try { fs.accessSync(p, fs.constants.W_OK); writable = true; } catch { }
  }
  return { p, exists, writable };
}

app.post("/api/settings/background", async (req, res) => {
  try {
    if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });

    const { type, value } = req.body || {};
    if (!type || typeof value === "undefined") return res.status(400).json({ error: "missing type/value" });

    const cssVal = makeCssBackground(type, value);
    if (!cssVal) return res.status(400).json({ error: "invalid background value" });

    const ok1 = setBodyBackgroundInFile(DASHBOARD_CSS, cssVal);
    const ok2 = setBodyBackgroundInFile(BOT_CSS, cssVal);

    if (ok1 && ok2) {
      req.app.locals.themeVersion = Date.now();
      return res.json({ ok: true });
    }
    return res.status(500).json({ error: "failed", details: { ok1, ok2 } });
  } catch (e) {
    console.error("[bg] ERROR:", e);
    return res.status(500).json({ error: "exception", message: String(e?.message || e) });
  }
});

const WEBHOOKS_CONFIG_PATH = path.join(__dirname, "webhooks-config.json");

const DANGEROUS_KEYS = new Set(["__proto__", "constructor", "prototype", "toString", "valueOf", "hasOwnProperty"]);

const DISCORD_WEBHOOK_REGEX = /^https:\/\/discord\.com\/api\/webhooks\/\d+\/[\w-]+$/;

function validateDiscordWebhookUrl(url) {
  if (!url || typeof url !== "string") return false;
  const trimmed = url.trim();
  if (trimmed.length > 200) return false;
  return DISCORD_WEBHOOK_REGEX.test(trimmed);
}

function maskWebhookUrlServer(url) {
  if (!url || typeof url !== "string") return null;
  try {
    const parts = url.split("/");
    if (parts.length >= 7) return parts.slice(0, 6).join("/") + "/••••••";
  } catch (_) { }
  return "https://discord.com/api/webhooks/••••••/••••••";
}

function sanitizeWebhooksConfig(raw) {
  const safe = Object.create(null);
  safe.mode = (raw && (raw.mode === "single" || raw.mode === "multiple")) ? raw.mode : "single";
  safe.single = (raw && typeof raw.single === "string" && validateDiscordWebhookUrl(raw.single)) ? raw.single.trim() : null;
  const safeMultiple = Object.create(null);
  if (raw && raw.multiple && typeof raw.multiple === "object" && !Array.isArray(raw.multiple)) {
    const validCatIds = new Set(WEBHOOK_CATEGORIES.map(c => c.id));
    for (const [k, v] of Object.entries(raw.multiple)) {
      if (DANGEROUS_KEYS.has(k)) continue;
      if (!validCatIds.has(k)) continue;
      if (typeof v === "string" && validateDiscordWebhookUrl(v)) {
        safeMultiple[k] = v.trim();
      }
    }
  }
  safe.multiple = safeMultiple;
  return safe;
}

let _webhooksConfigCache = null;
let _webhooksConfigCacheTs = 0;
const WEBHOOKS_CONFIG_CACHE_TTL_MS = 30000;

function loadWebhooksConfig() {
  const now = Date.now();
  if (_webhooksConfigCache && (now - _webhooksConfigCacheTs) < WEBHOOKS_CONFIG_CACHE_TTL_MS) return _webhooksConfigCache;
  try {
    if (fs.existsSync(WEBHOOKS_CONFIG_PATH)) {
      const raw = JSON.parse(fs.readFileSync(WEBHOOKS_CONFIG_PATH, "utf8"));
      const result = sanitizeWebhooksConfig(raw);
      _webhooksConfigCache = result;
      _webhooksConfigCacheTs = now;
      return result;
    }
  } catch (e) {
    console.error("[webhooks] Failed to load config:", e.message);
  }
  const result = sanitizeWebhooksConfig(null);
  _webhooksConfigCache = result;
  _webhooksConfigCacheTs = now;
  return result;
}

function saveWebhooksConfig(config) {
  const safe = sanitizeWebhooksConfig(config);
  try {
    fs.writeFileSync(WEBHOOKS_CONFIG_PATH, JSON.stringify(safe, null, 2), { encoding: "utf8", mode: 0o600 });
    _webhooksConfigCache = null;
    _webhooksConfigCacheTs = 0;
    return true;
  } catch (e) {
    console.error("[webhooks] Failed to save config:", e.message);
    return false;
  }
}

function maskConfigForResponse(config) {
  const masked = Object.create(null);
  masked.mode = config.mode;
  masked.single = config.single ? maskWebhookUrlServer(config.single) : null;
  masked.singleConfigured = !!config.single;
  const maskedMultiple = Object.create(null);
  if (config.multiple) {
    for (const [k, v] of Object.entries(config.multiple)) {
      maskedMultiple[k] = maskWebhookUrlServer(v);
    }
  }
  masked.multiple = maskedMultiple;
  const configured = Object.create(null);
  if (config.multiple) {
    for (const k of Object.keys(config.multiple)) {
      configured[k] = true;
    }
  }
  masked.multipleConfigured = configured;
  return masked;
}

const WEBHOOK_CATEGORIES = [
  { id: "server_start", label: "Server Start", icon: "fa-solid fa-play", color: "#34d399" },
  { id: "server_stop", label: "Server Stop", icon: "fa-solid fa-stop", color: "#f87171" },
  { id: "server_restart", label: "Server Restart", icon: "fa-solid fa-rotate", color: "#fbbf24" },
  { id: "server_kill", label: "Server Kill", icon: "fa-solid fa-skull", color: "#f87171" },
  { id: "console_command", label: "Console Command", icon: "fa-solid fa-terminal", color: "#60a5fa" },
  { id: "file_edit", label: "File Edit", icon: "fa-solid fa-pen", color: "#a78bfa" },
  { id: "file_create", label: "File Create", icon: "fa-solid fa-file-circle-plus", color: "#34d399" },
  { id: "file_delete", label: "File Delete", icon: "fa-solid fa-trash", color: "#f87171" },
  { id: "file_rename", label: "File Rename", icon: "fa-solid fa-i-cursor", color: "#fbbf24" },
  { id: "file_upload", label: "File Upload", icon: "fa-solid fa-upload", color: "#60a5fa" },
  { id: "file_extract", label: "File Extract", icon: "fa-solid fa-file-zipper", color: "#a78bfa" },
  { id: "file_archive", label: "File Archive", icon: "fa-solid fa-box-archive", color: "#a78bfa" },
  { id: "file_delete_batch", label: "Batch File Delete", icon: "fa-solid fa-trash-can", color: "#f87171" },
  { id: "scheduler_create", label: "Scheduler Create", icon: "fa-solid fa-clock", color: "#34d399" },
  { id: "scheduler_delete", label: "Scheduler Delete", icon: "fa-solid fa-clock", color: "#f87171" },

  { id: "password_change", label: "Password Change", icon: "fa-solid fa-lock", color: "#fbbf24" },
  { id: "user_create", label: "User Create", icon: "fa-solid fa-user-plus", color: "#34d399" },
  { id: "user_delete", label: "User Delete", icon: "fa-solid fa-user-minus", color: "#f87171" },
];

const VALID_CATEGORY_IDS = new Set(WEBHOOK_CATEGORIES.map(c => c.id));

const webhookRateLimit = new Map();
const WEBHOOK_RATE_LIMIT_MS = 2000;
const WEBHOOK_RATE_LIMIT_MAX_ENTRIES = 500;

setInterval(() => {
  try {
    const now = Date.now();
    for (const [key, ts] of webhookRateLimit.entries()) {
      if (now - ts > 60000) webhookRateLimit.delete(key);
    }
  } catch (err) { console.debug("[webhookRateLimit] sweep error:", err.message); }
}, 60000).unref();

function stripDiscordMentions(str) {
  if (!str || typeof str !== "string") return str;
  return str
    .replace(/@(everyone|here)/gi, "@\u200B$1")
    .replace(/<@[!&]?\d+>/g, "[mention removed]")
    .replace(/<#\d+>/g, "[channel removed]");
}

function sanitizeFieldValue(val) {
  if (val === null || val === undefined) return null;
  let s = String(val);
  s = s.slice(0, 256);
  s = stripDiscordMentions(s);
  s = s.replace(/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/g, "");
  return s;
}

function sanitizeFieldName(name) {
  if (!name || typeof name !== "string") return "field";
  let s = name.slice(0, 64);
  s = s.replace(/[\x00-\x1f\x7f]/g, "");
  return s || "field";
}

async function dispatchDiscordWebhook(webhookUrl, action, details, user, serverName) {
  if (!webhookUrl) return;

  if (!validateDiscordWebhookUrl(webhookUrl)) {
    console.warn("[SECURITY][webhooks] Blocked dispatch to invalid webhook URL (possible config tampering)");
    return;
  }

  const now = Date.now();
  const key = `${action}:${webhookUrl.slice(-12)}`;
  const lastSent = webhookRateLimit.get(key) || 0;
  if (now - lastSent < WEBHOOK_RATE_LIMIT_MS) return;

  if (webhookRateLimit.size >= WEBHOOK_RATE_LIMIT_MAX_ENTRIES) {
    const entries = [...webhookRateLimit.entries()].sort((a, b) => a[1] - b[1]);
    const toEvict = Math.max(1, Math.floor(entries.length / 4));
    for (let i = 0; i < toEvict; i++) webhookRateLimit.delete(entries[i][0]);
  }
  webhookRateLimit.set(key, now);

  const cat = WEBHOOK_CATEGORIES.find(c => c.id === action);
  const colorInt = cat ? parseInt(cat.color.replace("#", ""), 16) : 0x5865f2;
  const title = cat ? cat.label : String(action || "unknown").slice(0, 64);

  const fields = [];
  if (serverName) fields.push({ name: "Server", value: sanitizeFieldValue(serverName), inline: true });
  if (user) fields.push({ name: "User", value: sanitizeFieldValue(user), inline: true });
  fields.push({ name: "Time", value: new Date().toISOString(), inline: true });

  if (details && typeof details === "object" && !Array.isArray(details)) {
    let fieldCount = 0;
    const MAX_DETAIL_FIELDS = 10;
    for (const [k, v] of Object.entries(details)) {
      if (fieldCount >= MAX_DETAIL_FIELDS) break;
      if (v === null || v === undefined) continue;
      if (DANGEROUS_KEYS.has(k)) continue;
      fields.push({ name: sanitizeFieldName(k), value: sanitizeFieldValue(v), inline: true });
      fieldCount++;
    }
  }

  const payload = {
    embeds: [{
      title: stripDiscordMentions(`\u{1F4CB} ${title}`),
      color: colorInt,
      fields,
      footer: { text: "ADPanel Webhook" },
      timestamp: new Date().toISOString()
    }],
    allowed_mentions: { parse: [] }
  };

  try {
    const body = JSON.stringify(payload);
    if (Buffer.byteLength(body) > 8192) {
      console.warn("[webhooks] Payload too large, skipping dispatch");
      return;
    }
    const urlObj = new URL(webhookUrl);
    const options = {
      hostname: urlObj.hostname,
      path: urlObj.pathname,
      method: "POST",
      headers: { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body) },
      timeout: 5000
    };
    const req = https.request(options, (res) => { res.resume(); });
    req.on("error", () => { });
    req.on("timeout", () => { req.destroy(); });
    req.end(body);
  } catch (e) {
    console.error("[webhooks] Dispatch error:", e.message);
  }
}

function dispatchWebhookForActivity(action, details, user, serverName) {
  const config = loadWebhooksConfig();
  if (!config) return;

  if (config.mode === "single" && config.single) {
    dispatchDiscordWebhook(config.single, action, details, user, serverName);
  } else if (config.mode === "multiple" && config.multiple) {
    const url = config.multiple[action];
    if (url) {
      dispatchDiscordWebhook(url, action, details, user, serverName);
    }
  }
}

nodesRouter.setWebhookDispatcher(dispatchWebhookForActivity);

app.get("/api/settings/webhooks", async (req, res) => {
  const config = loadWebhooksConfig();
  res.json({ ok: true, config: maskConfigForResponse(config), categories: WEBHOOK_CATEGORIES });
});

app.post("/api/settings/webhooks", async (req, res) => {
  const { mode, url, category } = req.body || {};
  const hasMode = mode === "single" || mode === "multiple";
  const hasUrl = typeof url === "string" && !!url.trim();

  if (typeof mode !== "undefined" && !hasMode) {
    return res.status(400).json({ error: "Invalid mode. Must be 'single' or 'multiple'" });
  }
  if (!hasMode && !hasUrl) {
    return res.status(400).json({ error: "Missing mode or webhook URL" });
  }
  const config = loadWebhooksConfig();

  if (hasMode) {
    config.mode = mode;
  }

  if (hasUrl) {
    if (!validateDiscordWebhookUrl(url)) {
      return res.status(400).json({ error: "Invalid Discord webhook URL. Must be: https://discord.com/api/webhooks/..." });
    }

    if (category) {
      if (!VALID_CATEGORY_IDS.has(category)) {
        return res.status(400).json({ error: "Invalid category" });
      }
      if (DANGEROUS_KEYS.has(category)) {
        return res.status(400).json({ error: "Invalid category" });
      }
      if (!config.multiple) config.multiple = Object.create(null);
      config.multiple[category] = url.trim();
    } else {
      config.single = url.trim();
      config.mode = "single";
    }
  }

  const userEmail = req.session?.user || "unknown";
  recordActivity("_system", "webhook_config_change", {
    change: hasUrl ? (category ? `set_category:${category}` : "set_single") : `mode:${mode}`,
  }, userEmail, getRequestIp(req));

  if (saveWebhooksConfig(config)) return res.json({ ok: true, config: maskConfigForResponse(config) });
  res.status(500).json({ error: "Failed to save" });
});

app.delete("/api/settings/webhooks", async (req, res) => {
  const { category } = req.body || {};
  const config = loadWebhooksConfig();

  if (category) {
    if (!VALID_CATEGORY_IDS.has(category)) {
      return res.status(400).json({ error: "Invalid category" });
    }
    if (DANGEROUS_KEYS.has(category)) {
      return res.status(400).json({ error: "Invalid category" });
    }
    if (config.multiple) delete config.multiple[category];
  } else {
    config.single = null;
  }

  const userEmail = req.session?.user || "unknown";
  recordActivity("_system", "webhook_config_delete", {
    target: category || "single",
  }, userEmail, getRequestIp(req));

  if (saveWebhooksConfig(config)) return res.json({ ok: true });
  res.status(500).json({ error: "Failed to save" });
});

app.post("/api/settings/webhooks/test", async (req, res) => {
  const { category } = req.body || {};
  const config = loadWebhooksConfig();
  let webhookUrl = null;

  if (category) {
    if (!VALID_CATEGORY_IDS.has(category)) {
      return res.status(400).json({ error: "Invalid category" });
    }
    webhookUrl = config.multiple ? config.multiple[category] : null;
  } else {
    webhookUrl = config.single;
  }

  if (!webhookUrl || !validateDiscordWebhookUrl(webhookUrl)) {
    return res.status(400).json({ error: "No valid webhook URL configured" });
  }

  try {
    await dispatchDiscordWebhook(webhookUrl, "test", { message: "Webhook test from ADPanel" }, req.session?.user || "admin", "test");
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: "Failed to send test" });
  }
});

app.get("/api/settings/alert", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "Unauthorized" });
  res.json(getGlobalAlerts() || []);
});

app.post("/api/settings/alert", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "Unauthorized" });

  const user = await findUserByEmail(req.session.user);
  if (!user || !user.admin) return res.status(403).json({ error: "Admin only" });

  const { message, date, endDate, neverEnds } = req.body || {};
  const trimmedMessage = String(message || "").trim();
  if (!trimmedMessage) return res.status(400).json({ error: "Message required" });

  const startAt = date ? new Date(date).getTime() : NaN;
  if (!Number.isFinite(startAt)) {
    return res.status(400).json({ error: "Valid start date required" });
  }

  const isInfinite = neverEnds === true || String(neverEnds).toLowerCase() === "true";
  let normalizedEndDate = null;

  if (!isInfinite) {
    const endAt = endDate ? new Date(endDate).getTime() : NaN;
    if (!Number.isFinite(endAt)) {
      return res.status(400).json({ error: "Valid end date required" });
    }
    if (endAt <= startAt) {
      return res.status(400).json({ error: "End date must be after start date" });
    }
    normalizedEndDate = new Date(endAt).toISOString();
  }

  if (addGlobalAlert(trimmedMessage, new Date(startAt).toISOString(), normalizedEndDate, isInfinite)) {
    return res.json({ ok: true });
  }
  res.status(500).json({ error: "Failed to save alert" });
});

app.delete("/api/settings/alert/:id", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "Unauthorized" });

  const user = await findUserByEmail(req.session.user);
  if (!user || !user.admin) return res.status(403).json({ error: "Admin only" });

  const { id } = req.params;
  if (deleteGlobalAlert(id)) return res.json({ ok: true });
  res.status(500).json({ error: "Failed to delete alert" });
});

app.post("/api/settings/change-password", async (req, res) => {
  try {
    if (!(await isAuthenticated(req))) {
      return res.status(401).json({ error: "not authenticated" });
    }

    const email = req.session?.user;
    if (!email) return res.status(401).json({ error: "not authenticated" });

    const { current, newPassword, confirm, twoFactorCode } = req.body || {};

    if (!current || !newPassword || !confirm) {
      return res.status(400).json({ error: "missing required fields" });
    }

    if (newPassword.length < 8) {
      return res.status(400).json({ error: "new password must be at least 8 characters" });
    }

    if (newPassword !== confirm) {
      return res.status(400).json({ error: "passwords do not match" });
    }

    const user = await findUserByEmail(email);
    if (!user) {
      return res.status(404).json({ error: "user not found" });
    }

    const isValid = await withTimeout(bcrypt.compare(current, user.password), 8000, "bcrypt-timeout");
    if (!isValid) {
      return res.status(403).json({ error: "current password is incorrect" });
    }

    if (user.secret) {
      if (!twoFactorCode) {
        return res.status(400).json({ error: "2FA code required", requires2FA: true });
      }

      const codeStr = String(twoFactorCode).replace(/\s+/g, "");
      if (!/^\d{6}$/.test(codeStr)) {
        return res.status(400).json({ error: "invalid 2FA code format" });
      }

      const verified = speakeasy.totp.verify({
        secret: user.secret,
        encoding: "base32",
        token: codeStr,
        window: 1,
      });

      if (!verified) {
        return res.status(403).json({ error: "incorrect 2FA code" });
      }
    }

    const hashedPassword = await withTimeout(bcrypt.hash(newPassword, BCRYPT_ROUNDS), 10000, "bcrypt-hash-timeout");

    await db.query(
      "UPDATE users SET password = ? WHERE LOWER(email) = LOWER(?)",
      [hashedPassword, email]
    );

    console.log(`[auth] Password changed for user: ${email}`);
    setRememberLoginCookie(req, res, { ...user, password: hashedPassword });
    return res.json({ ok: true, message: "password changed successfully" });
  } catch (err) {
    console.error("[change-password] Error:", err);
    return res.status(500).json({ error: "failed to change password" });
  }
});

app.get("/api/settings/servers", async (req, res, next) => {
  try {
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
    const index = await loadServersIndex();
    let items = (index || [])
      .filter(e => e && e.name)
      .map(e => ({
        name: e.name,
        displayName: e.displayName || null,
        nodeId: e.nodeId || null,
        template: e.template || null,
      }))
      .sort((a, b) => a.name.localeCompare(b.name));

    const search = String(req.query.search || '').trim().toLowerCase();
    if (search) {
      items = items.filter(e => (e.name && e.name.toLowerCase().includes(search)) || (e.displayName && e.displayName.toLowerCase().includes(search)));
    }

    const total = items.length;
    const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 50, 1), 200);
    const totalPages = Math.max(1, Math.ceil(total / limit));
    const page = Math.min(Math.max(parseInt(req.query.page, 10) || 1, 1), totalPages);
    const offset = (page - 1) * limit;
    items = items.slice(offset, offset + limit);

    return res.json({ items, total, page, totalPages });
  } catch (err) {
    return next(err);
  }
});

app.get("/api/my-servers", async (req, res, next) => {
  try {
    if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
    const idx = (await loadServersIndex()) || [];
    const entries = (Array.isArray(idx) ? idx : []).filter((entry) => entry && entry.name);

    const userEmail = req.session.user;
    const u = await findUserByEmail(userEmail);
    if (u && u.admin) {
      return res.json({
        names: entries.map((entry) => entry.name),
        servers: entries.map((entry) => ({
          name: entry.name,
          displayName: entry.displayName || entry.name,
          status: entry.status || "unknown",
          template: entry.template || "custom",
          nodeId: entry.nodeId || null,
        })),
      });
    }

    const access = (await getAccessListForEmail(userEmail)) || [];
    const loweredAccess = new Set(access.map((name) => String(name || "").trim().toLowerCase()));
    const filteredEntries = loweredAccess.has("all")
      ? entries
      : entries.filter((entry) => loweredAccess.has(String(entry.name || "").trim().toLowerCase()));
    return res.json({
      names: filteredEntries.map((entry) => entry.name),
      servers: filteredEntries.map((entry) => ({
        name: entry.name,
        displayName: entry.displayName || entry.name,
        status: entry.status || "unknown",
        template: entry.template || "custom",
        nodeId: entry.nodeId || null,
      })),
    });
  } catch (err) {
    return next(err);
  }
});

app.get("/api/servers/:name/permissions", async (req, res, next) => {
  try {
    if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });

    const serverName = String(req.params.name || "").trim();
    const email = req.session.user;
    const user = email ? await findUserByEmail(email) : null;
    const isAdminUser = !!(user && user.admin);

    if (!isAdminUser && !(await userHasAccessToServer(email, serverName))) {
      return res.status(403).json({ error: "no access to server" });
    }

    const basePerms = await getEffectivePermsForUserOnServer(email, serverName);
    const perms = isAdminUser
      ? ALLOWED_PERM_KEYS.reduce((accumulator, key) => {
          accumulator[key] = true;
          return accumulator;
        }, {})
      : { ...(basePerms || {}) };

    const actionTokens = {};
    if (perms.files_create) {
      actionTokens.fileWrite = issueActionToken(req, "PUT /api/servers/:name/files/write", { serverName }, { ttlSeconds: 300 });
      actionTokens.fileMkdir = issueActionToken(req, "POST /api/servers/:name/files/mkdir", { serverName }, { ttlSeconds: 300 });
    }
    if (perms.files_delete) {
      actionTokens.fileDelete = issueActionToken(req, "DELETE /api/servers/:name/files/delete", { serverName }, { ttlSeconds: 120, oneTime: true });
      actionTokens.fileRename = issueActionToken(req, "POST /api/servers/:name/files/rename", { serverName }, { ttlSeconds: 120, oneTime: true });
    }
    if (perms.files_upload) {
      actionTokens.fileUpload = issueActionToken(req, "POST /upload", { serverName }, { ttlSeconds: 300 });
      actionTokens.fileExtract = issueActionToken(req, "POST /extract", { serverName }, { ttlSeconds: 300 });
      actionTokens.fileArchive = issueActionToken(req, "POST /archive", { serverName }, { ttlSeconds: 300 });
    }
    if (perms.store_access) {
      actionTokens.changeTemplate = issueActionToken(req, "POST /api/servers/:bot/template", { serverName }, { ttlSeconds: 120, oneTime: true });
      actionTokens.applyVersion = issueActionToken(req, "POST /api/servers/:bot/versions/apply", { serverName }, { ttlSeconds: 120, oneTime: true });
    }

    return res.json({
      isAdmin: isAdminUser,
      perms,
      email,
      user: user ? { email: user.email, admin: isAdminUser } : null,
      agent_access: !!(user && (isAdminUser || user.agent_access)),
      actionTokens,
    });
  } catch (err) {
    return next(err);
  }
});


app.get("/api/settings/accounts", async (req, res, next) => {
  try {
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });

    const users = await loadUsers();

    const accessRaw = await loadUserAccess();
    const accessMap = new Map();
    for (const a of (Array.isArray(accessRaw) ? accessRaw : [])) {
      const emailLower = String(a.email || "").toLowerCase();
      if (emailLower) {
        accessMap.set(emailLower, Array.isArray(a.servers) ? a.servers : []);
      }
    }

    const accounts = users
      .filter(u => u && u.email)
      .map(u => {
        const emailLower = String(u.email).toLowerCase();
        return {
          email: u.email,
          servers: accessMap.get(emailLower) || [],
          agent_access: !!u.agent_access,
          admin: !!u.admin
        };
      })
      .sort((a, b) => {
        if (a.admin !== b.admin) return a.admin ? -1 : 1;
        return String(a.email || "").localeCompare(String(b.email || ""));
      });

    const bots = (await loadServersIndex() || []).map(e => e?.name).filter(Boolean);

    return res.json({
      accounts,
      bots,
      actionTokens: {
        addAccess: issueActionToken(req, "POST /api/settings/accounts/:email/add", {}, { ttlSeconds: 300 }),
        removeAccess: issueActionToken(req, "POST /api/settings/accounts/:email/remove", {}, { ttlSeconds: 300 }),
        setAgentAccess: issueActionToken(req, "POST /api/settings/accounts/:email/agent-access", {}, { ttlSeconds: 300 }),
        grantPerms: issueActionToken(req, "POST /api/settings/accounts/:email/grant-perms", {}, { ttlSeconds: 300 }),
        deleteAccount: issueActionToken(req, "DELETE /api/settings/accounts/:email", {}, { ttlSeconds: 300 }),
        changePassword: issueActionToken(req, "POST /api/settings/accounts/:email/change-password", {}, { ttlSeconds: 300 }),
        reset2fa: issueActionToken(req, "POST /api/admin/user/reset-2fa", {}, { ttlSeconds: 300 }),
        confirmReset2fa: issueActionToken(req, "POST /api/admin/user/confirm-2fa-reset", {}, { ttlSeconds: 300 }),
        resetRecoveryCodes: issueActionToken(req, "POST /api/admin/user/reset-recovery-codes", {}, { ttlSeconds: 300 }),
      },
    });
  } catch (e) {
    console.error("Failed to read accounts:", e);
    return res.status(500).json({ error: "failed to read accounts" });
  }
});

app.post("/api/settings/accounts/:email/add", async (req, res, next) => {
  try {
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
    if (!requireActionTokenOr403(req, res, "POST /api/settings/accounts/:email/add")) return;

    let email;
    try { email = decodeURIComponent(req.params.email || ""); } catch { email = req.params.email || ""; }
    email = String(email).trim();

    const server = String(req.body?.server || "").trim();
    if (!email || !server) return res.status(400).json({ error: "missing email or server" });

    const targetUser = await findUserByEmail(email);
    if (!targetUser) return res.status(404).json({ error: "user not found" });
    if (targetUser.admin) return res.status(403).json({ error: "cannot change server access for admin users" });

    const allBots = (await loadServersIndex() || []).map(e => e?.name).filter(Boolean);
    if (!allBots.includes(server) && server !== "all") return res.status(400).json({ error: "server not found" });

    const ok = await addAccessForEmail(email, server);
    if (!ok) return res.status(500).json({ error: "failed to save access" });
    return res.json({ ok: true });
  } catch (err) {
    return next(err);
  }
});

app.post("/api/settings/accounts/:email/remove", async (req, res, next) => {
  try {
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
    if (!requireActionTokenOr403(req, res, "POST /api/settings/accounts/:email/remove")) return;

    let email;
    try { email = decodeURIComponent(req.params.email || ""); } catch { email = req.params.email || ""; }
    email = String(email).trim();

    const server = String(req.body?.server || "").trim();
    if (!email || !server) return res.status(400).json({ error: "missing email or server" });

    const targetUser = await findUserByEmail(email);
    if (!targetUser) return res.status(404).json({ error: "user not found" });
    if (targetUser.admin) return res.status(403).json({ error: "cannot change server access for admin users" });

    const ok = await removeAccessForEmail(email, server);
    if (!ok) return res.status(500).json({ error: "failed to save access" });
    return res.json({ ok: true });
  } catch (err) {
    return next(err);
  }
});

app.post("/api/settings/accounts/:email/agent-access", async (req, res, next) => {
  try {
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
    if (!requireActionTokenOr403(req, res, "POST /api/settings/accounts/:email/agent-access")) return;

    let email;
    try { email = decodeURIComponent(req.params.email || ""); } catch { email = req.params.email || ""; }
    email = String(email).trim().toLowerCase();

    if (!email) return res.status(400).json({ error: "missing email" });

    const targetUser = await findUserByEmail(email);
    if (!targetUser) return res.status(404).json({ error: "user not found" });
    if (targetUser.admin) return res.status(403).json({ error: "cannot change agent access for admin users" });

    const enabled = !!req.body?.enabled;

    try {
      await db.query("ALTER TABLE users ADD COLUMN agent_access TINYINT(1) NOT NULL DEFAULT 0");
    } catch (e) {
    }

    await db.query("UPDATE users SET agent_access = ? WHERE LOWER(email) = LOWER(?)", [enabled ? 1 : 0, email]);

    return res.json({ ok: true });
  } catch (err) {
    return next(err);
  }
});

app.get("/api/settings/accounts/:email/perms", async (req, res, next) => {
  try {
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });

    let email;
    try { email = decodeURIComponent(req.params.email || ""); } catch { email = req.params.email || ""; }
    email = String(email).trim().toLowerCase();

    const server = String(req.query?.server || "").trim();
    if (!email) return res.status(400).json({ error: "missing email" });
    if (!server) return res.status(400).json({ error: "missing server" });

    const targetUser = await findUserByEmail(email);
    if (!targetUser) return res.status(404).json({ error: "user not found" });
    if (targetUser.admin) return res.status(403).json({ error: "cannot query server permissions for admin users" });

    const list = await loadServersIndex();
    const entry = list.find(e => e && e.name === server);
    if (!entry) return res.status(404).json({ error: "server not found" });

    const acl = entry.acl && typeof entry.acl === "object" ? entry.acl : {};
    const rec = acl[email] || null;

    const permissions = {};
    for (const k of ALLOWED_PERM_KEYS) {
      permissions[k] = !!(rec && rec[k]);
    }
    if (rec && rec.console_write && !Object.prototype.hasOwnProperty.call(rec, "console_read")) {
      permissions.console_read = true;
    }
    if (rec && rec.backups_manage) {
      permissions.backups_view = true;
      permissions.backups_create = true;
      permissions.backups_delete = true;
    }

    return res.json({ ok: true, permissions });
  } catch (err) {
    return next(err);
  }
});

app.post("/api/settings/accounts/:email/grant-perms", async (req, res, next) => {
  try {
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
    if (!requireActionTokenOr403(req, res, "POST /api/settings/accounts/:email/grant-perms")) return;

    let email;
    try { email = decodeURIComponent(req.params.email || ""); } catch { email = req.params.email || ""; }
    email = String(email).trim().toLowerCase();

    const server = String(req.body?.server || "").trim();
    const permsIn = (req.body && typeof req.body.permissions === "object") ? req.body.permissions : null;

    if (!email) return res.status(400).json({ error: "missing email" });
    if (!server) return res.status(400).json({ error: "missing server" });

    const targetUser = await findUserByEmail(email);
    if (!targetUser) return res.status(404).json({ error: "user not found" });
    if (targetUser.admin) return res.status(403).json({ error: "cannot change server permissions for admin users" });

    const exists = !!(await findServer(server));
    if (!exists) return res.status(404).json({ error: "server not found" });

    const cleanPerms = {};
    for (const k of ALLOWED_PERM_KEYS) cleanPerms[k] = !!(permsIn && typeof permsIn[k] === "boolean" ? permsIn[k] : false);

    const list = await loadServersIndex();
    const idx = list.findIndex(e => e && e.name === server);
    if (idx === -1) {
      list.push({ name: server, acl: { [email]: cleanPerms } });
    } else {
      const entry = list[idx] || {};
      const acl = (entry.acl && typeof entry.acl === "object") ? entry.acl : {};
      acl[email] = cleanPerms;
      entry.acl = acl;
      list[idx] = entry;
    }

    if (!(await saveServersIndex(list))) return res.status(500).json({ error: "failed to write servers.json" });
    try { await addAccessForEmail(email, server); } catch { }
    return res.json({ ok: true });
  } catch (err) {
    return next(err);
  }
});

app.delete("/api/settings/accounts/:email", async (req, res, next) => {
  try {
    if (!req.session || !req.session.user) {
      return res.status(401).json({ error: "not authenticated" });
    }

    if (!(await isAdmin(req))) {
      return res.status(403).json({ error: "admin required" });
    }
    if (!requireActionTokenOr403(req, res, "DELETE /api/settings/accounts/:email")) return;

    let targetEmail;
    try {
      targetEmail = decodeURIComponent(req.params.email || "");
    } catch {
      targetEmail = req.params.email || "";
    }
    targetEmail = String(targetEmail).trim();

    if (!targetEmail || !targetEmail.includes("@") || targetEmail.length > 255) {
      return res.status(400).json({ error: "invalid email" });
    }

    if (targetEmail.toLowerCase() === req.session.user.toLowerCase()) {
      return res.status(400).json({ error: "cannot delete yourself" });
    }

    const targetUser = await findUserByEmail(targetEmail);
    if (!targetUser) {
      return res.status(404).json({ error: "user not found" });
    }

    await db.query("DELETE FROM user_access WHERE user_id = ?", [targetUser.id]);

    try {
      const servers = await loadServersIndex();
      let aclsModified = false;
      for (const srv of servers) {
        if (srv.acl && typeof srv.acl === "object") {
          const lowerEmail = targetEmail.toLowerCase();
          if (srv.acl[lowerEmail]) {
            delete srv.acl[lowerEmail];
            aclsModified = true;
          }
        }
      }
      if (aclsModified) {
        await saveServersIndex(servers);
      }
    } catch (aclErr) {
      console.warn("[delete-user] Failed to clean ACLs:", aclErr);
    }

    await db.query("DELETE FROM users WHERE id = ?", [targetUser.id]);

    console.log(`[delete-user] Admin ${req.session.user} deleted user ${targetEmail}`);
    return res.json({ ok: true, message: "User deleted successfully" });
  } catch (err) {
    console.error("[delete-user] Error:", err);
    return next(err);
  }
});

app.post("/api/settings/accounts/:email/change-password", async (req, res, next) => {
  try {
    if (!req.session || !req.session.user) {
      return res.status(401).json({ error: "not authenticated" });
    }

    if (!(await isAdmin(req))) {
      return res.status(403).json({ error: "admin required" });
    }
    if (!requireActionTokenOr403(req, res, "POST /api/settings/accounts/:email/change-password")) return;

    let targetEmail;
    try {
      targetEmail = decodeURIComponent(req.params.email || "");
    } catch {
      targetEmail = req.params.email || "";
    }
    targetEmail = String(targetEmail).trim();

    if (!targetEmail || !targetEmail.includes("@") || targetEmail.length > 255) {
      return res.status(400).json({ error: "invalid email" });
    }

    if (targetEmail.toLowerCase() === req.session.user.toLowerCase()) {
      return res.status(400).json({ error: "use the regular password change for your own account" });
    }

    const targetUser = await findUserByEmail(targetEmail);
    if (!targetUser) {
      return res.status(404).json({ error: "user not found" });
    }

    const { newPassword, confirm } = req.body || {};
    if (!newPassword || !confirm) {
      return res.status(400).json({ error: "missing required fields" });
    }

    if (newPassword.length < 8) {
      return res.status(400).json({ error: "password must be at least 8 characters" });
    }

    if (newPassword !== confirm) {
      return res.status(400).json({ error: "passwords do not match" });
    }

    const hashedPassword = await withTimeout(bcrypt.hash(newPassword, BCRYPT_ROUNDS), 10000, "bcrypt-hash-timeout");

    await db.query(
      "UPDATE users SET password = ? WHERE id = ?",
      [hashedPassword, targetUser.id]
    );

    console.log(`[admin-change-password] Admin ${req.session.user} changed password for user ${targetEmail}`);
    return res.json({ ok: true, message: "Password changed successfully" });
  } catch (err) {
    console.error("[admin-change-password] Error:", err);
    return next(err);
  }
});



app.get("/api/servers/:name/files/list", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });

  const name = String(req.params.name || "").trim();
  if (!(await isAdmin(req)) && !(await userHasAccessToServer(req.session.user, name))) {
    return res.status(403).json({ error: "no access to server" });
  }

  const perms = await getEffectivePermsForUserOnServer(req.session.user, name);
  if (!perms.files_read) return res.status(403).json({ error: "not authorized" });

  const rel = String(req.query.path || "");

  const ctx = await resolveRemoteFsContext(name);
  if (!ctx.remote) return res.status(404).json({ error: "server not found on node" });

  try {
    const dir = safeJoinUnix(ctx.baseDir, rel || "");
    const { status, json } = await nodeFsPost(ctx.node, "/v1/fs/list", { path: dir }, 60_000);

    if (status === 0) {
      console.error(`[files/list] Node connection failed for ${name} - node may be offline`);
      return res.status(502).json({ error: "node_offline", message: "Cannot connect to server node. The node may be offline." });
    }
    if (status !== 200 || !json || !json.ok) {
      console.error(`[files/list] Node returned error for ${name}: status=${status}, json=${JSON.stringify(json)}`);
      return res.status(502).json({ error: "node_list_failed", detail: json?.error || `status ${status}` });
    }
    return res.json({ ok: true, path: rel, entries: Array.isArray(json.entries) ? json.entries : [] });
  } catch (e) {
    console.error(`[files/list] Error for ${name}:`, e.message);
    return res.status(400).json({ error: maskPathErrorMessage(e, "invalid path", "invalid path") });
  }
});

app.get("/api/servers/:name/files/read", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });

  const name = String(req.params.name || "").trim();
  if (!(await isAdmin(req)) && !(await userHasAccessToServer(req.session.user, name))) {
    return res.status(403).json({ error: "no access to server" });
  }

  const perms = await getEffectivePermsForUserOnServer(req.session.user, name);
  if (!perms.files_read) return res.status(403).json({ error: "not authorized" });

  const rel = String(req.query.path || "");

  const ctx = await resolveRemoteFsContext(name);
  if (!ctx.remote) return res.status(404).json({ error: "server not found on node" });

  try {
    const full = safeJoinUnix(ctx.baseDir, rel || "");
    const { status, json } = await nodeFsPost(ctx.node, "/v1/fs/read", { path: full, encoding: "utf8" }, 120000);
    if (status === 404) return res.status(404).json({ error: "file not found" });
    if (status !== 200 || !json || !json.ok) return res.status(502).json({ error: "node_read_failed" });

    const content = typeof json.content === "string" ? json.content : "";
    return res.json({ ok: true, path: rel, content });
  } catch (e) {
    return res.status(400).json({ error: maskPathErrorMessage(e, "invalid path", "invalid path") });
  }
});

app.get("/api/servers/:name/files/stream", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).end("not authenticated");

  const name = String(req.params.name || "").trim();
  if (!(await isAdmin(req)) && !(await userHasAccessToServer(req.session.user, name))) {
    return res.status(403).end("no access to server");
  }

  const perms = await getEffectivePermsForUserOnServer(req.session.user, name);
  if (!perms.files_read) return res.status(403).end("not authorized");

  const rel = String(req.query.path || "");

  const ctx = await resolveRemoteFsContext(name);
  if (!ctx.remote) return res.status(404).end("server not found on node");

  try {
    const full = safeJoinUnix(ctx.baseDir, rel || "");
    const { status, json } = await nodeFsPost(ctx.node, "/v1/fs/read", { path: full, encoding: "utf8" }, 120000);
    if (status === 404) return res.status(404).end("file not found");
    if (status !== 200 || !json || !json.ok) return res.status(502).end("node-read-failed");

    const content = typeof json.content === "string" ? json.content : "";
    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    return res.end(content);
  } catch (e) {
    return res.status(400).end(maskPathErrorMessage(e, "invalid path", "invalid path"));
  }
});

app.get("/api/servers/:name/files/download", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).end("not authenticated");

  const name = String(req.params.name || "").trim();
  if (!(await isAdmin(req)) && !(await userHasAccessToServer(req.session.user, name))) {
    return res.status(403).end("no access to server");
  }

  const perms = await getEffectivePermsForUserOnServer(req.session.user, name);
  if (!perms.files_read) return res.status(403).end("not authorized");

  const rel = String(req.query.path || "");

  const ctx = await resolveRemoteFsContext(name);
  if (!ctx.remote) return res.status(404).end("server not found on node");

  try {
    const full = safeJoinUnix(ctx.baseDir, rel || "");
    const nodeBase = buildNodeBaseUrl(ctx.node.address, ctx.node.api_port || 8080, ctx.node.ssl_enabled);
    if (!nodeBase) return res.status(502).end("node unreachable");
    const postData = JSON.stringify({ path: full });
    const lib = nodeBase.startsWith("https:") ? require("https") : require("http");
    const url = new URL(`${nodeBase}/v1/fs/download`);
    const hdrs = nodeAuthHeadersFor(ctx.node, true);
    const reqOpts = {
      hostname: url.hostname,
      port: url.port,
      path: url.pathname,
      method: "POST",
      headers: Object.assign({ "Content-Type": "application/json", "Content-Length": Buffer.byteLength(postData) }, hdrs),
      timeout: 300000
    };
    const proxyReq = lib.request(reqOpts, (proxyRes) => {
      if (proxyRes.statusCode !== 200) {
        return res.status(proxyRes.statusCode || 502).end("download failed");
      }
      if (proxyRes.headers["content-type"]) res.setHeader("Content-Type", proxyRes.headers["content-type"]);
      if (proxyRes.headers["content-disposition"]) res.setHeader("Content-Disposition", proxyRes.headers["content-disposition"]);
      if (proxyRes.headers["content-length"]) res.setHeader("Content-Length", proxyRes.headers["content-length"]);
      proxyRes.pipe(res);
    });
    proxyReq.on("error", () => res.status(502).end("node connection failed"));
    proxyReq.on("timeout", () => { proxyReq.destroy(); res.status(504).end("download timeout"); });
    proxyReq.write(postData);
    proxyReq.end();
  } catch (e) {
    return res.status(400).end(maskPathErrorMessage(e, "invalid path", "invalid path"));
  }
});

app.put("/api/servers/:name/files/write", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });

  const name = String(req.params.name || "").trim();
  if (!requireActionTokenOr403(req, res, "PUT /api/servers/:name/files/write", { serverName: name })) return;
  if (!(await isAdmin(req)) && !(await userHasAccessToServer(req.session.user, name))) {
    return res.status(403).json({ error: "no access to server" });
  }

  const perms = await getEffectivePermsForUserOnServer(req.session.user, name);
  if (!perms.files_create) return res.status(403).json({ error: "not authorized" });

  const rel = String(req.body?.path || "");
  const content = String(req.body?.content ?? "");

  const ctx = await resolveRemoteFsContext(name);
  if (!ctx.remote) return res.status(404).json({ error: "server not found on node" });

  try {
    const full = safeJoinUnix(ctx.baseDir, rel || "");
    const { status, json } = await nodeFsPost(ctx.node, "/v1/fs/write", { path: full, content, encoding: "utf8" }, 120000);
    if (status === 507 && json?.error === "disk_limit_exceeded") {
      return res.status(507).json(json);
    }
    if (status !== 200 || !json || !json.ok) return res.status(502).json({ error: "node_write_failed" });
    return res.json({ ok: true });
  } catch (e) {
    return res.status(400).json({ error: maskPathErrorMessage(e, "invalid path", "invalid path") });
  }
});

app.post("/api/servers/:name/files/mkdir", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });

  const name = String(req.params.name || "").trim();
  if (!requireActionTokenOr403(req, res, "POST /api/servers/:name/files/mkdir", { serverName: name })) return;
  if (!(await isAdmin(req)) && !(await userHasAccessToServer(req.session.user, name))) {
    return res.status(403).json({ error: "no access to server" });
  }

  const perms = await getEffectivePermsForUserOnServer(req.session.user, name);
  if (!perms.files_create) return res.status(403).json({ error: "not authorized" });

  const rel = String(req.body?.path || "");

  const ctx = await resolveRemoteFsContext(name);
  if (!ctx.remote) return res.status(404).json({ error: "server not found on node" });

  try {
    const full = safeJoinUnix(ctx.baseDir, rel || "");
    const { status, json } = await nodeFsPost(ctx.node, "/v1/fs/mkdir", { path: full }, 120000);
    if (status !== 200 || !json || !json.ok) return res.status(502).json({ error: "node_mkdir_failed" });
    return res.json({ ok: true });
  } catch (e) {
    return res.status(400).json({ error: maskPathErrorMessage(e, "invalid path", "invalid path") });
  }
});

app.delete("/api/servers/:name/files/delete", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });

  const name = String(req.params.name || "").trim();
  if (!requireActionTokenOr403(req, res, "DELETE /api/servers/:name/files/delete", { serverName: name })) return;
  if (!(await isAdmin(req)) && !(await userHasAccessToServer(req.session.user, name))) {
    return res.status(403).json({ error: "no access to server" });
  }

  const perms = await getEffectivePermsForUserOnServer(req.session.user, name);
  if (!perms.files_delete) return res.status(403).json({ error: "not authorized" });

  const rel = String(req.query.path || req.body?.path || "");

  const ctx = await resolveRemoteFsContext(name);
  if (!ctx.remote) return res.status(404).json({ error: "server not found on node" });

  try {
    const full = safeJoinUnix(ctx.baseDir, rel || "");
    const isDir = String(req.query.isDir || req.body?.isDir || "0") === "1";
    const { status, json } = await nodeFsPost(ctx.node, "/v1/fs/delete", { path: full, isDir }, 120000);
    if (status !== 200 || !json || !json.ok) return res.status(502).json({ error: "node_delete_failed" });
    return res.json({ ok: true });
  } catch (e) {
    return res.status(400).json({ error: maskPathErrorMessage(e, "invalid path", "invalid path") });
  }
});

app.post("/api/servers/:name/files/rename", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });

  const name = String(req.params.name || "").trim();
  if (!requireActionTokenOr403(req, res, "POST /api/servers/:name/files/rename", { serverName: name })) return;
  if (!(await isAdmin(req)) && !(await userHasAccessToServer(req.session.user, name))) {
    return res.status(403).json({ error: "no access to server" });
  }

  const perms = await getEffectivePermsForUserOnServer(req.session.user, name);
  if (!perms.files_rename) return res.status(403).json({ error: "not authorized" });

  const srcRel = String(req.body?.src || "");
  const destRel = String(req.body?.dest || "");

  const ctx = await resolveRemoteFsContext(name);
  if (!ctx.remote) return res.status(404).json({ error: "server not found on node" });

  try {
    const src = safeJoinUnix(ctx.baseDir, srcRel || "");
    const dest = safeJoinUnix(ctx.baseDir, destRel || "");
    const { status, json } = await nodeFsPost(ctx.node, "/v1/fs/rename", { src, dest }, 120000);
    if (status !== 200 || !json || !json.ok) return res.status(502).json({ error: "node_rename_failed" });
    return res.json({ ok: true });
  } catch (e) {
    return res.status(400).json({ error: maskPathErrorMessage(e, "invalid path", "invalid path") });
  }
});

const UPLOAD_TMP_DIR = process.env.UPLOAD_TMP_DIR || path.join(os.tmpdir(), "adpanel-uploads");
const UPLOAD_ORPHAN_MAX_AGE_MS = parseInt(process.env.UPLOAD_ORPHAN_MAX_AGE_MS || "", 10) || 60 * 60 * 1000;
const UPLOAD_CLEANUP_INTERVAL_MS = parseInt(process.env.UPLOAD_CLEANUP_INTERVAL_MS || "", 10) || 10 * 60 * 1000;

try {
  fs.mkdirSync(UPLOAD_TMP_DIR, { recursive: true, mode: 0o700 });
} catch { }

setInterval(async () => {
  try {
    const files = await fsp.readdir(UPLOAD_TMP_DIR);
    const now = Date.now();
    for (const file of files) {
      try {
        const filePath = path.join(UPLOAD_TMP_DIR, file);
        const stat = await fsp.stat(filePath);
        if (stat.isFile() && (now - stat.mtimeMs) > UPLOAD_ORPHAN_MAX_AGE_MS) {
          await fsp.unlink(filePath);
          console.log(`[upload-cleanup] removed orphaned file: ${file}`);
        }
      } catch { }
    }
  } catch { }
}, UPLOAD_CLEANUP_INTERVAL_MS).unref();

let _fileUpload;
function getFileUpload() {
  if (!_fileUpload) {
    const multer = getLazyMulter();
    _fileUpload = multer({
      storage: multer.diskStorage({
        destination: (_req, _file, cb) => cb(null, UPLOAD_TMP_DIR),
        filename: (_req, _file, cb) => cb(null, crypto.randomBytes(16).toString("hex")),
      }),
      limits: {
        fileSize: parseInt(process.env.UPLOAD_MAX_BYTES || "", 10) || 100 * 1024 * 1024 * 1024,
        files: 1,
      },
    });
  }
  return _fileUpload;
}

app.post("/upload", (req, res, next) => getFileUpload().single("file")(req, res, next), async (req, res) => {
  if (!(await isAuthenticated(req))) {
    if (req.headers?.accept?.includes("text/html")) return res.redirect("/login");
    return res.status(401).json({ error: "Not authenticated" });
  }

  const bot = String(req.body?.bot || "").trim();
  const relPath = String(req.body?.path || "").trim();

  if (!bot) return res.status(400).json({ error: "missing bot" });
  if (!req.file || !req.file.path) return res.status(400).json({ error: "No file uploaded" });

  if (!requireActionTokenOr403(req, res, "POST /upload", { serverName: bot })) {
    try { await fsp.unlink(req.file.path); } catch { }
    return;
  }

  const currentEmail = req.session.user;
  if (!(await isAdmin(req)) && !(await userHasAccessToServer(currentEmail, bot))) {
    return res.status(403).json({ error: "no access to server" });
  }

  const perms = await getEffectivePermsForUserOnServer(currentEmail, bot);
  if (!perms.files_upload && !(await isAdmin(req))) {
    return res.status(403).json({ error: "no upload permission" });
  }

  const ctx = await resolveRemoteFsContext(bot);
  if (!ctx.remote) return res.status(404).json({ error: "server not found on node" });

  const tmpPath = req.file.path;

  const nodeMaxUploadMb = ctx.node?.max_upload_mb || 10240;
  const nodeMaxUploadBytes = nodeMaxUploadMb * 1024 * 1024;
  if (req.file.size > nodeMaxUploadBytes) {
    try { await fsp.unlink(tmpPath); } catch { }
    return res.status(413).json({
      error: "file_too_large",
      message: `File exceeds this node's upload limit of ${nodeMaxUploadMb} MB`,
      limit_mb: nodeMaxUploadMb
    });
  }

  try {
    const filename = sanitizeUploadFilename(req.file.originalname || "upload");
    if (!filename) return res.status(400).json({ error: "Invalid filename" });
    if (!isUploadExtensionAllowed(filename)) return res.status(400).json({ error: "file-type-not-allowed" });

    const targetDir = safeJoinUnix(ctx.baseDir, relPath || "");


    const form = new (getLazyFormData())();
    form.append("file", fs.createReadStream(tmpPath), { filename });
    form.append("dir", targetDir);

    const baseUrl = buildNodeBaseUrl(ctx.node.address, ctx.node.api_port || 8080, ctx.node.ssl_enabled);
    const uploadUrl = `${baseUrl}/v1/fs/upload`;
    const headers = nodeAuthHeadersFor(ctx.node, true);

    const formHeaders = form.getHeaders();
    Object.assign(headers, formHeaders);

    const lib = uploadUrl.startsWith("https:") ? https : httpMod;
    const parsedUrl = new URL(uploadUrl);
    const requestOptions = {
      method: "POST",
      hostname: parsedUrl.hostname,
      port: parsedUrl.port || (uploadUrl.startsWith("https:") ? 443 : 80),
      path: parsedUrl.pathname + parsedUrl.search,
      headers: headers,
      timeout: 3600000
    };

    await new Promise((resolve, reject) => {
      const nodeReq = lib.request(requestOptions, (response) => {
        const MAX_ERR_BYTES = 64 * 1024;
        const chunks = [];
        let total = 0;
        response.on("data", (d) => {
          if (total < MAX_ERR_BYTES) { chunks.push(d); total += d.length; }
        });
        response.on("end", () => {
          if (response.statusCode === 200) {
            resolve();
          } else {
            const body = Buffer.concat(chunks).toString().slice(0, 2000);
            const err = new Error(`Node upload failed: ${response.statusCode} ${body}`);
            err.nodeStatusCode = response.statusCode;
            err.nodeBody = body;
            reject(err);
          }
        });
      });

      nodeReq.on("error", (err) => reject(err));
      nodeReq.on("timeout", () => {
        nodeReq.destroy();
        reject(new Error("Upload request timed out"));
      });

      form.pipe(nodeReq);
    });


    if (req.headers?.accept?.includes("text/html")) return res.redirect("/");
    return res.json({ ok: true, msg: "Uploaded to node", path: path.posix.join(relPath || "", filename) });

  } catch (e) {
    console.error("[upload->node] failed:", e);
    if (e.nodeStatusCode === 507) {
      try {
        const nodeJson = JSON.parse(e.nodeBody);
        if (nodeJson.error === "disk_limit_exceeded") {
          return res.status(507).json(nodeJson);
        }
      } catch { }
    }
    return res.status(502).json({ error: "Failed to upload to node" });
  } finally {
    if (tmpPath) {
      try { await fsp.unlink(tmpPath); } catch { }
    }
  }
});

app.post("/extract", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });

  const bot = String(req.body?.bot || "").trim();
  const relPath = String(req.body?.path || "").trim();
  if (!bot || !relPath) return res.status(400).json({ error: "missing bot or path" });

  if (!requireActionTokenOr403(req, res, "POST /extract", { serverName: bot })) return;

  if (!(await isAdmin(req)) && !(await userHasAccessToServer(req.session.user, bot))) {
    return res.status(403).json({ error: "no access to server" });
  }

  const perms = await getEffectivePermsForUserOnServer(req.session.user, bot);
  if (!perms.files_create && !(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });

  const ctx = await resolveRemoteFsContext(bot);
  if (!ctx.remote) return res.status(404).json({ error: "server not found on node" });

  try {
    const full = safeJoinUnix(ctx.baseDir, relPath);
    const { status, json } = await nodeFsPost(ctx.node, "/v1/fs/extract", { path: full }, 300_000);
    if (status === 507 && json?.error === "disk_limit_exceeded") {
      return res.status(507).json(json);
    }
    if (status !== 200 || !json || !json.ok) return res.status(502).json({ error: "node_extract_failed" });
    return res.json({ ok: true, msg: "Extracted successfully" });
  } catch (e) {
    return res.status(400).json({ error: maskPathErrorMessage(e, "invalid path", "invalid path") });
  }
});

app.post("/archive", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });

  const bot = String(req.body?.bot || "").trim();
  const paths = req.body?.paths;
  const destDir = String(req.body?.destDir || "").trim();

  if (!bot || !Array.isArray(paths) || paths.length === 0) {
    return res.status(400).json({ error: "missing bot or paths" });
  }

  if (!requireActionTokenOr403(req, res, "POST /archive", { serverName: bot })) return;

  if (!(await isAdmin(req)) && !(await userHasAccessToServer(req.session.user, bot))) {
    return res.status(403).json({ error: "no access to server" });
  }

  const perms = await getEffectivePermsForUserOnServer(req.session.user, bot);
  if (!perms.files_archive && !(await isAdmin(req))) {
    return res.status(403).json({ error: "not authorized", permission: "files_archive" });
  }

  const ctx = await resolveRemoteFsContext(bot);
  if (!ctx.remote) return res.status(404).json({ error: "server not found on node" });

  try {
    const botSeg = encodeURIComponent(bot);
    const { status, json } = await nodeFsPost(ctx.node, `/v1/servers/${botSeg}/files/archive`, { paths, destDir }, 300_000);
    if (status === 507 && json?.error === "disk_limit_exceeded") {
      return res.status(507).json(json);
    }
    if (status !== 200 || !json || !json.ok) {
      return res.status(502).json({ error: json?.error || "archive failed" });
    }
    return res.json({ ok: true, path: json.path, name: json.name });
  } catch (e) {
    return res.status(400).json({ error: maskPathErrorMessage(e, "invalid path", "archive failed") });
  }
});

app.post("/create", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).send("Not authenticated");

  const bot = String(req.body?.bot || "").trim();
  const type = String(req.body?.type || "").trim().toLowerCase();
  const name = String(req.body?.name || "").trim();
  const relPath = String(req.body?.path || "").trim();

  if (!bot || !type || !name) return res.status(400).send("Missing fields");
  if (!(await isAdmin(req)) && !(await userHasAccessToServer(req.session.user, bot))) {
    return res.status(403).send("Not authorized");
  }

  const perms = await getEffectivePermsForUserOnServer(req.session.user, bot);
  if (!perms.files_create && !(await isAdmin(req))) return res.status(403).send("Not authorized");

  if (!["file", "folder"].includes(type)) return res.status(400).send("Invalid type");
  if (name.includes("..") || /[\\/]/.test(name)) return res.status(400).send("Invalid name");

  const ctx = await resolveRemoteFsContext(bot);
  if (!ctx.remote) return res.status(404).send("Server not found on node");

  try {
    const relativePosix = path.posix.join(relPath || "", name);
    const target = safeJoinUnix(ctx.baseDir, relativePosix);

    const payload = type === "folder"
      ? { path: safeJoinUnix(target, ".keep"), content: "", encoding: "utf8" }
      : { path: target, content: "", encoding: "utf8" };

    const { status, json } = await nodeFsPost(ctx.node, "/v1/fs/write", payload, 120000);
    if (status === 507 && json?.error === "disk_limit_exceeded") {
      return res.status(507).json(json);
    }
    if (status !== 200 || !json || !json.ok) throw new Error("node-create-failed");
    return res.json({ ok: true, path: relativePosix });
  } catch (e) {
    return res.status(400).send(maskPathErrorMessage(e, "Invalid path", "Create failed"));
  }
});

app.post("/rename", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).send("Not authenticated");

  const bot = String(req.body?.bot || "").trim();
  const oldPath = String(req.body?.oldPath || "").trim();
  const newName = String(req.body?.newName || "").trim();

  if (!bot || !oldPath || !newName) return res.status(400).send("Missing fields");
  if (!(await isAdmin(req)) && !(await userHasAccessToServer(req.session.user, bot))) {
    return res.status(403).send("Not authorized");
  }

  const perms = await getEffectivePermsForUserOnServer(req.session.user, bot);
  if (!perms.files_rename) return res.status(403).send("Not authorized");

  if (newName.includes("..") || /[\\/]/.test(newName)) return res.status(400).send("Invalid new name");

  const ctx = await resolveRemoteFsContext(bot);
  if (!ctx.remote) return res.status(404).send("Server not found on node");

  try {
    const relative = path.posix.join(path.posix.dirname(path.posix.join(oldPath)), newName);
    const src = safeJoinUnix(ctx.baseDir, oldPath);
    const dest = safeJoinUnix(ctx.baseDir, relative);

    const { status, json } = await nodeFsPost(ctx.node, "/v1/fs/rename", { src, dest }, 120000);
    if (status !== 200 || !json || !json.ok) return res.status(500).send("Rename failed");
    return res.json({ ok: true, path: relative });
  } catch (e) {
    return res.status(400).send(maskPathErrorMessage(e, "Invalid path", "Invalid path"));
  }
});

const nodeVersions = ["14", "16", "18", "20"];

app.get("/server/:server/resources", async (req, res) => {
  try {
    if (!(await isAuthenticated(req))) return res.redirect("/login");

    const requested = String(req.params.server || "");
    const botName = sanitizeServerName(requested) || requested;
    const entry = await findServer(botName);

    if (!entry) return res.status(404).send("Server not found");

    const hasAccess = (await isAdmin(req)) || (await userHasAccessToServer(req.session.user, botName));
    if (!hasAccess) return res.status(403).send("Unauthorized");

    return res.render("resource-popup", { bot: botName, cspNonce: res.locals.cspNonce });
  } catch (err) {
    console.error(err);
    return res.status(500).send("Internal Server Error");
  }
});

app.get("/server/:server", async (req, res, next) => {
  try {
    if (!(await isAuthenticated(req))) return res.redirect("/login");

    const requested = String(req.params.server || "");
    const botName = sanitizeServerName(requested) || requested;
    const entry = await findServer(botName);

    if (!entry) {
      res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");
      return res.status(404).render("404", { cspNonce: res.locals.cspNonce });
    }

    const hasAccess = (await isAdmin(req)) || (await userHasAccessToServer(req.session.user, botName));
    if (!hasAccess) {
      res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");
      return res.status(404).render("404", { cspNonce: res.locals.cspNonce });
    }

    const activeTransfer = getTransferJob(botName);
    const isTransferring = (entry && String(entry.status || "").toLowerCase() === "transferring") || (activeTransfer && !activeTransfer.finishedAt);
    if (isTransferring) {
      const botNameHtml = escapeHtml(botName);
      const html = `<!doctype html><html><head><meta charset="utf-8" /><meta name="viewport" content="width=device-width,initial-scale=1" /><title>ADPanel - Transferring</title><style>html,body{height:100%;margin:0}body{display:flex;align-items:center;justify-content:center;background:linear-gradient(180deg,#9ca3af 0%, #111827 100%);font-family:system-ui,-apple-system,Segoe UI,Roboto,Inter,Arial;color:#fff} .box{max-width:560px;padding:28px 26px;border-radius:16px;background:rgba(0,0,0,0.35);backdrop-filter:blur(10px);box-shadow:0 20px 80px rgba(0,0,0,0.45);text-align:center} .title{font-weight:800;font-size:22px;margin:0 0 8px 0} .sub{opacity:.9;margin:0 0 18px 0} .bar{height:10px;border-radius:999px;background:rgba(255,255,255,0.18);overflow:hidden} .fill{height:100%;width:${Math.max(5, Math.min(100, Number(activeTransfer?.percent || 0)))}%;background:linear-gradient(90deg,#e5e7eb,#ffffff)} .hint{margin-top:14px;font-size:13px;opacity:.85}</style></head><body><div class="box"><div class="title">Server is transferring</div><div class="sub">${botNameHtml} is being moved to another node. Please wait…</div><div class="bar"><div class="fill"></div></div><div class="hint">Refresh in a moment. This page will be available once the transfer finishes.</div></div></body></html>`;
      return res.status(200).send(html);
    }

    const email = req.session.user;
    const userObj = email ? await findUserByEmail(email) : null;
    const avatarUrl = resolveUserAvatarUrl(userObj);

    return res.render("bot", { bot: botName, displayName: entry.displayName || botName, nodeVersions, avatarUrl, user: userObj });
  } catch (err) {
    return next(err);
  }
});

app.get("/bot/:bot", (req, res) => {
  res.redirect(301, "/server/" + encodeURIComponent(req.params.bot));
});

async function resolveTemplateForBot(bot) {
  const entry = await findServer(bot);
  const explicit = normalizeTemplateId(entry?.template);
  if (explicit) return { entry, template: explicit };
  const start = String(entry?.start || "").toLowerCase();
  if (start.endsWith(".jar")) return { entry, template: "minecraft" };
  if (start.endsWith(".js") || start.endsWith(".ts")) return { entry, template: "nodejs" };
  if (start.endsWith(".py")) return { entry, template: "python" };
  return { entry, template: "" };
}

function setBodyBackgroundInFile(filePath, cssVal) {
  try {
    let css = fs.readFileSync(filePath, "utf8");

    const varRe = /(--ui-bg\s*:\s*)([^;]*)(;)/;
    if (varRe.test(css)) {
      css = css.replace(varRe, `$1${cssVal}$3`);
    } else {
      if (/:root\s*\{/.test(css)) {
        css = css.replace(/:root\s*\{/, `:root{\n  --ui-bg: ${cssVal};`);
      } else {
        css += `\n:root{ --ui-bg: ${cssVal}; }\n`;
      }
    }

    if (!/body\s*\{[^}]*background\s*:/.test(css)) {
      css += `\nbody{ background: var(--ui-bg) center/cover no-repeat fixed !important; }\n`;
    }

    fs.writeFileSync(filePath, css, "utf8");
    return true;
  } catch (e) {
    console.error("[setBodyBackgroundInFile] failed:", filePath, e);
    return false;
  }
}

app.get("/api/servers/:bot/versions", async (req, res, next) => {
  try {
    if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });

    const bot = String(req.params.bot || "").trim();
    const safeBotName = sanitizeServerName(bot) || bot;
    const { entry, template } = await resolveTemplateForBot(safeBotName);
    const preferredTemplate = normalizeTemplateId(req.query.template || req.query.tpl || "");
    const isAdminUser = await isAdmin(req);

    if (!entry) return res.status(404).json({ error: "server-not-found" });

    const serverName = entry.name || safeBotName;
    if (!isAdminUser && !(await userHasAccessToServer(req.session.user, serverName))) {
      return res.status(403).json({ error: "no-access-to-server" });
    }
    if (!isAdminUser) {
      const perms = await getEffectivePermsForUserOnServer(req.session.user, serverName);
      if (!perms.store_access) return res.status(403).json({ error: "not-authorized", permission: "store_access" });
    }

    const effectiveTemplate = preferredTemplate || template;

    const providers = providersForTemplate(effectiveTemplate).map(p => ({
      id: p.id,
      name: p.name,
      description: p.description,
      logo: p.logo,
    }));

    return res.json({ providers });
  } catch (err) {
    return next(err);
  }
});

app.get("/api/servers/:bot/versions/:providerId", async (req, res, next) => {
  try {
    if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });

    const bot = String(req.params.bot || "").trim();
    const safeBotName = sanitizeServerName(bot) || bot;
    const providerId = String(req.params.providerId || "").trim();
    const isAdminUser = await isAdmin(req);

    const { entry, template } = await resolveTemplateForBot(safeBotName);
    const preferredTemplate = normalizeTemplateId(req.query.template || req.query.tpl || "");
    if (!entry) return res.status(404).json({ error: "server-not-found" });

    const serverName = entry.name || safeBotName;
    if (!isAdminUser && !(await userHasAccessToServer(req.session.user, serverName))) {
      return res.status(403).json({ error: "no-access-to-server" });
    }
    if (!isAdminUser) {
      const perms = await getEffectivePermsForUserOnServer(req.session.user, serverName);
      if (!perms.store_access) return res.status(403).json({ error: "not-authorized", permission: "store_access" });
    }

    const effectiveTemplate = preferredTemplate || template;

    const provider = providersForTemplate(effectiveTemplate).find(p => p.id === providerId);
    if (!provider) return res.status(404).json({ error: "provider-not-found" });

    if (provider.id === "python") {
      try {
        const tags = await fetchPythonTagsFromGitHub();
        const versions = mapPythonTagsToVersions(tags);
        return res.json({ provider: provider.id, displayName: provider.name, description: provider.description, versions });
      } catch {
        return res.status(502).json({ error: "python-versions-unavailable" });
      }
    }

    if (provider.id === "nodejs") {
      try {
        const idx = await fetchNodeVersionsIndex();
        const versions = mapNodeVersionsToList(idx);
        return res.json({ provider: provider.id, displayName: provider.name, description: provider.description, versions });
      } catch {
        return res.status(502).json({ error: "node-versions-unavailable" });
      }
    }

    return res.json({
      provider: provider.id,
      displayName: provider.name,
      description: provider.description,
      versions: provider.versions || [],
    });
  } catch (err) {
    return next(err);
  }
});

app.get("/api/papermc/*", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not-authenticated" });
  const subPath = req.params[0] || "";
  if (subPath.includes("..") || subPath.length > 300) return res.status(400).json({ error: "invalid-path" });
  const upstream = `https://api.papermc.io/v2/${subPath}`;
  try {
    const r = await httpRequestJson(upstream, "GET", { "Accept": "application/json" }, null, 15000);
    if (!r.json) return res.status(502).json({ error: "upstream-error" });
    return res.status(r.status || 200).json(r.json);
  } catch {
    return res.status(502).json({ error: "proxy-failed" });
  }
});

app.get("/api/purpurmc/*", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not-authenticated" });
  const subPath = req.params[0] || "";
  if (subPath.includes("..") || subPath.length > 300) return res.status(400).json({ error: "invalid-path" });
  const upstream = `https://api.purpurmc.org/v2/${subPath}`;
  try {
    const r = await httpRequestJson(upstream, "GET", { "Accept": "application/json" }, null, 15000);
    if (!r.json) return res.status(502).json({ error: "upstream-error" });
    return res.status(r.status || 200).json(r.json);
  } catch {
    return res.status(502).json({ error: "proxy-failed" });
  }
});

app.post("/api/servers/:bot/versions/apply", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ ok: false, error: "not-authenticated" });

  const bot = String(req.params.bot || "").trim();
  if (!bot) return res.status(400).json({ ok: false, error: "missing-bot" });

  if (!requireActionTokenOr403(req, res, "POST /api/servers/:bot/versions/apply", { serverName: bot })) return;
  const isAdminUser = await isAdmin(req);

  if (!isAdminUser && !(await userHasAccessToServer(req.session.user, bot))) {
    return res.status(403).json({ ok: false, error: "no-access-to-server" });
  }
  if (!isAdminUser) {
    const perms = await getEffectivePermsForUserOnServer(req.session.user, bot);
    if (!perms.store_access) return res.status(403).json({ ok: false, error: "not-authorized", permission: "store_access" });
  }

  const entry = await findServer(bot);
  if (!entry) return res.status(404).json({ ok: false, error: "server-not-found" });
  if (!isRemoteEntry(entry)) return res.status(400).json({ ok: false, error: "server-not-on-node" });

  const node = await findNodeByIdOrName(entry.nodeId);
  if (!node) return res.status(400).json({ ok: false, error: "node-not-found" });

  const baseUrl = buildNodeBaseUrl(node.address, node.api_port || NODE_AGENT_PORT, node.ssl_enabled);
  if (!baseUrl) return res.status(400).json({ ok: false, error: "invalid-node-address" });

  const { providerId, versionId, url: bodyUrl, destPath: rawDestPath } = req.body || {};

  const serverTemplate = normalizeTemplateId(entry.template);
  const preferredTemplate = normalizeTemplateId(req.body?.template || req.body?.tpl || "");
  const runtimeTemplate = preferredTemplate || serverTemplate || ((providerId === "nodejs" || providerId === "python") ? "discord-bot" : "");
  const headers = nodeAuthHeadersFor(node, true);

  try {
    if (runtimeTemplate && runtimeTemplate !== "minecraft") {
      const providerCfg = findProviderConfig(providerId);
      if (!providerCfg || (runtimeTemplate && !providerSupportsTemplate(providerCfg, runtimeTemplate))) {
        return res.status(400).json({ ok: false, error: "provider-not-supported" });
      }

      let versionCfg = findProviderVersionConfig(providerId, versionId);
      if (!versionCfg && providerId === "python") versionCfg = buildPythonVersionConfig(versionId, entry);
      if (!versionCfg && providerId === "nodejs") versionCfg = buildNodeVersionConfig(versionId, entry);

      if (!versionCfg) return res.status(404).json({ ok: false, error: "version-not-found" });

      const dockerCfg = versionCfg.docker || {};
      if (!dockerCfg.image || !dockerCfg.tag) return res.status(400).json({ ok: false, error: "missing-docker-config" });

      const startFile = versionCfg.start || entry.start || (providerId === "python" ? "main.py" : "index.js");
      const port = clampAppPort(entry.port ?? 3001, 3001);

      const runtime = {
        providerId: providerCfg.id,
        versionId: versionCfg.id || versionId,
        image: dockerCfg.image,
        tag: dockerCfg.tag,
        command: dockerCfg.command || null,
        env: dockerCfg.env || {},
        volumes: dockerCfg.volumes || null,
      };

      const forwardUrl = `${baseUrl}/v1/servers/${encodeURIComponent(bot)}/runtime`;
      const payload = { runtime, template: entry.template || runtimeTemplate || "nodejs", start: startFile, port };

      const r = await httpRequestJson(forwardUrl, "POST", headers, payload, 60_000);
      if (r.status !== 200 || !(r.json && r.json.ok)) {
        const detail = (r.json && (r.json.detail || r.json.error)) || `node-status-${r.status}`;
        console.error(`[versions/apply] runtime forward failed: status=${r.status}, error=${detail}, node=${node.name || node.address}`);
        if (r.status === 401 || r.status === 0) {
          return res.status(502).json({ ok: false, error: "node-auth-failed", detail: "Token mismatch. Re-deploy config.yml to the node." });
        }
        return res.status(502).json({ ok: false, error: detail || `node-runtime-forward-failed-${r.status}` });
      }

      await upsertServerIndexEntry({ ...entry, runtime, start: startFile, port });
      return res.json({ ok: true, remote: true, msg: "runtime-updated", runtime });
    }

    let url = (bodyUrl && String(bodyUrl).trim()) || "";
    if (!url) {
      if (!providerId || !versionId) return res.status(400).json({ ok: false, error: "missing-params" });
      const vcfg = findProviderVersionConfig(providerId, versionId);
      url = vcfg?.url || vcfg?.link || vcfg?.download || vcfg?.href || "";
      if (!url) return res.status(404).json({ ok: false, error: "version-url-not-found" });
    }

    try {
      const safe = await assertSafeRemoteUrl(url);
      url = safe.toString();
    } catch {
      return res.status(400).json({ ok: false, error: "invalid-url" });
    }

    let destRel = String(rawDestPath || "").trim();
    if (!destRel) destRel = "server.jar";
    destRel = destRel.replace(/^\/+/, "");
    if (destRel.includes("..") || destRel.includes("\\")) return res.status(400).json({ ok: false, error: "invalid-destPath" });
    const destFile = path.posix.basename(destRel);
    if (!destFile || destFile === "." || destFile === "..") return res.status(400).json({ ok: false, error: "invalid-destPath" });

    const applyResult = await applyRemoteAssetToServer({
      findServer,
      isRemoteEntry,
      findNodeByIdOrName,
      buildNodeBaseUrl,
      nodeAuthHeadersFor,
      httpRequestJson,
      assertSafeRemoteUrl,
      resolveRemoteFsContext,
      nodeFsPost,
      httpGetRaw,
      remoteApplyProxyDownload: REMOTE_APPLY_PROXY_DOWNLOAD,
      remoteApplyMaxBytes: REMOTE_APPLY_MAX_BYTES,
      remoteApplyTimeoutMs: REMOTE_APPLY_TIMEOUT_MS,
      remoteFetchMaxRedirects: REMOTE_FETCH_MAX_REDIRECTS,
    }, {
      serverName: bot,
      entry,
      url,
      destPath: destRel,
    });

    if (!applyResult.ok) {
      return res.status(applyResult.statusCode || 500).json(applyResult);
    }
    return res.json(applyResult);
  } catch (e) {
    console.error("versions/apply error", e);
    return res.status(500).json({ ok: false, error: "server-error" });
  }
});

app.post("/api/servers/create", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  if (!(await isAdmin(req))) return res.status(403).json({ error: "admin required" });

  const { name: rawName, displayName: clientDisplayName, templateId, nodeId } = req.body || {};
  const name = sanitizeServerName(rawName);
  if (!name) return res.status(400).json({ error: "invalid name" });

  if (serverCreateLocks.has(name)) {
    return res.status(429).json({ error: "action-in-progress", message: `Server "${name}" is already being created. Please choose a different name or wait.` });
  }
  serverCreateLocks.add(name);

  try {
    const displayName = sanitizeDisplayName(clientDisplayName || rawName) || name;
    if (!templateId) return res.status(400).json({ error: "missing templateId" });

    const template = findTemplateById(templateId);
    if (!template) return res.status(400).json({ error: "unknown template" });

    const targetNodeId = String(nodeId || "").trim();
    if (!targetNodeId || targetNodeId === "local") return res.status(400).json({ error: "nodeId required" });

    const node = await findNodeByIdOrName(targetNodeId);
    if (!node) return res.status(400).json({ error: "node not found" });

    const mcFork = req.body?.mcFork ? String(req.body.mcFork).toLowerCase() : "paper";
    const mcVersion = req.body?.mcVersion ? String(req.body.mcVersion).trim() : "1.21.8";
    let importUrl = req.body?.importUrl ? String(req.body.importUrl).trim() : null;
    if (importUrl) {
      if (!isValidArchiveUrl(importUrl)) {
        return res.status(400).json({ error: "Invalid importUrl (must be http(s) and a supported archive extension)" });
      }
      try {
        await assertSafeRemoteUrl(importUrl);
      } catch (e) {
        return res.status(400).json({ error: `Unsafe importUrl: ${e.message}` });
      }
    }

    const reqResources = req.body?.resources || {};
    const resources = {};
    const parsePositiveIntOrNull = (value) => {
      if (value === null || value === undefined || String(value).trim() === "") return null;
      const parsed = parseInt(value, 10);
      return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
    };
    const parsePositiveFloatOrNull = (value) => {
      if (value === null || value === undefined || String(value).trim() === "") return null;
      const parsed = parseFloat(value);
      return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
    };
    const effectiveNodeCapacity = await getEffectiveNodeCapacity(node);
    const requestedRamMb = parsePositiveIntOrNull(reqResources.ramMb);
    const requestedCpuCores = parsePositiveFloatOrNull(reqResources.cpuCores);
    const requestedStorageMb = parsePositiveIntOrNull(reqResources.storageMb);
    const requestedStorageGb = parsePositiveIntOrNull(reqResources.storageGb);
    const effectiveDiskMb = effectiveNodeCapacity.diskGb > 0 ? Math.trunc(effectiveNodeCapacity.diskGb * 1024) : 0;
    if (requestedRamMb != null) {
      if (effectiveNodeCapacity.ramMb > 0 && requestedRamMb > effectiveNodeCapacity.ramMb) {
        return res.status(400).json({ error: `RAM limit cannot exceed node maximum (${effectiveNodeCapacity.ramMb} MB).` });
      }
      resources.ramMb = requestedRamMb;
    } else if (effectiveNodeCapacity.ramMb > 0) {
      resources.ramMb = effectiveNodeCapacity.ramMb;
    }
    if (requestedCpuCores != null) {
      if (effectiveNodeCapacity.cpuCores > 0 && requestedCpuCores > effectiveNodeCapacity.cpuCores) {
        return res.status(400).json({ error: `CPU cores cannot exceed node maximum (${effectiveNodeCapacity.cpuCores} cores).` });
      }
      resources.cpuCores = requestedCpuCores;
    } else if (effectiveNodeCapacity.cpuCores > 0) {
      resources.cpuCores = effectiveNodeCapacity.cpuCores;
    }
    if (requestedStorageMb != null) {
      if (effectiveDiskMb > 0 && requestedStorageMb > effectiveDiskMb) {
        return res.status(400).json({ error: `Storage limit cannot exceed node maximum (${effectiveDiskMb} MB).` });
      }
      resources.storageMb = requestedStorageMb;
    } else if (requestedStorageGb != null) {
      const requestedStorageFromGb = requestedStorageGb * 1024;
      if (effectiveDiskMb > 0 && requestedStorageFromGb > effectiveDiskMb) {
        return res.status(400).json({ error: `Storage limit cannot exceed node maximum (${effectiveDiskMb} MB).` });
      }
      resources.storageMb = requestedStorageFromGb;
    } else if (effectiveDiskMb > 0) {
      resources.storageMb = effectiveDiskMb;
    }
    if (reqResources.swapMb != null) resources.swapMb = parseInt(reqResources.swapMb, 10);
    if (reqResources.backupsMax != null) resources.backupsMax = parseInt(reqResources.backupsMax, 10) || 0;
    if (reqResources.maxSchedules != null) resources.maxSchedules = parseInt(reqResources.maxSchedules, 10) || 0;
    try {
      Object.assign(resources, collectResourcePerformanceOptions(reqResources));
    } catch (e) {
      return res.status(400).json({ error: e.message || "Invalid performance resources." });
    }

    let sanitizedDocker = normalizeTemplateDockerForRuntime(templateId, template.docker);
    if (sanitizedDocker && Array.isArray(sanitizedDocker.ports) && sanitizedDocker.ports.length > 1) {
      sanitizedDocker.ports = [sanitizedDocker.ports[0]];
    }
    if (req.body?.startupCommand && String(req.body.startupCommand).trim()) {
      return res.status(400).json({ error: "Raw Docker startup commands are no longer supported. Server containers are now created from structured template settings only." });
    }
    const requestedCommand = normalizeLegacyRuntimeProcessCommandInput(templateId, req.body?.command);
    const requestedCommandError = validateRuntimeProcessCommandInput(requestedCommand);
    if (requestedCommandError) {
      return res.status(400).json({ error: requestedCommandError });
    }
    if (requestedCommand) {
      sanitizedDocker.command = requestedCommand;
    }

    let hostPortRaw = (req.body && (req.body.hostPort ?? req.body.port)) ?? null;
    if (hostPortRaw != null && String(hostPortRaw).trim() !== "") {
      const parsedHostPort = validatePort(hostPortRaw);
      if (!parsedHostPort) {
        return res.status(400).json({ error: "Invalid port. Choose a value between 1 and 65535." });
      }
      hostPortRaw = parsedHostPort;
    } else {
      hostPortRaw = validatePort(template.defaultPort);
    }

    const resolvedPort = hostPortRaw != null ? hostPortRaw : (validatePort(template.defaultPort) || 0);
    if (resolvedPort > 0 && !isPortInNodeAllocation(node, resolvedPort)) {
      const alloc = node.ports || {};
      let allocDesc = "none";
      if (alloc.mode === "range") allocDesc = `range ${alloc.start}–${alloc.start + alloc.count - 1}`;
      else if (alloc.mode === "list" && Array.isArray(alloc.ports)) allocDesc = `list [${alloc.ports.slice(0, 10).join(", ")}${alloc.ports.length > 10 ? "…" : ""}]`;
      return res.status(400).json({ error: `Port ${resolvedPort} is not in this node's allocated ports (${allocDesc}). Choose a port within the node's allocation.` });
    }

    const tpl = normalizeTemplateId(templateId);

    const payload = {
      name,
      templateId,
      mcFork,
      mcVersion,
      hostPort: hostPortRaw,
      docker: sanitizedDocker,
      autoStart: true,
      importUrl: importUrl,
      resources: Object.keys(resources).length > 0 ? resources : null
    };
    const createResult = await createOnRemoteNode(node, payload);

    try {
      const me = req.session.user;
      const u = await findUserByEmail(me);
      if (!(u && u.admin)) await addAccessForEmail(me, name);
    } catch { }

    let savedPort = null;
    if (hostPortRaw != null) {
      savedPort = clampPort(hostPortRaw);
    } else if (validatePort(template.defaultPort)) {
      savedPort = clampPort(validatePort(template.defaultPort));
    } else if (tpl === "minecraft") {
      savedPort = 25565;
    } else if (tpl === "nodejs" || tpl === "discord-bot") {
      savedPort = 3000;
    }

    const serverEntry = {
      name,
      displayName,
      template: templateId,
      mcFork: tpl === "minecraft" ? mcFork : undefined,
      mcVersion: tpl === "minecraft" ? mcVersion : undefined,
      start: tpl === "minecraft"
        ? "server.jar"
        : (tpl === "nodejs" || tpl === "discord-bot"
          ? "index.js"
          : (tpl === "python" ? "main.py" : null)),
      nodeId: node.uuid || node.id || node.name,
      ip: node.address || null,
    };
    if (savedPort != null) serverEntry.port = savedPort;
    if (Object.keys(resources).length > 0) serverEntry.resources = resources;
    if (sanitizedDocker) serverEntry.docker = sanitizedDocker;
    if (sanitizedDocker) {
      serverEntry.runtime = {
        image: sanitizedDocker.image,
        tag: sanitizedDocker.tag,
        command: sanitizedDocker.command,
        volumes: sanitizedDocker.volumes,
        workdir: sanitizedDocker.workdir,
        env: sanitizedDocker.env,
        ports: sanitizedDocker.ports,
      };
    }

    await upsertServerIndexEntry(serverEntry);

    return res.json({ ok: true, name, displayName });
  } catch (e) {
    console.error("[/api/servers/create] failed:", e);
    return res.status(500).json({
      error: "create failed",
      detail: String(e && e.message ? e.message : "").trim() || null
    });
  } finally {
    serverCreateLocks.delete(name);
  }
});

const ALLOWED_ARCHIVE_EXTENSIONS = ['.zip', '.rar', '.7z', '.tar.gz', '.tgz', '.tar.bz2', '.tbz2'];

function isValidArchiveUrl(url) {
  if (!url) return false;
  try {
    const parsedUrl = new URL(url);
    if (!['http:', 'https:'].includes(parsedUrl.protocol)) return false;
    const pathname = parsedUrl.pathname.toLowerCase();
    return ALLOWED_ARCHIVE_EXTENSIONS.some(ext => pathname.endsWith(ext));
  } catch {
    return false;
  }
}

app.delete("/api/settings/servers/:name", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });
  const result = await deleteServerByName({
    findServer,
    isRemoteEntry,
    findNodeByIdOrName,
    buildNodeBaseUrl,
    nodeAuthHeadersFor,
    httpRequestJson,
    deleteServerSchedules,
    removeServerIndexEntry,
    removeAccessForServerName,
  }, req.params.name);
  if (!result.ok) {
    return res.status(result.statusCode || 500).json({ error: result.error || "delete failed" });
  }
  return res.json(result);
});

app.post("/api/settings/servers/:name/template", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });

  let nameParam = req.params.name || "";
  try { nameParam = decodeURIComponent(nameParam); } catch { }
  const name = String(nameParam).trim();
  if (!name) return res.status(400).json({ error: "missing name" });

  const rawTemplate = (req.body && (req.body.templateId || req.body.template)) || "";
  const templateId = normalizeTemplateId(rawTemplate);
  if (!templateId) return res.status(400).json({ error: "invalid template id" });

  const template = findTemplateById(templateId);
  if (!template) return res.status(404).json({ error: "template not found" });

  const list = await loadServersIndex();
  const idx = list.findIndex(e => e && e.name === name);
  if (idx < 0) return res.status(404).json({ error: "server not found" });

  const entry = list[idx];
  const technicalName = getTechnicalServerName(entry, name);

  if (isRemoteEntry(entry)) {
    const node = await findNodeByIdOrName(entry.nodeId);
    if (!node) return res.status(400).json({ error: "node not found for server" });

    const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
    if (!baseUrl) return res.status(400).json({ error: "invalid node address" });

    const headers = nodeAuthHeadersFor(node, true);

    let hostPort = entry.port || template.defaultPort || 8080;
    const normalizedTemplateDocker = normalizeTemplateDockerForRuntime(
      templateId,
      template.docker,
      defaultStartFileForTemplate(templateId)
    );

    const reinstallPayload = {
      templateId,
      docker: normalizedTemplateDocker || null,
      hostPort,
    };

    if (!signPanelAdminReinstallHeaders(headers, node, technicalName, templateId)) {
      return res.status(500).json({ error: "failed to sign reinstall request" });
    }

    console.log(`[template-change] Reinstalling server ${name} on node ${entry.nodeId} with template ${templateId}`);

    const r = await httpRequestJson(
      `${baseUrl}/v1/servers/${encodeURIComponent(technicalName)}/reinstall`,
      "POST",
      headers,
      reinstallPayload,
      300_000
    );

    if (r.status !== 200 || !(r.json && r.json.ok)) {
      const msg = (r.json && (r.json.error || r.json.detail)) ? (r.json.error || r.json.detail) : `reinstall failed (${r.status})`;
      console.error(`[template-change] Reinstall failed for ${name}: ${msg}`);
      return res.status(502).json({ error: msg });
    }

    console.log(`[template-change] Reinstall successful for ${name}`);
  }

  const updatedEntry = Object.assign({}, entry, {
    template: String(template.id || templateId),
  });
  const normalizedTemplateDocker = normalizeTemplateDockerForRuntime(
    templateId,
    template.docker,
    defaultStartFileForTemplate(templateId)
  );
  if (normalizedTemplateDocker) {
    updatedEntry.docker = normalizedTemplateDocker;
    updatedEntry.runtime = {
      image: normalizedTemplateDocker.image,
      tag: normalizedTemplateDocker.tag,
      command: normalizedTemplateDocker.command,
      volumes: normalizedTemplateDocker.volumes,
      workdir: normalizedTemplateDocker.workdir,
      env: normalizedTemplateDocker.env,
      ports: normalizedTemplateDocker.ports,
      restart: normalizedTemplateDocker.restart,
      console: normalizedTemplateDocker.console,
    };
  }
  delete updatedEntry.startupCommand;
  if (updatedEntry.runtime && typeof updatedEntry.runtime === "object") delete updatedEntry.runtime.startupCommand;
  if (updatedEntry.docker && typeof updatedEntry.docker === "object") delete updatedEntry.docker.startupCommand;
  if (template.defaultPort) {
    updatedEntry.port = template.defaultPort;
  }
  const tpl = normalizeTemplateId(templateId);
  if (tpl === 'minecraft') {
    updatedEntry.start = 'server.jar';
  } else if (tpl === 'nodejs' || tpl === 'discord-bot') {
    updatedEntry.start = 'index.js';
  } else if (tpl === 'python') {
    updatedEntry.start = 'main.py';
  } else {
    updatedEntry.start = null;
  }

  list[idx] = updatedEntry;
  if (!(await saveServersIndex(list))) return res.status(500).json({ error: "failed to save servers" });

  return res.json({ ok: true, server: list[idx] });
});

app.post("/api/servers/:name/reinstall", async (req, res) => {
  try {
    if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });

    let nameParam = req.params.name || "";
    try { nameParam = decodeURIComponent(nameParam); } catch { }
    const name = String(nameParam).trim();
    if (!name) return res.status(400).json({ error: "missing server name" });

    const email = req.session.user;
    const isAdminUser = await isAdmin(req);

    if (!isAdminUser && !(await userHasAccessToServer(email, name))) {
      return res.status(403).json({ error: "no access to server" });
    }

    const perms = await getEffectivePermsForUserOnServer(email, name);
    if (!perms.server_reinstall && !isAdminUser) {
      return res.status(403).json({ error: "you do not have permission to reinstall this server" });
    }

    const entry = await findServer(name);
    if (!entry) return res.status(404).json({ error: "server not found" });
    const technicalName = getTechnicalServerName(entry, name);

    if (!isRemoteEntry(entry)) return res.status(400).json({ error: "server is not on a remote node" });

    const node = await findNodeByIdOrName(entry.nodeId);
    if (!node) return res.status(400).json({ error: "node not found for server" });

    const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
    if (!baseUrl) return res.status(400).json({ error: "invalid node address" });

    const headers = nodeAuthHeadersFor(node, true);

    const templateId = normalizeTemplateId(entry.template) || "custom";
    const reinstallPayload = {
      templateId,
    };

    if (!signPanelAdminReinstallHeaders(headers, node, technicalName, templateId)) {
      return res.status(500).json({ error: "failed to sign reinstall request" });
    }

    console.log(`[reinstall] User ${email} (admin=${isAdminUser}) reinstalling server ${name} with template ${templateId}`);

    const r = await httpRequestJson(
      `${baseUrl}/v1/servers/${encodeURIComponent(technicalName)}/reinstall`,
      "POST",
      headers,
      reinstallPayload,
      300_000
    );

    if (r.status !== 200 || !(r.json && r.json.ok)) {
      const msg = (r.json && (r.json.error || r.json.detail)) ? (r.json.error || r.json.detail) : `reinstall failed (${r.status})`;
      console.error(`[reinstall] Failed for ${name}: ${msg}`);
      return res.status(502).json({ error: msg });
    }

    console.log(`[reinstall] Server ${name} reinstalled successfully`);
    const list = await loadServersIndex();
    const idx = list.findIndex(e => e && e.name === name);
    if (idx >= 0) {
      const updatedEntry = Object.assign({}, list[idx]);
      delete updatedEntry.startupCommand;
      if (updatedEntry.runtime && typeof updatedEntry.runtime === "object") {
        updatedEntry.runtime = { ...updatedEntry.runtime };
        delete updatedEntry.runtime.startupCommand;
      }
      if (updatedEntry.docker && typeof updatedEntry.docker === "object") {
        updatedEntry.docker = { ...updatedEntry.docker };
        delete updatedEntry.docker.startupCommand;
      }
      list[idx] = updatedEntry;
      await saveServersIndex(list);
    }
    try {
      if (typeof recordActivity === "function") {
        await recordActivity(name, email, "reinstall", `Server reinstalled by ${email}`);
      }
    } catch (actErr) {
      console.warn("[reinstall] Failed to record activity:", actErr);
    }

    return res.json({ ok: true, name, template: templateId });
  } catch (err) {
    console.error("[reinstall] Error:", err);
    return res.status(500).json({ error: "internal error during reinstall" });
  }
});

app.patch("/api/settings/servers/:name/resources", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });

  let nameParam = req.params.name || "";
  try { nameParam = decodeURIComponent(nameParam); } catch { }
  const name = String(nameParam).trim();
  if (!name) return res.status(400).json({ error: "missing server name" });

  const me = req.session?.user;
  const isAdminUser = await isAdmin(req);
  if (!isAdminUser) {
    const hasAccess = await userHasAccessToServer(me, name);
    if (!hasAccess) return res.status(403).json({ error: "not authorized" });
  }

  const serverEntry = await findServer(name);
  if (!serverEntry) return res.status(404).json({ error: "server not found" });

  if (!isRemoteEntry(serverEntry)) {
    return res.status(400).json({ error: "server is not on a remote node" });
  }

  const node = await findNodeByIdOrName(serverEntry.nodeId);
  if (!node) return res.status(400).json({ error: "node not found" });

  const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
  if (!baseUrl) return res.status(400).json({ error: "invalid node address" });

  const { resources, startupCommand, mainPort, command } = req.body || {};
  if (startupCommand != null && String(startupCommand).trim()) {
    return res.status(400).json({ error: "Editing raw Docker startup commands is no longer supported. Update ports/resources only." });
  }
  const templateForCommand = normalizeTemplateId(serverEntry?.template || serverEntry?.type);
  const cleanCommand = normalizeLegacyRuntimeProcessCommandInput(templateForCommand, command);
  const cleanCommandError = validateRuntimeProcessCommandInput(cleanCommand);
  if (cleanCommandError) {
    return res.status(400).json({ error: cleanCommandError });
  }
  const cleanedResourcePorts = Array.isArray(resources?.ports) ? validatePortListInput(resources.ports) : null;
  if (Array.isArray(resources?.ports) && cleanedResourcePorts === null) {
    return res.status(400).json({ error: "All server ports must be integers between 1 and 65535." });
  }
  if (cleanedResourcePorts) {
    for (const port of cleanedResourcePorts) {
      if (!isPortInNodeAllocation(node, port)) {
        return res.status(400).json({ error: `Port ${port} is not in this node's allocated ports.` });
      }
    }
  }

  const parsePositiveIntOrNull = (value) => {
    if (value === null || value === undefined || String(value).trim() === "") return null;
    const parsed = parseInt(value, 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
  };
  const parsePositiveFloatOrNull = (value) => {
    if (value === null || value === undefined || String(value).trim() === "") return null;
    const parsed = parseFloat(value);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
  };
  const effectiveNodeCapacity = await getEffectiveNodeCapacity(node);
  const requestedRamMb = resources ? parsePositiveIntOrNull(resources.ramMb) : null;
  const requestedCpuCores = resources ? parsePositiveFloatOrNull(resources.cpuCores) : null;
  const requestedStorageMb = resources ? parsePositiveIntOrNull(resources.storageMb) : null;
  const requestedStorageGb = resources ? parsePositiveIntOrNull(resources.storageGb) : null;
  const effectiveDiskMb = effectiveNodeCapacity.diskGb > 0 ? Math.trunc(effectiveNodeCapacity.diskGb * 1024) : 0;
  if (requestedRamMb != null && effectiveNodeCapacity.ramMb > 0 && requestedRamMb > effectiveNodeCapacity.ramMb) {
    return res.status(400).json({ error: `RAM limit cannot exceed node maximum (${effectiveNodeCapacity.ramMb} MB).` });
  }
  if (requestedCpuCores != null && effectiveNodeCapacity.cpuCores > 0 && requestedCpuCores > effectiveNodeCapacity.cpuCores) {
    return res.status(400).json({ error: `CPU cores cannot exceed node maximum (${effectiveNodeCapacity.cpuCores} cores).` });
  }
  if (requestedStorageMb != null && effectiveDiskMb > 0 && requestedStorageMb > effectiveDiskMb) {
    return res.status(400).json({ error: `Storage limit cannot exceed node maximum (${effectiveDiskMb} MB).` });
  }
  if (requestedStorageGb != null && effectiveDiskMb > 0 && requestedStorageGb * 1024 > effectiveDiskMb) {
    return res.status(400).json({ error: `Storage limit cannot exceed node maximum (${effectiveDiskMb} MB).` });
  }
  let requestedPerformanceOptions = {};
  try {
    requestedPerformanceOptions = resources ? collectResourcePerformanceOptions(resources) : {};
  } catch (e) {
    return res.status(400).json({ error: e.message || "Invalid performance resources." });
  }

  const payload = {};
  if (resources) {
    payload.resources = {};
    if (requestedRamMb != null) payload.resources.ramMb = requestedRamMb;
    if (requestedCpuCores != null) payload.resources.cpuCores = requestedCpuCores;
    if (requestedStorageMb != null) payload.resources.storageMb = requestedStorageMb;
    else if (requestedStorageGb != null) payload.resources.storageMb = requestedStorageGb * 1024;
    if (resources.swapMb != null) payload.resources.swapMb = parseInt(resources.swapMb, 10);
    if (resources.backupsMax != null) payload.resources.backupsMax = parseInt(resources.backupsMax, 10) || 0;
    if (resources.maxSchedules != null) payload.resources.maxSchedules = parseInt(resources.maxSchedules, 10) || 0;
    Object.assign(payload.resources, requestedPerformanceOptions);
    if (cleanedResourcePorts) payload.resources.ports = cleanedResourcePorts;
  }
  payload.command = cleanCommand;
  let newMainPort = null;
  if (mainPort != null) {
    const mp = validatePort(mainPort);
    if (!mp) return res.status(400).json({ error: "Main port must be an integer between 1 and 65535." });
    if (!isPortInNodeAllocation(node, mp)) {
      return res.status(400).json({ error: `Port ${mp} is not in this node's allocated ports.` });
    }
    if (cleanedResourcePorts && cleanedResourcePorts.includes(mp)) {
      return res.status(400).json({ error: `Port ${mp} cannot be both the main port and an additional port.` });
    }
    newMainPort = mp;
    payload.mainPort = mp;
  }

  try {
    const headers = nodeAuthHeadersFor(node, true);
    const r = await httpRequestJson(
      `${baseUrl}/v1/servers/${encodeURIComponent(name)}/resources`,
      "PATCH",
      headers,
      payload,
      30_000
    );

    if (r.status !== 200 || !(r.json && r.json.ok)) {
      const msg = (r.json && (r.json.error || r.json.detail)) ? (r.json.error || r.json.detail) : `failed to update resources (${r.status})`;
      return res.status(502).json({ error: msg });
    }

    const list = await loadServersIndex();
    const idx = list.findIndex(e => e && e.name === name);
    if (idx >= 0) {
      if (!list[idx].resources) list[idx].resources = {};
      if (resources) {
        if (resources.ramMb != null) {
          const n = parseInt(resources.ramMb, 10);
          if (!Number.isNaN(n)) list[idx].resources.ramMb = n;
        }
        if (resources.cpuCores != null) {
          const n = parseFloat(resources.cpuCores);
          if (!Number.isNaN(n)) list[idx].resources.cpuCores = n;
        }
        if (resources.storageMb != null) {
          const n = parseInt(resources.storageMb, 10);
          if (!Number.isNaN(n)) list[idx].resources.storageMb = n;
          delete list[idx].resources.storageGb;
        } else if (resources.storageGb != null) {
          const n = parseInt(resources.storageGb, 10);
          if (!Number.isNaN(n)) list[idx].resources.storageMb = n * 1024;
          delete list[idx].resources.storageGb;
        }
        if (resources.swapMb != null) list[idx].resources.swapMb = parseInt(resources.swapMb, 10);
        if (resources.backupsMax != null) list[idx].resources.backupsMax = parseInt(resources.backupsMax, 10) || 0;
        if (resources.maxSchedules != null) list[idx].resources.maxSchedules = parseInt(resources.maxSchedules, 10) || 0;
        Object.assign(list[idx].resources, requestedPerformanceOptions);
        if (cleanedResourcePorts) list[idx].resources.ports = cleanedResourcePorts;
      }
      if (!list[idx].runtime || typeof list[idx].runtime !== "object") list[idx].runtime = {};
      if (!list[idx].docker || typeof list[idx].docker !== "object") list[idx].docker = {};
      list[idx].runtime.command = cleanCommand;
      list[idx].docker.command = cleanCommand;
      if (newMainPort != null) {
        list[idx].port = newMainPort;
      }
      await saveServersIndex(list);
    }

    try {
      await httpRequestJson(`${baseUrl}/v1/servers/${encodeURIComponent(name)}/stop`, "POST", headers, null, 30_000).catch(() => { });
      const effectivePort = newMainPort != null ? newMainPort : ((serverEntry && serverEntry.port != null) ? Number(serverEntry.port) : null);
      const startPayload = (effectivePort != null && Number.isFinite(effectivePort)) ? { hostPort: effectivePort } : {};
      const rr = await httpRequestJson(`${baseUrl}/v1/servers/${encodeURIComponent(name)}/start`, "POST", headers, startPayload, 180_000);
      if (rr.status !== 200 || !(rr.json && (rr.json.ok === true || rr.json.ok === undefined))) {
        const msg = (rr.json && (rr.json.error || rr.json.detail)) ? (rr.json.error || rr.json.detail) : `remote start failed (${rr.status})`;
        return res.status(502).json({ error: msg });
      }
    } catch (e) {
      return res.status(502).json({ error: "resources updated but restart failed", detail: e && e.message });
    }

    return res.json({ ok: true, message: "Resources updated" });
  } catch (e) {
    console.error("[/api/settings/servers/:name/resources] failed:", e);
    return res.status(500).json({ error: "failed to update resources" });
  }
});

app.get("/api/settings/servers/:name/resources", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });

  let nameParam = req.params.name || "";
  try { nameParam = decodeURIComponent(nameParam); } catch { }
  const name = String(nameParam).trim();
  if (!name) return res.status(400).json({ error: "missing server name" });

  const me = req.session?.user;
  const isAdminUser = await isAdmin(req);
  if (!isAdminUser) {
    const hasAccess = await userHasAccessToServer(me, name);
    if (!hasAccess) return res.status(403).json({ error: "not authorized" });
  }

  const serverEntry = await findServer(name);
  if (!serverEntry) return res.status(404).json({ error: "server not found" });

  if (!isRemoteEntry(serverEntry)) {
    return res.status(400).json({ error: "server is not on a remote node" });
  }

  const node = await findNodeByIdOrName(serverEntry.nodeId);
  if (!node) return res.status(400).json({ error: "node not found" });

  const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
  if (!baseUrl) return res.status(400).json({ error: "invalid node address" });

  try {
    const headers = nodeAuthHeadersFor(node, true);
    const r = await httpRequestJson(
      `${baseUrl}/v1/servers/${encodeURIComponent(name)}/resources`,
      "GET",
      headers,
      null,
      10_000
    );

    if (r.status !== 200) {
      const msg = (r.json && r.json.error) ? r.json.error : `failed to get resources (${r.status})`;
      return res.status(502).json({ error: msg });
    }

    const result = r.json || { ok: true, resources: {} };

    if (!result.template && serverEntry.template) {
      result.template = serverEntry.template;
    }
    const resultTemplateId = serverEntry?.template || serverEntry?.type;
    if (typeof result.command === "string") {
      result.command = normalizeLegacyRuntimeProcessCommandInput(resultTemplateId, result.command);
    } else {
      result.command = normalizeLegacyRuntimeProcessCommandInput(resultTemplateId,
        serverEntry?.runtime?.command ||
        serverEntry?.docker?.command ||
        defaultRuntimeCommandForTemplate(serverEntry?.template, serverEntry?.docker, serverEntry?.start)
      );
    }
    if (!result.hostPort && serverEntry.port != null) {
      result.hostPort = Number(serverEntry.port);
    }
    return res.json(result);
  } catch (e) {
    console.error("[/api/settings/servers/:name/resources GET] failed:", e);
    return res.status(500).json({ error: "failed to get resources" });
  }
});

app.get("/api/settings/servers/:name/port-forwards", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  if (!(await isAdmin(req))) return res.status(403).json({ error: "admin access required" });

  let nameParam = req.params.name || "";
  try { nameParam = decodeURIComponent(nameParam); } catch { }
  const name = String(nameParam).trim();
  if (!name) return res.status(400).json({ error: "missing server name" });

  const serverEntry = await findServer(name);
  if (!serverEntry) return res.status(404).json({ error: "server not found" });

  if (!isRemoteEntry(serverEntry)) {
    return res.status(400).json({ error: "server is not on a remote node" });
  }

  const node = await findNodeByIdOrName(serverEntry.nodeId);
  if (!node) return res.status(400).json({ error: "node not found" });

  const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
  if (!baseUrl) return res.status(400).json({ error: "invalid node address" });

  try {
    const headers = nodeAuthHeadersFor(node, true);
    const r = await httpRequestJson(
      `${baseUrl}/v1/servers/${encodeURIComponent(name)}/port-forwards`,
      "GET", headers, null, 10_000
    );
    if (r.status !== 200) {
      const msg = (r.json && r.json.error) ? r.json.error : `failed to get port forwards (${r.status})`;
      return res.status(502).json({ error: msg });
    }
    const result = r.json || { ok: true, portForwards: [] };
    result.nodeAllocation = node.ports || null;
    result.mainPort = serverEntry.port ? Number(serverEntry.port) : null;
    if (!result.allocatedPorts) {
      result.allocatedPorts = [];
    }
    return res.json(result);
  } catch (e) {
    console.error("[/api/settings/servers/:name/port-forwards GET] failed:", e);
    return res.status(500).json({ error: "failed to get port forwards" });
  }
});

app.patch("/api/settings/servers/:name/port-forwards", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  if (!(await isAdmin(req))) return res.status(403).json({ error: "admin access required" });

  let nameParam = req.params.name || "";
  try { nameParam = decodeURIComponent(nameParam); } catch { }
  const name = String(nameParam).trim();
  if (!name) return res.status(400).json({ error: "missing server name" });

  const serverEntry = await findServer(name);
  if (!serverEntry) return res.status(404).json({ error: "server not found" });

  if (!isRemoteEntry(serverEntry)) {
    return res.status(400).json({ error: "server is not on a remote node" });
  }

  const node = await findNodeByIdOrName(serverEntry.nodeId);
  if (!node) return res.status(400).json({ error: "node not found" });

  const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
  if (!baseUrl) return res.status(400).json({ error: "invalid node address" });

  const { portForwards } = req.body || {};
  if (!Array.isArray(portForwards)) {
    return res.status(400).json({ error: "portForwards must be an array" });
  }

  const mainPort = serverEntry.port ? Number(serverEntry.port) : 0;
  const validated = [];
  for (const rule of portForwards) {
    const pub = validatePort(rule.publicPort);
    const internal = validatePort(rule.internalPort);
    if (!pub) {
      return res.status(400).json({ error: `Invalid public port: ${rule.publicPort}` });
    }
    if (!internal) {
      return res.status(400).json({ error: `Invalid internal port: ${rule.internalPort}` });
    }
    if (!isPortInNodeAllocation(node, pub)) {
      return res.status(400).json({ error: `Public port ${pub} is not in this node's allocated ports.` });
    }
    if (validated.some(v => v.publicPort === pub)) {
      return res.status(400).json({ error: `Duplicate public port: ${pub}` });
    }
    validated.push({ publicPort: pub, internalPort: internal });
  }

  try {
    const headers = nodeAuthHeadersFor(node, true);
    const r = await httpRequestJson(
      `${baseUrl}/v1/servers/${encodeURIComponent(name)}/port-forwards`,
      "PATCH", headers, { portForwards: validated }, 30_000
    );
    if (r.status !== 200 || !(r.json && r.json.ok)) {
      const msg = (r.json && r.json.error) ? r.json.error : `failed to update port forwards (${r.status})`;
      return res.status(502).json({ error: msg });
    }

    try {
      await httpRequestJson(`${baseUrl}/v1/servers/${encodeURIComponent(name)}/stop`, "POST", headers, null, 30_000).catch(() => { });
      await new Promise(resolve => setTimeout(resolve, 3000));
      const startPayload = mainPort > 0 ? { hostPort: mainPort } : {};
      const rr = await httpRequestJson(
        `${baseUrl}/v1/servers/${encodeURIComponent(name)}/start`,
        "POST", headers, startPayload, 180_000
      );
      if (rr.status !== 200 || !(rr.json && (rr.json.ok === true || rr.json.ok === undefined))) {
        const msg = (rr.json && (rr.json.error || rr.json.detail)) ? (rr.json.error || rr.json.detail) : `remote start failed (${rr.status})`;
        return res.status(502).json({ error: msg });
      }
    } catch (e) {
      return res.status(502).json({ error: "port forwards updated but restart failed", detail: e && e.message });
    }

    return res.json({ ok: true, message: "Port forwards updated" });
  } catch (e) {
    console.error("[/api/settings/servers/:name/port-forwards PATCH] failed:", e);
    return res.status(500).json({ error: "failed to update port forwards" });
  }
});

app.get("/api/settings/servers/:name/transfer", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });

  let nameParam = req.params.name || "";
  try { nameParam = decodeURIComponent(nameParam); } catch { }
  const name = String(nameParam).trim();
  if (!name) return res.status(400).json({ error: "missing server name" });

  const job = getTransferJob(name) || (await getTransferJobFromRedis(name));
  return res.json({ ok: true, job: job || null });
});

app.post("/api/settings/servers/:name/transfer", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });

  let nameParam = req.params.name || "";
  try { nameParam = decodeURIComponent(nameParam); } catch { }
  const name = String(nameParam).trim();
  if (!name) return res.status(400).json({ error: "missing server name" });

  const existingJob = getTransferJob(name) || (await getTransferJobFromRedis(name));
  if (existingJob && !existingJob.finishedAt) {
    return res.json({ ok: true, job: existingJob, alreadyRunning: true });
  }

  const serverEntry = await findServer(name);
  if (!serverEntry) return res.status(404).json({ error: "server not found" });
  if (!isRemoteEntry(serverEntry)) return res.status(400).json({ error: "server-not-on-node" });

  const sourceNode = await findNodeByIdOrName(serverEntry.nodeId);
  if (!sourceNode) return res.status(400).json({ error: "source node not found" });

  const targetNodeId = String(req.body?.targetNodeId || req.body?.nodeId || "").trim();
  if (!targetNodeId) return res.status(400).json({ error: "targetNodeId required" });

  const destNode = await findNodeByIdOrName(targetNodeId);
  if (!destNode) return res.status(400).json({ error: "destination node not found" });

  const srcId = String(sourceNode.uuid || sourceNode.id || sourceNode.name || "");
  const dstId = String(destNode.uuid || destNode.id || destNode.name || "");
  if (srcId && dstId && srcId === dstId) return res.status(400).json({ error: "cannot transfer to the same node" });

  const prevStatus = serverEntry.status ?? null;

  setTransferJob(name, {
    status: "preparing",
    percent: 0,
    message: "Preparing transfer",
    error: null,
    finishedAt: null,
    sourceNodeId: srcId,
    destNodeId: dstId,
  });

  try {
    await db.query("UPDATE servers SET status = ? WHERE name = ?", ["transferring", serverEntry.name]);
  } catch { }

  const job = getTransferJob(name);
  res.json({ ok: true, job });

  (async () => {
    let phase = "preparing";
    let dbCommittedToNewNode = false;
    try {
      phase = "stopping";
      setTransferJob(name, { status: "stopping", percent: 2, message: "Stopping server" });

      try {
        const baseUrl = buildNodeBaseUrl(sourceNode.address, sourceNode.api_port || 8080, sourceNode.ssl_enabled);
        if (baseUrl) {
          const headers = nodeAuthHeadersFor(sourceNode, true);
          const resources = (serverEntry.resources && typeof serverEntry.resources === "object") ? serverEntry.resources : null;
          if (resources && Object.keys(resources).length > 0) {
            await httpRequestJson(
              `${baseUrl}/v1/servers/${encodeURIComponent(serverEntry.name)}/resources`,
              "PATCH",
              headers,
              { resources },
              30_000
            ).catch(() => { });
          }
        }
      } catch { }

      {
        const srcBase = buildNodeBaseUrl(sourceNode.address, sourceNode.api_port || 8080, sourceNode.ssl_enabled);
        if (srcBase) {
          const headers = nodeAuthHeadersFor(sourceNode, true);
          try {
            const stopRes = await httpRequestJson(
              `${srcBase}/v1/servers/${encodeURIComponent(serverEntry.name)}/stop?wait=true`,
              "POST", headers, null, 60_000
            );
            if (stopRes.status === 200 && stopRes.json && stopRes.json.stopped) {
            } else {
              await stopServerOnNode(sourceNode, serverEntry.name);
            }
          } catch {
            await stopServerOnNode(sourceNode, serverEntry.name);
          }
        } else {
          await stopServerOnNode(sourceNode, serverEntry.name);
        }
      }

      setTransferJob(name, { status: "stopping", percent: 3, message: "Waiting for server to fully stop" });
      const stopped = await waitForServerStopped(sourceNode, serverEntry.name, { timeoutMs: 45_000, intervalMs: 2000 });
      if (!stopped) {
        console.warn("[transfer] Server did not confirm stopped within timeout, proceeding anyway", { server: name });
      }

      let sourceMeta = null;
      try {
        const srcBase = buildNodeBaseUrl(sourceNode.address, sourceNode.api_port || 8080, sourceNode.ssl_enabled);
        if (srcBase) {
          const srcHeaders = nodeAuthHeadersFor(sourceNode, true);
          const infoRes = await httpRequestJson(
            `${srcBase}/v1/servers/${encodeURIComponent(serverEntry.name)}`,
            "GET",
            srcHeaders,
            null,
            30_000
          );
          if (infoRes.status === 200 && infoRes.json && infoRes.json.meta) {
            sourceMeta = infoRes.json.meta;
          }
        }
      } catch { }

      phase = "transferring";
      setTransferJob(name, { status: "transferring", percent: 5, message: "Transferring files" });

      let extractTick = null;
      let uploadFinished = false;
      try {
        await streamExportToImport({
          sourceNode,
          destNode,
          serverName: serverEntry.name,
          onProgress: ({ percent }) => {
            if (percent == null) return;
            const p = Math.max(0, Math.min(100, percent));
            if (p >= 100) {
              setTransferJob(name, { status: "finalizing", percent: 91, message: "Finalizing transfer (extracting)" });
              if (!uploadFinished) {
                uploadFinished = true;
                let cur = 91;
                extractTick = setInterval(() => {
                  try {
                    const j = getTransferJob(name);
                    if (j && j.finishedAt) {
                      if (extractTick) {
                        clearInterval(extractTick);
                        extractTick = null;
                      }
                      return;
                    }
                    cur = Math.min(94, cur + 1);
                    setTransferJob(name, { status: "finalizing", percent: cur, message: "Finalizing transfer (extracting)" });
                    if (cur >= 94 && extractTick) {
                      clearInterval(extractTick);
                      extractTick = null;
                    }
                  } catch { }
                }, 5000);
              }
              return;
            }
            const mapped = 5 + Math.floor((p / 100) * 85);
            const pct = Math.max(5, Math.min(90, mapped));
            setTransferJob(name, { status: "transferring", percent: pct, message: "Transferring files" });
          }
        });
      } finally {
        if (extractTick) {
          try { clearInterval(extractTick); } catch { }
          extractTick = null;
        }
      }

      phase = "finalizing";
      setTransferJob(name, { status: "finalizing", percent: 93, message: "Finalizing transfer (extracting)" });

      phase = "resources";
      setTransferJob(name, { status: "finalizing", percent: 94, message: "Applying resources on destination node" });
      {
        const baseUrl = buildNodeBaseUrl(destNode.address, destNode.api_port || 8080, destNode.ssl_enabled);
        if (!baseUrl) throw new Error("invalid destination node address");
        const headers = nodeAuthHeadersFor(destNode, true);
        const eff = computeTransferEffectiveResources(serverEntry.resources, sourceNode, destNode);

        if (sourceMeta) {
          const srcMetaRes = (sourceMeta.resources && typeof sourceMeta.resources === "object") ? sourceMeta.resources : null;
          if (srcMetaRes && Array.isArray(srcMetaRes.ports) && srcMetaRes.ports.length > 0 && !Array.isArray(eff.ports)) {
            eff.ports = srcMetaRes.ports.map(p => Number(p)).filter(p => p > 0);
          }
        }

        const patchPayload = { resources: eff };

        const r = await httpRequestJson(
          `${baseUrl}/v1/servers/${encodeURIComponent(serverEntry.name)}/resources`,
          "PATCH",
          headers,
          patchPayload,
          30_000
        );
        if (r.status !== 200 || !(r.json && r.json.ok)) {
          const msg = (r.json && (r.json.error || r.json.detail)) ? (r.json.error || r.json.detail) : `failed to apply resources (${r.status})`;
          throw new Error(msg);
        }
      }

      phase = "starting";
      setTransferJob(name, { status: "starting", percent: 95, message: "Starting on new node" });
      const transferHostPort = serverEntry.port != null ? serverEntry.port
        : (sourceMeta && sourceMeta.hostPort != null) ? Number(sourceMeta.hostPort)
          : null;
      await startServerOnNode(destNode, serverEntry.name, transferHostPort);

      setTransferJob(name, { status: "starting", percent: 95, message: "Waiting for server to be ready on new node" });
      const ready = await waitForServerReady(destNode, serverEntry.name, { timeoutMs: 30_000, intervalMs: 2000 });
      if (!ready) {
        console.warn("[transfer] Server not confirmed ready on destination within timeout, proceeding with DB update", { server: name });
      }

      phase = "db-update";
      setTransferJob(name, { status: "finalizing", percent: 96, message: "Updating database" });
      const newNodeId = destNode.uuid || destNode.id || destNode.name;
      await db.query(
        "UPDATE servers SET node_id = ?, ip = ?, status = ? WHERE name = ?",
        [newNodeId, destNode.address || null, prevStatus || "stopped", serverEntry.name]
      );
      dbCommittedToNewNode = true;

      phase = "cleanup";
      setTransferJob(name, { status: "cleanup", percent: 97, message: "Cleaning up old node" });
      try {
        await deleteServerOnNode(sourceNode, serverEntry.name, { filesOnly: true });
      } catch (cleanupErr) {
        console.warn("[transfer] Source cleanup failed (non-fatal):", cleanupErr?.message || cleanupErr);
      }

      setTransferJob(name, { status: "done", percent: 100, message: "Transfer complete", finishedAt: Date.now(), error: null });
      cleanupTransferJobLater(name);
    } catch (err) {
      const msg = err?.message || String(err);
      const full = `phase=${phase}: ${msg}`;
      console.error("[transfer] failed", { server: name, phase, error: msg });
      const prev = getTransferJob(name);
      const pct = prev && typeof prev.percent === "number" ? prev.percent : 0;
      setTransferJob(name, { status: "error", percent: pct, message: full, error: full, finishedAt: Date.now() });
      cleanupTransferJobLater(name);

      if (dbCommittedToNewNode) {
        console.warn("[transfer] Error occurred after DB commit to destination. Server remains on new node.", { server: name, phase });
      } else {
        try {
          await db.query("UPDATE servers SET status = ? WHERE name = ?", [prevStatus || "stopped", serverEntry.name]);
        } catch { }
        try {
          await startServerOnNode(sourceNode, serverEntry.name, serverEntry.port);
        } catch { }
        try {
          await deleteServerOnNode(destNode, serverEntry.name);
        } catch { }
      }
    }
  })();
});

function getSshTerminalWsTarget(req) {
  const origin = getSshTerminalOrigin({
    ...req,
    secure: isRequestSecure(req),
    headers: req.headers,
    get: req.get ? req.get.bind(req) : (name) => req.headers?.[String(name || "").toLowerCase()],
  });
  const incoming = new URL(req.url || "", "http://adpanel.local");
  const target = new URL("/ws", origin);
  target.search = incoming.search || "";
  return target;
}

function proxySshTerminalUpgrade(req, socket, head) {
  let target;
  try {
    target = getSshTerminalWsTarget(req);
  } catch {
    try {
      socket.write("HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\n");
    } catch { }
    socket.destroy();
    return;
  }

  const isTls = target.protocol === "https:";
  const requester = isTls ? https : httpMod;
  const headers = { ...req.headers };
  headers.host = target.host;
  // Upstream websocket server validates Origin against Host. Preserve browser origin
  // at the panel edge, but forward an upstream-matching Origin for the proxy hop.
  headers.origin = `${target.protocol}//${target.host}`;

  const proxyReq = requester.request({
    protocol: target.protocol,
    hostname: target.hostname,
    port: target.port || (isTls ? 443 : 80),
    method: "GET",
    path: `${target.pathname}${target.search}`,
    headers,
  });

  proxyReq.on("upgrade", (proxyRes, proxySocket, proxyHead) => {
    try {
      const lines = [`HTTP/1.1 ${proxyRes.statusCode || 101} ${proxyRes.statusMessage || "Switching Protocols"}`];
      for (const [k, v] of Object.entries(proxyRes.headers || {})) {
        if (Array.isArray(v)) {
          for (const one of v) lines.push(`${k}: ${one}`);
        } else if (v !== undefined) {
          lines.push(`${k}: ${v}`);
        }
      }
      socket.write(`${lines.join("\r\n")}\r\n\r\n`);
      if (proxyHead && proxyHead.length) socket.write(proxyHead);
      if (head && head.length) proxySocket.write(head);
      socket.pipe(proxySocket).pipe(socket);
    } catch {
      try { socket.destroy(); } catch { }
      try { proxySocket.destroy(); } catch { }
    }
  });

  proxyReq.on("response", (proxyRes) => {
    try {
      socket.write(`HTTP/1.1 ${proxyRes.statusCode || 502} ${proxyRes.statusMessage || "Bad Gateway"}\r\nConnection: close\r\n\r\n`);
    } catch { }
    socket.destroy();
  });

  proxyReq.on("error", () => {
    try {
      socket.write("HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\n");
    } catch { }
    socket.destroy();
  });

  proxyReq.end();
}

function attachSshTerminalUpgradeProxy(server) {
  if (!server || typeof server.on !== "function") return;
  server.on("upgrade", (req, socket, head) => {
    try {
      const parsed = new URL(req.url || "", "http://adpanel.local");
      if (parsed.pathname !== "/ssh-terminal/ws") return;
      proxySshTerminalUpgrade(req, socket, head);
    } catch {
      try { socket.destroy(); } catch { }
    }
  });
}

const httpServer = httpMod.createServer(app);
httpServer.requestTimeout = 300000;
httpServer.headersTimeout = 60000;
httpServer.keepAliveTimeout = 5000;
attachSshTerminalUpgradeProxy(httpServer);
const SOCKET_HSTS_HEADER = "max-age=31536000; includeSubDomains";
function applySocketSecurityHeaders(headers, req) {
  if (!headers || typeof headers !== "object") return;
  if (isRequestSecure(req)) {
    headers["Strict-Transport-Security"] = SOCKET_HSTS_HEADER;
  }
}
const io = new SocketIOServer(httpServer, {
  maxHttpBufferSize: 1e6,
  pingTimeout: 20000,
  pingInterval: 25000,
  allowEIO3: false,
  cors: {
    origin: process.env.BASE_URL || false,
    methods: ["GET", "POST"],
    credentials: true,
  },
});
io.engine.on("initial_headers", applySocketSecurityHeaders);
io.engine.on("headers", applySocketSecurityHeaders);


broadcastDashboardUpdate = function (serverName, status) {
  io.to('dashboard').emit('server:status', {
    name: serverName,
    status: status.status,
    cpu: status.cpu,
    memory: status.memory,
    disk: status.disk,
    nodeOnline: status.nodeOnline,
    updatedAt: status.updatedAt
  });
};

broadcastNodeUpdate = function (nodeId, status) {
  io.to('dashboard').emit('node:status', {
    id: nodeId,
    online: status.online,
    latency: status.latency,
    serverCount: status.serverCount,
    updatedAt: status.updatedAt
  });
};

const LOG_CHUNK_MAX_CHARS = parseInt(process.env.LOG_CHUNK_MAX_CHARS || "", 10) || 10000;

function stripAnsiSafe(input) {
  const str = String(input || "");
  if (!str) return "";
  const out = [];
  let i = 0;
  while (i < str.length) {
    const code = str.charCodeAt(i);
    if (code === 0x1b || code === 0x9b) {
      if (code === 0x9b) {
        i = skipAnsiCsi(str, i + 1, out, i);
        continue;
      }
      const next = str.charCodeAt(i + 1);
      if (next === 0x5b) {
        i = skipAnsiCsi(str, i + 2, out, i);
        continue;
      }
      if (next === 0x5d) {
        i = skipAnsiOsc(str, i + 2);
        continue;
      }
      if (next === 0x50 || next === 0x58 || next === 0x5e || next === 0x5f) {
        i = skipAnsiEscTerminated(str, i + 2);
        continue;
      }
      if (next >= 0x40 && next <= 0x5f) {
        i += 2;
        continue;
      }
      i += 1;
      continue;
    }
    if ((code < 0x20 || code === 0x7f) && code !== 0x0a && code !== 0x0d && code !== 0x09) {
      i += 1;
      continue;
    }
    out.push(str[i]);
    i += 1;
  }
  return out.join("");
}

function skipAnsiCsi(str, idx, out = null, start = 0) {
  let i = idx;
  while (i < str.length) {
    const c = str.charCodeAt(i);
    if (c >= 0x40 && c <= 0x7e) {
      if (out && str[i] === "m") out.push(str.slice(start, i + 1));
      return i + 1;
    }
    i += 1;
  }
  return str.length;
}

function skipAnsiOsc(str, idx) {
  let i = idx;
  while (i < str.length) {
    const c = str.charCodeAt(i);
    if (c === 0x07) return i + 1;
    if (c === 0x1b && str.charCodeAt(i + 1) === 0x5c) return i + 2;
    i += 1;
  }
  return i;
}

function skipAnsiEscTerminated(str, idx) {
  let i = idx;
  while (i < str.length) {
    const c = str.charCodeAt(i);
    if (c === 0x1b && str.charCodeAt(i + 1) === 0x5c) return i + 2;
    i += 1;
  }
  return i;
}

function cleanLog(name, chunk) {
  if (!chunk) return "";
  let s = chunk.toString();
  if (LOG_CHUNK_MAX_CHARS > 0 && s.length > LOG_CHUNK_MAX_CHARS) {
    s = s.slice(0, LOG_CHUNK_MAX_CHARS);
  }
  s = stripAnsiSafe(s);
  s = s.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  s = s.replace(/^(?:stdout|stderr):\s?/gm, "");
  return s.trimEnd();
}

function getConsoleRoom(room) {
  return `console:${String(room || "").trim()}`;
}

function _emitLine(room, text) {
  const cleaned = cleanLog(room, text);
  if (!cleaned) return;
  rememberConsoleLine(room, cleaned, "output");
  io.to(getConsoleRoom(room)).emit("output", escapeHtml(cleaned));
}

function emitOutput(socket, room, text) {
  const cleaned = cleanLog(room, text);
  if (!cleaned) return;
  rememberConsoleLine(room, cleaned, "output");
  const safe = escapeHtml(cleaned);
  try { io.to(getConsoleRoom(room)).emit("output", safe); } catch { }
}

function panel(socket, room, msg) {
  emitOutput(socket, room, `[ADPanel] ${msg}`);
}

function emitPanel(room, message) {
  _emitLine(room, `[ADPanel] ${message}`);
}

const remoteLogClients = Object.create(null);
function tailLogsRemote(name, baseUrl, headers, options = {}) {
  if (remoteLogClients[name]) return;

  const requestedTail = Number(options && options.tail);
  const safeTail = Number.isFinite(requestedTail) && requestedTail >= 0
    ? Math.max(0, Math.min(10000, Math.trunc(requestedTail)))
    : 0;
  const query = `?tail=${encodeURIComponent(String(safeTail))}`;
  const url = `${baseUrl}/v1/servers/${encodeURIComponent(name)}/logs${query}`;
  const lib = url.startsWith("https:") ? https : httpMod;

  const hdrs = { ...(headers || {}) };
  delete hdrs["Content-Type"];

  hdrs.Accept = "text/event-stream";
  hdrs["Cache-Control"] = "no-cache";
  hdrs.Connection = "keep-alive";

  const req = lib.request(url, { method: "GET", headers: hdrs });

  req.on("socket", (s) => {
    try { s.setNoDelay(true); } catch { }
  });

  req.on("response", (res) => {
    res.setEncoding("utf8");

    const MAX_BUF = 256 * 1024;
    let buf = "";
    let dataLines = [];

    function flushEvent() {
      if (!dataLines.length) return;
      const payload = dataLines.join("\n");
      dataLines = [];

      try {
        const obj = JSON.parse(payload);
        if (obj && obj.line) _emitLine(name, obj.line);
        else if (typeof obj === "string") _emitLine(name, obj);
      } catch {
        _emitLine(name, payload);
      }
    }

    res.on("data", (chunk) => {
      buf += chunk;
      if (buf.length > MAX_BUF) buf = buf.slice(-MAX_BUF);

      while (true) {
        const nl = buf.indexOf("\n");
        if (nl === -1) break;

        let line = buf.slice(0, nl);
        buf = buf.slice(nl + 1);

        if (line.endsWith("\r")) line = line.slice(0, -1);

        if (line === "") {
          flushEvent();
          continue;
        }

        if (line.startsWith(":")) continue;

        if (line.startsWith("data:")) {
          dataLines.push(line.slice(5).trimStart());
        }
      }
    });

    res.on("end", () => {
      try { flushEvent(); } catch { }
      delete remoteLogClients[name];
    });

    res.on("aborted", () => {
      delete remoteLogClients[name];
    });
  });

  req.on("error", () => {
    delete remoteLogClients[name];
  });

  req.end();
  remoteLogClients[name] = req;
}

function stopTailLogsRemote(name) {
  const req = remoteLogClients[name];
  if (!req) return;
  try { req.destroy(); } catch (_) { }
  delete remoteLogClients[name];
}

const ROOM_STATUS_INTERVAL_MS = 4000;
const roomStatusWatchers = new Map();

function getRoomClientCount(room) {
  const set = io.sockets.adapter.rooms.get(room);
  return set ? set.size : 0;
}

function getConsoleRoomClientCount(room) {
  const set = io.sockets.adapter.rooms.get(getConsoleRoom(room));
  return set ? set.size : 0;
}

function stopRoomStatusWatcher(room) {
  const rec = roomStatusWatchers.get(room);
  if (rec?.timer) clearInterval(rec.timer);
  roomStatusWatchers.delete(room);
  stopTailLogsRemote(room);
}

function startRoomStatusWatcher(room, baseUrl, headers) {
  const existing = roomStatusWatchers.get(room);
  if (existing) {
    const nextUrl = String(baseUrl || "");
    if (nextUrl && existing.baseUrl !== nextUrl) {
      existing.baseUrl = nextUrl;
      existing.headers = headers;
      existing.failures = 0;
      if (!existing.nodeOnline) {
        existing.nodeOnline = true;
        io.to(room).emit("nodeStatus", { nodeOnline: true });
      }
    }
    return;
  }

  const rec = { last: null, failures: 0, timer: null, nodeOnline: true, baseUrl, headers };

  async function tick() {
    if (getRoomClientCount(room) === 0) return stopRoomStatusWatcher(room);
    if (getConsoleRoomClientCount(room) === 0) stopTailLogsRemote(room);

    try {
      const { status, json } = await httpRequestJson(
        `${rec.baseUrl}/v1/servers/${encodeURIComponent(room)}`,
        "GET",
        rec.headers,
        null,
        3000
      );

      if (status === 0) {
        throw new Error("connection-failed");
      }

      if (status !== 200 || !json) {
        if (!rec.nodeOnline) {
          rec.nodeOnline = true;
          io.to(room).emit("nodeStatus", { nodeOnline: true });
        }
        throw new Error("bad-status");
      }

      const rawStatus =
        (typeof json.status === "string" && json.status) ||
        (typeof json.state === "string" && json.state) ||
        "";

      const label = normalizeStatusLabel(rawStatus) || "unknown";

      rec.failures = 0;

      if (!rec.nodeOnline) {
        rec.nodeOnline = true;
        io.to(room).emit("nodeStatus", { nodeOnline: true });
        emitPanel(room, "Node connection restored");
      }

      if (rec.last && rec.last !== label) {
        if (label === "online") emitPanel(room, "Server started");
        else if (label === "stopped") emitPanel(room, "Server stopped");
        else emitPanel(room, `Server status: ${label}`);

        try {
          const entry = await findServer(room);
          if (entry) await upsertServerIndexEntry({ ...entry, status: label });
        } catch { }
      }

      rec.last = label;
    } catch {
      rec.failures++;

      if (rec.failures >= 2 && rec.nodeOnline) {
        rec.nodeOnline = false;
        io.to(room).emit("nodeStatus", { nodeOnline: false });
        emitPanel(room, "Node connection lost");
      }

      if (rec.failures === 3) {
        rec.last = "stopped";

        try {
          const entry = await findServer(room);
          if (entry) await upsertServerIndexEntry({ ...entry, status: "stopped" });
        } catch { }
      }
    }
  }

  rec.timer = setInterval(() => tick().catch(() => { }), ROOM_STATUS_INTERVAL_MS);
  roomStatusWatchers.set(room, rec);

  tick().then(() => {
    io.to(room).emit("nodeStatus", { nodeOnline: rec.nodeOnline });
  }).catch(() => {
    io.to(room).emit("nodeStatus", { nodeOnline: rec.nodeOnline });
  });
}

const LIVE_STATUS_TTL_MS = 13_000;
const liveStatusCache = new Map();

const DASHBOARD_POLL_INTERVAL_MS = 5000;

setInterval(() => {
  try {
    const now = Date.now();
    for (const [key, val] of liveStatusCache.entries()) {
      if (now - val.ts > LIVE_STATUS_TTL_MS * 2) {
        liveStatusCache.delete(key);
      }
    }
  } catch (err) { console.debug("[liveStatusCache] sweep error:", err.message); }
}, 60_000).unref();

const dashboardWatchers = new Map();
let dashboardSharedTimer = null;

async function emitDashboardStatuses(socket, bots) {
  if (!socket || !socket.connected) return;
  const list = Array.isArray(bots) ? bots : [];

  try {
    const results = await Promise.all(list.map((b) => queryLiveStatus(b)));
    socket.emit("dashboard:statuses", results);
  } catch {
  }
}

async function queryLiveStatus(botName) {
  const name = String(botName || "").trim();
  if (!name) return { name: botName, status: "unknown" };

  const cached = await getCachedServerStatusLenient(name);
  if (cached) {
    const result = {
      name,
      status: cached.status || "unknown"
    };
    if (typeof cached.nodeOnline === "boolean") {
      result.nodeOnline = cached.nodeOnline;
    }
    return result;
  }

  return { name, status: "unknown" };
}

function ensureDashboardTimer() {
  if (dashboardSharedTimer) return;
  dashboardSharedTimer = setInterval(() => {
    if (dashboardWatchers.size === 0) {
      clearInterval(dashboardSharedTimer);
      dashboardSharedTimer = null;
      return;
    }
    for (const [id, rec] of dashboardWatchers.entries()) {
      if (!rec.socket || !rec.socket.connected) {
        dashboardWatchers.delete(id);
        continue;
      }
      emitDashboardStatuses(rec.socket, rec.bots).catch(() => { });
    }
  }, DASHBOARD_POLL_INTERVAL_MS);
}

function stopDashboardWatcher(socket) {
  if (!socket) return;
  dashboardWatchers.delete(socket.id);
  if (dashboardWatchers.size === 0 && dashboardSharedTimer) {
    clearInterval(dashboardSharedTimer);
    dashboardSharedTimer = null;
  }
}

function startDashboardWatcher(socket, bots) {
  if (!socket || !socket.id) return;
  stopDashboardWatcher(socket);

  const list = [...new Set((bots || []).filter(Boolean))];
  if (!list.length) return;

  emitDashboardStatuses(socket, list).catch(() => { });
  dashboardWatchers.set(socket.id, { socket, bots: list });
  ensureDashboardTimer();
}

const RESOURCE_STATS_POLL_MS = 3000;
const resourceStatsWatchers = new Map();

function getResourceRoom(serverName) {
  return `res:${serverName}`;
}

function getResourceRoomClientCount(serverName) {
  const room = getResourceRoom(serverName);
  const set = io.sockets.adapter.rooms.get(room);
  return set ? set.size : 0;
}

function stopResourceStatsWatcher(serverName) {
  const rec = resourceStatsWatchers.get(serverName);
  if (!rec) return;
  if (rec.timer) clearInterval(rec.timer);
  resourceStatsWatchers.delete(serverName);
}

async function fetchResourceStatsFromNode(serverName, baseUrl, headers) {
  try {
    const { status, json } = await httpRequestJson(
      `${baseUrl}/v1/servers/${encodeURIComponent(serverName)}`,
      "GET",
      headers,
      null,
      4000
    );
    if (status !== 200 || !json || !json.ok) return null;
    return json;
  } catch {
    return null;
  }
}

function buildResourcePayload(serverName, nodeStats, cachedStatus) {
  if (nodeStats && nodeStats.stats) {
    const stats = nodeStats.stats;
    const statusLabel = normalizeStatusLabel(nodeStats.status || nodeStats.state) || 'unknown';
    return normalizeServerStatusRecord({
      server: serverName,
      ok: true,
      status: statusLabel,
      nodeOnline: true,
      cpu: stats.cpu?.percent ?? null,
      cpuLimit: stats.cpu?.limit ?? null,
      memory: {
        used: stats.memory?.usedMb ?? 0,
        total: stats.memory?.limitMb ?? 0,
        percent: stats.memory?.percent ?? 0
      },
      disk: {
        used: (stats.disk?.usedMb ?? 0) / 1024,
        total: (stats.disk?.limitMb ?? 0) / 1024,
        percent: stats.disk?.percent ?? 0
      },
      uptime: nodeStats.uptime ?? null,
      ts: Date.now()
    });
  }
  if (nodeStats) {
    const statusLabel = normalizeStatusLabel(nodeStats.status || nodeStats.state) || 'unknown';
    return normalizeServerStatusRecord({
      server: serverName,
      ok: true,
      status: statusLabel,
      nodeOnline: true,
      cpu: cachedStatus?.cpu ?? null,
      cpuLimit: cachedStatus?.cpuLimit ?? null,
      memory: cachedStatus?.memory ?? null,
      disk: cachedStatus?.disk ?? null,
      uptime: nodeStats.uptime ?? cachedStatus?.uptime ?? null,
      ts: Date.now()
    });
  }
  if (cachedStatus) {
    const payload = normalizeServerStatusRecord({
      server: serverName,
      ok: true,
      status: normalizeStatusLabel(cachedStatus.status) || 'unknown',
      cpu: cachedStatus.cpu ?? null,
      cpuLimit: cachedStatus.cpuLimit ?? null,
      memory: cachedStatus.memory ?? null,
      disk: cachedStatus.disk ?? null,
      uptime: cachedStatus.uptime ?? null,
      ts: Date.now()
    });
    if (cachedStatus.nodeOnline === true) payload.nodeOnline = true;
    return payload;
  }
  return { server: serverName, ok: false, status: 'unknown', ts: Date.now() };
}

const MAX_RESOURCE_WATCHERS = 500;
async function startResourceStatsWatcher(serverName) {
  if (resourceStatsWatchers.has(serverName)) return;
  if (resourceStatsWatchers.size >= MAX_RESOURCE_WATCHERS) return;

  const entry = await findServer(serverName);
  if (!entry || !isRemoteEntry(entry)) return;

  const node = await findNodeByIdOrName(entry.nodeId);
  if (!node) return;

  const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
  if (!baseUrl) return;

  const headers = nodeAuthHeadersFor(node, true);
  const rec = { timer: null, last: null, baseUrl, headers, inFlight: false };

  async function tick() {
    if (getResourceRoomClientCount(serverName) === 0) {
      stopResourceStatsWatcher(serverName);
      return;
    }
    if (rec.inFlight) return;
    rec.inFlight = true;
    try {
      const nodeStats = await fetchResourceStatsFromNode(serverName, rec.baseUrl, rec.headers);
      const cachedStatus = await getCachedServerStatusLenient(serverName);
      const payload = buildResourcePayload(serverName, nodeStats, cachedStatus);
      rec.last = payload;
      io.to(getResourceRoom(serverName)).emit('resources:data', payload);
    } catch {
      if (rec.last) {
        io.to(getResourceRoom(serverName)).emit('resources:data', rec.last);
      }
    } finally {
      rec.inFlight = false;
    }
  }

  rec.timer = setInterval(() => tick().catch(() => { }), RESOURCE_STATS_POLL_MS);
  resourceStatsWatchers.set(serverName, rec);

  tick().catch(() => { });
}

setInterval(() => {
  try {
    for (const [name] of resourceStatsWatchers) {
      if (getResourceRoomClientCount(name) === 0) {
        stopResourceStatsWatcher(name);
      }
    }
  } catch (err) { console.debug("[resourceStats] cleanup error:", err.message); }
}, 30_000).unref();

const SOCKET_ALLOWED_ORIGINS = (process.env.SOCKET_ALLOW_ORIGINS || "")
  .split(",")
  .map(s => s.trim())
  .filter(Boolean);

function normalizeHostForSocket(value) {
  const extracted = extractHostnameFromHeader(value);
  return extracted ? extracted.toLowerCase() : "";
}

function isSocketOriginAllowed(headers) {
  const originHeader = headers?.origin || headers?.Origin || "";
  const requestHost = normalizeHostForSocket(headers?.host || "");
  const allowlist = SOCKET_ALLOWED_ORIGINS.map(normalizeHostForSocket).filter(Boolean);

  if (!originHeader) return true;

  let originHost = "";
  try {
    originHost = normalizeHostForSocket(new URL(originHeader).host);
  } catch {
    originHost = normalizeHostForSocket(originHeader);
  }

  if (!originHost) return false;

  if (allowlist.length) return allowlist.includes(originHost);
  return originHost === requestHost;
}

function getSocketIp(socket) {
  const ip = getRequestIp(socket.request);
  if (ip && ip !== "unknown") return ip;
  return socket.handshake?.address || "unknown";
}

const socketConnectionsPerIp = new Map();

io.use((socket, next) => {
  const ip = getSocketIp(socket);
  const cookieHeader = socket.request?.headers?.cookie;
  if (!cookieHeader) {
    console.warn(`[SECURITY] Socket connection rejected (No cookies): ${ip}`);
    return next(new Error("Authentication required"));
  }
  if (!isSocketOriginAllowed(socket.handshake?.headers || {})) {
    return next(new Error("origin-not-allowed"));
  }

  sessionMiddleware(socket.request, socket.request.res || {}, (err) => {
    if (err) return next(err);
    const email = socket.request.session?.user;
    if (!email) return next(new Error("unauthorized"));

    const currentConnections = socketConnectionsPerIp.get(ip) || 0;
    if (currentConnections >= 10) {
      console.warn(`[SECURITY] Max socket connections exceeded: ${ip}`);
      return next(new Error("Too many connections from this IP"));
    }

    socketConnectionsPerIp.set(ip, currentConnections + 1);
    socket.on("disconnect", () => {
      const count = socketConnectionsPerIp.get(ip) || 1;
      if (count <= 1) socketConnectionsPerIp.delete(ip);
      else socketConnectionsPerIp.set(ip, count - 1);
    });

    return next();
  });
});

const consoleHistory = new Map();
const CONSOLE_HISTORY_MAX_SERVERS = Math.max(100, parseInt(process.env.CONSOLE_HISTORY_MAX_SERVERS || "", 10) || 2000);
const CONSOLE_HISTORY_MAX_LINES = Math.min(Math.max(parseInt(process.env.CONSOLE_HISTORY_MAX_LINES || "", 10) || 500, 100), 2000);
const CONSOLE_HISTORY_MAX_LINE_CHARS = Math.min(Math.max(parseInt(process.env.CONSOLE_HISTORY_MAX_LINE_CHARS || "", 10) || 12000, 1000), 50000);
const CONSOLE_HISTORY_NODE_CACHE_TTL_MS = 1200;
const consoleHistoryNodeFetchCache = new Map();
const consoleHistoryNodeFetchInflight = new Map();

function normalizeConsoleHistoryEntries(entries) {
  return (Array.isArray(entries) ? entries : [])
    .map((item) => ({
      line: String(item?.line || "").slice(0, CONSOLE_HISTORY_MAX_LINE_CHARS),
      ts: Number(item?.ts || 0) || Date.now(),
      source: String(item?.source || "output"),
    }))
    .filter((item) => item.line)
    .slice(-CONSOLE_HISTORY_MAX_LINES);
}

function cloneConsoleHistoryEntries(entries) {
  return normalizeConsoleHistoryEntries(entries).map((item) => ({
    line: item.line,
    ts: item.ts,
    source: item.source,
  }));
}

function consoleHistoryEntryKey(item) {
  return `${String(item?.source || "output")}\u0000${String(item?.line || "")}`;
}

function findConsoleHistoryOverlap(baseEntries, incomingEntries) {
  const base = normalizeConsoleHistoryEntries(baseEntries);
  const incoming = normalizeConsoleHistoryEntries(incomingEntries);
  if (!base.length || !incoming.length) return { start: -1, size: 0 };

  const maxOverlap = Math.min(base.length, incoming.length, 200);
  for (let size = maxOverlap; size > 0; size--) {
    const baseStart = base.length - size;
    for (let incomingStart = 0; incomingStart <= incoming.length - size; incomingStart++) {
      let matched = true;
      for (let i = 0; i < size; i++) {
        if (consoleHistoryEntryKey(base[baseStart + i]) !== consoleHistoryEntryKey(incoming[incomingStart + i])) {
          matched = false;
          break;
        }
      }
      if (matched) return { start: incomingStart, size };
    }
  }
  return { start: -1, size: 0 };
}

function mergeConsoleHistoryEntries(...groups) {
  const merged = [];
  for (const group of groups) {
    const incoming = normalizeConsoleHistoryEntries(group);
    if (!incoming.length) continue;
    const overlap = findConsoleHistoryOverlap(merged, incoming);
    const toAppend = overlap.size > 0
      ? incoming.slice(overlap.start + overlap.size)
      : incoming;
    for (const item of toAppend) {
      merged.push(item);
      if (merged.length > CONSOLE_HISTORY_MAX_LINES) merged.shift();
    }
  }
  return merged.slice(-CONSOLE_HISTORY_MAX_LINES);
}

async function fetchRecentConsoleHistoryFromNode(serverName, limit = 100) {
  const requestedLimit = Math.min(Math.max(parseInt(limit, 10) || 100, 1), CONSOLE_HISTORY_MAX_LINES);
  const requestedName = String(serverName || "").trim();
  if (!requestedName) return [];

  const cacheKey = `${requestedName}:${requestedLimit}`;
  const now = Date.now();
  const cached = consoleHistoryNodeFetchCache.get(cacheKey);
  if (cached && (now - cached.ts) <= CONSOLE_HISTORY_NODE_CACHE_TTL_MS) {
    return cloneConsoleHistoryEntries(cached.lines);
  }

  const inFlight = consoleHistoryNodeFetchInflight.get(cacheKey);
  if (inFlight) {
    const shared = await inFlight;
    return cloneConsoleHistoryEntries(shared);
  }

  const fetchPromise = (async () => {
    const entry = await findServer(requestedName);
    if (!entry || !isRemoteEntry(entry)) return [];

    const node = await findNodeByIdOrName(entry.nodeId);
    if (!node) return [];

    const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
    if (!baseUrl) return [];

    const fullUrl = `${baseUrl}/v1/servers/${encodeURIComponent(entry.name || requestedName)}/logs?tail=${encodeURIComponent(String(requestedLimit))}`;
    const headers = {
      ...nodeAuthHeadersFor(node, true),
      Accept: "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    };

    const fetched = await new Promise((resolve) => {
      try {
        const lib = fullUrl.startsWith("https:") ? https : httpMod;
        const req = lib.request(fullUrl, { method: "GET", headers }, (nres) => {
          if (nres.statusCode !== 200) {
            try { req.destroy(); } catch { }
            resolve([]);
            return;
          }

          const collected = [];
          let buf = "";
          let dataLines = [];
          let settled = false;

          const finish = () => {
            if (settled) return;
            settled = true;
            try { req.destroy(); } catch { }
            resolve(collected.slice(-requestedLimit));
          };

          const idleTimer = setTimeout(finish, 350);
          idleTimer.unref?.();
          const maxTimer = setTimeout(finish, 1500);
          maxTimer.unref?.();

          const flushEvent = () => {
            if (!dataLines.length) return;
            const payload = dataLines.join("\n");
            dataLines = [];
            try {
              const parsed = JSON.parse(payload);
              const line = typeof parsed?.line === "string"
                ? parsed.line
                : typeof parsed === "string"
                  ? parsed
                  : "";
              if (!line) return;
              const item = { line: String(line), ts: Date.now(), source: "output" };
              collected.push(item);
            } catch { }
          };

          nres.on("data", (chunk) => {
            clearTimeout(idleTimer);
            buf += Buffer.isBuffer(chunk) ? chunk.toString("utf8") : String(chunk || "");

            while (true) {
              const nl = buf.indexOf("\n");
              if (nl === -1) break;

              let line = buf.slice(0, nl);
              buf = buf.slice(nl + 1);
              if (line.endsWith("\r")) line = line.slice(0, -1);

              if (line === "") {
                flushEvent();
                continue;
              }

              if (line.startsWith(":") || line.startsWith("event:")) continue;
              if (line.startsWith("data:")) {
                dataLines.push(line.slice(5).trimStart());
              }
            }
          });
          nres.on("end", () => {
            clearTimeout(idleTimer);
            clearTimeout(maxTimer);
            flushEvent();
            finish();
          });
          nres.on("aborted", () => {
            clearTimeout(idleTimer);
            clearTimeout(maxTimer);
            flushEvent();
            finish();
          });
        });

        req.on("error", () => resolve([]));
        req.setTimeout(2000, () => {
          try { req.destroy(); } catch { }
          resolve([]);
        });
        req.end();
      } catch {
        resolve([]);
      }
    });

    const normalized = normalizeConsoleHistoryEntries(fetched);
    consoleHistoryNodeFetchCache.set(cacheKey, { ts: Date.now(), lines: normalized });
    return normalized;
  })().catch(() => {
    const empty = [];
    consoleHistoryNodeFetchCache.set(cacheKey, { ts: Date.now(), lines: empty });
    return empty;
  }).finally(() => {
    consoleHistoryNodeFetchInflight.delete(cacheKey);
  });

  consoleHistoryNodeFetchInflight.set(cacheKey, fetchPromise);
  const fetched = await fetchPromise;
  return cloneConsoleHistoryEntries(fetched);
}

async function getConsoleHistorySnapshot(serverName, limit = 200, options = {}) {
  const name = String(serverName || "").trim();
  if (!name) return { total: 0, lines: [] };

  const safeLimit = Math.min(Math.max(parseInt(limit, 10) || 200, 1), CONSOLE_HISTORY_MAX_LINES);
  const includeCommands = options && options.includeCommands === true;
  let history = normalizeConsoleHistoryEntries(consoleHistory.get(name));
  const latestOutputEntry = [...history].reverse().find((item) => String(item?.source || "output") !== "command");
  const lastOutputTs = latestOutputEntry
    ? Number(latestOutputEntry.ts || 0)
    : 0;
  const newestEntryIsCommand = history.length > 0 && String(history[history.length - 1]?.source || "output") === "command";
  const historyIsStale = !lastOutputTs || (Date.now() - lastOutputTs > 5000);
  const nodeWarmThreshold = Math.min(safeLimit, 40);

  if (history.length < nodeWarmThreshold || newestEntryIsCommand || historyIsStale) {
    const nodeHistory = await fetchRecentConsoleHistoryFromNode(name, safeLimit);
    if (nodeHistory.length > 0) {
      history = mergeConsoleHistoryEntries(history, nodeHistory);
      consoleHistory.set(name, history);
    }
  }

  const responseHistory = includeCommands
    ? history
    : history.filter((item) => String(item?.source || "output") !== "command");

  return {
    total: responseHistory.length,
    lines: responseHistory.slice(-safeLimit).map((item) => ({
      line: String(item?.line || ""),
      ts: Number(item?.ts || 0) || Date.now(),
      source: String(item?.source || "output"),
    })),
  };
}

function resetConsoleHistoryState(serverName) {
  const name = String(serverName || "").trim();
  if (!name) return;

  consoleHistory.delete(name);

  for (const key of Array.from(consoleHistoryNodeFetchCache.keys())) {
    if (key.startsWith(`${name}:`)) {
      consoleHistoryNodeFetchCache.delete(key);
    }
  }

  for (const key of Array.from(consoleHistoryNodeFetchInflight.keys())) {
    if (key.startsWith(`${name}:`)) {
      consoleHistoryNodeFetchInflight.delete(key);
    }
  }
}

function rememberConsoleLine(bot, line, source = "output") {
  if (!bot || !line) return;
  const name = String(bot);
  const item = normalizeConsoleHistoryEntries([{ line, ts: Date.now(), source }])[0];
  if (!item) return;
  let arr = consoleHistory.get(name) || [];

  if (!consoleHistory.has(name) && consoleHistory.size >= CONSOLE_HISTORY_MAX_SERVERS) {
    const oldest = consoleHistory.keys().next().value;
    if (oldest !== undefined) consoleHistory.delete(oldest);
  }

  arr.push(item);
  if (arr.length > CONSOLE_HISTORY_MAX_LINES) arr.shift();
  if (consoleHistory.has(name)) consoleHistory.delete(name);
  consoleHistory.set(name, arr);
}

function rememberConsoleCommand(bot, line) {
  rememberConsoleLine(bot, line, "command");
}

async function removeLegacyConsoleHistoryStorage() {
  try {
    await fsp.rm(path.join(process.cwd(), "data", "console-history"), { recursive: true, force: true });
  } catch (err) {
    console.warn("[consoleHistory] legacy cleanup warning:", err?.message || err);
  }
}

setInterval(() => {
  try {
    const now = Date.now();
    const ONE_DAY_MS = 24 * 60 * 60 * 1000;
    for (const [key, history] of consoleHistory.entries()) {
      if (!history || history.length === 0) {
        consoleHistory.delete(key);
        continue;
      }
      const lastEntry = history[history.length - 1];
      if (lastEntry && lastEntry.ts && (now - lastEntry.ts > ONE_DAY_MS)) {
        consoleHistory.delete(key);
      }
    }
  } catch (err) { console.debug("[consoleHistory] sweep error:", err.message); }
}, 60 * 60 * 1000).unref();

app.post("/api/servers/:name/console-history/reset", async (req, res, next) => {
  try {
    if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });

    const rawServerName = String(req.params.name || "").trim();
    const email = req.session.user;
    const user = email ? await findUserByEmail(email) : null;
    const isAdminUser = !!(user && user.admin);

    if (!isAdminUser && !(await userHasAccessToServer(email, rawServerName))) {
      return res.status(403).json({ error: "no access to server" });
    }

    const entry = await findServer(rawServerName);
    const serverName = entry?.name || rawServerName;
    if (!serverName) {
      return res.status(404).json({ error: "server not found" });
    }

    if (!isAdminUser) {
      const perms = await getEffectivePermsForUserOnServer(email, serverName);
      if (!perms?.server_start && !perms?.console_read) {
        return res.status(403).json({ error: "not authorized", permission: "server_start|console_read" });
      }
    }

    resetConsoleHistoryState(serverName);
    return res.json({ ok: true, server: serverName });
  } catch (err) {
    return next(err);
  }
});

app.get("/api/servers/:name/console-history", async (req, res, next) => {
  try {
    if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });

    const rawServerName = String(req.params.name || "").trim();
    const email = req.session.user;
    const user = email ? await findUserByEmail(email) : null;
    const isAdminUser = !!(user && user.admin);

    if (!isAdminUser && !(await userHasAccessToServer(email, rawServerName))) {
      return res.status(403).json({ error: "no access to server" });
    }

    const entry = await findServer(rawServerName);
    const serverName = entry?.name || rawServerName;
    if (!isAdminUser) {
      const perms = await getEffectivePermsForUserOnServer(email, serverName);
      if (!perms?.console_read) {
        return res.status(403).json({ error: "not authorized", permission: "console_read" });
      }
    }

    const snapshot = await getConsoleHistorySnapshot(serverName, req.query.limit, {
      includeCommands: String(req.query.includeCommands || "").trim() === "1",
    });

    return res.json({ ok: true, server: serverName, total: snapshot.total, lines: snapshot.lines });
  } catch (err) {
    return next(err);
  }
});

io.on("connection", async (socket) => {
  let email = socket.request?.session?.user || null;
  let user = null;
  let isAdminUser = false;
  let accessSet = new Set();
  try {
    user = email ? await findUserByEmail(email) : null;
    isAdminUser = !!(user && user.admin);
    if (!isAdminUser && email) {
      const accessList = await getAccessListForEmail(email);
      accessSet = new Set(accessList || []);
    }
  } catch (err) {
    console.error("[socket] init failed:", err);
    email = null;
    isAdminUser = false;
    accessSet = new Set();
  }
  let messageCount = 0;
  socket.use((packet, next) => {
    if (!packet) return next();
    messageCount += 1;
    if (messageCount > 60) {
      console.warn(`[SECURITY] Socket spam detected from IP: ${getSocketIp(socket)}`);
      return socket.disconnect(true);
    }
    return next();
  });

  const spamInterval = setInterval(() => {
    messageCount = 0;
  }, 60_000);

  function deny(room, msg = "Permission denied") {
    try { socket.emit("output", msg); } catch { }
  }

  async function hasPerm(botName, permKey) {
    if (!email) return false;
    if (isAdminUser) return true;
    const perms = await getEffectivePermsForUserOnServer(email, botName);
    return !!(perms && perms[permKey]);
  }

  function canAccessBot(botName) {
    if (!botName) return false;
    if (isAdminUser) return true;
    if (!email) return false;
    if (accessSet.has("all")) return true;
    return accessSet.has(botName);
  }

  socket.on("join", async (botName) => {
    const name = String(botName || "").trim();
    if (!name) return;
    if (!canAccessBot(name)) return deny(name, "[ADPanel] You don't have access to this server.");
    const canReadConsole = await hasPerm(name, "console_read");

    try { await socket.join(name); } catch { }
    if (canReadConsole) {
      try { await socket.join(getConsoleRoom(name)); } catch { }
    }
    panel(socket, name, "Connected");

    const entry = await findServer(name);
    if (!entry || !isRemoteEntry(entry)) {
      _emitLine(name, "[ADPanel] This server is not attached to a node.");
      return;
    }

    const node = await findNodeByIdOrName(entry.nodeId);
    if (!node) return _emitLine(name, "[ADPanel] Node not found.");
    const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
    if (!baseUrl) return _emitLine(name, "[ADPanel] Invalid node address.");

    const headers = nodeAuthHeadersFor(node, true);
    startRoomStatusWatcher(name, baseUrl, headers);
    if (canReadConsole) tailLogsRemote(name, baseUrl, headers);
  });

  socket.on("dashboard:watch", (payload) => {
    const rawBots = (Array.isArray(payload) ? payload : (Array.isArray(payload?.bots) ? payload.bots : [])).slice(0, 500);
    const bots = rawBots.map(b => String(b || "").trim()).filter(Boolean).filter(canAccessBot);
    startDashboardWatcher(socket, bots);
  });

  socket.on("dashboard:subscribe", async (payload) => {
    const rawBots = (Array.isArray(payload) ? payload :
      Array.isArray(payload?.bots) ? payload.bots : []).slice(0, 500);

    socket.join('dashboard');

    const statuses = {};
    const validNames = rawBots.map(n => String(n || '').trim()).filter(n => n && canAccessBot(n));
    const BATCH = 200;
    for (let i = 0; i < validNames.length; i += BATCH) {
      const batch = validNames.slice(i, i + BATCH);
      const entries = await Promise.all(batch.map(async (safeName) => {
        const cached = await getCachedServerStatusLenient(safeName);
        if (!cached) return null;
        const entry = {
          status: cached.status,
          cpu: cached.cpu,
          memory: cached.memory,
          disk: cached.disk,
          updatedAt: cached.updatedAt
        };
        if (typeof cached.nodeOnline === "boolean") {
          entry.nodeOnline = cached.nodeOnline;
        }
        return [safeName, entry];
      }));
      for (const e of entries) {
        if (e) statuses[e[0]] = e[1];
      }
    }

    socket.emit('dashboard:initial', { statuses });
  });

  socket.on("dashboard:unsubscribe", () => {
    socket.leave('dashboard');
  });

  socket.on("resources:subscribe", async (payload) => {
    const serverName = String(payload?.server || payload || '').trim();
    if (!serverName || !canAccessBot(serverName)) return;
    const room = getResourceRoom(serverName);
    try { await socket.join(room); } catch { }
    startResourceStatsWatcher(serverName).catch(() => { });
    const watcher = resourceStatsWatchers.get(serverName);
    if (watcher && watcher.last) {
      try { socket.emit('resources:data', watcher.last); } catch { }
    }
  });

  socket.on("resources:subscribe-bulk", async (payload) => {
    const servers = (Array.isArray(payload?.servers || payload)
      ? (payload?.servers || payload)
      : []).slice(0, 100);
    for (const raw of servers) {
      const serverName = String(raw || '').trim();
      if (!serverName || !canAccessBot(serverName)) continue;
      const room = getResourceRoom(serverName);
      try { await socket.join(room); } catch { continue; }
      startResourceStatsWatcher(serverName).catch(() => { });
      const watcher = resourceStatsWatchers.get(serverName);
      if (watcher && watcher.last) {
        try { socket.emit('resources:data', watcher.last); } catch { }
      }
    }
  });

  socket.on("resources:unsubscribe", (payload) => {
    const serverName = String(payload?.server || payload || '').trim();
    if (!serverName) return;
    const room = getResourceRoom(serverName);
    try { socket.leave(room); } catch { }
  });

  socket.on("resources:unsubscribe-all", () => {
    try {
      for (const room of socket.rooms) {
        if (room && room.startsWith('res:')) {
          socket.leave(room);
        }
      }
    } catch { }
  });

  socket.on("disconnect", () => {
    clearInterval(spamInterval);
    try {
      for (const room of socket.rooms) {
        if (room && room !== socket.id && getRoomClientCount(room) === 0) stopRoomStatusWatcher(room);
        if (room && room.startsWith('res:')) {
          const serverName = room.slice(4);
          if (getResourceRoomClientCount(serverName) === 0) {
            stopResourceStatsWatcher(serverName);
          }
        }
      }
    } catch { }
    stopDashboardWatcher(socket);
  });

  socket.on("readFile", async ({ bot, path: rel }) => {
    const name = String(bot || "").trim();
    if (!name) return;
    if (!canAccessBot(name) || !(await hasPerm(name, "files_read"))) return deny(name, "[ADPanel] You are not allowed to read files on this server.");

    const ctx = await resolveRemoteFsContext(name);
    if (!ctx.remote) return socket.emit("fileData", { path: rel, content: "/* ERROR: server not found on node */" });

    try {
      const full = safeJoinUnix(ctx.baseDir, rel || "");
      const { status, json } = await nodeFsPost(ctx.node, "/v1/fs/read", { path: full, encoding: "utf8" }, 120000);
      if (status !== 200 || !json || !json.ok) throw new Error("node-read-failed");
      socket.emit("fileData", { path: rel, content: typeof json.content === "string" ? json.content : "" });
    } catch (e) {
      console.error("[socket][readFile] failed:", e);
      socket.emit("fileData", { path: rel, content: "/* ERROR: failed to load file */" });
    }
  });

  socket.on("writeFile", async ({ bot, path: rel, content }) => {
    const name = String(bot || "").trim();
    if (!name) return;
    if (!canAccessBot(name) || !(await hasPerm(name, "files_create"))) return deny(name, "[ADPanel] You are not allowed to edit files on this server.");

    const ctx = await resolveRemoteFsContext(name);
    if (!ctx.remote) return io.to(name).emit("toast", { type: "error", msg: "Save failed: server not found on node" });

    try {
      const full = safeJoinUnix(ctx.baseDir, rel || "");
      const { status, json } = await nodeFsPost(ctx.node, "/v1/fs/write", { path: full, content: content ?? "", encoding: "utf8" }, 120000);
      if (status !== 200 || !json || !json.ok) throw new Error("node-write-failed");
      io.to(name).emit("toast", { type: "success", msg: `Saved ${escapeHtml(rel)}` });
    } catch (e) {
      console.error("[socket][writeFile] failed:", e);
      io.to(name).emit("toast", { type: "error", msg: "Save failed: internal error" });
    }
  });

  socket.on("deleteFile", async ({ bot, path: rel }) => {
    const name = String(bot || "").trim();
    if (!name) return;
    if (!canAccessBot(name) || !(await hasPerm(name, "files_delete"))) return deny(name, "[ADPanel] You are not allowed to delete files on this server.");

    const ctx = await resolveRemoteFsContext(name);
    if (!ctx.remote) return io.to(name).emit("toast", { type: "error", msg: "Delete failed: server not found on node" });

    try {
      const full = safeJoinUnix(ctx.baseDir, rel || "");
      const { status, json } = await nodeFsPost(ctx.node, "/v1/fs/delete", { path: full, isDir: false }, 120000);
      if (status !== 200 || !json || !json.ok) throw new Error("node-delete-failed");
      io.to(name).emit("toast", { type: "success", msg: `Deleted ${escapeHtml(rel)}` });
    } catch (e) {
      console.error("[socket][deleteFile] failed:", e);
      io.to(name).emit("toast", { type: "error", msg: "Delete failed: internal error" });
    }
  });

  socket.on("action", async (data) => {
    const { bot, cmd } = data || {};
    const botName = String(bot || "").trim();
    if (!botName) return;

    if (!canAccessBot(botName)) return deny(botName, "[ADPanel] You don't have access to this server.");
    const canReadConsole = await hasPerm(botName, "console_read");
    try { socket.join(botName); } catch { }
    if (canReadConsole) {
      try { socket.join(getConsoleRoom(botName)); } catch { }
    }
    const entry = await findServer(botName);
    if (!entry || !isRemoteEntry(entry)) return _emitLine(botName, "[ADPanel] This server is not attached to a node.");

    const node = await findNodeByIdOrName(entry.nodeId);
    if (!node) return _emitLine(botName, "[ADPanel] Node not found.");

    const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
    if (!baseUrl) return _emitLine(botName, "[ADPanel] Invalid node address.");

    const headers = nodeAuthHeadersFor(node, true);

    startRoomStatusWatcher(botName, baseUrl, headers);
    if (canReadConsole) tailLogsRemote(botName, baseUrl, headers);

    if ((cmd === "stop" || cmd === "restart" || cmd === "kill") && !(await hasPerm(botName, "server_stop"))) return deny(botName);
    if (cmd === "run" && !(await hasPerm(botName, "server_start"))) return deny(botName);

    try {
      if (cmd === "run") {
        resetConsoleHistoryState(botName);
        panel(socket, botName, "Server starting");

        const defaultPort = normalizeTemplateId(entry.template) === "minecraft" ? 25565 : 3001;
        const chosenPort = entry.port ?? defaultPort;
        const hostPort = normalizeTemplateId(entry.template) === "minecraft" ? clampPort(chosenPort) : clampAppPort(chosenPort, defaultPort);

        const r = await httpRequestJson(`${baseUrl}/v1/servers/${encodeURIComponent(botName)}/start?wait=false`, "POST", headers, { hostPort }, 1_000);
        if (!([200, 202].includes(r.status)) || !(r.json && r.json.ok)) {
          const msg = (r.json && (r.json.error || r.json.detail)) || `node status ${r.status}`;
          emitPanel(botName, `Start failed: ${msg}`);
          return;
        }

        try {
          const cur = await findServer(botName);
          if (cur) await upsertServerIndexEntry({ ...cur, status: "starting" });
        } catch { }

        panel(socket, botName, "Start request accepted.");
        if (canReadConsole) tailLogsRemote(botName, baseUrl, headers);
        return;
      }

      if (cmd === "stop") {
        panel(socket, botName, "Server stopping");

        const r = await httpRequestJson(`${baseUrl}/v1/servers/${encodeURIComponent(botName)}/stop`, "POST", headers, null, 1_000);
        if (r.status !== 200 || !(r.json && r.json.ok)) throw new Error((r.json && (r.json.error || r.json.detail)) || `node status ${r.status}`);
        try {
          const cur = await findServer(botName);
          if (cur) await upsertServerIndexEntry({ ...cur, status: "stopped" });
        } catch { }

        panel(socket, botName, "Server stopped");
        return;
      }

      if (cmd === "restart") {
        resetConsoleHistoryState(botName);
        emitPanel(botName, "Server restarting");

        const r = await httpRequestJson(`${baseUrl}/v1/servers/${encodeURIComponent(botName)}/restart`, "POST", headers, null, 20_000);
        if (r.status !== 200 || !(r.json && r.json.ok)) throw new Error((r.json && (r.json.error || r.json.detail)) || `node status ${r.status}`);

        emitPanel(botName, "Server restarted");
        if (canReadConsole) tailLogsRemote(botName, baseUrl, headers);
        return;
      }

      if (cmd === "kill") {
        emitPanel(botName, "Server killing");

        const r = await httpRequestJson(`${baseUrl}/v1/servers/${encodeURIComponent(botName)}/kill`, "POST", headers, null, 20_000);
        if (r.status !== 200 || !(r.json && r.json.ok)) throw new Error((r.json && (r.json.error || r.json.detail)) || `node status ${r.status}`);

        emitPanel(botName, "Server killed");
        return;
      }

      _emitLine(botName, "Unknown cmd");
    } catch (e) {
      _emitLine(botName, `[error] ${e?.message || String(e)}`);
    }
  });

  socket.on("command", async (payload, ack) => {
    const botName = String(payload?.bot || "").trim();
    const reply = (data) => {
      if (typeof ack === "function") {
        try { ack(data); } catch { }
      }
    };
    const commandCheck = typeof nodesRouter.sanitizeConsoleCommand === "function"
      ? nodesRouter.sanitizeConsoleCommand(payload?.command)
      : { ok: true, command: String(payload?.command || "").trim() };
    if (!botName || !commandCheck.ok) {
      const errorCode = !botName ? "missing params" : (commandCheck.error || "invalid_command");
      if (botName && commandCheck.detail) {
        _emitLine(botName, `[ADPanel] command rejected: ${commandCheck.detail}`);
      }
      reply({ ok: false, error: errorCode, detail: commandCheck.detail || null });
      return;
    }
    const trimmed = commandCheck.command;

    if (!canAccessBot(botName) || !(await hasPerm(botName, "console_write"))) {
      deny(botName);
      reply({ ok: false, error: "permission denied" });
      return;
    }

    rememberConsoleCommand(botName, trimmed);

    const entry = await findServer(botName);
    if (!entry || !isRemoteEntry(entry)) {
      _emitLine(botName, "[ADPanel] This server is not attached to a node.");
      reply({ ok: false, error: "not remote" });
      return;
    }

    const node = await findNodeByIdOrName(entry.nodeId);
    if (!node) {
      _emitLine(botName, "[ADPanel] Node not found.");
      reply({ ok: false, error: "node not found" });
      return;
    }

    const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
    if (!baseUrl) {
      _emitLine(botName, "[ADPanel] Invalid node address.");
      reply({ ok: false, error: "invalid node address" });
      return;
    }

    const headers = nodeAuthHeadersFor(node, true);

    try {
      const r = await httpRequestJson(`${baseUrl}/v1/servers/${encodeURIComponent(botName)}/command`, "POST", headers, { command: trimmed }, 20_000);
      if (r.status !== 200 || !(r.json && r.json.ok)) {
        const msg = (r.json && (r.json.error || r.json.detail)) || `node status ${r.status}`;
        _emitLine(botName, "[ADPanel] command failed: " + msg);
        reply({ ok: false, error: msg, status: r.status });
        return;
      }
      reply({ ok: true });
    } catch (e) {
      _emitLine(botName, "[ADPanel] command error: " + (e?.message || String(e)));
      reply({ ok: false, error: e?.message || String(e) });
    }
  });
});

let httpsServer = null;
const LISTEN_HOST = NGINX_ENABLED ? APP_HOST : HOST;
const LISTEN_HTTP_PORT = NGINX_ENABLED ? APP_PORT : HTTP_PORT;
const USE_HTTPS = ENABLE_HTTPS && !NGINX_ENABLED;
const ANSI_RESET = "\x1b[0m";
const ANSI_BOLD = "\x1b[1m";
const ANSI_YELLOW = "\x1b[33m";
const ANSI_BRIGHT_YELLOW = "\x1b[93m";
const ANSI_BRIGHT_CYAN = "\x1b[96m";
let startupMaturityNoticePrinted = false;
let startupMaturityNoticeTimer = null;

if (USE_HTTPS) {
  try {
    const httpsOptions = {
      key: fs.readFileSync(SSL_KEY_PATH),
      cert: fs.readFileSync(SSL_CERT_PATH),
    };
    if (SSL_CA_PATH) httpsOptions.ca = fs.readFileSync(SSL_CA_PATH);
    httpsServer = https.createServer(httpsOptions, app);
    attachSshTerminalUpgradeProxy(httpsServer);
    httpsAvailable = true;
  } catch (err) {
    console.error("[https] Failed to initialize HTTPS server:", err?.message || err);
  }
}

if (httpsServer) io.attach(httpsServer);

function printStartupMaturityNotice() {
  if (startupMaturityNoticePrinted) return;
  startupMaturityNoticePrinted = true;

  const productName = process.env.APP_NAME || "ADPanel";
  const lines = [
    `${ANSI_BRIGHT_YELLOW}${ANSI_BOLD}====================================================================${ANSI_RESET}`,
    `${ANSI_BRIGHT_YELLOW}${ANSI_BOLD} ${productName} Product Update${ANSI_RESET}`,
    `${ANSI_BRIGHT_CYAN} ${productName} has reached its first stable release and is ready for production use.${ANSI_RESET}`,
    `${ANSI_YELLOW} While the core platform is solid, we're continuing to refine the overall experience —${ANSI_RESET}`,
    `${ANSI_YELLOW} from SaaS-level polish and workflow ergonomics to day-to-day operator usability.${ANSI_RESET}`,
    `${ANSI_YELLOW} Expect steady improvements as we ship updates focused on reliability, smoother workflows, and a more intuitive UX.${ANSI_RESET}`,
    `${ANSI_BRIGHT_YELLOW}${ANSI_BOLD}====================================================================${ANSI_RESET}`,
  ];

  console.log("");
  for (const line of lines) {
    console.log(line);
  }
}

function scheduleStartupMaturityNotice(delayMs = 1500) {
  if (startupMaturityNoticePrinted) return;
  if (startupMaturityNoticeTimer) {
    clearTimeout(startupMaturityNoticeTimer);
  }
  startupMaturityNoticeTimer = setTimeout(() => {
    startupMaturityNoticeTimer = null;
    printStartupMaturityNotice();
  }, delayMs);
  startupMaturityNoticeTimer.unref?.();
}

async function startServers() {
  try {
    await ensureMySqlRunning();
  } catch (e) {
    console.warn('[startup] Could not verify MySQL/MariaDB status:', e.message);
  }

  await db.ensureSchema();
  await removeLegacyConsoleHistoryStorage();

  try {
    const [cols] = await db.pool.execute("SHOW COLUMNS FROM servers LIKE 'display_name'");
    if (!cols || cols.length === 0) {
      await db.pool.execute("ALTER TABLE servers ADD COLUMN display_name VARCHAR(120) NULL AFTER name");
      console.log("[migration] Added display_name column to servers table");
    }
  } catch (e) {
    if (!String(e.message).includes("Duplicate column")) {
      console.warn("[migration] display_name migration note:", e.message);
    }
  }

  startStatusPolling();

  // Migrate database tools nginx configs to internal-only proxy
  try {
    migrateDbToolsNginxOnStartup();
  } catch (e) {
    console.warn('[startup] DB tools nginx migration warning:', e.message);
  }

  try {
    const snippetsDir = '/etc/nginx/snippets';
    if (fs.existsSync('/etc/nginx')) {
      fs.mkdirSync(snippetsDir, { recursive: true });
      const placeholderSnippets = ['pgadmin4.conf', 'phpmyadmin.conf', 'mongodb.conf'];
      for (const snippet of placeholderSnippets) {
        const snippetPath = path.join(snippetsDir, snippet);
        if (!fs.existsSync(snippetPath)) {
          fs.writeFileSync(snippetPath, `# Placeholder - will be populated when service is installed\n`, { mode: 0o644 });
          console.log(`[nginx] Created placeholder snippet: ${snippetPath}`);
        }
      }
    }
  } catch (e) {
    console.debug('[nginx] Could not create placeholder snippets:', e.message);
  }

  // Warm/recover DB tool backends on startup so first token access does not fail.
  try {
    const pgCfg = loadPgAdminConfig();
    if (pgCfg.enabled) {
      const pgReady = await ensureDbToolBackendReady('pgadmin');
      if (!pgReady.ok) {
        console.warn('[startup] pgAdmin backend readiness warning:', pgReady.error || 'unknown');
      }
    }

    const phpCfg = loadDatabaseConfig();
    if (phpCfg.enabled && phpCfg.config) {
      const phpReady = await ensureDbToolBackendReady('phpmyadmin');
      if (!phpReady.ok) {
        console.warn('[startup] phpMyAdmin backend readiness warning:', phpReady.error || 'unknown');
      }
    }
  } catch (e) {
    console.warn('[startup] DB backend warmup warning:', e.message);
  }

  try {
    const [cols] = await db.pool.query(`SHOW COLUMNS FROM users LIKE 'agent_access'`);
    if (!cols || cols.length === 0) {
      await db.query("ALTER TABLE users ADD COLUMN agent_access TINYINT(1) NOT NULL DEFAULT 0");
      console.log('[db] Added agent_access column to users table');
    }
  } catch (err) {
    if (!err.message?.includes('Duplicate column')) {
      console.debug('[db] agent_access column check:', err.message);
    }
  }

  try {
    const [cols] = await db.pool.query(`SHOW COLUMNS FROM users LIKE 'avatar_url'`);
    if (!cols || cols.length === 0) {
      await db.query(`ALTER TABLE users ADD COLUMN avatar_url VARCHAR(2048) DEFAULT NULL`);
      console.log('[db] Added avatar_url column to users table');
    }
  } catch (err) {
    if (!err.message?.includes('Duplicate column')) {
      console.debug('[db] avatar_url column check:', err.message);
    }
  }

  try {
    const [cols] = await db.pool.query(`SHOW COLUMNS FROM users LIKE 'username'`);
    if (!cols || cols.length === 0) {
      await db.query(`ALTER TABLE users ADD COLUMN username VARCHAR(50) DEFAULT NULL`);
      console.log('[db] Added username column to users table');
    }
  } catch (err) {
    if (!err.message?.includes('Duplicate column')) {
      console.debug('[db] username column check:', err.message);
    }
  }

  try {
    const [cols] = await db.pool.query(`SHOW COLUMNS FROM users LIKE 'username_changed_at'`);
    if (!cols || cols.length === 0) {
      await db.query(`ALTER TABLE users ADD COLUMN username_changed_at BIGINT UNSIGNED DEFAULT NULL`);
      console.log('[db] Added username_changed_at column to users table');
    }
  } catch (err) {
    if (!err.message?.includes('Duplicate column')) {
      console.debug('[db] username_changed_at column check:', err.message);
    }
  }

  try {
    const [cols] = await db.pool.query(`SHOW COLUMNS FROM users LIKE 'preferences'`);
    if (!cols || cols.length === 0) {
      await db.query(`ALTER TABLE users ADD COLUMN preferences TEXT DEFAULT NULL`);
      console.log('[db] Added preferences column to users table');
    }
  } catch (err) {
    if (!err.message?.includes('Duplicate column')) {
      console.debug('[db] preferences column check:', err.message);
    }
  }

  if (SCHEDULER_ENABLED && SCHEDULER_REDIS_URL) {
    setTimeout(async () => {
      try {
        await initializeScheduler();
      } catch (err) {
        console.error("[scheduler] Deferred initialization failed:", err?.message || err);
      }
    }, 100);
  }

  httpServer.listen(LISTEN_HTTP_PORT, LISTEN_HOST, () => {
    console.log(`[ADPanel] HTTP listening on http://${LISTEN_HOST}:${LISTEN_HTTP_PORT}`);
    if (NGINX_ENABLED) {
      console.log("[ADPanel] NGINX_ENABLED=1; expecting reverse proxy in front.");
    }
    startMaintenanceChecker();
    startPanelAutoUpdateScheduler();
    if (!httpsServer) {
      scheduleStartupMaturityNotice();
    }
  });

  if (httpsServer) {
    httpsServer.listen(HTTPS_PORT, HOST, () => {
      console.log(`[ADPanel] HTTPS listening on https://${HOST}:${HTTPS_PORT}`);
      scheduleStartupMaturityNotice();
    });
  }
}

startServers().catch((err) => {
  console.error("[startup] Failed to initialize database schema:", err?.message || err);
  process.exit(1);
});
