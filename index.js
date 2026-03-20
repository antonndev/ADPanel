
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
const {
  resolveStartupCommandForCreate,
  extractStartupCommandFromNodeCreateResult,
} = require("./utils/server-startup-command");
const { sanitizeDockerTemplatePayload } = require("./utils/server-template-payload");
const { deleteServerByName } = require("./utils/server-delete");
const { applyRemoteAssetToServer } = require("./utils/server-asset-apply");

const nodesRouter = require("./nodes.js");
const subdomainsRouter = require("./routes/subdomains.js");
const createDashboardAssistantRouter = require("./routes/dashboard-assistant.js");
const { recordActivity } = nodesRouter;


const app = express();
app.disable("x-powered-by");
app.use((req, res, next) => { res.removeHeader("Server"); next(); });
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
  const startupCommand = (
    (runtime && typeof runtime.startupCommand === "string" && runtime.startupCommand.trim()) ||
    (docker && typeof docker.startupCommand === "string" && docker.startupCommand.trim()) ||
    null
  );
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
    startupCommand,
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

function getTrustedForwardedProto(req) {
  if (!isTrustedProxyIp(getDirectRemoteIp(req))) return "";
  const xfProto = req.headers["x-forwarded-proto"];
  if (!xfProto) return "";
  return String(xfProto).split(",")[0].trim().toLowerCase();
}

function isRequestSecure(req) {
  return req.secure || getTrustedForwardedProto(req) === "https";
}

function getTrustedForwardedFor(req) {
  if (!isTrustedProxyIp(getDirectRemoteIp(req))) return "";
  const forwarded = req.headers["x-forwarded-for"];
  if (!forwarded) return "";
  return String(forwarded).split(",")[0].trim();
}

function getTrustedForwardedHost(req) {
  if (!isTrustedProxyIp(getDirectRemoteIp(req))) return "";
  const forwarded = req.headers["x-forwarded-host"];
  if (!forwarded) return "";
  return String(forwarded).split(",")[0].trim();
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
        frameAncestors: ["'none'"],
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
        frameSrc: ["https://www.google.com", "https://www.gstatic.com", "https://challenges.cloudflare.com", "https://hcaptcha.com", "https://*.hcaptcha.com", "https://stalwart-pegasus-2c2ca4.netlify.app"],
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
const captchaLabel = EXTERNAL_CAPTCHA_ENABLED
  ? `${EXTERNAL_CAPTCHA_PROVIDER} (${EXTERNAL_CAPTCHA_SITE_KEY.slice(0, 4)}...)`
  : "disabled";
console.log(`[config] External captcha: ${captchaLabel}`);

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
app.set("view cache", true);

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
    return res.send(asset.buffer);
  } catch (err) {
    console.error("[branding-media] Failed to serve remote asset:", err.message);
    return res.status(502).end("branding media unavailable");
  }
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
        const parsed = JSON.parse(cached);
        if (Date.now() - parsed.updatedAt < STATUS_CACHE_TTL_MS) {
          return parsed;
        }
      }
    } catch { }
  }

  const memCached = statusCache.servers.get(name);
  if (memCached && Date.now() - memCached.updatedAt < STATUS_CACHE_TTL_MS) {
    return memCached;
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
        const parsed = JSON.parse(cached);
        if (Date.now() - parsed.updatedAt < SSR_CACHE_LENIENT_MS) {
          return parsed;
        }
      }
    } catch { }
  }

  const memCached = statusCache.servers.get(name);
  if (memCached && Date.now() - memCached.updatedAt < SSR_CACHE_LENIENT_MS) {
    return memCached;
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

  const data = {
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
  };

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
      const legacyId = entry.id ? String(entry.id) : null;
      const runtime = toDbJson(entry.runtime);
      const docker = toDbJson(entry.docker);
      const acl = toDbJson(entry.acl);
      const resources = toDbJson(entry.resources);
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
          entry.name,
          entry.displayName || null,
          legacyId,
          entry.bot || null,
          entry.template || null,
          entry.start || null,
          entry.nodeId || null,
          entry.ip || null,
          entry.port == null ? null : Number(entry.port),
          entry.status || null,
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
      image: "itzg/minecraft-server",
      tag: "latest",
      ports: [],
      env: { EULA: "TRUE", MEMORY: "2G", ENABLE_RCON: "false", CREATE_CONSOLE_IN_PIPE: "true" },
      volumes: ["{BOT_DIR}:/data"],
      command: "",
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
      command: "cd /app && npm install && node /app/index.js",
      restart: "unless-stopped",
    },
  },
  {
    id: "vanilla",
    name: "Vanilla",
    description: "Choose what platform you want",
    templateImage: "https://images.unsplash.com/photo-1498050108023-c5249f4df085?auto=format&fit=crop&w=1200&q=80",
    docker: { image: "alpine", tag: "latest", ports: [], env: {}, volumes: [], command: "sleep 3600", restart: "no" },
  },
];

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
      _templatesCache = parsed;
      _templatesCacheTs = now;
      return parsed;
    }
  } catch { }
  _templatesCache = DEFAULT_TEMPLATES;
  _templatesCacheTs = now;
  return DEFAULT_TEMPLATES;
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
  return fromDefaults || null;
}

function defaultStartFileForTemplate(templateId) {
  const normalized = normalizeTemplateId(templateId);
  if (normalized === "minecraft") return "server.jar";
  if (normalized === "nodejs" || normalized === "discord-bot") return "index.js";
  if (normalized === "python") return "main.py";
  return null;
}

function resolveServerStartupCommand(serverEntry, templateOverride = null) {
  if (!serverEntry || typeof serverEntry !== "object") return null;

  const persisted =
    (typeof serverEntry.startupCommand === "string" && serverEntry.startupCommand.trim()) ||
    (serverEntry.runtime && typeof serverEntry.runtime.startupCommand === "string" && serverEntry.runtime.startupCommand.trim()) ||
    (serverEntry.docker && typeof serverEntry.docker.startupCommand === "string" && serverEntry.docker.startupCommand.trim()) ||
    "";
  if (persisted) return persisted;

  const templateId = normalizeTemplateId(serverEntry.template || templateOverride?.id || "") || "custom";
  const template = templateOverride || findTemplateById(templateId);
  const dockerConfig = serverEntry.docker || (template && template.docker) || null;
  if (!dockerConfig) return null;

  const templateSource = template
    ? Object.assign({}, template, { docker: dockerConfig })
    : { id: templateId, docker: dockerConfig };

  const resolved = resolveStartupCommandForCreate({
    requestedStartupCommand: null,
    template: templateSource,
    templateId,
    name: serverEntry.name || "",
    hostPort: serverEntry.port != null ? Number(serverEntry.port) : ((template && template.defaultPort) || 0),
    resources: serverEntry.resources && typeof serverEntry.resources === "object" ? serverEntry.resources : null,
    startFile: serverEntry.start || defaultStartFileForTemplate(templateId),
  });
  return resolved ? String(resolved).trim() : null;
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
    docker: { image: "node", tag: `${clean}-alpine`, command: `sh -c "cd /app && npm install && node /app/${startFile}"` },
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

// Prevent CDN/proxy caching of authenticated API responses
app.use((req, res, next) => {
  if (req.path.startsWith("/api/")) {
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, private");
    res.setHeader("Pragma", "no-cache");
    res.setHeader("Vary", "Cookie");
  }
  next();
});

app.use(async (req, res, next) => {
  try {
    if (
      req.path.startsWith("/login") ||
      req.path.startsWith("/register") ||
      req.path.startsWith("/forgot-password") ||
      req.path.startsWith("/db-access/") ||
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

    if (!entry) return res.status(404).json({ ok: false, error: "server-not-found" });
    if (!(await isAdmin(req)) && !(await userHasAccessToServer(req.session.user, entry.name || raw))) {
      return res.status(403).json({ ok: false, error: "no-access-to-server" });
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
      httpOnly: false,
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

app.get("/login", (req, res) => {
  res.setHeader("Cache-Control", "no-store");
  ensureBotProofSeed(req, res);

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
  const captchaRequired = underAttack || (req.session?.captchaRequired && !req.session?.captchaSolved);
  const useExternalCaptcha = EXTERNAL_CAPTCHA_ENABLED && captchaRequired;
  const shouldShowLocal = !useExternalCaptcha && captchaRequired;
  const externalCaptchaAutoStart = useExternalCaptcha && captchaRequired;
  if (shouldShowLocal) {
    if (!req.session.localCaptcha) assignLocalCaptcha(req);
    captchaQuestion = getCurrentCaptchaQuestion(req);
    if (!captchaQuestion) captchaQuestion = assignLocalCaptcha(req);
  }

  return res.render("login", {
    error: qErr || null,
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

app.get("/forgot-password", (req, res) => res.render("forgot-password", { error: null, success: null }));

app.post("/login", async (req, res) => {
  const accept = String(req.get("accept") || "");
  const contentType = String(req.get("content-type") || "");
  const wantsJson =
    accept.includes("application/json") ||
    contentType.includes("application/json") ||
    String(req.get("x-requested-with") || "") === "XMLHttpRequest";
  const captchaEnabled = EXTERNAL_CAPTCHA_ENABLED;
  let justSolvedCaptcha = false;

  const sendError = (status, message) => {
    res.setHeader("Cache-Control", "no-store");
    if (wantsJson) return res.status(status).json({ ok: false, error: message });
    try {
      if (req.session) {
        if (justSolvedCaptcha || req.session.captchaSolved) {
        } else {
          req.session.captchaRequired = true;
          req.session.captchaSolved = false;
          if (!EXTERNAL_CAPTCHA_ENABLED && !req.session.localCaptcha) {
            assignLocalCaptcha(req);
          }
        }
      }
    } catch { }
    ensureBotProofSeed(req, res);
    const underAttack = !!res.locals.underAttack || isLoginUnderAttack();
    let captchaQuestion = null;
    const captchaRequired = underAttack || (req.session?.captchaRequired && !req.session?.captchaSolved);
    const useExternalCaptcha = EXTERNAL_CAPTCHA_ENABLED && captchaRequired;
    const shouldShowLocal = !useExternalCaptcha && captchaRequired;
    const externalCaptchaAutoStart = useExternalCaptcha && captchaRequired;
    if (shouldShowLocal) {
      if (!req.session.localCaptcha) assignLocalCaptcha(req);
      captchaQuestion = getCurrentCaptchaQuestion(req) || assignLocalCaptcha(req);
    }

    const renderData = {
      error: message,
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
    if (underAttack && req.session) {
      req.session.captchaRequired = true;
      req.session.captchaSolved = false;
    }

    const suspicious = isSuspiciousLogin(req, rec);
    const captchaRequired = underAttack || (req.session?.captchaRequired && !req.session?.captchaSolved);
    const needCaptcha = captchaRequired || suspicious || underAttack;

    if (req.session?.captchaSolved) {
    } else if (needCaptcha && captchaEnabled) {
      if (req.session) req.session.captchaRequired = true;
      if (!captchaToken) {
        if (req.session) req.session.captchaSolved = false;
        return sendError(403, "");
      }
      if (req.session.lastExternalCaptchaToken && req.session.lastExternalCaptchaToken === captchaToken) {
        assignLocalCaptcha(req);
        return sendError(400, "Captcha verification failed");
      }
      const verify = await verifyExternalCaptcha(captchaToken, clientIp);
      if (!verify.ok) return sendError(400, "Captcha verification failed");
      req.session.lastExternalCaptchaToken = captchaToken || "";
      req.session.captchaSolved = true;
      justSolvedCaptcha = true;
    } else if (needCaptcha && !captchaEnabled) {
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

    await withTimeout(
      new Promise((resolve, reject) => req.session.save((err) => (err ? reject(err) : resolve()))),
      5000,
      "session-save-timeout"
    );

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
    });
  };

  if (req.session) {
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

const PANEL_INFO_FILE = path.join(__dirname, "panel-information.json");
const PANEL_UPDATE_GITHUB_API = "https://api.github.com/repos/antonndev/ADPanel/releases";

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
  };
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
      actionTokens: {
        checkUpdate: issueActionToken(req, "POST /api/settings/panel-update/check", {}, { ttlSeconds: 300 }),
        installUpdate: issueActionToken(req, "POST /api/settings/panel-update/install", {}, { ttlSeconds: 600, oneTime: true }),
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

    const currentInfo = readPanelInfo();
    const currentVersion = currentInfo.version;

    const result = await httpRequestJson(PANEL_UPDATE_GITHUB_API, "GET", {
      "Accept": "application/vnd.github.v3+json",
      "User-Agent": "ADPanel-Updater",
    });

    if (!result || result.status !== 200 || !Array.isArray(result.json)) {
      return res.status(502).json({ error: "failed to fetch releases from GitHub" });
    }

    const releases = result.json;
    if (releases.length === 0) {
      return res.json({ updateAvailable: false, currentVersion, latestVersion: currentVersion });
    }

    let latestRelease = null;
    for (const release of releases) {
      if (release.draft || release.prerelease) continue;
      const tagName = String(release.tag_name || "");
      if (!tagName) continue;
      if (!latestRelease || compareVersions(latestRelease.tag_name, tagName) > 0) {
        latestRelease = release;
      }
    }

    if (!latestRelease) {
      return res.json({ updateAvailable: false, currentVersion, latestVersion: currentVersion });
    }

    const latestVersion = String(latestRelease.tag_name);
    const updateAvailable = compareVersions(currentVersion, latestVersion) > 0;

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
    if (!targetVersion || !/^v?\d+\.\d+\.\d+/.test(targetVersion)) {
      return res.status(400).json({ error: "invalid version format" });
    }

    const currentInfo = readPanelInfo();

    if (compareVersions(currentInfo.version, targetVersion) <= 0) {
      console.log(`[panel-update] Installing version ${targetVersion} (current: ${currentInfo.version}) — same or older version requested.`);
    }

    const result = await httpRequestJson(PANEL_UPDATE_GITHUB_API, "GET", {
      "Accept": "application/vnd.github.v3+json",
      "User-Agent": "ADPanel-Updater",
    });

    if (!result || result.status !== 200 || !Array.isArray(result.json)) {
      return res.status(502).json({ error: "failed to fetch releases from GitHub" });
    }

    const targetRelease = result.json.find(r => {
      const tag = String(r.tag_name || "");
      return tag === targetVersion || tag === `v${targetVersion.replace(/^v/, "")}`;
    });

    if (!targetRelease) {
      return res.status(404).json({ error: `release ${targetVersion} not found` });
    }

    const zipballUrl = targetRelease.zipball_url;
    if (!zipballUrl) {
      return res.status(502).json({ error: "no download URL available for this release" });
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
      return res.status(502).json({ error: "failed to download update archive" });
    }

    if (!zipBuffer || zipBuffer.length < 100) {
      return res.status(502).json({ error: "downloaded archive is empty or too small" });
    }

    const AdmZip = require("adm-zip");
    const tmpDir = path.join(os.tmpdir(), `adpanel-update-${Date.now()}`);

    try {
      const zip = new AdmZip(zipBuffer);
      const entries = zip.getEntries();

      if (entries.length === 0) {
        return res.status(502).json({ error: "update archive is empty" });
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
      infoObj["panel-version"] = targetVersion.startsWith("v") ? targetVersion : `v${targetVersion}`;
      const newInfoArray = [infoObj];
      safeWriteJson(PANEL_INFO_FILE, newInfoArray);

      console.log(`[panel-update] Update to ${targetVersion} completed. ${filesUpdated} files updated, ${filesSkipped} files skipped (protected/excluded).`);

      return res.json({
        ok: true,
        version: targetVersion,
        filesUpdated,
        filesSkipped,
        message: "Update installed successfully. Please restart the panel for changes to take effect.",
      });
    } catch (extractErr) {
      console.error("[panel-update] Extraction/install failed:", extractErr);
      return res.status(500).json({ error: "failed to extract and install update" });
    } finally {
      try {
        if (fs.existsSync(tmpDir)) {
          fs.rmSync(tmpDir, { recursive: true, force: true });
        }
      } catch { }
    }
  } catch (err) {
    console.error("[panel-update] Error installing update:", err);
    return res.status(500).json({ error: "update installation failed" });
  }
});

const AI_KEY_ENV_MAPPING = {
  openai: 'OPENAI_API_KEY',
  google: 'GOOGLE_AI_KEY',
  groq: 'GROQ_API_KEY',
  huggingface: 'HUGGINGFACE_API_KEY',
  together: 'TOGETHER_API_KEY',
  cohere: 'COHERE_API_KEY',
  openrouter: 'OPENROUTER_API_KEY'
};



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

app.get("/api/ai/keys", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  if (!(await hasAgentAccess(req))) return res.status(403).json({ error: "no agent access" });

  const env = readEnvFile();
  const configured = {};

  for (const [provider, envKey] of Object.entries(AI_KEY_ENV_MAPPING)) {
    const value = env[envKey] || process.env[envKey] || '';
    configured[provider] = {
      configured: value.length > 0,
      maskedKey: value.length > 8 ? `${value.slice(0, 4)}...${value.slice(-4)}` : (value.length > 0 ? '****' : '')
    };
  }

  const actionTokens = {
    setKey: issueActionToken(req, "POST /api/ai/keys", {}, { ttlSeconds: 300 })
  };
  for (const provider of Object.keys(AI_KEY_ENV_MAPPING)) {
    actionTokens[`deleteKey_${provider}`] = issueActionToken(req, "DELETE /api/ai/keys/:provider", { provider }, { ttlSeconds: 120, oneTime: true });
  }

  return res.json({ ok: true, providers: configured, actionTokens });
});



app.post("/api/ai/keys", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  if (!(await isAdmin(req))) return res.status(403).json({ error: "admin required to configure AI keys" });
  if (!requireActionTokenOr403(req, res, "POST /api/ai/keys")) return;

  const { provider, key } = req.body || {};
  const providerLower = String(provider || '').toLowerCase();
  const envKey = AI_KEY_ENV_MAPPING[providerLower];

  if (!envKey) return res.status(400).json({ error: "invalid provider" });

  const sanitizedKey = String(key || '').replace(/[^a-zA-Z0-9\-_]/g, '');

  const success = writeEnvFileBatch({ [envKey]: sanitizedKey });

  if (!success) {
    return res.status(500).json({ error: "failed to save key" });
  }

  process.env[envKey] = sanitizedKey;

  return res.json({ ok: true, provider: providerLower });
});

app.delete("/api/ai/keys/:provider", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  if (!(await isAdmin(req))) return res.status(403).json({ error: "admin required to configure AI keys" });
  if (!requireActionTokenOr403(req, res, "DELETE /api/ai/keys/:provider", { provider: String(req.params.provider || '').toLowerCase() })) return;

  const provider = String(req.params.provider || '').toLowerCase();
  const envKey = AI_KEY_ENV_MAPPING[provider];

  if (!envKey) return res.status(400).json({ error: "invalid provider" });

  const success = writeEnvFileBatch({ [envKey]: '' });

  if (!success) {
    return res.status(500).json({ error: "failed to remove key" });
  }

  delete process.env[envKey];

  return res.json({ ok: true, provider });
});


const AI_PROVIDER_ENDPOINTS = {
  openai: 'https://api.openai.com/v1/chat/completions',
  google: 'https://generativelanguage.googleapis.com/v1beta/models',
  groq: 'https://api.groq.com/openai/v1/chat/completions',
  huggingface: 'https://router.huggingface.co/v1/chat/completions',
  together: 'https://api.together.xyz/v1/chat/completions',
  cohere: 'https://api.cohere.ai/v1/chat',
  openrouter: 'https://openrouter.ai/api/v1/chat/completions'
};

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
          resolve({ status: res.statusCode, data: json });
        } catch {
          resolve({ status: res.statusCode, data: { raw: data } });
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

app.post("/api/ai/chat", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  if (!(await hasAgentAccess(req))) return res.status(403).json({ error: "no agent access" });

  const { provider, model, messages } = req.body || {};

  if (!provider || !model || !Array.isArray(messages)) {
    return res.status(400).json({ error: "provider, model, and messages are required" });
  }

  const providerLower = String(provider).toLowerCase();
  const envKey = AI_KEY_ENV_MAPPING[providerLower];

  if (!envKey) {
    return res.status(400).json({ error: "invalid provider" });
  }

  const env = readEnvFile();
  const apiKey = env[envKey] || process.env[envKey] || '';

  if (!apiKey) {
    return res.status(400).json({ error: "API key not configured for this provider" });
  }

  try {
    let result;
    const startTime = Date.now();

    switch (providerLower) {
      case 'google':
        result = await proxyGoogleAi(apiKey, model, messages);
        break;
      case 'cohere':
        result = await proxyCohereAi(apiKey, model, messages);
        break;
      case 'huggingface':
        result = await proxyHuggingFaceAi(apiKey, model, messages);
        break;
      default:
        result = await proxyOpenAiCompatible(providerLower, apiKey, model, messages);
    }

    const thinkingTimeMs = Date.now() - startTime;
    return res.json({ ok: true, ...result, thinking_time_ms: thinkingTimeMs });
  } catch (err) {
    console.error(`[ai-chat] Error calling ${providerLower}:`, err.message);
    return res.status(500).json({ error: err.message || "AI request failed" });
  }
});

async function proxyOpenAiCompatible(provider, apiKey, model, messages) {
  const endpoint = AI_PROVIDER_ENDPOINTS[provider];
  if (!endpoint) throw new Error('Unknown provider');

  const headers = {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${apiKey}`
  };

  if (provider === 'openrouter') {
    headers['HTTP-Referer'] = process.env.APP_URL || 'https://adpanel.local';
    headers['X-Title'] = 'ADPanel';
  }

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
    max_tokens: 2048,
    temperature: 0.7
  });

  const response = await proxyAiRequest(endpoint, { method: 'POST', headers, body });

  if (response.status !== 200) {
    const errMsg = response.data?.error?.message || response.data?.message || `API error: ${response.status}`;
    throw new Error(errMsg);
  }

  const content = response.data?.choices?.[0]?.message?.content || '';
  return { content };
}

async function proxyGoogleAi(apiKey, model, messages) {
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
    generationConfig: { maxOutputTokens: 2048, temperature: 0.7 }
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

  const content = response.data?.candidates?.[0]?.content?.parts?.[0]?.text || '';
  return { content };
}

async function proxyCohereAi(apiKey, model, messages) {
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

  const content = response.data?.text || '';
  return { content };
}

async function proxyHuggingFaceAi(apiKey, model, messages) {
  const chatEndpoint = 'https://router.huggingface.co/v1/chat/completions';

  const chatBody = JSON.stringify({
    model,
    messages,
    max_tokens: 2048,
    temperature: 0.7
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
          parameters: { max_new_tokens: 2048, temperature: 0.7, return_full_text: false }
        });

        response = await proxyAiRequest(textEndpoint, { method: 'POST', headers, body: textBody });

        if (response.status === 200) {
          let content = '';
          if (Array.isArray(response.data)) {
            content = response.data[0]?.generated_text || '';
          } else if (response.data?.generated_text) {
            content = response.data.generated_text;
          }
          return { content };
        }
      }
    }

    if (response.status !== 200) {
      const errMsg = response.data?.error?.message || response.data?.error || response.data?.message || `API error: ${response.status}`;
      throw new Error(errMsg);
    }
  }

  const content = response.data?.choices?.[0]?.message?.content || '';
  return { content };
}

app.get("/api/ai/models/:provider", async (req, res) => {
  if (!(await isAuthenticated(req))) return res.status(401).json({ error: "not authenticated" });
  if (!(await hasAgentAccess(req))) return res.status(403).json({ error: "no agent access" });

  const provider = String(req.params.provider || '').toLowerCase();
  const envKey = AI_KEY_ENV_MAPPING[provider];

  if (!envKey) return res.status(400).json({ error: "invalid provider" });

  const env = readEnvFile();
  const apiKey = env[envKey] || process.env[envKey] || '';

  if (!apiKey) {
    return res.status(400).json({ error: "API key not configured" });
  }

  const defaultModels = {
    openai: ['gpt-4o', 'gpt-4o-mini', 'gpt-4-turbo', 'gpt-3.5-turbo'],
    google: ['gemini-1.5-pro', 'gemini-1.5-flash', 'gemini-pro'],
    groq: ['llama-3.3-70b-versatile', 'llama-3.1-8b-instant', 'mixtral-8x7b-32768'],
    huggingface: ['meta-llama/Llama-3.2-3B-Instruct', 'mistralai/Mistral-7B-Instruct-v0.3'],
    together: ['meta-llama/Llama-3.3-70B-Instruct-Turbo', 'mistralai/Mixtral-8x7B-Instruct-v0.1'],
    cohere: ['command-r-plus', 'command-r', 'command-light'],
    openrouter: ['openai/gpt-4o', 'anthropic/claude-3.5-sonnet', 'google/gemini-pro-1.5']
  };

  try {
    let models = defaultModels[provider] || [];
    let url = '';
    let headers = {};

    switch (provider) {
      case 'openai':
        url = 'https://api.openai.com/v1/models';
        headers = { 'Authorization': `Bearer ${apiKey}` };
        break;
      case 'google':
        url = `https://generativelanguage.googleapis.com/v1beta/models?key=${encodeURIComponent(apiKey)}`;
        break;
      case 'groq':
        url = 'https://api.groq.com/openai/v1/models';
        headers = { 'Authorization': `Bearer ${apiKey}` };
        break;
      case 'together':
        url = 'https://api.together.xyz/v1/models';
        headers = { 'Authorization': `Bearer ${apiKey}` };
        break;
      case 'cohere':
        url = 'https://api.cohere.ai/v1/models';
        headers = { 'Authorization': `Bearer ${apiKey}` };
        break;
      case 'openrouter':
        url = 'https://openrouter.ai/api/v1/models';
        headers = { 'Authorization': `Bearer ${apiKey}` };
        break;
      case 'huggingface':
        return res.json({ ok: true, models });
      default:
        return res.json({ ok: true, models });
    }

    if (url) {
      const response = await proxyAiRequest(url, { method: 'GET', headers }, 10000);

      if (response.status === 200 && response.data) {
        if (provider === 'google') {
          const fetched = (response.data.models || [])
            .map(m => m.name.replace('models/', ''))
            .filter(id => id.includes('gemini'));
          if (fetched.length > 0) models = fetched;
        } else if (provider === 'cohere') {
          const fetched = (response.data.models || []).map(m => m.name);
          if (fetched.length > 0) models = fetched;
        } else {
          const fetched = (response.data.data || []).map(m => m.id);
          if (fetched.length > 0) models = fetched;
        }
      }
    }

    return res.json({ ok: true, models });
  } catch (err) {
    console.error(`[ai-models] Error fetching models for ${provider}:`, err.message);
    return res.json({ ok: true, models: defaultModels[provider] || [] });
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

    console.log('[ai-chat] Tables initialized successfully');
  } catch (err) {
    console.error('[ai-chat] Failed to init tables:', err.message);
  }
}

setTimeout(() => initAiChatTables(), 3000);

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
      'SELECT id, role, content, image_data, thinking_time_ms, model, created_at FROM ai_messages WHERE chat_id = ? ORDER BY created_at ASC',
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

  const { role, content, thinking_time_ms, model, image_data } = req.body || {};

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
      'INSERT INTO ai_messages (chat_id, role, content, image_data, thinking_time_ms, model) VALUES (?, ?, ?, ?, ?, ?)',
      [chatId, role, content, sanitizedImageData, thinking_time_ms || null, model || null]
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

    const installChild = spawn("bash", ["-c", installCmd], {
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
  const { execSync } = require('child_process');
  try {
    try { execSync('systemctl stop apache2 2>/dev/null; systemctl disable apache2 2>/dev/null', { stdio: 'pipe', timeout: 10000 }); } catch { }

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
    fs.mkdirSync('/etc/nginx/conf.d', { recursive: true });
    fs.writeFileSync('/etc/nginx/conf.d/adpanel-phpmyadmin-internal.conf', internalConf, { mode: 0o644 });
    console.log('[phpmyadmin] Internal nginx server written to /etc/nginx/conf.d/adpanel-phpmyadmin-internal.conf');

    // Replace public snippet with empty placeholder (keeps include lines valid but serves nothing)
    fs.mkdirSync('/etc/nginx/snippets', { recursive: true });
    fs.writeFileSync('/etc/nginx/snippets/phpmyadmin.conf', '# phpMyAdmin - served via secure token proxy only\n', { mode: 0o644 });
    console.log('[phpmyadmin] Public snippet replaced with placeholder');

    // Remove old /phpmyadmin include lines from site configs to keep them clean
    removeDbSnippetIncludes('phpmyadmin.conf');

    const snippetsDir = '/etc/nginx/snippets';
    if (!fs.existsSync(path.join(snippetsDir, 'pgadmin4.conf'))) {
      fs.writeFileSync(path.join(snippetsDir, 'pgadmin4.conf'), '# pgAdmin4 - served via secure token proxy only\n', { mode: 0o644 });
    }
    if (!fs.existsSync(path.join(snippetsDir, 'mongodb.conf'))) {
      fs.writeFileSync(path.join(snippetsDir, 'mongodb.conf'), '# Placeholder - MongoDB not installed\n', { mode: 0o644 });
    }

    try { execSync('nginx -t && systemctl reload nginx', { stdio: 'pipe', timeout: 15000 }); } catch (e) {
      console.error('[phpmyadmin] Nginx reload error:', e.message);
      try { execSync('systemctl restart nginx', { stdio: 'pipe', timeout: 15000 }); } catch { }
    }

    console.log('[phpmyadmin] Web server configuration complete (internal proxy only)');
    return true;
  } catch (e) {
    console.error('[phpmyadmin] Web server config error:', e.message);
    return false;
  }
}

function disablePhpMyAdminWebServer() {
  const { execSync } = require('child_process');
  try {
    const placeholderContent = '# phpMyAdmin disabled - placeholder\n';
    try {
      fs.mkdirSync('/etc/nginx/snippets', { recursive: true });
      fs.writeFileSync('/etc/nginx/snippets/phpmyadmin.conf', placeholderContent);
    } catch { }

    // Remove internal server config
    try {
      const internalConf = '/etc/nginx/conf.d/adpanel-phpmyadmin-internal.conf';
      if (fs.existsSync(internalConf)) fs.unlinkSync(internalConf);
    } catch { }

    // Remove old include lines from site configs
    removeDbSnippetIncludes('phpmyadmin.conf');

    try {
      execSync('nginx -t && systemctl reload nginx', { stdio: 'pipe', timeout: 15000 });
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
    try {
      fs.writeFileSync(configPath, configContent, { mode: 0o644 });
      console.log("[database] phpMyAdmin config written to", configPath);
      return true;
    } catch (err) {
      console.warn("[database] Could not write to", configPath, err.message);
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

async function installPostgreSQLAndPgAdmin(email, password, jobId, listenHost = '0.0.0.0', listenPort = '5432') {
  const runCmd = (cmd, timeoutMs = 120000) => new Promise((resolve) => {
    console.log("[pgadmin] Running install step...");
    const child = spawn("bash", ["-c", cmd], {
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
        sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
        wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add - 2>/dev/null
      fi
    `);

    if (jobId) updateInstallJob(jobId, { progress: 20, message: 'Adding pgAdmin4 repository...' });
    console.log("[pgadmin] Step 2: Adding pgAdmin4 repository...");
    await runCmd(`
      # Always download fresh GPG key and create sources list
      curl -fsS https://www.pgadmin.org/static/packages_pgadmin_org.pub | sudo gpg --batch --yes --dearmor -o /usr/share/keyrings/packages-pgadmin-org.gpg 2>/dev/null || true
      sudo sh -c 'echo "deb [signed-by=/usr/share/keyrings/packages-pgadmin-org.gpg] https://ftp.postgresql.org/pub/pgadmin/pgadmin4/apt/$(lsb_release -cs) pgadmin4 main" > /etc/apt/sources.list.d/pgadmin4.list'
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
# Only listen on port 5050 for pgAdmin4 (nginx handles port 80/443)
Listen 5050
`;
    try {
      fs.writeFileSync('/etc/apache2/ports.conf', apachePortsConf, { mode: 0o644 });
      console.log("[pgadmin] Apache ports.conf configured for port 5050 only");
    } catch (e) {
      console.warn("[pgadmin] Could not write ports.conf:", e.message);
      await runCmd("sed -i '/^Listen/d' /etc/apache2/ports.conf 2>/dev/null || true", 10000);
      await runCmd("echo 'Listen 5050' >> /etc/apache2/ports.conf", 10000);
    }

    await runCmd("a2enmod wsgi </dev/null 2>/dev/null || true", 15000);
    console.log("[pgadmin] Apache modules configured");

    const pgadminApacheConf = `
<VirtualHost *:5050>
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
    await runCmd(`find /etc/postgresql -name postgresql.conf -exec sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '${listenHost}'/" {} \\;`, 30000);
    await runCmd(`find /etc/postgresql -name postgresql.conf -exec sed -i "s/listen_addresses = 'localhost'/listen_addresses = '${listenHost}'/" {} \\;`, 30000);
    await runCmd(`find /etc/postgresql -name postgresql.conf -exec sed -i "s/#port = 5432/port = ${listenPort}/" {} \\;`, 30000);
    await runCmd(`find /etc/postgresql -name pg_hba.conf -exec sh -c 'grep -q "host all all 0.0.0.0/0" "\\$1" || echo "host all all 0.0.0.0/0 scram-sha-256" >> "\\$1"' _ {} \\;`, 30000);
    await runCmd(`find /etc/postgresql -name pg_hba.conf -exec sh -c 'grep -q "host all all ::0/0" "\\$1" || echo "host all all ::0/0 scram-sha-256" >> "\\$1"' _ {} \\;`, 30000);

    if (jobId) updateInstallJob(jobId, { progress: 68, message: 'Starting PostgreSQL service...' });
    await runCmd("systemctl enable postgresql 2>/dev/null; systemctl restart postgresql 2>/dev/null || systemctl reload postgresql 2>/dev/null || pg_ctlcluster --skip-systemctl-redirect $(pg_lsclusters -h | head -1 | awk '{print $1, $2}') restart 2>/dev/null || true", 60000);

    if (jobId) updateInstallJob(jobId, { progress: 72, message: 'Configuring firewall...' });
    await runCmd(`ufw allow ${listenPort}/tcp 2>/dev/null || true; ufw allow 5050/tcp 2>/dev/null || true`, 30000);
    await runCmd(`iptables -C INPUT -p tcp --dport ${listenPort} -j ACCEPT 2>/dev/null || iptables -A INPUT -p tcp --dport ${listenPort} -j ACCEPT 2>/dev/null || true`, 10000);
    await runCmd(`iptables -C INPUT -p tcp --dport 5050 -j ACCEPT 2>/dev/null || iptables -A INPUT -p tcp --dport 5050 -j ACCEPT 2>/dev/null || true`, 10000);

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
      const setupEnv = {
        ...process.env,
        PGADMIN_SETUP_EMAIL: email,
        PGADMIN_SETUP_PASSWORD: password
      };

      const runSetupSecure = (args = ['--yes']) => new Promise((resolve) => {
        console.log(`[pgadmin] Running setup script: ${setupPath} ${args.join(' ')}`);
        const child = spawn(setupPath, args, {
          shell: false,
          env: setupEnv,
          stdio: ['pipe', 'pipe', 'pipe']
        });
        let stdout = '', stderr = '';
        child.stdout.on('data', (d) => { stdout += d.toString(); });
        child.stderr.on('data', (d) => { stderr += d.toString(); });
        const timeout = setTimeout(() => {
          child.kill();
          resolve({ code: -1, stdout, stderr: 'Timeout' });
        }, 120000);
        child.on('close', (code) => {
          clearTimeout(timeout);
          resolve({ code, stdout, stderr });
        });
        child.on('error', (err) => {
          clearTimeout(timeout);
          resolve({ code: 1, stdout, stderr: err.message });
        });
      });

      const setupResult = await runSetupSecure(['--yes']);
      console.log("[pgadmin] Setup script result:", setupResult.code, (setupResult.stdout || '').substring(0, 300));

      if (setupResult.code !== 0) {
        console.log("[pgadmin] Retrying setup with stdin input...");
        const retryResult = await new Promise((resolve) => {
          const child = spawn(setupPath, [], {
            shell: false,
            env: { ...process.env },
            stdio: ['pipe', 'pipe', 'pipe']
          });
          let stdout = '', stderr = '';
          child.stdout.on('data', (d) => { stdout += d.toString(); });
          child.stderr.on('data', (d) => { stderr += d.toString(); });
          const timeout = setTimeout(() => {
            child.kill();
            resolve({ code: -1, stdout, stderr: 'Timeout' });
          }, 120000);
          child.on('close', (code) => {
            clearTimeout(timeout);
            resolve({ code, stdout, stderr });
          });
          child.on('error', (err) => {
            clearTimeout(timeout);
            resolve({ code: 1, stdout, stderr: err.message });
          });
          child.stdin.write(`${email}\n`);
          child.stdin.write(`${password}\n`);
          child.stdin.write(`${password}\n`);
          child.stdin.write('y\n');
          child.stdin.end();
        });
        console.log("[pgadmin] Retry result:", retryResult.code, (retryResult.stdout || '').substring(0, 200));
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
      console.log("[pgadmin] Enabled wsgi module");
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
    const child = spawn("sudo", ["-u", "postgres", "psql"], {
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

    if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(safeUser)) {
      throw new Error("Invalid username format for pg_hba.conf");
    }
    const hbaEntry = `host all ${safeUser} 0.0.0.0/0 scram-sha-256`;
    try {
      const hbaContent = fs.readFileSync('/etc/postgresql/16/main/pg_hba.conf', 'utf8');
      if (!hbaContent.includes(`host all ${safeUser}`)) {
        fs.appendFileSync('/etc/postgresql/16/main/pg_hba.conf', `\n${hbaEntry}\n`);
        console.log(`[pgadmin] Added pg_hba.conf entry for ${safeUser}`);
      }
    } catch (e) {
      try {
        const pgConfDirs = ['/etc/postgresql/14/main', '/etc/postgresql/15/main', '/etc/postgresql/16/main'];
        for (const dir of pgConfDirs) {
          const hbaPath = path.join(dir, 'pg_hba.conf');
          if (fs.existsSync(hbaPath)) {
            const content = fs.readFileSync(hbaPath, 'utf8');
            if (!content.includes(`host all ${safeUser}`)) {
              fs.appendFileSync(hbaPath, `\n${hbaEntry}\n`);
              console.log(`[pgadmin] Added pg_hba.conf entry to ${hbaPath}`);
            }
          }
        }
      } catch (e2) {
        console.warn(`[pgadmin] Could not update pg_hba.conf:`, e2.message);
      }
    }

    await new Promise((resolve) => {
      const child = spawn("sudo", ["systemctl", "reload", "postgresql"], { shell: false, stdio: 'pipe' });
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

async function installMongoDB(adminUser, adminPassword, jobId, listenHost = '0.0.0.0', listenPort = '27017') {
  const runCmd = (cmd, timeoutMs = 120000) => new Promise((resolve) => {
    console.log("[mongodb] Running install step...");
    const child = spawn("bash", ["-c", cmd], {
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

net:
  port: ${listenPort}
  bindIp: ${listenHost}

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

    await runCmd(`ufw allow ${listenPort}/tcp 2>/dev/null || true`, 10000);

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

    const child = spawn('bash', ['-c', script], {
      shell: false,
      stdio: ['pipe', 'pipe', 'pipe'],
      env: { ...process.env, DEBIAN_FRONTEND: 'noninteractive' }
    });
    let stderr = '', stdout = '';
    child.stdout.on('data', d => { stdout += d.toString(); });
    child.stderr.on('data', d => { stderr += d.toString(); });
    const timer = setTimeout(() => {
      child.kill('SIGKILL');
      spawn('bash', ['-c', 'killall -9 mysqld mariadbd 2>/dev/null; sleep 1; systemctl start mariadb 2>/dev/null || systemctl start mysql 2>/dev/null || systemctl start mysqld 2>/dev/null'], { stdio: 'ignore' });
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

    const child = spawn("sudo", ["-u", "postgres", "psql"], {
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

    const child = spawn("sudo", ["-u", "postgres", "psql"], {
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

    const child = spawn("sudo", ["-u", "postgres", "psql"], {
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

// ── Database Access Token API ────────────────────────────────────
app.post("/api/settings/database/access-token", async (req, res) => {
  if (!(await isAdmin(req))) return res.status(404).end();
  const { service } = req.body || {};
  const validServices = ["phpmyadmin", "pgadmin"];
  if (!validServices.includes(service)) {
    return res.status(400).json({ error: "Invalid service" });
  }
  const userIp = req.ip || req.connection?.remoteAddress || "";
  const username = req.session?.user || "unknown";
  const result = dbProxy.generateToken(service, userIp, username);
  if (!result) return res.status(500).json({ error: "Failed to generate token" });
  return res.json({
    url: `/db-access/${result.token}/`,
    token: result.token,
    expiresAt: result.expiresAt,
    ttl: dbProxy.TOKEN_TTL_MS
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

  const rawHost = dbHost || "0.0.0.0";
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
      const child = spawn("bash", ["-c", cmd], { shell: false });
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
      ufw delete allow 5050/tcp 2>/dev/null || true

      true
    `);
    console.log("[pgadmin] Firewall rules cleaned up");

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

  const rawHost = String(host || "0.0.0.0").trim();
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
      const child = spawn("bash", ["-c", cmd], {
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

      # Close firewall rules
      ufw delete allow 27017/tcp 2>/dev/null || true
      iptables -D INPUT -p tcp --dport 27017 -j ACCEPT 2>/dev/null || true

      true
    `);
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

  const { id, name, description, dockerImage, dockerTag, templateImage, defaultPort, ports, volumes, env, command, startupCommand } = req.body || {};
  const cleanId = String(id || "").trim().toLowerCase();
  const cleanName = String(name || "").trim();
  const cleanDesc = String(description || "").trim();
  const cleanImage = String(dockerImage || "").trim();
  const cleanTag = String(dockerTag || "latest").trim() || "latest";
  const cleanTemplateImage = String(templateImage || "").trim();
  const cleanCommand = String(command || "").trim();
  const cleanStartupCommand = String(startupCommand || "").trim();

  if (!cleanId || !/^[a-z0-9_-]{2,60}$/.test(cleanId)) return res.status(400).json({ error: "invalid id" });
  if (!cleanName) return res.status(400).json({ error: "missing name" });
  if (!cleanImage) return res.status(400).json({ error: "missing docker image" });

  const list = loadTemplatesFile();
  const exists = list.find(t => String(t?.id || "").toLowerCase() === cleanId);
  if (exists) return res.status(400).json({ error: "template id already exists" });

  let cleanPorts = [];
  if (Array.isArray(ports)) {
    cleanPorts = ports.map(p => parseInt(p, 10)).filter(p => p > 0 && p <= 65535);
  } else if (typeof ports === "string" && ports.trim()) {
    cleanPorts = ports.split(",").map(p => parseInt(p.trim(), 10)).filter(p => p > 0 && p <= 65535);
  }

  let cleanVolumes = ["{BOT_DIR}:/data"];
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

  const cleanDefaultPort = parseInt(defaultPort, 10) || (cleanPorts.length > 0 ? cleanPorts[0] : 8080);

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
      env: cleanEnv,
      command: cleanCommand,
      ...(cleanStartupCommand ? { startupCommand: cleanStartupCommand } : {}),
      restart: "unless-stopped",
      console: { type: "stdin" },
    },
  };

  list.push(tpl);
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
      user: safeUser,
      serverStartTime: SERVER_START,
      globalAlert: getActiveGlobalAlert(),
      quickActions: (function () {
        const all = loadQuickActions();
        return (safeUser && safeUser.admin) ? all.admin : all.user;
      })(),
      avatarUrl: safeUser?.avatar_url || 'https://icon-library.com/images/guest-account-icon/guest-account-icon-1.jpg',
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
  const defaultAvatar = "https://icon-library.com/images/guest-account-icon/guest-account-icon-1.jpg";

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
    avatarUrl: user.avatar_url || defaultAvatar,
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

  try {
    await db.query(
      `INSERT INTO users (email, password, secret, admin, recovery_codes) VALUES (?, ?, ?, 0, ?)`,
      [emailLower, passwordHash, secret.base32, recoveryCodesJson]
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
        result = {
          ok: true,
          status: nodeStatusLabel,
          nodeOnline: true,
          cpu: stats.cpu?.percent ?? cachedStatus?.cpu ?? null,
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
        };
      } else {
        result = {
          ok: true,
          status: nodeStatusLabel,
          nodeOnline: true,
          cpu: cachedStatus?.cpu ?? null,
          memory: cachedStatus?.memory ?? null,
          disk: cachedStatus?.disk ?? null,
          uptime: nodeStats.uptime ?? cachedStatus?.uptime ?? null
        };
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

      result = {
        ok: true,
        status: serverStatus,
        nodeOnline: nodeOnline,
        cpu: cachedStatus?.cpu ?? null,
        memory: cachedStatus?.memory ?? null,
        disk: cachedStatus?.disk ?? null,
        uptime: cachedStatus?.uptime ?? null
      };
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
    return res.json({
      ok: true,
      name,
      status: cached.status,
      cpu: cached.cpu,
      memory: cached.memory,
      disk: cached.disk,
      nodeOnline: cached.nodeOnline,
      updatedAt: cached.updatedAt,
      fresh: Date.now() - cached.updatedAt < STATUS_CACHE_TTL_MS
    });
  }

  return res.json({
    ok: true,
    name,
    status: 'unknown',
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

    if (ok1 && ok2) return res.json({ ok: true });
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
    const idx = await loadServersIndex();
    const names = (Array.isArray(idx) ? idx : []).map(e => e && e.name).filter(Boolean);

    const userEmail = req.session.user;
    const u = await findUserByEmail(userEmail);
    if (u && u.admin) return res.json({ names });

    const access = (await getAccessListForEmail(userEmail)) || [];
    const filtered = access.includes("all") ? names : names.filter(n => access.includes(n));
    return res.json({ names: filtered });
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

    if (!(await isAdmin(req)) && !(await userHasAccessToServer(email, serverName))) {
      return res.status(403).json({ error: "no access to server" });
    }

    const perms = await getEffectivePermsForUserOnServer(email, serverName);

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
    if (user && user.admin) {
      actionTokens.changeTemplate = issueActionToken(req, "POST /api/servers/:bot/template", { serverName }, { ttlSeconds: 120, oneTime: true });
      actionTokens.applyVersion = issueActionToken(req, "POST /api/servers/:bot/versions/apply", { serverName }, { ttlSeconds: 120, oneTime: true });
    }

    return res.json({
      isAdmin: !!(user && user.admin),
      perms,
      email,
      user: user ? { email: user.email, admin: !!user.admin } : null,
      agent_access: !!(user && (user.admin || user.agent_access)),
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
    const adminEmails = new Set(users.filter(u => u && u.admin).map(u => String(u.email).toLowerCase()));

    const accessRaw = await loadUserAccess();
    const accessMap = new Map();
    for (const a of (Array.isArray(accessRaw) ? accessRaw : [])) {
      const emailLower = String(a.email || "").toLowerCase();
      if (emailLower) {
        accessMap.set(emailLower, Array.isArray(a.servers) ? a.servers : []);
      }
    }

    const accounts = users
      .filter(u => u && u.email && !adminEmails.has(String(u.email).toLowerCase()))
      .map(u => {
        const emailLower = String(u.email).toLowerCase();
        return {
          email: u.email,
          servers: accessMap.get(emailLower) || [],
          agent_access: !!u.agent_access
        };
      });

    const bots = (await loadServersIndex() || []).map(e => e?.name).filter(Boolean);

    return res.json({ accounts, bots });
  } catch (e) {
    console.error("Failed to read accounts:", e);
    return res.status(500).json({ error: "failed to read accounts" });
  }
});

app.post("/api/settings/accounts/:email/add", async (req, res, next) => {
  try {
    if (!(await isAdmin(req))) return res.status(403).json({ error: "not authorized" });

    let email;
    try { email = decodeURIComponent(req.params.email || ""); } catch { email = req.params.email || ""; }
    email = String(email).trim();

    const server = String(req.body?.server || "").trim();
    if (!email || !server) return res.status(400).json({ error: "missing email or server" });

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

    let email;
    try { email = decodeURIComponent(req.params.email || ""); } catch { email = req.params.email || ""; }
    email = String(email).trim();

    const server = String(req.body?.server || "").trim();
    if (!email || !server) return res.status(400).json({ error: "missing email or server" });

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

    let email;
    try { email = decodeURIComponent(req.params.email || ""); } catch { email = req.params.email || ""; }
    email = String(email).trim().toLowerCase();

    if (!email) return res.status(400).json({ error: "missing email" });

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

    const list = await loadServersIndex();
    const entry = list.find(e => e && e.name === server);
    if (!entry) return res.status(404).json({ error: "server not found" });

    const acl = entry.acl && typeof entry.acl === "object" ? entry.acl : {};
    const rec = acl[email] || null;

    const permissions = {};
    for (const k of ALLOWED_PERM_KEYS) {
      permissions[k] = !!(rec && rec[k]);
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

    let email;
    try { email = decodeURIComponent(req.params.email || ""); } catch { email = req.params.email || ""; }
    email = String(email).trim().toLowerCase();

    const server = String(req.body?.server || "").trim();
    const permsIn = (req.body && typeof req.body.permissions === "object") ? req.body.permissions : null;

    if (!email) return res.status(400).json({ error: "missing email" });
    if (!server) return res.status(400).json({ error: "missing server" });

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

    if (targetUser.admin) {
      return res.status(403).json({ error: "cannot delete admin users" });
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

    if (targetUser.admin) {
      return res.status(403).json({ error: "cannot change admin passwords" });
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
    const avatarUrl = userObj?.avatar_url || 'https://icon-library.com/images/guest-account-icon/guest-account-icon-1.jpg';

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

    if (!entry) return res.status(404).json({ error: "server-not-found" });

    const serverName = entry.name || safeBotName;
    if (!(await isAdmin(req)) && !(await userHasAccessToServer(req.session.user, serverName))) {
      return res.status(403).json({ error: "no-access-to-server" });
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

    const { entry, template } = await resolveTemplateForBot(safeBotName);
    const preferredTemplate = normalizeTemplateId(req.query.template || req.query.tpl || "");
    if (!entry) return res.status(404).json({ error: "server-not-found" });

    const serverName = entry.name || safeBotName;
    if (!(await isAdmin(req)) && !(await userHasAccessToServer(req.session.user, serverName))) {
      return res.status(403).json({ error: "no-access-to-server" });
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

  if (!(await isAdmin(req)) && !(await userHasAccessToServer(req.session.user, bot))) {
    return res.status(403).json({ ok: false, error: "no-access-to-server" });
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
    if (reqResources.ramMb != null) resources.ramMb = parseInt(reqResources.ramMb, 10) || null;
    if (reqResources.cpuCores != null) resources.cpuCores = parseFloat(reqResources.cpuCores) || null;
    if (reqResources.storageMb != null) resources.storageMb = parseInt(reqResources.storageMb, 10) || null;
    else if (reqResources.storageGb != null) resources.storageMb = (parseInt(reqResources.storageGb, 10) || 0) * 1024 || null;
    if (reqResources.swapMb != null) resources.swapMb = parseInt(reqResources.swapMb, 10);
    if (reqResources.backupsMax != null) resources.backupsMax = parseInt(reqResources.backupsMax, 10) || 0;
    if (reqResources.maxSchedules != null) resources.maxSchedules = parseInt(reqResources.maxSchedules, 10) || 0;

    let hostPortRaw = (req.body && (req.body.hostPort ?? req.body.port)) ?? null;
    if (hostPortRaw == null && template.defaultPort) {
      hostPortRaw = template.defaultPort;
    }

    const resolvedPort = hostPortRaw != null ? Number(hostPortRaw) : (template.defaultPort || 0);
    if (resolvedPort > 0 && !isPortInNodeAllocation(node, resolvedPort)) {
      const alloc = node.ports || {};
      let allocDesc = "none";
      if (alloc.mode === "range") allocDesc = `range ${alloc.start}–${alloc.start + alloc.count - 1}`;
      else if (alloc.mode === "list" && Array.isArray(alloc.ports)) allocDesc = `list [${alloc.ports.slice(0, 10).join(", ")}${alloc.ports.length > 10 ? "…" : ""}]`;
      return res.status(400).json({ error: `Port ${resolvedPort} is not in this node's allocated ports (${allocDesc}). Choose a port within the node's allocation.` });
    }

    const tpl = normalizeTemplateId(templateId);
    const startFile = tpl === "minecraft"
      ? "server.jar"
      : (tpl === "nodejs" || tpl === "discord-bot"
        ? "index.js"
        : (tpl === "python" ? "main.py" : null));
    const finalStartupCommand = resolveStartupCommandForCreate({
      requestedStartupCommand: req.body?.startupCommand,
      template,
      templateId,
      name,
      hostPort: hostPortRaw,
      resources,
      startFile,
    }) || null;

    const resolvedHostPort = hostPortRaw != null ? Number(hostPortRaw) : (template.defaultPort || 0);
    if (finalStartupCommand && resolvedHostPort > 0) {
      const portError = validateDockerCommandPorts(finalStartupCommand, resolvedHostPort);
      if (portError) {
        return res.status(400).json({ error: portError });
      }
    }

    let sanitizedDocker = sanitizeDockerTemplatePayload(template.docker);
    if (sanitizedDocker && Array.isArray(sanitizedDocker.ports) && sanitizedDocker.ports.length > 1) {
      sanitizedDocker.ports = [sanitizedDocker.ports[0]];
    }

    const payload = {
      name,
      templateId,
      mcFork,
      mcVersion,
      hostPort: hostPortRaw,
      docker: sanitizedDocker,
      startupCommand: finalStartupCommand,
      autoStart: true,
      importUrl: importUrl,
      resources: Object.keys(resources).length > 0 ? resources : null
    };
    const createResult = await createOnRemoteNode(node, payload);
    const resolvedStartupCommand = extractStartupCommandFromNodeCreateResult(createResult) || finalStartupCommand;

    try {
      const me = req.session.user;
      const u = await findUserByEmail(me);
      if (!(u && u.admin)) await addAccessForEmail(me, name);
    } catch { }

    let savedPort = null;
    if (hostPortRaw != null) {
      savedPort = clampPort(hostPortRaw);
    } else if (template.defaultPort) {
      savedPort = clampPort(template.defaultPort);
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
    if (resolvedStartupCommand) serverEntry.startupCommand = resolvedStartupCommand;
    if (sanitizedDocker) {
      serverEntry.runtime = {
        image: sanitizedDocker.image,
        tag: sanitizedDocker.tag,
        command: sanitizedDocker.command,
        volumes: sanitizedDocker.volumes,
        env: sanitizedDocker.env,
        ports: sanitizedDocker.ports,
      };
    }
    if (resolvedStartupCommand) {
      if (!serverEntry.runtime) serverEntry.runtime = {};
      serverEntry.runtime.startupCommand = resolvedStartupCommand;
    }

    await upsertServerIndexEntry(serverEntry);

    return res.json({ ok: true, name, displayName });
  } catch (e) {
    console.error("[/api/servers/create] failed:", e);
    return res.status(500).json({ error: "create failed" });
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
  let nodeStartupCommand = null;
  let templateStartupCommand = null;

  if (isRemoteEntry(entry)) {
    const node = await findNodeByIdOrName(entry.nodeId);
    if (!node) return res.status(400).json({ error: "node not found for server" });

    const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
    if (!baseUrl) return res.status(400).json({ error: "invalid node address" });

    const headers = nodeAuthHeadersFor(node, true);

    let hostPort = entry.port || template.defaultPort || 8080;
    templateStartupCommand = resolveStartupCommandForCreate({
      requestedStartupCommand: null,
      template,
      templateId,
      name,
      hostPort,
      resources: entry.resources && typeof entry.resources === "object" ? entry.resources : null,
      startFile: defaultStartFileForTemplate(templateId),
    }) || null;

    const reinstallPayload = {
      templateId,
      docker: template.docker || null,
      startupCommand: templateStartupCommand,
      hostPort,
    };

    if (!signPanelAdminReinstallHeaders(headers, node, name, templateId)) {
      return res.status(500).json({ error: "failed to sign reinstall request" });
    }

    console.log(`[template-change] Reinstalling server ${name} on node ${entry.nodeId} with template ${templateId}`);

    const r = await httpRequestJson(
      `${baseUrl}/v1/servers/${encodeURIComponent(name)}/reinstall`,
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

    if (r.json.meta && r.json.meta.startupCommand) {
      nodeStartupCommand = r.json.meta.startupCommand;
    }
  }

  const updatedEntry = Object.assign({}, entry, {
    template: String(template.id || templateId),
  });
  if (template.docker) {
    updatedEntry.docker = template.docker;
    updatedEntry.runtime = {
      image: template.docker.image,
      tag: template.docker.tag,
      command: template.docker.command,
      volumes: template.docker.volumes,
      env: template.docker.env,
      ports: template.docker.ports,
    };
  }
  const resolvedTemplateStartupCommand = nodeStartupCommand || templateStartupCommand || null;
  if (resolvedTemplateStartupCommand) {
    if (!updatedEntry.runtime) updatedEntry.runtime = {};
    updatedEntry.runtime.startupCommand = resolvedTemplateStartupCommand;
    updatedEntry.startupCommand = resolvedTemplateStartupCommand;
  }
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

    if (!isRemoteEntry(entry)) return res.status(400).json({ error: "server is not on a remote node" });

    const node = await findNodeByIdOrName(entry.nodeId);
    if (!node) return res.status(400).json({ error: "node not found for server" });

    const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
    if (!baseUrl) return res.status(400).json({ error: "invalid node address" });

    const headers = nodeAuthHeadersFor(node, true);

    const templateId = normalizeTemplateId(entry.template) || "custom";
    const template = findTemplateById(templateId);

    let hostPort = entry.port || (template && template.defaultPort) || 8080;

    const activeStartupCommand = resolveServerStartupCommand(entry, template);
    const reinstallPayload = {
      templateId,
      docker: entry.docker || (template && template.docker) || null,
      startupCommand: activeStartupCommand,
      hostPort,
    };

    if (!signPanelAdminReinstallHeaders(headers, node, name, templateId)) {
      return res.status(500).json({ error: "failed to sign reinstall request" });
    }

    console.log(`[reinstall] User ${email} (admin=${isAdminUser}) reinstalling server ${name} with template ${templateId}`);

    const r = await httpRequestJson(
      `${baseUrl}/v1/servers/${encodeURIComponent(name)}/reinstall`,
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

    if (r.json.meta) {
      const list = await loadServersIndex();
      const idx = list.findIndex(e => e && e.name === name);
      if (idx >= 0) {
        const updatedEntry = Object.assign({}, list[idx]);
        if (r.json.meta.startupCommand) {
          if (!updatedEntry.runtime) updatedEntry.runtime = {};
          updatedEntry.runtime.startupCommand = r.json.meta.startupCommand;
          updatedEntry.startupCommand = r.json.meta.startupCommand;
        }
        list[idx] = updatedEntry;
        await saveServersIndex(list);
      }
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

  const { resources, startupCommand, mainPort } = req.body || {};

  if (startupCommand != null && String(startupCommand).trim()) {
    const allocatedPort = serverEntry.port ? Number(serverEntry.port) : 0;
    if (allocatedPort > 0) {
      const additionalPorts = Array.isArray(resources?.ports) ? resources.ports
        : (serverEntry.resources && Array.isArray(serverEntry.resources.ports) ? serverEntry.resources.ports : []);
      const portError = validateDockerCommandPorts(String(startupCommand).trim(), allocatedPort, additionalPorts);
      if (portError) {
        return res.status(400).json({ error: 'Port config through docker edit command is not allowed. Add or remove a port for this server by the category "Port Management"' });
      }
    }
  }

  const payload = {};
  if (resources) {
    payload.resources = {};
    if (resources.ramMb != null) {
      const n = parseInt(resources.ramMb, 10);
      if (!Number.isNaN(n)) payload.resources.ramMb = n;
    }
    if (resources.cpuCores != null) {
      const n = parseFloat(resources.cpuCores);
      if (!Number.isNaN(n)) payload.resources.cpuCores = n;
    }
    if (resources.storageMb != null) {
      const n = parseInt(resources.storageMb, 10);
      if (!Number.isNaN(n)) payload.resources.storageMb = n;
    } else if (resources.storageGb != null) {
      const n = parseInt(resources.storageGb, 10);
      if (!Number.isNaN(n)) payload.resources.storageMb = n * 1024;
    }
    if (resources.swapMb != null) payload.resources.swapMb = parseInt(resources.swapMb, 10);
    if (resources.backupsMax != null) payload.resources.backupsMax = parseInt(resources.backupsMax, 10) || 0;
    if (resources.maxSchedules != null) payload.resources.maxSchedules = parseInt(resources.maxSchedules, 10) || 0;
    if (Array.isArray(resources.ports)) payload.resources.ports = resources.ports;
  }
  if (startupCommand != null) {
    payload.startupCommand = String(startupCommand).trim();
  }

  let newMainPort = null;
  if (mainPort != null) {
    const mp = parseInt(mainPort, 10);
    if (!Number.isNaN(mp) && mp >= 1 && mp <= 65535) {
      newMainPort = mp;
      payload.mainPort = mp;
    }
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
      const msg = (r.json && r.json.error) ? r.json.error : `failed to update resources (${r.status})`;
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
        if (Array.isArray(resources.ports)) list[idx].resources.ports = resources.ports;
      }
      if (newMainPort != null) {
        list[idx].port = newMainPort;
      }
      if (startupCommand != null && String(startupCommand).trim()) {
        const cmd = String(startupCommand).trim();
        if (!list[idx].runtime) list[idx].runtime = {};
        list[idx].runtime.startupCommand = cmd;
        list[idx].startupCommand = cmd;
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
    const templateId = normalizeTemplateId(serverEntry.template) || "custom";
    const template = findTemplateById(templateId);
    const fallbackStartupCommand = resolveServerStartupCommand(serverEntry, template);
    const nodeStartupCommand = typeof result.startupCommand === "string" ? result.startupCommand.trim() : "";
    const effectiveStartupCommand = nodeStartupCommand || fallbackStartupCommand || "";

    if (effectiveStartupCommand) {
      result.startupCommand = effectiveStartupCommand;

      const persistedStartupCommand =
        (serverEntry.runtime && typeof serverEntry.runtime.startupCommand === "string" && serverEntry.runtime.startupCommand.trim()) ||
        "";
      if (!persistedStartupCommand) {
        try {
          const updatedEntry = Object.assign({}, serverEntry);
          updatedEntry.runtime = Object.assign({}, updatedEntry.runtime || {}, {
            startupCommand: effectiveStartupCommand,
          });
          updatedEntry.startupCommand = effectiveStartupCommand;
          await upsertServerIndexEntry(updatedEntry);
        } catch (persistError) {
          console.warn("[/api/settings/servers/:name/resources GET] failed to persist derived startup command:", persistError);
        }
      }
    }

    if (!result.template && serverEntry.template) {
      result.template = serverEntry.template;
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
    const pub = parseInt(rule.publicPort, 10);
    const internal = parseInt(rule.internalPort, 10);
    if (!Number.isFinite(pub) || pub < 1 || pub > 65535) {
      return res.status(400).json({ error: `Invalid public port: ${rule.publicPort}` });
    }
    if (!Number.isFinite(internal) || internal < 1 || internal > 65535) {
      return res.status(400).json({ error: `Invalid internal port: ${rule.internalPort}` });
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
        const startupCmd =
          (sourceMeta && sourceMeta.startupCommand) ? sourceMeta.startupCommand
            : (sourceMeta && sourceMeta.runtime && sourceMeta.runtime.startupCommand) ? sourceMeta.runtime.startupCommand
              : (serverEntry.docker && serverEntry.docker.startupCommand) ? serverEntry.docker.startupCommand
                : (serverEntry.runtime && serverEntry.runtime.startupCommand) ? serverEntry.runtime.startupCommand
                  : resolveServerStartupCommand(serverEntry);
        if (startupCmd) {
          patchPayload.startupCommand = startupCmd;
        }

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

const httpServer = httpMod.createServer(app);
httpServer.requestTimeout = 300000;
httpServer.headersTimeout = 60000;
httpServer.keepAliveTimeout = 5000;
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
        i = skipAnsiCsi(str, i + 1);
        continue;
      }
      const next = str.charCodeAt(i + 1);
      if (next === 0x5b) {
        i = skipAnsiCsi(str, i + 2);
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
      i += 1;
      continue;
    }
    out.push(str[i]);
    i += 1;
  }
  return out.join("");
}

function skipAnsiCsi(str, idx) {
  let i = idx;
  while (i < str.length) {
    const c = str.charCodeAt(i);
    if (c >= 0x40 && c <= 0x7e) return i + 1;
    i += 1;
  }
  return i;
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

function _emitLine(room, text) {
  const cleaned = cleanLog(room, text);
  if (!cleaned) return;
  io.to(room).emit("output", escapeHtml(cleaned));
}

function emitOutput(socket, room, text) {
  const cleaned = cleanLog(room, text);
  if (!cleaned) return;
  const safe = escapeHtml(cleaned);

  try { socket?.emit?.("output", safe); } catch { }

  try { io.to(room).emit("output", safe); } catch { }
}

function panel(socket, room, msg) {
  emitOutput(socket, room, `[ADPanel] ${msg}`);
}

function emitPanel(room, message) {
  _emitLine(room, `[ADPanel] ${message}`);
}

const remoteLogClients = Object.create(null);
function tailLogsRemote(name, baseUrl, headers) {
  if (remoteLogClients[name]) return;

  const url = `${baseUrl}/v1/servers/${encodeURIComponent(name)}/logs`;
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
    return {
      server: serverName,
      ok: true,
      status: statusLabel,
      nodeOnline: true,
      cpu: stats.cpu?.percent ?? null,
      cpuLimit: stats.cpu?.limit ?? null,
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
      uptime: nodeStats.uptime ?? null,
      ts: Date.now()
    };
  }
  if (nodeStats) {
    const statusLabel = normalizeStatusLabel(nodeStats.status || nodeStats.state) || 'unknown';
    return {
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
    };
  }
  if (cachedStatus) {
    const payload = {
      server: serverName,
      ok: true,
      status: normalizeStatusLabel(cachedStatus.status) || 'unknown',
      cpu: cachedStatus.cpu ?? null,
      cpuLimit: cachedStatus.cpuLimit ?? null,
      memory: cachedStatus.memory ?? null,
      disk: cachedStatus.disk ?? null,
      uptime: cachedStatus.uptime ?? null,
      ts: Date.now()
    };
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
const CONSOLE_HISTORY_MAX_SERVERS = 2000;
function rememberConsoleCommand(bot, line) {
  if (!bot || !line) return;
  const name = String(bot);
  if (!consoleHistory.has(name) && consoleHistory.size >= CONSOLE_HISTORY_MAX_SERVERS) {
    const oldest = consoleHistory.keys().next().value;
    if (oldest !== undefined) consoleHistory.delete(oldest);
  }
  const arr = consoleHistory.get(name) || [];
  arr.push({ line: String(line), ts: Date.now() });
  if (arr.length > 100) arr.shift();
  consoleHistory.set(name, arr);
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
    if (!room) return;
    io.to(room).emit("output", msg);
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

    try { await socket.join(name); } catch { }
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
    tailLogsRemote(name, baseUrl, headers);
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
    const { bot, cmd, port } = data || {};
    const botName = String(bot || "").trim();
    if (!botName) return;

    if (!canAccessBot(botName)) return deny(botName, "[ADPanel] You don't have access to this server.");
    try { socket.join(botName); } catch { }
    const entry = await findServer(botName);
    if (!entry || !isRemoteEntry(entry)) return _emitLine(botName, "[ADPanel] This server is not attached to a node.");

    const node = await findNodeByIdOrName(entry.nodeId);
    if (!node) return _emitLine(botName, "[ADPanel] Node not found.");

    const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
    if (!baseUrl) return _emitLine(botName, "[ADPanel] Invalid node address.");

    const headers = nodeAuthHeadersFor(node, true);

    startRoomStatusWatcher(botName, baseUrl, headers);
    tailLogsRemote(botName, baseUrl, headers);

    if ((cmd === "stop" || cmd === "restart" || cmd === "kill") && !(await hasPerm(botName, "server_stop"))) return deny(botName);
    if (cmd === "run" && !(await hasPerm(botName, "server_start"))) return deny(botName);

    try {
      if (cmd === "run") {
        panel(socket, botName, "Server starting");

        const defaultPort = normalizeTemplateId(entry.template) === "minecraft" ? 25565 : 3001;
        const chosenPort = entry.port ?? port ?? defaultPort;
        const hostPort = normalizeTemplateId(entry.template) === "minecraft" ? clampPort(chosenPort) : clampAppPort(chosenPort, defaultPort);

        const r = await httpRequestJson(`${baseUrl}/v1/servers/${encodeURIComponent(botName)}/start`, "POST", headers, { hostPort }, 1_000);
        if (r.status !== 200 || !(r.json && r.json.ok)) {
          const msg = (r.json && (r.json.error || r.json.detail)) || `node status ${r.status}`;
          emitPanel(botName, `Start failed: ${msg}`);
          return;
        }

        try {
          const cur = await findServer(botName);
          if (cur) await upsertServerIndexEntry({ ...cur, status: "online" });
        } catch { }

        panel(socket, botName, "Server started");
        tailLogsRemote(botName, baseUrl, headers);
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
        emitPanel(botName, "Server restarting");

        const r = await httpRequestJson(`${baseUrl}/v1/servers/${encodeURIComponent(botName)}/restart`, "POST", headers, null, 20_000);
        if (r.status !== 200 || !(r.json && r.json.ok)) throw new Error((r.json && (r.json.error || r.json.detail)) || `node status ${r.status}`);

        emitPanel(botName, "Server restarted");
        tailLogsRemote(botName, baseUrl, headers);
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

  socket.on("command", async ({ bot, command }) => {
    const botName = String(bot || "").trim();
    const trimmed = String(command || "").trim();
    if (!botName || !trimmed) return;

    if (!canAccessBot(botName) || !(await hasPerm(botName, "console_write"))) return deny(botName);

    rememberConsoleCommand(botName, trimmed);

    const entry = await findServer(botName);
    if (!entry || !isRemoteEntry(entry)) return _emitLine(botName, "[ADPanel] This server is not attached to a node.");

    const node = await findNodeByIdOrName(entry.nodeId);
    if (!node) return _emitLine(botName, "[ADPanel] Node not found.");

    const baseUrl = buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
    if (!baseUrl) return _emitLine(botName, "[ADPanel] Invalid node address.");

    const headers = nodeAuthHeadersFor(node, true);

    try {
      const r = await httpRequestJson(`${baseUrl}/v1/servers/${encodeURIComponent(botName)}/command`, "POST", headers, { command: trimmed }, 20_000);
      if (r.status !== 200 || !(r.json && r.json.ok)) {
        const msg = (r.json && (r.json.error || r.json.detail)) || `node status ${r.status}`;
        _emitLine(botName, "[ADPanel] command failed: " + msg);
        return;
      }
      _emitLine(botName, "[ADPanel] command sent");
    } catch (e) {
      _emitLine(botName, "[ADPanel] command error: " + (e?.message || String(e)));
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
