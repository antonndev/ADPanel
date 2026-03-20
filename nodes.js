const express = require("express");
const fs = require("fs");
const fsp = require("fs/promises");
const os = require("os");
const path = require("path");
const crypto = require("crypto");
const net = require("net");
const http = require("http");
const https = require("https");
let _multer; function getLazyMulter() { if (!_multer) _multer = require("multer"); return _multer; }
const db = require("./db");
const NODE_UPLOAD_TMP_DIR = process.env.NODE_UPLOAD_TMP_DIR || path.join(os.tmpdir(), "adpanel-node-uploads");
try {
  fs.mkdirSync(NODE_UPLOAD_TMP_DIR, { recursive: true, mode: 0o700 });
} catch { }

const ACTIVITY_LOGS_DIR = path.join(__dirname, ".activity-logs");
const ACTIVITY_LOG_BATCH_INTERVAL = parseInt(process.env.ACTIVITY_LOG_BATCH_INTERVAL || "", 10) || 1000;
const ACTIVITY_LOG_MAX_SIZE = parseInt(process.env.ACTIVITY_LOG_MAX_SIZE || "", 10) || 5 * 1024 * 1024;
const ACTIVITY_LOG_MAX_FILES = parseInt(process.env.ACTIVITY_LOG_MAX_FILES || "", 10) || 10;
const ACTIVITY_LOG_MAX_BUFFER_SIZE = 100;
const ACTIVITY_LOG_MAX_SERVERS = parseInt(process.env.ACTIVITY_LOG_MAX_SERVERS || "", 15) || 5000;

try { fs.mkdirSync(ACTIVITY_LOGS_DIR, { recursive: true }); } catch { }

const activityLogBuffers = new Map();
const activityLogWriteTimers = new Map();

function getActivityLogPath(serverName) {
  const safeName = String(serverName).replace(/[^a-zA-Z0-9_-]/g, "_");
  return path.join(ACTIVITY_LOGS_DIR, `${safeName}.log`);
}

async function rotateActivityLogIfNeeded(serverName) {
  const logPath = getActivityLogPath(serverName);
  try {
    const stats = await fsp.stat(logPath).catch(() => null);
    if (!stats || stats.size < ACTIVITY_LOG_MAX_SIZE) return;

    const safeName = String(serverName).replace(/[^a-zA-Z0-9_-]/g, "_");
    const rotatedPath = logPath.replace(/\.log$/, `.${Date.now()}.log`);
    await fsp.rename(logPath, rotatedPath);

    setImmediate(async () => {
      try {
        const files = await fsp.readdir(ACTIVITY_LOGS_DIR);
        const serverFiles = files
          .filter(f => f.startsWith(safeName + ".") && f.endsWith(".log") && f !== `${safeName}.log`)
          .sort()
          .reverse();
        for (let i = ACTIVITY_LOG_MAX_FILES; i < serverFiles.length; i++) {
          await fsp.unlink(path.join(ACTIVITY_LOGS_DIR, serverFiles[i])).catch(() => { });
        }
      } catch { }
    });
  } catch { }
}

async function flushActivityLogs(serverName) {
  const buffer = activityLogBuffers.get(serverName);
  if (!buffer || buffer.length === 0) return;

  activityLogBuffers.set(serverName, []);
  activityLogWriteTimers.delete(serverName);

  const logPath = getActivityLogPath(serverName);
  const lines = buffer.map(entry => JSON.stringify(entry)).join("\n") + "\n";

  try {
    await rotateActivityLogIfNeeded(serverName);
    await fsp.appendFile(logPath, lines, "utf8");
  } catch (err) {
    console.error(`[activity-log] Write error for ${serverName}:`, err?.message || err);
  }
}

let _webhookDispatcher = null;
function setWebhookDispatcher(fn) { _webhookDispatcher = fn; }

function recordActivity(serverName, action, details, userEmail, userIp) {
  if (!serverName) return;

  const entry = {
    ts: Date.now(),
    action: String(action || "unknown"),
    details: details || null,
    user: String(userEmail || "unknown"),
    ip: String(userIp || "unknown"),
  };

  if (!activityLogBuffers.has(serverName)) {
    if (activityLogBuffers.size >= ACTIVITY_LOG_MAX_SERVERS) {
      const oldest = activityLogBuffers.keys().next().value;
      if (oldest) {
        flushActivityLogs(oldest);
        activityLogBuffers.delete(oldest);
        if (activityLogWriteTimers.has(oldest)) {
          clearTimeout(activityLogWriteTimers.get(oldest));
          activityLogWriteTimers.delete(oldest);
        }
      }
    }
    activityLogBuffers.set(serverName, []);
  }
  const buffer = activityLogBuffers.get(serverName);
  buffer.push(entry);

  if (_webhookDispatcher) {
    try { _webhookDispatcher(entry.action, entry.details, entry.user, serverName); } catch (e) {}
  }

  if (buffer.length >= ACTIVITY_LOG_MAX_BUFFER_SIZE) {
    if (activityLogWriteTimers.has(serverName)) {
      clearTimeout(activityLogWriteTimers.get(serverName));
      activityLogWriteTimers.delete(serverName);
    }
    setImmediate(() => flushActivityLogs(serverName));
    return;
  }

  if (!activityLogWriteTimers.has(serverName)) {
    const timerId = setTimeout(() => flushActivityLogs(serverName), ACTIVITY_LOG_BATCH_INTERVAL);
    activityLogWriteTimers.set(serverName, timerId);
  }
}

function getRequestIp(req) {
  const xff = req.headers["x-forwarded-for"];
  if (xff) {
    const first = String(xff).split(",")[0].trim();
    if (first) return first;
  }
  return req.ip || req.connection?.remoteAddress || "unknown";
}

let _getEffectivePermsForUserOnServer = null;
let _isAdmin = null;
let _userHasAccessToServer = null;
function setPermissionCheckers(getPerms, isAdminFn, hasAccessFn) {
  _getEffectivePermsForUserOnServer = getPerms;
  _isAdmin = isAdminFn;
  _userHasAccessToServer = hasAccessFn || null;
}

async function checkUserPerm(req, serverName, permKey) {
  if (!_getEffectivePermsForUserOnServer || !_isAdmin) {
    console.error("[SECURITY] Permission checkers not configured - denying access");
    return false;
  }
  const userEmail = req.session?.user;
  if (!userEmail) return false;
  if (await _isAdmin(req)) return true;
  const perms = await _getEffectivePermsForUserOnServer(userEmail, serverName);
  return !!(perms && perms[permKey]);
}

async function checkServerAccess(req, serverName) {
  const userEmail = req.session?.user;
  if (!userEmail) return { ok: false, status: 401, error: "not authenticated" };
  if (_isAdmin && await _isAdmin(req)) return { ok: true };
  if (_userHasAccessToServer) {
    const hasAccess = await _userHasAccessToServer(userEmail, serverName);
    if (!hasAccess) return { ok: false, status: 403, error: "no access to server" };
  } else {
    console.error("[SECURITY] userHasAccessToServer not configured - denying non-admin access");
    return { ok: false, status: 403, error: "access checker not configured" };
  }
  return { ok: true };
}
const PANEL_MAX_UPLOAD_BYTES = 100 * 1024 * 1024 * 1024;
let _nodeUpload;
function getNodeUpload() {
  if (!_nodeUpload) {
    const multer = getLazyMulter();
    _nodeUpload = multer({
      storage: multer.diskStorage({
        destination: (_req, _file, cb) => cb(null, NODE_UPLOAD_TMP_DIR),
        filename: (_req, _file, cb) => cb(null, crypto.randomBytes(16).toString("hex")),
      }),
      limits: { fileSize: PANEL_MAX_UPLOAD_BYTES, files: 1 },
    });
  }
  return _nodeUpload;
}
const PORT = process.env.NODES_PORT ? Number(process.env.NODES_PORT) : 3550;
const HEARTBEAT_TTL_MS = 120_000;
const UPLOAD_TIMEOUT_MS = 3600_000;
const NODE_BOOTSTRAP_TOKEN = (process.env.NODE_BOOTSTRAP_TOKEN || "").trim();
const PANEL_PUBLIC_URL = (process.env.PANEL_PUBLIC_URL || "").trim();

(async () => {
  try {
    await db.query("ALTER TABLE nodes ADD COLUMN max_upload_mb INT NOT NULL DEFAULT 10240");
    console.log("[nodes] Migration: added max_upload_mb column to nodes table");
  } catch (e) {
    if (e && e.code !== "ER_DUP_FIELDNAME" && !String(e.message || "").includes("Duplicate column")) {
      console.warn("[nodes] Migration warning (max_upload_mb):", e.message);
    }
  }
})();

(async () => {
  try {
    await db.query("ALTER TABLE nodes ADD COLUMN sftp_port INT NOT NULL DEFAULT 2022 AFTER api_port");
    console.log("[nodes] Migration: added sftp_port column to nodes table");
  } catch (e) {
    if (e && e.code !== "ER_DUP_FIELDNAME" && !String(e.message || "").includes("Duplicate column")) {
      console.warn("[nodes] Migration warning (sftp_port):", e.message);
    }
  }
})();

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

function parseDbJson(value, fallback) {
  if (value === undefined || value === null || value === "") return fallback;
  if (typeof value === "object") return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function toDbJson(value, fallback) {
  if (value === undefined || value === null) return fallback;
  return JSON.stringify(value);
}
function uid() { return crypto.randomUUID(); }

function randTokenId() {
  return "tok_" + crypto.randomBytes(8).toString("hex");
}

function randSecret() {
  const ts = Buffer.alloc(8);
  const now = BigInt(Date.now());
  ts.writeBigUInt64BE(now);
  const rand = crypto.randomBytes(32);
  const combined = Buffer.concat([ts, rand]);
  return combined
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}
function sanitizeName(raw) {
  let s = String(raw || "").trim();
  if (!s) return "";
  s = s.replace(/[^\w\-. ]+/g, " ").replace(/\s+/g, " ").trim();
  s = s.replace(/\s/g, "-");
  if (s.length > 100) s = s.slice(0, 100);
  return s;
}
function isInt(n) { return Number.isInteger(n); }
function toInt(n, def = 0) { const x = Number(n); return Number.isFinite(x) ? Math.round(x) : def; }
function clampPort(p) {
  const n = toInt(p, 8080);
  if (n < 1 || n > 65535) return 8080;
  return n;
}
function normalizePorts(input) {
  if (!input) return { mode: "range", start: 25565, count: 10 };
  if (Array.isArray(input)) {
    const ports = Array.from(new Set(input.map(p => toInt(p)).filter(p => p >= 1 && p <= 65535)));
    return { mode: "list", ports };
  }
  if (typeof input === "object") {
    if (input.mode === "range") {
      let start = toInt(input.start, 25565);
      let count = toInt(input.count, 10);
      if (start < 1 || start > 65535) start = 25565;
      if (count < 1) count = 1;
      if (start + count - 1 > 65535) count = 65535 - start + 1;
      return { mode: "range", start, count };
    }
    if (input.mode === "list") {
      const ports = Array.from(new Set((input.ports || [])
        .map(p => toInt(p))
        .filter(p => p >= 1 && p <= 65535)));
      return { mode: "list", ports };
    }
  }
  return { mode: "range", start: 25565, count: 10 };
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

function hardenNode(n) {
  const clone = Object.assign({
    id: uid(),
    uuid: "",
    name: "node",
    address: "",
    ram_mb: 0,
    disk_gb: 0,
    ports: { mode: "range", start: 25565, count: 10 },
    token_id: randTokenId(),
    token: randSecret(),
    createdAt: Date.now(),
    api_port: 8080,
    sftp_port: 2022,
    port_ok: null,
    last_seen: null,
    last_check: null,
    online: null,
    buildConfig: {},
    max_upload_mb: 10240
  }, n || {});
  if (!clone.id) clone.id = uid();
  if (!clone.uuid) clone.uuid = clone.id;
  if (!isInt(clone.ram_mb)) clone.ram_mb = toInt(clone.ram_mb, 0);
  if (!isInt(clone.disk_gb)) clone.disk_gb = toInt(clone.disk_gb, 0);
  clone.name = sanitizeName(clone.name || "node");
  clone.address = String(clone.address || "").trim();
  clone.ports = normalizePorts(clone.ports);
  if (!clone.token_id) clone.token_id = randTokenId();
  if (!clone.token) clone.token = randSecret();
  if (!clone.createdAt) clone.createdAt = Date.now();
  clone.api_port = clampPort(clone.api_port || 8080);
  clone.sftp_port = clampPort(clone.sftp_port || 2022);
  clone.max_upload_mb = Math.max(1, Math.min(100000, toInt(clone.max_upload_mb, 10240)));
  if (typeof clone.port_ok !== "boolean") clone.port_ok = null;
  if (clone.last_seen != null) clone.last_seen = Number(clone.last_seen);
  if (clone.last_check != null) clone.last_check = Number(clone.last_check);
  return clone;
}
function mapNodeRow(row) {
  const bc = parseDbJson(row.build_config, {});
  return hardenNode({
    id: row.id,
    uuid: row.uuid,
    name: row.name,
    address: row.address,
    ram_mb: row.ram_mb,
    disk_gb: row.disk_gb,
    ports: parseDbJson(row.ports, { mode: "range", start: 25565, count: 10 }),
    token_id: row.token_id,
    token: row.token,
    createdAt: row.created_at ? Number(row.created_at) : null,
    api_port: row.api_port,
    sftp_port: row.sftp_port != null ? Number(row.sftp_port) : 2022,
    port_ok: row.port_ok === null ? null : !!row.port_ok,
    last_seen: row.last_seen,
    last_check: row.last_check,
    online: row.online === null ? null : !!row.online,
    buildConfig: bc,
    ssl_enabled: !!bc.ssl_enabled,
    max_upload_mb: row.max_upload_mb != null ? Number(row.max_upload_mb) : 10240,
  });
}

async function loadNodes() {
  const rows = await db.query("SELECT * FROM nodes");
  return rows.map(mapNodeRow);
}

async function loadNodesPaginated(page, limit, search) {
  page = Math.max(1, parseInt(page, 10) || 1);
  limit = Math.max(1, Math.min(100, parseInt(limit, 10) || 50));
  const offset = (page - 1) * limit;
  let countSql = "SELECT COUNT(*) AS total FROM nodes";
  let dataSql = "SELECT * FROM nodes";
  const params = [];
  const countParams = [];
  if (search && typeof search === "string" && search.trim()) {
    const pattern = `%${search.trim()}%`;
    const where = " WHERE LOWER(name) LIKE ? OR LOWER(address) LIKE ? OR LOWER(id) LIKE ? OR LOWER(uuid) LIKE ?";
    countSql += where;
    dataSql += where;
    countParams.push(pattern, pattern, pattern, pattern);
    params.push(pattern, pattern, pattern, pattern);
  }
  dataSql += " ORDER BY name ASC LIMIT ? OFFSET ?";
  params.push(limit, offset);
  const [countRows, dataRows] = await Promise.all([
    db.query(countSql, countParams),
    db.query(dataSql, params)
  ]);
  const total = Number(countRows[0]?.total || 0);
  return { nodes: dataRows.map(mapNodeRow), total, page, limit, totalPages: Math.ceil(total / limit) || 1 };
}

async function findNodeByIdOrName(idOrName) {
  const key = String(idOrName || "").trim().toLowerCase();
  if (!key) return null;
  const rows = await db.query(
    "SELECT * FROM nodes WHERE id = ? OR uuid = ? OR LOWER(name) = ? LIMIT 1",
    [key, key, key]
  );
  return rows.length ? mapNodeRow(rows[0]) : null;
}

async function insertNode(node) {
  const payload = hardenNode(node);
  const online = computeOnline(payload);
  await db.query(
    `INSERT INTO nodes
      (id, uuid, name, address, ram_mb, disk_gb, ports, token_id, token, created_at, api_port, sftp_port, port_ok, last_seen, last_check, online, build_config, max_upload_mb)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      payload.id,
      payload.uuid || payload.id,
      payload.name,
      payload.address,
      payload.ram_mb,
      payload.disk_gb,
      toDbJson(payload.ports, "{}"),
      payload.token_id,
      payload.token,
      payload.createdAt || Date.now(),
      payload.api_port || 8080,
      payload.sftp_port || 2022,
      payload.port_ok === null ? null : payload.port_ok ? 1 : 0,
      payload.last_seen ?? null,
      payload.last_check ?? null,
      online ? 1 : 0,
      toDbJson(payload.buildConfig || {}, "{}"),
      payload.max_upload_mb,
    ]
  );
  return payload;
}

async function saveNode(node) {
  const payload = hardenNode(node);
  const online = computeOnline(payload);
  await db.query(
    `UPDATE nodes SET
      uuid = ?, name = ?, address = ?, ram_mb = ?, disk_gb = ?, ports = ?, token_id = ?, token = ?, created_at = ?,
      api_port = ?, sftp_port = ?, port_ok = ?, last_seen = ?, last_check = ?, online = ?, build_config = ?, max_upload_mb = ?
      WHERE id = ?`,
    [
      payload.uuid || payload.id,
      payload.name,
      payload.address,
      payload.ram_mb,
      payload.disk_gb,
      toDbJson(payload.ports, "{}"),
      payload.token_id,
      payload.token,
      payload.createdAt || Date.now(),
      payload.api_port || 8080,
      payload.sftp_port || 2022,
      payload.port_ok === null ? null : payload.port_ok ? 1 : 0,
      payload.last_seen ?? null,
      payload.last_check ?? null,
      online ? 1 : 0,
      toDbJson(payload.buildConfig || {}, "{}"),
      payload.max_upload_mb,
      payload.id,
    ]
  );
  return payload;
}

async function deleteNode(nodeId) {
  await db.query("DELETE FROM nodes WHERE id = ? OR uuid = ?", [nodeId, nodeId]);
}

function sanitizeUpdatePayload(body) {
  const out = {};
  if (typeof body.name !== "undefined") out.name = sanitizeName(body.name);
  if (typeof body.address !== "undefined") out.address = String(body.address || "").trim();
  if (typeof body.cpu_cores !== "undefined") out.cpu_cores = toInt(body.cpu_cores, 0);
  if (typeof body.ram_mb !== "undefined") out.ram_mb = toInt(body.ram_mb, 0);
  else if (typeof body.ramMB !== "undefined") out.ram_mb = toInt(body.ramMB, 0);
  else if (typeof body.ram_gb !== "undefined") out.ram_mb = toInt(body.ram_gb, 0) * 1024;
  if (typeof body.disk_gb !== "undefined") out.disk_gb = toInt(body.disk_gb, 0);
  else if (typeof body.storage_gb !== "undefined") out.disk_gb = toInt(body.storage_gb, 0);
  else if (typeof body.storageGB !== "undefined") out.disk_gb = toInt(body.storageGB, 0);
  if (typeof body.ports !== "undefined" ||
    typeof body.port_list !== "undefined" ||
    typeof body.ports_list !== "undefined" ||
    typeof body.mode !== "undefined") {
    const candidate = body.ports ?? body.ports_list ?? body;
    out.ports = normalizePorts(candidate);
  }
  if (typeof body.api_port !== "undefined") out.api_port = clampPort(body.api_port);
  if (typeof body.sftp_port !== "undefined") out.sftp_port = clampPort(body.sftp_port);
  if (typeof body.max_upload_mb !== "undefined") {
    out.max_upload_mb = Math.max(1, Math.min(100000, toInt(body.max_upload_mb, 10240)));
  }
  if (typeof body.ssl_enabled !== "undefined") out.ssl_enabled = !!body.ssl_enabled;
  return out;
}
function computeOnline(n) {
  const fresh = !!(n.last_seen && (Date.now() - Number(n.last_seen)) < HEARTBEAT_TTL_MS);
  return !!(fresh && n.port_ok === true);
}
function toPublic(n, includeSecrets = false) {
  const bc = n.buildConfig || {};
  const out = {
    id: n.id,
    uuid: n.uuid,
    name: n.name,
    address: n.address,
    cpu_cores: bc.cpu_cores || 0,
    ram_mb: n.ram_mb,
    disk_gb: n.disk_gb,
    ports: n.ports,
    createdAt: n.createdAt,
    api_port: n.api_port,
    sftp_port: n.sftp_port || 2022,
    max_upload_mb: n.max_upload_mb || 10240,
    ssl_enabled: !!bc.ssl_enabled,
    port_ok: n.port_ok,
    last_seen: n.last_seen,
    online: computeOnline(n),
    buildConfig: bc
  };
  if (includeSecrets) {
    out.token_id = n.token_id;
    out.token = n.token;
  }
  return out;
}

function includeNodeSecrets(req) {
  return !!(req && req.isAdmin);
}
function buildNodeBaseUrl(address, port, sslEnabled) {
  let base = String(address || "").trim();
  if (!base) return null;
  if (/^https?:\/\//i.test(base)) {
    try {
      const u = new URL(base);
      if (!u.port) u.port = String(port || 8080);
      return u.toString().replace(/\/$/, "");
    } catch {
    }
  }
  const proto = sslEnabled ? "https" : "http";
  return `${proto}://${base}:${clampPort(port || 8080)}`;
}
function httpRequestJson(fullUrl, method = "GET", headers = {}, timeoutMs = 2500) {
  return new Promise((resolve) => {
    try {
      const lib = fullUrl.startsWith("https:") ? https : http;
      const req = lib.request(fullUrl, { method, headers }, (res) => {
        const { statusCode } = res;
        const chunks = [];
        res.on("data", (d) => chunks.push(d));
        res.on("end", () => {
          const bodyStr = Buffer.concat(chunks).toString("utf8");
          try {
            const json = bodyStr ? JSON.parse(bodyStr) : null;
            resolve({ status: statusCode, json });
          } catch {
            resolve({ status: statusCode, json: null });
          }
        });
      });
      req.on("timeout", () => { try { req.destroy(); } catch { } resolve({ status: 0, json: null }); });
      req.on("error", () => resolve({ status: 0, json: null }));
      req.setTimeout(timeoutMs);
      req.end();
    } catch {
      resolve({ status: 0, json: null });
    }
  });
}
const DEFAULT_NODE_TIMEOUT_MS = 10000;
const LONG_NODE_TIMEOUT_MS = 120000;
function callNodeApi(node, pathSuffix, method = "GET", body = null, timeoutMs = DEFAULT_NODE_TIMEOUT_MS) {
  return new Promise((resolve) => {
    try {
      const base = buildNodeBaseUrl(node.address, node.api_port, node.ssl_enabled);
      if (!base) return resolve({ status: 0, json: null });
      const fullUrl = `${base}${pathSuffix}`;
      const isHttps = fullUrl.startsWith("https:");
      const lib = isHttps ? https : http;
      const headers = {
        "Authorization": `Bearer ${node.token}`,
        "X-Node-Token": node.token || "",
        "X-Node-Token-Id": node.token_id || "",
        "Content-Type": "application/json",
      };
      const req = lib.request(fullUrl, { method, headers }, (res) => {
        const chunks = [];
        res.on("data", (d) => chunks.push(d));
        res.on("end", () => {
          const bodyStr = Buffer.concat(chunks).toString("utf8");
          try {
            const json = bodyStr ? JSON.parse(bodyStr) : null;
            resolve({ status: res.statusCode, json });
          } catch {
            resolve({ status: res.statusCode, json: null });
          }
        });
      });
      req.on("timeout", () => { try { req.destroy(); } catch { } resolve({ status: 0, json: null }); });
      req.on("error", () => resolve({ status: 0, json: null }));
      req.setTimeout(timeoutMs);
      if (body) req.write(JSON.stringify(body));
      req.end();
    } catch {
      resolve({ status: 0, json: null });
    }
  });
}
async function resolveNodeForServer(serverName) {
  const name = String(serverName || "").trim();
  if (!name) return { server: null, node: null, nodeId: null };
  const srv = await findServerByNameOrId(name);
  if (!srv) return { server: null, node: null, nodeId: null };
  const rawNodeId = (srv.node || srv.nodeId || srv.node_id || "");
  const key = String(rawNodeId || "").trim();
  if (!key) return { server: srv, node: null, nodeId: null };
  const node = await findNodeByIdOrName(key);
  return { server: srv, node, nodeId: node ? node.uuid : null };
}
function httpRequestJsonWithBody(fullUrl, method = "POST", body = null, headers = {}, timeoutMs = 8000) {
  return new Promise((resolve) => {
    try {
      const lib = fullUrl.startsWith("https:") ? https : http;
      const payload = body ? Buffer.from(JSON.stringify(body)) : null;
      const req = lib.request(fullUrl, {
        method,
        headers: {
          "Content-Type": "application/json",
          ...(payload ? { "Content-Length": payload.length } : {}),
          ...headers
        }
      }, (res) => {
        const { statusCode } = res;
        const chunks = [];
        res.on("data", (d) => chunks.push(d));
        res.on("end", () => {
          const bodyStr = Buffer.concat(chunks).toString("utf8");
          try {
            const json = bodyStr ? JSON.parse(bodyStr) : null;
            resolve({ status: statusCode, json });
          } catch {
            resolve({ status: statusCode, json: null });
          }
        });
      });
      req.on("timeout", () => { try { req.destroy(); } catch { } resolve({ status: 0, json: null }); });
      req.on("error", () => resolve({ status: 0, json: null }));
      req.setTimeout(timeoutMs);
      if (payload) req.write(payload);
      req.end();
    } catch {
      resolve({ status: 0, json: null });
    }
  });
}
function httpJson(fullUrl, { method = "GET", headers = {}, body = null, timeoutMs = 5000 } = {}) {
  return new Promise((resolve) => {
    try {
      const lib = fullUrl.startsWith("https:") ? https : http;
      const opts = { method, headers };
      const req = lib.request(fullUrl, opts, (res) => {
        const chunks = [];
        res.on("data", (d) => chunks.push(d));
        res.on("end", () => {
          const text = Buffer.concat(chunks).toString("utf8");
          let json = null;
          try { json = text ? JSON.parse(text) : null; } catch { }
          resolve({ status: res.statusCode, json, text });
        });
      });
      req.on("timeout", () => { try { req.destroy(); } catch { } resolve({ status: 0, json: null }); });
      req.on("error", () => resolve({ status: 0, json: null }));
      req.setTimeout(timeoutMs);
      if (body != null) {
        const data = typeof body === "string" ? body : JSON.stringify(body);
        if (!headers["Content-Type"]) req.setHeader("Content-Type", "application/json");
        req.setHeader("Content-Length", Buffer.byteLength(data));
        req.write(data);
      }
      req.end();
    } catch {
      resolve({ status: 0, json: null });
    }
  });
}
function tcpCheck(host, port, timeoutMs = 2000) {
  return new Promise((resolve) => {
    try {
      const socket = new net.Socket();
      let finished = false;
      const done = (ok) => {
        if (finished) return;
        finished = true;
        try { socket.destroy(); } catch { }
        resolve(!!ok);
      };
      socket.setTimeout(timeoutMs);
      socket.once("connect", () => done(true));
      socket.once("timeout", () => done(false));
      socket.once("error", () => done(false));
      socket.connect(clampPort(port || 8080), host);
    } catch {
      resolve(false);
    }
  });
}
async function activeCheckNode(node, opts = { force: false }) {
  const now = Date.now();
  if (!opts.force && node.last_check && (now - Number(node.last_check)) < 5000) return;
  node.last_check = now;

  let ok = false;
  const baseUrl = buildNodeBaseUrl(node.address, node.api_port, node.ssl_enabled);
  if (baseUrl) {
    const checkTimeout = 1500;
    try {
      const [infoRes, healthRes] = await Promise.all([
        httpRequestJson(`${baseUrl}/v1/info`, "GET", { "Authorization": `Bearer ${node.token}` }, checkTimeout),
        httpRequestJson(`${baseUrl}/health`, "GET", {}, checkTimeout)
      ]);

      if (infoRes.status === 200 && infoRes.json && infoRes.json.ok && infoRes.json.node) {
        const uuid = String(infoRes.json.node.uuid || "");
        if (uuid && (uuid === node.uuid || uuid === node.id)) ok = true;
        if (infoRes.json.node.volumesDir) {
          node.volumesDir = String(infoRes.json.node.volumesDir);
          const cacheKey = String(node.uuid || node.id || "");
          if (cacheKey) _nodeVolumeDirCache.set(cacheKey, node.volumesDir);
        }
      }
      if (!ok && healthRes.status === 200 && healthRes.json && healthRes.json.uuid) {
        const uuid = String(healthRes.json.uuid || "");
        if (uuid && (uuid === node.uuid || uuid === node.id)) ok = true;
      }
      if (!ok && healthRes.status === 200 && infoRes.status === 401) {
        console.warn(`[node-check] Node ${node.name} (${node.address}) is reachable but auth FAILED. Token mismatch — re-deploy config.yml.`);
      }
    } catch {
      ok = false;
    }
  }
  node.port_ok = !!ok;
  if (ok) {
    node.last_seen = now;
  }
}
function buildConfigYml(node, req) {
  let panelUrl;
  if (PANEL_PUBLIC_URL) {
    panelUrl = PANEL_PUBLIC_URL;
  } else {
    console.warn("[SECURITY] PANEL_PUBLIC_URL not set - config.yml panel URL derived from request headers. Set PANEL_PUBLIC_URL for production.");
    const host = req.get("x-forwarded-host") || req.get("host") || "localhost";
    const proto = (req.get("x-forwarded-proto") || req.protocol || "http");
    panelUrl = `${proto}://${host}`;
  }
  const sslEnabled = !!node.ssl_enabled;
  const lines = [
    `debug: false`,
    `uuid: ${node.uuid}`,
    `token_id: ${node.token_id}`,
    `token: ${node.token}`,
    `auth:`,
    `  token_id: ${node.token_id}`,
    `  token: ${node.token}`,
    `api:`,
    `  host: 0.0.0.0`,
    `  port: ${node.api_port || 8080}`,
    `  ssl:`,
    `    enabled: ${sslEnabled}`,
    `    cert: ""`,
    `    key: ""`,
    `  upload_limit: ${node.max_upload_mb || 10240}`,
    `system:`,
    `  data: /var/lib/node`,
    `  sftp:`,
    `    bind_port: ${node.sftp_port || 2022}`,
    `allowed_mounts: []`,
    `panel:`,
    `  url: ${panelUrl}`,
    `  node_id: ${node.uuid}`
  ];
  return lines.join("\n") + "\n";
}
function oneTimeCommand(node, req) {
  let base;
  if (PANEL_PUBLIC_URL) {
    base = PANEL_PUBLIC_URL;
  } else {
    const host = req.get("x-forwarded-host") || req.get("host") || "localhost";
    const proto = (req.get("x-forwarded-proto") || req.protocol || "http");
    base = `${proto}://${host}`;
  }
  const url = `${base}/api/nodes/${encodeURIComponent(node.uuid)}/config.yml`;
  const header = NODE_BOOTSTRAP_TOKEN ? `-H "X-Node-Bootstrap: ${NODE_BOOTSTRAP_TOKEN}" ` : "";
  return `mkdir -p /etc/adnode && curl -fsSL ${header}"${url}" -o /etc/adnode/config.yml && echo "Config saved to /etc/adnode/config.yml"`;
}
const app = express();
app.use(express.json({ limit: "2mb" }));
app.use(express.urlencoded({ extended: true }));
const CORS_ALLOWED_ORIGINS = new Set(
  (process.env.NODES_CORS_ORIGINS || "")
    .split(",")
    .map(s => s.trim())
    .filter(Boolean)
);

function sameOrigin(req, origin) {
  const host = req.get("host");
  if (!host) return false;
  const proto = (req.get("x-forwarded-proto") || req.protocol || "http");
  const expected = `${proto}://${host}`;
  return origin === expected;
}

function applyCors(req, res) {
  const origin = req.get("origin");
  if (!origin) return;
  if (CORS_ALLOWED_ORIGINS.size > 0) {
    if (!CORS_ALLOWED_ORIGINS.has(origin)) return;
  } else if (!sameOrigin(req, origin)) {
    return;
  }
  res.header("Access-Control-Allow-Origin", origin);
  res.header("Access-Control-Allow-Credentials", "true");
  res.header("Access-Control-Allow-Methods", "GET,POST,PATCH,DELETE,OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With, X-Node-Token-Id");
}

app.use((req, res, next) => {
  applyCors(req, res);
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

const NODES_PUBLIC_ROUTES = new Set([
  "/health",
  "/api/nodes/:id/heartbeat",
  "/api/nodes/:id/config.yml",
]);

const NODES_SERVER_ROUTES_PATTERN = /^\/api\/nodes\/server\/[^/]+\/(info|entries|file|logs|stats|action|command|delete|delete-batch|rename|upload|create|extract|archive|backups(\/[^/]+)?(\/restore)?)$/;

app.use((req, res, next) => {
  const path = req.path || "";

  if (path === "/health") return next();

  if (/^\/api\/nodes\/[^/]+\/heartbeat$/.test(path)) return next();

  if (NODES_SERVER_ROUTES_PATTERN.test(path)) return next();

  if (path.startsWith("/api/nodes")) {
    if (!req.isAdmin) {
      return res.status(403).json({ error: "admin required" });
    }
  }

  next();
});

app.get("/health", (_req, res) => res.json({ ok: true }));
app.get("/api/nodes", async (req, res) => {
  const wantPaginated = req.query.page || req.query.limit || req.query.search;
  if (wantPaginated) {
    const result = await loadNodesPaginated(req.query.page, req.query.limit, req.query.search);
    return res.json({
      nodes: result.nodes.map((n) => toPublic(n, includeNodeSecrets(req))),
      total: result.total,
      page: result.page,
      limit: result.limit,
      totalPages: result.totalPages
    });
  }
  const list = await loadNodes();
  res.json({ nodes: list.map((n) => toPublic(n, includeNodeSecrets(req))) });
});
app.get("/api/nodes/:id", async (req, res) => {
  const node = await findNodeByIdOrName(req.params.id);
  if (!node) return res.status(404).json({ error: "not found" });
  res.json(toPublic(node, includeNodeSecrets(req)));
});
app.post("/api/nodes/:id/check", async (req, res) => {
  const node = await findNodeByIdOrName(req.params.id);
  if (!node) return res.status(404).json({ error: "not found" });
  await activeCheckNode(node, { force: true });
  await saveNode(node);
  const isOnline = computeOnline(node);
  res.json({ ok: isOnline, node: toPublic(node, includeNodeSecrets(req)) });
});
app.post("/api/nodes/:id/servers/create", async (req, res) => {
  const node = await findNodeByIdOrName(req.params.id);
  if (!node) return res.status(404).json({ error: "node_not_found" });
  const name = String(req.body?.name || "").trim();
  const templateId = String(req.body?.templateId || "minecraft");
  const mcFork = String(req.body?.mcFork || "paper");
  const mcVersion = String(req.body?.mcVersion || "1.21.8");
  const hostPort = Number(req.body?.hostPort || 25565);
  const autoStart = !!req.body?.autoStart;
  if (!name) return res.status(400).json({ error: "missing_name" });

  if (hostPort > 0 && !isPortInNodeAllocation(node, hostPort)) {
    const alloc = node.ports || {};
    let allocDesc = "none";
    if (alloc.mode === "range") allocDesc = `range ${alloc.start}\u2013${alloc.start + alloc.count - 1}`;
    else if (alloc.mode === "list" && Array.isArray(alloc.ports)) allocDesc = `list [${alloc.ports.slice(0, 10).join(", ")}${alloc.ports.length > 10 ? "\u2026" : ""}]`;
    return res.status(400).json({ error: `Port ${hostPort} is not in this node's allocated ports (${allocDesc}).` });
  }

  const { status, json } = await callNodeApi(
    node, "/v1/servers/create", "POST",
    { name, templateId, mcFork, mcVersion, hostPort, autoStart },
    LONG_NODE_TIMEOUT_MS
  );
  if (status === 200 && json && json.ok) return res.json(json);
  return res.status(500).json({ error: "node_create_failed", status, detail: json && json.error });
});
app.post("/api/nodes", async (req, res) => {
  const body = req.body || {};
  const name = sanitizeName(body.name || body.node || body.id);
  const address = String(body.address || body.ip || body.fqdn || "").trim();
  if (!name) return res.status(400).json({ error: "invalid name" });
  if (!address) return res.status(400).json({ error: "invalid address" });
  const nameKey = name.toLowerCase();
  const exists = await db.query("SELECT id FROM nodes WHERE LOWER(name) = ? LIMIT 1", [nameKey]);
  if (exists.length) {
    return res.status(400).json({ error: "node already exists" });
  }
  const cpu_cores = toInt(body.cpu_cores, 0);
  const ram_mb =
    (typeof body.ram_mb !== "undefined") ? toInt(body.ram_mb, 0) :
      (typeof body.ramMB !== "undefined") ? toInt(body.ramMB, 0) :
        (typeof body.ram_gb !== "undefined") ? toInt(body.ram_gb, 0) * 1024 : 0;
  const disk_gb =
    (typeof body.disk_gb !== "undefined") ? toInt(body.disk_gb, 0) :
      (typeof body.storage_gb !== "undefined") ? toInt(body.storage_gb, 0) :
        (typeof body.storageGB !== "undefined") ? toInt(body.storageGB, 0) : 0;
  const ports = normalizePorts(body.ports);
  const api_port = clampPort(body.api_port || 8080);
  const sftp_port = clampPort(body.sftp_port || 2022);
  const max_upload_mb = Math.max(1, Math.min(100000, toInt(body.max_upload_mb, 10240)));
  const node = await insertNode({
    id: uid(),
    uuid: undefined,
    name,
    address,
    ram_mb,
    disk_gb,
    ports,
    token_id: randTokenId(),
    token: randSecret(),
    createdAt: Date.now(),
    api_port,
    sftp_port,
    max_upload_mb,
    port_ok: null,
    last_seen: null,
    last_check: null,
    buildConfig: { cpu_cores, ssl_enabled: !!body.ssl_enabled }
  });
  res.json(toPublic(node, includeNodeSecrets(req)));
});
app.patch("/api/nodes/:id", async (req, res) => {
  const node = await findNodeByIdOrName(req.params.id);
  if (!node) return res.status(404).json({ error: "not found" });
  const current = hardenNode(node);
  const upd = sanitizeUpdatePayload(req.body || {});
  if (typeof upd.name !== "undefined" && upd.name) current.name = upd.name;
  if (typeof upd.address !== "undefined") current.address = upd.address;
  if (typeof upd.cpu_cores !== "undefined" && upd.cpu_cores >= 0) {
    current.buildConfig = current.buildConfig || {};
    current.buildConfig.cpu_cores = upd.cpu_cores;
  }
  if (typeof upd.ssl_enabled !== "undefined") {
    current.buildConfig = current.buildConfig || {};
    current.buildConfig.ssl_enabled = !!upd.ssl_enabled;
  }
  if (typeof upd.ram_mb !== "undefined" && upd.ram_mb >= 0) current.ram_mb = upd.ram_mb;
  if (typeof upd.disk_gb !== "undefined" && upd.disk_gb >= 0) current.disk_gb = upd.disk_gb;
  if (typeof upd.ports !== "undefined") current.ports = normalizePorts(upd.ports);
  if (typeof upd.api_port !== "undefined") current.api_port = clampPort(upd.api_port);
  if (typeof upd.sftp_port !== "undefined") current.sftp_port = clampPort(upd.sftp_port);
  if (typeof upd.max_upload_mb !== "undefined") current.max_upload_mb = upd.max_upload_mb;
  current.last_check = null;
  const saved = await saveNode(current);
  res.json(toPublic(saved, includeNodeSecrets(req)));
});
app.delete("/api/nodes/:id", async (req, res) => {
  const node = await findNodeByIdOrName(req.params.id);
  if (!node) return res.status(404).json({ error: "not found" });
  const assignedServers = await db.query(
    "SELECT COUNT(*) AS cnt FROM servers WHERE node_id = ?",
    [node.id]
  );
  const serverCount = Number(assignedServers[0]?.cnt || 0);
  if (serverCount > 0) {
    return res.status(409).json({
      error: `Cannot delete node — ${serverCount} server${serverCount === 1 ? " is" : "s are"} still assigned to it. Reassign or delete them first.`
    });
  }
  await deleteNode(node.id);
  res.json({ ok: true, deleted: node.id });
});
app.get("/api/nodes/:id/build", async (req, res) => {
  const node = await findNodeByIdOrName(req.params.id);
  if (!node) return res.status(404).json({ error: "not found" });
  res.json({ build: node.buildConfig || {} });
});
app.post("/api/nodes/:id/build", async (req, res) => {
  const node = await findNodeByIdOrName(req.params.id);
  if (!node) return res.status(404).json({ error: "not found" });
  const incoming = (req.body && typeof req.body === "object") ? req.body : {};
  node.buildConfig = incoming;
  await saveNode(node);
  res.json({ ok: true });
});
app.get("/api/nodes/:id/config.yml", async (req, res) => {
  const bootstrapHeader = req.get("X-Node-Bootstrap") || "";
  const bearerToken = (req.get("authorization") || "").replace(/^Bearer\s+/i, "");

  const node = await findNodeByIdOrName(req.params.id);

  if (req.isAdmin) {
  }
  else if (NODE_BOOTSTRAP_TOKEN && bootstrapHeader && safeCompare(bootstrapHeader, NODE_BOOTSTRAP_TOKEN)) {
  }
  else if (bearerToken && node && safeCompare(bearerToken, node.token)) {
  }
  else {
    return res.status(401).send("authentication required");
  }

  if (!node) return res.status(404).send("not found");
  const yml = buildConfigYml(node, req);
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, private");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Content-Type", "text/yaml; charset=utf-8");
  res.send(yml);
});
app.get("/api/nodes/:id/one-time-command", async (req, res) => {
  const node = await findNodeByIdOrName(req.params.id);
  if (!node) return res.status(404).json({ error: "not found" });
  res.json({ command: oneTimeCommand(node, req) });
});
app.post("/api/nodes/:id/heartbeat", async (req, res) => {
  try {
    const node = await findNodeByIdOrName(req.params.id);
    if (!node) return res.status(404).json({ error: "not found" });
    const current = hardenNode(node);

    const authHeader = req.get("authorization") || "";
    const bearer = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : null;
    const body = req.body || {};
    const bodyToken = body.token || null;
    const bodyTokenId = body.token_id || null;

    if (!bearer && !bodyToken) {
      return res.status(401).json({ error: "authentication required" });
    }

    if (bearer && !safeCompare(bearer, current.token)) {
      return res.status(401).json({ error: "invalid token" });
    }

    if (!bearer && bodyToken && !safeCompare(bodyToken, current.token)) {
      return res.status(401).json({ error: "invalid token" });
    }

    if (bodyTokenId && !safeCompare(bodyTokenId, current.token_id)) {
      return res.status(401).json({ error: "invalid token_id" });
    }

    if (typeof body.api_port !== "undefined") current.api_port = clampPort(body.api_port);
    current.last_seen = Date.now();
    current.port_ok = true;
    current.last_check = Date.now();
    await saveNode(current);
    return res.json({
      ok: true,
      now: current.last_seen,
      port_ok: current.port_ok,
      online: computeOnline(current)
    });
  } catch (e) {
    console.error("[heartbeat] failed:", e && e.message);
    return res.status(500).json({ error: "heartbeat failed" });
  }
});
const HC_BATCH_SIZE = parseInt(process.env.HC_BATCH_SIZE || "10", 10) || 10;
const HC_INTERVAL_MS = parseInt(process.env.HC_INTERVAL_MS || "5000", 10) || 5000;
let _hcOffset = 0;
setInterval(async () => {
  try {
    const countRows = await db.query("SELECT COUNT(*) AS total FROM nodes");
    const total = Number(countRows[0]?.total || 0);
    if (total === 0) { _hcOffset = 0; return; }
    if (_hcOffset >= total) _hcOffset = 0;
    const batch = await db.query(
      "SELECT * FROM nodes ORDER BY id ASC LIMIT ? OFFSET ?",
      [HC_BATCH_SIZE, _hcOffset]
    );
    _hcOffset += HC_BATCH_SIZE;
    const nodes = batch.map(mapNodeRow);
    for (const n of nodes) {
      try {
        await activeCheckNode(n, { force: true });
        await saveNode(n);
      } catch (_) { }
    }
  } catch (_) { }
}, HC_INTERVAL_MS);
const NODE_VOLUME_ROOT = "/var/lib/node/servers";
const _nodeVolumeDirCache = new Map();
function getNodeVolumesDir(node) {
  const key = String(node.uuid || node.id || "");
  return (key && _nodeVolumeDirCache.get(key)) || node.volumesDir || NODE_VOLUME_ROOT;
}
function mapServerRow(row) {
  return {
    id: row.id,
    name: row.name,
    legacy_id: row.legacy_id,
    bot: row.bot,
    template: row.template,
    start: row.start,
    node_id: row.node_id,
    nodeId: row.node_id,
    node: row.node_id,
    ip: row.ip,
    port: row.port,
    status: row.status,
    runtime: parseDbJson(row.runtime, null),
    docker: parseDbJson(row.docker, null),
    acl: parseDbJson(row.acl, null),
  };
}

async function findServerByNameOrId(name) {
  const key = String(name || "").trim();
  if (!key) return null;
  const lower = key.toLowerCase();
  const numericId = /^\d+$/.test(key) ? Number(key) : null;
  const rows = await db.query(
    "SELECT * FROM servers WHERE LOWER(name) = ? OR LOWER(legacy_id) = ? OR LOWER(bot) = ? OR id = ? LIMIT 1",
    [lower, lower, lower, numericId]
  );
  return rows.length ? mapServerRow(rows[0]) : null;
}
function serverNodeRef(srv) {
  return (srv && (srv.node || srv.nodeId || srv.node_id)) || null;
}
function buildServerInfo(srv) {
  if (!srv) return {};
  const ip = srv.ip || srv.host || srv.address || srv.hostname || null;
  const port = srv.port || srv.server_port || srv.bind_port || null;
  const start = srv.start || srv.startFile || srv.entry || null;
  return { ip, port, start };
}
function nodeHeaders(node) {
  return {
    "Authorization": `Bearer ${node.token || ""}`,
    "X-Node-Token": node.token || "",
    "X-Node-Token-Id": node.token_id || ""
  };
}
function nodeUrl(node, suffix) {
  const base = buildNodeBaseUrl(node.address, node.api_port, node.ssl_enabled);
  return `${base}${suffix}`;
}

function streamUploadToNode(node, tmpPath, originalName, destDir, fileSize) {
  return new Promise((resolve) => {
    try {
      const fullUrl = nodeUrl(node, "/v1/fs/upload");
      const lib = fullUrl.startsWith("https:") ? https : http;

      const boundary = `----ADPanelUpload${crypto.randomBytes(16).toString("hex")}`;

      const dirPart =
        `--${boundary}\r\n` +
        `Content-Disposition: form-data; name="dir"\r\n\r\n` +
        `${destDir}\r\n`;

      const fileHeader =
        `--${boundary}\r\n` +
        `Content-Disposition: form-data; name="file"; filename="${originalName.replace(/"/g, '\\"')}"\r\n` +
        `Content-Type: application/octet-stream\r\n\r\n`;

      const fileFooter = `\r\n--${boundary}--\r\n`;

      const preamble = Buffer.from(dirPart + fileHeader, "utf8");
      const epilogue = Buffer.from(fileFooter, "utf8");
      const totalLength = preamble.length + fileSize + epilogue.length;

      const headers = {
        ...nodeHeaders(node),
        "Content-Type": `multipart/form-data; boundary=${boundary}`,
        "Content-Length": totalLength,
      };

      const req = lib.request(fullUrl, { method: "POST", headers }, (res) => {
        const chunks = [];
        res.on("data", (d) => chunks.push(d));
        res.on("end", () => {
          const bodyStr = Buffer.concat(chunks).toString("utf8");
          let json = null;
          try { json = bodyStr ? JSON.parse(bodyStr) : null; } catch { }
          resolve({ status: res.statusCode, json });
        });
      });

      const timeoutMs = Math.max(UPLOAD_TIMEOUT_MS, Math.ceil(fileSize / (1024 * 1024)) * 1000);
      req.setTimeout(timeoutMs);
      req.on("timeout", () => { try { req.destroy(); } catch { } resolve({ status: 0, json: { error: "upload_timeout" } }); });
      req.on("error", (err) => resolve({ status: 0, json: { error: "upload_network_error", detail: err.message } }));

      req.write(preamble);

      const fileStream = fs.createReadStream(tmpPath, { highWaterMark: 64 * 1024 });
      fileStream.on("data", (chunk) => {
        if (!req.write(chunk)) {
          fileStream.pause();
          req.once("drain", () => fileStream.resume());
        }
      });
      fileStream.on("end", () => {
        req.write(epilogue);
        req.end();
      });
      fileStream.on("error", (err) => {
        try { req.destroy(); } catch { }
        resolve({ status: 0, json: { error: "file_read_error", detail: err.message } });
      });
    } catch (err) {
      resolve({ status: 0, json: { error: "stream_error", detail: err.message } });
    }
  });
}

function safeJoinUnix(base, rel) {
  const b = base.endsWith("/") ? base.slice(0, -1) : base;
  const raw = String(rel || "").replace(/\\/g, "/");
  const norm = path.posix.normalize("/" + raw).replace(/^\/+/, "");
  const joined = `${b}/${norm}`;
  if (!joined.startsWith(b + "/") && joined !== b) throw new Error("path traversal");
  return joined;
}

function maskPathErrorMessage(err, fallback = "bad_path") {
  const msg = err && err.message ? String(err.message) : "";
  const lower = msg.toLowerCase();
  if (lower.includes("traversal") || lower.includes("path")) return "invalid path";
  return msg || fallback;
}
function mapFsEntries(entries) {
  const out = [];
  (entries || []).forEach(e => {
    if (!e || !e.name) return;
    const entry = { name: e.name, isDir: !!(e.type === "dir" || e.isDir) };
    if (typeof e.size === "number") entry.size = e.size;
    out.push(entry);
  });
  return out;
}
async function remoteContext(serverName) {
  const srv = await findServerByNameOrId(serverName);
  if (!srv) return { exists: false };
  const ref = serverNodeRef(srv);
  if (!ref) return { exists: true, remote: false, info: buildServerInfo(srv) };
  const node = await findNodeByIdOrName(ref);
  if (!node) return { exists: true, remote: false, info: buildServerInfo(srv) };
  const volRoot = getNodeVolumesDir(node);
  const baseDir = `${volRoot}/${sanitizeName(srv.name || serverName)}`;
  return {
    exists: true,
    remote: true,
    node,
    nodeId: node.uuid,
    baseDir,
    info: buildServerInfo(srv)
  };
}
app.get("/api/nodes/server/:name/info", async (req, res) => {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");
  const serverName = req.params.name;

  const accessCheck = await checkServerAccess(req, serverName);
  if (!accessCheck.ok) {
    return res.status(accessCheck.status).json({ error: accessCheck.error });
  }

  const ctx = await remoteContext(serverName);
  if (!ctx.exists) {
    console.log(`[node-info] Server ${serverName} not found`);
    return res.json({ ok: false, remote: false, info: null, nodeOnline: false });
  }
  if (ctx.remote && ctx.node) {
    try { await activeCheckNode(ctx.node, { force: true }); } catch (e) {
      console.log(`[node-info] activeCheckNode error for ${req.params.name}:`, e.message);
    }
    const nodeOnline = computeOnline(ctx.node);
    console.log(`[node-info] Server ${req.params.name} - remote: true, port_ok: ${ctx.node.port_ok}, last_seen: ${ctx.node.last_seen}, nodeOnline: ${nodeOnline}`);
    return res.json({
      ok: true,
      remote: true,
      nodeId: ctx.node.uuid,
      info: ctx.info || {},
      baseDir: ctx.baseDir,
      nodeOnline
    });
  }
  console.log(`[node-info] Server ${req.params.name} - remote: false (no nodeId), returning nodeOnline: true`);
  return res.json({ ok: true, remote: false, nodeId: null, info: ctx.info || {}, nodeOnline: true });
});
app.get("/api/nodes/server/:name/entries", async (req, res) => {
  const serverName = req.params.name;

  const accessCheck = await checkServerAccess(req, serverName);
  if (!accessCheck.ok) {
    return res.status(accessCheck.status).json({ error: accessCheck.error });
  }

  if (!(await checkUserPerm(req, serverName, "files_read"))) {
    return res.status(403).json({ error: "permission denied", permission: "files_read" });
  }

  const ctx = await remoteContext(serverName);
  if (!ctx.exists) return res.status(404).json({ error: "server not found" });
  if (!ctx.remote || !ctx.node) return res.status(400).json({ error: "not_remote" });
  if (!_nodeVolumeDirCache.has(String(ctx.node.uuid || ctx.node.id || ""))) {
    try { await activeCheckNode(ctx.node, { force: true }); } catch { }
    const volRoot = getNodeVolumesDir(ctx.node);
    ctx.baseDir = `${volRoot}/${sanitizeName(serverName)}`;
  }
  const rel = String(req.query.path || "");
  try {
    const full = safeJoinUnix(ctx.baseDir, rel);
    const { status, json } = await httpJson(
      nodeUrl(ctx.node, "/v1/fs/list"),
      { method: "POST", headers: Object.assign({ "Content-Type": "application/json" }, nodeHeaders(ctx.node)), body: { path: full, depth: 1 } }
    );
    if (status !== 200 || !json || !json.ok) return res.status(502).json({ error: "node_list_failed" });
    return res.json({ path: rel, entries: mapFsEntries(json.entries || []) });
  } catch (e) {
    return res.status(400).json({ error: maskPathErrorMessage(e) });
  }
});
app.get("/api/nodes/server/:name/file", async (req, res) => {
  const serverName = req.params.name;

  const accessCheck = await checkServerAccess(req, serverName);
  if (!accessCheck.ok) {
    return res.status(accessCheck.status).json({ error: accessCheck.error });
  }

  if (!(await checkUserPerm(req, serverName, "files_read"))) {
    return res.status(403).json({ error: "permission denied", permission: "files_read" });
  }

  const ctx = await remoteContext(serverName);
  if (!ctx.exists) return res.status(404).json({ error: "server not found" });
  if (!ctx.remote || !ctx.node) return res.status(400).json({ error: "not_remote" });
  const rel = String(req.query.path || "");
  try {
    const full = safeJoinUnix(ctx.baseDir, rel);
    const { status, json } = await httpJson(
      nodeUrl(ctx.node, "/v1/fs/read"),
      { method: "POST", headers: Object.assign({ "Content-Type": "application/json" }, nodeHeaders(ctx.node)), body: { path: full, encoding: "utf8" } }
    );
    if (status !== 200 || !json || !json.ok) return res.status(502).json({ error: "node_read_failed" });
    return res.json({ path: rel, content: typeof json.content === "string" ? json.content : "" });
  } catch (e) {
    return res.status(400).json({ error: maskPathErrorMessage(e) });
  }
});
app.get("/api/nodes/server/:name/download", async (req, res) => {
  const serverName = req.params.name;

  const accessCheck = await checkServerAccess(req, serverName);
  if (!accessCheck.ok) {
    return res.status(accessCheck.status).json({ error: accessCheck.error });
  }

  if (!(await checkUserPerm(req, serverName, "files_read"))) {
    return res.status(403).json({ error: "permission denied", permission: "files_read" });
  }

  const ctx = await remoteContext(serverName);
  if (!ctx.exists) return res.status(404).json({ error: "server not found" });
  if (!ctx.remote || !ctx.node) return res.status(400).json({ error: "not_remote" });
  const rel = String(req.query.path || "");
  try {
    const full = safeJoinUnix(ctx.baseDir, rel);
    const lib = nodeUrl(ctx.node, "").startsWith("https:") ? require("https") : require("http");
    const postData = JSON.stringify({ path: full });
    const url = new URL(nodeUrl(ctx.node, "/v1/fs/download"));
    const reqOpts = {
      hostname: url.hostname,
      port: url.port,
      path: url.pathname,
      method: "POST",
      headers: Object.assign({
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(postData)
      }, nodeHeaders(ctx.node)),
      timeout: 300000
    };
    const proxyReq = lib.request(reqOpts, (proxyRes) => {
      if (proxyRes.statusCode !== 200) {
        return res.status(proxyRes.statusCode || 502).json({ error: "download failed" });
      }
      if (proxyRes.headers["content-type"]) res.setHeader("Content-Type", proxyRes.headers["content-type"]);
      if (proxyRes.headers["content-disposition"]) res.setHeader("Content-Disposition", proxyRes.headers["content-disposition"]);
      if (proxyRes.headers["content-length"]) res.setHeader("Content-Length", proxyRes.headers["content-length"]);
      proxyRes.pipe(res);
    });
    proxyReq.on("error", () => res.status(502).json({ error: "node connection failed" }));
    proxyReq.on("timeout", () => { proxyReq.destroy(); res.status(504).json({ error: "download timeout" }); });
    proxyReq.write(postData);
    proxyReq.end();
  } catch (e) {
    return res.status(400).json({ error: maskPathErrorMessage(e) });
  }
});
app.post("/api/nodes/server/:name/file", async (req, res) => {
  const serverName = req.params.name;

  const accessCheck = await checkServerAccess(req, serverName);
  if (!accessCheck.ok) {
    return res.status(accessCheck.status).json({ error: accessCheck.error });
  }

  if (!(await checkUserPerm(req, serverName, "files_create"))) {
    return res.status(403).json({ error: "permission denied", permission: "files_create" });
  }

  const ctx = await remoteContext(serverName);
  if (!ctx.exists) return res.status(404).json({ error: "server not found" });
  if (!ctx.remote || !ctx.node) return res.status(400).json({ error: "not_remote" });
  const rel = String(req.body.path || "");
  const content = String((req.body && req.body.content) || "");
  try {
    const full = safeJoinUnix(ctx.baseDir, rel);
    const { status, json } = await httpJson(
      nodeUrl(ctx.node, "/v1/fs/write"),
      { method: "POST", headers: Object.assign({ "Content-Type": "application/json" }, nodeHeaders(ctx.node)), body: { path: full, content, encoding: "utf8" } }
    );
    if (status === 507 && json?.error === "disk_limit_exceeded") {
      return res.status(507).json(json);
    }
    if (status !== 200 || !json || !json.ok) return res.status(502).json({ error: "node_write_failed" });

    const userEmail = req.session?.user || "unknown";
    recordActivity(req.params.name, "file_edit", { path: rel }, userEmail, getRequestIp(req));

    return res.json({ ok: true });
  } catch (e) {
    return res.status(400).json({ error: maskPathErrorMessage(e) });
  }
});
app.post("/api/nodes/server/:name/delete", async (req, res) => {
  const ctx = await remoteContext(req.params.name);
  if (!ctx.exists) return res.status(404).json({ error: "server not found" });
  if (!ctx.remote || !ctx.node) return res.status(400).json({ error: "not_remote" });

  const accessCheck = await checkServerAccess(req, req.params.name);
  if (!accessCheck.ok) return res.status(accessCheck.status).json({ error: accessCheck.error });

  if (!(await checkUserPerm(req, req.params.name, "files_delete"))) {
    return res.status(403).json({ error: "permission denied", permission: "files_delete" });
  }
  const rel = String(req.body.path || "");
  const isDir = !!req.body.isDir;
  try {
    const full = safeJoinUnix(ctx.baseDir, rel);
    const { status, json } = await httpJson(
      nodeUrl(ctx.node, "/v1/fs/delete"),
      { method: "POST", headers: Object.assign({ "Content-Type": "application/json" }, nodeHeaders(ctx.node)), body: { path: full, isDir } }
    );
    if (status !== 200 || !json || !json.ok) return res.status(502).json({ error: "node_delete_failed" });

    const userEmail = req.session?.user || "unknown";
    recordActivity(req.params.name, "file_delete", { path: rel, isDir }, userEmail, getRequestIp(req));

    return res.json({ ok: true });
  } catch (e) {
    return res.status(400).json({ error: maskPathErrorMessage(e) });
  }
});

app.post("/api/nodes/server/:name/delete-batch", async (req, res) => {
  const ctx = await remoteContext(req.params.name);
  if (!ctx.exists) return res.status(404).json({ error: "server not found" });
  if (!ctx.remote || !ctx.node) return res.status(400).json({ error: "not_remote" });

  const accessCheck = await checkServerAccess(req, req.params.name);
  if (!accessCheck.ok) return res.status(accessCheck.status).json({ error: accessCheck.error });

  if (!(await checkUserPerm(req, req.params.name, "files_delete"))) {
    return res.status(403).json({ error: "permission denied", permission: "files_delete" });
  }

  const items = req.body.items;
  if (!Array.isArray(items) || items.length === 0) {
    return res.status(400).json({ error: "items array required" });
  }
  if (items.length > 500) {
    return res.status(400).json({ error: "too many items (max 500)" });
  }

  try {
    const fullItems = items.map(item => ({
      path: safeJoinUnix(ctx.baseDir, String(item.path || "")),
      isDir: !!item.isDir
    }));

    const { status, json } = await httpJson(
      nodeUrl(ctx.node, "/v1/fs/delete-batch"),
      {
        method: "POST",
        headers: Object.assign({ "Content-Type": "application/json" }, nodeHeaders(ctx.node)),
        body: { items: fullItems },
        timeoutMs: 120000
      }
    );

    if (status !== 200 || !json) {
      return res.status(502).json({ error: "node_batch_delete_failed" });
    }

    const userEmail = req.session?.user || "unknown";
    recordActivity(req.params.name, "file_delete_batch", {
      count: items.length,
      success: json.success || 0,
      failed: json.failed || 0
    }, userEmail, getRequestIp(req));

    return res.json(json);
  } catch (e) {
    console.error("[delete-batch] Error:", e);
    return res.status(500).json({ error: "batch_delete_error", detail: e.message });
  }
});

app.post("/api/nodes/server/:name/rename", async (req, res) => {
  const ctx = await remoteContext(req.params.name);
  if (!ctx.exists) return res.status(404).json({ error: "server not found" });
  if (!ctx.remote || !ctx.node) return res.status(400).json({ error: "not_remote" });

  const accessCheck = await checkServerAccess(req, req.params.name);
  if (!accessCheck.ok) return res.status(accessCheck.status).json({ error: accessCheck.error });

  if (!(await checkUserPerm(req, req.params.name, "files_rename"))) {
    return res.status(403).json({ error: "permission denied", permission: "files_rename" });
  }
  const rel = String(req.body.path || "");
  const newName = sanitizeName(req.body.newName || "");
  if (!newName) return res.status(400).json({ error: "invalid newName" });
  try {
    const full = safeJoinUnix(ctx.baseDir, rel);
    const { status, json } = await httpJson(
      nodeUrl(ctx.node, "/v1/fs/rename"),
      { method: "POST", headers: Object.assign({ "Content-Type": "application/json" }, nodeHeaders(ctx.node)), body: { path: full, newName } }
    );
    if (status !== 200 || !json || !json.ok) return res.status(502).json({ error: "node_rename_failed" });

    const userEmail = req.session?.user || "unknown";
    recordActivity(req.params.name, "file_rename", { src: rel, dest: newName }, userEmail, getRequestIp(req));

    return res.json({ ok: true });
  } catch (e) {
    return res.status(400).json({ error: maskPathErrorMessage(e) });
  }
});
app.post("/api/nodes/server/:name/extract", async (req, res) => {
  const ctx = await remoteContext(req.params.name);
  if (!ctx.exists) return res.status(404).json({ error: "server not found" });
  if (!ctx.remote || !ctx.node) return res.status(400).json({ error: "not_remote" });

  const accessCheck = await checkServerAccess(req, req.params.name);
  if (!accessCheck.ok) return res.status(accessCheck.status).json({ error: accessCheck.error });

  if (!(await checkUserPerm(req, req.params.name, "files_create"))) {
    return res.status(403).json({ error: "permission denied", permission: "files_create" });
  }
  const rel = String(req.body.path || "");
  try {
    const full = safeJoinUnix(ctx.baseDir, rel);
    const { status, json } = await httpJson(
      nodeUrl(ctx.node, "/v1/fs/extract"),
      { method: "POST", headers: Object.assign({ "Content-Type": "application/json" }, nodeHeaders(ctx.node)), body: { path: full } }
    );
    if (status === 507 && json?.error === "disk_limit_exceeded") {
      return res.status(507).json(json);
    }
    if (status !== 200 || !json || !json.ok) return res.status(502).json({ error: "node_extract_failed" });

    const userEmail = req.session?.user || "unknown";
    recordActivity(req.params.name, "file_extract", { path: rel }, userEmail, getRequestIp(req));

    return res.json({ ok: true, msg: json.msg || "Extracted" });
  } catch (e) {
    return res.status(400).json({ error: maskPathErrorMessage(e) });
  }
});
app.post("/api/nodes/server/:name/archive", async (req, res) => {
  const ctx = await remoteContext(req.params.name);
  if (!ctx.exists) return res.status(404).json({ error: "server not found" });
  if (!ctx.remote || !ctx.node) return res.status(400).json({ error: "not_remote" });

  const accessCheck = await checkServerAccess(req, req.params.name);
  if (!accessCheck.ok) return res.status(accessCheck.status).json({ error: accessCheck.error });

  if (!(await checkUserPerm(req, req.params.name, "files_archive"))) {
    return res.status(403).json({ error: "permission denied", permission: "files_archive" });
  }

  const paths = req.body.paths;
  const destDir = String(req.body.destDir || "");

  if (!Array.isArray(paths) || paths.length === 0) {
    return res.status(400).json({ error: "missing paths" });
  }

  try {
    const { status, json } = await httpJson(
      nodeUrl(ctx.node, `/v1/servers/${encodeURIComponent(req.params.name)}/files/archive`),
      { method: "POST", headers: Object.assign({ "Content-Type": "application/json" }, nodeHeaders(ctx.node)), body: { paths, destDir }, timeoutMs: 300000 }
    );
    if (status === 507 && json?.error === "disk_limit_exceeded") {
      return res.status(507).json(json);
    }
    if (status !== 200 || !json || !json.ok) {
      return res.status(502).json({ error: json?.error || "archive failed" });
    }

    const userEmail = req.session?.user || "unknown";
    recordActivity(req.params.name, "file_archive", { paths, destDir, archiveName: json.name }, userEmail, getRequestIp(req));

    return res.json({ ok: true, path: json.path, name: json.name });
  } catch (e) {
    return res.status(400).json({ error: maskPathErrorMessage(e) });
  }
});
app.post("/api/nodes/server/:name/upload", (req, res, next) => getNodeUpload().single("file")(req, res, next), async (req, res) => {
  const ctx = await remoteContext(req.params.name);
  if (!ctx.exists) return res.status(404).json({ error: "server not found" });
  if (!ctx.remote || !ctx.node) return res.status(400).json({ error: "not_remote" });

  const accessCheck = await checkServerAccess(req, req.params.name);
  if (!accessCheck.ok) return res.status(accessCheck.status).json({ error: accessCheck.error });

  if (!(await checkUserPerm(req, req.params.name, "files_upload"))) {
    return res.status(403).json({ error: "permission denied", permission: "files_upload" });
  }
  const rel = String(req.body.path || "");
  const file = req.file;
  if (!file) return res.status(400).json({ error: "no_file" });
  const tmpPath = file.path;

  const nodeMaxBytes = (ctx.node.max_upload_mb || 10240) * 1024 * 1024;
  if (file.size > nodeMaxBytes) {
    try { await fsp.unlink(tmpPath); } catch { }
    return res.status(413).json({
      error: "file_too_large",
      message: `File exceeds this node's upload limit of ${ctx.node.max_upload_mb || 10240} MB`,
      limit_mb: ctx.node.max_upload_mb || 10240
    });
  }

  try {
    const full = safeJoinUnix(ctx.baseDir, rel ? rel : "");

    const result = await streamUploadToNode(ctx.node, tmpPath, file.originalname, full, file.size);

    if (result.status === 507 && result.json?.error === "disk_limit_exceeded") {
      return res.status(507).json(result.json);
    }
    if (result.status !== 200 || !result.json || !result.json.ok) {
      return res.status(502).json({ error: "node_upload_failed", detail: result.json?.error });
    }

    const userEmail = req.session?.user || "unknown";
    recordActivity(req.params.name, "file_upload", { path: rel, filename: file.originalname, size: file.size }, userEmail, getRequestIp(req));

    return res.json({ ok: true, msg: result.json.msg || "Uploaded" });
  } catch (e) {
    return res.status(400).json({ error: maskPathErrorMessage(e) });
  } finally {
    if (tmpPath) {
      try { await fsp.unlink(tmpPath); } catch { }
    }
  }
});
async function startPayloadFor(name, hostPortFromReq) {
  const srv = await findServerByNameOrId(name);
  const chosenPort = Number.isFinite(hostPortFromReq)
    ? hostPortFromReq
    : (Number.isFinite(srv?.port) ? srv.port : undefined);
  const payload = {};
  if (Number.isFinite(chosenPort)) payload.hostPort = chosenPort;
  if (srv?.template) payload.templateId = srv.template;
  if (srv?.runtime) payload.runtime = srv.runtime;
  if (srv?.start) payload.start = srv.start;

  try {
    const subs = await db.query("SELECT domain FROM subdomains WHERE server_id = ? AND status = 'approved'", [srv.id]);
    if (subs.length > 0) {
      const joined = subs.map(s => s.domain).join(",");
      payload.env = { TRUSTED_SUBDOMAINS: joined };
    }
  } catch (e) {
    console.error("Failed to fetch subdomains for payload:", e);
  }

  return payload;
}
async function seedRuntimeOnNode(node, name, hostPortHint) {
  try {
    const srv = await findServerByNameOrId(name);
    const tpl = String(srv?.template || "").toLowerCase();
    const isMinecraft = tpl === "minecraft";
    const isRuntimeTemplate = ["nodejs", "python", "discord-bot"].includes(tpl);
    if (srv && tpl && !isMinecraft && isRuntimeTemplate) {
      const runtimePayload = {
        runtime: srv.runtime || {},
        template: tpl,
        start: srv.start || srv.startFile || srv.entry || null,
        port: srv.hostPort || srv.port || srv.server_port || hostPortHint || null,
        nodeId: node?.uuid || node?.id || null,
      };
      await callNodeApi(
        node,
        `/v1/servers/${encodeURIComponent(name)}/runtime`,
        "POST",
        runtimePayload,
        LONG_NODE_TIMEOUT_MS
      ).catch(() => { });
    }
  } catch (_) { }
}
app.post("/api/nodes/server/:name/action", async (req, res) => {
  try {
    const ctx = await remoteContext(req.params.name);
    if (!ctx.exists) return res.status(404).json({ error: "server not found" });
    if (!ctx.remote || !ctx.node) return res.status(400).json({ error: "not_remote" });

    const accessCheck = await checkServerAccess(req, req.params.name);
    if (!accessCheck.ok) return res.status(accessCheck.status).json({ error: accessCheck.error });

    const cmdRaw = String((req.body && (req.body.cmd || req.body.action)) || "").toLowerCase();
    const cmd = (cmdRaw === "run") ? "start" : cmdRaw;

    let requiredPerm = "server_start";
    if (cmd === "start" || cmd === "restart") requiredPerm = "server_start";
    else if (cmd === "stop" || cmd === "kill") requiredPerm = "server_stop";
    else if (cmd === "status") requiredPerm = null;


    if (requiredPerm && !(await checkUserPerm(req, req.params.name, requiredPerm))) {
      return res.status(403).json({ error: "permission denied", permission: requiredPerm });
    }
    const hostPort = Number(req.body && req.body.hostPort);
    let path = null, method = "POST", payload = null;
    if (cmd === "start") {
      try {
        if (ctx.node) await seedRuntimeOnNode(ctx.node, req.params.name, hostPort);
      } catch (_) {
      }
      path = `/v1/servers/${encodeURIComponent(req.params.name)}/start`;
      payload = await startPayloadFor(req.params.name, hostPort);
    } else if (cmd === "stop") {
      path = `/v1/servers/${encodeURIComponent(req.params.name)}/stop`;
    } else if (cmd === "kill") {
      path = `/v1/servers/${encodeURIComponent(req.params.name)}/kill`;
    } else if (cmd === "restart") {
      path = `/v1/servers/${encodeURIComponent(req.params.name)}/restart`;
    } else if (cmd === "status") {
      method = "GET";
      path = `/v1/servers/${encodeURIComponent(req.params.name)}`;
    } else {
      return res.status(400).json({ error: "invalid_cmd" });
    }

    if (cmd !== "status") {
      const actionMap = { start: "server_start", stop: "server_stop", restart: "server_restart", kill: "server_kill" };
      const actionName = actionMap[cmd] || `server_${cmd}`;
      const userEmail = req.session?.user || "unknown";
      recordActivity(req.params.name, actionName, null, userEmail, getRequestIp(req));
    }

    const timeoutMs = cmd === "start" ? LONG_NODE_TIMEOUT_MS : DEFAULT_NODE_TIMEOUT_MS;
    const { status, json } = await callNodeApi(ctx.node, path, method, payload, timeoutMs);
    if (status === 200 && json) return res.json(json);
    return res.status(502).json({ error: "node_action_failed", status, detail: json && json.error });
  } catch (e) {
    return res.status(500).json({ error: "bridge_failed", detail: e && e.message });
  }
});
app.post("/api/nodes/server/:name/command", async (req, res) => {
  try {
    const ctx = await remoteContext(req.params.name);
    if (!ctx.exists) return res.status(404).json({ error: "server not found" });
    if (!ctx.remote || !ctx.node) return res.status(400).json({ error: "not_remote" });

    const accessCheck = await checkServerAccess(req, req.params.name);
    if (!accessCheck.ok) return res.status(accessCheck.status).json({ error: accessCheck.error });

    if (!(await checkUserPerm(req, req.params.name, "console_write"))) {
      return res.status(403).json({ error: "permission denied", permission: "console_write" });
    }
    const command = String((req.body && req.body.command) || "").trim();
    if (!command) return res.status(400).json({ error: "empty_command" });

    const userEmail = req.session?.user || "unknown";
    recordActivity(req.params.name, "console_command", { command }, userEmail, getRequestIp(req));

    const { status, json } = await callNodeApi(
      ctx.node,
      `/v1/servers/${encodeURIComponent(req.params.name)}/command`,
      "POST",
      { command }
    );
    if (status === 200 && json && json.ok) return res.json(json);
    return res.status(502).json({ error: "node_command_failed", status, detail: json && json.error });
  } catch (e) {
    return res.status(500).json({ error: "bridge_failed", detail: e && e.message });
  }
});

app.post("/api/nodes/server/:name/extract-image", async (req, res) => {
  try {
    const ctx = await remoteContext(req.params.name);
    if (!ctx.exists) return res.status(404).json({ error: "server not found" });
    if (!ctx.remote || !ctx.node) return res.status(400).json({ error: "not_remote" });

    const payload = {
      image: req.body?.image || "",
      tag: req.body?.tag || "",
      extractPath: req.body?.extractPath || "",
      paths: Array.isArray(req.body?.paths) ? req.body.paths : []
    };

    const userEmail = req.session?.user || "unknown";
    recordActivity(req.params.name, "extract_image", { image: payload.image || "(from meta)" }, userEmail, getRequestIp(req));

    const { status, json } = await callNodeApi(
      ctx.node,
      `/v1/servers/${encodeURIComponent(req.params.name)}/extract-image`,
      "POST",
      payload,
      LONG_NODE_TIMEOUT_MS
    );

    if (status === 507 && json?.error === "disk_limit_exceeded") {
      return res.status(507).json(json);
    }
    if (status === 200 && json) return res.json(json);
    return res.status(502).json({ error: "extract_failed", status, detail: json && json.error });
  } catch (e) {
    return res.status(500).json({ error: "bridge_failed", detail: e && e.message });
  }
});

app.post("/api/nodes/:id/server/action", async (req, res) => {
  try {
    const node = await findNodeByIdOrName(req.params.id);
    if (!node) return res.status(404).json({ error: "not found" });
    const name = String((req.body && req.body.name) || "").trim();
    const cmd = String((req.body && req.body.cmd) || "").trim().toLowerCase();
    const finalCmd = cmd === "run" ? "start" : cmd;
    const hostPort = req.body && req.body.hostPort ? Number(req.body.hostPort) : undefined;
    if (!name || !cmd) return res.status(400).json({ error: "missing name/cmd" });

    const accessCheck = await checkServerAccess(req, name);
    if (!accessCheck.ok) return res.status(accessCheck.status).json({ error: accessCheck.error });

    let requiredPerm = "server_start";
    if (finalCmd === "start" || finalCmd === "restart") requiredPerm = "server_start";
    else if (finalCmd === "stop" || finalCmd === "kill") requiredPerm = "server_stop";
    else if (finalCmd === "status") requiredPerm = null;

    if (requiredPerm && !(await checkUserPerm(req, name, requiredPerm))) {
      return res.status(403).json({ error: "permission denied", permission: requiredPerm });
    }

    let path = null;
    let method = "POST";
    let payload = null;
    if (finalCmd === "start") {
      try {
        await seedRuntimeOnNode(node, name, hostPort);
      } catch (_) {
      }
      path = `/v1/servers/${encodeURIComponent(name)}/start`;
      payload = await startPayloadFor(name, hostPort);
    } else if (finalCmd === "stop") {
      path = `/v1/servers/${encodeURIComponent(name)}/stop`;
    } else if (finalCmd === "kill") {
      path = `/v1/servers/${encodeURIComponent(name)}/kill`;
    } else if (finalCmd === "restart") {
      path = `/v1/servers/${encodeURIComponent(name)}/restart`;
    } else if (finalCmd === "status") {
      method = "GET";
      path = `/v1/servers/${encodeURIComponent(name)}`;
    } else {
      return res.status(400).json({ error: "invalid cmd" });
    }

    if (finalCmd !== "status") {
      const actionMap = { start: "server_start", stop: "server_stop", restart: "server_restart", kill: "server_kill" };
      const actionName = actionMap[finalCmd] || `server_${finalCmd}`;
      const userEmail = req.session?.user || "unknown";
      recordActivity(name, actionName, null, userEmail, getRequestIp(req));
    }

    const timeoutMs = finalCmd === "start" ? LONG_NODE_TIMEOUT_MS : DEFAULT_NODE_TIMEOUT_MS;
    const { status, json } = await callNodeApi(node, path, method, payload, timeoutMs);
    if (status === 200 && json && (json.ok === true || json.ok === undefined)) {
      return res.json(json || { ok: true });
    }
    return res.status(502).json({ error: "node_action_failed", detail: `HTTP ${status}`, response: json });
  } catch (e) {
    return res.status(500).json({ error: "bridge_failed", detail: e && e.message });
  }
});
app.post("/api/nodes/:id/server/command", async (req, res) => {
  try {
    const node = await findNodeByIdOrName(req.params.id);
    if (!node) return res.status(404).json({ error: "not found" });
    const name = String((req.body && req.body.name) || "").trim();
    const command = String((req.body && req.body.command) || "").trim();
    if (!name || !command) return res.status(400).json({ error: "missing name/command" });

    const accessCheck = await checkServerAccess(req, name);
    if (!accessCheck.ok) return res.status(accessCheck.status).json({ error: accessCheck.error });

    if (!(await checkUserPerm(req, name, "console_write"))) {
      return res.status(403).json({ error: "permission denied", permission: "console_write" });
    }

    const userEmail = req.session?.user || "unknown";
    recordActivity(name, "console_command", { command }, userEmail, getRequestIp(req));

    const { status, json } = await callNodeApi(
      node,
      `/v1/servers/${encodeURIComponent(name)}/command`,
      "POST",
      { command }
    );
    if (status === 200 && json && json.ok) return res.json(json);
    return res.status(502).json({ error: "node_action_failed", detail: `HTTP ${status}`, response: json });
  } catch (e) {
    return res.status(500).json({ error: "bridge_failed", detail: e && e.message });
  }
});
app.post("/api/nodes/server/:name/create", async (req, res) => {
  try {
    const ctx = await remoteContext(req.params.name);
    if (!ctx.exists) return res.status(404).json({ error: "server not found" });
    if (!ctx.remote || !ctx.node) return res.status(400).json({ error: "not_remote" });

    const accessCheck = await checkServerAccess(req, req.params.name);
    if (!accessCheck.ok) return res.status(accessCheck.status).json({ error: accessCheck.error });

    if (!(await checkUserPerm(req, req.params.name, "files_create"))) {
      return res.status(403).json({ error: "permission denied", permission: "files_create" });
    }
    const typeRaw = String((req.body && req.body.type) || "").toLowerCase();
    const nameRaw = String((req.body && req.body.name) || "");
    const relPath = String((req.body && req.body.path) || "");
    if (typeRaw !== "file" && typeRaw !== "folder") return res.status(400).json({ error: "invalid_type" });
    const safeName = sanitizeName(nameRaw);
    if (!safeName) return res.status(400).json({ error: "invalid_name" });
    try {
      const relativePosix = path.posix.join(relPath || "", safeName);
      const target = safeJoinUnix(ctx.baseDir, relativePosix);
      const payload = typeRaw === "folder"
        ? { path: safeJoinUnix(target, ".keep"), content: "", encoding: "utf8" }
        : { path: target, content: "", encoding: "utf8" };
      const { status, json } = await httpJson(
        nodeUrl(ctx.node, "/v1/fs/write"),
        { method: "POST", headers: Object.assign({ "Content-Type": "application/json" }, nodeHeaders(ctx.node)), body: payload }
      );
      if (status === 507 && json?.error === "disk_limit_exceeded") {
        return res.status(507).json(json);
      }
      if (status !== 200 || !json || !json.ok) {
        const errMsg = json?.error || '';
        if (errMsg.includes('exist') || errMsg.includes('EEXIST')) {
          return res.status(400).json({ error: "already_exists", message: `A ${typeRaw} with that name already exists` });
        }
        return res.status(502).json({ error: "node_create_failed", message: `Failed to create ${typeRaw}. It may already exist.` });
      }

      const userEmail = req.session?.user || "unknown";
      const actionType = typeRaw === "folder" ? "folder_create" : "file_create";
      recordActivity(req.params.name, actionType, { path: relativePosix, type: typeRaw }, userEmail, getRequestIp(req));

      return res.json({ ok: true, path: relativePosix });
    } catch (e) {
      return res.status(400).json({ error: maskPathErrorMessage(e) });
    }
  } catch (e) {
    return res.status(500).json({ error: "bridge_failed", detail: e && e.message });
  }
});


app.get("/api/nodes/server/:name/backups", async (req, res) => {
  try {
    const serverName = req.params.name;
    if (!(await checkUserPerm(req, serverName, "backups_view"))) {
      return res.status(403).json({ error: "forbidden", detail: "You do not have permission to view backups" });
    }

    const ctx = await remoteContext(serverName);
    if (!ctx.exists) return res.status(404).json({ error: "server not found", detail: "Server does not exist in database" });
    if (!ctx.remote || !ctx.node) return res.status(400).json({ error: "not_remote", detail: "Server is not associated with a node" });

    const url = nodeUrl(ctx.node, `/v1/servers/${encodeURIComponent(serverName)}/backups`);
    const { status, json, text } = await httpJson(url, { method: "GET", headers: nodeHeaders(ctx.node), timeoutMs: 15000 });

    if (status === 0) {
      return res.status(502).json({ error: "node_unreachable", detail: "Could not connect to node agent" });
    }
    if (status !== 200 || !json || !json.ok) {
      return res.status(502).json({ error: "node_backup_list_failed", detail: json?.error || text || `status ${status}` });
    }
    return res.json({ ok: true, backups: json.backups || [] });
  } catch (e) {
    console.error("[backups] List error:", e);
    return res.status(500).json({ error: "bridge_failed", detail: e?.message });
  }
});

app.post("/api/nodes/server/:name/backups", async (req, res) => {
  try {
    const serverName = req.params.name;
    if (!(await checkUserPerm(req, serverName, "backups_create"))) {
      return res.status(403).json({ error: "forbidden", detail: "You do not have permission to create backups" });
    }

    const ctx = await remoteContext(serverName);
    if (!ctx.exists) return res.status(404).json({ error: "server not found", detail: "Server does not exist in database" });
    if (!ctx.remote || !ctx.node) return res.status(400).json({ error: "not_remote", detail: "Server is not associated with a node" });

    const { name: backupName, description } = req.body || {};

    const url = nodeUrl(ctx.node, `/v1/servers/${encodeURIComponent(serverName)}/backups`);
    const { status, json, text } = await httpJson(url, {
      method: "POST",
      headers: Object.assign({ "Content-Type": "application/json" }, nodeHeaders(ctx.node)),
      body: { name: backupName, description },
      timeoutMs: 120000
    });

    if (status === 0) {
      return res.status(502).json({ error: "node_unreachable", detail: "Could not connect to node agent" });
    }
    if (status === 507 && json?.error === "disk_limit_exceeded") {
      return res.status(507).json(json);
    }
    if (status !== 200 || !json || !json.ok) {
      return res.status(502).json({ error: "node_backup_create_failed", detail: json?.error || text || `status ${status}` });
    }

    const userEmail = req.session?.user || "unknown";
    const userIp = getRequestIp(req);
    recordActivity(serverName, "backup_create", { backupId: json.backup?.id, name: json.backup?.name }, userEmail, userIp);

    return res.json({ ok: true, backup: json.backup });
  } catch (e) {
    console.error("[backups] Create error:", e);
    return res.status(500).json({ error: "bridge_failed", detail: e?.message });
  }
});

app.post("/api/nodes/server/:name/backups/:backupId/restore", async (req, res) => {
  try {
    const serverName = req.params.name;
    if (!(await checkUserPerm(req, serverName, "backups_create"))) {
      return res.status(403).json({ error: "forbidden", detail: "You do not have permission to restore backups" });
    }

    const ctx = await remoteContext(serverName);
    if (!ctx.exists) return res.status(404).json({ error: "server not found", detail: "Server does not exist in database" });
    if (!ctx.remote || !ctx.node) return res.status(400).json({ error: "not_remote", detail: "Server is not associated with a node" });

    const backupId = String(req.params.backupId || "").trim();
    if (!backupId) return res.status(400).json({ error: "missing backup id" });

    const { deleteOldFiles } = req.body || {};

    const url = nodeUrl(ctx.node, `/v1/servers/${encodeURIComponent(serverName)}/backups/${encodeURIComponent(backupId)}/restore`);
    const { status, json, text } = await httpJson(url, {
      method: "POST",
      headers: Object.assign({ "Content-Type": "application/json" }, nodeHeaders(ctx.node)),
      body: { deleteOldFiles: !!deleteOldFiles },
      timeoutMs: 300000
    });

    if (status === 0) {
      return res.status(502).json({ error: "node_unreachable", detail: "Could not connect to node agent" });
    }
    if (status === 507 && json?.error === "disk_limit_exceeded") {
      return res.status(507).json(json);
    }
    if (status !== 200 || !json || !json.ok) {
      return res.status(502).json({ error: "node_backup_restore_failed", detail: json?.error || text || `status ${status}` });
    }

    const userEmail = req.session?.user || "unknown";
    const userIp = getRequestIp(req);
    recordActivity(serverName, "backup_restore", { backupId, deleteOldFiles: !!deleteOldFiles }, userEmail, userIp);

    return res.json({ ok: true });
  } catch (e) {
    console.error("[backups] Restore error:", e);
    return res.status(500).json({ error: "bridge_failed", detail: e?.message });
  }
});

app.delete("/api/nodes/server/:name/backups/:backupId", async (req, res) => {
  try {
    const serverName = req.params.name;
    if (!(await checkUserPerm(req, serverName, "backups_delete"))) {
      return res.status(403).json({ error: "forbidden", detail: "You do not have permission to delete backups" });
    }

    const ctx = await remoteContext(serverName);
    if (!ctx.exists) return res.status(404).json({ error: "server not found", detail: "Server does not exist in database" });
    if (!ctx.remote || !ctx.node) return res.status(400).json({ error: "not_remote", detail: "Server is not associated with a node" });

    const backupId = String(req.params.backupId || "").trim();
    if (!backupId) return res.status(400).json({ error: "missing backup id" });

    const url = nodeUrl(ctx.node, `/v1/servers/${encodeURIComponent(serverName)}/backups/${encodeURIComponent(backupId)}`);
    const { status, json, text } = await httpJson(url, {
      method: "DELETE",
      headers: nodeHeaders(ctx.node),
      timeoutMs: 30000
    });

    if (status === 0) {
      return res.status(502).json({ error: "node_unreachable", detail: "Could not connect to node agent" });
    }
    if (status !== 200 || !json || !json.ok) {
      return res.status(502).json({ error: "node_backup_delete_failed", detail: json?.error || text || `status ${status}` });
    }

    const userEmail = req.session?.user || "unknown";
    const userIp = getRequestIp(req);
    recordActivity(serverName, "backup_delete", { backupId }, userEmail, userIp);

    return res.json({ ok: true });
  } catch (e) {
    console.error("[backups] Delete error:", e);
    return res.status(500).json({ error: "bridge_failed", detail: e?.message });
  }
});

app.get("/api/nodes/server/:name/logs", async (req, res) => {
  try {
    const serverName = req.params.name;

    const accessCheck = await checkServerAccess(req, serverName);
    if (!accessCheck.ok) {
      return res.status(accessCheck.status).json({ error: accessCheck.error });
    }

    if (!(await checkUserPerm(req, serverName, "console_read"))) {
      return res.status(403).json({ error: "permission denied", permission: "console_read" });
    }

    const { node } = await resolveNodeForServer(serverName);
    if (!node) return res.status(404).json({ error: "server_or_node_not_found" });
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache, no-transform");
    res.setHeader("Connection", "keep-alive");
    res.flushHeaders?.();
    const base = buildNodeBaseUrl(node.address, node.api_port, node.ssl_enabled);
    const fullUrl = `${base}/v1/servers/${encodeURIComponent(serverName)}/logs`;
    const isHttps = fullUrl.startsWith("https:");
    const lib = isHttps ? https : http;
    const nreq = lib.request(fullUrl, {
      method: "GET",
      headers: { "Authorization": `Bearer ${node.token}` }
    }, (nres) => {
      nres.on("data", (chunk) => { try { res.write(chunk); } catch { } });
      nres.on("end", () => { try { res.end(); } catch { } });
    });
    nreq.on("error", () => { try { res.end(); } catch { } });
    nreq.end();
    req.on("close", () => { try { nreq.destroy(); } catch { } });
  } catch {
    try { res.end(); } catch { }
  }
});
if (require.main === module) {
  app.listen(PORT, () => console.log(`[nodes.js] Nodes API on :${PORT}`));
}

module.exports = app;
module.exports.remoteContext = remoteContext;
module.exports.httpJson = httpJson;
module.exports.nodeUrl = nodeUrl;
module.exports.nodeHeaders = nodeHeaders;
module.exports.recordActivity = recordActivity;
module.exports.setWebhookDispatcher = setWebhookDispatcher;
module.exports.setPermissionCheckers = setPermissionCheckers;
