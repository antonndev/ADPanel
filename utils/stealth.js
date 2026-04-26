"use strict";

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const STEALTH_CONFIG_PATH = path.join(__dirname, "..", "data", "stealth.json");
const DEFAULT_COOKIE_NAME = "adpanel_gate";
const DEFAULT_STEALTH_CONFIG = {
  enabled: true,
  cookieName: DEFAULT_COOKIE_NAME,
  cookieSecret: "",
  cookieTtlDays: 30,
  challengeSecret: "",
  htmlVariantSalt: "",
  createdAt: null,
  rotatedAt: null,
};

function randomToken(bytes = 32) {
  return crypto.randomBytes(bytes).toString("base64url");
}

function sanitizeCookieName(value) {
  const raw = String(value || "").trim();
  if (!raw) return DEFAULT_COOKIE_NAME;
  return /^[A-Za-z0-9._-]{3,64}$/.test(raw) ? raw : DEFAULT_COOKIE_NAME;
}

function normalizeStealthConfig(input = {}) {
  const now = new Date().toISOString();
  const normalized = {
    ...DEFAULT_STEALTH_CONFIG,
    ...(input && typeof input === "object" ? input : {}),
  };

  normalized.enabled = normalized.enabled !== false;
  normalized.cookieName = sanitizeCookieName(normalized.cookieName);
  normalized.cookieSecret = String(normalized.cookieSecret || "").trim() || randomToken(48);

  const ttlDays = parseInt(String(normalized.cookieTtlDays || "").trim(), 10);
  normalized.cookieTtlDays = Number.isFinite(ttlDays) && ttlDays > 0 ? Math.min(ttlDays, 365) : 30;

  normalized.challengeSecret = String(normalized.challengeSecret || "").trim() || randomToken(48);
  normalized.htmlVariantSalt = String(normalized.htmlVariantSalt || "").trim() || randomToken(24);
  normalized.createdAt = String(normalized.createdAt || "").trim() || now;
  normalized.rotatedAt = String(normalized.rotatedAt || "").trim() || normalized.createdAt;

  return normalized;
}

function readStealthConfigFile() {
  try {
    if (!fs.existsSync(STEALTH_CONFIG_PATH)) return null;
    const raw = fs.readFileSync(STEALTH_CONFIG_PATH, "utf8");
    return JSON.parse(raw);
  } catch (err) {
    console.warn("[stealth] Failed to read config:", err.message);
    return null;
  }
}

function writeStealthConfigFile(config) {
  const normalized = normalizeStealthConfig(config);
  const dir = path.dirname(STEALTH_CONFIG_PATH);
  fs.mkdirSync(dir, { recursive: true });
  const tmpPath = `${STEALTH_CONFIG_PATH}.tmp`;
  fs.writeFileSync(tmpPath, JSON.stringify(normalized, null, 2), { mode: 0o600 });
  fs.renameSync(tmpPath, STEALTH_CONFIG_PATH);
  return normalized;
}

function loadStealthConfig() {
  const existing = readStealthConfigFile();
  if (!existing) {
    return writeStealthConfigFile(DEFAULT_STEALTH_CONFIG);
  }

  const normalized = normalizeStealthConfig(existing);
  if (JSON.stringify(existing) !== JSON.stringify(normalized)) {
    return writeStealthConfigFile(normalized);
  }
  return normalized;
}

function saveStealthConfig(partial) {
  const current = loadStealthConfig();
  return writeStealthConfigFile({
    ...current,
    ...(partial && typeof partial === "object" ? partial : {}),
  });
}

function ensureStealthConfig(defaults = {}) {
  const current = loadStealthConfig();
  const merged = normalizeStealthConfig({
    ...current,
    ...(defaults && typeof defaults === "object" ? defaults : {}),
  });
  if (JSON.stringify(current) !== JSON.stringify(merged)) {
    return writeStealthConfigFile(merged);
  }
  return merged;
}

module.exports = {
  DEFAULT_COOKIE_NAME,
  DEFAULT_STEALTH_CONFIG,
  STEALTH_CONFIG_PATH,
  ensureStealthConfig,
  loadStealthConfig,
  saveStealthConfig,
  sanitizeCookieName,
};
