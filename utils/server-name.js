"use strict";

function sanitizeServerToken(raw) {
  let name = String(raw || "").trim();
  if (!name) return "";
  if (name.includes("..") || /[\/\\]/.test(name) || name.length > 120) return "";
  name = name.replace(/\s+/g, "-").replace(/[^\w\-_.]/g, "").replace(/^-+|-+$/g, "");
  return name;
}

function getCanonicalServerName(entry, fallback = "") {
  return sanitizeServerToken(entry?.name || fallback || "");
}

function getTechnicalServerName(entry, fallback = "") {
  const technical = sanitizeServerToken(entry?.bot || "");
  if (technical) return technical;
  return getCanonicalServerName(entry, fallback);
}

function escapeRegExp(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function rewriteStartupCommandServerName(command, oldName, newName) {
  const cleanCommand = String(command || "").trim();
  const cleanOldName = sanitizeServerToken(oldName);
  const cleanNewName = sanitizeServerToken(newName);

  if (!cleanCommand || !cleanOldName || !cleanNewName || cleanOldName === cleanNewName) {
    return cleanCommand;
  }

  const escapedOld = escapeRegExp(cleanOldName);
  const patterns = [
    new RegExp(`(^|\\s)(--name\\s+)(["']?)${escapedOld}\\3(?=\\s|$)`, "g"),
    new RegExp(`(^|\\s)(--name=)(["']?)${escapedOld}\\3(?=\\s|$)`, "g"),
    new RegExp(`(^|\\s)(container_name=)(["']?)${escapedOld}\\3(?=\\s|$)`, "g"),
  ];

  let rewritten = cleanCommand;
  for (const pattern of patterns) {
    rewritten = rewritten.replace(pattern, (_match, prefix, flag, quote) => {
      const safeQuote = quote || "";
      return `${prefix}${flag}${safeQuote}${cleanNewName}${safeQuote}`;
    });
  }

  return rewritten;
}

module.exports = {
  getCanonicalServerName,
  getTechnicalServerName,
  rewriteStartupCommandServerName,
  sanitizeServerToken,
};
