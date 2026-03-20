"use strict";

const crypto = require("crypto");
const httpMod = require("http");
const fs = require("fs");
const path = require("path");
const ipRangeCheck = require("ip-range-check");

// ─── Configuration ──────────────────────────────────────────────
const TOKEN_TTL_MS = 30 * 60 * 1000; // 30 minutes
const TOKEN_BYTES = 16;
const CLEANUP_INTERVAL_MS = 60 * 1000; // sweep every 60s
const TOKEN_PRESENCE_TTL_MS = 45 * 1000; // allow refresh/navigation within the same tab
const ACCESS_HEARTBEAT_INTERVAL_MS = 15 * 1000;
const MAX_SESSIONS = 500; // hard cap to prevent RAM bloat
const PHPMYADMIN_INTERNAL_PORT = 9082;

// ─── Token Store (self-cleaning Map) ────────────────────────────
const sessions = new Map();

// Periodic sweep for expired sessions
const _cleanupTimer = setInterval(() => {
  const now = Date.now();
  for (const [token, sess] of sessions) {
    if (now > sess.expiresAt || isSessionPresenceExpired(sess, now)) {
      clearTimeout(sess._timer);
      sessions.delete(token);
    }
  }
}, CLEANUP_INTERVAL_MS);
if (_cleanupTimer.unref) _cleanupTimer.unref();

// ─── Service Targets ────────────────────────────────────────────
const SERVICE_TARGETS = {
  phpmyadmin: {
    host: "127.0.0.1",
    port: PHPMYADMIN_INTERNAL_PORT,
    backendPath: "/phpmyadmin",
    label: "phpMyAdmin"
  },
  pgadmin: {
    host: "127.0.0.1",
    port: 5050,
    backendPath: "/pgadmin4",
    label: "pgAdmin4"
  }
};

// ─── Helpers ────────────────────────────────────────────────────

function normalizeIp(ip) {
  if (!ip || typeof ip !== "string") return "";
  let normalized = ip.trim();
  if (normalized.startsWith("::ffff:")) normalized = normalized.slice(7);
  return normalized;
}

function loadTrustedSubnets() {
  try {
    const secPath = path.join(__dirname, "..", "security.json");
    const sec = JSON.parse(fs.readFileSync(secPath, "utf8"));
    if (Array.isArray(sec.trusted_subnets)) {
      return sec.trusted_subnets.filter(s => typeof s === "string" && s.length > 0);
    }
  } catch { /* ignore */ }
  return [];
}

function isIpAuthorized(requestIp, sessionIp) {
  const normReq = normalizeIp(requestIp);
  const normSess = normalizeIp(sessionIp);

  if (normReq === normSess) return true;

  const subnets = loadTrustedSubnets();
  if (subnets.length > 0) {
    const reqInSubnet = ipRangeCheck(normReq, subnets);
    const sessInSubnet = ipRangeCheck(normSess, subnets);
    if (reqInSubnet && sessInSubnet) return true;
  }

  return false;
}

function isSessionPresenceExpired(sess, now = Date.now()) {
  return Boolean(sess && sess.presenceExpiresAt > 0 && now > sess.presenceExpiresAt);
}

function markSessionPresent(sess) {
  if (!sess) return;
  const now = Date.now();
  sess.lastSeenAt = now;
  sess.presenceExpiresAt = now + TOKEN_PRESENCE_TTL_MS;
}

function buildPresenceScript(token) {
  const heartbeatUrl = "/api/settings/database/access-heartbeat";
  const heartbeatPayload = JSON.stringify({ token });

  return [
    "<script>",
    "(function(){",
    "var heartbeatUrl=" + JSON.stringify(heartbeatUrl) + ";",
    "var heartbeatPayload=" + JSON.stringify(heartbeatPayload) + ";",
    "function sendHeartbeat(useBeacon){",
    "try{",
    "if(useBeacon && navigator.sendBeacon){",
    "var blob=new Blob([heartbeatPayload],{type:'application/json'});",
    "navigator.sendBeacon(heartbeatUrl,blob);",
    "return;",
    "}",
    "fetch(heartbeatUrl,{method:'POST',headers:{'Content-Type':'application/json'},body:heartbeatPayload,credentials:'same-origin',keepalive:true}).catch(function(){});",
    "}catch(err){}",
    "}",
    "sendHeartbeat(false);",
    "setInterval(function(){sendHeartbeat(false);}," + ACCESS_HEARTBEAT_INTERVAL_MS + ");",
    "window.addEventListener('pageshow',function(){sendHeartbeat(false);});",
    "window.addEventListener('pagehide',function(){sendHeartbeat(true);});",
    "document.addEventListener('visibilitychange',function(){if(document.visibilityState==='visible'){sendHeartbeat(false);}});",
    "})();",
    "</script>"
  ].join("");
}

function injectPresenceScriptIntoHtml(html, token) {
  const script = buildPresenceScript(token);
  if (/<\/body>/i.test(html)) return html.replace(/<\/body>/i, script + "</body>");
  if (/<\/head>/i.test(html)) return html.replace(/<\/head>/i, script + "</head>");
  return html + script;
}

// ─── Token Management ───────────────────────────────────────────

function generateToken(service, userIp, username) {
  if (!SERVICE_TARGETS[service]) return null;

  if (sessions.size >= MAX_SESSIONS) {
    let oldestToken = null, oldestTime = Infinity;
    for (const [t, s] of sessions) {
      if (s.createdAt < oldestTime) { oldestTime = s.createdAt; oldestToken = t; }
    }
    if (oldestToken) {
      clearTimeout(sessions.get(oldestToken)._timer);
      sessions.delete(oldestToken);
    }
  }

  const token = "v-tmp-" + crypto.randomBytes(TOKEN_BYTES).toString("hex");
  const now = Date.now();
  const expiresAt = now + TOKEN_TTL_MS;

  const _timer = setTimeout(() => {
    sessions.delete(token);
  }, TOKEN_TTL_MS);
  if (_timer.unref) _timer.unref();

  sessions.set(token, {
    service,
    userIp: normalizeIp(userIp),
    username,
    createdAt: now,
    expiresAt,
    lastSeenAt: 0,
    presenceExpiresAt: 0,
    _timer
  });

  return { token, expiresAt };
}

function validateToken(token) {
  if (!token || typeof token !== "string") return null;
  const sess = sessions.get(token);
  if (!sess) return null;
  if (Date.now() > sess.expiresAt || isSessionPresenceExpired(sess)) {
    clearTimeout(sess._timer);
    sessions.delete(token);
    return null;
  }
  return sess;
}

function touchToken(token) {
  const sess = validateToken(token);
  if (!sess) return false;
  markSessionPresent(sess);
  return true;
}

function revokeToken(token) {
  const sess = sessions.get(token);
  if (sess) {
    clearTimeout(sess._timer);
    sessions.delete(token);
  }
}

function revokeAllForUser(username) {
  for (const [token, sess] of sessions) {
    if (sess.username === username) {
      clearTimeout(sess._timer);
      sessions.delete(token);
    }
  }
}

// ─── Streaming Proxy Middleware ─────────────────────────────────

function rewriteCookiePaths(headers, backendPath, tokenPath) {
  const raw = headers["set-cookie"];
  if (!raw) return;
  const cookies = Array.isArray(raw) ? raw : [raw];
  headers["set-cookie"] = cookies.map(c =>
    c.replace(new RegExp("path=" + backendPath.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + "/?", "gi"), "path=" + tokenPath)
  );
}

function stripUpstreamSecurityHeaders(headers) {
  // Remove backend security headers that conflict with our proxy context
  delete headers["x-frame-options"];
  delete headers["content-security-policy"];
  delete headers["x-content-security-policy"];
  delete headers["x-webkit-csp"];
  delete headers["x-xss-protection"];
  delete headers["x-robots-tag"];
  delete headers["x-permitted-cross-domain-policies"];
}

function createProxyMiddleware() {
  const DB_ACCESS_PREFIX = "/db-access/";

  return function dbProxyMiddleware(req, res, next) {
    if (!req.url.startsWith(DB_ACCESS_PREFIX)) return next();

    // Parse: /db-access/<token>/...
    const withoutPrefix = req.url.slice(DB_ACCESS_PREFIX.length);
    const slashIdx = withoutPrefix.indexOf("/");
    const token = slashIdx === -1 ? withoutPrefix : withoutPrefix.slice(0, slashIdx);
    const remainingPath = slashIdx === -1 ? "/" : withoutPrefix.slice(slashIdx);

    const sess = validateToken(token);
    if (!sess) { res.status(404).end(); return; }

    const requestIp = req.ip || req.connection?.remoteAddress || "";
    if (!isIpAuthorized(requestIp, sess.userIp)) { res.status(404).end(); return; }

    const target = SERVICE_TARGETS[sess.service];
    if (!target) { res.status(404).end(); return; }

    if (sess.presenceExpiresAt > 0) {
      markSessionPresent(sess);
    }

    const tokenPath = DB_ACCESS_PREFIX + token + "/";
    const proxyPath = target.backendPath + (remainingPath === "/" ? "/" : remainingPath);

    // Rewrite cookies coming from the browser: change tokenPath back to backendPath
    const incomingCookie = req.headers["cookie"];
    if (incomingCookie) {
      req.headers["cookie"] = incomingCookie;
    }

    const proxyHeaders = Object.assign({}, req.headers);
    delete proxyHeaders["connection"];
    delete proxyHeaders["upgrade"];
    if (sess.service === "phpmyadmin") {
      proxyHeaders["host"] = "127.0.0.1:" + PHPMYADMIN_INTERNAL_PORT;
    }

    // Request uncompressed content for text so we can rewrite paths
    const likelyText = req.method === "GET" && (
      remainingPath === "/" ||
      remainingPath.endsWith(".php") ||
      remainingPath.endsWith(".html") ||
      !remainingPath.includes(".")
    );
    if (likelyText) {
      proxyHeaders["accept-encoding"] = "identity";
    }

    const proxyOpts = {
      hostname: target.host,
      port: target.port,
      path: proxyPath,
      method: req.method,
      headers: proxyHeaders,
      timeout: 300000
    };

    const proxyReq = httpMod.request(proxyOpts, (proxyRes) => {
      // Rewrite Location headers
      const location = proxyRes.headers["location"];
      if (location) {
        let rewritten = location;
        if (sess.service === "phpmyadmin" && location.includes("/phpmyadmin")) {
          rewritten = location.replace(/\/phpmyadmin/g, DB_ACCESS_PREFIX + token);
        } else if (sess.service === "pgadmin") {
          if (location.includes("/pgadmin4")) {
            rewritten = location.replace(/\/pgadmin4/g, DB_ACCESS_PREFIX + token);
          } else if (location.includes("/pgadmin")) {
            rewritten = location.replace(/\/pgadmin/g, DB_ACCESS_PREFIX + token);
          }
        }
        proxyRes.headers["location"] = rewritten;
      }

      // Rewrite Set-Cookie paths so cookies work on the proxy URL
      rewriteCookiePaths(proxyRes.headers, target.backendPath, tokenPath);
      // Remove upstream security headers to avoid conflicts with our proxy
      stripUpstreamSecurityHeaders(proxyRes.headers);

      // Clear any headers previously set by Express middleware (helmet, etc.)
      // to prevent them from leaking into the proxied response
      const prevHeaders = res.getHeaderNames ? res.getHeaderNames() : [];
      for (const h of prevHeaders) res.removeHeader(h);

      const respContentType = (proxyRes.headers["content-type"] || "").toLowerCase();
      const isTextContent = respContentType.includes("text/html") || respContentType.includes("javascript") || respContentType.includes("text/css");

      if (isTextContent) {
        delete proxyRes.headers["content-length"];
        const encoding = proxyRes.headers["content-encoding"];
        delete proxyRes.headers["content-encoding"];

        res.writeHead(proxyRes.statusCode, proxyRes.headers);

        const chunks = [];
        let totalSize = 0;
        const MAX_REWRITE_SIZE = 5 * 1024 * 1024;

        proxyRes.on("data", (chunk) => {
          totalSize += chunk.length;
          if (totalSize <= MAX_REWRITE_SIZE) chunks.push(chunk);
        });
        proxyRes.on("end", () => {
          let body = Buffer.concat(chunks);
          if (encoding === "gzip") {
            try { body = require("zlib").gunzipSync(body); } catch { /* use as-is */ }
          } else if (encoding === "deflate") {
            try { body = require("zlib").inflateSync(body); } catch { /* use as-is */ }
          }
          let text = body.toString("utf8");
          if (sess.service === "pgadmin") {
            text = text.replace(/\/pgadmin4\//g, tokenPath);
            text = text.replace(/"\/pgadmin4"/g, '"' + DB_ACCESS_PREFIX + token + '"');
            text = text.replace(/'\/pgadmin4'/g, "'" + DB_ACCESS_PREFIX + token + "'");
          } else if (sess.service === "phpmyadmin") {
            text = text.replace(/\/phpmyadmin\//g, tokenPath);
            text = text.replace(/"\/phpmyadmin"/g, '"' + DB_ACCESS_PREFIX + token + '"');
            text = text.replace(/'\/phpmyadmin'/g, "'" + DB_ACCESS_PREFIX + token + "'");
          }
          if (respContentType.includes("text/html")) {
            text = injectPresenceScriptIntoHtml(text, token);
          }
          res.end(text);
        });
        proxyRes.on("error", () => res.end());
      } else {
        // Stream directly — no buffering
        res.writeHead(proxyRes.statusCode, proxyRes.headers);
        proxyRes.pipe(res, { end: true });
      }
    });

    proxyReq.on("error", (err) => {
      console.error("[db-proxy] Proxy error:", err.message);
      if (!res.headersSent) res.status(502).end();
    });

    proxyReq.on("timeout", () => {
      proxyReq.destroy();
      if (!res.headersSent) res.status(504).end();
    });

    req.pipe(proxyReq, { end: true });
  };
}

// ─── Stats ──────────────────────────────────────────────────────
function getSessionCount() {
  return sessions.size;
}

module.exports = {
  generateToken,
  validateToken,
  touchToken,
  revokeToken,
  revokeAllForUser,
  createProxyMiddleware,
  getSessionCount,
  normalizeIp,
  isIpAuthorized,
  SERVICE_TARGETS,
  TOKEN_TTL_MS,
  TOKEN_PRESENCE_TTL_MS,
  PHPMYADMIN_INTERNAL_PORT
};
