"use strict";

const crypto = require("crypto");
const fs = require("fs");
const fsp = require("fs/promises");
const httpMod = require("http");
const https = require("https");
const os = require("os");
const path = require("path");
const FormData = require("form-data");
const { getTechnicalServerName } = require("./server-name");

function safeJoinUnix(baseDir, rel) {
  const base = path.posix.normalize(String(baseDir || "")).replace(/\0/g, "");
  const raw = String(rel || "").replace(/\0/g, "");
  if (path.posix.isAbsolute(raw)) {
    throw new Error("Absolute path not allowed");
  }
  const normalized = path.posix.normalize(`/${raw}`).replace(/^\/+/, "");
  if (normalized === ".." || normalized.startsWith("../") || normalized.includes("/../")) {
    throw new Error("Path traversal detected");
  }
  return path.posix.join(base, normalized);
}

async function uploadFileToNode({
  node,
  uploadUrl,
  headers,
  filePath,
  filename,
  targetDir,
}) {
  const form = new FormData();
  form.append("file", fs.createReadStream(filePath), { filename });
  form.append("dir", targetDir);

  const requestHeaders = Object.assign({}, headers);
  delete requestHeaders["Content-Type"];
  Object.assign(requestHeaders, form.getHeaders());

  const parsedUrl = new URL(uploadUrl);
  const lib = uploadUrl.startsWith("https:") ? https : httpMod;

  await new Promise((resolve, reject) => {
    const req = lib.request({
      method: "POST",
      hostname: parsedUrl.hostname,
      port: parsedUrl.port || (uploadUrl.startsWith("https:") ? 443 : 80),
      path: parsedUrl.pathname + parsedUrl.search,
      headers: requestHeaders,
      timeout: 600_000,
    }, (res) => {
      const chunks = [];
      let total = 0;
      res.on("data", (chunk) => {
        if (total < 65_536) {
          chunks.push(chunk);
          total += chunk.length;
        }
      });
      res.on("end", () => {
        if (res.statusCode === 200) {
          resolve();
          return;
        }
        const body = Buffer.concat(chunks).toString().slice(0, 2000);
        const error = new Error(`Node upload failed: ${res.statusCode} ${body}`);
        error.nodeStatusCode = res.statusCode;
        error.nodeBody = body;
        reject(error);
      });
    });

    req.on("error", reject);
    req.on("timeout", () => {
      req.destroy();
      reject(new Error("Upload request timed out"));
    });
    form.pipe(req);
  });
}

async function applyRemoteAssetToServer(deps, { serverName, entry, url, destPath }) {
  if (!deps || typeof deps !== "object") {
    throw new Error("Asset apply dependencies are required.");
  }

  const bot = String(serverName || entry?.name || "").trim();
  if (!bot) {
    return { ok: false, statusCode: 400, error: "missing-bot" };
  }

  const resolvedEntry = entry || (typeof deps.findServer === "function" ? await deps.findServer(bot) : null);
  if (!resolvedEntry) {
    return { ok: false, statusCode: 404, error: "server-not-found" };
  }
  if (!deps.isRemoteEntry(resolvedEntry)) {
    return { ok: false, statusCode: 400, error: "server-not-on-node" };
  }

  const node = await deps.findNodeByIdOrName(resolvedEntry.nodeId);
  if (!node) {
    return { ok: false, statusCode: 400, error: "node-not-found" };
  }

  const baseUrl = deps.buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
  if (!baseUrl) {
    return { ok: false, statusCode: 400, error: "invalid-node-address" };
  }

  const headers = deps.nodeAuthHeadersFor(node, true);
  const technicalName = getTechnicalServerName(resolvedEntry, bot);

  let safeUrl = String(url || "").trim();
  if (!safeUrl) {
    return { ok: false, statusCode: 400, error: "missing-url" };
  }

  try {
    const validated = await deps.assertSafeRemoteUrl(safeUrl);
    safeUrl = validated.toString();
  } catch {
    return { ok: false, statusCode: 400, error: "invalid-url" };
  }

  let destRel = String(destPath || "").trim();
  if (!destRel) destRel = "server.jar";
  destRel = destRel.replace(/^\/+/, "");
  if (destRel.includes("..") || destRel.includes("\\")) {
    return { ok: false, statusCode: 400, error: "invalid-destPath" };
  }

  const destFile = path.posix.basename(destRel);
  if (!destFile || destFile === "." || destFile === "..") {
    return { ok: false, statusCode: 400, error: "invalid-destPath" };
  }

  if (deps.remoteApplyProxyDownload) {
    const ctx = await deps.resolveRemoteFsContext(bot);
    if (!ctx.remote || !ctx.node) {
      return { ok: false, statusCode: 404, error: "server-not-on-node" };
    }

    try {
      const pingRes = await deps.nodeFsPost(ctx.node, "/v1/fs/list", { path: ctx.baseDir, depth: 0 }, 5000);
      if (pingRes.status === 401) {
        return {
          ok: false,
          statusCode: 502,
          error: "node-auth-failed",
          detail: "Token mismatch. Re-deploy config.yml to the node.",
        };
      }
      if (pingRes.status === 0) {
        return {
          ok: false,
          statusCode: 502,
          error: "node-unreachable",
          detail: "Cannot connect to node agent.",
        };
      }
    } catch {
    }

    let buffer;
    try {
      buffer = await deps.httpGetRaw(safeUrl, {
        maxBytes: deps.remoteApplyMaxBytes,
        timeoutMs: deps.remoteApplyTimeoutMs,
        maxRedirects: deps.remoteFetchMaxRedirects,
      });
    } catch {
      return { ok: false, statusCode: 400, error: "remote-download-failed" };
    }

    let targetDir = ctx.baseDir;
    try {
      const destDirRel = path.posix.dirname(destRel);
      targetDir = safeJoinUnix(ctx.baseDir, destDirRel === "." ? "" : destDirRel);
    } catch {
      return { ok: false, statusCode: 400, error: "invalid-destPath" };
    }

    const tmpFile = path.join(os.tmpdir(), `adpanel-apply-${crypto.randomBytes(8).toString("hex")}`);
    try {
      await fsp.writeFile(tmpFile, buffer);
      const uploadUrl = `${baseUrl}/v1/fs/upload`;
      await uploadFileToNode({
        node: ctx.node,
        uploadUrl,
        headers,
        filePath: tmpFile,
        filename: destFile,
        targetDir,
      });
      return { ok: true, remote: true, msg: "downloaded-and-uploaded", destPath: destRel };
    } catch (error) {
      const detail = error.message || "stream-upload-failed";
      if (error.nodeStatusCode === 507) {
        try {
          const nodeJson = JSON.parse(error.nodeBody);
          if (nodeJson.error === "disk_limit_exceeded") {
            return Object.assign({ ok: false, statusCode: 507 }, nodeJson);
          }
        } catch {
        }
      }
      if (detail.includes("401")) {
        return {
          ok: false,
          statusCode: 502,
          error: "node-auth-failed",
          detail: "Token mismatch. Re-deploy config.yml to the node.",
        };
      }
      return { ok: false, statusCode: 502, error: "node_upload_failed", detail };
    } finally {
      try {
        await fsp.unlink(tmpFile);
      } catch {
      }
    }
  }

  const forwardUrl = `${baseUrl}/v1/servers/${encodeURIComponent(technicalName)}/apply-version`;
  const response = await deps.httpRequestJson(
    forwardUrl,
    "POST",
    headers,
    { url: safeUrl, nodeId: resolvedEntry.nodeId, destPath: destRel },
    60_000
  );

  if (response.status === 507 && response.json?.error === "disk_limit_exceeded") {
    return Object.assign({ ok: false, statusCode: 507 }, response.json);
  }
  if (response.status !== 200 || !(response.json && response.json.ok)) {
    const detail = response.json && (response.json.detail || response.json.error)
      ? (response.json.detail || response.json.error)
      : `node-status-${response.status}`;
    if (response.status === 401 || response.status === 403) {
      return {
        ok: false,
        statusCode: 502,
        error: "node-auth-failed",
        detail: "Token or HMAC mismatch. Re-deploy config.yml to the node.",
      };
    }
    return { ok: false, statusCode: 502, error: detail || `node-forward-failed-${response.status}` };
  }

  return { ok: true, remote: true, msg: "forwarded-to-node", destPath: destRel };
}

module.exports = {
  applyRemoteAssetToServer,
};
