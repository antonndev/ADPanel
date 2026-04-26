"use strict";

const { getTechnicalServerName } = require("./server-name");

async function deleteServerByName(deps, rawName) {
  if (!deps || typeof deps !== "object") {
    throw new Error("Server delete dependencies are required.");
  }

  let nameParam = rawName || "";
  try {
    nameParam = decodeURIComponent(nameParam);
  } catch {
  }

  const name = String(nameParam || "").trim();
  if (!name) {
    return { ok: false, statusCode: 400, error: "missing name" };
  }

  const entry = await deps.findServer(name);
  if (!entry) {
    await deps.deleteServerSchedules(name);
    await deps.removeServerIndexEntry(name);
    return { ok: true };
  }
  const technicalName = getTechnicalServerName(entry, name);

  if (!deps.isRemoteEntry(entry)) {
    return { ok: false, statusCode: 400, error: "server-not-on-node" };
  }

  const node = await deps.findNodeByIdOrName(entry.nodeId);
  if (!node) {
    return { ok: false, statusCode: 400, error: "node not found for server" };
  }

  const baseUrl = deps.buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
  if (!baseUrl) {
    return { ok: false, statusCode: 400, error: "invalid node address" };
  }

  const headers = deps.nodeAuthHeadersFor(node, true);

  try {
    await deps.httpRequestJson(`${baseUrl}/v1/servers/${encodeURIComponent(technicalName)}/stop`, "POST", headers, null, 30_000);
    console.log(`[delete] Stopped server ${name}`);
  } catch (error) {
    console.log(`[delete] Could not stop server ${name} (may already be stopped): ${error.message}`);
  }

  try {
    const backupsRes = await deps.httpRequestJson(`${baseUrl}/v1/servers/${encodeURIComponent(technicalName)}/backups`, "GET", headers, null, 10_000);
    if (backupsRes.status === 200 && backupsRes.json?.backups) {
      for (const backup of backupsRes.json.backups) {
        if (!backup?.id) continue;
        await deps.httpRequestJson(
          `${baseUrl}/v1/servers/${encodeURIComponent(technicalName)}/backups/${encodeURIComponent(backup.id)}`,
          "DELETE",
          headers,
          null,
          30_000
        );
        console.log(`[delete] Deleted backup ${backup.id} for server ${name}`);
      }
    }
  } catch (error) {
    console.log(`[delete] Could not delete backups for ${name}: ${error.message}`);
  }

  await deps.deleteServerSchedules(name);

  const response = await deps.httpRequestJson(`${baseUrl}/v1/servers/${encodeURIComponent(technicalName)}`, "DELETE", headers, null, 60_000);
  if (response.status !== 200 || !(response.json && response.json.ok)) {
    const message = response.json && (response.json.error || response.json.detail)
      ? (response.json.error || response.json.detail)
      : `remote delete failed (${response.status})`;
    return { ok: false, statusCode: 502, error: message };
  }

  try {
    await deps.removeAccessForServerName(name);
  } catch {
  }

  await deps.removeServerIndexEntry(name);
  return { ok: true, remote: true };
}

module.exports = {
  deleteServerByName,
};
