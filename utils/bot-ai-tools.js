"use strict";

const MAX_BOT_TOOL_LOOPS = 8;
const MAX_PROMPT_SERVER_NAMES = 80;

const BOT_TOOL_PARAMETER_SCHEMAS = Object.freeze({
  list_accessible_servers: {
    type: "object",
    properties: {},
    required: [],
  },
  inspect_server: {
    type: "object",
    properties: {
      server: {
        type: "string",
        description: "The accessible server name, display name, alias, or the user's raw server wording.",
      },
    },
    required: ["server"],
  },
  power_server: {
    type: "object",
    properties: {
      server: {
        type: "string",
        description: "The accessible server name, display name, alias, or the user's raw server wording.",
      },
      action: {
        type: "string",
        enum: ["start", "stop", "restart", "kill"],
        description: "The power action to run.",
      },
    },
    required: ["server", "action"],
  },
  query_console: {
    type: "object",
    properties: {
      server: {
        type: "string",
        description: "The accessible server name, display name, alias, or the user's raw server wording.",
      },
      question: {
        type: "string",
        description: "Optional focus question about the console output.",
      },
      limit: {
        type: "number",
        description: "Optional number of recent console lines to inspect, capped at 100.",
      },
    },
    required: ["server"],
  },
  send_console_command: {
    type: "object",
    properties: {
      server: {
        type: "string",
        description: "The accessible server name, display name, alias, or the user's raw server wording.",
      },
      command: {
        type: "string",
        description: "One real console command to send.",
      },
    },
    required: ["server", "command"],
  },
  list_backups: {
    type: "object",
    properties: {
      server: {
        type: "string",
        description: "The accessible server name, display name, alias, or the user's raw server wording.",
      },
    },
    required: ["server"],
  },
  create_backup: {
    type: "object",
    properties: {
      server: {
        type: "string",
        description: "The accessible server name, display name, alias, or the user's raw server wording.",
      },
      name: {
        type: "string",
        description: "Optional backup name.",
      },
      description: {
        type: "string",
        description: "Optional backup description.",
      },
    },
    required: ["server"],
  },
  restore_backup: {
    type: "object",
    properties: {
      server: {
        type: "string",
        description: "The accessible server name, display name, alias, or the user's raw server wording.",
      },
      backup: {
        type: "string",
        description: "A backup id, name, archive name, or the user's raw backup wording.",
      },
      deleteOldFiles: {
        type: "boolean",
        description: "Whether old files should be removed before restoring.",
      },
    },
    required: ["server", "backup"],
  },
  delete_backup: {
    type: "object",
    properties: {
      server: {
        type: "string",
        description: "The accessible server name, display name, alias, or the user's raw server wording.",
      },
      backup: {
        type: "string",
        description: "A backup id, name, archive name, or the user's raw backup wording.",
      },
    },
    required: ["server", "backup"],
  },
});

const BOT_TOOL_DEFINITIONS = Object.freeze([
  {
    name: "list_accessible_servers",
    description: "List the servers the current user can access.",
    args: {},
  },
  {
    name: "inspect_server",
    description: "Inspect one accessible server, including its status and the current user's permissions.",
    args: {
      server: "string",
    },
  },
  {
    name: "power_server",
    description: "Start, stop, restart, or kill one accessible server when the user has permission.",
    args: {
      server: "string",
      action: "start|stop|restart|kill",
    },
  },
  {
    name: "query_console",
    description: "Read up to 100 recent console lines from one accessible server and return them for analysis.",
    args: {
      server: "string",
      question: "string?",
      limit: "number?",
    },
  },
  {
    name: "send_console_command",
    description: "Send one real console command to one accessible server when the user explicitly asked for it and has console_write permission.",
    args: {
      server: "string",
      command: "string",
    },
  },
  {
    name: "list_backups",
    description: "List backups for one accessible server when the user has backups_view permission.",
    args: {
      server: "string",
    },
  },
  {
    name: "create_backup",
    description: "Create one backup for an accessible server when the user has backups_create permission.",
    args: {
      server: "string",
      name: "string?",
      description: "string?",
    },
  },
  {
    name: "restore_backup",
    description: "Restore one backup on an accessible server when the user has backups_create permission.",
    args: {
      server: "string",
      backup: "string",
      deleteOldFiles: "boolean?",
    },
  },
  {
    name: "delete_backup",
    description: "Delete one backup on an accessible server when the user has backups_delete permission.",
    args: {
      server: "string",
      backup: "string",
    },
  },
]);

const BOT_OPENAI_TOOL_DEFINITIONS = Object.freeze(
  BOT_TOOL_DEFINITIONS.map((tool) => ({
    type: "function",
    function: {
      name: tool.name,
      description: tool.description,
      parameters: BOT_TOOL_PARAMETER_SCHEMAS[tool.name] || { type: "object", properties: {}, required: [] },
    },
  }))
);

const BOT_GOOGLE_FUNCTION_DECLARATIONS = Object.freeze(
  BOT_TOOL_DEFINITIONS.map((tool) => ({
    name: tool.name,
    description: tool.description,
    parameters: BOT_TOOL_PARAMETER_SCHEMAS[tool.name] || { type: "object", properties: {}, required: [] },
  }))
);

function stripDiacritics(value) {
  return String(value || "").normalize("NFKD").replace(/[\u0300-\u036f]/g, "");
}

function normalizeLookupText(value) {
  return stripDiacritics(value)
    .toLowerCase()
    .replace(/[`"'“”‘’()[\]{}<>]/g, " ")
    .replace(/[_./-]+/g, " ")
    .replace(/[^a-z0-9 ]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function compactLookupText(value) {
  return normalizeLookupText(value).replace(/\s+/g, "");
}

function getServerAliases(entry) {
  const aliases = new Set();
  for (const value of [
    entry?.name,
    entry?.displayName,
    entry?.bot,
    entry?.legacy_id,
    entry?.legacyId,
    entry?.id,
  ]) {
    const clean = String(value || "").trim();
    if (!clean) continue;
    aliases.add(clean);
    aliases.add(clean.replace(/[_./-]+/g, " ").replace(/\s+/g, " ").trim());
  }
  return Array.from(aliases).filter(Boolean);
}

function getCurrentServerReference(currentServerName, accessibleServers) {
  const currentName = String(currentServerName || "").trim();
  if (!currentName) return null;
  const target = compactLookupText(currentName);
  return (Array.isArray(accessibleServers) ? accessibleServers : []).find((entry) => {
    return getServerAliases(entry).some((alias) => compactLookupText(alias) === target);
  }) || null;
}

function isCurrentServerReference(value) {
  const normalized = normalizeLookupText(value);
  if (!normalized) return true;
  return [
    "current",
    "current server",
    "this",
    "this server",
    "server",
    "serverul",
    "serverul curent",
    "serverul asta",
    "asta",
    "curent",
    "it",
  ].includes(normalized);
}

function resolveServerFromAccessibleList(accessibleServers, requestedServer, currentServerName = "") {
  const entries = Array.isArray(accessibleServers) ? accessibleServers.filter((entry) => entry?.name) : [];
  if (!entries.length) {
    return { entry: null, error: "You do not have access to any servers." };
  }

  const raw = String(requestedServer || "").trim();
  if (!raw || isCurrentServerReference(raw)) {
    const currentEntry = getCurrentServerReference(currentServerName, entries);
    if (currentEntry) return { entry: currentEntry };
    if (entries.length === 1) return { entry: entries[0] };
    return {
      entry: null,
      error: "The target server is not clear.",
      candidates: entries.slice(0, 5).map((entry) => entry.displayName || entry.name),
    };
  }

  const normalizedRequested = normalizeLookupText(raw);
  const compactRequested = compactLookupText(raw);
  const scored = entries.map((entry) => {
    let best = 0;
    for (const alias of getServerAliases(entry)) {
      const normalizedAlias = normalizeLookupText(alias);
      const compactAlias = compactLookupText(alias);
      if (!normalizedAlias && !compactAlias) continue;

      if (normalizedRequested && normalizedAlias === normalizedRequested) {
        best = Math.max(best, 1600 + normalizedAlias.length);
      }
      if (compactRequested && compactAlias === compactRequested) {
        best = Math.max(best, 1550 + compactAlias.length);
      }
      if (normalizedRequested && normalizedAlias && normalizedRequested.includes(normalizedAlias)) {
        best = Math.max(best, 900 + normalizedAlias.length);
      }
      if (normalizedRequested && normalizedAlias && normalizedAlias.includes(normalizedRequested)) {
        best = Math.max(best, 880 + normalizedRequested.length);
      }
      if (compactRequested && compactAlias && compactRequested.includes(compactAlias)) {
        best = Math.max(best, 760 + compactAlias.length);
      }

      const aliasTokens = normalizedAlias.split(" ").filter(Boolean);
      const requestedTokens = normalizedRequested.split(" ").filter(Boolean);
      if (aliasTokens.length && requestedTokens.length) {
        const overlap = aliasTokens.filter((token) => requestedTokens.includes(token)).length;
        if (overlap === aliasTokens.length) {
          best = Math.max(best, 680 + aliasTokens.length * 30);
        } else if (overlap > 0) {
          best = Math.max(best, 280 + overlap * 45);
        }
      }
    }
    return { entry, score: best };
  }).filter((item) => item.score > 0).sort((left, right) => right.score - left.score);

  if (!scored.length) {
    return {
      entry: null,
      error: "I could not match that server to one you can access.",
      candidates: entries.slice(0, 5).map((entry) => entry.displayName || entry.name),
    };
  }

  const [best, second] = scored;
  if (second && best.score < second.score + 140) {
    return {
      entry: null,
      error: "That server name is ambiguous.",
      candidates: scored.slice(0, 5).map((item) => item.entry.displayName || item.entry.name),
    };
  }

  return { entry: best.entry };
}

function resolveBackupFromList(backups, requestedBackup) {
  const items = Array.isArray(backups) ? backups : [];
  const raw = String(requestedBackup || "").trim();
  if (!raw) {
    return { backup: null, error: "A backup identifier is required." };
  }

  const normalizedRequested = normalizeLookupText(raw);
  const compactRequested = compactLookupText(raw);
  const scored = items.map((backup) => {
    const aliases = [
      backup?.id,
      backup?.name,
      backup?.archive_name,
      backup?.archiveName,
      backup?.uuid,
    ].map((value) => String(value || "").trim()).filter(Boolean);

    let best = 0;
    for (const alias of aliases) {
      const normalizedAlias = normalizeLookupText(alias);
      const compactAlias = compactLookupText(alias);
      if (!normalizedAlias && !compactAlias) continue;

      if (normalizedAlias === normalizedRequested) best = Math.max(best, 1600 + normalizedAlias.length);
      if (compactAlias === compactRequested) best = Math.max(best, 1550 + compactAlias.length);
      if (normalizedRequested && normalizedAlias.includes(normalizedRequested)) best = Math.max(best, 900 + normalizedRequested.length);
      if (normalizedRequested && normalizedRequested.includes(normalizedAlias)) best = Math.max(best, 880 + normalizedAlias.length);
      if (compactRequested && compactRequested.includes(compactAlias)) best = Math.max(best, 760 + compactAlias.length);
    }

    return { backup, score: best };
  }).filter((item) => item.score > 0).sort((left, right) => right.score - left.score);

  if (!scored.length) {
    return { backup: null, error: "I could not match that backup." };
  }

  const [best, second] = scored;
  if (second && best.score < second.score + 120) {
    return {
      backup: null,
      error: "That backup reference is ambiguous.",
      candidates: scored.slice(0, 5).map((item) => item.backup?.name || item.backup?.id).filter(Boolean),
    };
  }

  return { backup: best.backup };
}

function extractJsonObjectFromText(value) {
  const text = String(value || "").trim();
  if (!text) return "";

  const taggedMatch = text.match(/<adpanel_tool_plan>\s*([\s\S]*?)\s*<\/adpanel_tool_plan>/i);
  if (taggedMatch && taggedMatch[1]) {
    return String(taggedMatch[1]).trim();
  }

  const fencedMatch = text.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fencedMatch && fencedMatch[1]) {
    return String(fencedMatch[1]).trim();
  }

  const firstBrace = text.indexOf("{");
  const lastBrace = text.lastIndexOf("}");
  if (firstBrace !== -1 && lastBrace !== -1 && lastBrace > firstBrace) {
    return text.slice(firstBrace, lastBrace + 1).trim();
  }

  return "";
}

function parseBotAssistantToolPlan(rawContent) {
  const extracted = extractJsonObjectFromText(rawContent);
  if (!extracted) return null;

  try {
    const parsed = JSON.parse(extracted);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
    const toolCalls = Array.isArray(parsed.tool_calls) ? parsed.tool_calls : [];
    return {
      reply: String(parsed.reply || "").trim(),
      tool_calls: toolCalls
        .filter((item) => item && typeof item === "object")
        .map((item) => ({
          name: String(item.name || "").trim(),
          args: item.args && typeof item.args === "object" && !Array.isArray(item.args) ? item.args : {},
        }))
        .filter((item) => item.name),
    };
  } catch {
    return null;
  }
}

function formatPermissionSummary(perms) {
  const entries = [];
  if (perms?.server_start) entries.push("start");
  if (perms?.server_stop) entries.push("stop");
  if (perms?.console_read) entries.push("read console");
  if (perms?.console_write) entries.push("send console commands");
  if (perms?.backups_view) entries.push("view backups");
  if (perms?.backups_create) entries.push("create or restore backups");
  if (perms?.backups_delete) entries.push("delete backups");
  return entries.length ? entries.join(", ") : "no elevated server actions";
}

function buildBotAssistantSystemPrompt({ currentServerName = "", accessibleServers = [], currentServerPerms = null }) {
  const currentServer = String(currentServerName || "").trim();
  const visibleServerNames = (Array.isArray(accessibleServers) ? accessibleServers : [])
    .slice(0, MAX_PROMPT_SERVER_NAMES)
    .map((entry) => entry?.displayName || entry?.name)
    .filter(Boolean);
  const currentPermSummary = formatPermissionSummary(currentServerPerms);

  return [
    "You are ADPanel Agent inside a server page.",
    "Always reply in English.",
    "Keep replies short, natural, and precise.",
    "Use one short sentence by default.",
    "Never narrate internal planning, tool selection, or hidden reasoning.",
    "Never say phrases like 'The user wants', 'I need to', 'I should use', 'Plan', or 'Current page server'.",
    "You may still use the existing file command format from other system instructions for file tasks.",
    "For sensitive server actions, use only the native tool plan format below.",
    "Never use SERVER_START, SERVER_STOP, SERVER_RESTART, or SERVER_KILL text lines.",
    "When you need a native server action, your entire reply must be one <adpanel_tool_plan> JSON block with no extra text.",
    'Schema: {"reply":"short queued reply","tool_calls":[{"name":"tool_name","args":{}}]}',
    "Set tool_calls to an empty array when no native tool is needed.",
    'Example for current server start: <adpanel_tool_plan>{"reply":"Starting it now.","tool_calls":[{"name":"power_server","args":{"server":"current server","action":"start"}}]}</adpanel_tool_plan>',
    'Example for status check: <adpanel_tool_plan>{"reply":"Checking it now.","tool_calls":[{"name":"inspect_server","args":{"server":"current server"}}]}</adpanel_tool_plan>',
    'Example for status plus console check: <adpanel_tool_plan>{"reply":"Checking that now.","tool_calls":[{"name":"inspect_server","args":{"server":"current server"}},{"name":"query_console","args":{"server":"current server","limit":60}}]}</adpanel_tool_plan>',
    'Example for an explicit console command: <adpanel_tool_plan>{"reply":"Sending it now.","tool_calls":[{"name":"send_console_command","args":{"server":"current server","command":"say hello"}}]}</adpanel_tool_plan>',
    "Only claim a sensitive action succeeded after the tool result confirms it.",
    "If you need more than one tool, call multiple tools in the same tool plan or ask for another tool plan after tool results.",
    "For console analysis, use query_console.",
    "For real console execution, use send_console_command only when the user explicitly asked to run a command; the backend will deny it unless the user has console_write.",
    "For backup restore or delete, list backups first when the backup identifier is unclear.",
    "If the user says this server or current server, use the current page server.",
    "If the user names a server unclearly, you may pass the raw wording in args.server and the backend will resolve it safely.",
    `Current page server: ${currentServer || "unknown"}.`,
    `Current page server permissions: ${currentPermSummary}.`,
    visibleServerNames.length
      ? `Accessible servers visible to this user: ${visibleServerNames.join(", ")}.`
      : "No accessible servers were preloaded.",
    "Allowed native tools:",
    BOT_TOOL_DEFINITIONS.map((tool) => `${tool.name}: ${tool.description}`).join(" "),
  ].join(" ");
}

function buildBotReplyFromToolResult(toolName, toolResult) {
  const name = String(toolName || "").trim();
  const result = toolResult && typeof toolResult === "object" ? toolResult : {};
  if (result.ok === false) {
    return String(result.error || result.detail || "That action could not be completed.").trim();
  }

  switch (name) {
    case "list_accessible_servers": {
      const servers = Array.isArray(result.servers) ? result.servers : [];
      if (!servers.length) return "You do not have access to any servers.";
      const labels = servers.slice(0, 6).map((entry) => entry.displayName || entry.name).filter(Boolean);
      return `You can access ${servers.length} server${servers.length === 1 ? "" : "s"}: ${labels.join(", ")}.`;
    }
    case "inspect_server":
      return result.server?.name
        ? `${result.server.displayName || result.server.name} is ${result.server.status || "unknown"}.`
        : "I checked that server.";
    case "power_server":
      if (!(result.server && result.action)) return "Power action sent.";
      return `Sent the ${result.action} request for ${result.server.displayName || result.server.name}.`;
    case "query_console":
      return result.server ? `I checked the recent console output for ${result.server.displayName || result.server.name}.` : "I checked the recent console output.";
    case "send_console_command":
      return result.server ? `Sent that command to ${result.server.displayName || result.server.name}.` : "Sent that command.";
    case "list_backups":
      return result.server
        ? `I found ${Number(result.total || 0)} backup${Number(result.total || 0) === 1 ? "" : "s"} for ${result.server.displayName || result.server.name}.`
        : "I checked the backups.";
    case "create_backup":
      return result.server ? `Created a backup for ${result.server.displayName || result.server.name}.` : "Created the backup.";
    case "restore_backup":
      return result.server ? `Started restoring that backup on ${result.server.displayName || result.server.name}.` : "Started restoring that backup.";
    case "delete_backup":
      return result.server ? `Deleted that backup from ${result.server.displayName || result.server.name}.` : "Deleted that backup.";
    default:
      return "";
  }
}

function capitalize(value) {
  const text = String(value || "").trim();
  return text ? `${text[0].toUpperCase()}${text.slice(1)}` : "";
}

function formatPowerProgressLabel(action) {
  const normalized = String(action || "").trim().toLowerCase();
  const labels = {
    start: "Starting",
    stop: "Stopping",
    restart: "Restarting",
    kill: "Killing",
  };
  return labels[normalized] || `${capitalize(normalized)}ing`;
}

module.exports = {
  BOT_GOOGLE_FUNCTION_DECLARATIONS,
  BOT_TOOL_DEFINITIONS,
  BOT_OPENAI_TOOL_DEFINITIONS,
  MAX_BOT_TOOL_LOOPS,
  buildBotAssistantSystemPrompt,
  buildBotReplyFromToolResult,
  extractJsonObjectFromText,
  parseBotAssistantToolPlan,
  resolveBackupFromList,
  resolveServerFromAccessibleList,
};
