"use strict";

const path = require("path");
const {
  resolveStartupCommandForCreate,
  extractStartupCommandFromNodeCreateResult,
} = require("./server-startup-command");
const { sanitizeDockerTemplatePayload } = require("./server-template-payload");

const FILE_PREVIEW_LIMIT = 16000;
const MODRINTH_API_BASE = "https://api.modrinth.com/v2";
const MODRINTH_PLUGIN_LOADERS = Object.freeze([
  "paper",
  "purpur",
  "spigot",
  "bukkit",
  "velocity",
  "waterfall",
  "bungeecord",
  "fabric",
  "quilt",
  "forge",
  "neoforge",
]);

const TOOL_DEFINITIONS = [
  {
    type: "function",
    function: {
      name: "list_templates",
      description: "List the available ADPanel server templates that can be used for new servers.",
      parameters: {
        type: "object",
        properties: {},
        additionalProperties: false,
      },
    },
  },
  {
    type: "function",
    function: {
      name: "list_nodes",
      description: "List the available nodes where new servers can be created.",
      parameters: {
        type: "object",
        properties: {},
        additionalProperties: false,
      },
    },
  },
  {
    type: "function",
    function: {
      name: "create_server",
      description: "Create a new server from a chosen template on a chosen node. Use this only after you know the template, node, server name, host port, and the desired RAM/CPU/storage limits, unless the user explicitly asked to use defaults.",
      parameters: {
        type: "object",
        properties: {
          name: {
            type: "string",
            description: "The server name/ID to create.",
          },
          displayName: {
            type: "string",
            description: "Optional display name to show in the dashboard.",
          },
          templateId: {
            type: "string",
            description: "The ADPanel template id or name, such as 'minecraft' or 'nodejs'.",
          },
          nodeId: {
            type: "string",
            description: "The target node id, uuid, or name.",
          },
          hostPort: {
            type: "number",
            description: "The public port for the new server. If omitted, the template default port will be used when available.",
          },
          mcFork: {
            type: "string",
            description: "Optional Minecraft fork, for example 'paper'.",
          },
          mcVersion: {
            type: "string",
            description: "Optional Minecraft version.",
          },
          startupCommand: {
            type: "string",
            description: "Optional startup command override.",
          },
          importUrl: {
            type: "string",
            description: "Optional archive URL to import after creation.",
          },
          resources: {
            type: "object",
            properties: {
              ramMb: { type: "number" },
              cpuCores: { type: "number" },
              storageMb: { type: "number" },
              storageGb: { type: "number" },
              swapMb: { type: "number" },
              backupsMax: { type: "number" },
              maxSchedules: { type: "number" },
            },
            additionalProperties: false,
          },
        },
        required: ["name", "templateId", "nodeId"],
        additionalProperties: false,
      },
    },
  },
  {
    type: "function",
    function: {
      name: "list_servers",
      description: "List the servers the current user can access.",
      parameters: {
        type: "object",
        properties: {},
        additionalProperties: false,
      },
    },
  },
  {
    type: "function",
    function: {
      name: "install_plugin",
      description: "Install a Minecraft plugin from Modrinth onto a server by downloading the correct file into the plugins folder. Use this for plugin requests such as ViaVersion, LuckPerms, Vault, Geyser, or similar.",
      parameters: {
        type: "object",
        properties: {
          server: {
            type: "string",
            description: "The target server name as used inside ADPanel.",
          },
          pluginName: {
            type: "string",
            description: "The plugin name or slug to install from Modrinth.",
          },
          platform: {
            type: "string",
            description: "Optional loader/platform such as paper, purpur, spigot, bukkit, velocity, fabric, forge, or neoforge.",
          },
          gameVersion: {
            type: "string",
            description: "Optional Minecraft version such as 1.21.8.",
          },
        },
        required: ["server", "pluginName"],
        additionalProperties: false,
      },
    },
  },
  {
    type: "function",
    function: {
      name: "inspect_server",
      description: "Inspect one server, including status, template, and the current user's permissions.",
      parameters: {
        type: "object",
        properties: {
          server: {
            type: "string",
            description: "The server name as used inside ADPanel.",
          },
        },
        required: ["server"],
        additionalProperties: false,
      },
    },
  },
  {
    type: "function",
    function: {
      name: "request_delete_server",
      description: "Ask the browser to show a confirmation modal before deleting a server. Use this instead of deleting immediately.",
      parameters: {
        type: "object",
        properties: {
          server: {
            type: "string",
            description: "The server name to delete.",
          },
        },
        required: ["server"],
        additionalProperties: false,
      },
    },
  },
  {
    type: "function",
    function: {
      name: "list_files",
      description: "List files inside a server directory. Use this before reading or editing when you are unsure about the path.",
      parameters: {
        type: "object",
        properties: {
          server: {
            type: "string",
            description: "The server name as used inside ADPanel.",
          },
          path: {
            type: "string",
            description: "A relative directory path inside the server, for example '' or 'plugins'.",
          },
        },
        required: ["server"],
        additionalProperties: false,
      },
    },
  },
  {
    type: "function",
    function: {
      name: "read_file",
      description: "Read a text file from a server. Use this before changing an existing file unless the user already gave you the full replacement content.",
      parameters: {
        type: "object",
        properties: {
          server: {
            type: "string",
            description: "The server name as used inside ADPanel.",
          },
          path: {
            type: "string",
            description: "The relative file path inside the server.",
          },
        },
        required: ["server", "path"],
        additionalProperties: false,
      },
    },
  },
  {
    type: "function",
    function: {
      name: "write_file",
      description: "Create or replace a text file on a server.",
      parameters: {
        type: "object",
        properties: {
          server: {
            type: "string",
            description: "The server name as used inside ADPanel.",
          },
          path: {
            type: "string",
            description: "The relative file path inside the server.",
          },
          content: {
            type: "string",
            description: "The complete file content that should be written.",
          },
        },
        required: ["server", "path", "content"],
        additionalProperties: false,
      },
    },
  },
  {
    type: "function",
    function: {
      name: "create_directory",
      description: "Create a new directory on a server.",
      parameters: {
        type: "object",
        properties: {
          server: {
            type: "string",
            description: "The server name as used inside ADPanel.",
          },
          path: {
            type: "string",
            description: "The relative directory path to create.",
          },
        },
        required: ["server", "path"],
        additionalProperties: false,
      },
    },
  },
  {
    type: "function",
    function: {
      name: "rename_path",
      description: "Rename or move a file or directory on a server.",
      parameters: {
        type: "object",
        properties: {
          server: {
            type: "string",
            description: "The server name as used inside ADPanel.",
          },
          source: {
            type: "string",
            description: "The current relative path.",
          },
          destination: {
            type: "string",
            description: "The new relative path.",
          },
        },
        required: ["server", "source", "destination"],
        additionalProperties: false,
      },
    },
  },
  {
    type: "function",
    function: {
      name: "delete_path",
      description: "Delete a file or directory on a server.",
      parameters: {
        type: "object",
        properties: {
          server: {
            type: "string",
            description: "The server name as used inside ADPanel.",
          },
          path: {
            type: "string",
            description: "The relative path to delete.",
          },
          isDirectory: {
            type: "boolean",
            description: "Set to true when the target is a directory.",
          },
        },
        required: ["server", "path"],
        additionalProperties: false,
      },
    },
  },
  {
    type: "function",
    function: {
      name: "power_server",
      description: "Start, stop, restart, or kill a server when the current user has permission.",
      parameters: {
        type: "object",
        properties: {
          server: {
            type: "string",
            description: "The server name as used inside ADPanel.",
          },
          action: {
            type: "string",
            enum: ["start", "stop", "restart", "kill"],
            description: "The power action to execute.",
          },
        },
        required: ["server", "action"],
        additionalProperties: false,
      },
    },
  },
  {
    type: "function",
    function: {
      name: "open_account_flow",
      description: "Open an existing ADPanel account flow in the browser for sensitive actions like password, email, or 2FA changes.",
      parameters: {
        type: "object",
        properties: {
          flow: {
            type: "string",
            enum: [
              "change_password",
              "change_email",
              "change_2fa",
              "recover_password",
              "recover_email",
              "recover_2fa",
            ],
            description: "The account flow that should be opened.",
          },
        },
        required: ["flow"],
        additionalProperties: false,
      },
    },
  },
];

function buildSystemPrompt({ appName = "ADPanel", userEmail }) {
  return [
    `You are ${appName} Assistant inside the ${appName} dashboard.`,
    "Always reply in English.",
    "Keep replies extremely short, natural, and easy to understand when spoken aloud.",
    "Sound like a voice assistant, not a chatbot.",
    "Default to one short sentence.",
    "Use two short sentences only when the second sentence adds essential help.",
    "Never give long explanations unless the user explicitly asks for detail.",
    "Use previous messages as context, but always answer the user's latest message directly.",
    "Do not ignore or override the user's latest request because of older context.",
    "When an action succeeds, confirm it in one short sentence.",
    "When you need missing information, ask one short direct question.",
    "If you list choices, keep it to the best 3 options and keep the wording brief.",
    "The wake phrase is optional. Respond normally even when the user does not say 'Hey ADPanel' first.",
    "You can read context from previous messages in this chat and you can use tools to perform real ADPanel actions when they are actually needed.",
    "Only claim an action succeeded after the matching tool succeeded.",
    "If the user wants to change their password, email, or 2FA, never ask them to send secrets in chat. Use the open_account_flow tool instead.",
    "If the user needs a recovery flow for password, email, or 2FA, use the matching recovery account flow instead of asking for sensitive codes in chat.",
    "If a user wants to modify an existing file and you do not already have the current content, read the file first before writing a replacement.",
    "If a request is missing critical details, ask one short follow-up question instead of guessing.",
    "If the user lacks permission for an action, explain that clearly and do not attempt to bypass permissions.",
    "General questions, casual chat, short phrases, or assistant-related questions should be answered directly without using server tools.",
    "Do not assume a random word or short phrase is a server name.",
    "If the user only says one or two ambiguous words, ask a brief clarifying question instead of calling tools.",
    "If the user wants to create a new server, gather the required creation details: template, node, server name, and host port.",
    "RAM, CPU, and storage limits are optional. Use them only when the user provided them or explicitly asks for limits.",
    "If the user explicitly says to use defaults, you may use the template default port and leave optional resource limits unset.",
    "If the user already provided a node, template, port, RAM, CPU, storage, or server name earlier in this chat, reuse it instead of asking again unless it is ambiguous.",
    "For server creation, use list_templates and list_nodes when needed so you can offer concrete choices with node capacity and port information.",
    "When the user asks to create a server but has not chosen a node or template yet, ask a short direct question listing the best available options.",
    "For provisioning, never reply with generic phrases like 'I need more detail' or 'tell me more'. Name the exact missing field instead.",
    "If the user already gave enough information to create the server, call create_server immediately.",
    "If the user replies with a short follow-up like 'yes', 'go ahead', a port number, a node name, or a resource value, use the earlier provisioning context instead of restarting the flow.",
    "Do not call create_server until you have enough information to create the server correctly.",
    "If the user also asked for plugins during server creation, remember that request and install the plugins after the server exists.",
    "Use install_plugin for Minecraft plugin requests. Prefer the server's known fork or version when available, otherwise use the latest compatible plugin build.",
    "Never delete a server immediately from chat. Use request_delete_server first so the browser can show a confirmation modal, and wait for the user's confirmation there.",
    "You can assist any accessible server regardless of docker image or template for file and power actions, as long as the user has permission.",
    "If a server name is unclear, use the available tools to discover or confirm the correct server before taking action.",
    `Current authenticated user: ${String(userEmail || "unknown").trim() || "unknown"}.`,
  ].join(" ");
}

function parseToolArguments(raw) {
  if (!raw) return {};
  if (typeof raw === "object") return raw;
  try {
    const parsed = JSON.parse(String(raw));
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function buildMaskedError(message, extra = {}) {
  return Object.assign({ ok: false, error: message }, extra);
}

function buildSuccess(data) {
  return Object.assign({ ok: true }, data);
}

function normalizeRelativePath(input) {
  return String(input || "").replace(/\0/g, "").trim();
}

function safeJoinUnix(baseDir, relativePath) {
  const base = path.posix.normalize(String(baseDir ?? "")).replace(/\0/g, "");
  const raw = String(relativePath ?? "").replace(/\0/g, "");
  if (path.posix.isAbsolute(raw)) {
    throw new Error("Absolute path not allowed");
  }

  const normalized = path.posix.normalize(`/${raw}`).replace(/^\/+/, "");
  if (normalized === ".." || normalized.startsWith("../") || normalized.includes("/../")) {
    throw new Error("Unix path traversal detected");
  }

  return path.posix.join(base, normalized);
}

function truncateText(text, limit = FILE_PREVIEW_LIMIT) {
  const value = String(text ?? "");
  if (value.length <= limit) {
    return { content: value, truncated: false, originalLength: value.length };
  }
  return {
    content: `${value.slice(0, limit)}\n\n[Truncated by ADPanel Assistant]`,
    truncated: true,
    originalLength: value.length,
  };
}

function coerceNumber(value) {
  if (value === undefined || value === null || value === "") return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function normalizeLooseText(value) {
  return String(value || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

function compareMcVersionsDesc(left, right) {
  const leftParts = String(left || "").split(".").map((part) => parseInt(part, 10) || 0);
  const rightParts = String(right || "").split(".").map((part) => parseInt(part, 10) || 0);
  const maxLength = Math.max(leftParts.length, rightParts.length);
  for (let index = 0; index < maxLength; index += 1) {
    const delta = (rightParts[index] || 0) - (leftParts[index] || 0);
    if (delta !== 0) return delta;
  }
  return 0;
}

function normalizePluginLoader(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized) return "";
  if (normalized === "bungee" || normalized === "bungeecord") return "bungeecord";
  if (normalized === "waterfall" || normalized === "velocity") return normalized;
  if (normalized === "paper" || normalized === "purpur" || normalized === "spigot" || normalized === "bukkit") return normalized;
  if (normalized === "neo" || normalized === "neo-forge" || normalized === "neoforge") return "neoforge";
  if (normalized === "forge" || normalized === "fabric" || normalized === "quilt") return normalized;
  return normalized;
}

function inferServerPluginLoader(entry) {
  const candidates = [
    entry?.mcFork,
    entry?.runtime?.providerId,
    entry?.runtime?.versionId,
    entry?.runtime?.command,
    entry?.runtime?.startupCommand,
    entry?.startupCommand,
    entry?.template,
  ].map((value) => String(value || "").toLowerCase());

  for (const loader of MODRINTH_PLUGIN_LOADERS) {
    if (candidates.some((candidate) => candidate.includes(loader))) {
      return loader;
    }
  }

  if (String(entry?.template || "").trim().toLowerCase() === "minecraft") {
    return normalizePluginLoader(entry?.mcFork || "paper");
  }
  return "";
}

function inferServerMinecraftVersion(entry) {
  return String(entry?.mcVersion || "").trim();
}

function scoreModrinthHit(hit, query) {
  const queryText = normalizeLooseText(query);
  const slug = normalizeLooseText(hit?.slug);
  const title = normalizeLooseText(hit?.title);
  let score = 0;
  if (slug === queryText || title === queryText) score += 12;
  if (slug.replace(/\s+/g, "") === queryText.replace(/\s+/g, "")) score += 10;
  if (title.includes(queryText)) score += 5;
  if (slug.includes(queryText)) score += 4;
  score += Math.min(Number(hit?.downloads || 0), 500000) / 500000;
  if (hit?.featured) score += 1;
  return score;
}

async function fetchJsonOrError(deps, url) {
  const response = await deps.httpRequestJson(url, "GET", {
    "Accept": "application/json",
    "User-Agent": "ADPanel-Assistant",
  }, null, 20_000);
  if (response.status !== 200 || !response.json) {
    return { error: `Request failed (${response.status || 0}).` };
  }
  return { json: response.json };
}

async function resolveModrinthProject(deps, pluginName) {
  const query = String(pluginName || "").trim();
  if (!query) {
    return { error: "A plugin name is required." };
  }

  const facets = encodeURIComponent('[["project_type:plugin"]]');
  const url = `${MODRINTH_API_BASE}/search?limit=8&query=${encodeURIComponent(query)}&facets=${facets}`;
  const { json, error } = await fetchJsonOrError(deps, url);
  if (error) return { error: `Modrinth search failed. ${error}` };

  const hits = Array.isArray(json?.hits) ? json.hits : [];
  if (!hits.length) {
    return { error: `No Modrinth plugin matched "${query}".` };
  }

  const sorted = hits
    .slice()
    .sort((left, right) => scoreModrinthHit(right, query) - scoreModrinthHit(left, query));
  const best = sorted[0];
  if (!best?.project_id) {
    return { error: `No Modrinth plugin matched "${query}".` };
  }

  return {
    project: {
      id: best.project_id,
      slug: best.slug || "",
      title: best.title || best.slug || query,
      description: best.description || "",
    },
  };
}

async function fetchModrinthProjectVersions(deps, projectId) {
  const url = `${MODRINTH_API_BASE}/project/${encodeURIComponent(projectId)}/version`;
  const { json, error } = await fetchJsonOrError(deps, url);
  if (error) return { error: `Failed to load plugin versions. ${error}` };
  const versions = Array.isArray(json) ? json : [];
  return { versions };
}

function chooseBestPluginVersion(versions, { platform, gameVersion }) {
  const normalizedPlatform = normalizePluginLoader(platform);
  const normalizedVersion = String(gameVersion || "").trim();

  const candidates = (Array.isArray(versions) ? versions : [])
    .filter((entry) => Array.isArray(entry?.files) && entry.files.some((file) => file?.url))
    .map((entry) => {
      const loaders = Array.isArray(entry?.loaders) ? entry.loaders.map(normalizePluginLoader).filter(Boolean) : [];
      const gameVersions = Array.isArray(entry?.game_versions) ? entry.game_versions.map((value) => String(value || "").trim()).filter(Boolean) : [];
      let score = 0;
      if (normalizedPlatform && loaders.includes(normalizedPlatform)) score += 8;
      if (!normalizedPlatform && loaders.some((loader) => MODRINTH_PLUGIN_LOADERS.includes(loader))) score += 2;
      if (normalizedVersion && gameVersions.includes(normalizedVersion)) score += 8;
      if (!normalizedVersion && gameVersions.length) score += 1;
      if (entry?.featured) score += 1;
      return { entry, loaders, gameVersions, score };
    })
    .filter((item) => {
      if (normalizedPlatform && !item.loaders.includes(normalizedPlatform)) return false;
      if (normalizedVersion && !item.gameVersions.includes(normalizedVersion)) return false;
      return true;
    });

  const pool = candidates.length ? candidates : (Array.isArray(versions) ? versions : [])
    .filter((entry) => Array.isArray(entry?.files) && entry.files.some((file) => file?.url))
    .map((entry) => ({
      entry,
      loaders: Array.isArray(entry?.loaders) ? entry.loaders.map(normalizePluginLoader).filter(Boolean) : [],
      gameVersions: Array.isArray(entry?.game_versions) ? entry.game_versions.map((value) => String(value || "").trim()).filter(Boolean) : [],
      score: 0,
    }));

  if (!pool.length) return null;

  pool.sort((left, right) => {
    if (right.score !== left.score) return right.score - left.score;
    const versionCompare = compareMcVersionsDesc(left.gameVersions[0] || "", right.gameVersions[0] || "");
    if (versionCompare !== 0) return versionCompare;
    return new Date(right.entry?.date_published || 0).getTime() - new Date(left.entry?.date_published || 0).getTime();
  });
  return pool[0];
}

function getServerAliases(entry) {
  const aliases = new Set();
  for (const value of [
    entry?.name,
    entry?.displayName,
    entry?.id,
    entry?.bot,
    entry?.legacyId,
  ]) {
    const cleaned = String(value || "").trim();
    if (cleaned) aliases.add(cleaned);
  }
  return Array.from(aliases);
}

function getTemplateAliases(entry) {
  const aliases = new Set();
  for (const value of [entry?.id, entry?.name]) {
    const cleaned = String(value || "").trim();
    if (cleaned) aliases.add(cleaned);
  }
  return Array.from(aliases);
}

function resolveTemplateFromList(templates, requestedTemplate) {
  const requested = String(requestedTemplate || "").trim().toLowerCase();
  if (!requested) {
    return { error: "A template is required." };
  }

  const exactMatches = templates.filter((entry) =>
    getTemplateAliases(entry).some((alias) => alias.toLowerCase() === requested)
  );
  if (exactMatches.length === 1) {
    return { entry: exactMatches[0] };
  }
  if (exactMatches.length > 1) {
    return {
      error: "Multiple templates matched that name.",
      candidates: exactMatches.map((entry) => entry.id || entry.name),
    };
  }

  const partialMatches = templates.filter((entry) =>
    getTemplateAliases(entry).some((alias) => alias.toLowerCase().includes(requested))
  );
  if (partialMatches.length === 1) {
    return { entry: partialMatches[0] };
  }
  if (partialMatches.length > 1) {
    return {
      error: "The template name is ambiguous.",
      candidates: partialMatches.slice(0, 10).map((entry) => entry.id || entry.name),
    };
  }

  return { error: "No template matched that name." };
}

function getNodeAliases(entry) {
  const aliases = new Set();
  for (const value of [entry?.id, entry?.uuid, entry?.name]) {
    const cleaned = String(value || "").trim();
    if (cleaned) aliases.add(cleaned);
  }
  return Array.from(aliases);
}

function resolveNodeFromList(nodes, requestedNode) {
  const requested = String(requestedNode || "").trim().toLowerCase();
  if (!requested) {
    return { error: "A node is required." };
  }

  const exactMatches = nodes.filter((entry) =>
    getNodeAliases(entry).some((alias) => alias.toLowerCase() === requested)
  );
  if (exactMatches.length === 1) {
    return { entry: exactMatches[0] };
  }
  if (exactMatches.length > 1) {
    return {
      error: "Multiple nodes matched that name.",
      candidates: exactMatches.map((entry) => entry.name || entry.id || entry.uuid),
    };
  }

  const partialMatches = nodes.filter((entry) =>
    getNodeAliases(entry).some((alias) => alias.toLowerCase().includes(requested))
  );
  if (partialMatches.length === 1) {
    return { entry: partialMatches[0] };
  }
  if (partialMatches.length > 1) {
    return {
      error: "The node name is ambiguous.",
      candidates: partialMatches.slice(0, 10).map((entry) => entry.name || entry.id || entry.uuid),
    };
  }

  return { error: "No node matched that name." };
}

function getNodeResourceLimits(node) {
  const buildConfig = node?.buildConfig && typeof node.buildConfig === "object" ? node.buildConfig : {};
  const ramMb = coerceNumber(node?.ram_mb ?? buildConfig.ram_mb ?? buildConfig.ramMb) || 0;
  const cpuCores = coerceNumber(node?.cpu_cores ?? buildConfig.cpu_cores ?? buildConfig.cpuCores) || 0;
  const diskGb = coerceNumber(node?.disk_gb ?? buildConfig.disk_gb ?? buildConfig.diskGb) || 0;
  return {
    ramMb,
    cpuCores,
    diskGb,
    diskMb: diskGb > 0 ? Math.round(diskGb * 1024) : 0,
  };
}

function describeNodePorts(node) {
  const ports = node?.ports && typeof node.ports === "object" ? node.ports : {};
  if (ports.mode === "range" && coerceNumber(ports.start) > 0 && coerceNumber(ports.count) > 0) {
    const start = Number(ports.start);
    const end = start + Number(ports.count) - 1;
    return {
      mode: "range",
      start,
      end,
      description: `${start}-${end}`,
    };
  }
  if (ports.mode === "list" && Array.isArray(ports.ports) && ports.ports.length > 0) {
    const normalizedPorts = ports.ports
      .map((value) => Number(value))
      .filter((value) => Number.isInteger(value) && value > 0)
      .slice(0, 50);
    return {
      mode: "list",
      ports: normalizedPorts,
      description: normalizedPorts.slice(0, 12).join(", ") + (normalizedPorts.length > 12 ? "..." : ""),
    };
  }
  return {
    mode: "unknown",
    description: "not declared",
  };
}

async function createAccessibleServerCache(deps, userEmail) {
  const list = (await deps.loadServersIndex()) || [];
  const user = await deps.findUserByEmail(userEmail);
  if (user?.admin) return list.filter(Boolean);

  const accessList = (await deps.getAccessListForEmail(userEmail)) || [];
  const lowered = new Set(accessList.map((item) => String(item || "").trim().toLowerCase()));
  if (lowered.has("all")) return list.filter(Boolean);

  return list.filter((entry) => entry?.name && lowered.has(String(entry.name).toLowerCase()));
}

function resolveServerFromList(accessibleServers, requestedServer) {
  const requested = String(requestedServer || "").trim().toLowerCase();
  if (!requested) {
    return { error: "A server name is required." };
  }

  const exactMatches = accessibleServers.filter((entry) =>
    getServerAliases(entry).some((alias) => alias.toLowerCase() === requested)
  );
  if (exactMatches.length === 1) {
    return { entry: exactMatches[0] };
  }
  if (exactMatches.length > 1) {
    return {
      error: "Multiple servers matched that name.",
      candidates: exactMatches.map((entry) => entry.name),
    };
  }

  const partialMatches = accessibleServers.filter((entry) =>
    getServerAliases(entry).some((alias) => alias.toLowerCase().includes(requested))
  );
  if (partialMatches.length === 1) {
    return { entry: partialMatches[0] };
  }
  if (partialMatches.length > 1) {
    return {
      error: "The server name is ambiguous.",
      candidates: partialMatches.slice(0, 10).map((entry) => entry.name),
    };
  }

  return { error: "No accessible server matched that name." };
}

async function performPowerAction(deps, entry, action) {
  const node = await deps.findNodeByIdOrName(entry.nodeId);
  if (!node) {
    return buildMaskedError("The node for this server could not be found.");
  }

  const baseUrl = deps.buildNodeBaseUrl(node.address, node.api_port || 8080, node.ssl_enabled);
  if (!baseUrl) {
    return buildMaskedError("The server node address is invalid.");
  }

  const headers = deps.nodeAuthHeadersFor(node, true);
  let payload = null;
  let timeoutMs = 20_000;

  if (action === "start") {
    const defaultPort = deps.normalizeTemplateId(entry.template) === "minecraft" ? 25565 : 3001;
    const chosenPort = entry.port ?? defaultPort;
    const hostPort = deps.normalizeTemplateId(entry.template) === "minecraft"
      ? deps.clampPort(chosenPort)
      : deps.clampAppPort(chosenPort, defaultPort);
    payload = { hostPort };
    timeoutMs = 120_000;
  }

  const response = await deps.httpRequestJson(
    `${baseUrl}/v1/servers/${encodeURIComponent(entry.name)}/${encodeURIComponent(action)}`,
    "POST",
    headers,
    payload,
    timeoutMs
  );

  if (response.status !== 200 || !(response.json && (response.json.ok === true || response.json.ok === undefined))) {
    const detail = response.json?.error || response.json?.detail || `node status ${response.status}`;
    return buildMaskedError(`The node rejected the ${action} request.`, { detail });
  }

  if (typeof deps.upsertServerIndexEntry === "function") {
    try {
      const nextStatus =
        action === "start" || action === "restart"
          ? "online"
          : action === "stop" || action === "kill"
            ? "stopped"
            : entry.status;
      await deps.upsertServerIndexEntry({ ...entry, status: nextStatus });
    } catch {
    }
  }

  return buildSuccess({
    server: entry.name,
    action,
    status: action === "start" || action === "restart" ? "online" : "stopped",
  });
}

function createDashboardAssistantToolRunner(deps) {
  if (!deps || typeof deps !== "object") {
    throw new Error("Dashboard assistant tool dependencies are required.");
  }

  return async function runTool({ name, args, userEmail, userIp, clientActions }) {
    const toolName = String(name || "").trim();
    const parsedArgs = parseToolArguments(args);
    const accessibleServers = await createAccessibleServerCache(deps, userEmail);

    const resolveServerOrError = async (requestedServer, permKey = null) => {
      const resolved = resolveServerFromList(accessibleServers, requestedServer);
      if (!resolved.entry) {
        return { error: buildMaskedError(resolved.error || "Server not found.", resolved.candidates ? { candidates: resolved.candidates } : {}) };
      }

      const entry = resolved.entry;
      const perms = await deps.getEffectivePermsForUserOnServer(userEmail, entry.name);
      if (permKey && !perms[permKey]) {
        return {
          error: buildMaskedError(`You do not have permission to ${permKey.replace(/_/g, " ")} on ${entry.name}.`),
        };
      }

      return { entry, perms };
    };

    switch (toolName) {
      case "list_templates": {
        const templates = (typeof deps.loadTemplatesFile === "function" ? deps.loadTemplatesFile() : []) || [];
        return buildSuccess({
          templates: templates.slice(0, 100).map((entry) => ({
            id: entry?.id || "",
            name: entry?.name || entry?.id || "",
            description: entry?.description || "",
            defaultPort: entry?.defaultPort ?? null,
          })),
        });
      }

      case "list_nodes": {
        const currentUser = await deps.findUserByEmail(userEmail);
        if (!(currentUser && currentUser.admin)) {
          return buildMaskedError("Only administrators can create servers on nodes.");
        }

        const nodes = (typeof deps.loadNodes === "function" ? await deps.loadNodes() : []) || [];
        return buildSuccess({
          nodes: nodes.slice(0, 100).map((entry) => ({
            ...getNodeResourceLimits(entry),
            id: entry?.id || null,
            uuid: entry?.uuid || null,
            name: entry?.name || entry?.id || entry?.uuid || "",
            address: entry?.address || null,
            online: !!entry?.online,
            apiPort: entry?.api_port ?? 8080,
            portAllocation: describeNodePorts(entry),
          })),
        });
      }

      case "create_server": {
        const currentUser = await deps.findUserByEmail(userEmail);
        if (!(currentUser && currentUser.admin)) {
          return buildMaskedError("Only administrators can create new servers.");
        }

        const rawName = String(parsedArgs.name || "").trim();
        const name = typeof deps.sanitizeServerName === "function" ? deps.sanitizeServerName(rawName) : rawName;
        if (!name) {
          return buildMaskedError("A valid server name is required.");
        }

        const rawDisplayName = String(parsedArgs.displayName || rawName || "").trim();
        const displayName = typeof deps.sanitizeDisplayName === "function"
          ? (deps.sanitizeDisplayName(rawDisplayName) || name)
          : (rawDisplayName || name);

        const templates = (typeof deps.loadTemplatesFile === "function" ? deps.loadTemplatesFile() : []) || [];
        const resolvedTemplate = resolveTemplateFromList(templates, parsedArgs.templateId);
        if (!resolvedTemplate.entry) {
          return buildMaskedError(
            resolvedTemplate.error || "Template not found.",
            resolvedTemplate.candidates ? { candidates: resolvedTemplate.candidates } : {}
          );
        }
        const template = resolvedTemplate.entry;
        const normalizedTemplateId = typeof deps.normalizeTemplateId === "function"
          ? deps.normalizeTemplateId(template.id || parsedArgs.templateId)
          : String(template.id || parsedArgs.templateId || "").trim().toLowerCase();

        const nodes = (typeof deps.loadNodes === "function" ? await deps.loadNodes() : []) || [];
        const resolvedNode = resolveNodeFromList(nodes, parsedArgs.nodeId);
        if (!resolvedNode.entry) {
          return buildMaskedError(
            resolvedNode.error || "Node not found.",
            resolvedNode.candidates ? { candidates: resolvedNode.candidates } : {}
          );
        }
        const node = resolvedNode.entry;
        if (node?.online === false) {
          return buildMaskedError("That node is offline right now.");
        }

        let hostPortRaw = coerceNumber(parsedArgs.hostPort);
        if (hostPortRaw == null && template.defaultPort != null) {
          hostPortRaw = coerceNumber(template.defaultPort);
        }
        if (hostPortRaw == null) {
          return buildMaskedError("A port is required for that server. Tell me which port to allocate, or explicitly say to use defaults if the template has one.");
        }
        if (!Number.isInteger(hostPortRaw) || hostPortRaw < 1 || hostPortRaw > 65535) {
          return buildMaskedError("The port must be a whole number between 1 and 65535.");
        }

        const resolvedPort = hostPortRaw != null ? Number(hostPortRaw) : (coerceNumber(template.defaultPort) || 0);
        if (resolvedPort > 0 && typeof deps.isPortInNodeAllocation === "function" && !deps.isPortInNodeAllocation(node, resolvedPort)) {
          return buildMaskedError(`Port ${resolvedPort} is not in this node's allocated ports.`, {
            allocation: describeNodePorts(node),
          });
        }

        const inputResources = parsedArgs.resources && typeof parsedArgs.resources === "object" ? parsedArgs.resources : {};
        const startFile = normalizedTemplateId === "minecraft"
          ? "server.jar"
          : (normalizedTemplateId === "nodejs" || normalizedTemplateId === "discord-bot"
            ? "index.js"
            : (normalizedTemplateId === "python" ? "main.py" : null));
        const mcFork = normalizedTemplateId === "minecraft"
          ? String(parsedArgs.mcFork || "paper").trim().toLowerCase()
          : undefined;
        const mcVersion = normalizedTemplateId === "minecraft"
          ? String(parsedArgs.mcVersion || "1.21.8").trim()
          : undefined;
        const startupCommand = resolveStartupCommandForCreate({
          requestedStartupCommand: parsedArgs.startupCommand,
          template,
          templateId: template.id || normalizedTemplateId,
          name,
          hostPort: hostPortRaw,
          resources: inputResources,
          startFile,
        }) || null;
        const importUrl = parsedArgs.importUrl ? String(parsedArgs.importUrl).trim() : null;

        if (importUrl) {
          if (typeof deps.isValidArchiveUrl === "function" && !deps.isValidArchiveUrl(importUrl)) {
            return buildMaskedError("The import URL must be http(s) and point to a supported archive file.");
          }
          if (typeof deps.assertSafeRemoteUrl === "function") {
            try {
              await deps.assertSafeRemoteUrl(importUrl);
            } catch (error) {
              return buildMaskedError("The import URL is not allowed.", {
                detail: error?.message || String(error),
              });
            }
          }
        }

        if (startupCommand && resolvedPort > 0 && typeof deps.validateDockerCommandPorts === "function") {
          const portError = deps.validateDockerCommandPorts(startupCommand, resolvedPort);
          if (portError) {
            return buildMaskedError(portError);
          }
        }

        const resources = {};
        if (inputResources.ramMb != null) resources.ramMb = parseInt(inputResources.ramMb, 10) || null;
        if (inputResources.cpuCores != null) resources.cpuCores = parseFloat(inputResources.cpuCores) || null;
        if (inputResources.storageMb != null) resources.storageMb = parseInt(inputResources.storageMb, 10) || null;
        else if (inputResources.storageGb != null) resources.storageMb = (parseInt(inputResources.storageGb, 10) || 0) * 1024 || null;
        if (inputResources.swapMb != null) resources.swapMb = parseInt(inputResources.swapMb, 10);
        if (inputResources.backupsMax != null) resources.backupsMax = parseInt(inputResources.backupsMax, 10) || 0;
        if (inputResources.maxSchedules != null) resources.maxSchedules = parseInt(inputResources.maxSchedules, 10) || 0;

        const nodeLimits = getNodeResourceLimits(node);
        if (resources.ramMb != null && resources.ramMb <= 0) {
          return buildMaskedError("RAM must be greater than 0 MB.");
        }
        if (resources.cpuCores != null && resources.cpuCores <= 0) {
          return buildMaskedError("CPU cores must be greater than 0.");
        }
        if (resources.storageMb != null && resources.storageMb <= 0) {
          return buildMaskedError("Storage must be greater than 0 MB.");
        }
        if (nodeLimits.ramMb > 0 && resources.ramMb != null && resources.ramMb > nodeLimits.ramMb) {
          return buildMaskedError(`RAM cannot exceed this node's limit (${nodeLimits.ramMb} MB).`);
        }
        if (nodeLimits.cpuCores > 0 && resources.cpuCores != null && resources.cpuCores > nodeLimits.cpuCores) {
          return buildMaskedError(`CPU cannot exceed this node's limit (${nodeLimits.cpuCores} cores).`);
        }
        if (nodeLimits.diskMb > 0 && resources.storageMb != null && resources.storageMb > nodeLimits.diskMb) {
          return buildMaskedError(`Storage cannot exceed this node's limit (${nodeLimits.diskMb} MB).`);
        }

        let sanitizedDocker = sanitizeDockerTemplatePayload(template.docker);
        if (sanitizedDocker && Array.isArray(sanitizedDocker.ports) && sanitizedDocker.ports.length > 1) {
          sanitizedDocker.ports = [sanitizedDocker.ports[0]];
        }

        const payload = {
          name,
          templateId: template.id || normalizedTemplateId,
          mcFork,
          mcVersion,
          hostPort: hostPortRaw,
          docker: sanitizedDocker,
          startupCommand,
          autoStart: true,
          importUrl,
          resources: Object.keys(resources).length > 0 ? resources : null,
        };

        let createResult = null;
        try {
          createResult = await deps.createOnRemoteNode(node, payload);
        } catch (error) {
          return buildMaskedError("Failed to create that server.", {
            detail: error?.message || String(error),
          });
        }
        const resolvedStartupCommand = extractStartupCommandFromNodeCreateResult(createResult) || startupCommand;

        let savedPort = null;
        if (hostPortRaw != null) {
          savedPort = typeof deps.clampPort === "function" ? deps.clampPort(hostPortRaw) : hostPortRaw;
        } else if (template.defaultPort) {
          savedPort = typeof deps.clampPort === "function" ? deps.clampPort(template.defaultPort) : template.defaultPort;
        } else if (normalizedTemplateId === "minecraft") {
          savedPort = 25565;
        } else if (normalizedTemplateId === "nodejs" || normalizedTemplateId === "discord-bot") {
          savedPort = 3000;
        }

        const serverEntry = {
          name,
          displayName,
          template: template.id || normalizedTemplateId,
          mcFork: normalizedTemplateId === "minecraft" ? mcFork : undefined,
          mcVersion: normalizedTemplateId === "minecraft" ? mcVersion : undefined,
          start: normalizedTemplateId === "minecraft"
            ? "server.jar"
            : (normalizedTemplateId === "nodejs" || normalizedTemplateId === "discord-bot"
              ? "index.js"
              : (normalizedTemplateId === "python" ? "main.py" : null)),
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

        if (typeof deps.upsertServerIndexEntry === "function") {
          await deps.upsertServerIndexEntry(serverEntry);
        }

        if (typeof deps.recordActivity === "function") {
          deps.recordActivity(name, "server_create", {
            template: template.id || normalizedTemplateId,
            nodeId: node.uuid || node.id || node.name,
          }, userEmail, userIp);
        }

        return buildSuccess({
          created: true,
          server: {
            name,
            displayName,
            template: template.id || normalizedTemplateId,
            templateName: template.name || template.id || normalizedTemplateId,
            nodeId: node.uuid || node.id || node.name,
            nodeName: node.name || node.id || node.uuid,
            port: savedPort,
          },
        });
      }

      case "list_servers": {
        return buildSuccess({
          servers: accessibleServers.slice(0, 100).map((entry) => ({
            name: entry.name,
            displayName: entry.displayName || entry.name,
            status: entry.status || "unknown",
            template: entry.template || "custom",
          })),
        });
      }

      case "inspect_server": {
        const { entry, perms, error } = await resolveServerOrError(parsedArgs.server);
        if (error) return error;

        return buildSuccess({
          server: {
            name: entry.name,
            displayName: entry.displayName || entry.name,
            status: entry.status || "unknown",
            template: entry.template || "custom",
            nodeId: entry.nodeId || null,
            ip: entry.ip || null,
            port: entry.port ?? null,
            mcFork: entry.mcFork || null,
            mcVersion: entry.mcVersion || null,
            permissions: perms,
          },
        });
      }

      case "install_plugin": {
        const { entry, perms, error } = await resolveServerOrError(parsedArgs.server, "files_create");
        if (error) return error;

        if (!entry?.nodeId) {
          return buildMaskedError("This server is not attached to a node.");
        }

        if (typeof deps.applyRemoteAssetToServer !== "function") {
          return buildMaskedError("Plugin installation is not available here.");
        }

        const requestedPlugin = String(parsedArgs.pluginName || "").trim();
        if (!requestedPlugin) {
          return buildMaskedError("A plugin name is required.");
        }

        const projectResult = await resolveModrinthProject(deps, requestedPlugin);
        if (!projectResult.project) {
          return buildMaskedError(projectResult.error || "Plugin not found.");
        }

        const inferredPlatform = normalizePluginLoader(parsedArgs.platform || inferServerPluginLoader(entry) || "paper");
        const inferredGameVersion = String(parsedArgs.gameVersion || inferServerMinecraftVersion(entry) || "").trim();

        const versionResult = await fetchModrinthProjectVersions(deps, projectResult.project.id);
        if (!versionResult.versions) {
          return buildMaskedError(versionResult.error || "Failed to load plugin versions.");
        }

        const chosen = chooseBestPluginVersion(versionResult.versions, {
          platform: inferredPlatform,
          gameVersion: inferredGameVersion,
        });
        if (!chosen) {
          return buildMaskedError(`No compatible Modrinth version was found for ${projectResult.project.title}.`, {
            platform: inferredPlatform || null,
            gameVersion: inferredGameVersion || null,
          });
        }

        const file = chosen.entry.files.find((item) => item?.primary && item?.url) || chosen.entry.files.find((item) => item?.url);
        if (!file?.url) {
          return buildMaskedError("That plugin version does not provide a downloadable file.");
        }

        const pluginFileName = String(file.filename || file.url.split("/").pop() || `${projectResult.project.slug || "plugin"}.jar`).trim();
        const applyResult = await deps.applyRemoteAssetToServer({
          serverName: entry.name,
          entry,
          url: file.url,
          destPath: `plugins/${pluginFileName}`,
        });

        if (!applyResult?.ok) {
          return buildMaskedError("Failed to install that plugin.", {
            detail: applyResult?.detail || applyResult?.error || "unknown-error",
          });
        }

        if (typeof deps.recordActivity === "function") {
          deps.recordActivity(entry.name, "plugin_install", {
            plugin: projectResult.project.slug || projectResult.project.title,
            file: pluginFileName,
            platform: inferredPlatform || null,
            gameVersion: inferredGameVersion || null,
          }, userEmail, userIp);
        }

        return buildSuccess({
          server: entry.name,
          plugin: {
            name: projectResult.project.title,
            slug: projectResult.project.slug || "",
            loader: chosen.loaders[0] || inferredPlatform || null,
            gameVersion: chosen.gameVersions[0] || inferredGameVersion || null,
            versionId: chosen.entry.id || null,
            fileName: pluginFileName,
          },
          destPath: `plugins/${pluginFileName}`,
          permissions: perms,
        });
      }

      case "request_delete_server": {
        const currentUser = await deps.findUserByEmail(userEmail);
        if (!(currentUser && currentUser.admin)) {
          return buildMaskedError("Only administrators can delete servers.");
        }

        const resolved = resolveServerFromList(accessibleServers, parsedArgs.server);
        const serverName = resolved.entry?.name || String(parsedArgs.server || "").trim();
        if (!serverName) {
          return buildMaskedError("A server name is required.");
        }

        if (Array.isArray(clientActions)) {
          clientActions.push({
            type: "confirm_delete_server",
            server: serverName,
            displayName: resolved.entry?.displayName || serverName,
          });
        }

        return buildSuccess({
          server: serverName,
          confirmationRequired: true,
        });
      }

      case "list_files": {
        const { entry, error } = await resolveServerOrError(parsedArgs.server, "files_read");
        if (error) return error;

        const ctx = await deps.resolveRemoteFsContext(entry.name);
        if (!ctx.remote || !ctx.node) {
          return buildMaskedError("This server is not attached to a node.");
        }

        try {
          const fullPath = safeJoinUnix(ctx.baseDir, normalizeRelativePath(parsedArgs.path));
          const { status, json } = await deps.nodeFsPost(ctx.node, "/v1/fs/list", { path: fullPath }, 60_000);
          if (status !== 200 || !json || !json.ok) {
            return buildMaskedError("Failed to list files for that path.", {
              detail: json?.error || `status ${status}`,
            });
          }

          return buildSuccess({
            server: entry.name,
            path: normalizeRelativePath(parsedArgs.path),
            entries: (Array.isArray(json.entries) ? json.entries : []).slice(0, 200).map((item) => ({
              name: item?.name || "",
              isDir: !!item?.isDir,
              size: item?.size ?? null,
              modifiedAt: item?.modifiedAt || item?.mtime || null,
            })),
          });
        } catch (err) {
          return buildMaskedError(err?.message || "Invalid path.");
        }
      }

      case "read_file": {
        const { entry, error } = await resolveServerOrError(parsedArgs.server, "files_read");
        if (error) return error;

        const filePath = normalizeRelativePath(parsedArgs.path);
        if (!filePath) {
          return buildMaskedError("A file path is required.");
        }

        const ctx = await deps.resolveRemoteFsContext(entry.name);
        if (!ctx.remote || !ctx.node) {
          return buildMaskedError("This server is not attached to a node.");
        }

        try {
          const fullPath = safeJoinUnix(ctx.baseDir, filePath);
          const { status, json } = await deps.nodeFsPost(ctx.node, "/v1/fs/read", { path: fullPath, encoding: "utf8" }, 120_000);
          if (status !== 200 || !json || !json.ok) {
            return buildMaskedError("Failed to read that file.", {
              detail: json?.error || `status ${status}`,
            });
          }

          const preview = truncateText(json.content || "");
          return buildSuccess({
            server: entry.name,
            path: filePath,
            content: preview.content,
            truncated: preview.truncated,
            originalLength: preview.originalLength,
          });
        } catch (err) {
          return buildMaskedError(err?.message || "Invalid path.");
        }
      }

      case "write_file": {
        const { entry, error } = await resolveServerOrError(parsedArgs.server, "files_create");
        if (error) return error;

        const filePath = normalizeRelativePath(parsedArgs.path);
        if (!filePath) {
          return buildMaskedError("A file path is required.");
        }

        const ctx = await deps.resolveRemoteFsContext(entry.name);
        if (!ctx.remote || !ctx.node) {
          return buildMaskedError("This server is not attached to a node.");
        }

        try {
          const fullPath = safeJoinUnix(ctx.baseDir, filePath);
          const { status, json } = await deps.nodeFsPost(
            ctx.node,
            "/v1/fs/write",
            { path: fullPath, content: String(parsedArgs.content ?? ""), encoding: "utf8" },
            120_000
          );
          if (status !== 200 || !json || !json.ok) {
            return buildMaskedError("Failed to write that file.", {
              detail: json?.error || `status ${status}`,
            });
          }

          if (typeof deps.recordActivity === "function") {
            deps.recordActivity(entry.name, "file_edit", { path: filePath }, userEmail, userIp);
          }

          return buildSuccess({ server: entry.name, path: filePath, written: true });
        } catch (err) {
          return buildMaskedError(err?.message || "Invalid path.");
        }
      }

      case "create_directory": {
        const { entry, error } = await resolveServerOrError(parsedArgs.server, "files_create");
        if (error) return error;

        const dirPath = normalizeRelativePath(parsedArgs.path);
        if (!dirPath) {
          return buildMaskedError("A directory path is required.");
        }

        const ctx = await deps.resolveRemoteFsContext(entry.name);
        if (!ctx.remote || !ctx.node) {
          return buildMaskedError("This server is not attached to a node.");
        }

        try {
          const fullPath = safeJoinUnix(ctx.baseDir, dirPath);
          const { status, json } = await deps.nodeFsPost(ctx.node, "/v1/fs/mkdir", { path: fullPath }, 120_000);
          if (status !== 200 || !json || !json.ok) {
            return buildMaskedError("Failed to create that directory.", {
              detail: json?.error || `status ${status}`,
            });
          }

          if (typeof deps.recordActivity === "function") {
            deps.recordActivity(entry.name, "file_mkdir", { path: dirPath }, userEmail, userIp);
          }

          return buildSuccess({ server: entry.name, path: dirPath, created: true });
        } catch (err) {
          return buildMaskedError(err?.message || "Invalid path.");
        }
      }

      case "rename_path": {
        const { entry, error } = await resolveServerOrError(parsedArgs.server, "files_rename");
        if (error) return error;

        const source = normalizeRelativePath(parsedArgs.source);
        const destination = normalizeRelativePath(parsedArgs.destination);
        if (!source || !destination) {
          return buildMaskedError("Both source and destination paths are required.");
        }

        const ctx = await deps.resolveRemoteFsContext(entry.name);
        if (!ctx.remote || !ctx.node) {
          return buildMaskedError("This server is not attached to a node.");
        }

        try {
          const src = safeJoinUnix(ctx.baseDir, source);
          const dest = safeJoinUnix(ctx.baseDir, destination);
          const { status, json } = await deps.nodeFsPost(ctx.node, "/v1/fs/rename", { src, dest }, 120_000);
          if (status !== 200 || !json || !json.ok) {
            return buildMaskedError("Failed to rename that path.", {
              detail: json?.error || `status ${status}`,
            });
          }

          if (typeof deps.recordActivity === "function") {
            deps.recordActivity(entry.name, "file_rename", { src: source, dest: destination }, userEmail, userIp);
          }

          return buildSuccess({ server: entry.name, source, destination, renamed: true });
        } catch (err) {
          return buildMaskedError(err?.message || "Invalid path.");
        }
      }

      case "delete_path": {
        const { entry, error } = await resolveServerOrError(parsedArgs.server, "files_delete");
        if (error) return error;

        const targetPath = normalizeRelativePath(parsedArgs.path);
        if (!targetPath) {
          return buildMaskedError("A path is required.");
        }

        const ctx = await deps.resolveRemoteFsContext(entry.name);
        if (!ctx.remote || !ctx.node) {
          return buildMaskedError("This server is not attached to a node.");
        }

        try {
          const fullPath = safeJoinUnix(ctx.baseDir, targetPath);
          const { status, json } = await deps.nodeFsPost(
            ctx.node,
            "/v1/fs/delete",
            { path: fullPath, isDir: !!parsedArgs.isDirectory },
            120_000
          );
          if (status !== 200 || !json || !json.ok) {
            return buildMaskedError("Failed to delete that path.", {
              detail: json?.error || `status ${status}`,
            });
          }

          if (typeof deps.recordActivity === "function") {
            deps.recordActivity(entry.name, "file_delete", { path: targetPath, isDir: !!parsedArgs.isDirectory }, userEmail, userIp);
          }

          return buildSuccess({ server: entry.name, path: targetPath, deleted: true });
        } catch (err) {
          return buildMaskedError(err?.message || "Invalid path.");
        }
      }

      case "power_server": {
        const action = String(parsedArgs.action || "").trim().toLowerCase();
        const requiredPerm = action === "start" ? "server_start" : "server_stop";
        if (!["start", "stop", "restart", "kill"].includes(action)) {
          return buildMaskedError("Invalid power action.");
        }

        const { entry, error } = await resolveServerOrError(parsedArgs.server, requiredPerm);
        if (error) return error;

        if (!entry?.nodeId) {
          return buildMaskedError("This server is not attached to a node.");
        }

        const result = await performPowerAction(deps, entry, action);
        if (result.ok && typeof deps.recordActivity === "function") {
          deps.recordActivity(entry.name, `server_${action}`, null, userEmail, userIp);
        }
        return result;
      }

      case "open_account_flow": {
        const flow = String(parsedArgs.flow || "").trim();
        if (![
          "change_password",
          "change_email",
          "change_2fa",
          "recover_password",
          "recover_email",
          "recover_2fa",
        ].includes(flow)) {
          return buildMaskedError("Invalid account flow.");
        }

        if (Array.isArray(clientActions)) {
          clientActions.push({ type: "open_account_flow", flow });
        }

        return buildSuccess({ flow, browserActionQueued: true });
      }

      default:
        return buildMaskedError(`Unknown tool: ${toolName}`);
    }
  };
}

module.exports = {
  TOOL_DEFINITIONS,
  buildSystemPrompt,
  createDashboardAssistantToolRunner,
};
