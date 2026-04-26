"use strict";

const https = require("https");
const crypto = require("crypto");
const FormData = require("form-data");
const {
  TOOL_DEFINITIONS,
  POWER_TOOL_DEFINITIONS,
  buildSystemPrompt,
  createDashboardAssistantToolRunner,
} = require("./dashboard-assistant-tools");

const DEFAULT_CHAT_TITLE = "ADPanel Assistant";
const DEFAULT_GROQ_MODEL = process.env.DASHBOARD_ASSISTANT_GROQ_MODEL || "llama-3.1-8b-instant";
const DEFAULT_GROQ_TRANSCRIPTION_MODEL = process.env.DASHBOARD_ASSISTANT_GROQ_STT_MODEL || "whisper-large-v3";
const DEFAULT_GOOGLE_MODEL = process.env.DASHBOARD_ASSISTANT_GOOGLE_MODEL || "gemini-2.5-flash-lite";
const DEFAULT_GOOGLE_TRANSCRIPTION_MODEL = process.env.DASHBOARD_ASSISTANT_GOOGLE_STT_MODEL || "gemini-2.5-flash";
const VISIBLE_CHAT_HISTORY_LIMIT = 80;
const CHAT_HISTORY_LIMIT = VISIBLE_CHAT_HISTORY_LIMIT;
const MAX_CHAT_MESSAGES = VISIBLE_CHAT_HISTORY_LIMIT;
const MODEL_CONTEXT_RECENT_MESSAGE_LIMIT = 16;
const MODEL_CONTEXT_RECENT_MESSAGE_CHAR_LIMIT = 520;
const MODEL_CONTEXT_NEAR_MESSAGE_CHAR_LIMIT = 760;
const MODEL_CONTEXT_LATEST_MESSAGE_CHAR_LIMIT = 1400;
const MODEL_CONTEXT_OLDER_TRANSCRIPT_COUNT = 10;
const MODEL_CONTEXT_OLDER_TRANSCRIPT_TOTAL_LIMIT = 1400;
const ACCESSIBLE_SERVERS_CACHE_TTL_MS = 15_000;
const ASSISTANT_MAX_LOOPS = 6;
const AUDIO_UPLOAD_LIMIT_BYTES = parseInt(process.env.DASHBOARD_ASSISTANT_AUDIO_LIMIT_BYTES || "", 10) || 15 * 1024 * 1024;
const GOOGLE_OPENAI_CHAT_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions";
const GOOGLE_INTERACTIONS_URL = "https://generativelanguage.googleapis.com/v1beta/interactions";
const POWER_ONLY_ASSISTANT = true;
const PROVIDER_LABELS = Object.freeze({
  groq: "Groq",
  google: "Google AI Studio",
});
const SUPPORTED_PROVIDERS = new Set(Object.keys(PROVIDER_LABELS));
const ASSISTANT_PROVIDER_SECRET_ENV_KEYS = Object.freeze({
  groq: {
    encrypted: "DASHBOARD_ASSISTANT_GROQ_API_KEY_ENCRYPTED",
    legacy: ["GROQ_API_KEY"],
  },
  google: {
    encrypted: "DASHBOARD_ASSISTANT_GOOGLE_AI_API_KEY_ENCRYPTED",
    legacy: ["GOOGLE_AI_STUDIO_API_KEY", "GOOGLE_AI_KEY"],
  },
});
const ASSISTANT_PROVIDER_SECRET_VERSION = "enc-v1";
const ASSISTANT_PROVIDER_SECRET_CONTEXT = "adpanel:dashboard-assistant:provider-key:v1";
const ASSISTANT_SERVER_PAGE_SECTION_LABELS = Object.freeze({
  console: "console",
  info: "info",
  files: "files",
  activity: "activity",
  backups: "backups",
  scheduler: "scheduler",
  store: "store",
  resource_stats: "resource stats",
  subdomains: "subdomains",
  reinstall: "reinstall",
  ai_help: "AI help",
});
const ASSISTANT_SETTINGS_PANEL_LABELS = Object.freeze({
  preferences: "Preferences",
  customization: "Customization",
  account: "Account",
  security: "Security",
  databases: "Databases",
  nodes: "Nodes",
  templates: "Templates",
  servers: "Servers",
  webhooks: "Webhooks",
  panelinfo: "Panel Info",
});
const ASSISTANT_SETTINGS_ACTION_LABELS = Object.freeze({
  create_user: "Create User",
  branding: "Branding",
  login_watermark: "Login Watermark",
  login_background: "Login Background",
  alert: "Alerts",
  quick_action_admin: "Admin Quick Actions",
  quick_action_user: "User Quick Actions",
  database_setup: "Database Setup",
  pgadmin_setup: "pgAdmin Setup",
  mongodb_setup: "MongoDB Setup",
  captcha: "Captcha",
  maintenance: "Maintenance",
  create_node: "Create Node",
  create_template: "Create Template",
  add_webhook: "Add Webhook",
});
const accessibleServersForAssistantCache = new Map();
const ASSISTANT_PROVIDER_HTTPS_AGENTS = Object.freeze({
  groq: new https.Agent({
    keepAlive: true,
    keepAliveMsecs: 10_000,
    maxSockets: 24,
    maxFreeSockets: 12,
    timeout: 90_000,
  }),
  google: new https.Agent({
    keepAlive: true,
    keepAliveMsecs: 10_000,
    maxSockets: 24,
    maxFreeSockets: 12,
    timeout: 90_000,
  }),
});

function buildStoredKeyFieldValue(value) {
  const clean = String(value || "").trim();
  if (!clean) return "";
  return "\u2022".repeat(clean.length);
}

function normalizeMessageContent(content) {
  if (Array.isArray(content)) {
    return content
      .map((part) => {
        if (typeof part === "string") return part;
        if (part && typeof part.text === "string") return part.text;
        if (part && part.type === "text" && typeof part.text === "string") return part.text;
        return "";
      })
      .join("")
      .trim();
  }
  return String(content || "").trim();
}

function stripAssistantLookupDiacritics(value) {
  return String(value || "").normalize("NFKD").replace(/[\u0300-\u036f]/g, "");
}

function normalizeAssistantLookupText(value) {
  return stripAssistantLookupDiacritics(value)
    .toLowerCase()
    .replace(/[`"'“”‘’()[\]{}<>]/g, " ")
    .replace(/[_./-]+/g, " ")
    .replace(/[^a-z0-9 ]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function compactAssistantLookupText(value) {
  return normalizeAssistantLookupText(value).replace(/\s+/g, "");
}

function getAssistantServerMentionAliases(entry) {
  const aliases = new Set();
  for (const value of [entry?.name, entry?.displayName, entry?.id, entry?.bot, entry?.legacyId]) {
    const clean = String(value || "").trim();
    if (!clean) continue;
    aliases.add(clean);
    const spaced = clean.replace(/[_./-]+/g, " ").replace(/\s+/g, " ").trim();
    if (spaced) aliases.add(spaced);
  }
  return Array.from(aliases);
}

function inferAssistantServerMention(message, accessibleServers) {
  const raw = String(message || "").trim();
  if (!raw) return null;

  const normalizedMessage = normalizeAssistantLookupText(raw);
  const compactMessage = compactAssistantLookupText(raw);
  if (!normalizedMessage && !compactMessage) return null;

  const wrappedMentions = Array.from(raw.matchAll(/["'`“”(\[]([^"'`“”()[\]]{1,120})["'`”)\]]/g))
    .map((match) => String(match?.[1] || "").trim())
    .filter(Boolean);
  const normalizedWrapped = wrappedMentions.map((value) => normalizeAssistantLookupText(value)).filter(Boolean);
  const compactWrapped = wrappedMentions.map((value) => compactAssistantLookupText(value)).filter(Boolean);

  const scored = (Array.isArray(accessibleServers) ? accessibleServers : [])
    .filter((entry) => entry?.name)
    .map((entry) => {
      let best = 0;
      for (const alias of getAssistantServerMentionAliases(entry)) {
        const normalizedAlias = normalizeAssistantLookupText(alias);
        const compactAlias = compactAssistantLookupText(alias);
        if (!normalizedAlias && !compactAlias) continue;

        if (normalizedAlias && normalizedWrapped.includes(normalizedAlias)) {
          best = Math.max(best, 1600 + normalizedAlias.length);
        }
        if (compactAlias && compactWrapped.includes(compactAlias)) {
          best = Math.max(best, 1550 + compactAlias.length);
        }

        if (normalizedAlias && normalizedMessage && ` ${normalizedMessage} `.includes(` ${normalizedAlias} `)) {
          best = Math.max(best, 1200 + normalizedAlias.length);
        }
        if (compactAlias && compactAlias.length >= 4 && compactMessage.includes(compactAlias)) {
          best = Math.max(best, 900 + compactAlias.length);
        }

        const aliasTokens = normalizedAlias.split(" ").filter(Boolean);
        if (aliasTokens.length >= 2) {
          const overlap = aliasTokens.filter((token) => ` ${normalizedMessage} `.includes(` ${token} `)).length;
          if (overlap === aliasTokens.length) {
            best = Math.max(best, 650 + aliasTokens.length * 40);
          } else if (overlap > 0) {
            best = Math.max(best, 240 + overlap * 35);
          }
        } else if (aliasTokens.length === 1 && aliasTokens[0].length >= 4 && ` ${normalizedMessage} `.includes(` ${aliasTokens[0]} `)) {
          best = Math.max(best, 520 + aliasTokens[0].length);
        }
      }
      return { entry, score: best };
    })
    .filter((item) => item.score > 0)
    .sort((left, right) => right.score - left.score);

  if (!scored.length) return null;

  const [best, second] = scored;
  if (second && best.score < second.score + 140) {
    return null;
  }
  if (best.score < 520) {
    return null;
  }

  return {
    name: best.entry.name,
    displayName: best.entry.displayName || best.entry.name,
    score: best.score,
  };
}

function sanitizeToken(rawToken) {
  return String(rawToken || "").replace(/\0/g, "").replace(/[\r\n]+/g, "").trim();
}

function sanitizeTranscriptionLanguage(value) {
  const primary = String(value || "").trim().toLowerCase().split(/[-_]/)[0];
  return /^[a-z]{2,3}$/.test(primary) ? primary : "";
}

function sanitizeTranscriptionPrompt(value) {
  const clean = String(value || "").replace(/\s+/g, " ").trim();
  return clean.slice(0, 500);
}

function sanitizeProvider(value) {
  const provider = String(value || "").trim().toLowerCase();
  return SUPPORTED_PROVIDERS.has(provider) ? provider : "";
}

function getProviderLabel(provider) {
  return PROVIDER_LABELS[sanitizeProvider(provider)] || "AI provider";
}

function getAssistantProviderSecretConfig(provider) {
  return ASSISTANT_PROVIDER_SECRET_ENV_KEYS[sanitizeProvider(provider)] || null;
}

function getAssistantProviderSecretMaterial() {
  return sanitizeToken(
    process.env.DASHBOARD_ASSISTANT_ENCRYPTION_SECRET ||
    process.env.SESSION_SECRET ||
    process.env.SECRET_KEY ||
    ""
  );
}

function getAssistantProviderCipherKey() {
  const secretMaterial = getAssistantProviderSecretMaterial();
  if (!secretMaterial) {
    throw new Error("Assistant encryption secret is unavailable.");
  }

  return crypto
    .createHash("sha256")
    .update(ASSISTANT_PROVIDER_SECRET_CONTEXT)
    .update("\0")
    .update(secretMaterial)
    .digest();
}

function encryptAssistantProviderToken(rawToken) {
  const token = sanitizeToken(rawToken);
  if (!token) return "";

  // Provider API keys must stay reversible so the assistant can call Groq/Google later.
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv("aes-256-gcm", getAssistantProviderCipherKey(), iv);
  const ciphertext = Buffer.concat([cipher.update(token, "utf8"), cipher.final()]);
  const authTag = cipher.getAuthTag();

  return [
    ASSISTANT_PROVIDER_SECRET_VERSION,
    iv.toString("base64"),
    authTag.toString("base64"),
    ciphertext.toString("base64"),
  ].join(":");
}

function decryptAssistantProviderToken(payload) {
  const value = sanitizeToken(payload);
  if (!value) return "";

  const [version, ivBase64, authTagBase64, ciphertextBase64] = value.split(":");
  if (
    version !== ASSISTANT_PROVIDER_SECRET_VERSION ||
    !ivBase64 ||
    !authTagBase64 ||
    !ciphertextBase64
  ) {
    throw new Error("Invalid assistant provider token format.");
  }

  const decipher = crypto.createDecipheriv(
    "aes-256-gcm",
    getAssistantProviderCipherKey(),
    Buffer.from(ivBase64, "base64")
  );
  decipher.setAuthTag(Buffer.from(authTagBase64, "base64"));

  const plaintext = Buffer.concat([
    decipher.update(Buffer.from(ciphertextBase64, "base64")),
    decipher.final(),
  ]).toString("utf8");

  return sanitizeToken(plaintext);
}

function detectDirectAccountFlowIntent(message) {
  const text = String(message || "").trim().toLowerCase();
  if (!text) return null;

  const wantsRecovery =
    /\b(recover|recovery|forgot|lost|reset)\b/.test(text) &&
    /\b(code|codes|access|password|email|2fa|two[- ]factor)\b/.test(text);
  const wantsChange =
    /\b(change|update|open|show|edit|manage|configure|setup|set up|turn on|turn off|disable|enable)\b/.test(text);

  if (/\bpassword\b/.test(text)) {
    if (wantsRecovery || /\brecovery code|forgot password|lost password\b/.test(text)) {
      return {
        flow: "recover_password",
        reply: "Opening password recovery.",
      };
    }
    if (wantsChange || /\bchange my password\b/.test(text)) {
      return {
        flow: "change_password",
        reply: "Opening password change.",
      };
    }
  }

  if (/\bemail\b/.test(text)) {
    if (wantsRecovery || /\brecovery code|lost email access|recover email\b/.test(text)) {
      return {
        flow: "recover_email",
        reply: "Opening email recovery.",
      };
    }
    if (wantsChange || /\bchange my email\b/.test(text)) {
      return {
        flow: "change_email",
        reply: "Opening email change.",
      };
    }
  }

  if (/\b2fa\b|\btwo[- ]factor\b|\bauthenticator\b/.test(text)) {
    if (wantsRecovery || /\brecovery code|lost authenticator|recover 2fa|recover two[- ]factor\b/.test(text)) {
      return {
        flow: "recover_2fa",
        reply: "Opening 2FA recovery.",
      };
    }
    if (wantsChange || /\bchange my 2fa\b|\bchange my two[- ]factor\b/.test(text)) {
      return {
        flow: "change_2fa",
        reply: "Opening 2FA settings.",
      };
    }
  }

  return null;
}

function detectDirectCreateServerFlowIntent(message) {
  const text = String(message || "").trim().toLowerCase();
  if (!text) return null;

  if (/\b(how|what|which|why|explain|guide|tutorial|cum|ce|care|de ce)\b/.test(text)) {
    return null;
  }

  const wantsCreateServer =
    (
      /\b(create|make|spin\s*up|deploy|provision|start)\b/.test(text) ||
      /\b(can you create|could you create|help me create|let'?s create|i want to create|want to create|wanna create|would like to create)\b/.test(text) ||
      /\b(vreau|vrea|vreau sa|hai sa|te rog)\b/.test(text) ||
      /\b(creez|creeaza|creezi|creare|deschide|arata)\b/.test(text)
    ) &&
    /\b(server|minecraft|nodejs|python|discord)\b/.test(text);

  if (!wantsCreateServer) {
    return null;
  }

  return {
    reply: "Opening the Create Server window.",
  };
}

function detectDirectBulkPowerIntent(message) {
  const text = String(message || "").trim().toLowerCase();
  if (!text) return null;

  if (/\b(how|what|which|why|explain|guide|tutorial|cum|ce|care|de ce)\b/.test(text)) {
    return null;
  }

  const mentionsServerWords = /\b(server|servers|servere|serverele)\b/.test(text);
  const mentionsBulkQualifier = /\b(?:all|every|all accessible|all my|all the|toate|toți|toate serverele|toate serverele mele|toate serverele la care am acces)\b/.test(text);
  const mentionsBulkPronoun = /\b(?:them all|all of them|pe toate)\b/.test(text);
  const mentionsNonServerTargets = /\b(plugin|plugins|file|files|folder|folders|directory|directories|node|nodes|template|templates)\b/.test(text);
  const mentionsAllServers =
    (mentionsServerWords && mentionsBulkQualifier) ||
    (mentionsBulkPronoun && !mentionsNonServerTargets);

  if (!mentionsAllServers) {
    return null;
  }

  if (/\b(restart|reboot|cycle|bounce|reload|reporneste|repornește)\b/.test(text)) {
    return { action: "restart" };
  }
  if (/\b(kill|force stop|hard stop|omoara|omoară|ucide)\b/.test(text)) {
    return { action: "kill" };
  }
  if (/\b(stop|shutdown|shut down|turn off|power off|take down|opreste|oprește|inchide|închide)\b/.test(text)) {
    return { action: "stop" };
  }
  if (/\b(start|run|boot|turn on|power on|bring up|spin up|porneste|pornește|deschide)\b/.test(text)) {
    return { action: "start" };
  }

  return null;
}

function detectAssistantPowerAction(message) {
  const text = String(message || "").trim().toLowerCase();
  if (!text) return "";
  if (/\b(restart|reboot|cycle|bounce|reload|reporneste|repornește|da restart)\b/.test(text)) return "restart";
  if (/\b(kill|force stop|hard stop|omoara|omoară|ucide|terminate)\b/.test(text)) return "kill";
  if (/\b(stop|shutdown|shut down|turn off|power off|take down|opreste|oprește|inchide|închide|da stop)\b/.test(text)) return "stop";
  if (/\b(start|run|boot|turn on|power on|bring up|spin up|porneste|pornește|da start)\b/.test(text)) return "start";
  return "";
}

function detectDirectSinglePowerIntent(message, serverHint = "") {
  const text = String(message || "").trim();
  if (!text) return null;

  const lower = text.toLowerCase();
  if (/\b(how|what|which|why|explain|guide|tutorial|cum|ce|care|de ce)\b/.test(lower)) {
    return null;
  }

  if (detectDirectBulkPowerIntent(text)) {
    return null;
  }

  const hasNonPowerIntent = /\b(file|files|config|folder|directory|log|logs|console|error|backup|plugin|plugins|mod|mods|edit|change|write|read|show|list|inspect|analy[sz]e|fix|debug|server\.properties)\b/i.test(text);
  if (hasNonPowerIntent) {
    return null;
  }

  const action = detectAssistantPowerAction(text);
  if (!action) return null;

  const server = extractAssistantServerCandidate(text) || String(serverHint || "").trim();
  if (!server) return null;

  return {
    toolName: "power_server",
    args: { server: text, action },
    model: "adpanel-power-router",
  };
}

function messageLooksLikeBusiestLookup(message) {
  const text = String(message || "").trim().toLowerCase();
  if (!text) return false;

  const mentionsTarget = /\b(node|nodes|nod|noduri|server|servers|servere|serverele)\b/.test(text);
  if (!mentionsTarget) return false;

  const mentionsLoadWords = /\b(busiest|busy|most loaded|highest load|load|usage|heaviest|incarcat|încărcat|solicitat|ocupat)\b/.test(text);
  const mentionsSuperlativeMetric =
    /\b(?:uses?|using|consumes?|consuming|has|with)\s+the\s+most\s+(?:cpu|ram|memory|mem|disk|storage|space)\b/.test(text) ||
    /\b(?:most|highest)\s+(?:cpu|ram|memory|mem|disk|storage|space)\b/.test(text) ||
    /\b(?:cel mai mult|cea mai multa|cea mai multă)\s+(?:cpu|ram|memorie|memory|disk|storage|spatiu|spațiu)\b/.test(text);

  return mentionsLoadWords || mentionsSuperlativeMetric;
}

function messageLooksLikeDiagnosisRequest(message) {
  const text = String(message || "").trim().toLowerCase();
  if (!text) return false;

  return (
    /\b(server|servers|servere|serverele)\b/.test(text) &&
    /\b(diagnose|diagnostic|crash|crashed|crashing|lag|lagging|slow|startup|start up|won'?t start|problem|issue|error|failing|failure|why is|why did|de ce|problema|eroare|a cazut|a căzut)\b/.test(text)
  );
}

function messageLooksLikeConsoleCheckRequest(message) {
  const text = String(message || "").trim().toLowerCase();
  if (!text) return false;

  if (messageLooksLikeDiagnosisRequest(text)) return true;

  const mentionsConsole =
    /\b(console|logs?|log output|output|latest log|latest logs|startup log|crash log|stderr|stdout|stack trace|traceback)\b/.test(text);
  const mentionsInspection =
    /\b(check|check on|show|read|look|inspect|review|scan|analy[sz]e|what|why|tell me|see|look into|figure out)\b/.test(text);
  const mentionsFailure =
    /\b(error|errors|crash|crashed|crashing|startup|start up|won'?t start|issue|problem|failing|failure|panic|exception)\b/.test(text);
  const mentionsStartupTimingOrHealth =
    /\b(how many seconds|how long(?: did it take)?|startup time|start time|boot time|time to start|took(?: .*?)? to start|startup seconds|started (?:correctly|good|well|fine|okay|ok)|did it start (?:correctly|well|fine|okay|ok)|running well|run(?:ning)? good|run(?:ning)? fine|running okay|came online|is it healthy|is it ok|is it okay|started healthy|fully started|all good|booted|launched)\b/.test(text);

  return (mentionsConsole && (mentionsInspection || mentionsFailure)) || mentionsStartupTimingOrHealth;
}

function messageLooksLikeMinecraftBatchPropertyUpdate(message) {
  const text = String(message || "").trim().toLowerCase();
  if (!text) return false;

  return (
    (
      /\bminecraft\b/.test(text) ||
      /\bserver\.properties\b/.test(text) ||
      /\b(online[- ]mode|max[- ]players|motd|pvp|white[- ]list|whitelist|difficulty|view[- ]distance|simulation[- ]distance|spawn[- ]protection|allow[- ]flight|hardcore)\b/.test(text)
    ) &&
    /\b(all|every|all accessible|all my|them all|all of them|toate|pe toate)\b/.test(text) &&
    /\b(set|change|update|edit|replace|switch|make|put|pune|seteaza|setează|schimba|schimbă|modifica|modifică)\b/.test(text) &&
    (
      /\b(server\.properties|properties|config)\b/.test(text) ||
      /[a-z0-9_.-]+\s*(?:=|to|pe)\s*[^\s]+/.test(text)
    )
  );
}

function messageLooksLikeStructuredMinecraftPropertyMutation(message) {
  const text = String(message || "").trim();
  if (!text) return false;

  const lower = text.toLowerCase();
  const mentionsMinecraftContext =
    /\bminecraft\b/.test(lower) ||
    /\bserver\.properties\b/.test(lower) ||
    MINECRAFT_PROPERTY_ALIASES.some((entry) => entry.pattern.test(text));
  if (!mentionsMinecraftContext) return false;

  return /\b(set|change|update|edit|replace|switch|make|put|pune|seteaza|setează|schimba|schimbă|modifica|modifică)\b/i.test(text);
}

const MINECRAFT_PROPERTY_ALIASES = Object.freeze([
  { pattern: /\bonline[- ]mode\b/i, key: "online-mode" },
  { pattern: /\bmax[- ]players\b/i, key: "max-players" },
  { pattern: /\bview[- ]distance\b/i, key: "view-distance" },
  { pattern: /\bsimulation[- ]distance\b/i, key: "simulation-distance" },
  { pattern: /\bspawn[- ]protection\b/i, key: "spawn-protection" },
  { pattern: /\ballow[- ]flight\b/i, key: "allow-flight" },
  { pattern: /\bwhite[- ]list\b/i, key: "white-list" },
  { pattern: /\bwhitelist\b/i, key: "white-list" },
  { pattern: /\bmotd\b/i, key: "motd" },
  { pattern: /\bdifficulty\b/i, key: "difficulty" },
  { pattern: /\bpvp\b/i, key: "pvp" },
  { pattern: /\bhardcore\b/i, key: "hardcore" },
]);

function inferAssistantMetric(message) {
  const text = String(message || "").trim().toLowerCase();
  if (/\bcpu\b/.test(text)) return "cpu";
  if (/\b(memory|ram)\b/.test(text)) return "memory";
  if (/\b(disk|storage|space)\b/.test(text)) return "disk";
  return "balanced";
}

function inferAssistantMetricDetails(message) {
  const text = String(message || "").trim().toLowerCase();
  const includeMetrics = [];

  if (/\bcpu\b/.test(text)) includeMetrics.push("cpu");
  if (/\b(memory|ram)\b/.test(text)) includeMetrics.push("memory");
  if (/\b(disk|storage|space)\b/.test(text)) includeMetrics.push("disk");

  return {
    metric: includeMetrics.length === 1 ? includeMetrics[0] : "balanced",
    includeMetrics,
  };
}

function detectDirectLoadLookupIntent(message) {
  const text = String(message || "").trim().toLowerCase();
  if (!text || !messageLooksLikeBusiestLookup(text)) return null;

  const metricDetails = inferAssistantMetricDetails(text);
  const mentionsNode = /\b(node|nodes|nod|noduri)\b/.test(text);
  const mentionsServer = /\b(server|servers|servere|serverele)\b/.test(text);

  if (mentionsNode) {
    return {
      toolName: "get_busiest_node",
      args: { metric: metricDetails.metric, includeMetrics: metricDetails.includeMetrics },
      model: "adpanel-load-router",
    };
  }
  if (mentionsServer) {
    return {
      toolName: "get_busiest_server",
      args: { metric: metricDetails.metric, includeMetrics: metricDetails.includeMetrics },
      model: "adpanel-load-router",
    };
  }
  return null;
}

function detectDirectConsoleCheckIntent(message, serverHint = "") {
  const text = String(message || "").trim();
  if (!text) return null;

  const looksLikeConsoleCheck = messageLooksLikeConsoleCheckRequest(text)
    || (!!serverHint && /\b(console|logs?|log output|startup|crash|error|errors|why|diagnose|issue|problem|failing|failure|started|seconds?|healthy|running well|running fine|running okay|took to start|time to start|boot time|startup time|start time|started good|started well|started fine|started okay|started ok|all good|booted|launched)\b/i.test(text));
  if (!looksLikeConsoleCheck) return null;

  return {
    toolName: "check_server_console",
    args: { server: text, question: text },
    model: "adpanel-console-router",
  };
}

function messageLooksLikeServerLimitsCheckRequest(message) {
  const text = String(message || "").trim().toLowerCase();
  if (!text) return false;
  if (messageLooksLikeBusiestLookup(text)) return false;
   if (messageLooksLikeAssistantOpenRequest(text)) return false;
   if (messageLooksLikeConsoleCheckRequest(text)) return false;

  const mentionsMetric = /\b(cpu|ram|memory|disk|storage|space|resource(?:s| stats?)?|quota|quotas|limit|limits|headroom|free|remaining|left|available|usage|used)\b/.test(text);
  const mentionsInspection = /\b(check|show|see|tell me|what|how much|how many|look|inspect|review|analy[sz]e|is|are|close|near|almost|running out|enough)\b/.test(text);
  const mentionsPressure = /\b(close to|near(?:ly)? full|almost full|very high|too high|running out|low on|full|running hot|saturated|overloaded|healthy|okay|ok|fine)\b/.test(text);
  return mentionsMetric && (mentionsInspection || mentionsPressure);
}

function detectDirectServerLimitsIntent(message, serverHint = "") {
  const text = String(message || "").trim();
  if (!text) return null;

  const looksLikeLimitsCheck = messageLooksLikeServerLimitsCheckRequest(text)
    || (!!serverHint && /\b(cpu|ram|memory|disk|storage|space|quota|quotas|limit|limits|headroom|free|remaining|left|available|close to|near full|almost full|too high)\b/i.test(text));
  if (!looksLikeLimitsCheck) return null;

  const server = extractAssistantServerCandidate(text) || String(serverHint || "").trim();
  if (!server) return null;

  const metricDetails = inferAssistantMetricDetails(text);
  return {
    toolName: "check_server_limits",
    args: {
      server: text,
      question: text,
      includeMetrics: metricDetails.includeMetrics,
    },
    model: "adpanel-limits-router",
  };
}

function messageLooksLikeAssistantOpenRequest(message) {
  const text = String(message || "").trim().toLowerCase();
  if (!text) return false;
  return /\b(open|show|take me|go to|goto|navigate|bring me|launch|pop ?up|new tab|tab|manage|configure|config|edit|access)\b/.test(text);
}

function normalizeAssistantServerPageSection(value) {
  const text = normalizeAssistantLookupText(value);
  if (!text) return "console";
  if (/\b(backups?|snapshots?)\b/.test(text)) return "backups";
  if (/\b(files?|file manager|explorer|folders?|directory|directories)\b/.test(text)) return "files";
  if (/\b(activity|history|events?)\b/.test(text)) return "activity";
  if (/\b(scheduler|schedule|schedules|tasks?)\b/.test(text)) return "scheduler";
  if (/\b(store|plugins?|versions?|marketplace)\b/.test(text)) return "store";
  if (/\b(resource(?:s| stats?)?|cpu|ram|memory|disk|storage)\b/.test(text)) return "resource_stats";
  if (/\b(subdomains?|domains?)\b/.test(text)) return "subdomains";
  if (/\b(reinstall|rebuild|reimage|re-image)\b/.test(text)) return "reinstall";
  if (/\b(ai help|assistant help)\b/.test(text)) return "ai_help";
  if (/\b(info|overview|details?)\b/.test(text)) return "info";
  return "console";
}

function formatAssistantServerPageSectionLabel(section) {
  const key = String(section || "").trim();
  return ASSISTANT_SERVER_PAGE_SECTION_LABELS[key] || "server page";
}

function normalizeAssistantSettingsAction(value) {
  const text = normalizeAssistantLookupText(value);
  if (!text) return "";
  if (/\b(pgadmin|postgres setup|postgresql setup)\b/.test(text)) return "pgadmin_setup";
  if (/\b(mongodb|mongo setup)\b/.test(text)) return "mongodb_setup";
  if (/\b(create|new|add)\b.*\buser\b/.test(text)) return "create_user";
  if (/\bbranding\b/.test(text)) return "branding";
  if (/\blogin watermark\b|\bwatermark\b/.test(text)) return "login_watermark";
  if (/\blogin background\b/.test(text)) return "login_background";
  if (/\b(alert|announcement|banner|global alert)\b/.test(text)) return "alert";
  if (/\b(admin quick action|quick action admin|admin action)\b/.test(text)) return "quick_action_admin";
  if (/\b(user quick action|quick action user|user action)\b/.test(text)) return "quick_action_user";
  if (/\b(database setup|setup database|set up database|phpmyadmin|mysql setup)\b/.test(text)) return "database_setup";
  if (/\bcaptcha\b/.test(text)) return "captcha";
  if (/\bmaintenance\b/.test(text)) return "maintenance";
  if (/\b(create|new|add)\b.*\bnode\b/.test(text)) return "create_node";
  if (/\b(create|new|add)\b.*\btemplate\b/.test(text)) return "create_template";
  if ((/\b(add|create|new)\b/.test(text) || /\bpopup\b|\bmodal\b/.test(text)) && /\bwebhook\b/.test(text)) return "add_webhook";
  return "";
}

function inferAssistantSettingsPanelFromAction(action) {
  const normalized = String(action || "").trim();
  if (!normalized) return "";
  if (["branding", "login_watermark", "login_background"].includes(normalized)) return "customization";
  if (["create_user"].includes(normalized)) return "account";
  if (["captcha", "maintenance"].includes(normalized)) return "security";
  if (["database_setup", "pgadmin_setup", "mongodb_setup"].includes(normalized)) return "databases";
  if (["create_node"].includes(normalized)) return "nodes";
  if (["create_template"].includes(normalized)) return "templates";
  if (["add_webhook"].includes(normalized)) return "webhooks";
  if (["alert", "quick_action_admin", "quick_action_user"].includes(normalized)) return "customization";
  return "";
}

function normalizeAssistantSettingsPanel(value) {
  const text = normalizeAssistantLookupText(value);
  if (!text) return "";
  if (/\b(panel info|panelinfo)\b/.test(text)) return "panelinfo";
  if (/\b(customization|customisation|appearance|look|theme|style|branding)\b/.test(text)) return "customization";
  if (/\b(account|accounts?|users?|user management)\b/.test(text)) return "account";
  if (/\b(security|captcha|maintenance)\b/.test(text)) return "security";
  if (/\b(databases?|database|db|mysql|postgres|postgresql|pgadmin|mongo|mongodb)\b/.test(text)) return "databases";
  if (/\b(nodes?|infrastructure)\b/.test(text)) return "nodes";
  if (/\b(templates?|docker templates?)\b/.test(text)) return "templates";
  if (/\b(servers?|instances?)\b/.test(text)) return "servers";
  if (/\b(webhooks?|discord)\b/.test(text)) return "webhooks";
  if (/\b(preferences?|general settings?|settings home)\b/.test(text)) return "preferences";
  return "";
}

function formatAssistantSettingsPanelLabel(panel) {
  const key = String(panel || "").trim();
  return ASSISTANT_SETTINGS_PANEL_LABELS[key] || "Settings";
}

function formatAssistantSettingsActionLabel(action) {
  const key = String(action || "").trim();
  return ASSISTANT_SETTINGS_ACTION_LABELS[key] || "that settings tool";
}

function extractAssistantUrlsFromText(value) {
  return Array.from(String(value || "").matchAll(/https?:\/\/[^\s<>"')]+/gi))
    .map((match) => String(match?.[0] || "").replace(/[.,!?;:]+$/g, "").trim())
    .filter(Boolean);
}

function parseAssistantServerSectionFromUrlAction(value) {
  const action = String(value || "").trim().toLowerCase();
  const map = {
    "open-info": "info",
    "open-files": "files",
    "open-activity": "activity",
    "open-backups": "backups",
    "open-scheduler": "scheduler",
    "open-store": "store",
    "open-resource-stats": "resource_stats",
    "open-subdomains": "subdomains",
    "open-reinstall": "reinstall",
    "open-ai-help": "ai_help",
  };
  return map[action] || "console";
}

function parseAssistantNavigationContextFromUrl(rawUrl) {
  const urlText = String(rawUrl || "").trim();
  if (!urlText) return null;

  try {
    const parsed = new URL(urlText);
    if (parsed.pathname === "/settings") {
      const action = normalizeAssistantSettingsAction(parsed.searchParams.get("assistantAction") || "");
      const panel = normalizeAssistantSettingsPanel(parsed.searchParams.get("panel") || "")
        || (action ? inferAssistantSettingsPanelFromAction(action) : "");
      if (panel || action) {
        return {
          kind: "settings",
          panel,
          action,
        };
      }
    }

    const serverMatch = parsed.pathname.match(/^\/server\/([^/?#]+)/i);
    if (serverMatch?.[1]) {
      return {
        kind: "server",
        server: sanitizeToken(decodeURIComponent(serverMatch[1] || "")),
        section: parseAssistantServerSectionFromUrlAction(parsed.searchParams.get("assistantAction") || ""),
      };
    }
  } catch {
  }

  return null;
}

function getRecentAssistantNavigationContext(conversationHistory = []) {
  const recentHistory = Array.isArray(conversationHistory) ? conversationHistory.slice(-12) : [];

  for (let index = recentHistory.length - 1; index >= 0; index -= 1) {
    const content = normalizeMessageContent(recentHistory[index]?.content);
    const urls = extractAssistantUrlsFromText(content);
    for (let urlIndex = urls.length - 1; urlIndex >= 0; urlIndex -= 1) {
      const parsed = parseAssistantNavigationContextFromUrl(urls[urlIndex]);
      if (parsed) return parsed;
    }
  }

  return null;
}

function isShortAssistantNavigationShortcut(message) {
  const text = normalizeAssistantLookupText(message);
  if (!text || text.length > 48) return false;
  if (text.split(" ").filter(Boolean).length > 4) return false;
  return !/[?]/.test(String(message || ""));
}

function messageExplicitlyNamesAssistantServerSection(message) {
  return /\b(console|terminal|logs?|backups?|files?|activity|scheduler|tasks?|store|plugins?|versions?|resource(?:s| stats?)?|overview|info|details?|subdomains?|domains?|reinstall|ai help|assistant help)\b/i.test(String(message || ""));
}

function messageExplicitlyNamesAssistantSettingsDestination(message) {
  return /\b(settings?|preferences?|appearance|customization|account|security|databases?|db|nodes?|templates?|servers?|webhooks?|panel info|branding|watermark|background|captcha|maintenance|quick action|create user|create node|create template|pgadmin|mongodb|webhook)\b/i.test(String(message || ""));
}

function detectDirectOpenServerPageIntent(message, serverHint = "", conversationHistory = []) {
  const text = String(message || "").trim();
  if (!text) return null;
  const recentNavigationContext = getRecentAssistantNavigationContext(conversationHistory);
  const shortNavigationShortcut = isShortAssistantNavigationShortcut(text);
  if (!messageLooksLikeAssistantOpenRequest(text) && !(shortNavigationShortcut && recentNavigationContext?.kind === "server")) return null;
  if (/\bsettings?\b/i.test(text)) return null;

  const mentionsServerArea =
    messageExplicitlyNamesAssistantServerSection(text)
    || /\b(server|page)\b/i.test(text);
  if (!mentionsServerArea) return null;

  const extractedServer = extractAssistantServerCandidate(text);
  const recentServer = recentNavigationContext?.kind === "server"
    ? String(recentNavigationContext.server || "").trim()
    : "";
  const server = !isWeakAssistantServerReference(extractedServer)
    ? extractedServer
    : (String(serverHint || "").trim() || recentServer);
  if (!server) return null;

  return {
    toolName: "open_server_page",
    args: {
      server,
      section: normalizeAssistantServerPageSection(text),
    },
    model: "adpanel-navigation-router",
  };
}

function detectDirectOpenSettingsIntent(message, conversationHistory = []) {
  const text = String(message || "").trim();
  if (!text) return null;

  const recentNavigationContext = getRecentAssistantNavigationContext(conversationHistory);
  const normalizedPanel = normalizeAssistantSettingsPanel(text);
  const normalizedAction = normalizeAssistantSettingsAction(text);
  if (!normalizedPanel && !normalizedAction) return null;
  if (
    normalizedPanel === "servers"
    && !/\bsettings?\b/i.test(text)
    && (
      messageExplicitlyNamesAssistantServerSection(text)
      || /\bfor\s+(?:a|an|the|that|this)?\s*server\b/i.test(text)
    )
  ) {
    return null;
  }

  const shortNavigationShortcut = isShortAssistantNavigationShortcut(text);
  if (
    !messageLooksLikeAssistantOpenRequest(text)
    && !/\bsettings?\b/i.test(text)
    && !shortNavigationShortcut
    && recentNavigationContext?.kind !== "settings"
  ) {
    return null;
  }

  return {
    toolName: "open_settings_destination",
    args: {
      panel: normalizedPanel || inferAssistantSettingsPanelFromAction(normalizedAction),
      action: normalizedAction,
    },
    model: "adpanel-settings-router",
  };
}

function cleanAssistantServerCandidate(value) {
  let candidate = String(value || "")
    .replace(/^[`"'“”‘’([{<\s]+/g, "")
    .replace(/[`"'“”‘’)\]}>]+$/g, "")
    .replace(/[.,!?;:]+$/g, "")
    .trim();
  if (!candidate) return "";

  candidate = candidate
    .replace(/\b(?:if|when|daca|dacă|and|then|si|și|restart|reboot|reporn(?:e|i)ste|repornește|restarteaza|restartează)\b.*$/i, " ")
    .replace(/\b(?:a|an|the|my|our|please|pls|now|right|server|servers|servere|serverele|named|called|target|crash|crashed|crashing|lag|lagging|issue|issues|problem|problema|startup|start up|error|errors|failing|failure|why|did|de|ce|căzut|cazut)\b/gi, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (!candidate) return "";
  if (/^(?:a|an|it|them|that|this|one|all)$/i.test(candidate)) return "";
  return candidate;
}

function extractAssistantServerCandidate(message) {
  const raw = String(message || "").trim();
  if (!raw) return "";

  const quoted = raw.match(/["'`“”]([^"'`“”]{1,80})["'`“”]/);
  if (quoted) {
    const candidate = cleanAssistantServerCandidate(quoted[1]);
    if (candidate) return candidate;
  }

  const wrappedMatches = Array.from(raw.matchAll(/[\(\[]([^()\[\]]{1,80})[\)\]]/g));
  for (const match of wrappedMatches) {
    const candidate = cleanAssistantServerCandidate(match[1]);
    if (candidate) return candidate;
  }

  const patterns = [
    /\bserver(?:ul)?\s+([a-z0-9][a-z0-9._-]*(?:\s+[a-z0-9][a-z0-9._-]*){0,2})/i,
    /\b(?:srv|bot)\s+([a-z0-9][a-z0-9._-]*(?:\s+[a-z0-9][a-z0-9._-]*){0,2})/i,
    /\b(?:for|on|la|pe)\s+server(?:ul)?\s+([a-z0-9][a-z0-9._-]*(?:\s+[a-z0-9][a-z0-9._-]*){0,2})/i,
    /\b(?:for|on|about|check|diagnose|inspect|why did|why is|problema la|de ce a cazut|de ce a căzut)\s+([a-z0-9][a-z0-9._-]*(?:\s+[a-z0-9][a-z0-9._-]*){0,2})/i,
    /\b(?:start|run|boot|turn on|power on|bring up|spin up|restart|reboot|cycle|bounce|reload|stop|shutdown|shut down|turn off|power off|take down|kill|terminate|porneste|pornește|opreste|oprește|inchide|închide|reporneste|repornește|omoara|omoară|ucide)\s+([a-z0-9][a-z0-9._-]*(?:\s+[a-z0-9][a-z0-9._-]*){0,2})/i,
  ];

  for (const pattern of patterns) {
    const match = raw.match(pattern);
    if (!match) continue;
    const candidate = cleanAssistantServerCandidate(match[1]);
    if (candidate) return candidate;
  }

  return "";
}

function detectDirectDiagnosisIntent(message, serverHint = "") {
  const text = String(message || "").trim();
  if (!text) return null;

  const looksLikeDiagnosis = messageLooksLikeDiagnosisRequest(text)
    || (!!serverHint && /\b(diagnose|diagnostic|crash|crashed|crashing|lag|lagging|slow|startup|start up|won'?t start|problem|issue|error|failing|failure|why is|why did|de ce|problema|eroare|a cazut|a căzut)\b/i.test(text));
  if (!looksLikeDiagnosis) return null;

  const server = extractAssistantServerCandidate(text) || String(serverHint || "").trim();
  if (!server) return null;

  return {
    toolName: "diagnose_server",
    args: { server },
    model: "adpanel-diagnosis-router",
  };
}

function normalizeMinecraftPropertyKey(value) {
  const clean = String(value || "").trim();
  if (/^[A-Za-z0-9._-]+$/.test(clean)) return clean;

  for (const entry of MINECRAFT_PROPERTY_ALIASES) {
    if (entry.pattern.test(clean)) {
      return entry.key;
    }
  }

  return "";
}

function trimMinecraftPropertyValueSegment(value) {
  let clean = String(value || "").trim();
  if (!clean) return "";

  clean = clean
    .split(/\s+\b(?:for|on|pe|la)\b\s+(?:all|every|all accessible|all my|them all|all of them|toate|pe toate|server(?:ul)?|servers|servere|serverele|minecraft)\b/i)[0]
    .replace(/\s+\b(?:only if|doar daca|doar dacă)\b[\s\S]*$/i, "")
    .replace(/\s+\b(?:if|when|daca|dacă)\b[\s\S]*$/i, "")
    .replace(/\s+\b(?:and|then|si|și)\b\s+(?:restart|reboot|reporn(?:e|i)ste|repornește|restarteaza|restartează)\b[\s\S]*$/i, "")
    .replace(/\s+\b(?:after(?:wards)?|after that|după|dupa)\b[\s\S]*$/i, "")
    .trim();

  return clean;
}

function normalizeMinecraftPropertyValue(value) {
  let clean = trimMinecraftPropertyValueSegment(value);
  if (!clean) return "";

  clean = clean
    .replace(/^[`"'“”‘’]+|[`"'“”‘’]+$/g, "")
    .replace(/^[:=]\s*/g, "")
    .replace(/[.,!?;:]+$/g, "")
    .replace(/^(?:is|este|e)\s+/i, "")
    .trim();

  if (/^(true|on|enabled?|yes|da)$/i.test(clean)) return "true";
  if (/^(false|off|disabled?|no|nu)$/i.test(clean)) return "false";
  return clean.slice(0, 160);
}

function extractMinecraftConditionalCurrentValue(message) {
  const text = String(message || "").trim();
  if (!text) return "";

  const patterns = [
    /\b(?:if|when)\s+(?:it(?:'s| is)?|the current value is|current value is)?\s*(true|false|on|off|enabled?|disabled?|yes|no)\b/i,
    /\b(?:only if)\s+(?:it(?:'s| is)?|the current value is|current value is)?\s*(true|false|on|off|enabled?|disabled?|yes|no)\b/i,
    /\b(?:daca|dacă)\s+(?:e|este)?\s*(true|false|on|off|enabled?|disabled?|yes|no|da|nu)\b/i,
    /\b(?:doar daca|doar dacă)\s+(?:e|este)?\s*(true|false|on|off|enabled?|disabled?|yes|no|da|nu)\b/i,
  ];

  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (!match) continue;
    const normalized = normalizeMinecraftPropertyValue(match[1]);
    if (normalized) return normalized;
  }

  return "";
}

function messageRequestsRestartAfterPropertyUpdate(message) {
  const text = String(message || "").trim();
  if (!text) return false;
  return /\b(restart|reboot|reporn(?:e|i)ste|repornește|restarteaza|restartează)\b/i.test(text);
}

function stripTrailingServerHintFromValue(value, serverHint) {
  const cleanValue = String(value || "").trim();
  const cleanServer = String(serverHint || "").trim();
  if (!cleanValue || !cleanServer) return cleanValue;

  const normalizedTokens = normalizeAssistantLookupText(cleanServer).split(" ").filter(Boolean);
  if (!normalizedTokens.length) return cleanValue;
  const flexibleServerPattern = normalizedTokens.map((token) => escapeRegex(token)).join("[\\s._-]+");
  return cleanValue
    .replace(new RegExp(`\\s+(?:on|for|pe|la)\\s+${flexibleServerPattern}\\s*$`, "i"), "")
    .trim();
}

function extractChainedPowerAction(message) {
  const text = String(message || "").trim().toLowerCase();
  if (!text) return "";

  if (!/\b(and|then|after|afterwards|after that|si|și|apoi|după|dupa|la final|cand termini|când termini|when done)\b/.test(text)) {
    return "";
  }

  return detectAssistantPowerAction(text);
}

function extractMinecraftPropertyValue(message, propertyKey) {
  const raw = String(message || "").trim();
  if (!raw || !propertyKey) return "";

  const exactPattern = new RegExp(`\\b${escapeRegex(propertyKey)}\\b\\s*(?::|=|to|pe|as|la)?\\s*([^\\n]+)`, "i");
  const exactMatch = raw.match(exactPattern);
  if (exactMatch) {
    const exactValue = normalizeMinecraftPropertyValue(exactMatch[1]);
    if (exactValue) return exactValue;
  }

  const aliasEntry = MINECRAFT_PROPERTY_ALIASES.find((entry) => entry.key === propertyKey);
  if (!aliasEntry) return "";
  const aliasMatch = raw.match(aliasEntry.pattern);
  if (!aliasMatch || typeof aliasMatch.index !== "number") return "";

  const remainder = raw.slice(aliasMatch.index + aliasMatch[0].length)
    .replace(/^\s*(?:to|=|pe|as|la)\s*/i, "")
    .trim();
  if (!remainder) return "";

  if (/^["'`“”]/.test(remainder)) {
    const quoted = remainder.match(/^[`"'“”]([^`"'“”]{1,160})[`"'“”]/);
    if (quoted) return normalizeMinecraftPropertyValue(quoted[1]);
  }

  return normalizeMinecraftPropertyValue(
    remainder
  );
}

function detectDirectMinecraftPropertyIntent(message, serverHint = "") {
  const text = String(message || "").trim();
  if (!text) return null;

  if (!messageLooksLikeStructuredMinecraftPropertyMutation(text)) return null;

  const wantsAll = /\b(all|every|all accessible|all my|them all|all of them|toate|pe toate)\b/i.test(text);
  const server = extractAssistantServerCandidate(text) || String(serverHint || "").trim();
  const scope = wantsAll ? "all_accessible_minecraft" : (server ? "single_server" : "");
  if (!scope) return null;

  const propertyFromAssignment = text.match(/\b([a-z0-9._-]+)\b\s*(?:=|:)\s*([^\n]+)/i);
  const property = normalizeMinecraftPropertyKey(propertyFromAssignment?.[1] || text);
  const rawValue = propertyFromAssignment?.[2] || extractMinecraftPropertyValue(text, property);
  const value = normalizeMinecraftPropertyValue(stripTrailingServerHintFromValue(rawValue, server));
  const onlyIfCurrent = extractMinecraftConditionalCurrentValue(text);
  const restartAfter = messageRequestsRestartAfterPropertyUpdate(text);

  if (!property || !value) return null;

  return {
    toolName: "update_minecraft_server_property",
    args: scope === "single_server"
      ? { property, value, scope, server, ...(onlyIfCurrent ? { onlyIfCurrent } : {}), ...(restartAfter ? { restartAfter: true } : {}) }
      : { property, value, scope, ...(onlyIfCurrent ? { onlyIfCurrent } : {}), ...(restartAfter ? { restartAfter: true } : {}) },
    model: "adpanel-minecraft-router",
  };
}

function cleanAssistantPathCandidate(value) {
  let candidate = String(value || "")
    .replace(/^[`"'“”‘’]+|[`"'“”‘’]+$/g, "")
    .replace(/[),.!?;:]+$/g, "")
    .trim();

  candidate = candidate
    .replace(/^(?:file|path|folder|directory|config)\s+/i, "")
    .replace(/\\/g, "/")
    .replace(/^\.\/+/, "")
    .replace(/^\/+/, "")
    .trim();

  if (!candidate || candidate === "." || candidate === "..") return "";
  return candidate.slice(0, 220);
}

function extractAssistantFilePath(message) {
  const raw = String(message || "").trim();
  if (!raw) return "";

  const quotedMatches = Array.from(raw.matchAll(/[`"'“”]([^`"'“”]{1,220})[`"'“”]/g));
  for (const match of quotedMatches) {
    const candidate = cleanAssistantPathCandidate(match[1]);
    if (/[/.]/.test(candidate) || /\.[A-Za-z0-9]{1,12}$/.test(candidate)) {
      return candidate;
    }
  }

  const knownFileMatch = raw.match(/\b(server\.properties|eula\.txt|package\.json|docker-compose\.ya?ml|requirements\.txt|pyproject\.toml|cargo\.toml|dockerfile|makefile|(?:index|main|app)\.(?:js|ts|mjs|cjs|py)|config\.(?:ya?ml|json|toml|ini|cfg)|latest\.log|\.env|plugins|mods|config|logs|crash-reports)\b/i);
  if (knownFileMatch) {
    return cleanAssistantPathCandidate(knownFileMatch[1]);
  }

  const pathMatch = raw.match(/(?:^|[\s(])([A-Za-z0-9._-]+(?:\/[A-Za-z0-9._ -]+)+\/?)(?=$|[\s),.!?])/);
  if (pathMatch) {
    return cleanAssistantPathCandidate(pathMatch[1]);
  }

  const flatFileMatch = raw.match(/(?:^|[\s(])([A-Za-z0-9._-]+\.[A-Za-z0-9]{1,12})(?=$|[\s),.!?])/);
  if (flatFileMatch) {
    return cleanAssistantPathCandidate(flatFileMatch[1]);
  }

  return "";
}

function looksLikeAssistantDirectoryPath(filePath) {
  const clean = cleanAssistantPathCandidate(filePath);
  if (!clean) return false;
  if (clean.endsWith("/")) return true;
  const base = clean.split("/").filter(Boolean).pop() || "";
  return !/\.[A-Za-z0-9]{1,12}$/i.test(base);
}

function detectDirectFileInspectIntent(message, serverHint = "") {
  const text = String(message || "").trim();
  if (!text) return null;

  const server = extractAssistantServerCandidate(text) || String(serverHint || "").trim();
  const path = extractAssistantFilePath(text);
  if (!server || !path) return null;

  const lower = text.toLowerCase();
  const wantsFileContext =
    /[/.]/.test(path) ||
    /\b(file|files|folder|directory|config|log|logs|path|server\.properties|eula|package\.json)\b/.test(lower);
  if (!wantsFileContext) return null;

  const wantsInspection =
    /\b(read|show|open|view|check|inspect|look at|see|vezi|arata|arată|deschide|what'?s in|what is in|analy[sz]e|review|ce sa vad|ce să văd)\b/i.test(text);
  const wantsAdvice = /\b(what should i change|what to change|ce sa schimb|ce să schimb)\b/i.test(text);
  if (!wantsInspection && !wantsAdvice) return null;

  const toolName = looksLikeAssistantDirectoryPath(path) && /\b(list|show|see|inside|contents?|explore|browse)\b/i.test(text)
    ? "list_files"
    : "read_file";

  return {
    toolName,
    args: { server, path },
    model: "adpanel-file-router",
  };
}

function messageNeedsRealActionExecution(message) {
  const text = String(message || "").trim().toLowerCase();
  if (!text) return false;

  const createServerIntent =
    /\b(create|make|spin\s*up|deploy|provision)\b/.test(text) &&
    /\b(server|minecraft|nodejs|python|discord|template|templates|node|nodes)\b/.test(text);
  if (createServerIntent) return true;

  const pluginInstallIntent =
    /\b(install|add|download|setup|set up)\b/.test(text) &&
    /\b(plugin|plugins|modrinth|viaversion|geyser|luckperms|vault)\b/.test(text);
  if (pluginInstallIntent) return true;

  const deleteServerIntent =
    /\b(delete|remove|destroy|wipe)\b/.test(text) &&
    /\b(server|servers)\b/.test(text);
  if (deleteServerIntent) return true;

  const serverPowerIntent =
    /\b(start|stop|restart|kill|power|porneste|pornește|opreste|oprește|inchide|închide|omoara|omoară|ucide|reporneste|repornește)\b/.test(text) &&
    /\b(server|servers|servere|serverele)\b/.test(text);
  if (serverPowerIntent) return true;

  if (detectAssistantPowerAction(text) && extractAssistantServerCandidate(text)) return true;

  if (messageLooksLikeBusiestLookup(text)) return true;

  if (messageLooksLikeDiagnosisRequest(text)) return true;

  if (messageLooksLikeStructuredMinecraftPropertyMutation(text)) return true;

  const fileMutationIntent =
    /\b(edit|write|replace|overwrite|append|create|delete|remove|rename|move|mkdir|make)\b/.test(text) &&
    /\b(file|files|folder|directory|config|properties|server\.properties|eula|plugin|plugins|world|mods?)\b/.test(text);
  if (fileMutationIntent) return true;

  if (messageLooksLikeMinecraftBatchPropertyUpdate(text)) return true;

  const explicitPathMutation =
    /\b(edit|write|replace|overwrite|append|create|delete|remove|rename|move|mkdir|make)\b/.test(text) &&
    /[./\\][\w./\\-]+/.test(text);
  if (explicitPathMutation) return true;

  return false;
}

function replyClaimsActionSucceeded(message) {
  const text = String(message || "").trim().toLowerCase();
  if (!text) return false;
  if (/[?]/.test(text)) return false;
  if (/\b(i need|need one more detail|which|what|tell me|choose|pick|do you want|would you like|please provide|cannot|can't|do not have permission|don't have permission|failed|error)\b/.test(text)) {
    return false;
  }
  return /\b(done|completed|created|started|stopped|restarted|killed|updated|deleted|removed|renamed|moved|written|saved|applied|opened|opening)\b/.test(text);
}

function shouldAllowToolUseForMessage(message, conversationHistory = []) {
  const text = String(message || "").trim().toLowerCase();
  if (!text) return false;

  const recentHistory = Array.isArray(conversationHistory) ? conversationHistory.slice(-8) : [];
  const lastAssistantMessage = recentHistory
    .slice()
    .reverse()
    .find((entry) => String(entry?.role || "").toLowerCase() === "assistant");
  const lastAssistantText = normalizeMessageContent(lastAssistantMessage?.content).toLowerCase();
  const assistantAskedStructuredFollowUp =
    /\b(which|what|how much|how many|choose|pick|tell me|do you want|would you like)\b/.test(lastAssistantText) &&
    /\b(template|templates|node|nodes|server name|name|ram|cpu|storage|port|password|email|2fa|two[- ]factor|recovery|file|path|plugin|plugins|loader|platform|minecraft version|version)\b/.test(lastAssistantText);
  const assistantAskedActionConfirmation =
    /\b(ready|want me to|should i|go ahead|create it|create the server|apply it|use that|use this)\b/.test(lastAssistantText) &&
    /\b(server|template|node|port|ram|cpu|storage|create|provision)\b/.test(lastAssistantText);
  const shortStructuredReply =
    text.length <= 80 &&
    (
      isAffirmativeShortReply(text) ||
      /^(that one|this one)$/i.test(String(text || "").trim()) ||
      /^\d{2,5}$/.test(text) ||
      /\b\d+(?:\.\d+)?\s*(?:mb|gb|tb|cores?)\b/.test(text) ||
      /\b(?:port|ram|cpu|storage|node|template|plugin|loader|platform|paper|purpur|spigot|bukkit|velocity|fabric|forge|neoforge)\b/.test(text)
    );
  const infrastructureShortcut = /^(nodes?|templates?|servers?)$/.test(text);
  if (infrastructureShortcut) return true;

  const accountIntent =
    /\b(change|reset|recover|update|open|show)\b/.test(text) &&
    /\b(password|email|2fa|two[- ]factor|recovery|recovery code|recovery codes)\b/.test(text);
  if (accountIntent) return true;

  const explicitServerListing =
    /\b(list|show|which|what)\b/.test(text) &&
    /\b(server|servers|node|nodes)\b/.test(text);
  if (explicitServerListing) return true;

  const explicitTemplateOrNodeListing =
    /\b(list|show|which|what)\b/.test(text) &&
    /\b(template|templates|node|nodes)\b/.test(text);
  if (explicitTemplateOrNodeListing) return true;

  const serverQuestion =
    /\b(status|port|template|permission|permissions|ip|uptime|online|offline|running|stopped|console)\b/.test(text) &&
    /\b(?:server|servers|node|nodes)\b/.test(text);
  if (serverQuestion) return true;

  if (messageLooksLikeBusiestLookup(text)) return true;

  if (messageLooksLikeDiagnosisRequest(text)) return true;

  const createServerIntent =
    /\b(create|make|spin\s*up|deploy|provision)\b/.test(text) &&
    /\b(server|minecraft|nodejs|python|discord|template|templates|node|nodes)\b/.test(text);
  if (createServerIntent) return true;

  const pluginInstallIntent =
    /\b(install|add|download|setup|set up)\b/.test(text) &&
    /\b(plugin|plugins|modrinth|viaversion|geyser|luckperms|vault)\b/.test(text);
  if (pluginInstallIntent) return true;

  const deleteServerIntent =
    /\b(delete|remove|destroy|wipe)\b/.test(text) &&
    /\b(server|servers)\b/.test(text);
  if (deleteServerIntent) return true;

  const fileOrServerAction =
    /\b(start|stop|restart|kill|power|edit|read|write|create|delete|remove|rename|move|open|inspect|list|show|porneste|pornește|opreste|oprește|inchide|închide|omoara|omoară|ucide|reporneste|repornește|citeste|citește|scrie|editeaza|editează|sterge|șterge|arata|arată)\b/.test(text) &&
    /\b(server|servers|servere|serverele|file|files|folder|directory|config|properties|server\.properties|eula|plugin|plugins|world|mods?)\b/.test(text);
  if (fileOrServerAction) return true;

  if (messageLooksLikeMinecraftBatchPropertyUpdate(text)) return true;

  const explicitPathAction =
    /\b(read|write|edit|create|delete|rename|move|list|show)\b/.test(text) &&
    /[./\\][\w./\\-]+/.test(text);
  if (explicitPathAction) return true;

  if (assistantAskedStructuredFollowUp) {
    return true;
  }

  if (assistantAskedActionConfirmation && shortStructuredReply) {
    return true;
  }

  return false;
}

function getLastAssistantText(conversationHistory = []) {
  const recentHistory = Array.isArray(conversationHistory) ? conversationHistory.slice(-10) : [];
  const lastAssistantMessage = recentHistory
    .slice()
    .reverse()
    .find((entry) => String(entry?.role || "").toLowerCase() === "assistant");
  return normalizeMessageContent(lastAssistantMessage?.content).trim();
}

function buildCarryForwardVisibleTranscript(messages, options = {}) {
  const recentMessages = (Array.isArray(messages) ? messages : [])
    .filter((entry) => entry && (entry.role === "user" || entry.role === "assistant"))
    .slice(-Math.max(1, Number(options.count) || 18));
  if (!recentMessages.length) return "";

  const perMessageLimit = Math.max(60, Number(options.perMessageLimit) || 220);
  const totalLimit = Math.max(400, Number(options.totalLimit) || 2600);
  const lines = [];

  for (const entry of recentMessages) {
    const role = String(entry.role || "").toLowerCase() === "user" ? "User" : "Assistant";
    const content = normalizeMessageContent(entry.content)
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, perMessageLimit);
    if (!content) continue;
    lines.push(`${role}: ${content}`);
  }

  return lines.join("\n").slice(0, totalLimit);
}

function compactAssistantModelMessageContent(content, maxLength = MODEL_CONTEXT_RECENT_MESSAGE_CHAR_LIMIT) {
  const text = normalizeMessageContent(content).replace(/\s+/g, " ").trim();
  if (!text) return "";
  if (text.length <= maxLength) return text;
  return `${text.slice(0, Math.max(120, maxLength - 3)).trim()}...`;
}

function buildAssistantModelConversationWindow(messages, options = {}) {
  const sourceMessages = (Array.isArray(messages) ? messages : [])
    .filter((entry) => entry && (entry.role === "user" || entry.role === "assistant"));

  if (!sourceMessages.length) {
    return {
      olderContextMessage: "",
      recentMessages: [],
    };
  }

  const recentCount = Math.max(4, Number(options.recentCount) || MODEL_CONTEXT_RECENT_MESSAGE_LIMIT);
  const olderTranscriptCount = Math.max(4, Number(options.olderTranscriptCount) || MODEL_CONTEXT_OLDER_TRANSCRIPT_COUNT);
  const olderTranscriptTotalLimit = Math.max(400, Number(options.olderTranscriptTotalLimit) || MODEL_CONTEXT_OLDER_TRANSCRIPT_TOTAL_LIMIT);
  const recentMessagesSource = sourceMessages.slice(-recentCount);
  const olderMessagesSource = sourceMessages.slice(0, Math.max(0, sourceMessages.length - recentCount));
  const olderTranscript = olderMessagesSource.length
    ? buildCarryForwardVisibleTranscript(olderMessagesSource, {
        count: Math.min(olderMessagesSource.length, olderTranscriptCount),
        perMessageLimit: 150,
        totalLimit: olderTranscriptTotalLimit,
      })
    : "";

  const recentMessages = recentMessagesSource
    .map((entry, index) => {
      const isLatest = index === recentMessagesSource.length - 1;
      const isNearLatest = index >= recentMessagesSource.length - 4;
      const maxLength = isLatest
        ? MODEL_CONTEXT_LATEST_MESSAGE_CHAR_LIMIT
        : (isNearLatest ? MODEL_CONTEXT_NEAR_MESSAGE_CHAR_LIMIT : MODEL_CONTEXT_RECENT_MESSAGE_CHAR_LIMIT);
      const content = compactAssistantModelMessageContent(entry.content, maxLength);
      if (!content) return null;
      return {
        role: entry.role,
        content,
      };
    })
    .filter(Boolean);

  return {
    olderContextMessage: olderTranscript
      ? `Earlier visible chat summary:\n${olderTranscript}\nUse it only when the latest messages omit a reference.`
      : "",
    recentMessages,
  };
}

function looksLikeRestrictedAssistantFollowUp(message, conversationHistory = []) {
  const text = String(message || "").trim();
  if (!text || text.length > 180) return false;

  const lower = text.toLowerCase();
  if (
    detectAssistantPowerAction(lower)
    || messageLooksLikeBusiestLookup(lower)
    || messageLooksLikeConsoleCheckRequest(lower)
    || !!detectDirectAccountFlowIntent(lower)
  ) {
    return false;
  }

  const lastAssistantText = getLastAssistantText(conversationHistory).toLowerCase();
  if (!lastAssistantText) return false;

  const recentRestrictedResult =
    /\b(console|log|started|stopped|restarted|killed|busiest|highest|cpu|ram|memory|disk|storage|node|server|2fa|password|email|healthy|running|seconds?|uptime)\b/.test(lastAssistantText);
  if (!recentRestrictedResult) return false;

  const asksFollowUp =
    /[?]/.test(text)
    || /\b(so|and|what|why|how|how many|how much|which|did|does|is|was|are|were|okay|ok|then|mean|seconds?|sec|deci|bun|acum)\b/.test(lower);
  const refersToPreviousAnswer =
    /\b(it|that|those|them|this|seconds?|sec|started|crashed|running|healthy|console|logs?|cpu|ram|memory|disk|storage|node|server)\b/.test(lower);

  return asksFollowUp || refersToPreviousAnswer;
}

function messageLikelyNeedsAccessibleServerContext(message, conversationHistory = []) {
  const text = String(message || "").trim();
  const lower = text.toLowerCase();
  if (!lower) return false;

  if (extractAssistantServerCandidate(text)) return true;
  if (detectAssistantPowerAction(lower)) return true;
  if (messageLooksLikeBusiestLookup(lower)) return true;
  if (messageLooksLikeConsoleCheckRequest(lower)) return true;
  if (messageLooksLikeServerLimitsCheckRequest(lower)) return true;
  if (messageLooksLikeDiagnosisRequest(lower)) return true;
  const wantsNavigation = /\b(open|show|take me|go to|goto|navigate|bring me|launch|new tab|tab|pop ?up|manage|configure|config|edit|access)\b/.test(lower)
    && /\b(server|console|terminal|backups?|files?|activity|scheduler|tasks?|store|plugins?|versions?|resource(?:s| stats?)?|overview|info|details?|subdomains?|domains?|reinstall|settings?|preferences?|appearance|customization|account|security|databases?|nodes?|templates?|webhooks?|panel info|branding|watermark|background|captcha|maintenance|quick action|create user|create node|create template|pgadmin|mongodb|webhook)\b/.test(lower);
  if (wantsNavigation) return true;
  if (looksLikeRestrictedAssistantFollowUp(text, conversationHistory)) return true;

  return /\b(server|servers|node|nodes|console|logs?|backups?|scheduler|activity|files?|resource(?:s| stats?)?|cpu|ram|memory|disk|storage|quota|limits?)\b/i.test(text);
}

function normalizeShortReply(message) {
  return String(message || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function isAffirmativeShortReply(message) {
  const text = normalizeShortReply(message);
  return [
    "yes",
    "yeah",
    "yep",
    "correct",
    "go ahead",
    "do it",
    "that works",
    "sounds good",
    "sure",
    "ok",
    "okay",
    "ok create it",
    "okay create it",
    "yes create it",
    "create it",
    "confirm",
    "confirmed",
    "use that",
    "use this",
    "use default",
    "use defaults",
    "default",
    "recommended",
  ].includes(text);
}

function assistantAskedProvisioningQuestion(conversationHistory = []) {
  const text = getLastAssistantText(conversationHistory).toLowerCase();
  if (!text) return false;
  return (
    /\b(which|what|how much|how many|choose|pick|tell me|do you want|would you like)\b/.test(text) &&
    /\b(template|templates|node|nodes|server name|name|host port|port|ram|cpu|storage|defaults?)\b/.test(text)
  );
}

function assistantAskedProvisioningConfirmation(conversationHistory = []) {
  const text = getLastAssistantText(conversationHistory).toLowerCase();
  if (!text) return false;
  if (/\bi can create it now\b|\bi can create the server now\b|\bi can provision it now\b/.test(text)) {
    return true;
  }
  return (
    /\b(ready|want me to|should i|go ahead|create it|create the server|apply it|use that|use this|confirm)\b/.test(text) &&
    /\b(server|template|node|port|ram|cpu|storage|create|provision)\b/.test(text)
  );
}

function assistantAskedDefaultsQuestion(conversationHistory = []) {
  const text = getLastAssistantText(conversationHistory).toLowerCase();
  if (!text) return false;
  return /\bdefaults?\b/.test(text) && /\b(port|ram|cpu|storage)\b/.test(text);
}

function isCreateServerConversation(message, conversationHistory = []) {
  const text = String(message || "").trim().toLowerCase();
  if (!text) return false;

  const explicitCreateIntent =
    /\b(create|make|spin\s*up|deploy|provision)\b/.test(text) &&
    /\b(server|minecraft|nodejs|python|discord|template|templates|node|nodes)\b/.test(text);
  if (explicitCreateIntent) return true;

  const shortProvisioningReply =
    text.length <= 120 &&
    (
      isAffirmativeShortReply(text) ||
      /^\d{2,5}$/.test(text) ||
      /\b\d+(?:\.\d+)?\s*(?:mb|gb|tb|cores?)\b/.test(text) ||
      /\b(?:node|template|port|ram|cpu|storage|minecraft|paper|purpur|spigot|bukkit|fabric|forge|neoforge|python|nodejs)\b/.test(text)
    );

  return (assistantAskedProvisioningQuestion(conversationHistory) || assistantAskedProvisioningConfirmation(conversationHistory)) && shortProvisioningReply;
}

function inferTranscriptionFilename(filename, mimeType) {
  const cleanName = String(filename || "").trim();
  if (cleanName) return cleanName;
  const mime = String(mimeType || "").toLowerCase();
  if (mime.includes("wav")) return "dashboard-assistant.wav";
  if (mime.includes("mp4") || mime.includes("m4a")) return "dashboard-assistant.m4a";
  if (mime.includes("ogg")) return "dashboard-assistant.ogg";
  if (mime.includes("mpeg") || mime.includes("mp3")) return "dashboard-assistant.mp3";
  return "dashboard-assistant.webm";
}

function isGenericAssistantReply(value) {
  const text = String(value || "").trim().toLowerCase();
  return text === "" || text === "done." || text === "done" || text === "completed." || text === "completed";
}

function buildReplyFromToolResult(toolName, toolResult) {
  const result = toolResult && typeof toolResult === "object" ? toolResult : null;
  if (!result) return "";
  if (result.ok === false) {
    const error = String(result.error || "").trim();
    const detail = String(result.detail || "").trim();
    const candidates = Array.isArray(result.candidates)
      ? result.candidates.map((item) => String(item || "").trim()).filter(Boolean).slice(0, 5)
      : [];
    if (candidates.length && /server name is ambiguous|multiple servers matched|no accessible server matched/i.test(error)) {
      return `${error} Try ${candidates.join(", ")}.`;
    }
    if (detail && (!error || /^failed to create that server\.?$/i.test(error) || /^that action failed\.?$/i.test(error))) {
      return detail;
    }
    return error || detail || "That action failed.";
  }

  switch (String(toolName || "").trim()) {
    case "create_server": {
      const server = result.server && typeof result.server === "object" ? result.server : {};
      const name = server.displayName || server.name || "the server";
      const node = server.nodeName || server.nodeId || "";
      return node ? `Created ${name} on ${node}.` : `Created ${name}.`;
    }
    case "install_plugin": {
      const pluginName = result.plugin?.name || result.plugin?.slug || "the plugin";
      const serverName = result.server || "the server";
      return `Installed ${pluginName} on ${serverName}.`;
    }
    case "request_delete_server":
      return result.server ? `Please confirm deletion for ${result.server}.` : "Please confirm that deletion first.";
    case "power_server": {
      if (result.resolutionRequired) {
        const action = result.action || "update";
        if (action === "start") return "Finding the matching server to start.";
        if (action === "stop") return "Finding the matching server to stop.";
        if (action === "restart") return "Finding the matching server to restart.";
        if (action === "kill") return "Finding the matching server to kill.";
        return "Finding the matching server.";
      }
      const server = result.displayName || result.server || result.targets?.[0]?.displayName || result.targets?.[0]?.server || "the server";
      const action = result.action || "updated";
      if (action === "start") return `Requesting start for ${server}.`;
      if (action === "stop") return `Requesting stop for ${server}.`;
      if (action === "restart") return `Requesting restart for ${server}.`;
      if (action === "kill") return `Requesting kill for ${server}.`;
      return `Updated ${server}.`;
    }
    case "power_accessible_servers": {
      const count = Number(result.count) || (Array.isArray(result.servers) ? result.servers.length : 0) || (Array.isArray(result.targets) ? result.targets.length : 0);
      if (result.action === "start") return `Requesting start for ${count || "your"} accessible server${count === 1 ? "" : "s"}.`;
      if (result.action === "stop") return `Requesting stop for ${count || "your"} accessible server${count === 1 ? "" : "s"}.`;
      if (result.action === "restart") return `Requesting restart for ${count || "your"} accessible server${count === 1 ? "" : "s"}.`;
      if (result.action === "kill") return `Requesting kill for ${count || "your"} accessible server${count === 1 ? "" : "s"}.`;
      return "Running that power action now.";
    }
    case "open_server_page": {
      const section = formatAssistantServerPageSectionLabel(result.section);
      if (result.resolutionRequired) {
        return `Preparing a link to ${section}.`;
      }
      const server = result.displayName || result.server || "that server";
      return section === "console"
        ? `Preparing a link to ${server}.`
        : `Preparing a link to ${section} for ${server}.`;
    }
    case "open_settings_destination": {
      const actionLabel = formatAssistantSettingsActionLabel(result.action);
      const panelLabel = formatAssistantSettingsPanelLabel(result.panel || inferAssistantSettingsPanelFromAction(result.action));
      if (result.action) {
        return `Preparing a link to ${actionLabel}.`;
      }
      return `Preparing a link to ${panelLabel}.`;
    }
    case "get_busiest_node":
      if (result.metric === "cpu") return "Checking the busiest node by CPU now.";
      if (result.metric === "memory") return "Checking the busiest node by memory now.";
      if (result.metric === "disk") return "Checking the busiest node by disk now.";
      return "Checking the busiest node now.";
    case "get_busiest_server":
      if (result.metric === "cpu") return "Checking your busiest server by CPU now.";
      if (result.metric === "memory") return "Checking your busiest server by memory now.";
      if (result.metric === "disk") return "Checking your busiest server by disk now.";
      return "Checking your busiest server now.";
    case "check_server_console":
      return "Checking that server console now.";
    case "check_server_limits":
      return "Checking that server's limits now.";
    case "diagnose_server": {
      const server = result.displayName || result.server || "that server";
      return `Collecting diagnostics for ${server}.`;
    }
    case "update_minecraft_server_property": {
      const property = result.property || "that setting";
      const count = Number(result.count) || (Array.isArray(result.targets) ? result.targets.length : 0);
      const restartText = result.restartAfter ? " with restart requested" : "";
      return `Applying ${property} on ${count || "your"} Minecraft server${count === 1 ? "" : "s"}${restartText}.`;
    }
    case "list_files": {
      const server = result.server || "that server";
      const path = result.path || "/";
      const count = Array.isArray(result.entries) ? result.entries.length : 0;
      return `Found ${count} item${count === 1 ? "" : "s"} in ${path} on ${server}.`;
    }
    case "read_file": {
      const server = result.server || "that server";
      const path = result.path || "that file";
      return `Read ${path} on ${server}.`;
    }
    case "write_file":
      return result.path ? `Updated ${result.path} on ${result.server || "the server"}.` : "File updated.";
    case "create_directory":
      return result.path ? `Created ${result.path}.` : "Folder created.";
    case "rename_path":
      return result.destination ? `Moved to ${result.destination}.` : "Path renamed.";
    case "delete_path":
      return result.path ? `Deleted ${result.path}.` : "Path deleted.";
    case "open_account_flow":
      if (result.flow === "change_password") return "Opening password change.";
      if (result.flow === "change_email") return "Opening email change.";
      if (result.flow === "change_2fa") return "Opening 2FA settings.";
      if (result.flow === "recover_password") return "Opening password recovery.";
      if (result.flow === "recover_email") return "Opening email recovery.";
      if (result.flow === "recover_2fa") return "Opening 2FA recovery.";
      return "Opening that account flow.";
    default:
      return "";
  }
}

function escapeRegex(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function shouldForceQueuedReply(toolName, toolResult) {
  if (!(toolResult && typeof toolResult === "object" && toolResult.browserActionQueued)) {
    return false;
  }

  return [
    "get_busiest_node",
    "get_busiest_server",
    "check_server_console",
    "check_server_limits",
    "diagnose_server",
    "update_minecraft_server_property",
  ].includes(String(toolName || "").trim());
}

function normalizeLooseText(value) {
  return String(value || "").toLowerCase().replace(/\s+/g, " ").trim();
}

function parseAssistantToolArgs(value) {
  if (!value) return null;
  if (typeof value === "object") return value;
  if (typeof value !== "string") return null;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function isGenericServerPropertiesWriteToolCall(toolName, rawArgs) {
  if (String(toolName || "").trim() !== "write_file") return false;
  const parsedArgs = parseAssistantToolArgs(rawArgs);
  const path = String(parsedArgs?.path || "").trim().toLowerCase();
  return path === "server.properties" || path.endsWith("/server.properties");
}

function isWeakAssistantServerReference(value) {
  const clean = normalizeAssistantLookupText(value);
  if (!clean) return true;
  return /^(?:a|an|server|servers|a server|the server|that server|this server|serverul|serverele|srv|bot|instance|current|this|it|that|them|one)$/i.test(clean);
}

function applyServerHintToToolArgs(toolName, rawArgs, serverHint = "") {
  const hint = String(serverHint || "").trim();
  if (!hint) return rawArgs;

  const parsedArgs = parseAssistantToolArgs(rawArgs);
  if (!parsedArgs || typeof parsedArgs !== "object" || Array.isArray(parsedArgs)) {
    return rawArgs;
  }

  const name = String(toolName || "").trim();
  const nextArgs = { ...parsedArgs };
  let changed = false;

  const needsServerFallback = [
    "power_server",
    "open_server_page",
    "check_server_console",
    "check_server_limits",
    "diagnose_server",
    "inspect_server",
    "list_files",
    "read_file",
    "write_file",
    "create_directory",
    "rename_path",
    "delete_path",
    "install_plugin",
  ].includes(name);

  if (needsServerFallback && isWeakAssistantServerReference(nextArgs.server)) {
    nextArgs.server = hint;
    changed = true;
  }

  if (name === "update_minecraft_server_property" && String(nextArgs.scope || "").trim().toLowerCase() !== "all_accessible_minecraft") {
    if (isWeakAssistantServerReference(nextArgs.server)) {
      nextArgs.server = hint;
      changed = true;
    }
    if (!String(nextArgs.scope || "").trim()) {
      nextArgs.scope = "single_server";
      changed = true;
    }
  }

  if (!changed) return rawArgs;
  return typeof rawArgs === "string" ? JSON.stringify(nextArgs) : nextArgs;
}

function buildWriteThenPowerReply(writeResult, powerResult) {
  const writeReply = buildReplyFromToolResult("write_file", writeResult) || "File updated.";
  const powerReply = buildReplyFromToolResult("power_server", powerResult)
    || String(powerResult?.error || powerResult?.detail || "Power action failed.").trim();
  return `${writeReply} ${powerReply}`.trim();
}

function buildPowerOnlyCapabilityReply() {
  return "I can currently control servers, share direct links to server pages and admin settings tools, open account security popups, check live load, inspect a server console, and check a server's RAM, CPU, and storage headroom.";
}

function buildInstantSmallTalkReply(message) {
  const text = String(message || "").trim();
  const lower = text.toLowerCase();
  if (!lower) return "";

  if (/^(?:hey|hi|hello|yo|sup|what'?s up|good morning|good afternoon|good evening)(?:\s+(?:there|adpanel))?[!?.]*$/i.test(text)) {
    return "Hey.";
  }

  if (/^(?:thanks|thank you|ty|thx|nice|perfect|great|awesome)[!?.]*$/i.test(text)) {
    return "You're welcome.";
  }

  if (/^(?:how are you|are you there|you there|still there)[!?.]*$/i.test(text)) {
    return "I'm here.";
  }

  if (/^(?:what can you do|what do you do|help|help me|capabilities|what are your capabilities)[!?.]*$/i.test(text)) {
    return buildPowerOnlyCapabilityReply();
  }

  return "";
}

function buildRestrictedIntentSystemHint(intents = {}) {
  const hints = [
    "Older chat may help resolve omitted references, but the latest explicit user request always wins.",
  ];

  if (intents.directAccountFlow?.flow) {
    hints.push(`The latest user message is a clear account security request. Prefer calling open_account_flow with flow "${intents.directAccountFlow.flow}".`);
  }
  if (intents.directBulkPowerFlow?.action) {
    hints.push(`The latest user message is a clear bulk power request. Prefer calling power_accessible_servers with action "${intents.directBulkPowerFlow.action}".`);
  }
  if (intents.directSinglePowerFlow?.args?.action) {
    hints.push(`The latest user message is a clear single-server power request. Prefer calling power_server with action "${intents.directSinglePowerFlow.args.action}" and the user's raw server wording.`);
  }
  if (intents.directLoadLookup?.toolName) {
    hints.push(`The latest user message is a clear live load lookup. Prefer calling ${intents.directLoadLookup.toolName}.`);
  }
  if (intents.directConsoleCheck?.args?.server) {
    hints.push("The latest user message is a clear live console request. Prefer calling check_server_console with the user's raw server wording.");
  }
  if (intents.directServerLimits?.args?.server) {
    hints.push("The latest user message is a clear per-server quota or limits request. Prefer calling check_server_limits with the user's raw server wording.");
  }
  if (intents.directOpenServerPage?.args?.server) {
    hints.push(`The latest user message is a clear server navigation request. Prefer calling open_server_page with section "${normalizeAssistantServerPageSection(intents.directOpenServerPage.args.section)}".`);
  }
  if (intents.directOpenSettings?.args?.panel || intents.directOpenSettings?.args?.action) {
    hints.push("The latest user message is a clear settings navigation request. Prefer calling open_settings_destination.");
  }

  return hints.join(" ").trim();
}

function extractJsonObjectFromText(value) {
  const text = String(value || "").trim();
  if (!text) return "";

  const fencedMatch = text.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fencedMatch && fencedMatch[1]) {
    return String(fencedMatch[1]).trim();
  }

  const firstBrace = text.indexOf("{");
  const lastBrace = text.lastIndexOf("}");
  if (firstBrace !== -1 && lastBrace !== -1 && lastBrace > firstBrace) {
    return text.slice(firstBrace, lastBrace + 1).trim();
  }

  return text;
}

function parseRestrictedAssistantPlan(rawContent) {
  const extracted = extractJsonObjectFromText(rawContent);
  if (!extracted) return null;

  try {
    const parsed = JSON.parse(extracted);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function normalizeRestrictedToolName(value) {
  const name = String(value || "").trim();
  return [
    "power_server",
    "power_accessible_servers",
    "open_server_page",
    "open_settings_destination",
    "open_account_flow",
    "get_busiest_node",
    "get_busiest_server",
    "check_server_console",
    "check_server_limits",
  ].includes(name) ? name : "";
}

function sanitizeRestrictedToolArgs(toolName, rawArgs) {
  const parsed = rawArgs && typeof rawArgs === "object" && !Array.isArray(rawArgs) ? rawArgs : {};

  if (toolName === "power_server") {
    const action = detectAssistantPowerAction(parsed.action);
    return {
      server: String(parsed.server || parsed.query || "").trim(),
      action: action || "",
    };
  }

  if (toolName === "power_accessible_servers") {
    const action = detectAssistantPowerAction(parsed.action);
    return {
      action: action || "",
    };
  }

  if (toolName === "open_server_page") {
    const server = String(parsed.server || parsed.query || "").trim();
    const rawSection = [
      parsed.section,
      parsed.destination,
      parsed.area,
      parsed.page,
      parsed.tab,
    ].find((value) => String(value || "").trim());
    const nextArgs = {};
    if (server) {
      nextArgs.server = server;
    }
    if (rawSection) {
      nextArgs.section = normalizeAssistantServerPageSection(rawSection);
    }
    return nextArgs;
  }

  if (toolName === "open_settings_destination") {
    const rawAction = [
      parsed.action,
      parsed.modal,
      parsed.destination,
      parsed.popup,
    ].find((value) => String(value || "").trim());
    const rawPanel = [
      parsed.panel,
      parsed.section,
      parsed.category,
      parsed.destination,
      parsed.page,
      parsed.tab,
    ].find((value) => String(value || "").trim());
    const action = normalizeAssistantSettingsAction(rawAction || "");
    const panel = normalizeAssistantSettingsPanel(rawPanel || "")
      || (action ? inferAssistantSettingsPanelFromAction(action) : "");
    const nextArgs = {};
    if (panel) {
      nextArgs.panel = panel;
    }
    if (action) {
      nextArgs.action = action;
    }
    return nextArgs;
  }

  if (toolName === "open_account_flow") {
    const flow = String(parsed.flow || "").trim();
    return {
      flow: [
        "change_password",
        "change_email",
        "change_2fa",
        "recover_password",
        "recover_email",
        "recover_2fa",
      ].includes(flow) ? flow : "",
    };
  }

  if (toolName === "get_busiest_node" || toolName === "get_busiest_server") {
    const metricDetails = inferAssistantMetricDetails([
      parsed.metric,
      Array.isArray(parsed.includeMetrics) ? parsed.includeMetrics.join(" ") : parsed.includeMetrics,
    ].filter(Boolean).join(" "));
    return {
      metric: metricDetails.metric,
      includeMetrics: metricDetails.includeMetrics,
    };
  }

  if (toolName === "check_server_console") {
    return {
      server: String(parsed.server || parsed.query || "").trim(),
      question: String(parsed.question || "").trim(),
    };
  }

  if (toolName === "check_server_limits") {
    const metricDetails = inferAssistantMetricDetails([
      parsed.question,
      Array.isArray(parsed.includeMetrics) ? parsed.includeMetrics.join(" ") : parsed.includeMetrics,
    ].filter(Boolean).join(" "));
    return {
      server: String(parsed.server || parsed.query || "").trim(),
      question: String(parsed.question || "").trim(),
      includeMetrics: metricDetails.includeMetrics,
    };
  }

  return {};
}

function hasMeaningfulRestrictedArgValue(value) {
  if (Array.isArray(value)) {
    return value.some((entry) => hasMeaningfulRestrictedArgValue(entry));
  }
  if (value == null) {
    return false;
  }
  if (typeof value === "string") {
    return value.trim().length > 0;
  }
  if (typeof value === "object") {
    return Object.values(value).some((entry) => hasMeaningfulRestrictedArgValue(entry));
  }
  return true;
}

function mergeRestrictedToolArgs(fallbackArgs, selectedArgs) {
  const fallback = fallbackArgs && typeof fallbackArgs === "object" && !Array.isArray(fallbackArgs)
    ? { ...fallbackArgs }
    : {};
  const selected = selectedArgs && typeof selectedArgs === "object" && !Array.isArray(selectedArgs)
    ? selectedArgs
    : {};

  for (const [key, value] of Object.entries(selected)) {
    if (!hasMeaningfulRestrictedArgValue(value)) {
      continue;
    }
    fallback[key] = value;
  }

  return fallback;
}

function buildRestrictedFallbackPlan({
  trimmedMessage,
  directAccountFlow,
  directBulkPowerFlow,
  directSinglePowerFlow,
  directLoadLookup,
  directConsoleCheck,
  directServerLimits,
  directOpenServerPage,
  directOpenSettings,
}) {
  if (directAccountFlow) {
    return {
      reply: directAccountFlow.reply,
      toolName: "open_account_flow",
      toolArgs: { flow: directAccountFlow.flow },
    };
  }

  if (directBulkPowerFlow) {
    return {
      reply: "",
      toolName: "power_accessible_servers",
      toolArgs: { action: directBulkPowerFlow.action },
    };
  }

  if (directSinglePowerFlow) {
    return {
      reply: "",
      toolName: "power_server",
      toolArgs: directSinglePowerFlow.args,
    };
  }

  if (directLoadLookup) {
    return {
      reply: "",
      toolName: directLoadLookup.toolName,
      toolArgs: directLoadLookup.args,
    };
  }

  if (directConsoleCheck) {
    return {
      reply: "",
      toolName: "check_server_console",
      toolArgs: directConsoleCheck.args,
    };
  }

  if (directServerLimits) {
    return {
      reply: "",
      toolName: "check_server_limits",
      toolArgs: directServerLimits.args,
    };
  }

  if (directOpenServerPage) {
    return {
      reply: "",
      toolName: "open_server_page",
      toolArgs: directOpenServerPage.args,
    };
  }

  if (directOpenSettings) {
    return {
      reply: "",
      toolName: "open_settings_destination",
      toolArgs: directOpenSettings.args,
    };
  }

  return {
    reply: "",
    toolName: "",
    toolArgs: {},
  };
}

async function buildRestrictedSingleShotPlan({
  provider,
  apiKey,
  modelName,
  systemPrompt,
  latestUserMessage,
  inferredServerContextMessage,
  restrictedIntentSystemHint,
  conversationContextMessage,
  persistedMessages,
  requestChatCompletion,
}) {
  const { olderContextMessage, recentMessages } = buildAssistantModelConversationWindow(persistedMessages, {
    recentCount: 16,
    olderTranscriptCount: 10,
    olderTranscriptTotalLimit: 1400,
  });

  const response = await requestChatCompletion(provider, apiKey, {
    model: modelName,
    temperature: 0.1,
    max_tokens: 180,
    messages: [
      { role: "system", content: systemPrompt },
      {
        role: "system",
        content: [
          "Return exactly one JSON object and nothing else.",
          'Schema: {"reply":"short natural reply for the user","toolName":"power_server|power_accessible_servers|open_server_page|open_settings_destination|open_account_flow|get_busiest_node|get_busiest_server|check_server_console|check_server_limits|","toolArgs":{}}',
          "Use an empty toolName when no tool is needed and answer directly in reply.",
          "If a tool is needed, keep reply short and queued-style, such as checking now or preparing a link now.",
          "Treat the latest user message as the main instruction. Older messages may only help resolve omitted references like it, that server, or short follow-up questions.",
          "If the latest message explicitly names a server, action, section, destination, popup, or panel, do not let older context override it.",
          "Do not claim the action is finished before the browser runs it.",
          "For navigation tools, never say the page was opened automatically. The dashboard will provide a clickable link the user can choose to open.",
          "For messy natural requests about startup time, crash cause, logs, whether a server started correctly, or whether it is running well, prefer check_server_console.",
          "For one named server questions about RAM, CPU, disk, storage, quotas, remaining space, or whether it is close to its limits, prefer check_server_limits.",
          "For all accessible servers power requests, prefer power_accessible_servers.",
          "For one named server power requests, prefer power_server and pass the user's raw server wording in toolArgs.server.",
          "For requests to open, manage, configure, edit, or access a server page, console, backups, files, scheduler, activity, store, resource stats, or similar server areas, prefer open_server_page.",
          "For admin requests to open, manage, configure, edit, or access settings pages or settings modals such as nodes, appearance, branding, maintenance, captcha, databases, quick actions, or webhooks, prefer open_settings_destination.",
          "For load questions, choose get_busiest_node or get_busiest_server and include any asked metrics.",
          "Use recent conversation context when the user refers to earlier answers.",
          "The latest explicit user request always overrides older context.",
          "Use older context only to resolve omitted references such as it, that, there, or the server we just discussed.",
          "If the latest user message explicitly names a server, destination, section, action, popup, or settings area, do not let older context replace it.",
        ].join(" "),
      },
      ...(latestUserMessage ? [{
        role: "system",
        content: `Primary task: answer the latest user message exactly as requested: ${JSON.stringify(String(latestUserMessage || "").trim())}.`,
      }] : []),
      ...(inferredServerContextMessage ? [{ role: "system", content: inferredServerContextMessage }] : []),
      ...(restrictedIntentSystemHint ? [{ role: "system", content: restrictedIntentSystemHint }] : []),
      ...(conversationContextMessage ? [{ role: "system", content: conversationContextMessage }] : []),
      ...(olderContextMessage ? [{ role: "system", content: olderContextMessage }] : []),
      ...recentMessages,
    ],
  });

  return response;
}

function coerceFiniteNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function convertAmountToMb(amount, unit) {
  const numeric = coerceFiniteNumber(amount);
  const normalizedUnit = String(unit || "").trim().toLowerCase();
  if (numeric == null) return null;
  if (normalizedUnit === "gb") return Math.round(numeric * 1024);
  if (normalizedUnit === "tb") return Math.round(numeric * 1024 * 1024);
  return Math.round(numeric);
}

function getRecentMessagesByRole(messages, role, limit = 14) {
  return (Array.isArray(messages) ? messages : [])
    .filter((entry) => String(entry?.role || "").toLowerCase() === String(role || "").toLowerCase())
    .slice(-limit)
    .map((entry) => normalizeMessageContent(entry.content))
    .filter(Boolean);
}

function findLatestRegexValue(texts, patterns, transform) {
  const list = Array.isArray(texts) ? texts : [];
  for (let index = list.length - 1; index >= 0; index -= 1) {
    const text = String(list[index] || "");
    for (const pattern of patterns) {
      const match = pattern.exec(text);
      if (!match) continue;
      const result = typeof transform === "function" ? transform(match, text) : match[1];
      if (result != null && result !== "") {
        return result;
      }
    }
  }
  return null;
}

function detectProvisioningDefaults(messages) {
  const recentMessages = Array.isArray(messages) ? messages : [];
  const userTexts = getRecentMessagesByRole(recentMessages, "user", 10);
  if (userTexts.some((text) => /\b(use|with|pick|choose|go with|take)\s+(the\s+)?defaults?\b/i.test(text))) {
    return true;
  }
  if (userTexts.some((text) => /\b(use|with|pick|choose|go with|take)\s+(the\s+)?recommended\b/i.test(text))) {
    return true;
  }
  if (userTexts.some((text) => /\bwhatever you recommend\b/i.test(text))) {
    return true;
  }

  const lastUserText = String(userTexts[userTexts.length - 1] || "").trim();
  if (lastUserText && isAffirmativeShortReply(lastUserText) && assistantAskedDefaultsQuestion(recentMessages)) {
    return true;
  }

  return false;
}

function detectProvisioningNeedsResourcePrompt(messages) {
  const userTexts = getRecentMessagesByRole(messages, "user", 10);
  if (!userTexts.length) return false;
  const mentionedResources = userTexts.some((text) => /\b(limit(?:ed)?|resources?|ram|memory|cpu|storage|disk)\b/i.test(text));
  const mentionedValues = userTexts.some((text) => /\b\d+(?:\.\d+)?\s*(?:mb|gb|tb|cores?)\b/i.test(text));
  return mentionedResources && !mentionedValues;
}

function messageMentionsPluginInstall(message) {
  const text = String(message || "").trim().toLowerCase();
  if (!text) return false;
  return /\b(install|add|download|setup|set up)\b/.test(text) && /\b(plugin|plugins|modrinth|viaversion|geyser|luckperms|vault)\b/.test(text);
}

function resolveEntryByAliases(entries, requestedValue, aliasGetter) {
  const requested = String(requestedValue || "").trim().toLowerCase();
  if (!requested) return null;
  const sourceEntries = Array.isArray(entries) ? entries : [];

  const exactMatch = sourceEntries.find((entry) =>
    (typeof aliasGetter === "function" ? aliasGetter(entry) : [])
      .map((alias) => String(alias || "").trim().toLowerCase())
      .includes(requested)
  );
  if (exactMatch) return exactMatch;

  return sourceEntries.find((entry) =>
    (typeof aliasGetter === "function" ? aliasGetter(entry) : [])
      .map((alias) => String(alias || "").trim().toLowerCase())
      .some((alias) => alias.includes(requested) || requested.includes(alias))
  ) || null;
}

function findAssistantSuggestedEntry(lastAssistantText, entries, aliasGetter) {
  const text = normalizeLooseText(lastAssistantText);
  if (!text) return null;

  const matches = [];
  for (const entry of Array.isArray(entries) ? entries : []) {
    const aliases = (typeof aliasGetter === "function" ? aliasGetter(entry) : [])
      .map((alias) => normalizeLooseText(alias))
      .filter(Boolean)
      .sort((left, right) => right.length - left.length);

    if (!aliases.length) continue;
    const matched = aliases.some((alias) => {
      const pattern = new RegExp(`(^|[^a-z0-9])${escapeRegex(alias)}(?=$|[^a-z0-9])`, "i");
      return pattern.test(text);
    });
    if (matched) matches.push(entry);
  }

  return matches.length === 1 ? matches[0] : null;
}

function formatChoiceList(values, limit = 3) {
  const items = Array.from(new Set((Array.isArray(values) ? values : []).map((value) => String(value || "").trim()).filter(Boolean))).slice(0, limit);
  if (!items.length) return "";
  if (items.length === 1) return items[0];
  if (items.length === 2) return `${items[0]} or ${items[1]}`;
  return `${items.slice(0, -1).join(", ")}, or ${items[items.length - 1]}`;
}

function isVagueProvisioningReply(message) {
  const text = String(message || "").trim().toLowerCase();
  if (!text) return true;
  if (/\b(which|what|how much|how many|choose|pick|tell me)\b/.test(text) && /\b(template|node|name|host port|port|ram|cpu|storage|defaults?)\b/.test(text)) {
    return false;
  }
  if (text.split(/\s+/).filter(Boolean).length <= 4 && !/\b(template|node|name|host port|port|ram|cpu|storage|created|defaults?)\b/.test(text)) {
    return true;
  }
  return /\b(i need|need one more detail|need more detail|need more information|need to know more|tell me more)\b/.test(text);
}

  function findLatestAliasEntry(texts, entries, aliasGetter) {
  const sourceTexts = Array.isArray(texts) ? texts : [];
  const sourceEntries = Array.isArray(entries) ? entries : [];
  const preparedEntries = sourceEntries
    .map((entry) => {
      const aliases = (typeof aliasGetter === "function" ? aliasGetter(entry) : [])
        .map((alias) => String(alias || "").trim())
        .filter(Boolean)
        .sort((left, right) => right.length - left.length);
      return { entry, aliases };
    })
    .filter((item) => item.aliases.length > 0);

  for (let textIndex = sourceTexts.length - 1; textIndex >= 0; textIndex -= 1) {
    const text = normalizeLooseText(sourceTexts[textIndex]);
    for (const item of preparedEntries) {
      for (const alias of item.aliases) {
        const aliasText = normalizeLooseText(alias);
        if (!aliasText) continue;
        const pattern = new RegExp(`(^|[^a-z0-9])${escapeRegex(aliasText)}(?=$|[^a-z0-9])`, "i");
        if (pattern.test(text)) {
          return item.entry;
        }
      }
    }
  }

  return null;
}

function parseCarryForwardProvisioningContext(texts) {
  const sourceTexts = Array.isArray(texts) ? texts : [];
  const carryText = sourceTexts
    .slice()
    .reverse()
    .find((text) => /\bcarry-forward provisioning context:\b/i.test(String(text || "")));
  if (!carryText) {
    return {};
  }

  const readField = (name) => {
    const match = new RegExp(`\\b${escapeRegex(name)}=([^;]+)`, "i").exec(String(carryText || ""));
    return match ? String(match[1] || "").trim() : "";
  };
  const readIntField = (name) => {
    const raw = readField(name);
    const parsed = parseInt(raw, 10);
    return Number.isInteger(parsed) ? parsed : null;
  };
  const readFloatField = (name) => {
    const raw = readField(name);
    const parsed = parseFloat(raw);
    return Number.isFinite(parsed) ? parsed : null;
  };

  const nodeValue = readField("node");
  return {
    templateId: readField("template") || null,
    templateName: readField("template") || null,
    nodeId: nodeValue || null,
    nodeName: nodeValue || null,
    serverName: readField("serverName") || null,
    mcFork: readField("mcFork") || null,
    mcVersion: readField("mcVersion") || null,
    hostPort: readIntField("hostPort"),
    ramMb: readIntField("ramMb"),
    cpuCores: readFloatField("cpuCores"),
    storageMb: readIntField("storageMb"),
    useDefaults: /^true$/i.test(readField("useDefaults")),
    wantsResourcePrompt: /^true$/i.test(readField("wantsResourcePrompt")),
  };
}

function jsonApiRequest(fullUrl, { method = "POST", headers = {}, body = null, timeoutMs = 60_000, timeoutMessage = "Request timeout", agent = null } = {}) {
  return new Promise((resolve, reject) => {
    const payload = body == null ? null : (typeof body === "string" ? body : JSON.stringify(body || {}));
    const url = new URL(fullUrl);
    const req = https.request(
      {
        hostname: url.hostname,
        port: Number(url.port) || 443,
        path: `${url.pathname}${url.search}`,
        method,
        headers: Object.assign({}, headers, payload == null ? {} : {
          "Content-Length": Buffer.byteLength(payload),
        }),
        agent: agent || undefined,
        servername: url.hostname || undefined,
        timeout: timeoutMs,
      },
      (res) => {
        let data = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => { data += chunk; });
        res.on("end", () => {
          try {
            resolve({
              status: res.statusCode || 0,
              data: data ? JSON.parse(data) : {},
            });
          } catch {
            resolve({
              status: res.statusCode || 0,
              data: { raw: data },
            });
          }
        });
      }
    );

    req.on("error", reject);
    req.on("timeout", () => req.destroy(new Error(timeoutMessage)));
    if (payload != null) {
      req.write(payload);
    }
    req.end();
  });
}

function groqMultipartRequest(pathname, apiKey, formData, timeoutMs = 60_000) {
  return new Promise((resolve, reject) => {
    const headers = formData.getHeaders({
      "Authorization": `Bearer ${apiKey}`,
    });

    const req = https.request(
      {
        hostname: "api.groq.com",
        port: 443,
        path: pathname,
        method: "POST",
        headers,
        agent: ASSISTANT_PROVIDER_HTTPS_AGENTS.groq,
        servername: "api.groq.com",
        timeout: timeoutMs,
      },
      (res) => {
        let data = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => { data += chunk; });
        res.on("end", () => {
          try {
            resolve({
              status: res.statusCode || 0,
              data: data ? JSON.parse(data) : {},
            });
          } catch {
            resolve({
              status: res.statusCode || 0,
              data: { raw: data },
            });
          }
        });
      }
    );

    req.on("error", reject);
    req.on("timeout", () => req.destroy(new Error("Groq request timeout")));
    formData.pipe(req);
  });
}

function createDashboardAssistantService(deps) {
  if (!deps || typeof deps !== "object") {
    throw new Error("Dashboard assistant dependencies are required.");
  }

  const runTool = createDashboardAssistantToolRunner(deps);
  let initPromise = null;
  // Keep decrypted provider keys in memory so chat requests avoid repeated .env reads and crypto work.
  const providerSecretCache = {
    loaded: false,
    providerPreference: "",
    keys: {
      groq: "",
      google: "",
    },
    encrypted: {
      groq: "",
      google: "",
    },
  };

  function loadProviderSecretCache() {
    const env = typeof deps.readEnvFile === "function" ? deps.readEnvFile() : {};
    const nextKeys = { groq: "", google: "" };
    const nextEncrypted = { groq: "", google: "" };

    for (const providerId of Object.keys(PROVIDER_LABELS)) {
      const secretConfig = getAssistantProviderSecretConfig(providerId);
      if (!secretConfig) continue;

      const encryptedValue = sanitizeToken(
        env[secretConfig.encrypted] ||
        process.env[secretConfig.encrypted] ||
        ""
      );

      let resolvedKey = "";
      if (encryptedValue) {
        try {
          resolvedKey = decryptAssistantProviderToken(encryptedValue);
        } catch (error) {
          console.error(`[dashboard-assistant] Failed to decrypt ${providerId} API key:`, error);
        }
      }

      if (!resolvedKey) {
        for (const legacyEnvKey of secretConfig.legacy) {
          resolvedKey = sanitizeToken(env[legacyEnvKey] || process.env[legacyEnvKey] || "");
          if (resolvedKey) break;
        }
      }

      nextKeys[providerId] = resolvedKey;
      nextEncrypted[providerId] = encryptedValue;
    }

    providerSecretCache.providerPreference = sanitizeProvider(
      env.DASHBOARD_ASSISTANT_PROVIDER ||
      process.env.DASHBOARD_ASSISTANT_PROVIDER ||
      ""
    );
    providerSecretCache.keys = nextKeys;
    providerSecretCache.encrypted = nextEncrypted;
    providerSecretCache.loaded = true;
    return providerSecretCache;
  }

  function ensureProviderSecretCache() {
    if (providerSecretCache.loaded) return providerSecretCache;
    return loadProviderSecretCache();
  }

  function getGoogleKey() {
    return sanitizeToken(ensureProviderSecretCache().keys.google || "");
  }

  function getGroqKey() {
    return sanitizeToken(ensureProviderSecretCache().keys.groq || "");
  }

  function getStoredProviderPreference() {
    return sanitizeProvider(ensureProviderSecretCache().providerPreference || "");
  }

  function getProviderKey(provider) {
    if (provider === "google") return getGoogleKey();
    if (provider === "groq") return getGroqKey();
    return "";
  }

  function getResolvedProvider() {
    const preferred = getStoredProviderPreference();
    if (preferred && getProviderKey(preferred)) {
      return preferred;
    }
    if (getGroqKey()) return "groq";
    if (getGoogleKey()) return "google";
    return preferred || "groq";
  }

  function getChatModelForProvider(provider) {
    return provider === "google" ? DEFAULT_GOOGLE_MODEL : DEFAULT_GROQ_MODEL;
  }

  function getTranscriptionModelForProvider(provider) {
    return provider === "google" ? DEFAULT_GOOGLE_TRANSCRIPTION_MODEL : DEFAULT_GROQ_TRANSCRIPTION_MODEL;
  }

  async function loadAccessibleServersForAssistant(userEmail) {
    const normalizedEmail = String(userEmail || "").trim().toLowerCase();
    const cachedEntry = accessibleServersForAssistantCache.get(normalizedEmail);
    if (cachedEntry && Date.now() - cachedEntry.updatedAt < ACCESSIBLE_SERVERS_CACHE_TTL_MS) {
      return cachedEntry.list;
    }

    const list = ((typeof deps.loadServersIndex === "function" ? await deps.loadServersIndex() : []) || []).filter(Boolean);
    const user = typeof deps.findUserByEmail === "function" ? await deps.findUserByEmail(normalizedEmail) : null;
    let resolvedList = list;

    if (!user?.admin) {
      const accessList = ((typeof deps.getAccessListForEmail === "function" ? await deps.getAccessListForEmail(normalizedEmail) : []) || [])
        .map((item) => String(item || "").trim().toLowerCase())
        .filter(Boolean);
      const loweredAccess = new Set(accessList);
      resolvedList = loweredAccess.has("all")
        ? list
        : list.filter((entry) => entry?.name && loweredAccess.has(String(entry.name).trim().toLowerCase()));
    }

    accessibleServersForAssistantCache.set(normalizedEmail, {
      updatedAt: Date.now(),
      list: resolvedList,
    });
    return resolvedList;
  }

  async function ensureTables() {
    if (initPromise) return initPromise;

    initPromise = (async () => {
      await deps.db.query(`
        CREATE TABLE IF NOT EXISTS dashboard_assistant_chats (
          id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          user_email VARCHAR(255) NOT NULL,
          title VARCHAR(255) NOT NULL DEFAULT '${DEFAULT_CHAT_TITLE}',
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          KEY idx_dashboard_assistant_chats_user (user_email),
          KEY idx_dashboard_assistant_chats_updated (updated_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
      `);

      await deps.db.query(`
        CREATE TABLE IF NOT EXISTS dashboard_assistant_messages (
          id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          chat_id BIGINT UNSIGNED NOT NULL,
          role ENUM('user', 'assistant', 'system') NOT NULL,
          content LONGTEXT NOT NULL,
          model VARCHAR(255) NULL,
          thinking_time_ms INT UNSIGNED NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          KEY idx_dashboard_assistant_messages_chat (chat_id),
          KEY idx_dashboard_assistant_messages_created (created_at),
          CONSTRAINT fk_dashboard_assistant_messages_chat
            FOREIGN KEY (chat_id) REFERENCES dashboard_assistant_chats(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
      `);
    })().catch((error) => {
      initPromise = null;
      throw error;
    });

    return initPromise;
  }

  async function getPrimaryChat(userEmail) {
    await ensureTables();
    const normalizedEmail = String(userEmail || "").trim().toLowerCase();
    const existingRows = await deps.db.query(
      `SELECT id, title, created_at, updated_at
       FROM dashboard_assistant_chats
       WHERE user_email = ?
       ORDER BY updated_at DESC
       LIMIT 1`,
      [normalizedEmail]
    );

    if (existingRows[0]) {
      return existingRows[0];
    }

    const result = await deps.db.query(
      "INSERT INTO dashboard_assistant_chats (user_email, title) VALUES (?, ?)",
      [normalizedEmail, DEFAULT_CHAT_TITLE]
    );

    const createdRows = await deps.db.query(
      "SELECT id, title, created_at, updated_at FROM dashboard_assistant_chats WHERE id = ? LIMIT 1",
      [result.insertId]
    );
    return createdRows[0];
  }

  async function createFreshChat(userEmail) {
    await ensureTables();
    const normalizedEmail = String(userEmail || "").trim().toLowerCase();
    const result = await deps.db.query(
      "INSERT INTO dashboard_assistant_chats (user_email, title) VALUES (?, ?)",
      [normalizedEmail, DEFAULT_CHAT_TITLE]
    );
    const rows = await deps.db.query(
      "SELECT id, title, created_at, updated_at FROM dashboard_assistant_chats WHERE id = ? LIMIT 1",
      [result.insertId]
    );
    return rows[0];
  }

  async function getChatById(userEmail, chatId) {
    await ensureTables();
    const normalizedEmail = String(userEmail || "").trim().toLowerCase();
    const numericChatId = Number(chatId);
    if (!Number.isFinite(numericChatId) || numericChatId <= 0) {
      return null;
    }
    const rows = await deps.db.query(
      `SELECT id, title, created_at, updated_at
       FROM dashboard_assistant_chats
       WHERE id = ? AND user_email = ?
       LIMIT 1`,
      [numericChatId, normalizedEmail]
    );
    return rows[0] || null;
  }

  async function getChatMessages(chatId, limit = 60) {
    await ensureTables();
    const safeLimit = Math.max(1, Math.min(200, parseInt(limit, 10) || 60));
    const rows = await deps.db.query(
      `SELECT id, role, content, model, thinking_time_ms, created_at
       FROM dashboard_assistant_messages
       WHERE chat_id = ?
       ORDER BY created_at ASC, id ASC
       LIMIT ${safeLimit}`,
      [chatId]
    );
    return Array.isArray(rows) ? rows : [];
  }

  async function getChatMessageCount(chatId) {
    await ensureTables();
    const rows = await deps.db.query(
      "SELECT COUNT(*) AS total FROM dashboard_assistant_messages WHERE chat_id = ? AND role IN ('user', 'assistant') LIMIT 1",
      [chatId]
    );
    return Number(rows?.[0]?.total || 0);
  }

  async function addMessage(chatId, { role, content, model = null, thinkingTimeMs = null }) {
    await ensureTables();
    await deps.db.query(
      `INSERT INTO dashboard_assistant_messages (chat_id, role, content, model, thinking_time_ms)
       VALUES (?, ?, ?, ?, ?)`,
      [chatId, role, String(content || ""), model || null, thinkingTimeMs || null]
    );
    await deps.db.query("UPDATE dashboard_assistant_chats SET updated_at = NOW() WHERE id = ?", [chatId]);
  }

  async function deleteChat(userEmail, chatId) {
    await ensureTables();
    const normalizedEmail = String(userEmail || "").trim().toLowerCase();
    const numericChatId = Number(chatId);
    if (!Number.isFinite(numericChatId) || numericChatId <= 0) return;
    await deps.db.query(
      "DELETE FROM dashboard_assistant_chats WHERE id = ? AND user_email = ? LIMIT 1",
      [numericChatId, normalizedEmail]
    );
  }

  async function getActiveChat(userEmail, requestedChatId = null) {
    const requestedChat = requestedChatId ? await getChatById(userEmail, requestedChatId) : null;
    const chat = requestedChat || await getPrimaryChat(userEmail);
    if (!chat) return null;

    const messageCount = await getChatMessageCount(chat.id);
    if (messageCount < MAX_CHAT_MESSAGES) {
      return chat;
    }

    const previousMessages = await getChatMessages(chat.id, CHAT_HISTORY_LIMIT);
    const freshChat = await createFreshChat(userEmail);
    const carryForwardSummary = await buildCarryForwardSystemSummary(previousMessages);
    if (carryForwardSummary) {
      await addMessage(freshChat.id, {
        role: "system",
        content: carryForwardSummary,
        model: "adpanel-chat-handoff",
      });
    }
    if (chat.id !== freshChat.id) {
      await deleteChat(userEmail, chat.id).catch(() => {});
    }
    return freshChat;
  }

  async function inferProvisioningContext(messages) {
    const userTexts = getRecentMessagesByRole(messages, "user", 16);
    const assistantTexts = getRecentMessagesByRole(messages, "assistant", 12);
    const systemTexts = getRecentMessagesByRole(messages, "system", 6);
    const carryForwardContext = parseCarryForwardProvisioningContext(systemTexts);
    if (!userTexts.length && !assistantTexts.length && !Object.keys(carryForwardContext).length) return null;

    const provisioningThreadLikely =
      userTexts.some((text) => /\b(create|make|spin up|deploy|provision|new server|server)\b/i.test(text)) ||
      assistantTexts.some((text) => /\b(template|node|server name|host port|port|ram|cpu|storage|create)\b/i.test(text)) ||
      Object.keys(carryForwardContext).length > 0;
    if (!provisioningThreadLikely) return null;

    let templates = [];
    let nodes = [];
    try {
      templates = typeof deps.loadTemplatesFile === "function" ? (deps.loadTemplatesFile() || []) : [];
    } catch {
      templates = [];
    }
    try {
      nodes = typeof deps.loadNodes === "function" ? await deps.loadNodes() || [] : [];
    } catch {
      nodes = [];
    }

    const lastUserText = String(userTexts[userTexts.length - 1] || "").trim();
    const lastAssistantText = String(assistantTexts[assistantTexts.length - 1] || "").trim().toLowerCase();
    const acceptedAssistantSuggestion = isAffirmativeShortReply(lastUserText);
    const impliedMinecraft = userTexts.some((text) => /\b(minecraft|paper|purpur|spigot|bukkit|vanilla)\b/i.test(text));
    const impliedNodeJs = userTexts.some((text) => /\b(node\.?js|nodejs|discord bot|discord-bot)\b/i.test(text));
    const impliedPython = userTexts.some((text) => /\bpython\b/i.test(text));
    const template = findLatestAliasEntry(userTexts, templates, (entry) => [entry?.id, entry?.name]);
    const node = findLatestAliasEntry(userTexts, nodes, (entry) => [entry?.name, entry?.id, entry?.uuid]);
    const assistantSuggestedTemplate = acceptedAssistantSuggestion
      ? findAssistantSuggestedEntry(lastAssistantText, templates, (entry) => [entry?.id, entry?.name])
      : null;
    const assistantSuggestedNode = acceptedAssistantSuggestion
      ? findAssistantSuggestedEntry(lastAssistantText, nodes, (entry) => [entry?.name, entry?.id, entry?.uuid])
      : null;
    const hostPort = findLatestRegexValue(
      userTexts,
      [
        /\b(?:host\s+port|hostport|port)\s*(?:is\s+|to\s+|=)?(\d{1,5})\b/i,
        /\b(?:on|using)\s+port\s+(\d{1,5})\b/i,
        /\buse\s+(\d{1,5})\b/i,
        /^(\d{2,5})$/i,
      ],
      (match) => {
        const port = parseInt(match[1], 10);
        return Number.isInteger(port) && port >= 1 && port <= 65535 ? port : null;
      }
    );
    const ramMb = findLatestRegexValue(
      userTexts,
      [
        /\b(?:ram|memory)\s*(?:is\s+|to\s+|=)?(\d+(?:\.\d+)?)\s*(mb|gb|tb)\b/i,
        /\b(\d+(?:\.\d+)?)\s*(mb|gb|tb)\s*(?:of\s+)?(?:ram|memory)\b/i,
      ],
      (match) => convertAmountToMb(match[1], match[2])
    );
    const cpuCores = findLatestRegexValue(
      userTexts,
      [
        /\b(?:cpu|cores?)\s*(?:is\s+|to\s+|=)?(\d+(?:\.\d+)?)\b/i,
        /\b(\d+(?:\.\d+)?)\s*(?:cpu\s*cores?|cores?)\b/i,
      ],
      (match) => coerceFiniteNumber(match[1])
    );
    const storageMb = findLatestRegexValue(
      userTexts,
      [
        /\b(?:storage|disk)\s*(?:is\s+|to\s+|=)?(\d+(?:\.\d+)?)\s*(mb|gb|tb)\b/i,
        /\b(\d+(?:\.\d+)?)\s*(mb|gb|tb)\s*(?:of\s+)?(?:storage|disk)\b/i,
      ],
      (match) => convertAmountToMb(match[1], match[2])
    );
    const serverName = findLatestRegexValue(
      userTexts,
      [
        /\b(?:name(?:\s+it|\s+is)?|call\s+it|server\s+name(?:\s+is)?|create\s+(?:it\s+)?as)\s+["']?([a-z0-9._-]{2,48})["']?\b/i,
        /\b(?:named|called)\s+["']?([a-z0-9._-]{2,48})["']?\b/i,
      ],
      (match) => match[1]
    );
    const mcFork = findLatestRegexValue(
      userTexts,
      [
        /\b(paper|purpur|spigot|bukkit|vanilla)\b/i,
      ],
      (match) => String(match[1] || "").trim().toLowerCase()
    );
    const mcVersion = findLatestRegexValue(
      userTexts,
      [
        /\b(?:minecraft\s+)?(1\.\d{1,2}(?:\.\d{1,2})?)\b/i,
      ],
      (match) => String(match[1] || "").trim()
    );

    const context = {
      templateId: template?.id || assistantSuggestedTemplate?.id || carryForwardContext.templateId || (impliedMinecraft ? "minecraft" : (impliedNodeJs ? "nodejs" : (impliedPython ? "python" : null))),
      templateName: template?.name || template?.id || assistantSuggestedTemplate?.name || assistantSuggestedTemplate?.id || carryForwardContext.templateName || null,
      nodeId: node?.id || node?.uuid || node?.name || assistantSuggestedNode?.id || assistantSuggestedNode?.uuid || assistantSuggestedNode?.name || carryForwardContext.nodeId || null,
      nodeName: node?.name || node?.id || node?.uuid || assistantSuggestedNode?.name || assistantSuggestedNode?.id || assistantSuggestedNode?.uuid || carryForwardContext.nodeName || null,
      hostPort: hostPort ?? carryForwardContext.hostPort,
      ramMb: ramMb ?? carryForwardContext.ramMb,
      cpuCores: cpuCores ?? carryForwardContext.cpuCores,
      storageMb: storageMb ?? carryForwardContext.storageMb,
      serverName: serverName || carryForwardContext.serverName || null,
      mcFork: mcFork === "vanilla" ? "paper" : (mcFork || carryForwardContext.mcFork || null),
      mcVersion: mcVersion || carryForwardContext.mcVersion || null,
      useDefaults: detectProvisioningDefaults(messages) || !!carryForwardContext.useDefaults,
      wantsResourcePrompt: detectProvisioningNeedsResourcePrompt(messages) || !!carryForwardContext.wantsResourcePrompt,
    };

    if (!context.templateId && /\btemplate\b/.test(lastAssistantText) && lastUserText) {
      context.templateId = lastUserText;
    }
    if (!context.nodeId && /\bnode\b/.test(lastAssistantText) && lastUserText && !isAffirmativeShortReply(lastUserText)) {
      context.nodeId = lastUserText;
    }
    if (!context.serverName && /\bserver name\b|\bname the server\b|\bname should i use\b/.test(lastAssistantText) && lastUserText && !isAffirmativeShortReply(lastUserText)) {
      context.serverName = lastUserText;
    }
    if (context.hostPort == null && /\bport\b/.test(lastAssistantText) && /^\d{2,5}$/.test(lastUserText)) {
      const parsedPort = parseInt(lastUserText, 10);
      if (Number.isInteger(parsedPort) && parsedPort >= 1 && parsedPort <= 65535) {
        context.hostPort = parsedPort;
      }
    }
    if (context.hostPort == null && acceptedAssistantSuggestion && /\bport\b/.test(lastAssistantText)) {
      const defaultPortMatch = /\bdefault(?: is|:)?\s*(\d{2,5})\b/i.exec(lastAssistantText);
      if (defaultPortMatch) {
        const parsedPort = parseInt(defaultPortMatch[1], 10);
        if (Number.isInteger(parsedPort) && parsedPort >= 1 && parsedPort <= 65535) {
          context.hostPort = parsedPort;
        }
      }
    }
    if (context.ramMb == null && /\bram|memory\b/.test(lastAssistantText)) {
      const ramMatch = /\b(\d+(?:\.\d+)?)\s*(mb|gb|tb)\b/i.exec(lastUserText);
      if (ramMatch) context.ramMb = convertAmountToMb(ramMatch[1], ramMatch[2]);
    }
    if (context.cpuCores == null && /\bcpu|cores?\b/.test(lastAssistantText)) {
      const cpuMatch = /\b(\d+(?:\.\d+)?)\s*(?:cpu\s*cores?|cores?)\b/i.exec(lastUserText) || /^(\d+(?:\.\d+)?)$/i.exec(lastUserText);
      if (cpuMatch) context.cpuCores = coerceFiniteNumber(cpuMatch[1]);
    }
    if (context.storageMb == null && /\bstorage|disk\b/.test(lastAssistantText)) {
      const storageMatch = /\b(\d+(?:\.\d+)?)\s*(mb|gb|tb)\b/i.exec(lastUserText);
      if (storageMatch) context.storageMb = convertAmountToMb(storageMatch[1], storageMatch[2]);
    }

    return Object.values(context).some((value) => value != null && value !== "") ? context : null;
  }

  async function buildCarryForwardSystemSummary(messages) {
    const recentMessages = (Array.isArray(messages) ? messages : [])
      .filter((entry) => entry && (entry.role === "user" || entry.role === "assistant"))
      .slice(-24);
    if (!recentMessages.length) {
      return "";
    }

    const parts = [];
    const provisioningContext = await inferProvisioningContext(recentMessages);
    const provisioningSummary = buildConversationContextMessage(provisioningContext);
    if (provisioningSummary) {
      parts.push(provisioningSummary.replace(/^Recent inferred provisioning context:/, "Carry-forward provisioning context:"));
    }

    const lastAssistantText = getLastAssistantText(recentMessages);
    if (lastAssistantText && (assistantAskedProvisioningQuestion(recentMessages) || assistantAskedProvisioningConfirmation(recentMessages))) {
      parts.push(`Carry-forward last assistant prompt: ${lastAssistantText}`);
    }

    const recentUserTexts = getRecentMessagesByRole(recentMessages, "user", 5);
    if (recentUserTexts.length) {
      parts.push(`Carry-forward recent user intent: ${recentUserTexts.join(" | ").slice(0, 700)}`);
    }

    const visibleTranscript = buildCarryForwardVisibleTranscript(recentMessages, {
      count: 18,
      perMessageLimit: 180,
      totalLimit: 2600,
    });
    if (visibleTranscript) {
      parts.push(`Carry-forward visible chat transcript:\n${visibleTranscript}`);
    }

    return parts.join("\n\n").trim().slice(0, 4200);
  }

  function buildConversationContextMessage(context) {
    if (!context) return "";

    const parts = [];
    if (context.templateId || context.templateName) {
      parts.push(`template=${context.templateId || context.templateName}`);
    }
    if (context.nodeName || context.nodeId) {
      parts.push(`node=${context.nodeName || context.nodeId}`);
    }
    if (context.serverName) {
      parts.push(`serverName=${context.serverName}`);
    }
    if (context.mcFork) {
      parts.push(`mcFork=${context.mcFork}`);
    }
    if (context.mcVersion) {
      parts.push(`mcVersion=${context.mcVersion}`);
    }
    if (context.hostPort != null) {
      parts.push(`hostPort=${context.hostPort}`);
    }
    if (context.ramMb != null) {
      parts.push(`ramMb=${context.ramMb}`);
    }
    if (context.cpuCores != null) {
      parts.push(`cpuCores=${context.cpuCores}`);
    }
    if (context.storageMb != null) {
      parts.push(`storageMb=${context.storageMb}`);
    }
    if (context.useDefaults) {
      parts.push("useDefaults=true");
    }
    if (context.wantsResourcePrompt) {
      parts.push("wantsResourcePrompt=true");
    }

    if (!parts.length) return "";

    return `Recent inferred provisioning context: ${parts.join("; ")}. Reuse these values unless the user changes them. Ask only for missing or ambiguous fields.`;
  }

  async function buildProvisioningPlan(messages) {
    const context = await inferProvisioningContext(messages);
    if (!context) return null;

    let templates = [];
    let nodes = [];
    try {
      templates = typeof deps.loadTemplatesFile === "function" ? (deps.loadTemplatesFile() || []) : [];
    } catch {
      templates = [];
    }
    try {
      nodes = typeof deps.loadNodes === "function" ? await deps.loadNodes() || [] : [];
    } catch {
      nodes = [];
    }

    const template = resolveEntryByAliases(templates, context.templateId || context.templateName, (entry) => [entry?.id, entry?.name]);
    const node = resolveEntryByAliases(nodes, context.nodeId || context.nodeName, (entry) => [entry?.id, entry?.uuid, entry?.name]);
    const useDefaults = !!context.useDefaults;
    const templateDefaultPort = template?.defaultPort != null ? parseInt(template.defaultPort, 10) || null : null;

    const missingFields = [];
    if (!template) missingFields.push("template");
    if (!node) missingFields.push("node");
    if (!context.serverName) missingFields.push("name");
    if (context.hostPort == null && !(useDefaults && templateDefaultPort != null)) {
      missingFields.push("port");
    }
    const createArgs = {
      name: context.serverName || "",
      displayName: context.serverName || "",
      templateId: template?.id || context.templateId || "",
      nodeId: node?.id || node?.uuid || node?.name || context.nodeId || "",
      hostPort: context.hostPort != null ? context.hostPort : templateDefaultPort,
      mcFork: context.mcFork || undefined,
      mcVersion: context.mcVersion || undefined,
      resources: Object.assign(
        {},
        context.ramMb != null ? { ramMb: context.ramMb } : {},
        context.cpuCores != null ? { cpuCores: context.cpuCores } : {},
        context.storageMb != null ? { storageMb: context.storageMb } : {}
      ),
    };

    if (!Object.keys(createArgs.resources).length) {
      delete createArgs.resources;
    }

    return {
      context,
      template,
      node,
      useDefaults,
      wantsResourcePrompt: !!context.wantsResourcePrompt,
      templateDefaultPort,
      missingFields,
      createArgs,
      templateChoices: templates.slice(0, 3).map((entry) => entry?.name || entry?.id).filter(Boolean),
      nodeChoices: nodes.filter((entry) => entry?.online !== false).slice(0, 3).map((entry) => entry?.name || entry?.id || entry?.uuid).filter(Boolean),
    };
  }

  function buildProvisioningFollowUp(plan) {
    if (!plan) {
      return "Which detail should I use to create the server?";
    }

    const missing = Array.isArray(plan.missingFields) ? plan.missingFields : [];
    if (!missing.length) {
      return "I can create it now.";
    }

    const initialProvisioningPrompt =
      missing.includes("template") &&
      missing.includes("node") &&
      missing.includes("name") &&
      missing.includes("port");
    if (initialProvisioningPrompt) {
      return "Tell me the template, node, server name, and port. If you want limits, add RAM, CPU, and storage too.";
    }

    const missingCore = missing.filter((field) => ["template", "node", "name", "port"].includes(field));
    if (missingCore.length > 1) {
      const labels = missingCore.map((field) => {
        if (field === "template") return "template";
        if (field === "node") return "node";
        if (field === "name") return "server name";
        return "port";
      });
      return `Tell me the ${formatChoiceList(labels, 4)}.`;
    }

    if (missing.includes("template")) {
      const choices = formatChoiceList(plan.templateChoices);
      return choices ? `Which template should I use: ${choices}?` : "Which template should I use?";
    }
    if (missing.includes("node")) {
      const choices = formatChoiceList(plan.nodeChoices);
      return choices ? `Which node should I use: ${choices}?` : "Which node should I use?";
    }
    if (missing.includes("name")) {
      return "What should I name the server?";
    }

    if (missing.includes("port")) {
      return plan.templateDefaultPort != null
        ? `Which port should I use? The default is ${plan.templateDefaultPort}.`
        : "Which port should I use?";
    }

    if (plan.wantsResourcePrompt) {
      return "If you want limits, tell me the RAM, CPU, or storage.";
    }

    return "Which detail should I use to create the server?";
  }

  async function maybeRunProvisioningFallback({ userEmail, userIp, trimmedMessage, persistedMessages, clientActions }) {
    if (!isCreateServerConversation(trimmedMessage, persistedMessages)) {
      return null;
    }

    const plan = await buildProvisioningPlan(persistedMessages);
    if (!plan) {
      return null;
    }

    if (plan.missingFields.length > 0) {
      return {
        reply: buildProvisioningFollowUp(plan),
        model: "adpanel-provisioning-router",
        toolName: "",
        toolResult: null,
      };
    }

    if (plan.wantsResourcePrompt && !plan.createArgs.resources) {
      return {
        reply: "If you want limits, tell me the RAM, CPU, or storage.",
        model: "adpanel-provisioning-router",
        toolName: "",
        toolResult: null,
      };
    }

    const toolResult = await runTool({
      name: "create_server",
      args: plan.createArgs,
      userEmail,
      userIp,
      clientActions,
    });

    return {
      reply: buildReplyFromToolResult("create_server", toolResult) || String(toolResult?.error || toolResult?.detail || "That server could not be created.").trim(),
      model: "adpanel-provisioning-router",
      toolName: "create_server",
      toolResult,
    };
  }

  async function maybeUpdateChatTitle(chatId, userMessage) {
    const rows = await deps.db.query("SELECT title FROM dashboard_assistant_chats WHERE id = ? LIMIT 1", [chatId]);
    const currentTitle = String(rows[0]?.title || DEFAULT_CHAT_TITLE);
    if (currentTitle && currentTitle !== DEFAULT_CHAT_TITLE) {
      return;
    }

    const nextTitle = String(userMessage || "").trim().slice(0, 80) || DEFAULT_CHAT_TITLE;
    await deps.db.query("UPDATE dashboard_assistant_chats SET title = ? WHERE id = ?", [nextTitle, chatId]);
  }

  async function saveProviderToken({ provider, token }) {
    const resolvedProvider = sanitizeProvider(provider) || "groq";
    const providerLabel = getProviderLabel(resolvedProvider);
    const providerSecretConfig = getAssistantProviderSecretConfig(resolvedProvider);
    if (!providerSecretConfig) {
      throw new Error(`Unsupported provider: ${resolvedProvider}`);
    }

    const cachedSecrets = ensureProviderSecretCache();
    const existingKey = getProviderKey(resolvedProvider);
    const sanitized = sanitizeToken(token);
    const existingEncryptedValue = sanitizeToken(cachedSecrets.encrypted[resolvedProvider] || "");

    if (!sanitized && !existingKey) {
      throw new Error(`Please enter a valid ${providerLabel} API key.`);
    }

    const updates = { DASHBOARD_ASSISTANT_PROVIDER: resolvedProvider };
    const tokenToPersist = sanitized || (!existingEncryptedValue ? existingKey : "");
    const encryptedToken = tokenToPersist ? encryptAssistantProviderToken(tokenToPersist) : "";
    if (encryptedToken) {
      updates[providerSecretConfig.encrypted] = encryptedToken;
    }

    const saved = deps.writeEnvFileBatch(updates);
    if (!saved) {
      throw new Error(`Failed to save the ${providerLabel} API key.`);
    }

    process.env.DASHBOARD_ASSISTANT_PROVIDER = resolvedProvider;
    if (encryptedToken) {
      process.env[providerSecretConfig.encrypted] = encryptedToken;
    }

    const effectiveKey = sanitized || existingKey;
    cachedSecrets.providerPreference = resolvedProvider;
    cachedSecrets.keys[resolvedProvider] = effectiveKey;
    if (encryptedToken) {
      cachedSecrets.encrypted[resolvedProvider] = encryptedToken;
    }
    cachedSecrets.loaded = true;

    return {
      provider: resolvedProvider,
      label: providerLabel,
      configured: !!effectiveKey,
      obscuredValue: buildStoredKeyFieldValue(effectiveKey),
    };
  }

  async function transcribeWithGroq(apiKey, { buffer, mimeType, filename, language, prompt }) {
    const formData = new FormData();
    formData.append("file", buffer, {
      filename: inferTranscriptionFilename(filename, mimeType),
      contentType: mimeType || "audio/webm",
    });
    formData.append("model", DEFAULT_GROQ_TRANSCRIPTION_MODEL);
    formData.append("response_format", "json");
    formData.append("temperature", "0");

    const safeLanguage = sanitizeTranscriptionLanguage(language);
    if (safeLanguage) {
      formData.append("language", safeLanguage);
    }
    const safePrompt = sanitizeTranscriptionPrompt(prompt);
    if (safePrompt) {
      formData.append("prompt", safePrompt);
    }

    const response = await groqMultipartRequest("/openai/v1/audio/transcriptions", apiKey, formData, 90_000);
    if (response.status !== 200) {
      const detail = response.data?.error?.message || response.data?.error || response.data?.raw || `status ${response.status}`;
      throw new Error(detail);
    }

    const transcript = String(response.data?.text || "").trim();
    if (!transcript) {
      throw new Error("The recording could not be transcribed.");
    }

    return {
      transcript,
      model: DEFAULT_GROQ_TRANSCRIPTION_MODEL,
    };
  }

  async function transcribeWithGoogle(apiKey, { buffer, mimeType, prompt }) {
    const safePrompt = sanitizeTranscriptionPrompt(prompt) || "Transcribe this audio literally and accurately. Keep technical product words intact and prefer numeric digits.";
    const requestBody = {
      model: DEFAULT_GOOGLE_TRANSCRIPTION_MODEL,
      input: [
        { type: "text", text: safePrompt },
        {
          type: "audio",
          data: buffer.toString("base64"),
          mime_type: mimeType || "audio/webm",
        },
      ],
    };

    const response = await jsonApiRequest(GOOGLE_INTERACTIONS_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-goog-api-key": apiKey,
      },
      body: requestBody,
      agent: ASSISTANT_PROVIDER_HTTPS_AGENTS.google,
      timeoutMs: 90_000,
      timeoutMessage: "Google AI Studio request timeout",
    });

    if (response.status !== 200) {
      const detail = response.data?.error?.message || response.data?.error || response.data?.raw || `status ${response.status}`;
      throw new Error(detail);
    }

    const outputs = Array.isArray(response.data?.outputs) ? response.data.outputs : [];
    const transcript = String(outputs[outputs.length - 1]?.text || response.data?.output?.text || "").trim();
    if (!transcript) {
      throw new Error("The recording could not be transcribed.");
    }

    return {
      transcript,
      model: DEFAULT_GOOGLE_TRANSCRIPTION_MODEL,
    };
  }

  async function transcribeAudio({ buffer, mimeType, filename, language, prompt }) {
    const provider = getResolvedProvider();
    const apiKey = getProviderKey(provider);
    if (!apiKey) {
      throw new Error(`${getProviderLabel(provider)} is not configured.`);
    }

    if (!Buffer.isBuffer(buffer) || buffer.length === 0) {
      throw new Error("No audio data was received.");
    }

    if (buffer.length > AUDIO_UPLOAD_LIMIT_BYTES) {
      throw new Error("The audio recording is too large.");
    }

    if (provider === "google") {
      return transcribeWithGoogle(apiKey, { buffer, mimeType, filename, language, prompt });
    }
    return transcribeWithGroq(apiKey, { buffer, mimeType, filename, language, prompt });
  }

  async function requestChatCompletion(provider, apiKey, payload) {
    if (provider === "google") {
      return jsonApiRequest(GOOGLE_OPENAI_CHAT_URL, {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${apiKey}`,
          "Content-Type": "application/json",
        },
        body: Object.assign({ reasoning_effort: "low" }, payload),
        agent: ASSISTANT_PROVIDER_HTTPS_AGENTS.google,
        timeoutMs: 90_000,
        timeoutMessage: "Google AI Studio request timeout",
      });
    }

    return jsonApiRequest("https://api.groq.com/openai/v1/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: payload,
      agent: ASSISTANT_PROVIDER_HTTPS_AGENTS.groq,
      timeoutMs: 90_000,
      timeoutMessage: "Groq request timeout",
    });
  }

  function mergeContinuationText(base, continuation) {
    const first = String(base || "").trim();
    const second = String(continuation || "").trim();
    if (!first) return second;
    if (!second) return first;
    if (first === second) return first;
    if (second.toLowerCase().startsWith(first.toLowerCase())) return second;
    if (first.toLowerCase().endsWith(second.toLowerCase())) return first;
    const needsSpace = /[a-z0-9)]$/i.test(first) && /^[a-z0-9(]/i.test(second);
    return `${first}${needsSpace ? " " : ""}${second}`.trim();
  }

  async function continueTruncatedReply({ provider, apiKey, modelName, modelMessages, partialContent }) {
    const response = await requestChatCompletion(provider, apiKey, {
      model: modelName,
      temperature: 0.1,
      max_tokens: 160,
      messages: [
        ...modelMessages,
        { role: "assistant", content: partialContent },
        {
          role: "user",
          content: "Continue from exactly where you stopped. Do not restart or repeat. Finish the answer in one short complete reply.",
        },
      ],
    });

    if (response.status !== 200) {
      return partialContent;
    }

    const continuation = normalizeMessageContent(response.data?.choices?.[0]?.message?.content);
    return mergeContinuationText(partialContent, continuation);
  }

  async function summarizeDirectAssistantToolResult({ provider, apiKey, userMessage, toolName, toolResult }) {
    const modelName = getChatModelForProvider(provider);
    const compactResult = toolName === "read_file"
      ? {
          server: toolResult?.server || null,
          path: toolResult?.path || null,
          truncated: !!toolResult?.truncated,
          originalLength: toolResult?.originalLength ?? null,
          content: String(toolResult?.content || "").slice(0, 5000),
        }
      : {
          server: toolResult?.server || null,
          path: toolResult?.path || null,
          entries: Array.isArray(toolResult?.entries) ? toolResult.entries.slice(0, 80) : [],
        };

    const response = await requestChatCompletion(provider, apiKey, {
      model: modelName,
      temperature: 0.1,
      max_tokens: 260,
      messages: [
        {
          role: "system",
          content: [
            "You are ADPanel Assistant.",
            "Always reply in English.",
            "Keep replies short and useful for speech.",
            "Use only the tool result you were given.",
            "If the user asked what to change, point to the most relevant setting or issue briefly.",
            "If the user asked to see a file, summarize the important parts instead of dumping the whole file.",
            "If the tool listed a directory, mention the most relevant entries only.",
          ].join(" "),
        },
        {
          role: "user",
          content: `User request: ${userMessage}\n\nTool used: ${toolName}\nTool result JSON:\n${JSON.stringify(compactResult)}`,
        },
      ],
    });

    if (response.status !== 200) {
      return buildReplyFromToolResult(toolName, toolResult) || "I checked it.";
    }

    const summary = normalizeMessageContent(response.data?.choices?.[0]?.message?.content);
    return summary || buildReplyFromToolResult(toolName, toolResult) || "I checked it.";
  }

  async function completeLightweightAssistantChat({
    provider,
    apiKey,
    userEmail,
    chat,
    trimmedMessage,
    persistedMessages,
  }) {
    const modelName = getChatModelForProvider(provider);
    const { olderContextMessage, recentMessages } = buildAssistantModelConversationWindow(persistedMessages, {
      recentCount: 10,
      olderTranscriptCount: 6,
      olderTranscriptTotalLimit: 900,
    });

    const startedAt = Date.now();
    const response = await requestChatCompletion(provider, apiKey, {
      model: modelName,
      temperature: 0.2,
      max_tokens: 140,
      messages: [
        {
          role: "system",
          content: buildSystemPrompt({ appName: "ADPanel", userEmail, powerOnly: true }),
        },
        ...(olderContextMessage ? [{ role: "system", content: olderContextMessage }] : []),
        ...recentMessages,
      ],
    });
    const thinkingTimeMs = Date.now() - startedAt;

    if (response.status !== 200) {
      const detail = response.data?.error?.message || response.data?.error || response.data?.raw || `status ${response.status}`;
      throw new Error(detail);
    }

    const reply = normalizeMessageContent(response.data?.choices?.[0]?.message?.content) || "How can I help?";

    await addMessage(chat.id, {
      role: "assistant",
      content: reply,
      model: modelName,
      thinkingTimeMs,
    });

    const freshChat = await getChatById(userEmail, chat.id);
    const freshMessages = await getChatMessages(chat.id, VISIBLE_CHAT_HISTORY_LIMIT);
    return {
      chat: freshChat,
      messages: freshMessages,
      reply,
      model: modelName,
      thinkingTimeMs,
      clientActions: [],
    };
  }

  async function runRestrictedDirectToolFlow({
    userEmail,
    chat,
    userIp,
    toolName,
    toolArgs,
    model = "adpanel-restricted-router",
    suppressQueuedReply = false,
  }) {
    const clientActions = [];
    const startedAt = Date.now();
    const toolResult = await runTool({
      name: toolName,
      args: toolArgs,
      userEmail,
      userIp,
      clientActions,
    });
    const thinkingTimeMs = Date.now() - startedAt;
    const browserActionQueued = !!toolResult?.browserActionQueued;
    const reply = suppressQueuedReply && browserActionQueued
      ? ""
      : (
        buildReplyFromToolResult(toolName, toolResult)
        || String(toolResult?.error || toolResult?.detail || "That request failed.").trim()
      );

    if (reply) {
      await addMessage(chat.id, {
        role: "assistant",
        content: reply,
        model,
        thinkingTimeMs,
      });
    }

    const freshChat = await getChatById(userEmail, chat.id);
    const freshMessages = await getChatMessages(chat.id, VISIBLE_CHAT_HISTORY_LIMIT);
    return {
      chat: freshChat,
      messages: freshMessages,
      reply,
      model,
      thinkingTimeMs,
      clientActions,
    };
  }

  async function executeAssistantConversation({ userEmail, chat, trimmedMessage, userIp, persistedMessages }) {
    const provider = getResolvedProvider();
    const apiKey = getProviderKey(provider);
    if (!apiKey) {
      throw new Error(`${getProviderLabel(provider)} is not configured.`);
    }

    const directAccountFlow = detectDirectAccountFlowIntent(trimmedMessage);
    const directBulkPowerFlow = detectDirectBulkPowerIntent(trimmedMessage);
    const directSinglePowerFlow = detectDirectSinglePowerIntent(trimmedMessage);
    const directLoadLookup = detectDirectLoadLookupIntent(trimmedMessage);
    const directOpenSettings = detectDirectOpenSettingsIntent(trimmedMessage, persistedMessages);
    const restrictedFollowUp = looksLikeRestrictedAssistantFollowUp(trimmedMessage, persistedMessages);
    const likelyNeedsServerContext = messageLikelyNeedsAccessibleServerContext(trimmedMessage, persistedMessages);
    const accessibleServers = likelyNeedsServerContext ? await loadAccessibleServersForAssistant(userEmail) : [];
    const inferredServerHint = likelyNeedsServerContext ? inferAssistantServerMention(trimmedMessage, accessibleServers) : null;
    const inferredServerName = String(inferredServerHint?.name || "").trim();
    const inferredServerContextMessage = inferredServerName
      ? `Latest user message most likely refers to accessible server "${inferredServerHint.displayName || inferredServerName}" (internal name: ${inferredServerName}). Use that server unless the user clearly names a different one.`
      : "";

    const directCreateFlow = !POWER_ONLY_ASSISTANT ? detectDirectCreateServerFlowIntent(trimmedMessage) : null;
    if (directCreateFlow && !messageMentionsPluginInstall(trimmedMessage)) {
      const clientActions = [{ type: "open_create_server_modal" }];
      await addMessage(chat.id, {
        role: "assistant",
        content: directCreateFlow.reply,
        model: "adpanel-create-server-router",
      });

      const freshChat = await getChatById(userEmail, chat.id);
      const freshMessages = await getChatMessages(chat.id, VISIBLE_CHAT_HISTORY_LIMIT);
      return {
        chat: freshChat,
        messages: freshMessages,
        reply: directCreateFlow.reply,
        model: "adpanel-create-server-router",
        thinkingTimeMs: 0,
        clientActions,
      };
    }

    const directConsoleCheck = detectDirectConsoleCheckIntent(trimmedMessage, inferredServerName);
    const directServerLimits = detectDirectServerLimitsIntent(trimmedMessage, inferredServerName);
    const directOpenServerPage = detectDirectOpenServerPageIntent(trimmedMessage, inferredServerName, persistedMessages);
    const explicitNavigationRequest =
      messageLooksLikeAssistantOpenRequest(trimmedMessage) ||
      isShortAssistantNavigationShortcut(trimmedMessage);
    const unresolvedNavigationReply = !directOpenServerPage && !directOpenSettings && explicitNavigationRequest
      ? (
          messageExplicitlyNamesAssistantServerSection(trimmedMessage) || /\b(?:server|servers?|page)\b/i.test(trimmedMessage)
            ? "I can't help with that server link right now."
            : (messageExplicitlyNamesAssistantSettingsDestination(trimmedMessage)
              ? "I can't help with that settings link right now."
              : "")
        )
      : "";
    const restrictedCapabilityRequested =
      !!directAccountFlow
      || !!directLoadLookup
      || !!directConsoleCheck
      || !!directServerLimits
      || !!directOpenServerPage
      || !!directOpenSettings
      || !!unresolvedNavigationReply
      || !!restrictedFollowUp
      || !!detectAssistantPowerAction(trimmedMessage)
      || messageLooksLikeConsoleCheckRequest(trimmedMessage)
      || messageLooksLikeServerLimitsCheckRequest(trimmedMessage);

    const restrictedIntentSystemHint = buildRestrictedIntentSystemHint({
      directAccountFlow,
      directBulkPowerFlow,
      directSinglePowerFlow,
      directLoadLookup,
      directConsoleCheck,
      directServerLimits,
      directOpenServerPage,
      directOpenSettings,
    });

    if (POWER_ONLY_ASSISTANT) {
      if (directBulkPowerFlow) {
        return runRestrictedDirectToolFlow({
          userEmail,
          chat,
          userIp,
          toolName: "power_accessible_servers",
          toolArgs: { action: directBulkPowerFlow.action },
          model: directBulkPowerFlow.model || "adpanel-power-router",
        });
      }

      if (directSinglePowerFlow) {
        return runRestrictedDirectToolFlow({
          userEmail,
          chat,
          userIp,
          toolName: "power_server",
          toolArgs: directSinglePowerFlow.args,
          model: directSinglePowerFlow.model || "adpanel-power-router",
        });
      }

      if (directLoadLookup) {
        return runRestrictedDirectToolFlow({
          userEmail,
          chat,
          userIp,
          toolName: directLoadLookup.toolName,
          toolArgs: directLoadLookup.args,
          model: directLoadLookup.model || "adpanel-load-router",
        });
      }

      if (directOpenServerPage) {
        return runRestrictedDirectToolFlow({
          userEmail,
          chat,
          userIp,
          toolName: "open_server_page",
          toolArgs: directOpenServerPage.args,
          model: directOpenServerPage.model || "adpanel-navigation-router",
        });
      }

      if (directOpenSettings) {
        return runRestrictedDirectToolFlow({
          userEmail,
          chat,
          userIp,
          toolName: "open_settings_destination",
          toolArgs: directOpenSettings.args,
          model: directOpenSettings.model || "adpanel-settings-router",
        });
      }

      if (directAccountFlow) {
        return runRestrictedDirectToolFlow({
          userEmail,
          chat,
          userIp,
          toolName: "open_account_flow",
          toolArgs: { flow: directAccountFlow.flow },
          model: directAccountFlow.model || "adpanel-account-router",
        });
      }

      if (unresolvedNavigationReply) {
        await addMessage(chat.id, {
          role: "assistant",
          content: unresolvedNavigationReply,
          model: "adpanel-navigation-guard",
          thinkingTimeMs: 0,
        });

        const freshChat = await getChatById(userEmail, chat.id);
        const freshMessages = await getChatMessages(chat.id, VISIBLE_CHAT_HISTORY_LIMIT);
        return {
          chat: freshChat,
          messages: freshMessages,
          reply: unresolvedNavigationReply,
          model: "adpanel-navigation-guard",
          thinkingTimeMs: 0,
          clientActions: [],
        };
      }

      if (!restrictedCapabilityRequested && !directBulkPowerFlow && !directSinglePowerFlow) {
        const instantSmallTalkReply = buildInstantSmallTalkReply(trimmedMessage);
        if (instantSmallTalkReply) {
          await addMessage(chat.id, {
            role: "assistant",
            content: instantSmallTalkReply,
            model: "adpanel-smalltalk-router",
            thinkingTimeMs: 0,
          });

          const freshChat = await getChatById(userEmail, chat.id);
          const freshMessages = await getChatMessages(chat.id, VISIBLE_CHAT_HISTORY_LIMIT);
          return {
            chat: freshChat,
            messages: freshMessages,
            reply: instantSmallTalkReply,
            model: "adpanel-smalltalk-router",
            thinkingTimeMs: 0,
            clientActions: [],
          };
        }

        return completeLightweightAssistantChat({
          provider,
          apiKey,
          userEmail,
          chat,
          trimmedMessage,
          persistedMessages,
        });
      }

      if (directConsoleCheck) {
        return runRestrictedDirectToolFlow({
          userEmail,
          chat,
          userIp,
          toolName: directConsoleCheck.toolName,
          toolArgs: directConsoleCheck.args,
          model: directConsoleCheck.model || "adpanel-console-router",
          suppressQueuedReply: true,
        });
      }

      if (directServerLimits) {
        return runRestrictedDirectToolFlow({
          userEmail,
          chat,
          userIp,
          toolName: directServerLimits.toolName,
          toolArgs: directServerLimits.args,
          model: directServerLimits.model || "adpanel-limits-router",
          suppressQueuedReply: true,
        });
      }

      const modelName = getChatModelForProvider(provider);
      const conversationContextMessage = "";
      const startedAt = Date.now();
      const response = await buildRestrictedSingleShotPlan({
        provider,
        apiKey,
        modelName,
        systemPrompt: buildSystemPrompt({ appName: "ADPanel", userEmail, powerOnly: true }),
        latestUserMessage: trimmedMessage,
        inferredServerContextMessage,
        restrictedIntentSystemHint,
        conversationContextMessage,
        persistedMessages,
        requestChatCompletion,
      });
      const thinkingTimeMs = Date.now() - startedAt;

      if (response.status !== 200) {
        const detail = response.data?.error?.message || response.data?.error || response.data?.raw || `status ${response.status}`;
        throw new Error(detail);
      }

      const rawContent = normalizeMessageContent(response.data?.choices?.[0]?.message?.content);
      const parsedPlan = parseRestrictedAssistantPlan(rawContent);
      const fallbackPlan = buildRestrictedFallbackPlan({
        trimmedMessage,
        directAccountFlow,
        directBulkPowerFlow,
        directSinglePowerFlow,
        directLoadLookup,
        directConsoleCheck,
        directServerLimits,
        directOpenServerPage,
        directOpenSettings,
      });
      const plannedToolName = normalizeRestrictedToolName(parsedPlan?.toolName);
      const fallbackToolName = normalizeRestrictedToolName(fallbackPlan.toolName);
      const hasExplicitLatestIntent = !!fallbackToolName;
      const selectedToolName = hasExplicitLatestIntent
        ? fallbackToolName
        : (plannedToolName || fallbackToolName);
      const fallbackToolArgs = selectedToolName === fallbackToolName && selectedToolName
        ? sanitizeRestrictedToolArgs(selectedToolName, fallbackPlan.toolArgs)
        : {};
      const selectedToolArgs = selectedToolName && plannedToolName === selectedToolName
        ? sanitizeRestrictedToolArgs(selectedToolName, parsedPlan?.toolArgs)
        : {};
      const mergedToolArgs = hasExplicitLatestIntent
        ? mergeRestrictedToolArgs(selectedToolArgs, fallbackToolArgs)
        : mergeRestrictedToolArgs(fallbackToolArgs, selectedToolArgs);
      const forcedLatestIntentOverride = !!(hasExplicitLatestIntent && plannedToolName && plannedToolName !== fallbackToolName);
      const clientActions = [];
      let finalContent = String(parsedPlan?.reply || "").trim();

      if (selectedToolName) {
        const baseToolArgs = Object.keys(mergedToolArgs).length ? mergedToolArgs : fallbackPlan.toolArgs;
        const toolArgs = applyServerHintToToolArgs(selectedToolName, baseToolArgs, inferredServerName);
        const toolResult = await runTool({
          name: selectedToolName,
          args: toolArgs,
          userEmail,
          userIp,
          clientActions,
        });
        const toolReply = buildReplyFromToolResult(selectedToolName, toolResult)
          || String(toolResult?.error || toolResult?.detail || "").trim();

        if (forcedLatestIntentOverride && toolReply) {
          finalContent = toolReply;
        } else if (!finalContent || toolResult?.ok === false || (toolResult?.browserActionQueued && replyClaimsActionSucceeded(finalContent))) {
          finalContent = toolReply || finalContent;
        }
      } else if (!finalContent) {
        finalContent = rawContent;
      }

      if (!finalContent) {
        finalContent = buildPowerOnlyCapabilityReply();
      }

      await addMessage(chat.id, {
        role: "assistant",
        content: finalContent,
        model: modelName,
        thinkingTimeMs,
      });

      const freshChat = await getChatById(userEmail, chat.id);
      const freshMessages = await getChatMessages(chat.id, VISIBLE_CHAT_HISTORY_LIMIT);
      return {
        chat: freshChat,
        messages: freshMessages,
        reply: finalContent,
        model: modelName,
        thinkingTimeMs,
        clientActions,
      };
    }

    const directDiagnosisFlow = !POWER_ONLY_ASSISTANT ? detectDirectDiagnosisIntent(trimmedMessage, inferredServerName) : null;
    if (directDiagnosisFlow) {
      const clientActions = [];
      const toolResult = await runTool({
        name: directDiagnosisFlow.toolName,
        args: directDiagnosisFlow.args,
        userEmail,
        userIp,
        clientActions,
      });
      const reply = buildReplyFromToolResult(directDiagnosisFlow.toolName, toolResult)
        || String(toolResult?.error || toolResult?.detail || "That diagnosis request failed.").trim();

      await addMessage(chat.id, {
        role: "assistant",
        content: reply,
        model: directDiagnosisFlow.model,
      });

      const freshChat = await getChatById(userEmail, chat.id);
      const freshMessages = await getChatMessages(chat.id, VISIBLE_CHAT_HISTORY_LIMIT);
      return {
        chat: freshChat,
        messages: freshMessages,
        reply,
        model: directDiagnosisFlow.model,
        thinkingTimeMs: 0,
        clientActions,
      };
    }

    const directMinecraftPropertyFlow = !POWER_ONLY_ASSISTANT ? detectDirectMinecraftPropertyIntent(trimmedMessage, inferredServerName) : null;
    if (directMinecraftPropertyFlow) {
      const clientActions = [];
      const toolResult = await runTool({
        name: directMinecraftPropertyFlow.toolName,
        args: directMinecraftPropertyFlow.args,
        userEmail,
        userIp,
        clientActions,
      });
      const reply = buildReplyFromToolResult(directMinecraftPropertyFlow.toolName, toolResult)
        || String(toolResult?.error || toolResult?.detail || "That Minecraft update failed.").trim();

      await addMessage(chat.id, {
        role: "assistant",
        content: reply,
        model: directMinecraftPropertyFlow.model,
      });

      const freshChat = await getChatById(userEmail, chat.id);
      const freshMessages = await getChatMessages(chat.id, VISIBLE_CHAT_HISTORY_LIMIT);
      return {
        chat: freshChat,
        messages: freshMessages,
        reply,
        model: directMinecraftPropertyFlow.model,
        thinkingTimeMs: 0,
        clientActions,
      };
    }

    const directFileInspectFlow = !POWER_ONLY_ASSISTANT ? detectDirectFileInspectIntent(trimmedMessage, inferredServerName) : null;
    if (directFileInspectFlow) {
      const clientActions = [];
      const toolResult = await runTool({
        name: directFileInspectFlow.toolName,
        args: directFileInspectFlow.args,
        userEmail,
        userIp,
        clientActions,
      });

      const reply = toolResult?.ok
        ? await summarizeDirectAssistantToolResult({
            provider,
            apiKey,
            userMessage: trimmedMessage,
            toolName: directFileInspectFlow.toolName,
            toolResult,
          })
        : (buildReplyFromToolResult(directFileInspectFlow.toolName, toolResult)
          || String(toolResult?.error || toolResult?.detail || "That file request failed.").trim());

      await addMessage(chat.id, {
        role: "assistant",
        content: reply,
        model: directFileInspectFlow.model,
      });

      const freshChat = await getChatById(userEmail, chat.id);
      const freshMessages = await getChatMessages(chat.id, VISIBLE_CHAT_HISTORY_LIMIT);
      return {
        chat: freshChat,
        messages: freshMessages,
        reply,
        model: directFileInspectFlow.model,
        thinkingTimeMs: 0,
        clientActions,
      };
    }

    const createConversationLikely = isCreateServerConversation(trimmedMessage, persistedMessages);
    if (createConversationLikely && !messageMentionsPluginInstall(trimmedMessage)) {
      const clientActions = [];
      const provisioningResult = await maybeRunProvisioningFallback({
        userEmail,
        userIp,
        trimmedMessage,
        persistedMessages,
        clientActions,
      });

      if (provisioningResult) {
        await addMessage(chat.id, {
          role: "assistant",
          content: provisioningResult.reply,
          model: provisioningResult.model || "adpanel-provisioning-router",
        });

        const freshChat = await getChatById(userEmail, chat.id);
        const freshMessages = await getChatMessages(chat.id, VISIBLE_CHAT_HISTORY_LIMIT);
        return {
          chat: freshChat,
          messages: freshMessages,
          reply: provisioningResult.reply,
          model: provisioningResult.model || "adpanel-provisioning-router",
          thinkingTimeMs: 0,
          clientActions,
        };
      }
    }

    const provisioningContext = await inferProvisioningContext(persistedMessages);
    const conversationContextMessage = buildConversationContextMessage(provisioningContext);
    const { olderContextMessage, recentMessages } = buildAssistantModelConversationWindow(persistedMessages, {
      recentCount: 18,
      olderTranscriptCount: 10,
      olderTranscriptTotalLimit: 1400,
    });
    const modelMessages = [
      {
        role: "system",
        content: buildSystemPrompt({ appName: "ADPanel", userEmail, powerOnly: POWER_ONLY_ASSISTANT }),
      },
      ...(inferredServerContextMessage ? [{ role: "system", content: inferredServerContextMessage }] : []),
      ...(restrictedIntentSystemHint ? [{ role: "system", content: restrictedIntentSystemHint }] : []),
      ...(conversationContextMessage ? [{ role: "system", content: conversationContextMessage }] : []),
      ...(olderContextMessage ? [{ role: "system", content: olderContextMessage }] : []),
      ...recentMessages,
    ];

    const clientActions = [];
    const modelName = getChatModelForProvider(provider);
    let responseModel = modelName;
    const allowToolUse =
      shouldAllowToolUseForMessage(trimmedMessage, persistedMessages)
      || !!directAccountFlow
      || !!directBulkPowerFlow
      || !!directSinglePowerFlow
      || !!directLoadLookup
      || !!directConsoleCheck
      || !!restrictedCapabilityRequested;
    const needsRealActionExecution = messageNeedsRealActionExecution(trimmedMessage);
    let thinkingTimeMs = 0;
    let finalContent = "";
    let lastToolName = "";
    let lastToolResult = null;
    let forcedToolReply = "";

    for (let attempt = 0; attempt < ASSISTANT_MAX_LOOPS; attempt += 1) {
      const startedAt = Date.now();
      const completionPayload = {
        model: modelName,
        temperature: 0.2,
        max_tokens: needsRealActionExecution ? 420 : 280,
        messages: modelMessages,
      };

      if (allowToolUse) {
        completionPayload.tool_choice = "auto";
        completionPayload.tools = POWER_ONLY_ASSISTANT ? POWER_TOOL_DEFINITIONS : TOOL_DEFINITIONS;
      }

      const response = await requestChatCompletion(provider, apiKey, completionPayload);
      thinkingTimeMs += Date.now() - startedAt;

      if (response.status !== 200) {
        const detail = response.data?.error?.message || response.data?.error || response.data?.raw || `status ${response.status}`;
        throw new Error(detail);
      }

      const choice = response.data?.choices?.[0] || {};
      const message = choice.message;
      const finishReason = String(choice.finish_reason || "").trim().toLowerCase();
      if (!message) {
        throw new Error(`${getProviderLabel(provider)} returned an empty response.`);
      }

      if (Array.isArray(message.tool_calls) && message.tool_calls.length > 0) {
        modelMessages.push({
          role: "assistant",
          content: normalizeMessageContent(message.content),
          tool_calls: message.tool_calls,
        });

        let powerToolUsedThisTurn = false;
        let lastWriteToolResultThisTurn = null;

        for (const call of message.tool_calls) {
          if (
            isGenericServerPropertiesWriteToolCall(call?.function?.name, call?.function?.arguments)
            && messageLooksLikeStructuredMinecraftPropertyMutation(trimmedMessage)
          ) {
            const toolResult = {
              ok: false,
              error: "Use the Minecraft property updater for server.properties key changes.",
            };
            lastToolName = String(call?.function?.name || "").trim();
            lastToolResult = toolResult;
            modelMessages.push({
              role: "tool",
              tool_call_id: call.id,
              content: JSON.stringify(toolResult),
            });
            continue;
          }

          const toolResult = await runTool({
            name: call?.function?.name,
            args: applyServerHintToToolArgs(call?.function?.name, call?.function?.arguments, inferredServerName),
            userEmail,
            userIp,
            clientActions,
          });
          lastToolName = String(call?.function?.name || "").trim();
          lastToolResult = toolResult;
          if (lastToolName === "power_server" || lastToolName === "power_accessible_servers") {
            powerToolUsedThisTurn = true;
          }
          if (lastToolName === "write_file" && toolResult?.ok !== false) {
            lastWriteToolResultThisTurn = toolResult;
          }

          modelMessages.push({
            role: "tool",
            tool_call_id: call.id,
            content: JSON.stringify(toolResult),
          });
        }

        if (!powerToolUsedThisTurn && lastWriteToolResultThisTurn?.server) {
          const chainedPowerAction = extractChainedPowerAction(trimmedMessage);
          if (chainedPowerAction) {
            const powerToolResult = await runTool({
              name: "power_server",
              args: {
                server: lastWriteToolResultThisTurn.server,
                action: chainedPowerAction,
              },
              userEmail,
              userIp,
              clientActions,
            });
            lastToolName = "power_server";
            lastToolResult = powerToolResult;
            forcedToolReply = buildWriteThenPowerReply(lastWriteToolResultThisTurn, powerToolResult);
            break;
          }
        }

        if (forcedToolReply) {
          finalContent = forcedToolReply;
          break;
        }
        continue;
      }

      finalContent = normalizeMessageContent(message.content);
      if (forcedToolReply) {
        finalContent = forcedToolReply;
      }
      if (shouldForceQueuedReply(lastToolName, lastToolResult)) {
        finalContent = buildReplyFromToolResult(lastToolName, lastToolResult) || finalContent;
      }
      if (lastToolResult?.browserActionQueued && replyClaimsActionSucceeded(finalContent)) {
        finalContent = buildReplyFromToolResult(lastToolName, lastToolResult) || finalContent;
      }
      if (isGenericAssistantReply(finalContent) && lastToolResult) {
        finalContent = buildReplyFromToolResult(lastToolName, lastToolResult) || finalContent;
      }
      if (isGenericAssistantReply(finalContent) && allowToolUse && !lastToolResult) {
        finalContent = "I need one more detail before I can do that.";
      }
      if (needsRealActionExecution && !lastToolResult && replyClaimsActionSucceeded(finalContent)) {
        finalContent = "I need one more detail before I can do that.";
      }
      if (!lastToolResult && messageLooksLikeBusiestLookup(trimmedMessage) && !/[?]/.test(finalContent)) {
        finalContent = /\b(node|nodes|nod|noduri)\b/i.test(trimmedMessage)
          ? "I can check that live. Say which node metric you want, or just say busiest node."
          : "I can check that live. Say busiest server, or name a metric like CPU or RAM.";
      }
      if (!lastToolResult && messageLooksLikeDiagnosisRequest(trimmedMessage) && !/[?]/.test(finalContent)) {
        finalContent = "Tell me which server to diagnose.";
      }
      if (!lastToolResult && messageLooksLikeConsoleCheckRequest(trimmedMessage) && !/[?]/.test(finalContent)) {
        finalContent = extractAssistantServerCandidate(trimmedMessage)
          ? "I can check that console now."
          : "Tell me which server console to check.";
      }
      if (!lastToolResult && detectAssistantPowerAction(trimmedMessage) && !detectDirectBulkPowerIntent(trimmedMessage) && !/[?]/.test(finalContent)) {
        finalContent = extractAssistantServerCandidate(trimmedMessage)
          ? "I can run that power action now."
          : "Tell me which server to start, stop, restart, or kill.";
      }
      if (!lastToolResult && messageLooksLikeMinecraftBatchPropertyUpdate(trimmedMessage) && !/[?]/.test(finalContent)) {
        finalContent = "Tell me the exact server.properties key and value to apply.";
      }
      if (!lastToolResult && messageLooksLikeStructuredMinecraftPropertyMutation(trimmedMessage) && !/[?]/.test(finalContent)) {
        finalContent = "Tell me which server to change, or say all accessible Minecraft servers.";
      }
      if (!finalContent) {
        finalContent = buildReplyFromToolResult(lastToolName, lastToolResult) || "Done.";
      }
      if (finishReason === "length" && finalContent) {
        finalContent = await continueTruncatedReply({
          provider,
          apiKey,
          modelName,
          modelMessages,
          partialContent: finalContent,
        });
      }
      break;
    }

    const exploratoryProvisioningTool = ["list_nodes", "list_templates"].includes(lastToolName);
    if (createConversationLikely && isVagueProvisioningReply(finalContent) && (!lastToolResult || exploratoryProvisioningTool)) {
      const fallbackResult = await maybeRunProvisioningFallback({
        userEmail,
        userIp,
        trimmedMessage,
        persistedMessages,
        clientActions,
      });
      if (fallbackResult) {
        finalContent = fallbackResult.reply;
        lastToolName = fallbackResult.toolName;
        lastToolResult = fallbackResult.toolResult;
        responseModel = fallbackResult.model || responseModel;
      }
    }

    if (!finalContent) {
      throw new Error("The assistant reached its tool limit before finishing.");
    }

    await addMessage(chat.id, {
      role: "assistant",
      content: finalContent,
      model: responseModel,
      thinkingTimeMs,
    });

    const freshChat = await getChatById(userEmail, chat.id);
    const freshMessages = await getChatMessages(chat.id, VISIBLE_CHAT_HISTORY_LIMIT);

    return {
      chat: freshChat,
      messages: freshMessages,
      reply: finalContent,
      model: responseModel,
      thinkingTimeMs,
      clientActions,
    };
  }

  async function completeChat({ userEmail, chatId, userMessage, userIp }) {
    const trimmedMessage = String(userMessage || "").trim();
    if (!trimmedMessage) {
      throw new Error("A message is required.");
    }

    const chat = await getActiveChat(userEmail, chatId);
    if (!chat) {
      throw new Error("The assistant chat could not be found.");
    }

    await addMessage(chat.id, { role: "user", content: trimmedMessage });
    await maybeUpdateChatTitle(chat.id, trimmedMessage);
    const persistedMessages = await getChatMessages(chat.id, CHAT_HISTORY_LIMIT);

    return executeAssistantConversation({
      userEmail,
      chat,
      trimmedMessage,
      userIp,
      persistedMessages,
    });
  }

  async function confirmDeleteServer({ userEmail, chatId, serverName, userIp }) {
    const normalizedName = String(serverName || "").trim();
    if (!normalizedName) {
      throw new Error("A server name is required.");
    }

    const currentUser = await deps.findUserByEmail(userEmail);
    if (!(currentUser && currentUser.admin)) {
      throw new Error("Admin required.");
    }

    if (typeof deps.deleteServerByName !== "function") {
      throw new Error("Server deletion is not available here.");
    }

    const result = await deps.deleteServerByName(normalizedName);
    if (!result?.ok) {
      throw new Error(result?.error || "Server deletion failed.");
    }

    const chat = await getActiveChat(userEmail, chatId);
    if (!chat) {
      throw new Error("The assistant chat could not be found.");
    }

    const reply = `Deleted ${normalizedName}.`;
    await addMessage(chat.id, {
      role: "assistant",
      content: reply,
      model: "adpanel-delete-confirm",
    });

    if (typeof deps.recordActivity === "function") {
      try {
        deps.recordActivity(normalizedName, "server_delete", null, userEmail, userIp);
      } catch {
      }
    }

    const freshChat = await getChatById(userEmail, chat.id);
    const freshMessages = await getChatMessages(chat.id, VISIBLE_CHAT_HISTORY_LIMIT);
    return {
      chat: freshChat,
      messages: freshMessages,
      reply,
      model: "adpanel-delete-confirm",
      thinkingTimeMs: 0,
      clientActions: [],
    };
  }

  async function completeClientFollowUp({ userEmail, chatId, summary, sourceType, context, skipAi = false }) {
    const verifiedSummary = String(summary || "").trim();
    if (!verifiedSummary) {
      throw new Error("A follow-up summary is required.");
    }

    const chat = await getActiveChat(userEmail, chatId);
    if (!chat) {
      throw new Error("The assistant chat could not be found.");
    }

    const provider = getResolvedProvider();
    const apiKey = getProviderKey(provider);
    const recentMessages = await getChatMessages(chat.id, VISIBLE_CHAT_HISTORY_LIMIT);
    const lastUserText = getRecentMessagesByRole(recentMessages, "user", 1)[0] || "";
    const recentVisibleTranscript = buildCarryForwardVisibleTranscript(recentMessages, {
      count: 8,
      perMessageLimit: 160,
      totalLimit: 1200,
    });
    const modelName = getChatModelForProvider(provider);
    const compactContext = context && typeof context === "object"
      ? JSON.stringify(context).slice(0, 4000)
      : "";

    let reply = verifiedSummary;
    let thinkingTimeMs = 0;
    let responseModel = skipAi ? "adpanel-client-local" : "adpanel-client-follow-up";

    if (!skipAi && apiKey) {
      const startedAt = Date.now();
      const response = await requestChatCompletion(provider, apiKey, {
        model: modelName,
        temperature: 0.1,
        max_tokens: 220,
        messages: [
          {
            role: "system",
            content: [
              "You are ADPanel Assistant inside the ADPanel dashboard.",
              "Always reply in English.",
              "Keep replies very short, natural, and good for speech.",
              "The browser already executed the action and produced verified result data.",
              "Answer using only that verified result and the visible conversation context.",
              "Do not mention the browser, tools, routing, JSON, or internal mechanics.",
              "If the verified result already answers the user, say it directly.",
              "If the action type is console_check, interpret the logs and status directly for the user.",
              "If the action type is server_limits_check, explain the live RAM, CPU, and storage headroom directly for the user.",
              "When RAM, CPU, or storage looks dangerously high, say that clearly and suggest checking the console next.",
              "Prefer the raw log lines and verified status over any prewritten summary when they are available.",
              "If startup timing is visible in the verified result, state the seconds clearly.",
              "If the user asks whether the server started correctly or is healthy, answer that directly from the logs and status.",
              "Do not just repeat the verified summary if the log lines let you answer more precisely.",
            ].join(" "),
          },
          {
            role: "user",
            content: [
              recentVisibleTranscript ? `Recent visible chat:\n${recentVisibleTranscript}` : "",
              lastUserText ? `Last user request: ${lastUserText}` : "",
              sourceType ? `Action type: ${String(sourceType).trim()}` : "",
              `Verified result summary: ${verifiedSummary}`,
              compactContext ? `Extra verified context JSON: ${compactContext}` : "",
            ].filter(Boolean).join("\n\n"),
          },
        ],
      });
      thinkingTimeMs = Date.now() - startedAt;
      responseModel = modelName;

      if (response.status === 200) {
        const aiReply = normalizeMessageContent(response.data?.choices?.[0]?.message?.content);
        if (aiReply) {
          reply = aiReply;
        }
      }
    }

    await addMessage(chat.id, {
      role: "assistant",
      content: reply,
      model: responseModel,
      thinkingTimeMs,
    });

    const freshChat = await getChatById(userEmail, chat.id);
    const freshMessages = await getChatMessages(chat.id, VISIBLE_CHAT_HISTORY_LIMIT);
    return {
      chat: freshChat,
      messages: freshMessages,
      reply,
      model: responseModel,
      thinkingTimeMs,
      clientActions: [],
    };
  }

  async function getBootstrap(req) {
    const userEmail = String(req.session?.user || "").trim().toLowerCase();
    const currentUser = await deps.findUserByEmail(userEmail);
    const chat = await getActiveChat(userEmail);
    const messages = await getChatMessages(chat.id, VISIBLE_CHAT_HISTORY_LIMIT);
    const activeProvider = getResolvedProvider();
    const activeKey = getProviderKey(activeProvider);
    const canConfigure = !!(currentUser && currentUser.admin);
    const assistantConfig = typeof deps.getDashboardAssistantConfig === "function"
      ? deps.getDashboardAssistantConfig()
      : { allowNormalUsers: false };

    const actionTokens = {};
    if (canConfigure && typeof deps.issueActionToken === "function") {
      actionTokens.saveToken = deps.issueActionToken(req, "POST /api/dashboard-assistant/token", {}, { ttlSeconds: 300 });
      actionTokens.updateAccess = deps.issueActionToken(req, "POST /api/dashboard-assistant/access", {}, { ttlSeconds: 300 });
    }

    const providers = Object.keys(PROVIDER_LABELS).map((providerId) => {
      const key = getProviderKey(providerId);
      return {
        id: providerId,
        label: getProviderLabel(providerId),
        configured: !!key,
        obscuredValue: canConfigure ? buildStoredKeyFieldValue(key) : "",
      };
    });

    return {
      configured: !!activeKey,
      provider: activeProvider,
      providerLabel: getProviderLabel(activeProvider),
      providers,
      canConfigure,
      allowNormalUsers: !!assistantConfig.allowNormalUsers,
      chat,
      messages,
      actionTokens,
      user: currentUser ? {
        email: currentUser.email,
        admin: !!currentUser.admin,
      } : null,
    };
  }

  return {
    ensureTables,
    getBootstrap,
    createFreshChat,
    getPrimaryChat,
    saveProviderToken,
    transcribeAudio,
    completeChat,
    completeClientFollowUp,
    confirmDeleteServer,
  };
}

module.exports = {
  createDashboardAssistantService,
  DEFAULT_CHAT_TITLE,
};
