"use strict";

const https = require("https");
const FormData = require("form-data");
const {
  TOOL_DEFINITIONS,
  buildSystemPrompt,
  createDashboardAssistantToolRunner,
} = require("./dashboard-assistant-tools");

const DEFAULT_CHAT_TITLE = "ADPanel Assistant";
const DEFAULT_GROQ_MODEL = process.env.DASHBOARD_ASSISTANT_GROQ_MODEL || "llama-3.3-70b-versatile";
const DEFAULT_GROQ_TRANSCRIPTION_MODEL = process.env.DASHBOARD_ASSISTANT_GROQ_STT_MODEL || "whisper-large-v3";
const DEFAULT_GOOGLE_MODEL = process.env.DASHBOARD_ASSISTANT_GOOGLE_MODEL || "gemini-2.5-flash";
const DEFAULT_GOOGLE_TRANSCRIPTION_MODEL = process.env.DASHBOARD_ASSISTANT_GOOGLE_STT_MODEL || "gemini-2.5-flash";
const CHAT_HISTORY_LIMIT = 24;
const MAX_CHAT_MESSAGES = 20;
const ASSISTANT_MAX_LOOPS = 6;
const AUDIO_UPLOAD_LIMIT_BYTES = parseInt(process.env.DASHBOARD_ASSISTANT_AUDIO_LIMIT_BYTES || "", 10) || 15 * 1024 * 1024;
const GOOGLE_OPENAI_CHAT_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions";
const GOOGLE_INTERACTIONS_URL = "https://generativelanguage.googleapis.com/v1beta/interactions";
const PROVIDER_LABELS = Object.freeze({
  groq: "Groq",
  google: "Google AI Studio",
});
const SUPPORTED_PROVIDERS = new Set(Object.keys(PROVIDER_LABELS));

function maskKey(value) {
  const clean = String(value || "").trim();
  if (!clean) return "";
  if (clean.length <= 8) return "****";
  return `${clean.slice(0, 4)}...${clean.slice(-4)}`;
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
    /\b(start|stop|restart|kill|power)\b/.test(text) &&
    /\b(server|servers)\b/.test(text);
  if (serverPowerIntent) return true;

  const fileMutationIntent =
    /\b(edit|write|replace|overwrite|append|create|delete|remove|rename|move|mkdir|make)\b/.test(text) &&
    /\b(file|files|folder|directory|config|properties|server\.properties|eula|plugin|plugins|world|mods?)\b/.test(text);
  if (fileMutationIntent) return true;

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
    /\b(start|stop|restart|kill|power|edit|read|write|create|delete|remove|rename|move|open|inspect|list|show)\b/.test(text) &&
    /\b(server|servers|file|files|folder|directory|config|properties|server\.properties|eula|plugin|plugins|world|mods?)\b/.test(text);
  if (fileOrServerAction) return true;

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
      const server = result.server || "the server";
      const action = result.action || "updated";
      if (action === "start") return `Started ${server}.`;
      if (action === "stop") return `Stopped ${server}.`;
      if (action === "restart") return `Restarted ${server}.`;
      if (action === "kill") return `Killed ${server}.`;
      return `Updated ${server}.`;
    }
    case "write_file":
      return result.path ? `Updated ${result.path}.` : "File updated.";
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

function normalizeLooseText(value) {
  return String(value || "").toLowerCase().replace(/\s+/g, " ").trim();
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

function jsonApiRequest(fullUrl, { method = "POST", headers = {}, body = null, timeoutMs = 60_000, timeoutMessage = "Request timeout" } = {}) {
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

  function getGoogleKey() {
    const env = deps.readEnvFile();
    return sanitizeToken(
      env.GOOGLE_AI_STUDIO_API_KEY ||
      env.GOOGLE_AI_KEY ||
      process.env.GOOGLE_AI_STUDIO_API_KEY ||
      process.env.GOOGLE_AI_KEY ||
      ""
    );
  }

  function getGroqKey() {
    const env = deps.readEnvFile();
    return sanitizeToken(env.GROQ_API_KEY || process.env.GROQ_API_KEY || "");
  }

  function getStoredProviderPreference() {
    const env = deps.readEnvFile();
    return sanitizeProvider(
      env.DASHBOARD_ASSISTANT_PROVIDER ||
      process.env.DASHBOARD_ASSISTANT_PROVIDER ||
      ""
    );
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
       LIMIT ?`,
      [chatId, safeLimit]
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
      .slice(-12);
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

    if (!parts.length) {
      const recentUserTexts = getRecentMessagesByRole(recentMessages, "user", 3);
      if (recentUserTexts.length) {
        parts.push(`Carry-forward recent user intent: ${recentUserTexts.join(" | ").slice(0, 360)}`);
      }
    }

    return parts.join(" ").trim().slice(0, 700);
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
    const existingKey = getProviderKey(resolvedProvider);
    const sanitized = sanitizeToken(token);

    if (!sanitized && !existingKey) {
      throw new Error(`Please enter a valid ${providerLabel} API key.`);
    }

    const updates = { DASHBOARD_ASSISTANT_PROVIDER: resolvedProvider };
    if (sanitized) {
      if (resolvedProvider === "google") {
        updates.GOOGLE_AI_STUDIO_API_KEY = sanitized;
        updates.GOOGLE_AI_KEY = sanitized;
      } else {
        updates.GROQ_API_KEY = sanitized;
      }
    }

    const saved = deps.writeEnvFileBatch(updates);
    if (!saved) {
      throw new Error(`Failed to save the ${providerLabel} API key.`);
    }

    process.env.DASHBOARD_ASSISTANT_PROVIDER = resolvedProvider;
    if (sanitized) {
      if (resolvedProvider === "google") {
        process.env.GOOGLE_AI_STUDIO_API_KEY = sanitized;
        process.env.GOOGLE_AI_KEY = sanitized;
      } else {
        process.env.GROQ_API_KEY = sanitized;
      }
    }

    const effectiveKey = sanitized || existingKey;
    return {
      provider: resolvedProvider,
      label: providerLabel,
      configured: !!effectiveKey,
      maskedKey: maskKey(effectiveKey),
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

  async function executeAssistantConversation({ userEmail, chat, trimmedMessage, userIp, persistedMessages }) {
    const provider = getResolvedProvider();
    const apiKey = getProviderKey(provider);
    if (!apiKey) {
      throw new Error(`${getProviderLabel(provider)} is not configured.`);
    }

    const directAccountFlow = detectDirectAccountFlowIntent(trimmedMessage);
    if (directAccountFlow) {
      const clientActions = [{ type: "open_account_flow", flow: directAccountFlow.flow }];
      await addMessage(chat.id, {
        role: "assistant",
        content: directAccountFlow.reply,
        model: "adpanel-account-router",
      });

      const freshChat = await getChatById(userEmail, chat.id);
      const freshMessages = await getChatMessages(chat.id, 80);
      return {
        chat: freshChat,
        messages: freshMessages,
        reply: directAccountFlow.reply,
        model: "adpanel-account-router",
        thinkingTimeMs: 0,
        clientActions,
      };
    }

    const directCreateFlow = detectDirectCreateServerFlowIntent(trimmedMessage);
    if (directCreateFlow && !messageMentionsPluginInstall(trimmedMessage)) {
      const clientActions = [{ type: "open_create_server_modal" }];
      await addMessage(chat.id, {
        role: "assistant",
        content: directCreateFlow.reply,
        model: "adpanel-create-server-router",
      });

      const freshChat = await getChatById(userEmail, chat.id);
      const freshMessages = await getChatMessages(chat.id, 80);
      return {
        chat: freshChat,
        messages: freshMessages,
        reply: directCreateFlow.reply,
        model: "adpanel-create-server-router",
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
        const freshMessages = await getChatMessages(chat.id, 80);
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
    const modelMessages = [
      {
        role: "system",
        content: buildSystemPrompt({ appName: "ADPanel", userEmail }),
      },
      ...(conversationContextMessage ? [{ role: "system", content: conversationContextMessage }] : []),
      ...persistedMessages.map((message) => ({
        role: message.role,
        content: message.content,
      })),
    ];

    const clientActions = [];
    const modelName = getChatModelForProvider(provider);
    let responseModel = modelName;
    const allowToolUse = shouldAllowToolUseForMessage(trimmedMessage, persistedMessages);
    const needsRealActionExecution = messageNeedsRealActionExecution(trimmedMessage);
    let thinkingTimeMs = 0;
    let finalContent = "";
    let lastToolName = "";
    let lastToolResult = null;

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
        completionPayload.tools = TOOL_DEFINITIONS;
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

        for (const call of message.tool_calls) {
          const toolResult = await runTool({
            name: call?.function?.name,
            args: call?.function?.arguments,
            userEmail,
            userIp,
            clientActions,
          });
          lastToolName = String(call?.function?.name || "").trim();
          lastToolResult = toolResult;

          modelMessages.push({
            role: "tool",
            tool_call_id: call.id,
            content: JSON.stringify(toolResult),
          });
        }
        continue;
      }

      finalContent = normalizeMessageContent(message.content);
      if (isGenericAssistantReply(finalContent) && lastToolResult) {
        finalContent = buildReplyFromToolResult(lastToolName, lastToolResult) || finalContent;
      }
      if (isGenericAssistantReply(finalContent) && allowToolUse && !lastToolResult) {
        finalContent = "I need one more detail before I can do that.";
      }
      if (needsRealActionExecution && !lastToolResult && replyClaimsActionSucceeded(finalContent)) {
        finalContent = "I need one more detail before I can do that.";
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
    const freshMessages = await getChatMessages(chat.id, 80);

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
    const freshMessages = await getChatMessages(chat.id, 80);
    return {
      chat: freshChat,
      messages: freshMessages,
      reply,
      model: "adpanel-delete-confirm",
      thinkingTimeMs: 0,
      clientActions: [],
    };
  }

  async function getBootstrap(req) {
    const userEmail = String(req.session?.user || "").trim().toLowerCase();
    const currentUser = await deps.findUserByEmail(userEmail);
    const chat = await getActiveChat(userEmail);
    const messages = await getChatMessages(chat.id, 80);
    const activeProvider = getResolvedProvider();
    const activeKey = getProviderKey(activeProvider);
    const canConfigure = !!(currentUser && currentUser.admin);

    const actionTokens = {};
    if (canConfigure && typeof deps.issueActionToken === "function") {
      actionTokens.saveToken = deps.issueActionToken(req, "POST /api/dashboard-assistant/token", {}, { ttlSeconds: 300 });
    }

    const providers = Object.keys(PROVIDER_LABELS).map((providerId) => {
      const key = getProviderKey(providerId);
      return {
        id: providerId,
        label: getProviderLabel(providerId),
        configured: !!key,
        maskedKey: canConfigure ? maskKey(key) : "",
      };
    });

    return {
      configured: !!activeKey,
      provider: activeProvider,
      providerLabel: getProviderLabel(activeProvider),
      providers,
      maskedKey: canConfigure ? maskKey(activeKey) : "",
      canConfigure,
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
    confirmDeleteServer,
  };
}

module.exports = {
  createDashboardAssistantService,
  DEFAULT_CHAT_TITLE,
};
