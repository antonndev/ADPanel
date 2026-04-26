"use strict";

const { sanitizeServerToken } = require("./server-name");

function cleanString(value) {
  return String(value || "").trim();
}

function toPositiveNumber(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) && numeric > 0 ? numeric : 0;
}

function parseStrictPort(value) {
  if (value === undefined || value === null) return null;
  if (typeof value === "number") {
    if (!Number.isInteger(value) || value < 1 || value > 65535) return null;
    return value;
  }
  const str = cleanString(value);
  if (!/^[1-9]\d{0,4}$/.test(str)) return null;
  const numeric = Number(str);
  if (!Number.isInteger(numeric) || numeric < 1 || numeric > 65535) return null;
  return numeric;
}

function parseShellArgs(input) {
  const src = cleanString(input);
  const args = [];
  let current = "";
  let quote = "";
  let escapeNext = false;

  for (let i = 0; i < src.length; i++) {
    const ch = src[i];
    if (escapeNext) {
      current += ch;
      escapeNext = false;
      continue;
    }
    if (ch === "\\") {
      escapeNext = true;
      continue;
    }
    if (quote) {
      if (ch === quote) {
        quote = "";
      } else {
        current += ch;
      }
      continue;
    }
    if (ch === '"' || ch === "'") {
      quote = ch;
      continue;
    }
    if (ch === " " || ch === "\t" || ch === "\n" || ch === "\r") {
      if (current) {
        args.push(current);
        current = "";
      }
      continue;
    }
    current += ch;
  }

  if (current) args.push(current);
  return args;
}

function extractHostPortFromMapping(mapping) {
  let value = cleanString(mapping);
  const slashIdx = value.indexOf("/");
  if (slashIdx >= 0) value = value.slice(0, slashIdx);
  const parts = value.split(":");
  if (parts.length === 2) return parseStrictPort(parts[0]) || 0;
  if (parts.length === 3) return parseStrictPort(parts[1]) || 0;
  return 0;
}

function normalizeDockerImage(raw) {
  let image = cleanString(raw);
  if (!image) return "";
  const digestIdx = image.indexOf("@");
  if (digestIdx > 0) image = image.slice(0, digestIdx);
  const tagIdx = image.lastIndexOf(":");
  if (tagIdx > 0) {
    const afterColon = image.slice(tagIdx + 1);
    if (!afterColon.includes("/")) image = image.slice(0, tagIdx);
  }
  image = image.toLowerCase().replace(/^docker\.io\//, "").replace(/^index\.docker\.io\//, "");
  if (!image.includes("/")) image = `library/${image}`;
  return image;
}

const DOCKER_RUN_FLAGS_WITH_VALUE = new Set([
  "-e", "--env", "-v", "--volume", "-p", "--publish", "-w", "--workdir",
  "--name", "-m", "--memory", "--cpus", "--memory-swap", "--memory-reservation", "--cpu-period", "--cpu-quota",
  "-u", "--user", "-h", "--hostname", "--network", "--net", "--restart",
  "-l", "--label", "--entrypoint", "--mount", "--tmpfs", "--device",
  "--pid", "--ipc", "--userns", "--uts", "--cgroupns", "--shm-size",
  "--ulimit", "--dns", "--dns-search", "--add-host", "--log-driver", "--log-opt",
  "--stop-signal", "--stop-timeout", "--health-cmd", "--health-interval",
  "--health-retries", "--health-timeout", "--runtime", "--platform", "--pull",
  "--ip", "--ip6", "--mac-address", "--expose", "--link", "--cidfile",
]);

const DOCKER_RUN_SAFE_STANDALONE_FLAGS = new Set([
  "-d", "--detach", "-i", "--interactive", "-t", "--tty", "--rm", "--init", "--read-only",
]);

const DOCKER_RUN_BLOCKED_FLAGS = new Set([
  "--privileged",
  "--cap-add",
  "--cap-drop",
  "--security-opt",
  "--volumes-from",
  "--env-file",
  "--device",
  "--device-cgroup-rule",
  "--sysctl",
  "--oom-kill-disable",
  "--oom-score-adj",
]);

const DOCKER_RUN_HOST_NAMESPACE_FLAGS = new Set([
  "--network", "--net", "--pid", "--userns", "--ipc", "--uts", "--cgroupns",
]);

function isSafeShortDockerFlagCluster(flag) {
  return /^-[dit]+$/i.test(flag);
}

function readDockerFlagValue(args, index, inlineValue) {
  if (inlineValue) return { value: inlineValue, nextIndex: index };
  if (index + 1 >= args.length) return { value: "", nextIndex: index };
  return { value: args[index + 1], nextIndex: index + 1 };
}

function extractDockerImageFromCommand(cmdStr) {
  const cmd = cleanString(cmdStr);
  if (!/^docker run(?:\s|$)/i.test(cmd)) return "";
  const args = parseShellArgs(cmd.slice("docker run".length).trim());
  for (let i = 0; i < args.length; i++) {
    const token = cleanString(args[i]);
    if (!token) continue;
    const tokenLower = token.toLowerCase();
    if (!tokenLower.startsWith("-")) return token;

    let flagName = tokenLower;
    let inlineValue = "";
    if (tokenLower.startsWith("--") && tokenLower.includes("=")) {
      const cutIdx = tokenLower.indexOf("=");
      flagName = tokenLower.slice(0, cutIdx);
      inlineValue = token.slice(cutIdx + 1);
    }

    if (isSafeShortDockerFlagCluster(flagName) || DOCKER_RUN_SAFE_STANDALONE_FLAGS.has(flagName)) {
      continue;
    }
    if (DOCKER_RUN_FLAGS_WITH_VALUE.has(flagName)) {
      const valueInfo = readDockerFlagValue(args, i, inlineValue);
      i = valueInfo.nextIndex;
    }
  }
  return "";
}

function validateDockerRunCommandSafety(command, options = {}) {
  const cmd = cleanString(command);
  if (!cmd) return null;
  if (!/^docker run(?:\s|$)/i.test(cmd)) {
    return "Startup command must start with 'docker run'.";
  }
  if (/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/.test(cmd)) {
    return "Startup command contains unsupported control characters.";
  }

  const allowedHostPorts = new Set(
    (Array.isArray(options.allowedHostPorts) ? options.allowedHostPorts : [])
      .map((entry) => parseStrictPort(entry))
      .filter((entry) => entry != null)
  );
  const reservedHostPorts = new Set(
    (Array.isArray(options.reservedHostPorts) ? options.reservedHostPorts : [])
      .map((entry) => parseStrictPort(entry))
      .filter((entry) => entry != null)
  );
  const expectedImageRef = cleanString(options.expectedImageRef);
  const args = parseShellArgs(cmd.slice("docker run".length).trim());

  for (let i = 0; i < args.length; i++) {
    const token = cleanString(args[i]);
    if (!token) continue;
    const tokenLower = token.toLowerCase();

    if (!tokenLower.startsWith("-")) {
      if (expectedImageRef) {
        const normalizedExpected = normalizeDockerImage(expectedImageRef);
        const normalizedCommand = normalizeDockerImage(token);
        if (normalizedExpected && normalizedCommand && normalizedExpected !== normalizedCommand) {
          return `The Docker image in the startup command (${token}) does not match the configured image (${expectedImageRef}).`;
        }
      }
      return null;
    }

    let flagName = tokenLower;
    let inlineValue = "";
    if (tokenLower.startsWith("--") && tokenLower.includes("=")) {
      const cutIdx = tokenLower.indexOf("=");
      flagName = tokenLower.slice(0, cutIdx);
      inlineValue = token.slice(cutIdx + 1);
    }

    if (DOCKER_RUN_BLOCKED_FLAGS.has(flagName)) {
      return `Blocked dangerous Docker flag: ${flagName}`;
    }

    if (flagName === "--mount") {
      const valueInfo = readDockerFlagValue(args, i, inlineValue);
      const mountValue = cleanString(valueInfo.value).toLowerCase();
      if (!mountValue) return "Docker flag --mount requires a value.";
      if (mountValue.includes("type=bind")) {
        return "Blocked dangerous Docker flag: --mount with bind";
      }
      i = valueInfo.nextIndex;
      continue;
    }

    if (flagName === "--tmpfs") {
      const valueInfo = readDockerFlagValue(args, i, inlineValue);
      const tmpfsValue = cleanString(valueInfo.value).toLowerCase();
      if (!tmpfsValue) return "Docker flag --tmpfs requires a value.";
      if (tmpfsValue.includes("exec")) {
        return "Blocked dangerous Docker flag: --tmpfs with exec";
      }
      i = valueInfo.nextIndex;
      continue;
    }

    if (DOCKER_RUN_HOST_NAMESPACE_FLAGS.has(flagName)) {
      const valueInfo = readDockerFlagValue(args, i, inlineValue);
      const nsMode = cleanString(valueInfo.value).toLowerCase();
      if (!nsMode) return `Docker flag ${flagName} requires a value.`;
      if (nsMode === "host") {
        return `Blocked dangerous Docker flag: ${flagName}=host`;
      }
      i = valueInfo.nextIndex;
      continue;
    }

    if (flagName === "-p" || flagName === "--publish" || flagName === "-p=" || flagName === "--publish=") {
      const valueInfo = readDockerFlagValue(args, i, inlineValue);
      const mapping = cleanString(valueInfo.value);
      if (!mapping) return "Docker publish flag is missing a port mapping.";
      if (!mapping.includes("{PORT}")) {
        const hostPort = extractHostPortFromMapping(mapping);
        if (!hostPort) return `Invalid Docker port mapping: ${mapping}`;
        if (!allowedHostPorts.has(hostPort)) {
          return 'Port config through docker edit command is not allowed. Add or remove a port through Port Management.';
        }
        if (reservedHostPorts.has(hostPort)) {
          return `Port ${hostPort} in the Docker command conflicts with a port forwarding rule.`;
        }
      }
      i = valueInfo.nextIndex;
      continue;
    }

    if (isSafeShortDockerFlagCluster(flagName) || DOCKER_RUN_SAFE_STANDALONE_FLAGS.has(flagName)) {
      continue;
    }

    if (DOCKER_RUN_FLAGS_WITH_VALUE.has(flagName)) {
      const valueInfo = readDockerFlagValue(args, i, inlineValue);
      if (!cleanString(valueInfo.value)) {
        return `Docker flag ${flagName} requires a value.`;
      }
      i = valueInfo.nextIndex;
      continue;
    }

    return `Unsupported Docker flag in startup command: ${flagName}`;
  }

  return "Startup command must include a Docker image.";
}

function getDockerImageRef(docker) {
  const image = cleanString(docker?.image);
  if (!image) return "";
  const tag = cleanString(docker?.tag) || "latest";
  return `${image}:${tag}`;
}

function resolveTemplateDockerConfig(source) {
  if (!source || typeof source !== "object") return null;

  if (source.docker && typeof source.docker === "object") {
    return source.docker;
  }

  const image = cleanString(source.image || source.dockerImage);
  const tag = cleanString(source.tag || source.dockerTag);
  const command = cleanString(source.command);
  const startupCommand = cleanString(source.startupCommand);
  const restart = cleanString(source.restart || source.restartPolicy);
  const ports = Array.isArray(source.ports) ? source.ports : [];
  const volumes = Array.isArray(source.volumes) ? source.volumes : [];
  const env = source.env && typeof source.env === "object" && !Array.isArray(source.env) ? source.env : {};

  if (!image && !tag && !command && !startupCommand && !restart && !ports.length && !volumes.length && !Object.keys(env).length) {
    return null;
  }

  return {
    image,
    tag,
    command,
    startupCommand,
    restart,
    ports,
    volumes,
    env,
  };
}

function normalizeStartupCommandForCreate(command, docker) {
  const cleanCommand = cleanString(command);
  if (!cleanCommand) return "";
  const imageRef = getDockerImageRef(resolveTemplateDockerConfig(docker) || docker);
  if (!imageRef) return cleanCommand;
  return cleanCommand.replace(/\{IMAGE\}/gi, imageRef).trim();
}

function getDockerVolumes(docker, templateId) {
  const configuredVolumes = Array.isArray(docker?.volumes)
    ? docker.volumes.map((entry) => cleanString(entry)).filter(Boolean)
    : [];
  if (configuredVolumes.length) {
    return configuredVolumes;
  }

  const normalizedTemplateId = cleanString(templateId).toLowerCase();
  if (["nodejs", "discord-bot", "python", "runtime"].includes(normalizedTemplateId)) {
    return ["{DATA_DIR}:/app"];
  }
  return ["{DATA_DIR}:/data"];
}

function getRuntimeCommand(templateId, docker, startFile) {
  const explicit = cleanString(docker?.command);
  if (explicit) return explicit;

  const normalizedTemplateId = cleanString(templateId).toLowerCase();
  const resolvedStartFile = cleanString(startFile);
  if (normalizedTemplateId === "python") {
    return `python /app/${resolvedStartFile || "main.py"}`;
  }
  if (normalizedTemplateId === "nodejs" || normalizedTemplateId === "discord-bot") {
    return `cd /app && npm install && node /app/${resolvedStartFile || "index.js"}`;
  }
  return "";
}

function getContainerPort(templateId, docker, hostPort) {
  const normalizedTemplateId = cleanString(templateId).toLowerCase();
  if (["nodejs", "discord-bot", "python", "runtime"].includes(normalizedTemplateId)) {
    return toPositiveNumber(hostPort);
  }

  const ports = Array.isArray(docker?.ports) ? docker.ports : [];
  for (const entry of ports) {
    const port = toPositiveNumber(entry);
    if (port > 0) return port;
  }
  return toPositiveNumber(hostPort);
}

function buildResourceArgs(resources) {
  const args = [];
  const ramMb = toPositiveNumber(resources?.ramMb);
  const cpuCores = toPositiveNumber(resources?.cpuCores);

  if (ramMb > 0) {
    args.push("--memory", `${Math.floor(ramMb)}m`);
    args.push("--memory-reservation", `${Math.max(1, Math.floor((ramMb * 90) / 100))}m`);
  }

  if (cpuCores > 0) {
    args.push("--cpus", String(cpuCores));
  }

  return args;
}

function buildDefaultStartupCommand({ name, templateId, docker, hostPort, resources, startFile }) {
  const normalizedTemplateId = cleanString(templateId).toLowerCase();
  if (!docker || typeof docker !== "object") return "";
  if (normalizedTemplateId === "minecraft") return "";

  const imageRef = getDockerImageRef(docker);
  if (!imageRef) return "";

  const serverName = cleanString(name) || "{SERVER_NAME}";
  const externalPort = toPositiveNumber(hostPort);
  const containerPort = getContainerPort(normalizedTemplateId, docker, hostPort);
  const parts = ["docker run -d -t", "--name", serverName, "--restart", cleanString(docker?.restart) || "unless-stopped"];

  parts.push(...buildResourceArgs(resources));

  if (externalPort > 0 && containerPort > 0) {
    parts.push("-p", `${externalPort}:${containerPort}`);
  }

  for (const volume of getDockerVolumes(docker, normalizedTemplateId)) {
    parts.push("-v", volume);
  }

  const env = docker?.env && typeof docker.env === "object" ? docker.env : {};
  for (const [key, value] of Object.entries(env)) {
    const envKey = cleanString(key);
    if (!envKey) continue;
    parts.push("-e", `${envKey}=${String(value ?? "")}`);
  }
  if (externalPort > 0) {
    parts.push("-e", `PORT=${externalPort}`);
  }

  parts.push(imageRef);

  const runtimeCommand = getRuntimeCommand(normalizedTemplateId, docker, startFile);
  if (runtimeCommand) {
    parts.push("sh", "-lc", JSON.stringify(runtimeCommand));
  }

  return parts.join(" ").trim();
}

function resolveStartupCommandForCreate({ requestedStartupCommand, template, templateId, name, hostPort, resources, startFile }) {
  const templateDocker = resolveTemplateDockerConfig(template);
  const requested = normalizeStartupCommandForCreate(requestedStartupCommand, templateDocker);
  if (requested) return requested;

  const templateStartupCommand = normalizeStartupCommandForCreate(templateDocker?.startupCommand, templateDocker);
  if (templateStartupCommand) return templateStartupCommand;

  return buildDefaultStartupCommand({
    name,
    templateId: templateId || template?.id || "",
    docker: templateDocker,
    hostPort,
    resources,
    startFile,
  });
}

function extractStartupCommandFromNodeCreateResult(result) {
  if (!result || typeof result !== "object") return "";

  const direct = cleanString(result.startupCommand);
  if (direct) return direct;

  const metaStartup = cleanString(result.meta?.startupCommand);
  if (metaStartup) return metaStartup;

  const runtimeStartup = cleanString(result.meta?.runtime?.startupCommand);
  if (runtimeStartup) return runtimeStartup;

  return "";
}

function extractTechnicalServerNameFromNodeCreateResult(result, fallback = "") {
  const candidates = [
    result?.technicalName,
    result?.serverTechnicalName,
    result?.server?.technicalName,
    result?.meta?.technicalName,
    result?.meta?.serverTechnicalName,
    result?.meta?.server?.technicalName,
    result?.serverName,
    result?.server?.name,
    result?.name,
    result?.meta?.serverName,
    result?.meta?.server?.name,
    result?.meta?.name,
    result?.created?.technicalName,
    result?.created?.serverName,
    result?.created?.name,
    fallback,
  ];

  for (const candidate of candidates) {
    const sanitized = sanitizeServerToken(candidate);
    if (sanitized) return sanitized;
  }
  return "";
}

module.exports = {
  buildDefaultStartupCommand,
  resolveStartupCommandForCreate,
  extractStartupCommandFromNodeCreateResult,
  extractTechnicalServerNameFromNodeCreateResult,
  validateDockerRunCommandSafety,
};
