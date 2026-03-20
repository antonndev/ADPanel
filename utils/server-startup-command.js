"use strict";

function cleanString(value) {
  return String(value || "").trim();
}

function toPositiveNumber(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) && numeric > 0 ? numeric : 0;
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

module.exports = {
  buildDefaultStartupCommand,
  resolveStartupCommandForCreate,
  extractStartupCommandFromNodeCreateResult,
};
