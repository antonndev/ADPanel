"use strict";

function cleanString(value) {
  return String(value ?? "").trim();
}

function toFiniteNumber(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function sanitizeDockerTemplatePayload(docker) {
  if (!docker || typeof docker !== "object") return null;

  const image = cleanString(docker.image);
  if (!image) return null;

  const payload = {
    image,
  };

  const tag = cleanString(docker.tag) || "latest";
  if (tag) payload.tag = tag;

  const command = cleanString(docker.command);
  if (command) payload.command = command;

  const restart = cleanString(docker.restart);
  if (restart) payload.restart = restart;

  const restartPolicy = cleanString(docker.restartPolicy);
  if (restartPolicy) payload.restartPolicy = restartPolicy;

  const startupCommand = cleanString(docker.startupCommand);
  if (startupCommand) payload.startupCommand = startupCommand;

  const ports = Array.isArray(docker.ports)
    ? docker.ports
        .map((entry) => {
          const port = toFiniteNumber(entry);
          return Number.isInteger(port) && port > 0 ? port : null;
        })
        .filter((entry) => entry != null)
    : [];
  if (ports.length) payload.ports = ports;

  const volumes = Array.isArray(docker.volumes)
    ? docker.volumes.map((entry) => cleanString(entry)).filter(Boolean)
    : [];
  if (volumes.length) payload.volumes = volumes;

  const env = docker.env && typeof docker.env === "object" && !Array.isArray(docker.env)
    ? Object.entries(docker.env).reduce((acc, [key, value]) => {
        const envKey = cleanString(key);
        if (!envKey) return acc;
        acc[envKey] = String(value ?? "");
        return acc;
      }, {})
    : null;
  if (env && Object.keys(env).length) payload.env = env;

  if (docker.console !== undefined) {
    payload.console = docker.console;
  }

  return payload;
}

module.exports = {
  sanitizeDockerTemplatePayload,
};
