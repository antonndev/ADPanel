"use strict";

const express = require("express");
const multer = require("multer");
const { createDashboardAssistantService } = require("../utils/dashboard-assistant-service");

const AUDIO_UPLOAD_LIMIT_BYTES = parseInt(process.env.DASHBOARD_ASSISTANT_AUDIO_LIMIT_BYTES || "", 10) || 15 * 1024 * 1024;

function inferStatusCode(error) {
  const message = String(error?.message || "").toLowerCase();
  if (message.includes("not authenticated")) return 401;
  if (message.includes("admin required") || message.includes("permission")) return 403;
  if (message.includes("not configured")) return 503;
  if (
    message.includes("required") ||
    message.includes("valid") ||
    message.includes("too large") ||
    message.includes("could not be transcribed") ||
    message.includes("no audio")
  ) {
    return 400;
  }
  return 500;
}

module.exports = function createDashboardAssistantRouter(deps) {
  if (!deps || typeof deps !== "object") {
    throw new Error("Dashboard assistant router dependencies are required.");
  }

  const router = express.Router();
  const upload = multer({
    storage: multer.memoryStorage(),
    limits: { fileSize: AUDIO_UPLOAD_LIMIT_BYTES, files: 1 },
  });
  const service = createDashboardAssistantService(deps);

  async function requireAuth(req, res) {
    if (!(await deps.isAuthenticated(req))) {
      res.status(401).json({ error: "not authenticated" });
      return false;
    }
    return true;
  }

  router.get("/bootstrap", async (req, res) => {
    try {
      if (!(await requireAuth(req, res))) return;
      const state = await service.getBootstrap(req);
      return res.json({ ok: true, ...state });
    } catch (error) {
      console.error("[dashboard-assistant] bootstrap failed:", error);
      return res.status(500).json({ error: "failed to load assistant" });
    }
  });

  router.post("/token", async (req, res) => {
    try {
      if (!(await requireAuth(req, res))) return;
      if (!(await deps.isAdmin(req))) {
        return res.status(403).json({ error: "admin required" });
      }
      if (typeof deps.requireActionTokenOr403 === "function") {
        const ok = deps.requireActionTokenOr403(req, res, "POST /api/dashboard-assistant/token");
        if (!ok) return;
      }

      const { provider, token } = req.body || {};
      const saved = await service.saveProviderToken({ provider, token });
      return res.json({ ok: true, provider: saved });
    } catch (error) {
      const statusCode = inferStatusCode(error);
      return res.status(statusCode).json({ error: error?.message || "failed to save provider token" });
    }
  });

  router.post("/reset", async (req, res) => {
    try {
      if (!(await requireAuth(req, res))) return;
      const userEmail = String(req.session?.user || "").trim().toLowerCase();
      const chat = await service.createFreshChat(userEmail);
      return res.json({ ok: true, chat, messages: [] });
    } catch (error) {
      console.error("[dashboard-assistant] reset failed:", error);
      return res.status(500).json({ error: "failed to reset assistant chat" });
    }
  });

  router.post("/transcribe", (req, res, next) => {
    upload.single("audio")(req, res, (error) => {
      if (error) {
        const statusCode = error.code === "LIMIT_FILE_SIZE" ? 400 : 500;
        return res.status(statusCode).json({ error: error.code === "LIMIT_FILE_SIZE" ? "The recording is too large." : "Audio upload failed." });
      }
      next();
    });
  }, async (req, res) => {
    try {
      if (!(await requireAuth(req, res))) return;
      if (!req.file?.buffer) {
        return res.status(400).json({ error: "No audio was uploaded." });
      }

      const transcript = await service.transcribeAudio({
        buffer: req.file.buffer,
        mimeType: req.file.mimetype,
        filename: req.file.originalname,
        language: req.body?.language,
        prompt: req.body?.prompt,
      });

      return res.json({ ok: true, ...transcript });
    } catch (error) {
      const statusCode = inferStatusCode(error);
      return res.status(statusCode).json({ error: error?.message || "Transcription failed." });
    }
  });

  router.post("/chat", async (req, res) => {
    try {
      if (!(await requireAuth(req, res))) return;

      const userEmail = String(req.session?.user || "").trim().toLowerCase();
      const result = await service.completeChat({
        userEmail,
        chatId: req.body?.chatId,
        userMessage: req.body?.message,
        userIp: req.ip || req.connection?.remoteAddress || "unknown",
      });

      return res.json({ ok: true, ...result });
    } catch (error) {
      const statusCode = inferStatusCode(error);
      console.error("[dashboard-assistant] chat failed:", error);
      return res.status(statusCode).json({ error: error?.message || "Assistant request failed." });
    }
  });

  router.post("/confirm-delete", async (req, res) => {
    try {
      if (!(await requireAuth(req, res))) return;

      const userEmail = String(req.session?.user || "").trim().toLowerCase();
      const result = await service.confirmDeleteServer({
        userEmail,
        chatId: req.body?.chatId,
        serverName: req.body?.server,
        userIp: req.ip || req.connection?.remoteAddress || "unknown",
      });
      return res.json({ ok: true, ...result });
    } catch (error) {
      const statusCode = inferStatusCode(error);
      console.error("[dashboard-assistant] confirm-delete failed:", error);
      return res.status(statusCode).json({ error: error?.message || "Server deletion failed." });
    }
  });

  return router;
};
