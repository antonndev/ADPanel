(function () {
  const AudioContextClass = window.AudioContext || window.webkitAudioContext || null;
  const SpeechRecognitionClass = window.SpeechRecognition || window.webkitSpeechRecognition || null;
  const TARGET_PCM_SAMPLE_RATE = 16000;
  const ASSISTANT_PROVIDER_META = Object.freeze({
    groq: {
      label: "Groq",
      tokenLabel: "Groq API key",
      emptyText: "No Groq key has been saved yet.",
      placeholder: "Paste your Groq API key",
    },
    google: {
      label: "Google AI Studio",
      tokenLabel: "Google AI Studio API key",
      emptyText: "No Google AI Studio key has been saved yet.",
      placeholder: "Paste your Google AI Studio API key",
    },
  });
  const TECHNICAL_DIGIT_WORDS = Object.freeze({
    zero: "0",
    oh: "0",
    o: "0",
    one: "1",
    won: "1",
    two: "2",
    too: "2",
    to: "2",
    three: "3",
    tree: "3",
    four: "4",
    for: "4",
    five: "5",
    six: "6",
    seven: "7",
    eight: "8",
    ate: "8",
    nine: "9",
    ten: "10",
    eleven: "11",
    twelve: "12",
  });

  function getInitialSpeechLanguage() {
    const locale = String((navigator.languages && navigator.languages[0]) || navigator.language || "").trim();
    if (!locale) return "en-US";
    return /^en(?:-|$)/i.test(locale) ? locale : "en-US";
  }

  const state = {
    open: false,
    bootstrapLoaded: false,
    bootstrapPromise: null,
    bootstrapRequestId: 0,
    requestInFlight: false,
    transcribing: false,
    recording: false,
    speaking: false,
    voicePrimed: false,
    chatId: null,
    canConfigure: false,
    configured: false,
    maskedKey: "",
    actionTokens: {},
    mediaRecorder: null,
    mediaStream: null,
    mediaMimeType: "",
    mediaChunks: [],
    audioContext: null,
    audioSourceNode: null,
    audioProcessorNode: null,
    audioMonitorGain: null,
    audioFilterNodes: [],
    pcmChunks: [],
    pcmSampleRate: 44100,
    recognition: null,
    recognitionMode: false,
    recognitionFinalTranscript: "",
    recognitionInterimTranscript: "",
    recognitionStopping: false,
    recognitionErrorMessage: "",
    recognitionPreviewOnly: false,
    recognitionFallbackTranscript: "",
    lastAssistantReply: "",
    provider: "groq",
    providers: [],
    preferredSpeechLang: getInitialSpeechLanguage(),
    recordingSessionId: 0,
    chatGeneration: 0,
    lastSubmittedTranscript: "",
    lastSubmittedAt: 0,
    pendingDeleteServer: null,
    deletingServer: false,
  };

  const els = {};

  function $(id) {
    return document.getElementById(id);
  }

  function getProviderMeta(providerId) {
    return ASSISTANT_PROVIDER_META[String(providerId || "").trim().toLowerCase()] || ASSISTANT_PROVIDER_META.groq;
  }

  function supportsSpeechSynthesis() {
    return "speechSynthesis" in window && typeof window.SpeechSynthesisUtterance !== "undefined";
  }

  function supportsSpeechRecognition() {
    return !!SpeechRecognitionClass;
  }

  function isLocalhostHost() {
    const host = String(location.hostname || "").trim().toLowerCase();
    return host === "localhost" || host === "127.0.0.1" || host === "::1";
  }

  function getMicrophoneCapabilityIssue() {
    if (!navigator.mediaDevices?.getUserMedia && !supportsSpeechRecognition()) {
      if (!window.isSecureContext && !isLocalhostHost()) {
        return "Microphone access requires HTTPS or localhost. Safari and other browsers will not show the permission prompt on plain HTTP.";
      }
      return "This browser does not expose microphone access on this page.";
    }

    if (!supportsSpeechRecognition() && typeof MediaRecorder === "undefined" && !AudioContextClass) {
      return "This browser cannot capture audio for voice input here.";
    }

    return null;
  }

  function inferAudioExtension(mimeType) {
    const value = String(mimeType || "").toLowerCase();
    if (value.includes("wav")) return "wav";
    if (value.includes("mp4") || value.includes("m4a")) return "m4a";
    if (value.includes("ogg")) return "ogg";
    if (value.includes("mpeg") || value.includes("mp3")) return "mp3";
    return "webm";
  }

  function clampSample(sample) {
    if (sample > 1) return 1;
    if (sample < -1) return -1;
    return sample;
  }

  function mergePcmChunks(chunks) {
    const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
    const merged = new Float32Array(totalLength);
    let offset = 0;
    for (const chunk of chunks) {
      merged.set(chunk, offset);
      offset += chunk.length;
    }
    return merged;
  }

  function encodeWavBlob(pcm, sampleRate) {
    const buffer = new ArrayBuffer(44 + pcm.length * 2);
    const view = new DataView(buffer);

    function writeString(offset, value) {
      for (let i = 0; i < value.length; i += 1) {
        view.setUint8(offset + i, value.charCodeAt(i));
      }
    }

    writeString(0, "RIFF");
    view.setUint32(4, 36 + pcm.length * 2, true);
    writeString(8, "WAVE");
    writeString(12, "fmt ");
    view.setUint32(16, 16, true);
    view.setUint16(20, 1, true);
    view.setUint16(22, 1, true);
    view.setUint32(24, sampleRate, true);
    view.setUint32(28, sampleRate * 2, true);
    view.setUint16(32, 2, true);
    view.setUint16(34, 16, true);
    writeString(36, "data");
    view.setUint32(40, pcm.length * 2, true);

    let offset = 44;
    for (let i = 0; i < pcm.length; i += 1) {
      const sample = clampSample(pcm[i]);
      view.setInt16(offset, sample < 0 ? sample * 0x8000 : sample * 0x7fff, true);
      offset += 2;
    }

    return new Blob([buffer], { type: "audio/wav" });
  }

  function trimPcmSilence(pcm, sampleRate, threshold = 0.01, paddingMs = 120) {
    if (!(pcm instanceof Float32Array) || !pcm.length) return pcm;

    let start = -1;
    let end = -1;
    for (let index = 0; index < pcm.length; index += 1) {
      if (Math.abs(pcm[index]) >= threshold) {
        if (start === -1) start = index;
        end = index;
      }
    }

    if (start === -1 || end === -1) {
      return pcm;
    }

    const padding = Math.max(0, Math.round((sampleRate * paddingMs) / 1000));
    const safeStart = Math.max(0, start - padding);
    const safeEnd = Math.min(pcm.length, end + padding + 1);
    return pcm.slice(safeStart, safeEnd);
  }

  function normalizePcm(pcm) {
    if (!(pcm instanceof Float32Array) || !pcm.length) return pcm;

    let peak = 0;
    for (let index = 0; index < pcm.length; index += 1) {
      const value = Math.abs(pcm[index]);
      if (value > peak) peak = value;
    }

    if (!peak || peak >= 0.92) return pcm;

    const gain = Math.min(4, 0.92 / peak);
    const normalized = new Float32Array(pcm.length);
    for (let index = 0; index < pcm.length; index += 1) {
      normalized[index] = clampSample(pcm[index] * gain);
    }
    return normalized;
  }

  function resamplePcmLinear(pcm, fromRate, toRate) {
    if (!(pcm instanceof Float32Array) || !pcm.length) return pcm;
    if (!fromRate || !toRate || fromRate === toRate) return pcm;

    const ratio = fromRate / toRate;
    const targetLength = Math.max(1, Math.round(pcm.length / ratio));
    const resampled = new Float32Array(targetLength);

    for (let index = 0; index < targetLength; index += 1) {
      const sourceIndex = index * ratio;
      const leftIndex = Math.floor(sourceIndex);
      const rightIndex = Math.min(pcm.length - 1, leftIndex + 1);
      const weight = sourceIndex - leftIndex;
      resampled[index] = (pcm[leftIndex] * (1 - weight)) + (pcm[rightIndex] * weight);
    }

    return resampled;
  }

  function prepareSpeechPcm(chunks, sampleRate) {
    let pcm = mergePcmChunks(chunks);
    pcm = trimPcmSilence(pcm, sampleRate);
    pcm = normalizePcm(pcm);
    pcm = resamplePcmLinear(pcm, sampleRate, TARGET_PCM_SAMPLE_RATE);
    return {
      pcm,
      sampleRate: TARGET_PCM_SAMPLE_RATE,
    };
  }

  function getPreferredTranscriptionLanguage() {
    const locale = String(state.preferredSpeechLang || "").trim().toLowerCase();
    if (!locale) return "";
    const primary = locale.split(/[-_]/)[0];
    return primary === "en" ? "en" : "";
  }

  function buildTranscriptionPrompt() {
    return [
      "The user is speaking to ADPanel Assistant.",
      "Transcribe the speech accurately and literally, keeping technical product words intact.",
      "The user may speak English with a non-native accent, so prefer technical dashboard vocabulary over generic words when the audio is close.",
      "Important product terms may include: ADPanel, Hey ADPanel, Groq, NGINX, systemctl, Redis, MySQL, server.properties, 2FA, recovery codes.",
      "The user may also talk about nodes, templates, ports, host port, RAM, CPU, swap, storage, backups, schedules, server creation, Minecraft, Paper, Purpur, Velocity, BungeeCord, Node.js, Python, and Discord bot templates.",
      "Prefer numeric digits for ports, versions, RAM, CPU, storage sizes, and 2FA.",
      "If a spoken word sounds like to or too in a numeric technical context, prefer the digit 2.",
      "If the user says port two five five six five, write 25565.",
      "In ADPanel context, prefer node or nodes over note or notes.",
      "Prefer the product name ADPanel over similar-sounding words."
    ].join(" ");
  }

  function normalizeTechnicalUnits(text) {
    return String(text || "")
      .replace(/\bg\s*b\b/gi, "GB")
      .replace(/\bm\s*b\b/gi, "MB")
      .replace(/\bt\s*b\b/gi, "TB")
      .replace(/\bgigabytes?\b/gi, "GB")
      .replace(/\bgigs?\b/gi, "GB")
      .replace(/\bmegabytes?\b/gi, "MB")
      .replace(/\bterabytes?\b/gi, "TB");
  }

  function technicalWordToDigit(token) {
    const lowered = String(token || "").trim().toLowerCase();
    if (!lowered) return null;
    if (/^\d+$/.test(lowered)) return lowered;
    return TECHNICAL_DIGIT_WORDS[lowered] || null;
  }

  function convertTechnicalDigitSequence(raw) {
    const parts = String(raw || "").trim().split(/[\s-]+/).filter(Boolean);
    if (!parts.length || parts.length > 8) return null;
    const converted = parts.map(technicalWordToDigit);
    if (converted.some((entry) => !entry)) return null;
    return converted.join("");
  }

  function normalizeTechnicalNumbers(text) {
    let normalized = normalizeTechnicalUnits(text);

    normalized = normalized.replace(
      /\b(host\s+port|hostport|port)\s+((?:(?:zero|oh|o|one|won|two|too|to|three|tree|four|for|five|six|seven|eight|ate|nine|\d+)[\s-]+){0,7}(?:zero|oh|o|one|won|two|too|to|three|tree|four|for|five|six|seven|eight|ate|nine|\d+))\b/gi,
      (match, label, sequence) => {
        const digits = convertTechnicalDigitSequence(sequence);
        return digits ? `${label} ${digits}` : match;
      }
    );

    normalized = normalized.replace(
      /\b(zero|oh|o|one|won|two|too|to|three|tree|four|for|five|six|seven|eight|ate|nine|ten|eleven|twelve)\b(?=\s+(?:GB|MB|TB|RAM|CPU|cores?|ports?|schedules?|backups?|players?)\b)/gi,
      (match) => technicalWordToDigit(match) || match
    );

    normalized = normalized.replace(
      /\b(RAM|CPU|storage|swap|backups?|schedules?|players?)\s+(zero|oh|o|one|won|two|too|to|three|tree|four|for|five|six|seven|eight|ate|nine|ten|eleven|twelve)\b/gi,
      (match, label, amount) => `${label} ${technicalWordToDigit(amount) || amount}`
    );

    normalized = normalized.replace(/\b(to|too|two)\s*fa\b/gi, "2FA");
    normalized = normalized.replace(/\b(to|too|two)\s*factor\b/gi, "2FA");
    normalized = normalized.replace(/\b2\s*factor\b/gi, "2FA");

    return normalized;
  }

  function normalizeRecognizedTranscript(value) {
    let transcript = String(value || "").trim();
    if (!transcript) return "";

    transcript = normalizeTechnicalNumbers(transcript)
      .replace(/\b(?:a\.?\s*d\.?\s*panel|ad[\s-]*panel|adpannel|ad pannel)\b/gi, "ADPanel")
      .replace(/\bhey\s+adpanel\b/gi, "Hey ADPanel")
      .replace(/\bnotes\b/gi, "nodes")
      .replace(/\bnote\b/gi, "node")
      .replace(/\bnoads\b/gi, "nodes")
      .replace(/\bwhat'?s\s+app\b/gi, "WhatsApp")
      .replace(/\bserver\s+properties\b/gi, "server.properties")
      .replace(/\b(?:two[\s-]*factor|two fa|2 fa)\b/gi, "2FA")
      .replace(/\bgrok\b/gi, "Groq")
      .replace(/\bgroq\b/gi, "Groq")
      .replace(/\bnode\s*js\b/gi, "Node.js")
      .replace(/\bdiscord\s+bot\b/gi, "Discord bot")
      .replace(/\bram\b/gi, "RAM")
      .replace(/\bcpu\b/gi, "CPU")
      .replace(/\bnginx\b/gi, "NGINX");

    return transcript.trim();
  }

  function getRecognitionTranscript() {
    return normalizeRecognizedTranscript([
      state.recognitionFinalTranscript,
      state.recognitionInterimTranscript,
    ].filter(Boolean).join(" "));
  }

  function hasCapturedAudioActive() {
    return !!state.mediaRecorder || !!state.audioContext || !!state.mediaStream;
  }

  function scoreSpeechCandidate(transcript, confidence = 0) {
    const text = String(transcript || "").toLowerCase();
    let score = Number.isFinite(confidence) ? confidence : 0;
    if (/\bad\s*panel\b|\badpanel\b/.test(text)) score += 2;
    if (/hey\s+ad\s*panel|hey\s+adpanel/.test(text)) score += 3;
    if (/server\.?properties|systemctl|nginx|groq|2fa|redis|mysql/.test(text)) score += 0.75;
    if (/\bnodes?\b|\btemplates?\b|\bhost port\b|\bram\b|\bcpu\b|\bstorage\b|\bminecraft\b|\bpaper\b|\bpurpur\b|\bnode\.?js\b/.test(text)) score += 1.1;
    return score;
  }

  function pickBestRecognitionAlternative(result) {
    const alternatives = Array.from(result || []);
    if (!alternatives.length) return "";
    alternatives.sort((left, right) => (
      scoreSpeechCandidate(right?.transcript, right?.confidence) - scoreSpeechCandidate(left?.transcript, left?.confidence)
    ));
    return String(alternatives[0]?.transcript || "").trim();
  }

  function setStatus(message, kind = "idle") {
    if (!els.status) return;
    els.status.dataset.state = kind;
    els.status.textContent = message;
  }

  function setNotice(message) {
    if (!els.notice) return;
    if (!message) {
      els.notice.hidden = true;
      els.notice.textContent = "";
      return;
    }
    els.notice.hidden = false;
    els.notice.textContent = message;
  }

  function setVoiceState(message) {
    if (!els.voiceState) return;
    els.voiceState.textContent = message;
  }

  function syncProviderUi() {
    const meta = getProviderMeta(state.provider);
    if (els.providerSelect) {
      els.providerSelect.value = state.provider;
    }
    if (els.tokenLabel) {
      els.tokenLabel.textContent = meta.tokenLabel;
    }
    if (els.tokenInput) {
      els.tokenInput.placeholder = meta.placeholder;
    }
  }

  function setTranscriptPreview(message) {
    if (!els.transcript) return;
    if (!message) {
      els.transcript.hidden = true;
      els.transcript.textContent = "";
      return;
    }
    els.transcript.hidden = false;
    els.transcript.textContent = message;
  }

  function openDeleteModal(action) {
    if (!els.deleteModal) return;
    state.pendingDeleteServer = action && typeof action === "object" ? action : null;
    state.deletingServer = false;
    const displayName = state.pendingDeleteServer?.displayName || state.pendingDeleteServer?.server || "—";
    if (els.deleteName) {
      els.deleteName.textContent = displayName;
    }
    if (els.deleteSubtitle) {
      els.deleteSubtitle.textContent = `Confirm deletion before the assistant removes "${displayName}".`;
    }
    if (els.deleteConfirm) {
      els.deleteConfirm.disabled = false;
      els.deleteConfirm.innerHTML = '<i class="fa-solid fa-trash-can"></i>Delete server';
    }
    els.deleteModal.classList.add("show");
    els.deleteModal.setAttribute("aria-hidden", "false");
  }

  function closeDeleteModal(force = false) {
    if (!els.deleteModal || (state.deletingServer && !force)) return;
    els.deleteModal.classList.remove("show");
    els.deleteModal.setAttribute("aria-hidden", "true");
    state.pendingDeleteServer = null;
  }

  function updateComposerAvailability() {
    const canUseVoice = state.configured && !state.requestInFlight && !state.transcribing;
    if (els.mic) els.mic.disabled = !canUseVoice;
    if (els.replay) els.replay.disabled = !state.lastAssistantReply || state.speaking;
    if (els.reset) els.reset.disabled = !!(state.requestInFlight || state.transcribing || state.recording);
    if (els.send) {
      const canSendText = state.configured && !state.requestInFlight && !state.transcribing && !state.recording &&
        !!String(els.textInput?.value || "").trim();
      els.send.disabled = !canSendText;
    }
    if (els.textInput) {
      els.textInput.disabled = !state.configured || state.requestInFlight || state.transcribing;
    }
  }

  function updateButtons() {
    if (els.trigger) els.trigger.setAttribute("aria-expanded", String(state.open));
    if (els.menu) {
      els.menu.hidden = !state.open;
      els.menu.classList.toggle("is-open", state.open);
      els.menu.setAttribute("aria-hidden", String(!state.open));
    }

    if (els.mic) {
      els.mic.classList.toggle("dashboard-assistant-voice-btn--recording", state.recording);
      els.mic.innerHTML = state.recording
        ? '<span class="dashboard-assistant-voice-icon"><i class="fa-solid fa-stop"></i></span><span class="dashboard-assistant-voice-copy">Stop and send</span>'
        : '<span class="dashboard-assistant-voice-icon"><i class="fa-solid fa-microphone"></i></span><span class="dashboard-assistant-voice-copy">Tap to talk</span>';
      els.mic.title = state.recording ? "Stop recording and send" : "Start voice input";
    }

    if (els.replay) {
      els.replay.disabled = !state.lastAssistantReply || state.speaking;
    }

    if (els.speakingIndicator) {
      els.speakingIndicator.hidden = !state.speaking;
    }

    updateComposerAvailability();
  }

  function createMessageNode(message) {
    const role = message.role === "user" ? "user" : "assistant";
    const wrapper = document.createElement("article");
    wrapper.className = `dashboard-assistant-message dashboard-assistant-message--${role}`;

    const bubble = document.createElement("div");
    bubble.className = "dashboard-assistant-message-bubble";
    bubble.textContent = String(message.content || "").trim() || (role === "assistant" ? "Done." : "");
    wrapper.appendChild(bubble);

    const meta = document.createElement("div");
    meta.className = "dashboard-assistant-message-meta";
    const createdAt = message.created_at ? new Date(message.created_at) : null;
    meta.textContent = createdAt && !Number.isNaN(createdAt.getTime())
      ? `${role === "user" ? "You said" : "Assistant"} • ${createdAt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`
      : role === "user" ? "You said" : "Assistant";
    wrapper.appendChild(meta);

    return wrapper;
  }

  function renderMessages(messages) {
    if (!els.messages) return;
    els.messages.innerHTML = "";

    const visibleMessages = (Array.isArray(messages) ? messages : []).filter((message) =>
      message && (message.role === "user" || message.role === "assistant")
    );

    const lastAssistant = [...visibleMessages].reverse().find((message) => message.role === "assistant");
    state.lastAssistantReply = lastAssistant ? String(lastAssistant.content || "").trim() : "";

    if (!visibleMessages.length) {
      const empty = document.createElement("div");
      empty.className = "dashboard-assistant-empty";
      const activeMeta = getProviderMeta(state.provider);
      empty.textContent = state.configured
        ? "Ask with text or voice. The assistant keeps the current chat context and runs allowed actions."
        : state.canConfigure
          ? `Save a ${activeMeta.label} API key to unlock voice input, context-aware chat, and spoken replies.`
          : "The administrator has not configured an AI provider yet.";
      els.messages.appendChild(empty);
      updateButtons();
      return;
    }

    visibleMessages.forEach((message) => {
      els.messages.appendChild(createMessageNode(message));
    });
    els.messages.scrollTop = els.messages.scrollHeight;
    updateButtons();
  }

  function appendMessageToUi(message) {
    if (!els.messages) return;
    if (els.messages.querySelector(".dashboard-assistant-empty")) {
      els.messages.innerHTML = "";
    }
    els.messages.appendChild(createMessageNode(message));
    els.messages.scrollTop = els.messages.scrollHeight;
    if (message?.role === "assistant") {
      state.lastAssistantReply = String(message.content || "").trim();
    }
  }

  function positionMenu() {
    if (!state.open || !els.menu || !els.trigger) return;
    const triggerRect = els.trigger.getBoundingClientRect();
    const menuWidth = Math.min(540, window.innerWidth - 20);
    const left = Math.min(
      window.innerWidth - menuWidth - 10,
      Math.max(10, triggerRect.right - menuWidth)
    );

    els.menu.style.width = `${menuWidth}px`;
    els.menu.style.left = `${left}px`;
    els.menu.style.top = `${Math.max(10, triggerRect.bottom + 14)}px`;

    requestAnimationFrame(() => {
      const menuRect = els.menu.getBoundingClientRect();
      if (menuRect.bottom > window.innerHeight - 10) {
        const fallbackTop = Math.max(10, triggerRect.top - menuRect.height - 12);
        els.menu.style.top = `${fallbackTop}px`;
      }
    });
  }

  function stopSpeaking() {
    if (!supportsSpeechSynthesis()) return;
    try {
      window.speechSynthesis.cancel();
    } catch {
    }
    state.speaking = false;
    updateButtons();
  }

  function findPreferredVoice() {
    if (!supportsSpeechSynthesis()) return null;
    const voices = window.speechSynthesis.getVoices();
    if (!Array.isArray(voices) || !voices.length) return null;

    const preferredLangs = [];
    const normalizedPreferred = String(state.preferredSpeechLang || "en-US").trim();
    if (normalizedPreferred) preferredLangs.push(normalizedPreferred.toLowerCase());
    if (normalizedPreferred.includes("-")) preferredLangs.push(normalizedPreferred.split("-")[0].toLowerCase());
    preferredLangs.push("en-us", "en");

    for (const lang of preferredLangs) {
      const exact = voices.find((voice) => String(voice.lang || "").toLowerCase() === lang);
      if (exact) return exact;
      const partial = voices.find((voice) => String(voice.lang || "").toLowerCase().startsWith(lang));
      if (partial) return partial;
    }

    return voices[0] || null;
  }

  function primeSpeechSynthesis() {
    if (!supportsSpeechSynthesis() || state.voicePrimed) return;
    try {
      window.speechSynthesis.getVoices();
      const utterance = new window.SpeechSynthesisUtterance(" ");
      utterance.volume = 0;
      utterance.rate = 1;
      utterance.pitch = 1;
      window.speechSynthesis.speak(utterance);
      window.speechSynthesis.cancel();
      state.voicePrimed = true;
    } catch {
    }
  }

  function speakAssistantReply(text) {
    const message = String(text || "").trim();
    if (!message || !supportsSpeechSynthesis()) return false;

    stopSpeaking();

    const utterance = new window.SpeechSynthesisUtterance(message);
    const voice = findPreferredVoice();
    if (voice) {
      utterance.voice = voice;
      utterance.lang = voice.lang;
    } else {
      utterance.lang = state.preferredSpeechLang || "en-US";
    }
    utterance.rate = 1;
    utterance.pitch = 1;
    utterance.volume = 1;

    utterance.onstart = () => {
      state.speaking = true;
      setVoiceState("Speaking response...");
      updateButtons();
    };
    utterance.onend = () => {
      state.speaking = false;
      setVoiceState(state.configured ? "Voice replies are on." : "Voice replies are unavailable.");
      updateButtons();
    };
    utterance.onerror = () => {
      state.speaking = false;
      setVoiceState("Voice playback could not start in this browser.");
      updateButtons();
    };

    try {
      window.speechSynthesis.speak(utterance);
      return true;
    } catch {
      state.speaking = false;
      updateButtons();
      return false;
    }
  }

  function applyBootstrap(data, options = {}) {
    const preserveRenderedMessages = !!options.preserveRenderedMessages;
    state.bootstrapLoaded = true;
    state.bootstrapPromise = null;
    const nextChatId = data.chat?.id || null;
    if (state.chatId && nextChatId && state.chatId !== nextChatId) {
      state.chatGeneration += 1;
      state.lastSubmittedTranscript = "";
      state.lastSubmittedAt = 0;
      state.recognitionFallbackTranscript = "";
      state.recognitionFinalTranscript = "";
      state.recognitionInterimTranscript = "";
      setTranscriptPreview("");
    }
    state.chatId = nextChatId;
    state.canConfigure = !!data.canConfigure;
    state.configured = !!data.configured;
    state.provider = String(data.provider || state.provider || "groq").trim().toLowerCase() || "groq";
    state.providers = Array.isArray(data.providers) ? data.providers : [];
    state.maskedKey = String(data.maskedKey || "");
    state.actionTokens = data.actionTokens || {};
    syncProviderUi();

    if (els.configToggle) {
      els.configToggle.hidden = !state.canConfigure || !state.configured;
    }

    if (els.config) {
      const shouldShowConfig = state.canConfigure && !state.configured;
      els.config.hidden = !shouldShowConfig;
    }

    if (els.tokenState) {
      const activeMeta = getProviderMeta(state.provider);
      els.tokenState.textContent = state.configured
        ? `${activeMeta.label}: ${state.maskedKey || "Configured"}`
        : activeMeta.emptyText;
    }

    const capabilityIssue = getMicrophoneCapabilityIssue();
    if (!state.configured && !state.canConfigure) {
      setNotice("The administrator has not configured an AI provider yet. Ask an admin to open the AI menu and save one.");
    } else if (capabilityIssue) {
      setNotice(capabilityIssue);
    } else {
      setNotice("");
    }

    if (!preserveRenderedMessages) {
      renderMessages(data.messages || []);
    } else {
      updateButtons();
    }
    updateButtons();
    setVoiceState(
      supportsSpeechSynthesis()
        ? "Voice replies are on."
        : "Voice playback is not available in this browser."
    );
    setStatus(
      state.recording
        ? "Listening..."
        : state.transcribing
          ? "Transcribing your recording..."
          : state.requestInFlight
            ? "Assistant is working..."
            : state.configured
              ? "Voice assistant ready."
              : state.canConfigure
                ? `Save a ${getProviderMeta(state.provider).label} key to continue.`
                : "No AI provider is configured.",
      state.recording
        ? "recording"
        : state.transcribing
          ? "transcribing"
          : state.requestInFlight
            ? "working"
            : state.configured
              ? "idle"
              : "warning"
    );
  }

  async function fetchJson(url, options) {
    const response = await fetch(url, Object.assign({ credentials: "include" }, options || {}));
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(data.error || data.message || "Request failed.");
    }
    return data;
  }

  async function ensureBootstrap(force = false) {
    if (state.bootstrapPromise && !force) return state.bootstrapPromise;
    if (state.bootstrapLoaded && !force) return Promise.resolve();

    const requestId = ++state.bootstrapRequestId;
    const startedAt = Date.now();
    state.bootstrapPromise = (async () => {
      const data = await fetchJson("/api/dashboard-assistant/bootstrap");
      if (requestId !== state.bootstrapRequestId) return;
      const preserveRenderedMessages = !force && state.lastSubmittedAt > startedAt;
      applyBootstrap(data, { preserveRenderedMessages });
    })().catch((error) => {
      if (requestId === state.bootstrapRequestId) {
        state.bootstrapPromise = null;
      }
      throw error;
    });

    return state.bootstrapPromise;
  }

  function openMenu() {
    state.open = true;
    primeSpeechSynthesis();
    updateButtons();
    positionMenu();
    ensureBootstrap().catch((error) => {
      setStatus(error.message || "Failed to load the assistant.", "error");
      setNotice(error.message || "Failed to load the assistant.");
    });
  }

  function closeMenu() {
    state.open = false;
    updateButtons();
  }

  async function saveToken(event) {
    event?.preventDefault();
    if (!els.tokenInput || !els.providerSelect) return;

    const token = els.tokenInput.value.trim();
    const provider = String(els.providerSelect.value || state.provider || "groq").trim().toLowerCase();
    const providerMeta = getProviderMeta(provider);
    if (!token && !state.providers.some((item) => item && item.id === provider && item.configured)) {
      setStatus(`Enter a ${providerMeta.label} API key first.`, "warning");
      return;
    }

    const saveTokenHeader = state.actionTokens?.saveToken;
    const headers = { "Content-Type": "application/json" };
    if (saveTokenHeader) headers["x-action-token"] = saveTokenHeader;

    try {
      if (els.saveToken) els.saveToken.disabled = true;
      setStatus(token ? `Saving ${providerMeta.label} API key...` : `Activating ${providerMeta.label}...`, "working");
      await fetchJson("/api/dashboard-assistant/token", {
        method: "POST",
        headers,
        body: JSON.stringify({ provider, token }),
      });
      els.tokenInput.value = "";
      await ensureBootstrap(true);
      setStatus(token ? `${providerMeta.label} API key saved.` : `${providerMeta.label} is now active.`, "idle");
    } catch (error) {
      setStatus(error.message || "Failed to save the assistant provider key.", "error");
    } finally {
      if (els.saveToken) els.saveToken.disabled = false;
    }
  }

  async function resetChat() {
    try {
      state.bootstrapRequestId += 1;
      state.chatGeneration += 1;
      state.lastSubmittedTranscript = "";
      state.lastSubmittedAt = 0;
      state.recognitionFallbackTranscript = "";
      state.recognitionFinalTranscript = "";
      state.recognitionInterimTranscript = "";
      if (els.reset) els.reset.disabled = true;
      setStatus("Starting a new chat...", "working");
      const data = await fetchJson("/api/dashboard-assistant/reset", { method: "POST" });
      setTranscriptPreview("");
      applyBootstrap({
        configured: state.configured,
        canConfigure: state.canConfigure,
        provider: state.provider,
        providers: state.providers,
        chat: data.chat,
        messages: data.messages || [],
        actionTokens: state.actionTokens,
        maskedKey: state.maskedKey,
      });
      setStatus("New voice chat ready.", "idle");
    } catch (error) {
      setStatus(error.message || "Failed to reset the assistant chat.", "error");
    } finally {
      if (els.reset) els.reset.disabled = false;
    }
  }

  async function runClientActions(actions) {
    if (!Array.isArray(actions) || !actions.length) return;

    for (const action of actions) {
      if (action?.type === "open_account_flow") {
        const helper = window.ADPanelDashboardAccount;
        if (!helper || typeof helper.openAccountFlow !== "function") {
          setStatus("The account flow could not be opened here.", "error");
          continue;
        }
        try {
          await helper.openAccountFlow(action.flow);
        } catch (error) {
          setStatus(error.message || "Failed to open the requested account flow.", "error");
        }
        continue;
      }
      if (action?.type === "open_create_server_modal") {
        const helper = window.ADPanelDashboardCreate;
        if (!helper || typeof helper.openCreateModal !== "function") {
          setStatus("The create server window could not be opened here.", "error");
          continue;
        }
        try {
          closeMenu();
          await helper.openCreateModal();
        } catch (error) {
          setStatus(error.message || "Failed to open the create server window.", "error");
        }
        continue;
      }
      if (action?.type === "confirm_delete_server") {
        openDeleteModal(action);
      }
    }
  }

  async function confirmDeleteModal() {
    if (!state.pendingDeleteServer || state.deletingServer) return;

    const serverName = String(state.pendingDeleteServer.server || "").trim();
    if (!serverName) return;

    state.deletingServer = true;
    if (els.deleteConfirm) {
      els.deleteConfirm.disabled = true;
      els.deleteConfirm.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>Deleting...';
    }
    setStatus(`Deleting ${serverName}...`, "working");

    try {
      const data = await fetchJson("/api/dashboard-assistant/confirm-delete", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chatId: state.chatId,
          server: serverName,
        }),
      });

      applyBootstrap({
        configured: state.configured,
        canConfigure: state.canConfigure,
        provider: data.provider || state.provider,
        providers: data.providers || state.providers,
        chat: data.chat,
        messages: Array.isArray(data.messages) ? data.messages : [],
        actionTokens: state.actionTokens,
        maskedKey: state.maskedKey,
      });

      closeDeleteModal(true);
      const reply = String(data.reply || `Deleted ${serverName}.`).trim();
      setStatus("Assistant ready.", "idle");
      if (reply) {
        const spoken = speakAssistantReply(reply);
        if (!spoken) {
          setVoiceState("Voice playback could not start automatically.");
        }
      }
    } catch (error) {
      setStatus(error.message || "Failed to delete that server.", "error");
      if (els.deleteConfirm) {
        els.deleteConfirm.disabled = false;
        els.deleteConfirm.innerHTML = '<i class="fa-solid fa-trash-can"></i>Delete server';
      }
    } finally {
      state.deletingServer = false;
    }
  }

  async function submitMessage(rawMessage, options = {}) {
    const normalize = !!options.normalize;
    const message = normalize ? normalizeRecognizedTranscript(rawMessage) : String(rawMessage || "").trim();
    if (!message || state.requestInFlight) return false;
    const now = Date.now();
    if (
      state.lastSubmittedTranscript &&
      message === state.lastSubmittedTranscript &&
      now - state.lastSubmittedAt < 4000
    ) {
      return false;
    }
    state.recognitionFallbackTranscript = "";
    if (!state.configured) {
      setStatus(
        state.canConfigure
          ? `Save a ${getProviderMeta(state.provider).label} API key before using the voice assistant.`
          : "The administrator has not configured an AI provider yet.",
        "warning"
      );
      return false;
    }

    const previousMessages = Array.from((els.messages && els.messages.children) || []).map((node) => node.cloneNode(true));
    const requestGeneration = state.chatGeneration;
    state.bootstrapRequestId += 1;
    state.requestInFlight = true;
    state.lastSubmittedTranscript = message;
    state.lastSubmittedAt = now;
    stopSpeaking();
    updateButtons();
    setStatus("Assistant is working...", "working");
    setTranscriptPreview(normalize ? `You said: ${message}` : "");

    if (els.messages) {
      appendMessageToUi({ role: "user", content: message, created_at: new Date().toISOString() });
    }

    try {
      const data = await fetchJson("/api/dashboard-assistant/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chatId: state.chatId,
          message,
        }),
      });
      if (requestGeneration !== state.chatGeneration) {
        return false;
      }
      const reply = String(data.reply || "").trim();
      const serverMessages = Array.isArray(data.messages) ? data.messages : [];
      const lastServerAssistant = [...serverMessages].reverse().find((message) => message && message.role === "assistant");
      const serverAssistantReply = String(lastServerAssistant?.content || "").trim();
      const hasRenderedAssistant = !!serverAssistantReply && (!reply || serverAssistantReply === reply);
      applyBootstrap({
        configured: state.configured,
        canConfigure: state.canConfigure,
        provider: data.provider || state.provider,
        providers: data.providers || state.providers,
        chat: data.chat,
        messages: hasRenderedAssistant ? serverMessages : [],
        actionTokens: state.actionTokens,
        maskedKey: state.maskedKey,
      }, { preserveRenderedMessages: !hasRenderedAssistant });
      if (!hasRenderedAssistant && reply) {
        appendMessageToUi({ role: "assistant", content: reply, created_at: new Date().toISOString() });
        state.lastAssistantReply = reply;
      }
      setTranscriptPreview("");
      setStatus("Assistant ready.", "idle");
      if (reply) {
        const spoken = speakAssistantReply(reply);
        if (!spoken) {
          setVoiceState("Voice playback could not start automatically.");
        }
      }
      await runClientActions(data.clientActions || []);
      return true;
    } catch (error) {
      if (requestGeneration !== state.chatGeneration) {
        return false;
      }
      if (els.messages) {
        els.messages.innerHTML = "";
        previousMessages.forEach((node) => els.messages.appendChild(node));
      }
      setStatus(error.message || "Assistant request failed.", "error");
      return false;
    } finally {
      state.requestInFlight = false;
      updateButtons();
      if (!state.recording && !state.transcribing) {
        setTranscriptPreview("");
      }
    }
  }

  async function submitTranscript(rawTranscript) {
    return submitMessage(rawTranscript, { normalize: true });
  }

  async function submitTextMessage(event) {
    event?.preventDefault();
    const value = String(els.textInput?.value || "").trim();
    if (!value) return;
    const sent = await submitMessage(value, { normalize: false });
    if (sent && els.textInput) {
      els.textInput.value = "";
    }
    updateButtons();
  }

  async function uploadRecording(blob, mimeType, fallbackTranscript = "") {
    const formData = new FormData();
    const extension = inferAudioExtension(mimeType || blob.type);
    formData.append("audio", blob, `dashboard-assistant.${extension}`);
    const language = getPreferredTranscriptionLanguage();
    if (language) {
      formData.append("language", language);
    }
    formData.append("prompt", buildTranscriptionPrompt());
    state.transcribing = true;
    updateButtons();
    setStatus("Transcribing your recording...", "transcribing");

    try {
      const response = await fetch("/api/dashboard-assistant/transcribe", {
        method: "POST",
        body: formData,
        credentials: "include",
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(data.error || "Transcription failed.");
      }
      await submitTranscript(data.transcript || "");
    } catch (error) {
      const fallback = normalizeRecognizedTranscript(fallbackTranscript || state.recognitionFallbackTranscript);
      if (fallback) {
        setStatus("AI transcription failed, using the live speech transcript instead.", "warning");
        await submitTranscript(fallback);
        return;
      }
      setStatus(error.message || "Voice transcription failed.", "error");
    } finally {
      state.transcribing = false;
      updateButtons();
      if (!state.requestInFlight && !state.recording) {
        setStatus(state.configured ? "Voice assistant ready." : "No AI provider is configured.", state.configured ? "idle" : "warning");
      }
    }
  }

  function cleanupAudioGraph() {
    try {
      state.audioProcessorNode?.disconnect();
    } catch {
    }
    try {
      state.audioSourceNode?.disconnect();
    } catch {
    }
    try {
      state.audioMonitorGain?.disconnect();
    } catch {
    }
    state.audioFilterNodes.forEach((node) => {
      try {
        node?.disconnect?.();
      } catch {
      }
    });
    if (state.audioContext) {
      state.audioContext.close().catch(() => {});
    }
    state.audioProcessorNode = null;
    state.audioSourceNode = null;
    state.audioMonitorGain = null;
    state.audioContext = null;
    state.audioFilterNodes = [];
  }

  function stopActiveStream() {
    try {
      state.mediaStream?.getTracks().forEach((track) => track.stop());
    } catch {
    }
    state.mediaStream = null;
  }

  async function stopRecordingAndUpload() {
    const currentSessionId = state.recordingSessionId;
    if (state.recognitionMode && state.recognition && !hasCapturedAudioActive()) {
      state.recognitionStopping = true;
      state.recording = false;
      updateButtons();
      setStatus("Finalizing transcript...", "working");
      setVoiceState("Finishing speech recognition...");
      try {
        state.recognition.stop();
      } catch {
        const transcript = getRecognitionTranscript();
        cleanupRecognition();
        if (transcript) {
          await submitTranscript(transcript);
        } else {
          setStatus("I didn't catch that. Try speaking a little closer to the mic.", "warning");
        }
      }
      return;
    }

    if (state.recognitionMode && state.recognition) {
      state.recognitionFallbackTranscript = getRecognitionTranscript();
      state.recognitionStopping = true;
      try {
        state.recognition.stop();
      } catch {
        cleanupRecognition();
      }
    }

    if (state.mediaRecorder) {
      state.recording = false;
      updateButtons();
      state.mediaRecorder.stop();
      return;
    }

    if (state.audioContext) {
      state.recording = false;
      updateButtons();
      const pcmChunks = state.pcmChunks.slice();
      const sampleRate = state.pcmSampleRate;
      state.pcmChunks = [];
      cleanupAudioGraph();
      stopActiveStream();

      if (!pcmChunks.length) {
        const fallback = normalizeRecognizedTranscript(state.recognitionFallbackTranscript);
        if (fallback) {
          await submitTranscript(fallback);
        } else {
          setStatus("The recording was empty.", "warning");
        }
        return;
      }

      const prepared = prepareSpeechPcm(pcmChunks, sampleRate);
      if (!prepared.pcm.length) {
        const fallback = normalizeRecognizedTranscript(state.recognitionFallbackTranscript);
        if (fallback) {
          await submitTranscript(fallback);
        } else {
          setStatus("The recording was too quiet to transcribe clearly.", "warning");
        }
        return;
      }
      const wavBlob = encodeWavBlob(prepared.pcm, prepared.sampleRate);
      if (currentSessionId === state.recordingSessionId) {
        await uploadRecording(wavBlob, "audio/wav", state.recognitionFallbackTranscript);
      }
    }
  }

  function createMicrophoneStartErrorMessage(error) {
    const name = String(error?.name || "").toLowerCase();
    if (!window.isSecureContext && !isLocalhostHost()) {
      return "Microphone access requires HTTPS or localhost. Safari will not prompt on plain HTTP.";
    }
    if (name === "notallowederror" || name === "securityerror") {
      return "Microphone permission was denied. Check the browser site permissions and try again.";
    }
    if (name === "notfounderror") {
      return "No microphone was found on this device.";
    }
    if (name === "notreadableerror" || name === "trackstarterror") {
      return "The microphone is already in use by another app or tab.";
    }
    return error?.message || "Failed to start microphone recording.";
  }

  function createRecognitionErrorMessage(errorCode) {
    const code = String(errorCode || "").trim().toLowerCase();
    if (code === "not-allowed" || code === "service-not-allowed") {
      return "Speech recognition permission was denied. Check the browser site permissions and try again.";
    }
    if (code === "audio-capture") {
      return "The browser could not access a working microphone.";
    }
    if (code === "network") {
      return "Live speech recognition failed because the browser recognition service was unavailable.";
    }
    if (code === "no-speech") {
      return "No clear speech was detected. Try speaking a little closer to the microphone.";
    }
    return "Live speech recognition could not start in this browser.";
  }

  function cleanupRecognition() {
    if (state.recognition) {
      state.recognition.onstart = null;
      state.recognition.onresult = null;
      state.recognition.onerror = null;
      state.recognition.onend = null;
    }
    state.recognition = null;
    state.recognitionMode = false;
    state.recognitionFinalTranscript = "";
    state.recognitionInterimTranscript = "";
    state.recognitionStopping = false;
    state.recognitionErrorMessage = "";
    state.recognitionPreviewOnly = false;
  }

  async function applyPreferredTrackConstraints(stream) {
    const track = stream?.getAudioTracks?.()[0];
    if (!track || typeof track.applyConstraints !== "function") return;

    try {
      await track.applyConstraints({
        echoCancellation: true,
        noiseSuppression: true,
        autoGainControl: true,
        channelCount: 1,
        sampleRate: TARGET_PCM_SAMPLE_RATE,
        sampleSize: 16,
        advanced: [
          {
            echoCancellation: true,
            noiseSuppression: true,
            autoGainControl: true,
            voiceIsolation: true,
          },
        ],
      });
    } catch {
    }
  }

  async function startCapturedRecording() {
    state.recordingSessionId += 1;
    state.recognitionFallbackTranscript = "";
    state.recognitionFinalTranscript = "";
    state.recognitionInterimTranscript = "";
    state.recognitionErrorMessage = "";
    setTranscriptPreview("");

    const stream = await navigator.mediaDevices.getUserMedia({
      audio: {
        channelCount: { ideal: 1 },
        sampleRate: { ideal: TARGET_PCM_SAMPLE_RATE },
        sampleSize: { ideal: 16 },
        echoCancellation: true,
        noiseSuppression: true,
        autoGainControl: true,
        voiceIsolation: true,
      },
    });

    await applyPreferredTrackConstraints(stream);

    const mode = chooseRecorderMode();
    if (mode === "media-recorder") {
      await startMediaRecorder(stream);
      return;
    }
    await startPcmRecorder(stream);
  }

  async function startSpeechRecognition(options = {}) {
    if (!supportsSpeechRecognition()) {
      throw new Error("This browser does not support live speech recognition here.");
    }

    const previewOnly = !!options.previewOnly;
    const sessionId = Number(options.sessionId || state.recordingSessionId || 0);

    return new Promise((resolve, reject) => {
      let started = false;
      let settled = false;
      const recognition = new SpeechRecognitionClass();

      state.recognition = recognition;
      state.recognitionMode = true;
      state.recognitionFinalTranscript = "";
      state.recognitionInterimTranscript = "";
      state.recognitionStopping = false;
      state.recognitionErrorMessage = "";
      state.recognitionPreviewOnly = previewOnly;

      recognition.lang = state.preferredSpeechLang || "en-US";
      recognition.continuous = true;
      recognition.interimResults = true;
      recognition.maxAlternatives = 5;

      recognition.onstart = () => {
        started = true;
        settled = true;
        if (!previewOnly) {
          state.recording = true;
          updateButtons();
          setStatus("Listening live...", "recording");
          setVoiceState("Listening with live speech recognition...");
          setTranscriptPreview("");
        } else {
          setVoiceState("Live speech preview is active.");
        }
        resolve();
      };

      recognition.onresult = (event) => {
        if (sessionId !== state.recordingSessionId) return;
        const results = Array.from(event.results || []);
        const finalParts = [];
        const interimParts = [];

        results.forEach((result) => {
          const transcript = pickBestRecognitionAlternative(result);
          if (!transcript) return;
          if (result.isFinal) {
            finalParts.push(transcript);
          } else {
            interimParts.push(transcript);
          }
        });

        state.recognitionFinalTranscript = finalParts.join(" ").trim();
        state.recognitionInterimTranscript = interimParts.join(" ").trim();

        const preview = getRecognitionTranscript();

        if (preview) {
          state.recognitionFallbackTranscript = preview;
          setTranscriptPreview(`Listening: ${preview}`);
          setVoiceState("Speech detected. Tap again to send or keep talking.");
        }
      };

      recognition.onerror = (event) => {
        if (sessionId !== state.recordingSessionId) return;
        const errorCode = String(event?.error || "").trim().toLowerCase();
        if (errorCode === "aborted" && state.recognitionStopping) {
          return;
        }
        const message = createRecognitionErrorMessage(errorCode);
        state.recognitionErrorMessage = message;
        if (!started && !settled) {
          settled = true;
          cleanupRecognition();
          reject(new Error(message));
        }
      };

      recognition.onend = () => {
        if (sessionId !== state.recordingSessionId) return;
        const transcript = getRecognitionTranscript();
        const shouldSubmit = !!transcript;
        const errorMessage = state.recognitionErrorMessage;
        const stoppedByUser = state.recognitionStopping;
        const previewOnlyMode = state.recognitionPreviewOnly;
        const captureActive = hasCapturedAudioActive();
        if (transcript) {
          state.recognitionFallbackTranscript = transcript;
        }

        cleanupRecognition();
        if (!previewOnlyMode) {
          state.recording = false;
          updateButtons();
        }

        if (previewOnlyMode) {
          if (errorMessage && !captureActive) {
            setStatus(errorMessage, "error");
            setNotice(errorMessage);
          } else if (errorMessage) {
            setVoiceState(errorMessage);
          }
          return;
        }

        if (shouldSubmit) {
          setStatus("Speech recognized.", "working");
          void submitTranscript(transcript);
          return;
        }

        if (errorMessage) {
          setStatus(errorMessage, "error");
          setNotice(errorMessage);
          return;
        }

        if (stoppedByUser) {
          setStatus("I didn't catch that. Try speaking a little closer to the mic.", "warning");
          setVoiceState("No speech detected.");
        }
      };

      try {
        recognition.start();
      } catch (error) {
        cleanupRecognition();
        reject(error);
      }
    });
  }

  function chooseRecorderMode() {
    if (typeof MediaRecorder === "undefined") {
      return "pcm";
    }
    const preferredMimeTypes = [
      "audio/webm;codecs=opus",
      "audio/webm",
      "audio/mp4",
      "audio/mp4;codecs=mp4a.40.2",
    ];
    const supported = preferredMimeTypes.find((mimeType) =>
      typeof MediaRecorder.isTypeSupported === "function" ? MediaRecorder.isTypeSupported(mimeType) : false
    );
    if (supported) {
      state.mediaMimeType = supported;
      if (supported.includes("webm")) return "media-recorder";
    }
    return AudioContextClass ? "pcm" : "media-recorder";
  }

  async function startMediaRecorder(stream) {
    const options = state.mediaMimeType ? { mimeType: state.mediaMimeType } : undefined;
    const recorder = options ? new MediaRecorder(stream, options) : new MediaRecorder(stream);
    const sessionId = state.recordingSessionId;

    state.mediaRecorder = recorder;
    state.mediaChunks = [];
    state.mediaStream = stream;
    state.recording = true;

    recorder.addEventListener("dataavailable", (event) => {
      if (event.data && event.data.size > 0) {
        state.mediaChunks.push(event.data);
      }
    });

    recorder.addEventListener("stop", async () => {
      const mimeType = recorder.mimeType || state.mediaMimeType || "audio/webm";
      const blob = new Blob(state.mediaChunks, { type: mimeType });
      state.mediaChunks = [];
      state.mediaRecorder = null;
      stopActiveStream();

      if (sessionId !== state.recordingSessionId) {
        return;
      }

      if (blob.size > 0) {
        await uploadRecording(blob, mimeType, state.recognitionFallbackTranscript);
      } else {
        const fallback = normalizeRecognizedTranscript(state.recognitionFallbackTranscript);
        if (fallback) {
          await submitTranscript(fallback);
        } else {
          setStatus("The recording was empty.", "warning");
        }
      }
    });

    recorder.start();
    updateButtons();
    setStatus("Listening...", "recording");
    setVoiceState("Recording voice input...");
  }

  async function startPcmRecorder(stream) {
    if (!AudioContextClass) {
      throw new Error("This browser cannot record audio here.");
    }

    const audioContext = new AudioContextClass();
    if (audioContext.state === "suspended") {
      await audioContext.resume().catch(() => {});
    }

    const sourceNode = audioContext.createMediaStreamSource(stream);
    const highPass = audioContext.createBiquadFilter();
    const lowPass = audioContext.createBiquadFilter();
    const compressor = audioContext.createDynamicsCompressor();
    const processorNode = audioContext.createScriptProcessor(4096, 1, 1);
    const monitorGain = audioContext.createGain();
    highPass.type = "highpass";
    highPass.frequency.value = 80;
    lowPass.type = "lowpass";
    lowPass.frequency.value = 4600;
    compressor.threshold.value = -24;
    compressor.knee.value = 18;
    compressor.ratio.value = 3;
    compressor.attack.value = 0.003;
    compressor.release.value = 0.25;
    monitorGain.gain.value = 0;

    state.audioContext = audioContext;
    state.audioSourceNode = sourceNode;
    state.audioProcessorNode = processorNode;
    state.audioMonitorGain = monitorGain;
    state.audioFilterNodes = [highPass, lowPass, compressor];
    state.mediaStream = stream;
    state.pcmChunks = [];
    state.pcmSampleRate = audioContext.sampleRate;
    state.recording = true;

    processorNode.onaudioprocess = (event) => {
      const input = event.inputBuffer.getChannelData(0);
      state.pcmChunks.push(new Float32Array(input));
    };

    sourceNode.connect(highPass);
    highPass.connect(lowPass);
    lowPass.connect(compressor);
    compressor.connect(processorNode);
    processorNode.connect(monitorGain);
    monitorGain.connect(audioContext.destination);

    updateButtons();
    setStatus("Listening...", "recording");
    setVoiceState("Recording voice input...");
  }

  async function toggleRecording() {
    if (state.transcribing || state.requestInFlight) return;
    if (!state.configured) {
      setStatus(`Save a ${getProviderMeta(state.provider).label} key before using voice input.`, "warning");
      return;
    }

    if (state.recording) {
      await stopRecordingAndUpload();
      return;
    }

    const capabilityIssue = getMicrophoneCapabilityIssue();
    if (capabilityIssue) {
      setStatus(capabilityIssue, "error");
      setNotice(capabilityIssue);
      return;
    }

    primeSpeechSynthesis();
    stopSpeaking();

    try {
      try {
        await startCapturedRecording();
        const activeSessionId = state.recordingSessionId;
        if (supportsSpeechRecognition()) {
          void startSpeechRecognition({ previewOnly: true, sessionId: activeSessionId }).catch(() => {
            setVoiceState("Live speech preview is unavailable here. AI transcription will still be used.");
          });
        }
        return;
      } catch (captureError) {
        if (supportsSpeechRecognition()) {
          await startSpeechRecognition();
          return;
        }
        throw captureError;
      }
    } catch (error) {
      const message = createMicrophoneStartErrorMessage(error);
      setStatus(message, "error");
      setNotice(message);
      stopActiveStream();
      cleanupAudioGraph();
      cleanupRecognition();
      state.mediaRecorder = null;
      state.mediaChunks = [];
      state.pcmChunks = [];
      state.recording = false;
      updateButtons();
    }
  }

  function bindEvents() {
    els.trigger?.addEventListener("click", (event) => {
      event.stopPropagation();
      if (!state.open) {
        openMenu();
        return;
      }
      positionMenu();
    });

    els.close?.addEventListener("click", closeMenu);
    els.reset?.addEventListener("click", () => { void resetChat(); });
    els.configToggle?.addEventListener("click", () => {
      if (!els.config) return;
      els.config.hidden = !els.config.hidden;
    });
    els.providerSelect?.addEventListener("change", () => {
      state.provider = String(els.providerSelect.value || state.provider || "groq").trim().toLowerCase() || "groq";
      syncProviderUi();
      const selectedProvider = state.providers.find((item) => item && item.id === state.provider);
      if (els.tokenState) {
        els.tokenState.textContent = selectedProvider?.configured
          ? `${getProviderMeta(state.provider).label}: ${selectedProvider.maskedKey || "Configured"}`
          : getProviderMeta(state.provider).emptyText;
      }
    });
    els.tokenForm?.addEventListener("submit", (event) => { void saveToken(event); });
    els.composeForm?.addEventListener("submit", (event) => { void submitTextMessage(event); });
    els.textInput?.addEventListener("input", updateButtons);
    els.textInput?.addEventListener("keydown", (event) => {
      if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        void submitTextMessage(event);
      }
    });
    els.menu?.addEventListener("click", (event) => {
      event.stopPropagation();
    });

    els.mic?.addEventListener("click", (event) => {
      event.stopPropagation();
      void toggleRecording();
    });
    els.replay?.addEventListener("click", (event) => {
      event.stopPropagation();
      if (!state.lastAssistantReply) return;
      primeSpeechSynthesis();
      const spoken = speakAssistantReply(state.lastAssistantReply);
      if (!spoken) {
        setVoiceState("Voice playback could not start automatically.");
      }
    });

    els.deleteClose?.addEventListener("click", () => closeDeleteModal());
    els.deleteCancel?.addEventListener("click", () => closeDeleteModal());
    els.deleteConfirm?.addEventListener("click", () => { void confirmDeleteModal(); });
    els.deleteModal?.addEventListener("click", (event) => {
      if (event.target === els.deleteModal) {
        closeDeleteModal();
      }
    });

    document.addEventListener("click", (event) => {
      if (!state.open || !els.menu || !els.trigger) return;
      if (els.deleteModal?.classList.contains("show") && els.deleteModal.contains(event.target)) return;
      if (els.menu.contains(event.target) || els.trigger.contains(event.target)) return;
      closeMenu();
    });

    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && state.open) return;
    });

    if (supportsSpeechSynthesis()) {
      window.speechSynthesis.onvoiceschanged = () => {
        if (state.lastAssistantReply && state.speaking) {
          updateButtons();
        }
      };
    }

    window.addEventListener("resize", positionMenu);
    window.addEventListener("scroll", positionMenu, true);
    window.addEventListener("beforeunload", () => {
      stopSpeaking();
      stopActiveStream();
      cleanupAudioGraph();
      cleanupRecognition();
    });
  }

  function init() {
    els.trigger = $("dashboardAssistantButton");
    els.menu = $("dashboardAssistantMenu");
    els.close = $("dashboardAssistantClose");
    els.reset = $("dashboardAssistantReset");
    els.configToggle = $("dashboardAssistantConfigToggle");
    els.status = $("dashboardAssistantStatus");
    els.config = $("dashboardAssistantConfig");
    els.notice = $("dashboardAssistantNotice");
    els.messages = $("dashboardAssistantMessages");
    els.composeForm = $("dashboardAssistantComposerForm");
    els.textInput = $("dashboardAssistantTextInput");
    els.send = $("dashboardAssistantSend");
    els.mic = $("dashboardAssistantMic");
    els.replay = $("dashboardAssistantReplay");
    els.voiceState = $("dashboardAssistantVoiceState");
    els.speakingIndicator = $("dashboardAssistantSpeaking");
    els.transcript = $("dashboardAssistantTranscript");
    els.tokenForm = $("dashboardAssistantTokenForm");
    els.providerSelect = $("dashboardAssistantProviderSelect");
    els.tokenLabel = $("dashboardAssistantTokenLabel");
    els.tokenInput = $("dashboardAssistantTokenInput");
    els.saveToken = $("dashboardAssistantSaveToken");
    els.tokenState = $("dashboardAssistantTokenState");
    els.deleteModal = $("dashboardAssistantDeleteModal");
    els.deleteClose = $("dashboardAssistantDeleteClose");
    els.deleteCancel = $("dashboardAssistantDeleteCancel");
    els.deleteConfirm = $("dashboardAssistantDeleteConfirm");
    els.deleteName = $("dashboardAssistantDeleteName");
    els.deleteSubtitle = $("dashboardAssistantDeleteSubtitle");

    if (!els.trigger || !els.menu) return;

    bindEvents();
    syncProviderUi();
    updateButtons();
    setVoiceState(
      supportsSpeechSynthesis()
        ? "Voice replies are on."
        : "Voice playback is not available in this browser."
    );
    setStatus("Assistant ready.", "idle");
    if (getMicrophoneCapabilityIssue()) {
      setNotice(getMicrophoneCapabilityIssue());
    }
  }

  document.addEventListener("DOMContentLoaded", init);
})();
