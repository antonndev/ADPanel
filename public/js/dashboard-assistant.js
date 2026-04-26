(function () {
  const AudioContextClass = window.AudioContext || window.webkitAudioContext || null;
  const SpeechRecognitionClass = window.SpeechRecognition || window.webkitSpeechRecognition || null;
  const TARGET_PCM_SAMPLE_RATE = 16000;
  const ACCESSIBLE_SERVER_CACHE_TTL_MS = 5 * 60 * 1000;
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
    isAdmin: false,
    allowNormalUsers: false,
    configured: false,
    actionTokens: {},
    savingAccessSetting: false,
    tokenFieldUsesStoredValue: false,
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
    accessibleServerDirectory: [],
    accessibleServerDirectoryFetchedAt: 0,
    accessibleServerDirectoryPromise: null,
  };

  const els = {};

  function $(id) {
    return document.getElementById(id);
  }

  function getProviderMeta(providerId) {
    return ASSISTANT_PROVIDER_META[String(providerId || "").trim().toLowerCase()] || ASSISTANT_PROVIDER_META.groq;
  }

  function getSelectedProviderState(providerId = state.provider) {
    return state.providers.find((item) => item && item.id === providerId) || null;
  }

  function getStoredTokenFieldValue(providerId = state.provider) {
    const providerState = getSelectedProviderState(providerId);
    if (!providerState?.configured) return "";
    return String(providerState.obscuredValue || "");
  }

  function populateStoredTokenField(providerId = state.provider) {
    if (!els.tokenInput) return;
    const nextValue = getStoredTokenFieldValue(providerId);
    els.tokenInput.value = nextValue;
    state.tokenFieldUsesStoredValue = !!nextValue;
    els.tokenInput.dataset.storedValue = state.tokenFieldUsesStoredValue ? "1" : "";
  }

  function clearStoredTokenFieldIfNeeded() {
    if (!els.tokenInput || !state.tokenFieldUsesStoredValue) return;
    els.tokenInput.value = "";
    state.tokenFieldUsesStoredValue = false;
    els.tokenInput.dataset.storedValue = "";
  }

  function syncStoredTokenField(force = false) {
    if (!els.tokenInput) return;
    const preserveTypedValue = !force
      && document.activeElement === els.tokenInput
      && !state.tokenFieldUsesStoredValue
      && !!String(els.tokenInput.value || "").trim();
    if (preserveTypedValue) return;
    populateStoredTokenField(state.provider);
  }

  function updateTokenStateText() {
    if (!els.tokenState) return;
    const selectedProvider = getSelectedProviderState(state.provider);
    els.tokenState.textContent = selectedProvider?.configured
      ? `${getProviderMeta(state.provider).label} key saved.`
      : getProviderMeta(state.provider).emptyText;
  }

  function syncAssistantAccessUi() {
    if (!els.userAccessToggle) return;
    els.userAccessToggle.checked = !!state.allowNormalUsers;
    els.userAccessToggle.disabled = !state.canConfigure || state.savingAccessSetting;
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

  function syncAssistantLayout() {
    const compactSetup = !state.configured;
    const showBlockedEmptyState = compactSetup && !state.canConfigure;
    const showAdminSetupOnly = compactSetup && state.canConfigure;
    const showProviderOnly = !!els.config && !els.config.hidden;

    if (els.menu) {
      els.menu.classList.toggle("dashboard-assistant-menu--setup", compactSetup);
    }

    if (els.messages) {
      els.messages.hidden = showAdminSetupOnly || showProviderOnly;
    }

    if (els.composer) {
      els.composer.hidden = compactSetup || showProviderOnly;
    }

    if (els.reset) {
      els.reset.hidden = compactSetup;
    }

    if (els.notice) {
      if (showBlockedEmptyState) {
        els.notice.hidden = true;
        els.notice.textContent = "";
      } else if (showProviderOnly) {
        els.notice.hidden = true;
      } else if (String(els.notice.textContent || "").trim()) {
        els.notice.hidden = false;
      }
    }
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

    syncAssistantLayout();

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

  function getAssistantCopyButtonMarkup(copied = false) {
    return copied
      ? '<i class="fa-solid fa-check"></i><span>Copied</span>'
      : '<i class="fa-regular fa-copy"></i><span>Copy</span>';
  }

  function setAssistantCopyButtonState(button, copied = false) {
    if (!(button instanceof HTMLElement)) return;
    button.classList.toggle("is-copied", !!copied);
    button.innerHTML = getAssistantCopyButtonMarkup(copied);
  }

  function captureDocumentSelection() {
    const selection = typeof window.getSelection === "function" ? window.getSelection() : null;
    if (!selection || typeof selection.rangeCount !== "number") {
      return { selection: null, ranges: [] };
    }

    const ranges = [];
    for (let index = 0; index < selection.rangeCount; index += 1) {
      try {
        const range = selection.getRangeAt(index);
        if (range) {
          ranges.push(range.cloneRange());
        }
      } catch {
      }
    }

    return { selection, ranges };
  }

  function restoreDocumentSelection(snapshot) {
    const selection = snapshot?.selection;
    const ranges = Array.isArray(snapshot?.ranges) ? snapshot.ranges : [];
    if (!selection || typeof selection.removeAllRanges !== "function") {
      return;
    }

    try {
      selection.removeAllRanges();
      ranges.forEach((range) => selection.addRange(range));
    } catch {
    }
  }

  function restoreFocusedElement(element) {
    if (!(element instanceof HTMLElement) || typeof element.focus !== "function") {
      return;
    }

    try {
      element.focus({ preventScroll: true });
    } catch {
      try {
        element.focus();
      } catch {
      }
    }
  }

  function copyTextWithExecCommand(value) {
    if (!document?.body || typeof document.execCommand !== "function") {
      return false;
    }

    const selectionSnapshot = captureDocumentSelection();
    const activeElement = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const textarea = document.createElement("textarea");
    textarea.value = value;
    textarea.setAttribute("aria-hidden", "true");
    textarea.setAttribute("tabindex", "-1");
    textarea.style.position = "fixed";
    textarea.style.top = "0";
    textarea.style.left = "-9999px";
    textarea.style.width = "1px";
    textarea.style.height = "1px";
    textarea.style.opacity = "0";
    textarea.style.pointerEvents = "none";
    textarea.style.fontSize = "16px";
    textarea.style.padding = "0";
    textarea.style.border = "0";
    textarea.style.outline = "0";
    textarea.style.boxShadow = "none";
    textarea.style.background = "transparent";
    textarea.style.whiteSpace = "pre-wrap";
    textarea.style.userSelect = "text";
    textarea.style.webkitUserSelect = "text";

    document.body.appendChild(textarea);

    let copied = false;
    try {
      textarea.focus({ preventScroll: true });
    } catch {
      textarea.focus();
    }
    textarea.select();
    textarea.setSelectionRange(0, textarea.value.length);

    try {
      copied = document.execCommand("copy");
    } catch {
      copied = false;
    } finally {
      textarea.remove();
      restoreDocumentSelection(selectionSnapshot);
      restoreFocusedElement(activeElement);
    }

    if (copied) {
      return true;
    }

    const selectionFallback = captureDocumentSelection();
    const activeFallback = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const copyNode = document.createElement("div");
    copyNode.textContent = value;
    copyNode.setAttribute("aria-hidden", "true");
    copyNode.setAttribute("tabindex", "-1");
    copyNode.contentEditable = "true";
    copyNode.style.position = "fixed";
    copyNode.style.top = "0";
    copyNode.style.left = "-9999px";
    copyNode.style.opacity = "0";
    copyNode.style.pointerEvents = "none";
    copyNode.style.whiteSpace = "pre-wrap";
    copyNode.style.fontSize = "16px";
    copyNode.style.userSelect = "text";
    copyNode.style.webkitUserSelect = "text";

    document.body.appendChild(copyNode);

    try {
      const range = document.createRange();
      range.selectNodeContents(copyNode);
      const selection = typeof window.getSelection === "function" ? window.getSelection() : null;
      if (selection) {
        selection.removeAllRanges();
        selection.addRange(range);
      }
      copyNode.focus({ preventScroll: true });
      copied = document.execCommand("copy");
    } catch {
      copied = false;
    } finally {
      copyNode.remove();
      restoreDocumentSelection(selectionFallback);
      restoreFocusedElement(activeFallback);
    }

    return copied;
  }

  async function copyAssistantText(text, button) {
    const value = String(text || "").trim();
    if (!value) return false;

    let copied = copyTextWithExecCommand(value);

    if (!copied && navigator.clipboard?.writeText) {
      try {
        await navigator.clipboard.writeText(value);
        copied = true;
      } catch {
      }
    }

    if (button) {
      setAssistantCopyButtonState(button, copied);
      if (button._assistantCopyTimer) {
        window.clearTimeout(button._assistantCopyTimer);
      }
      button._assistantCopyTimer = window.setTimeout(() => {
        setAssistantCopyButtonState(button, false);
      }, copied ? 1800 : 1200);
    }

    setStatus(copied ? "Reply copied." : "Could not copy that reply.", copied ? "idle" : "warning");
    return copied;
  }

  function appendAssistantBubbleContent(bubble, text) {
    const content = String(text || "");
    const urlPattern = /https?:\/\/[^\s]+/g;
    let lastIndex = 0;
    let matched = false;

    content.replace(urlPattern, (match, offset) => {
      matched = true;
      if (offset > lastIndex) {
        bubble.appendChild(document.createTextNode(content.slice(lastIndex, offset)));
      }

      const link = document.createElement("a");
      link.href = match;
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      link.className = "dashboard-assistant-message-link";
      link.textContent = match;
      bubble.appendChild(link);

      lastIndex = offset + match.length;
      return match;
    });

    if (!matched) {
      bubble.textContent = content;
      return;
    }

    if (lastIndex < content.length) {
      bubble.appendChild(document.createTextNode(content.slice(lastIndex)));
    }
  }

  function createMessageNode(message) {
    const role = message.role === "user" ? "user" : "assistant";
    const wrapper = document.createElement("article");
    wrapper.className = `dashboard-assistant-message dashboard-assistant-message--${role}`;

    const bubble = document.createElement("div");
    bubble.className = "dashboard-assistant-message-bubble";
    const bubbleText = String(message.content || "").trim() || (role === "assistant" ? "Done." : "");
    appendAssistantBubbleContent(bubble, bubbleText);
    if (role === "assistant") {
      bubble.title = "Select text or use copy.";
    }
    wrapper.appendChild(bubble);

    const metaRow = document.createElement("div");
    metaRow.className = "dashboard-assistant-message-meta-row";

    const meta = document.createElement("div");
    meta.className = "dashboard-assistant-message-meta";
    const createdAt = message.created_at ? new Date(message.created_at) : null;
    meta.textContent = createdAt && !Number.isNaN(createdAt.getTime())
      ? `${role === "user" ? "You said" : "Assistant"} • ${createdAt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`
      : role === "user" ? "You said" : "Assistant";
    metaRow.appendChild(meta);

    if (role === "assistant" && bubbleText) {
      const copyButton = document.createElement("button");
      copyButton.type = "button";
      copyButton.className = "dashboard-assistant-message-copy";
      copyButton.setAttribute("aria-label", "Copy assistant reply");
      setAssistantCopyButtonState(copyButton, false);
      copyButton.addEventListener("click", async (event) => {
        event.preventDefault();
        event.stopPropagation();
        await copyAssistantText(bubbleText, copyButton);
      });
      metaRow.appendChild(copyButton);
    }

    wrapper.appendChild(metaRow);

    return wrapper;
  }

  function renderMessages(messages) {
    if (!els.messages) return;
    els.messages.innerHTML = "";

    const blockedByMissingProvider = !state.configured && !state.canConfigure;
    const sourceMessages = blockedByMissingProvider ? [] : (Array.isArray(messages) ? messages : []);
    const visibleMessages = sourceMessages.filter((message) =>
      message && (message.role === "user" || message.role === "assistant")
    );

    const lastAssistant = [...visibleMessages].reverse().find((message) => message.role === "assistant");
    state.lastAssistantReply = lastAssistant ? String(lastAssistant.content || "").trim() : "";

    if (!visibleMessages.length) {
      const empty = document.createElement("div");
      empty.className = "dashboard-assistant-empty";
      const activeMeta = getProviderMeta(state.provider);
      empty.textContent = state.configured
        ? "Start a conversation."
        : state.canConfigure
          ? `Save a ${activeMeta.label} API key to unlock voice input, context-aware chat, and spoken replies.`
          : "Your admin didn't put an API Key.";
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

  function countAssistantMessagesInUi() {
    if (!els.messages) return 0;
    return els.messages.querySelectorAll(".dashboard-assistant-message--assistant").length;
  }

  function positionMenu() {
    if (!state.open || !els.menu || !els.trigger) return;
    const triggerRect = els.trigger.getBoundingClientRect();
    const preferredWidth = state.configured ? 580 : 430;
    const menuWidth = Math.min(preferredWidth, window.innerWidth - 24);
    const leftBias = 150;

    // Center on trigger, then keep the panel fully visible in viewport.
    const idealLeft = triggerRect.left + (triggerRect.width / 2) - (menuWidth / 2) - leftBias;
    const left = Math.max(12, Math.min(idealLeft, window.innerWidth - menuWidth - 12));

    els.menu.style.width = `${menuWidth}px`;
    els.menu.style.left = `${left}px`;
    els.menu.style.right = 'auto';
    els.menu.style.top = `${triggerRect.bottom + 20}px`;
    els.menu.style.bottom = 'auto';
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

  function getPreferredVoiceLangs() {
    const langs = [];
    const normalizedPreferred = String(state.preferredSpeechLang || "en-US").trim().toLowerCase();
    if (normalizedPreferred) langs.push(normalizedPreferred);
    if (normalizedPreferred.includes("-")) langs.push(normalizedPreferred.split("-")[0]);
    langs.push("en-us", "en-gb", "en");
    return Array.from(new Set(langs.filter(Boolean)));
  }

  function scoreAssistantVoice(voice) {
    const name = String(voice?.name || "").trim().toLowerCase();
    const lang = String(voice?.lang || "").trim().toLowerCase();
    let score = 0;

    const preferredLangs = getPreferredVoiceLangs();
    const exactIndex = preferredLangs.indexOf(lang);
    if (exactIndex >= 0) {
      score += 160 - (exactIndex * 12);
    } else {
      const partialIndex = preferredLangs.findIndex((candidate) => candidate && lang.startsWith(candidate));
      if (partialIndex >= 0) {
        score += 120 - (partialIndex * 10);
      }
    }

    if (/^en(?:-|$)/.test(lang)) score += 24;
    if (voice?.localService) score += 18;
    if (voice?.default) score += 10;

    if (/\bsiri\b/.test(name)) score += 280;
    if (/\bsamantha\b/.test(name)) score += 220;
    if (/\b(ava|allison|serena|karen|moira|tessa)\b/.test(name)) score += 190;
    if (/\b(jenny|aria)\b/.test(name)) score += 180;
    if (/\bdaniel\b/.test(name)) score += 150;
    if (/\bgoogle us english\b/.test(name)) score += 170;
    if (/\b(microsoft .+ online|natural|neural|enhanced|premium)\b/.test(name)) score += 70;

    if (/\b(whisper|zarvox|bad news|good news|bells|boing|bubble|cellos|fred|junior|pipe organ|superstar|trinoids)\b/.test(name)) {
      score -= 320;
    }

    return score;
  }

  function findPreferredVoice() {
    if (!supportsSpeechSynthesis()) return null;
    const voices = window.speechSynthesis.getVoices();
    if (!Array.isArray(voices) || !voices.length) return null;

    return voices
      .slice()
      .sort((left, right) => scoreAssistantVoice(right) - scoreAssistantVoice(left))[0] || null;
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
    utterance.rate = 0.96;
    utterance.pitch = 1.02;
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
    if (Object.prototype.hasOwnProperty.call(data || {}, "allowNormalUsers")) {
      state.allowNormalUsers = !!data.allowNormalUsers;
    }
    if (data.user) {
      state.isAdmin = !!data.user.admin;
    }
    state.configured = !!data.configured;
    state.provider = String(data.provider || state.provider || "groq").trim().toLowerCase() || "groq";
    state.providers = Array.isArray(data.providers) ? data.providers : [];
    state.actionTokens = data.actionTokens || {};
    syncProviderUi();
    syncStoredTokenField(true);
    updateTokenStateText();
    syncAssistantAccessUi();

    if (els.configToggle) {
      els.configToggle.hidden = !state.canConfigure || !state.configured;
    }

    if (els.config) {
      const shouldShowConfig = state.canConfigure && !state.configured;
      els.config.hidden = !shouldShowConfig;
    }

    const capabilityIssue = getMicrophoneCapabilityIssue();
    if (!state.configured && !state.canConfigure) {
      setNotice("");
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
    warmAccessibleServerDirectory();
    updateButtons();
    if (state.open) {
      positionMenu();
    }
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

    const token = state.tokenFieldUsesStoredValue ? "" : els.tokenInput.value.trim();
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
      state.tokenFieldUsesStoredValue = false;
      els.tokenInput.value = "";
      await ensureBootstrap(true);
      setStatus(token ? `${providerMeta.label} API key saved.` : `${providerMeta.label} is now active.`, "idle");
    } catch (error) {
      setStatus(error.message || "Failed to save the assistant provider key.", "error");
    } finally {
      if (els.saveToken) els.saveToken.disabled = false;
    }
  }

  async function saveAssistantAccessSetting(enabled) {
    const previousValue = !!state.allowNormalUsers;
    state.allowNormalUsers = !!enabled;
    state.savingAccessSetting = true;
    syncAssistantAccessUi();

    try {
      setStatus("Saving assistant access...", "working");
      await fetchJson("/api/dashboard-assistant/access", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-action-token": state.actionTokens?.updateAccess || "",
        },
        body: JSON.stringify({ allowNormalUsers: !!enabled }),
      });
      setStatus(
        enabled
          ? "Normal users can now access Assistant."
          : "Assistant access is now limited to admins.",
        "idle"
      );
    } catch (error) {
      state.allowNormalUsers = previousValue;
      syncAssistantAccessUi();
      setStatus(error.message || "Failed to save assistant access.", "error");
    } finally {
      state.savingAccessSetting = false;
      syncAssistantAccessUi();
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
        allowNormalUsers: state.allowNormalUsers,
        provider: state.provider,
        providers: state.providers,
        chat: data.chat,
        messages: data.messages || [],
        actionTokens: state.actionTokens,
      });
      setStatus("New voice chat ready.", "idle");
    } catch (error) {
      setStatus(error.message || "Failed to reset the assistant chat.", "error");
    } finally {
      if (els.reset) els.reset.disabled = false;
    }
  }

  function normalizeAssistantPowerAction(value) {
    const action = String(value || "").trim().toLowerCase();
    return ["start", "stop", "restart", "kill"].includes(action) ? action : "";
  }

  function capitalizeText(value) {
    const text = String(value || "");
    return text ? `${text.charAt(0).toUpperCase()}${text.slice(1)}` : "";
  }

  function getPowerActionWord(action, tense = "base") {
    const normalized = normalizeAssistantPowerAction(action);
    if (!normalized) return "update";

    if (tense === "progressive") {
      if (normalized === "start") return "starting";
      if (normalized === "stop") return "stopping";
      if (normalized === "restart") return "restarting";
      if (normalized === "kill") return "killing";
    }

    if (tense === "past") {
      if (normalized === "start") return "started";
      if (normalized === "stop") return "stopped";
      if (normalized === "restart") return "restarted";
      if (normalized === "kill") return "killed";
    }

    return normalized;
  }

  function getPowerTargetLabel(target) {
    return String(target?.displayName || target?.server || target?.name || "that server").trim() || "that server";
  }

  async function requestAssistantClientFollowUp(message, options = {}) {
    const summary = String(message || "").trim();
    if (!summary || !state.chatId || !state.configured) return "";

    try {
      const data = await fetchJson("/api/dashboard-assistant/client-follow-up", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chatId: state.chatId,
          summary,
          sourceType: String(options.sourceType || "").trim(),
          context: options.context && typeof options.context === "object" ? options.context : null,
          skipAi: options.skipAi !== false,
        }),
      });
      return String(data?.reply || "").trim();
    } catch {
      return "";
    }
  }

  function appendAssistantFollowUpToUi(message, options = {}) {
    const text = String(message || "").trim();
    if (!text) return "";

    appendMessageToUi({ role: "assistant", content: text, created_at: new Date().toISOString() });
    state.lastAssistantReply = text;
    if (options.speak) {
      const spoken = speakAssistantReply(text);
      if (!spoken) {
        setVoiceState("Voice playback could not start automatically.");
      }
    }

    return text;
  }

  async function appendAssistantFollowUp(message, options = {}) {
    const text = String(message || "").trim();
    if (!text) return "";

    if (options.ai === true && state.chatId && state.configured) {
      const aiReply = await requestAssistantClientFollowUp(text, {
        sourceType: options.sourceType,
        context: options.context,
        skipAi: false,
      });

      if (aiReply) {
        return appendAssistantFollowUpToUi(aiReply, options);
      }
    }

    const localText = appendAssistantFollowUpToUi(text, options);
    void requestAssistantClientFollowUp(localText, {
      sourceType: options.sourceType,
      context: options.context,
      skipAi: true,
    });
    return localText;
  }

  function messageLooksLikeAssistantServerPrefetchIntent(message) {
    const text = String(message || "").trim().toLowerCase();
    if (!text) return false;
    if (/\b(open|show|take me|go to|goto|navigate|bring me|launch|new tab|tab|pop ?up)\b/.test(text)) return true;
    if (/\b(start|stop|restart|kill|power)\b/.test(text)) return true;
    if (/\b(console|logs?|crash|crashed|startup|started|healthy|health|running well|running okay|diagnos)\b/.test(text)) return true;
    if (/\b(limits?|quota|ram|memory|cpu|disk|storage|headroom|remaining)\b/.test(text)) return true;
    if (/\b(server|servers|bot|instance|backups?|files?|activity|scheduler|resources?)\b/.test(text)) return true;
    return false;
  }

  function buildAssistantAbsoluteUrl(url) {
    const targetUrl = String(url || "").trim();
    if (!targetUrl) return "";
    try {
      return new URL(targetUrl, window.location.origin).toString();
    } catch {
      return targetUrl;
    }
  }

  function assistantActionMatches(action, types) {
    const value = String(action?.type || "").trim().toLowerCase();
    return Array.isArray(types) && types.includes(value);
  }

  function isExecutionClientAction(action) {
    if (
      action?.type === "power_servers"
      || action?.type === "assistant_resolve_power_server"
      || action?.type === "resolve_power_server"
      || action?.type === "assistant_power_accessible_servers"
      || action?.type === "power_accessible_servers"
    ) return true;
    return assistantActionMatches(action, [
      "find_busiest_node",
      "assistant_find_busiest_node",
      "busiest_node",
      "find_busiest_server",
      "assistant_find_busiest_server",
      "busiest_server",
      "check_server_console",
      "assistant_check_server_console",
      "server_console_check",
      "check_server_limits",
      "assistant_check_server_limits",
      "server_limits_check",
      "open_server_page",
      "assistant_open_server_page",
      "open_settings_destination",
      "assistant_open_settings_destination",
      "diagnose_server",
      "assistant_diagnose_server",
      "server_diagnosis",
      "update_minecraft_server_property",
      "assistant_update_minecraft_server_property",
      "update_minecraft_server_properties",
      "assistant_update_minecraft_server_properties",
      "batch_update_server_properties",
      "batch_update_minecraft_property",
    ]);
  }

  function normalizeAssistantServerTarget(target) {
    if (!target) return null;
    const server = typeof target === "string"
      ? String(target).trim()
      : String(target.server || target.name || "").trim();
    if (!server) return null;

    return {
      server,
      name: server,
      displayName: String(target.displayName || target.label || server).trim() || server,
      template: String(target.template || target.templateId || "").trim().toLowerCase() || null,
      nodeId: target.nodeId || null,
      hostPort: Number.isFinite(Number(target.hostPort)) ? Number(target.hostPort) : null,
    };
  }

  function normalizeAssistantServerTargets(targets) {
    const list = Array.isArray(targets) ? targets : (targets ? [targets] : []);
    const seen = new Set();
    const normalized = [];

    list.forEach((target) => {
      const entry = normalizeAssistantServerTarget(target);
      if (!entry) return;
      const key = entry.server.toLowerCase();
      if (seen.has(key)) return;
      seen.add(key);
      normalized.push(entry);
    });

    return normalized;
  }

  function storeAccessibleServerDirectory(data) {
    const serverEntries = Array.isArray(data?.servers) ? data.servers : [];
    const normalizedDirectory = serverEntries.length
      ? serverEntries.map((entry) => normalizeAssistantServerTarget(entry)).filter(Boolean)
      : normalizeAssistantServerTargets(Array.isArray(data?.names) ? data.names : []);

    state.accessibleServerDirectory = normalizedDirectory;
    state.accessibleServerDirectoryFetchedAt = Date.now();
    return normalizedDirectory.slice();
  }

  function getCachedAccessibleServerDirectory() {
    if (!Array.isArray(state.accessibleServerDirectory) || !state.accessibleServerDirectory.length) {
      return [];
    }
    if ((Date.now() - Number(state.accessibleServerDirectoryFetchedAt || 0)) > ACCESSIBLE_SERVER_CACHE_TTL_MS) {
      return [];
    }
    return state.accessibleServerDirectory.slice();
  }

  async function ensureAccessibleServerDirectory(options = {}) {
    const force = !!options.force;
    const cachedDirectory = !force ? getCachedAccessibleServerDirectory() : [];
    if (cachedDirectory.length) {
      return cachedDirectory;
    }

    if (state.accessibleServerDirectoryPromise && !force) {
      return state.accessibleServerDirectoryPromise;
    }

    const requestPromise = (async () => {
      const data = await fetchJson("/api/my-servers", { cache: "no-store" });
      return storeAccessibleServerDirectory(data);
    })();

    state.accessibleServerDirectoryPromise = requestPromise;
    try {
      return await requestPromise;
    } finally {
      if (state.accessibleServerDirectoryPromise === requestPromise) {
        state.accessibleServerDirectoryPromise = null;
      }
    }
  }

  function warmAccessibleServerDirectory() {
    void ensureAccessibleServerDirectory().catch(() => {});
  }

  function normalizeAssistantNodeTargets(targets) {
    const list = Array.isArray(targets) ? targets : (targets ? [targets] : []);
    const seen = new Set();
    const normalized = [];

    list.forEach((target) => {
      const id = typeof target === "string"
        ? String(target).trim()
        : String(target.id || target.uuid || target.nodeId || target.name || "").trim();
      if (!id) return;
      const key = id.toLowerCase();
      if (seen.has(key)) return;
      seen.add(key);
      normalized.push({
        id,
        name: String(target?.name || id).trim() || id,
      });
    });

    return normalized;
  }

  function normalizePercent(value) {
    const number = Number(value);
    if (!Number.isFinite(number)) return null;
    return Math.max(0, Math.min(100, Math.round(number)));
  }

  function normalizeStatusLabel(value) {
    return String(value || "").trim().toLowerCase();
  }

  function stripAssistantLookupDiacritics(value) {
    return String(value || "").normalize("NFKD").replace(/[\u0300-\u036f]/g, "");
  }

  function normalizeAssistantLookupPhrase(value) {
    return stripAssistantLookupDiacritics(value)
      .toLowerCase()
      .replace(/[`"'“”‘’]/g, " ")
      .replace(/[_./-]+/g, " ")
      .replace(/[^a-z0-9 ]+/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function compactAssistantLookupPhrase(value) {
    return normalizeAssistantLookupPhrase(value).replace(/\s+/g, "");
  }

  function stripAssistantServerLookupFiller(value) {
    return normalizeAssistantLookupPhrase(value)
      .replace(/\b(?:the|my|our|please|pls|now|right|right now|server|servers|serverul|serverele|servere|srv|named|called|bot|instance|start|run|boot|bring|spin|restart|reboot|stop|shutdown|shut|kill|terminate|turn|power|on|off|up|down|for|on|la|pe)\b/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function addAssistantLookupForms(set, value) {
    const raw = String(value || "").trim().toLowerCase();
    if (raw) set.add(raw);

    const normalized = normalizeAssistantLookupPhrase(value);
    if (normalized) set.add(normalized);

    const compact = compactAssistantLookupPhrase(value);
    if (compact) set.add(compact);
  }

  function buildAssistantRequestedServerForms(requestedServer) {
    const forms = new Set();
    const raw = String(requestedServer || "").trim();
    if (!raw) return forms;

    addAssistantLookupForms(forms, raw);
    addAssistantLookupForms(forms, stripAssistantServerLookupFiller(raw));

    const quoted = raw.match(/["'`“”]([^"'`“”]{1,120})["'`“”]/);
    if (quoted) {
      addAssistantLookupForms(forms, quoted[1]);
      addAssistantLookupForms(forms, stripAssistantServerLookupFiller(quoted[1]));
    }

    const wrappedMatches = Array.from(raw.matchAll(/[\(\[]([^()\[\]]{1,120})[\)\]]/g));
    for (const match of wrappedMatches) {
      addAssistantLookupForms(forms, match[1]);
      addAssistantLookupForms(forms, stripAssistantServerLookupFiller(match[1]));
    }

    return forms;
  }

  function buildAssistantServerAliases(entry) {
    const aliases = new Set();
    for (const value of [entry?.name, entry?.displayName, entry?.id]) {
      const cleaned = String(value || "").trim();
      if (!cleaned) continue;
      aliases.add(cleaned);
      const spaced = cleaned.replace(/[_./-]+/g, " ").replace(/\s+/g, " ").trim();
      if (spaced) aliases.add(spaced);
    }
    return Array.from(aliases);
  }

  function scoreAssistantServerAliasMatch(alias, requestedForms) {
    const aliasRaw = String(alias || "").trim().toLowerCase();
    const aliasNormalized = normalizeAssistantLookupPhrase(alias);
    const aliasCompact = compactAssistantLookupPhrase(alias);
    const aliasTokens = new Set(aliasNormalized.split(" ").filter(Boolean));
    let best = 0;

    for (const requested of Array.from(requestedForms || [])) {
      const needle = String(requested || "").trim().toLowerCase();
      if (!needle) continue;

      if (needle === aliasRaw || needle === aliasNormalized || needle === aliasCompact) {
        return 1000;
      }

      const needleNormalized = normalizeAssistantLookupPhrase(needle);
      const needleCompact = compactAssistantLookupPhrase(needle);
      if (!needleNormalized && !needleCompact) continue;

      if (needleCompact && aliasCompact && (aliasCompact.startsWith(needleCompact) || needleCompact.startsWith(aliasCompact))) {
        best = Math.max(best, 260 - Math.abs(aliasCompact.length - needleCompact.length));
      }
      if (needleCompact && aliasCompact && (aliasCompact.includes(needleCompact) || needleCompact.includes(aliasCompact))) {
        best = Math.max(best, 210 - Math.abs(aliasCompact.length - needleCompact.length));
      }
      if (needleNormalized && aliasNormalized && (aliasNormalized.startsWith(needleNormalized) || needleNormalized.startsWith(aliasNormalized))) {
        best = Math.max(best, 180 - Math.abs(aliasNormalized.length - needleNormalized.length));
      }
      if (needleNormalized && aliasNormalized && (aliasNormalized.includes(needleNormalized) || needleNormalized.includes(aliasNormalized))) {
        best = Math.max(best, 150 - Math.abs(aliasNormalized.length - needleNormalized.length));
      }

      const needleTokens = needleNormalized.split(" ").filter(Boolean);
      const overlap = needleTokens.filter((token) => aliasTokens.has(token)).length;
      if (overlap > 0) {
        best = Math.max(best, overlap * 40 - Math.max(0, aliasTokens.size - overlap) * 4);
      }
    }

    return best;
  }

  function resolveAssistantServerFromDirectory(servers, requestedServer) {
    const requested = String(requestedServer || "").trim();
    if (!requested) {
      return { error: "A server name is required." };
    }

    const requestedForms = buildAssistantRequestedServerForms(requested);
    const scoredEntries = (Array.isArray(servers) ? servers : [])
      .filter((entry) => entry?.name)
      .map((entry) => {
        const aliases = buildAssistantServerAliases(entry);
        const score = aliases.reduce((best, alias) => Math.max(best, scoreAssistantServerAliasMatch(alias, requestedForms)), 0);
        return { entry, score };
      });

    const exactMatches = scoredEntries.filter((item) => item.score >= 1000).map((item) => item.entry);
    if (exactMatches.length === 1) {
      return { entry: exactMatches[0] };
    }
    if (exactMatches.length > 1) {
      return {
        error: "Multiple servers matched that name.",
        candidates: exactMatches.map((entry) => entry.displayName || entry.name),
      };
    }

    const partialMatches = scoredEntries
      .filter((item) => item.score >= 120)
      .sort((left, right) => right.score - left.score);
    if (partialMatches.length === 1) {
      return { entry: partialMatches[0].entry };
    }
    if (partialMatches.length > 1) {
      if (partialMatches[0] && partialMatches[1] && partialMatches[0].score >= partialMatches[1].score + 50) {
        return { entry: partialMatches[0].entry };
      }
      return {
        error: "The server name is ambiguous.",
        candidates: partialMatches.slice(0, 5).map((item) => item.entry.displayName || item.entry.name),
      };
    }

    return {
      error: "I could not match that server name.",
      candidates: scoredEntries
        .filter((item) => item.score > 0)
        .sort((left, right) => right.score - left.score)
        .slice(0, 5)
        .map((item) => item.entry.displayName || item.entry.name),
    };
  }

  function getReadableTargetLabel(target) {
    return String(target?.displayName || target?.server || target?.name || "that target").trim() || "that target";
  }

  async function mapWithConcurrency(items, limit, mapper) {
    const list = Array.isArray(items) ? items : [];
    if (!list.length) return [];

    const maxConcurrency = Math.max(1, Math.min(list.length, Number(limit) || 1));
    const results = new Array(list.length);
    let nextIndex = 0;

    async function worker() {
      while (true) {
        const currentIndex = nextIndex;
        nextIndex += 1;
        if (currentIndex >= list.length) return;
        results[currentIndex] = await mapper(list[currentIndex], currentIndex);
      }
    }

    await Promise.all(Array.from({ length: maxConcurrency }, () => worker()));
    return results;
  }

  async function fetchAccessibleServerNames() {
    const directory = await ensureAccessibleServerDirectory();
    return directory
      .map((entry) => String(entry?.server || entry?.name || "").trim())
      .filter(Boolean);
  }

  async function fetchAccessibleServerDirectory() {
    return ensureAccessibleServerDirectory();
  }

  async function fetchKnownNodes() {
    const data = await fetchJson("/api/nodes", { cache: "no-store" });
    return normalizeAssistantNodeTargets(Array.isArray(data?.nodes) ? data.nodes : []);
  }

  async function fetchAssistantServerStatuses(names) {
    const cleanNames = [...new Set((Array.isArray(names) ? names : []).map((name) => String(name || "").trim()).filter(Boolean))];
    if (!cleanNames.length) return {};

    const data = await fetchJson("/api/servers/statuses", {
      method: "POST",
      cache: "no-store",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ names: cleanNames }),
    });
    return data?.statuses && typeof data.statuses === "object" ? data.statuses : {};
  }

  async function fetchAssistantSingleServerStatus(serverName) {
    const cleanName = String(serverName || "").trim();
    if (!cleanName) return null;
    return fetchJson(`/api/server/${encodeURIComponent(cleanName)}/node-status`, { cache: "no-store" });
  }

  async function fetchAssistantCachedServerStatus(serverName) {
    const cleanName = String(serverName || "").trim();
    if (!cleanName) return null;
    return fetchJson(`/api/server/${encodeURIComponent(cleanName)}/status`, { cache: "no-store" });
  }

  function normalizeAssistantServerPageSectionValue(value) {
    const text = normalizeAssistantLookupPhrase(value);
    if (!text) return "console";
    if (/\b(backups?|snapshots?)\b/.test(text)) return "backups";
    if (/\b(files?|file manager|explorer|folders?|directory|directories)\b/.test(text)) return "files";
    if (/\b(activity|history|events?)\b/.test(text)) return "activity";
    if (/\b(scheduler|schedule|schedules|tasks?)\b/.test(text)) return "scheduler";
    if (/\b(store|plugins?|versions?|marketplace)\b/.test(text)) return "store";
    if (/\b(resource(?:s| stats?)?|cpu|ram|memory|disk|storage)\b/.test(text)) return "resource_stats";
    if (/\b(subdomains?|domains?)\b/.test(text)) return "subdomains";
    if (/\b(reinstall|rebuild|reimage|re image)\b/.test(text)) return "reinstall";
    if (/\b(ai help|assistant help)\b/.test(text)) return "ai_help";
    if (/\b(info|overview|details?)\b/.test(text)) return "info";
    return "console";
  }

  function cleanAssistantOptimisticServerCandidate(value) {
    const candidate = String(value || "")
      .trim()
      .replace(/^[`"'“”‘’([{<\s]+/g, "")
      .replace(/[`"'“”‘’)\]}>.,!?;:\s]+$/g, "")
      .replace(/\s+/g, " ")
      .trim();
    if (!candidate) return "";
    if (/^(?:open|show|goto|go to|navigate|launch|new tab|tab|popup|pop up|menu|page|screen|section|area|window|server|servers|bot|instance)$/i.test(candidate)) {
      return "";
    }
    return candidate;
  }

  function deriveOptimisticAssistantServerRouteCandidate(requestedServer, section) {
    const raw = String(requestedServer || "").trim();
    if (!raw) return "";

    const candidates = [];
    const seen = new Set();
    const pushCandidate = (value) => {
      const candidate = cleanAssistantOptimisticServerCandidate(value);
      if (!candidate) return;
      const key = candidate.toLowerCase();
      if (seen.has(key)) return;
      seen.add(key);
      candidates.push(candidate);
    };

    const quoted = raw.match(/["'`“”]([^"'`“”]{1,120})["'`“”]/);
    if (quoted?.[1]) pushCandidate(quoted[1]);

    const wrappedMatches = Array.from(raw.matchAll(/[\(\[]([^()\[\]]{1,120})[\)\]]/g));
    wrappedMatches.forEach((match) => pushCandidate(match?.[1]));

    const tailMatch = raw.match(/\b(?:in|for|on|at)\s+([a-z0-9][a-z0-9 _.-]{0,80})$/i);
    if (tailMatch?.[1]) pushCandidate(tailMatch[1]);

    const labeledMatch = raw.match(/\b(?:server|bot|instance)\s+([a-z0-9][a-z0-9 _.-]{0,80})$/i);
    if (labeledMatch?.[1]) pushCandidate(labeledMatch[1]);

    const cleaned = raw
      .replace(/\b(?:open|show|take me|go to|goto|navigate|bring me|launch|new tab|tab|popup|pop up|menu|page|screen|section|area|window)\b/gi, " ")
      .replace(/\b(?:console|terminal|backups?|snapshots?|files?|activity|scheduler|tasks?|store|plugins?|versions?|resource(?:s| stats?)?|overview|info|details?|subdomains?|domains?|reinstall|ai help|assistant help)\b/gi, " ")
      .replace(/\b(?:the|my|our|please|pls|now|right|right now|server|servers|srv|named|called|bot|instance)\b/gi, " ")
      .replace(/\s+/g, " ")
      .trim();
    pushCandidate(cleaned);
    pushCandidate(raw);

    const normalizedSection = normalizeAssistantServerPageSectionValue(section);
    return candidates.find((candidate) => {
      const normalized = normalizeAssistantLookupPhrase(candidate);
      if (!normalized || normalized === normalizedSection) return false;
      if (/\b(?:open|show|goto|go to|navigate|bring me|launch|new tab|tab|popup|pop up|menu|page|screen|section|area|window|console|terminal|backups?|snapshots?|files?|activity|scheduler|tasks?|store|plugins?|versions?|resource(?:s| stats?)?|overview|info|details?|subdomains?|domains?|reinstall|ai help|assistant help)\b/.test(normalized)) {
        return false;
      }
      return normalized.split(" ").some(Boolean);
    }) || "";
  }

  function getAssistantServerPageSectionLabel(section) {
    const normalized = normalizeAssistantServerPageSectionValue(section);
    const labels = {
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
    };
    return labels[normalized] || "server page";
  }

  function normalizeAssistantSettingsActionValue(value) {
    const text = normalizeAssistantLookupPhrase(value);
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

  function inferAssistantSettingsPanelFromActionValue(action) {
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

  function normalizeAssistantSettingsPanelValue(value) {
    const text = normalizeAssistantLookupPhrase(value);
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

  function getAssistantSettingsPanelLabel(panel) {
    const normalized = normalizeAssistantSettingsPanelValue(panel);
    const labels = {
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
    };
    return labels[normalized] || "Settings";
  }

  function getAssistantSettingsActionLabel(action) {
    const normalized = normalizeAssistantSettingsActionValue(action);
    const labels = {
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
    };
    return labels[normalized] || "that settings tool";
  }

  function buildAssistantServerPageUrl(serverName, section) {
    const cleanName = String(serverName || "").trim();
    if (!cleanName) return "";
    const normalizedSection = normalizeAssistantServerPageSectionValue(section);
    const basePath = `/server/${encodeURIComponent(cleanName)}`;
    const queryActions = {
      console: "",
      info: "open-info",
      files: "open-files",
      activity: "open-activity",
      backups: "open-backups",
      scheduler: "open-scheduler",
      store: "open-store",
      resource_stats: "open-resource-stats",
      subdomains: "open-subdomains",
      reinstall: "open-reinstall",
      ai_help: "open-ai-help",
    };
    const assistantAction = queryActions[normalizedSection] || "";
    if (!assistantAction) return basePath;
    return `${basePath}?assistantAction=${encodeURIComponent(assistantAction)}`;
  }

  function appendAssistantNavigationLink(label, url, options = {}) {
    const safeLabel = String(label || "").trim() || "Open link";
    const absoluteUrl = buildAssistantAbsoluteUrl(url);
    if (!absoluteUrl) return false;

    const prefix = String(options.prefix || "").trim();
    const suffix = String(options.suffix || "").trim();
    const parts = [prefix, `${safeLabel}: ${absoluteUrl}`, suffix].filter(Boolean);
    const message = parts.join("\n");
    setStatus("Assistant prepared a link.", "idle");
    appendAssistantFollowUp(message, { speak: !!options.speak });
    return true;
  }

  function buildAssistantSettingsUrl(panel, action) {
    const normalizedAction = normalizeAssistantSettingsActionValue(action);
    const normalizedPanel = normalizeAssistantSettingsPanelValue(panel) || inferAssistantSettingsPanelFromActionValue(normalizedAction) || "preferences";
    const params = new URLSearchParams();
    params.set("panel", normalizedPanel);
    if (normalizedAction) {
      params.set("assistantAction", normalizedAction);
    }
    return `/settings?${params.toString()}`;
  }

  async function waitForAssistantPowerObservation(serverName, action, options = {}) {
    const target = String(serverName || "").trim();
    const normalizedAction = normalizeAssistantPowerAction(action);
    if (!target || !normalizedAction || normalizedAction === "restart") {
      return { confirmed: false, pending: true, contradictory: false, status: "" };
    }

    const attempts = Math.max(1, Number(options.attempts) || 4);
    const delayMs = Math.max(250, Number(options.delayMs) || 1200);
    let lastStatus = "";

    for (let attempt = 0; attempt < attempts; attempt += 1) {
      if (attempt > 0) {
        await new Promise((resolve) => window.setTimeout(resolve, delayMs));
      }
      try {
        const directStatus = await fetchAssistantSingleServerStatus(target);
        lastStatus = normalizeStatusLabel(directStatus?.status);
      } catch {
      }

      if (!lastStatus || lastStatus === "unknown") {
        try {
          const statuses = await fetchAssistantServerStatuses([target]);
          lastStatus = normalizeStatusLabel(statuses?.[target]?.status) || lastStatus;
        } catch {
        }
      }

      if ((normalizedAction === "start" && ["starting", "running", "online"].includes(lastStatus))
        || ((normalizedAction === "stop" || normalizedAction === "kill") && ["stopping", "stopped", "offline"].includes(lastStatus))) {
        return { confirmed: true, pending: false, contradictory: false, status: lastStatus };
      }
    }

    const pending = !lastStatus || ["unknown", "starting", "stopping"].includes(lastStatus);
    return {
      confirmed: false,
      pending,
      contradictory: !pending,
      status: lastStatus,
    };
  }

  function getAssistantPropertyValue(content, key) {
    const propertyKey = String(key || "").trim();
    if (!propertyKey) return "";
    const lines = String(content || "").split(/\r?\n/);
    for (const line of lines) {
      if (extractAssistantPropertiesKey(line) === propertyKey) {
        return extractAssistantPropertiesValue(line);
      }
    }
    return "";
  }

  function scoreAssistantNodeLoad(stats, metric = "balanced") {
    const cpu = normalizePercent(stats?.stats?.cpu?.percent);
    const ram = normalizePercent(stats?.stats?.ram?.percent);
    const disk = normalizePercent(stats?.stats?.disk?.percent);
    let score = (cpu || 0) * 0.55 + (ram || 0) * 0.3 + (disk || 0) * 0.15;
    if (metric === "cpu") score = cpu || 0;
    else if (metric === "memory") score = ram || 0;
    else if (metric === "disk") score = disk || 0;
    if (stats?.online) score += 8;
    return score;
  }

  function scoreAssistantServerLoad(status, metric = "balanced") {
    const stateLabel = normalizeStatusLabel(status?.status);
    const cpu = normalizePercent(status?.cpu);
    const memory = normalizePercent(status?.memory?.percent);
    const disk = normalizePercent(status?.disk?.percent);
    let score = (cpu || 0) * 0.62 + (memory || 0) * 0.28 + (disk || 0) * 0.1;
    if (metric === "cpu") score = cpu || 0;
    else if (metric === "memory") score = memory || 0;
    else if (metric === "disk") score = disk || 0;

    if (stateLabel === "running") score += 12;
    else if (stateLabel === "starting") score += 4;
    else if (["stopped", "offline", "unknown", "installing"].includes(stateLabel)) score -= 35;

    if (status?.nodeOnline === false) score -= 50;
    return score;
  }

  function normalizeAssistantMetricList(value) {
    const rawList = Array.isArray(value) ? value : [value];
    const seen = new Set();
    const metrics = [];

    rawList.forEach((entry) => {
      const metric = String(entry || "").trim().toLowerCase();
      if (!["cpu", "memory", "disk"].includes(metric) || seen.has(metric)) return;
      seen.add(metric);
      metrics.push(metric);
    });

    return metrics;
  }

  function formatAssistantMemoryAmount(valueMb) {
    const amount = Number(valueMb);
    if (!Number.isFinite(amount) || amount < 0) return null;
    if (amount >= 1024) {
      const gb = amount / 1024;
      return `${gb >= 10 ? Math.round(gb) : gb.toFixed(1)} GB`;
    }
    return `${Math.round(amount)} MB`;
  }

  function formatAssistantDiskAmount(valueGb) {
    const amount = Number(valueGb);
    if (!Number.isFinite(amount) || amount < 0) return null;
    return `${amount >= 10 ? Math.round(amount) : amount.toFixed(1)} GB`;
  }

  function buildAssistantNodeUsageDetail(nodeStats, includeMetrics = []) {
    const metrics = normalizeAssistantMetricList(includeMetrics);
    if (!metrics.length) return "";

    const parts = [];
    if (metrics.includes("cpu")) {
      const cpu = normalizePercent(nodeStats?.stats?.cpu?.percent);
      if (cpu !== null) parts.push(`${cpu}% CPU`);
    }
    if (metrics.includes("memory")) {
      const used = formatAssistantMemoryAmount(nodeStats?.stats?.ram?.usedMb);
      const total = formatAssistantMemoryAmount(nodeStats?.stats?.ram?.totalMb);
      const percent = normalizePercent(nodeStats?.stats?.ram?.percent);
      if (used && total) parts.push(`${used}/${total} RAM`);
      else if (percent !== null) parts.push(`${percent}% RAM`);
    }
    if (metrics.includes("disk")) {
      const used = formatAssistantDiskAmount(nodeStats?.stats?.disk?.usedGb);
      const total = formatAssistantDiskAmount(nodeStats?.stats?.disk?.totalGb);
      const percent = normalizePercent(nodeStats?.stats?.disk?.percent);
      if (used && total) parts.push(`${used}/${total} disk`);
      else if (percent !== null) parts.push(`${percent}% disk`);
    }

    return parts.join(", ");
  }

  function buildAssistantServerUsageDetail(status, includeMetrics = []) {
    const metrics = normalizeAssistantMetricList(includeMetrics);
    if (!metrics.length) return "";

    const parts = [];
    if (metrics.includes("cpu")) {
      const cpu = normalizePercent(status?.cpu);
      if (cpu !== null) parts.push(`${cpu}% CPU`);
    }
    if (metrics.includes("memory")) {
      const used = formatAssistantMemoryAmount(status?.memory?.used);
      const total = formatAssistantMemoryAmount(status?.memory?.total);
      const percent = normalizePercent(status?.memory?.percent);
      if (used && total) parts.push(`${used}/${total} RAM`);
      else if (percent !== null) parts.push(`${percent}% RAM`);
    }
    if (metrics.includes("disk")) {
      const used = formatAssistantDiskAmount(status?.disk?.used);
      const total = formatAssistantDiskAmount(status?.disk?.total);
      const percent = normalizePercent(status?.disk?.percent);
      if (used && total) parts.push(`${used}/${total} disk`);
      else if (percent !== null) parts.push(`${percent}% disk`);
    }

    return parts.join(", ");
  }

  function summarizeAssistantNodeLoad(nodeStats, metric = "balanced", includeMetrics = []) {
    const label = String(nodeStats?.nodeName || nodeStats?.nodeId || "That node").trim() || "That node";
    const cpu = normalizePercent(nodeStats?.stats?.cpu?.percent);
    const ram = normalizePercent(nodeStats?.stats?.ram?.percent);
    const disk = normalizePercent(nodeStats?.stats?.disk?.percent);
    const requestedMetrics = normalizeAssistantMetricList(includeMetrics);
    const details = buildAssistantNodeUsageDetail(
      nodeStats,
      metric === "cpu" ? requestedMetrics.filter((entry) => entry !== "cpu") : requestedMetrics
    );
    if (metric === "cpu" && cpu !== null) return `${label} is highest on CPU at ${cpu}%.${details ? ` It is using ${details}.` : ""}`;
    if (metric === "memory" && ram !== null) return `${label} is highest on RAM at ${ram}%.${details ? ` It is using ${details}.` : ""}`;
    if (metric === "disk" && disk !== null) return `${label} is highest on disk at ${disk}%.${details ? ` It is using ${details}.` : ""}`;

    const parts = [];
    if (cpu !== null) parts.push(`${cpu}% CPU`);
    if (ram !== null) parts.push(`${ram}% RAM`);
    if (disk !== null && disk >= 80) parts.push(`${disk}% disk`);
    const detailSuffix = buildAssistantNodeUsageDetail(nodeStats, requestedMetrics);
    if (detailSuffix) {
      return `${label} is busiest. It is using ${detailSuffix}.`;
    }
    if (parts.length) {
      return `${label} is busiest: ${parts.join(", ")}.`;
    }
    return `${label} looks like the busiest node right now.`;
  }

  function summarizeAssistantServerLoad(target, status, metric = "balanced", includeMetrics = []) {
    const label = getReadableTargetLabel(target);
    const cpu = normalizePercent(status?.cpu);
    const memory = normalizePercent(status?.memory?.percent);
    const disk = normalizePercent(status?.disk?.percent);
    const requestedMetrics = normalizeAssistantMetricList(includeMetrics);
    const details = buildAssistantServerUsageDetail(
      status,
      metric === "cpu" ? requestedMetrics.filter((entry) => entry !== "cpu") : requestedMetrics
    );
    if (metric === "cpu" && cpu !== null) return `${label} is highest on CPU at ${cpu}%.${details ? ` It is using ${details}.` : ""}`;
    if (metric === "memory" && memory !== null) return `${label} is highest on RAM at ${memory}%.${details ? ` It is using ${details}.` : ""}`;
    if (metric === "disk" && disk !== null) return `${label} is highest on disk at ${disk}%.${details ? ` It is using ${details}.` : ""}`;

    const parts = [];
    if (cpu !== null) parts.push(`${cpu}% CPU`);
    if (memory !== null) parts.push(`${memory}% RAM`);
    if (disk !== null && disk >= 80) parts.push(`${disk}% disk`);
    const detailSuffix = buildAssistantServerUsageDetail(status, requestedMetrics);
    if (detailSuffix) {
      return `${label} is busiest. It is using ${detailSuffix}.`;
    }
    if (parts.length) {
      return `${label} is busiest: ${parts.join(", ")}.`;
    }
    return `${label} looks like the busiest accessible server right now.`;
  }

  function mergeAssistantServerLimitStatus(liveStatus, cachedStatus) {
    const live = liveStatus && typeof liveStatus === "object" ? liveStatus : {};
    const cached = cachedStatus && typeof cachedStatus === "object" ? cachedStatus : {};
    return {
      status: live.status || cached.status || "unknown",
      nodeOnline: live.nodeOnline !== false && cached.nodeOnline !== false,
      cpu: live.cpu ?? cached.cpu ?? null,
      cpuLimit: live.cpuLimit ?? cached.cpuLimit ?? null,
      memory: live.memory || cached.memory || null,
      disk: live.disk || cached.disk || null,
      uptime: live.uptime ?? cached.uptime ?? null,
    };
  }

  function buildAssistantServerLimitsSummary(target, status, question = "") {
    const label = getReadableTargetLabel(target);
    const cpu = normalizePercent(status?.cpu);
    const cpuLimitRaw = Number(status?.cpuLimit);
    const cpuLimit = Number.isFinite(cpuLimitRaw) && cpuLimitRaw > 0 ? Math.round(cpuLimitRaw) : 100;
    const cpuRemaining = cpu !== null ? Math.max(0, cpuLimit - cpu) : null;

    const memoryPercent = normalizePercent(status?.memory?.percent);
    const memoryUsed = Number(status?.memory?.used);
    const memoryTotal = Number(status?.memory?.total);
    const memoryRemaining = Number.isFinite(memoryTotal) && Number.isFinite(memoryUsed)
      ? Math.max(0, memoryTotal - memoryUsed)
      : null;

    const diskPercent = normalizePercent(status?.disk?.percent);
    const diskUsed = Number(status?.disk?.used);
    const diskTotal = Number(status?.disk?.total);
    const diskRemaining = Number.isFinite(diskTotal) && Number.isFinite(diskUsed)
      ? Math.max(0, diskTotal - diskUsed)
      : null;

    const warnings = [];
    if (status?.nodeOnline === false) warnings.push("node_offline");
    if (cpu !== null && cpu >= Math.max(90, cpuLimit - 5)) warnings.push("cpu_high");
    if (memoryPercent !== null && memoryPercent >= 90) warnings.push("memory_high");
    if (diskPercent !== null && diskPercent >= 90) warnings.push("disk_high");

    const parts = [];
    if (cpu !== null) {
      parts.push(cpuLimit && cpuLimit !== 100
        ? `${cpu}% CPU of ${cpuLimit}% limit`
        : `${cpu}% CPU`);
    }
    const memoryUsedText = formatAssistantMemoryAmount(memoryUsed);
    const memoryTotalText = formatAssistantMemoryAmount(memoryTotal);
    const memoryRemainingText = formatAssistantMemoryAmount(memoryRemaining);
    if (memoryUsedText && memoryTotalText) {
      parts.push(`${memoryUsedText}/${memoryTotalText} RAM${memoryRemainingText ? `, about ${memoryRemainingText} left` : ""}`);
    } else if (memoryPercent !== null) {
      parts.push(`${memoryPercent}% RAM`);
    }
    const diskUsedText = formatAssistantDiskAmount(diskUsed);
    const diskTotalText = formatAssistantDiskAmount(diskTotal);
    const diskRemainingText = formatAssistantDiskAmount(diskRemaining);
    if (diskUsedText && diskTotalText) {
      parts.push(`${diskUsedText}/${diskTotalText} storage${diskRemainingText ? `, about ${diskRemainingText} left` : ""}`);
    } else if (diskPercent !== null) {
      parts.push(`${diskPercent}% storage`);
    }

    let message = parts.length
      ? `${label} is using ${parts.join(", ")}.`
      : `I could only read partial limits data for ${label}.`;

    if (warnings.includes("node_offline")) {
      message = `The node hosting ${label} looks offline right now.`;
    } else if (warnings.includes("memory_high")) {
      message += " RAM looks close to the limit.";
    } else if (warnings.includes("disk_high")) {
      message += " Storage looks close to full.";
    } else if (warnings.includes("cpu_high")) {
      message += " CPU looks close to saturation.";
    } else if (parts.length) {
      message += " Headroom looks okay.";
    }

    const asksForProblemCheck = /\b(problem|issue|healthy|health|okay|ok|fine|good|bad|risk)\b/i.test(String(question || ""));
    const needsConsoleCheck = asksForProblemCheck || warnings.some((entry) => entry !== "node_offline");
    if (needsConsoleCheck) {
      message += " If you want, I can check the console logs next.";
    }

    return {
      message,
      warnings,
      needsConsoleCheck,
      kind: warnings.length ? "warning" : "idle",
      remaining: {
        cpuPercent: cpuRemaining,
        memoryMb: Number.isFinite(memoryRemaining) ? Math.round(memoryRemaining) : null,
        diskGb: Number.isFinite(diskRemaining) ? Number(diskRemaining.toFixed(2)) : null,
      },
    };
  }

  function buildAssistantPropertiesLine(key, value) {
    return `${String(key || "").trim()}=${String(value ?? "").replace(/\r?\n/g, " ").trim()}`;
  }

  function extractAssistantPropertiesKey(line) {
    const text = String(line || "");
    const trimmed = text.replace(/^\s+/, "");
    if (!trimmed || /^[#!]/.test(trimmed)) return "";

    let key = "";
    let escaping = false;
    for (let index = 0; index < trimmed.length; index += 1) {
      const char = trimmed[index];
      if (escaping) {
        key += char;
        escaping = false;
        continue;
      }
      if (char === "\\") {
        key += char;
        escaping = true;
        continue;
      }
      if (char === "=" || char === ":" || /\s/.test(char)) break;
      key += char;
    }

    return key.trim();
  }

  function extractAssistantPropertiesValue(line) {
    const text = String(line || "");
    const propertyKey = extractAssistantPropertiesKey(text);
    if (!propertyKey) return "";
    return text
      .slice(text.indexOf(propertyKey) + propertyKey.length)
      .replace(/^\s*[:=]?\s*/, "")
      .trim();
  }

  function normalizeAssistantPropertyValueToken(value) {
    const clean = String(value ?? "").trim();
    if (/^(true|on|enabled?|yes|da)$/i.test(clean)) return "true";
    if (/^(false|off|disabled?|no|nu)$/i.test(clean)) return "false";
    return clean;
  }

  function updateAssistantPropertiesContent(content, key, value, options = {}) {
    const propertyKey = String(key || "").trim();
    if (!propertyKey) {
      throw new Error("A server.properties key is required.");
    }

    const normalizedContent = typeof content === "string" ? content : "";
    const newline = normalizedContent.includes("\r\n") ? "\r\n" : "\n";
    const lines = normalizedContent.split(/\r?\n/);
    const desiredLine = buildAssistantPropertiesLine(propertyKey, value);
    const expectedCurrent = normalizeAssistantPropertyValueToken(options?.onlyIfCurrent);
    const currentLine = lines.find((line) => extractAssistantPropertiesKey(line) === propertyKey);
    const currentValue = currentLine ? extractAssistantPropertiesValue(currentLine) : "";

    if (expectedCurrent) {
      const normalizedCurrent = normalizeAssistantPropertyValueToken(currentValue);
      if (!currentLine || normalizedCurrent !== expectedCurrent) {
        return {
          changed: false,
          conditionMatched: false,
          currentValue,
          content: normalizedContent,
        };
      }
    }

    const updatedLines = [];
    let replaced = false;
    let changed = false;

    for (const line of lines) {
      if (extractAssistantPropertiesKey(line) === propertyKey) {
        if (!replaced) {
          updatedLines.push(desiredLine);
          replaced = true;
          if (line !== desiredLine) changed = true;
        } else {
          changed = true;
        }
        continue;
      }
      updatedLines.push(line);
    }

    if (!replaced) {
      if (updatedLines.length && updatedLines[updatedLines.length - 1] === "") {
        updatedLines.splice(updatedLines.length - 1, 0, desiredLine);
      } else {
        updatedLines.push(desiredLine);
      }
      changed = true;
    }

    return {
      changed,
      conditionMatched: true,
      currentValue,
      content: updatedLines.join(newline),
    };
  }

  async function fetchServerPermissions(serverName) {
    return fetchJson(`/api/servers/${encodeURIComponent(serverName)}/permissions`, { cache: "no-store" });
  }

  function getAssistantPowerRequiredPermissions(action) {
    const normalizedAction = normalizeAssistantPowerAction(action);
    if (normalizedAction === "start") return ["server_start"];
    if (normalizedAction === "restart") return ["server_stop", "server_start"];
    if (normalizedAction === "stop" || normalizedAction === "kill") return ["server_stop"];
    return [];
  }

  function canAssistantRunPowerAction(perms, action) {
    const required = getAssistantPowerRequiredPermissions(action);
    return required.every((perm) => !!perms?.[perm]);
  }

  async function buildAccessibleAssistantPowerTargets(action) {
    const servers = await fetchAccessibleServerDirectory();
    if (!servers.length) {
      return { targets: [], skipped: [] };
    }

    const resolved = await mapWithConcurrency(servers, 6, async (target) => {
      const nodeId = String(target?.nodeId || "").trim().toLowerCase();
      if (!nodeId || nodeId === "local") {
        return {
          skip: {
            server: target.server,
            displayName: getReadableTargetLabel(target),
            reason: "not_attached_to_node",
          },
        };
      }

      try {
        const permissions = await fetchServerPermissions(target.server);
        if (!canAssistantRunPowerAction(permissions?.perms, action)) {
          return {
            skip: {
              server: target.server,
              displayName: getReadableTargetLabel(target),
              reason: "permission_denied",
            },
          };
        }
      } catch {
        return {
          skip: {
            server: target.server,
            displayName: getReadableTargetLabel(target),
            reason: "permission_lookup_failed",
          },
        };
      }

      return { target };
    });

    return {
      targets: resolved.map((item) => item?.target).filter(Boolean),
      skipped: resolved.map((item) => item?.skip).filter(Boolean),
    };
  }

  async function resolveMinecraftPropertyTargets(actionConfig) {
    const explicitTargets = normalizeAssistantServerTargets(
      actionConfig?.targets
      || actionConfig?.servers
      || actionConfig?.serverNames
      || actionConfig?.target
      || actionConfig?.server
    );
    const candidates = explicitTargets.length
      ? explicitTargets
      : normalizeAssistantServerTargets(await fetchAccessibleServerNames());

    if (!candidates.length) return [];

    const resolved = await mapWithConcurrency(candidates, 6, async (target) => {
      if (target.template) {
        return target.template === "minecraft" ? target : null;
      }

      try {
        const info = await fetchJson(`/api/server-info/${encodeURIComponent(target.server)}`, { cache: "no-store" });
        return String(info?.template || "").trim().toLowerCase() === "minecraft"
          ? Object.assign({}, target, { template: "minecraft" })
          : null;
      } catch {
        return null;
      }
    });

    return resolved.filter(Boolean);
  }

  async function readAssistantLogSnapshot(serverName, options = {}) {
    const includeMeta = !!options?.includeMeta;
    if (typeof window.TextDecoder === "undefined" || typeof AbortController === "undefined") {
      return includeMeta
        ? { ok: false, status: 0, lines: [], error: "This browser cannot stream console output here." }
        : [];
    }

    const timeoutMs = Math.max(250, Number(options.timeoutMs) || 900);
    const maxLines = Math.max(1, Number(options.maxLines) || 4);
    const controller = new AbortController();
    const decoder = new window.TextDecoder("utf-8");
    const timer = window.setTimeout(() => controller.abort(), timeoutMs);
    const lines = [];
    let pending = "";

    function extractLines(flush = false) {
      pending = pending.replace(/\r\n/g, "\n");

      while (true) {
        const boundary = pending.indexOf("\n\n");
        if (boundary === -1) break;

        const chunk = pending.slice(0, boundary);
        pending = pending.slice(boundary + 2);
        const payload = chunk
          .split("\n")
          .filter((line) => line.startsWith("data:"))
          .map((line) => line.slice(5).trimStart())
          .join("\n")
          .trim();

        if (!payload) continue;

        try {
          const parsed = JSON.parse(payload);
          const text = typeof parsed?.line === "string"
            ? parsed.line
            : typeof parsed === "string"
              ? parsed
              : "";
          if (text.trim()) lines.push(text.trim());
        } catch {
          if (payload.trim()) lines.push(payload.trim());
        }

        if (lines.length >= maxLines) return;
      }

      if (flush && pending.trim()) {
        const tail = pending.trim();
        try {
          const parsed = JSON.parse(tail);
          const text = typeof parsed?.line === "string"
            ? parsed.line
            : typeof parsed === "string"
              ? parsed
              : "";
          if (text.trim()) lines.push(text.trim());
        } catch {
          lines.push(tail);
        }
        pending = "";
      }
    }

    try {
      const response = await fetch(`/api/nodes/server/${encodeURIComponent(serverName)}/logs`, {
        credentials: "include",
        headers: { Accept: "text/event-stream" },
        signal: controller.signal,
        cache: "no-store",
      });
      if (!response.ok) {
        let errorMessage = "";
        try {
          const contentType = String(response.headers.get("content-type") || "").toLowerCase();
          if (contentType.includes("application/json")) {
            const json = await response.json().catch(() => null);
            errorMessage = String(json?.detail || json?.error || json?.message || "").trim();
          } else {
            errorMessage = String(await response.text().catch(() => "")).trim();
          }
        } catch {
        }

        return includeMeta
          ? {
              ok: false,
              status: response.status,
              lines: [],
              error: errorMessage,
            }
          : [];
      }
      if (!response.body) {
        return includeMeta
          ? { ok: false, status: response.status, lines: [], error: "No console stream was returned." }
          : [];
      }

      const reader = response.body.getReader();
      while (lines.length < maxLines) {
        const chunk = await reader.read();
        if (chunk.done) break;
        pending += decoder.decode(chunk.value || new Uint8Array(), { stream: true });
        extractLines(false);
      }
    } catch (error) {
      if (error?.name !== "AbortError") {
        return includeMeta
          ? {
              ok: false,
              status: 0,
              lines: [],
              error: String(error?.message || "Console request failed.").trim(),
            }
          : [];
      }
    } finally {
      window.clearTimeout(timer);
      controller.abort();
    }

    extractLines(true);
    const finalLines = lines.slice(0, maxLines);
    return includeMeta
      ? { ok: true, status: 200, lines: finalLines, error: "" }
      : finalLines;
  }

  function stripAssistantAnsiCodes(value) {
    return String(value || "").replace(/\u001b\[[0-9;]*m/g, "");
  }

  function cleanAssistantConsoleLine(value) {
    return stripAssistantAnsiCodes(value)
      .replace(/\s+/g, " ")
      .replace(/^\[[^\]]{1,40}\]\s*/g, "")
      .replace(/^(?:info|warn|warning|error|debug|trace)\s*[:|-]\s*/i, "")
      .trim();
  }

  function truncateAssistantConsoleLine(value, maxLength = 150) {
    const clean = cleanAssistantConsoleLine(value);
    if (clean.length <= maxLength) return clean;
    return `${clean.slice(0, Math.max(20, maxLength - 3)).trim()}...`;
  }

  function findAssistantConsoleEvidence(lines, pattern) {
    const list = Array.isArray(lines) ? lines : [];
    for (let index = list.length - 1; index >= 0; index -= 1) {
      const line = cleanAssistantConsoleLine(list[index]);
      if (!line) continue;
      if (pattern.test(line.toLowerCase())) {
        return truncateAssistantConsoleLine(line);
      }
    }
    return "";
  }

  function pickAssistantLatestConsoleLine(lines) {
    const list = Array.isArray(lines) ? lines : [];
    for (let index = list.length - 1; index >= 0; index -= 1) {
      const line = truncateAssistantConsoleLine(list[index]);
      if (line) return line;
    }
    return "";
  }

  function inferAssistantConsoleIntent(question) {
    const text = String(question || "").trim().toLowerCase();
    if (!text) return "diagnose";
    if (/\b(show|read|what(?:'s| is)? in|latest|tail|see|display)\b/.test(text)) return "show";
    return "diagnose";
  }

  function buildAssistantConsoleCheckSummary(target, logLines, context = {}) {
    const label = getReadableTargetLabel(target);
    const statusLabel = normalizeStatusLabel(context?.status?.status);
    const intent = inferAssistantConsoleIntent(context?.question);
    const lines = (Array.isArray(logLines) ? logLines : []).map(cleanAssistantConsoleLine).filter(Boolean);
    const latestLine = pickAssistantLatestConsoleLine(lines);

    if (context?.status?.nodeOnline === false) {
      return { kind: "warning", message: `Node looks offline for ${label}.` };
    }

    if (!lines.length) {
      if (statusLabel && statusLabel !== "unknown") {
        return { kind: statusLabel === "running" ? "idle" : "warning", message: `I could not read fresh console output for ${label}. It is ${statusLabel}.` };
      }
      return { kind: "warning", message: `I could not read fresh console output for ${label}.` };
    }

    const rules = [
      { pattern: /you need to agree to the eula|eula\.txt|eula=false/, message: `Console shows EULA is not accepted on ${label}.` },
      { pattern: /out of memory|cannot allocate memory|oom|killed process/, message: `Console shows an out-of-memory crash on ${label}.` },
      { pattern: /address already in use|eaddrinuse|failed to bind|bind: address|port is already allocated/, message: `Console shows a port conflict on ${label}.` },
      { pattern: /no space left|disk quota exceeded|enospc/, message: `Console shows disk space is exhausted on ${label}.` },
      { pattern: /unsupportedclassversionerror|more recent version of the java runtime|class file version/, message: `Console shows a Java version mismatch on ${label}.` },
      { pattern: /failed to load plugin|could not load .*plugins|error occurred while enabling|paper plugin loader/, message: `Console shows a broken plugin on ${label}.` },
      { pattern: /permission denied|operation not permitted|eacces|eperm/, message: `Console shows a permission error on ${label}.` },
      { pattern: /cannot find module|module not found|no module named|missing dependency|could not resolve dependency/, message: `Console shows a missing module or dependency on ${label}.` },
      { pattern: /connection refused|timed out|timeout|failed to connect|could not connect/, message: `Console shows a connection failure on ${label}.` },
      { pattern: /yaml|toml|json|scannerexception|parsererror|syntax error|mapping values are not allowed/, message: `Console shows a config syntax error on ${label}.` },
      { pattern: /traceback|panic:|uncaught exception|fatal error|java\.lang\./, message: `${label} is crashing with an application exception.` },
    ];

    for (const rule of rules) {
      const evidence = findAssistantConsoleEvidence(lines, rule.pattern);
      if (evidence) {
        return {
          kind: "warning",
          message: `${rule.message}${evidence ? ` ${evidence}.` : ""}`,
        };
      }
    }

    if (intent === "show" && latestLine) {
      return { kind: "idle", message: `Latest console on ${label}: ${latestLine}.` };
    }

    if (latestLine) {
      const prefix = statusLabel === "running"
        ? `Console on ${label} does not show a clear fault.`
        : `Console on ${label} points to:`;
      return { kind: statusLabel === "running" ? "idle" : "warning", message: `${prefix} ${latestLine}.` };
    }

    return { kind: "warning", message: `I could not find a clear console clue for ${label}.` };
  }

  function tailAssistantLines(content, maxLines = 18) {
    const text = String(content || "");
    if (!text.trim()) return [];
    return text
      .replace(/\r\n/g, "\n")
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean)
      .slice(-Math.max(1, Number(maxLines) || 18));
  }

  function buildAssistantLogFileCandidates(template) {
    const normalizedTemplate = String(template || "").trim().toLowerCase();
    const candidates = [
      "logs/latest.log",
      "latest.log",
      "server.log",
      "logs/server.log",
    ];
    if (normalizedTemplate === "minecraft") {
      candidates.unshift("logs/latest.log");
      candidates.push("crash-reports/latest.log");
    }
    return [...new Set(candidates)];
  }

  async function readAssistantHistoricalLogSnapshot(serverName, template, maxLines = 18) {
    const candidates = buildAssistantLogFileCandidates(template);
    for (const filePath of candidates) {
      try {
        const fileData = await fetchJson(`/api/servers/${encodeURIComponent(serverName)}/files/read?path=${encodeURIComponent(filePath)}`, {
          cache: "no-store",
        });
        const lines = tailAssistantLines(fileData?.content, maxLines);
        if (lines.length) {
          return { path: filePath, lines };
        }
      } catch {
      }
    }

    return { path: "", lines: [] };
  }

  function buildAssistantDiagnosis(target, context) {
    const label = getReadableTargetLabel(target);
    const statusLabel = normalizeStatusLabel(context?.status?.status);
    const cpu = normalizePercent(context?.status?.cpu);
    const memory = normalizePercent(context?.status?.memory?.percent);
    const disk = normalizePercent(context?.status?.disk?.percent);
    const activityEntries = Array.isArray(context?.activityEntries) ? context.activityEntries : [];
    const recentActions = activityEntries.map((entry) => String(entry?.action || "").trim().toLowerCase()).filter(Boolean);
    const logText = (Array.isArray(context?.logLines) ? context.logLines : []).join(" ").toLowerCase();
    const nodeCpu = normalizePercent(context?.nodeStats?.stats?.cpu?.percent);
    const nodeRam = normalizePercent(context?.nodeStats?.stats?.ram?.percent);
    const nodeDisk = normalizePercent(context?.nodeStats?.stats?.disk?.percent);

    if (context?.status?.nodeOnline === false || context?.nodeInfo?.nodeOnline === false) {
      return { message: `Node looks offline for ${label}.`, kind: "warning" };
    }

    if (context?.eulaAccepted === false || /you need to agree to the eula|eula\.txt/.test(logText)) {
      return { message: `EULA is not accepted on ${label} yet.`, kind: "warning" };
    }

    if (/out of memory|oom|cannot allocate memory|killed process/.test(logText) || memory !== null && memory >= 96) {
      return { message: `Likely RAM issue on ${label}.`, kind: "warning" };
    }

    if (/address already in use|eaddrinuse|failed to bind|port is already allocated|bind: address/.test(logText)) {
      return { message: `Likely port conflict on ${label}.`, kind: "warning" };
    }

    if (/no space left|disk quota exceeded|enospc/.test(logText) || disk !== null && disk >= 98) {
      return { message: `Disk looks full on ${label}.`, kind: "warning" };
    }

    if (/unsupportedclassversionerror|has been compiled by a more recent version of the java runtime|class file version/.test(logText)) {
      return { message: `Java looks mismatched on ${label}.`, kind: "warning" };
    }

    if (/failed to load plugin|could not load .*plugins|error occurred while enabling|paper plugin loader/i.test(logText)) {
      return { message: `A plugin looks broken on ${label}.`, kind: "warning" };
    }

    if (/permission denied|operation not permitted|eacces|eperm/.test(logText)) {
      return { message: `Looks like a file permission issue on ${label}.`, kind: "warning" };
    }

    if (/cannot find module|module not found|no module named|traceback|fatal error|panic:|uncaught exception|java\.lang\./.test(logText)) {
      return { message: `${label} looks crashed by an app error.`, kind: "warning" };
    }

    if ((statusLabel === "stopped" || statusLabel === "offline") && recentActions.some((action) => /(?:^|_)(stop|kill)$/.test(action))) {
      return { message: `${label} looks intentionally stopped.`, kind: "idle" };
    }

    const restartishCount = recentActions.filter((action) => /(?:^|_)(start|restart|kill)$/.test(action)).length;
    if ((statusLabel === "starting" || statusLabel === "stopped" || statusLabel === "offline") && restartishCount >= 3) {
      return { message: `${label} may be stuck in a restart loop.`, kind: "warning" };
    }

    if ((nodeRam !== null && nodeRam >= 96) || (nodeDisk !== null && nodeDisk >= 98) || (nodeCpu !== null && nodeCpu >= 98)) {
      return { message: `The node hosting ${label} looks overloaded.`, kind: "warning" };
    }

    if (statusLabel === "running") {
      if (cpu !== null && cpu >= 95) return { message: `${label} is up, but CPU is saturated.`, kind: "warning" };
      if (memory !== null && memory >= 92) return { message: `${label} is up, but RAM is nearly full.`, kind: "warning" };
      return { message: `${label} looks healthy right now.`, kind: "idle" };
    }

    if (statusLabel === "starting") {
      return { message: `${label} is still starting.`, kind: "idle" };
    }

    if (statusLabel === "unknown") {
      if (context?.noLogAccess) {
        return { message: `I only have limited diagnostics for ${label} here.`, kind: "warning" };
      }
      return { message: `I cannot read a clean status for ${label} yet.`, kind: "warning" };
    }

    return {
      message: `${label} is ${statusLabel || "not healthy"} right now.`,
      kind: statusLabel === "stopped" ? "idle" : "warning",
    };
  }

  function buildBatchPropertiesSummary(key, value, updated, unchanged, failures, skipped, options = {}) {
    const propertyKey = String(key || "").trim() || "that property";
    const valueText = String(value ?? "").trim();
    const onlyIfCurrent = String(options?.onlyIfCurrent || "").trim();
    const conditionSkipped = Array.isArray(options?.conditionSkipped) ? options.conditionSkipped : [];
    const restartAfter = !!options?.restartAfter;
    const restartSuccesses = Array.isArray(options?.restartSuccesses) ? options.restartSuccesses : [];
    const restartFailures = Array.isArray(options?.restartFailures) ? options.restartFailures : [];
    const restartSkipped = Array.isArray(options?.restartSkipped) ? options.restartSkipped : [];
    const parts = [];

    if (updated.length) {
      parts.push(
        updated.length === 1
          ? `Updated ${propertyKey} on ${updated[0]}.`
          : `Updated ${propertyKey} on ${updated.length} Minecraft servers.`
      );
    }

    if (!updated.length && unchanged.length && !conditionSkipped.length) {
      parts.push(`All targets already had ${propertyKey}=${valueText}.`);
    } else if (unchanged.length) {
      parts.push(`${unchanged.length} already matched.`);
    }

    if (conditionSkipped.length) {
      parts.push(`${conditionSkipped.length} skipped because ${propertyKey} was not ${onlyIfCurrent || "the expected value"}.`);
    }

    if (failures.length) {
      parts.push(`${failures.length} failed.`);
    }

    if (skipped.length) {
      parts.push(`${skipped.length} skipped.`);
    }

    if (restartAfter) {
      if (restartSuccesses.length) {
        parts.push(
          restartSuccesses.length === 1
            ? `Requested restart for ${restartSuccesses[0]}.`
            : `Requested restart for ${restartSuccesses.length} servers.`
        );
      }

      if (restartFailures.length) {
        parts.push(`${restartFailures.length} restart${restartFailures.length === 1 ? "" : "s"} failed.`);
      }

      if (restartSkipped.length) {
        parts.push(`${restartSkipped.length} restart${restartSkipped.length === 1 ? "" : "s"} skipped.`);
      }
    }

    if (!parts.length) {
      return `I could not update ${propertyKey}.`;
    }

    return parts.join(" ");
  }

  async function runFindBusiestNodeClientAction(actionConfig) {
    try {
      const metric = String(actionConfig?.metric || "balanced").trim().toLowerCase() || "balanced";
      const includeMetrics = normalizeAssistantMetricList(actionConfig?.includeMetrics);
      let nodes = normalizeAssistantNodeTargets(actionConfig?.nodes || actionConfig?.nodeIds);
      if (!nodes.length) {
        nodes = await fetchKnownNodes();
      }

      if (!nodes.length) {
        const message = "I could not find any nodes to inspect.";
        setStatus(message, "warning");
        appendAssistantFollowUp(message, { speak: true });
        return;
      }

      const results = await mapWithConcurrency(nodes, 4, async (node) => {
        try {
          return await fetchJson(`/api/admin/nodes/${encodeURIComponent(node.id)}/stats`, { cache: "no-store" });
        } catch (error) {
          return { ok: false, nodeId: node.id, nodeName: node.name, error: String(error?.message || "Request failed.").trim() };
        }
      });

      const adminDenied = results.some((entry) => /admin/i.test(String(entry?.error || "")));
      if (adminDenied) {
        const message = "Node load is available only for admins.";
        setStatus(message, "warning");
        appendAssistantFollowUp(message, { speak: true });
        return;
      }

      const onlineNodes = results.filter((entry) => entry?.ok && entry?.online);
      if (!onlineNodes.length) {
        const message = "I could not read live node stats right now.";
        setStatus(message, "warning");
        appendAssistantFollowUp(message, { speak: true });
        return;
      }

      const busiest = onlineNodes
        .slice()
        .sort((left, right) => scoreAssistantNodeLoad(right, metric) - scoreAssistantNodeLoad(left, metric))[0];
      const summary = summarizeAssistantNodeLoad(busiest, metric, includeMetrics);
      setStatus(summary, "idle");
      appendAssistantFollowUp(summary, {
        speak: true,
        sourceType: "busiest_node",
        context: {
          metric,
          includeMetrics,
          nodeId: busiest?.nodeId || null,
          nodeName: busiest?.nodeName || null,
          stats: busiest?.stats || null,
        },
      });
    } catch (error) {
      const rawMessage = String(error?.message || "Failed to inspect node load.").trim();
      const adminOnly = /not authorized|admin/i.test(rawMessage);
      const message = adminOnly ? "Node load is available only for admins." : rawMessage;
      setStatus(message, adminOnly ? "warning" : "error");
      appendAssistantFollowUp(message, { speak: true });
    }
  }

  async function runFindBusiestServerClientAction(actionConfig) {
    try {
      const metric = String(actionConfig?.metric || "balanced").trim().toLowerCase() || "balanced";
      const includeMetrics = normalizeAssistantMetricList(actionConfig?.includeMetrics);
      let targets = normalizeAssistantServerTargets(
        actionConfig?.targets
        || actionConfig?.servers
        || actionConfig?.serverNames
        || actionConfig?.target
        || actionConfig?.server
      );
      if (!targets.length) {
        targets = normalizeAssistantServerTargets(await fetchAccessibleServerNames());
      }

      if (!targets.length) {
        const message = "I could not find any accessible servers.";
        setStatus(message, "warning");
        appendAssistantFollowUp(message, { speak: true });
        return;
      }

      const statuses = await fetchAssistantServerStatuses(targets.map((target) => target.server));
      const ranked = targets.map((target) => ({
        target,
        status: statuses[target.server] || {},
        score: scoreAssistantServerLoad(statuses[target.server] || {}, metric),
      }));

      const candidates = ranked.filter((entry) => entry.status && entry.status.nodeOnline !== false);
      const busiest = (candidates.length ? candidates : ranked)
        .slice()
        .sort((left, right) => right.score - left.score)[0];

      if (!busiest) {
        const message = "I could not rank your servers right now.";
        setStatus(message, "warning");
        appendAssistantFollowUp(message, { speak: true });
        return;
      }

      const detailedSummary = summarizeAssistantServerLoad(busiest.target, busiest.status, metric, includeMetrics);
      setStatus(detailedSummary, "idle");
      appendAssistantFollowUp(detailedSummary, {
        speak: true,
        sourceType: "busiest_server",
        context: {
          metric,
          includeMetrics,
          server: busiest?.target?.server || null,
          displayName: busiest?.target?.displayName || busiest?.target?.server || null,
          status: busiest?.status || null,
        },
      });
    } catch (error) {
      const message = String(error?.message || "Failed to inspect server load.").trim();
      setStatus(message, "error");
      appendAssistantFollowUp(message, { speak: true });
    }
  }

  async function runCheckServerLimitsClientAction(actionConfig) {
    const query = String(actionConfig?.query || actionConfig?.server || "").trim();
    const question = String(actionConfig?.question || query).trim();
    if (!query) {
      const message = "I need a server name for that limits check.";
      setStatus(message, "warning");
      await appendAssistantFollowUp(message, {
        speak: true,
        ai: true,
        sourceType: "server_limits_check",
        context: { question },
      });
      return;
    }

    setStatus("Finding the matching server limits...", "working");
    const servers = await fetchAccessibleServerDirectory();
    const resolved = resolveAssistantServerFromDirectory(servers, query);
    if (!resolved.entry) {
      const candidates = Array.isArray(resolved.candidates) && resolved.candidates.length
        ? ` Try ${resolved.candidates.join(", ")}.`
        : "";
      const message = `${resolved.error || "I could not match that server."}${candidates}`.trim();
      setStatus(message, "warning");
      await appendAssistantFollowUp(message, {
        speak: true,
        ai: true,
        sourceType: "server_limits_check",
        context: {
          question,
          query,
          candidates: Array.isArray(resolved.candidates) ? resolved.candidates.slice(0, 8) : [],
        },
      });
      return;
    }

    setStatus(`Checking limits on ${getReadableTargetLabel(resolved.entry)}...`, "working");

    try {
      const [liveStatus, cachedStatus] = await Promise.all([
        fetchAssistantSingleServerStatus(resolved.entry.server).catch(() => null),
        fetchAssistantCachedServerStatus(resolved.entry.server).catch(() => null),
      ]);

      const mergedStatus = mergeAssistantServerLimitStatus(liveStatus, cachedStatus);
      const summary = buildAssistantServerLimitsSummary(resolved.entry, mergedStatus, question);
      setStatus(`Interpreting limits for ${getReadableTargetLabel(resolved.entry)}...`, "working");
      const finalReply = await appendAssistantFollowUp(summary.message, {
        speak: true,
        ai: true,
        sourceType: "server_limits_check",
        context: {
          server: resolved.entry.server,
          displayName: getReadableTargetLabel(resolved.entry),
          question,
          status: mergedStatus,
          warnings: summary.warnings,
          remaining: summary.remaining,
          needsConsoleCheck: summary.needsConsoleCheck,
        },
      });
      setStatus(finalReply || summary.message, summary.kind || "idle");
    } catch (error) {
      const message = String(error?.message || `Failed to inspect the limits for ${getReadableTargetLabel(resolved.entry)}.`).trim();
      setStatus(message, "error");
      await appendAssistantFollowUp(message, {
        speak: true,
        ai: true,
        sourceType: "server_limits_check",
        context: {
          server: resolved.entry.server,
          displayName: getReadableTargetLabel(resolved.entry),
          question,
          error: message,
        },
      });
    }
  }

  async function runCheckServerConsoleClientAction(actionConfig) {
    const query = String(actionConfig?.query || actionConfig?.server || "").trim();
    const question = String(actionConfig?.question || query).trim();
    if (!query) {
      const message = "I need a server name for that console check.";
      setStatus(message, "warning");
      await appendAssistantFollowUp(message, {
        speak: true,
        ai: true,
        sourceType: "console_check",
        context: { question },
      });
      return;
    }

    setStatus("Finding the matching server console...", "working");
    const servers = await fetchAccessibleServerDirectory();
    const resolved = resolveAssistantServerFromDirectory(servers, query);
    if (!resolved.entry) {
      const candidates = Array.isArray(resolved.candidates) && resolved.candidates.length
        ? ` Try ${resolved.candidates.join(", ")}.`
        : "";
      const message = `${resolved.error || "I could not match that server."}${candidates}`.trim();
      setStatus(message, "warning");
      await appendAssistantFollowUp(message, {
        speak: true,
        ai: true,
        sourceType: "console_check",
        context: {
          question,
          query,
          candidates: Array.isArray(resolved.candidates) ? resolved.candidates.slice(0, 8) : [],
        },
      });
      return;
    }

    setStatus(`Checking console on ${getReadableTargetLabel(resolved.entry)}...`, "working");

    try {
      const [statusResult, logResult] = await Promise.all([
        fetchAssistantSingleServerStatus(resolved.entry.server).catch(() => null),
        readAssistantLogSnapshot(resolved.entry.server, { timeoutMs: 2200, maxLines: 80, includeMeta: true }),
      ]);

      if (!logResult?.ok) {
        const statusCode = Number(logResult?.status) || 0;
        if (statusCode === 401 || statusCode === 403) {
          const message = `You do not have permission to read the console on ${getReadableTargetLabel(resolved.entry)}.`;
          setStatus(message, "warning");
          await appendAssistantFollowUp(message, {
            speak: true,
            ai: true,
            sourceType: "console_check",
            context: {
              server: resolved.entry.server,
              displayName: getReadableTargetLabel(resolved.entry),
              question,
              statusCode,
            },
          });
          return;
        }

        const detail = String(logResult?.error || "").trim();
        const message = detail
          ? `I could not read the console on ${getReadableTargetLabel(resolved.entry)}. ${detail}`
          : `I could not read the console on ${getReadableTargetLabel(resolved.entry)} right now.`;
        setStatus(message, "warning");
        await appendAssistantFollowUp(message, {
          speak: true,
          ai: true,
          sourceType: "console_check",
          context: {
            server: resolved.entry.server,
            displayName: getReadableTargetLabel(resolved.entry),
            question,
            statusCode,
            error: detail || "",
          },
        });
        return;
      }

      const summary = buildAssistantConsoleCheckSummary(resolved.entry, logResult.lines, {
        status: statusResult,
        question,
      });
      setStatus(`Interpreting console output for ${getReadableTargetLabel(resolved.entry)}...`, "working");
      const finalReply = await appendAssistantFollowUp(summary.message, {
        speak: true,
        ai: true,
        sourceType: "console_check",
        context: {
          server: resolved.entry.server,
          displayName: getReadableTargetLabel(resolved.entry),
          question,
          status: statusResult,
          logLines: Array.isArray(logResult.lines) ? logResult.lines.slice(-60) : [],
          kind: summary.kind || "idle",
        },
      });
      setStatus(finalReply || summary.message, summary.kind || "idle");
    } catch (error) {
      const message = String(error?.message || `Failed to inspect the console for ${getReadableTargetLabel(resolved.entry)}.`).trim();
      setStatus(message, "error");
      await appendAssistantFollowUp(message, {
        speak: true,
        ai: true,
        sourceType: "console_check",
        context: {
          server: resolved.entry.server,
          displayName: getReadableTargetLabel(resolved.entry),
          question,
          error: message,
        },
      });
    }
  }

  async function runDiagnoseServerClientAction(actionConfig) {
    const target = normalizeAssistantServerTarget(actionConfig?.target || actionConfig?.server || actionConfig);
    if (!target) {
      const message = "I need a server to diagnose.";
      setStatus(message, "warning");
      appendAssistantFollowUp(message, { speak: true });
      return;
    }

    try {
      setStatus(`Diagnosing ${getReadableTargetLabel(target)}...`, "working");

      const [statusResult, infoResult, nodeInfoResult, activityResult, logLines] = await Promise.all([
        fetchJson(`/api/server/${encodeURIComponent(target.server)}/node-status`, { cache: "no-store" }).catch((error) => ({ error: error?.message || "status_failed" })),
        fetchJson(`/api/server-info/${encodeURIComponent(target.server)}`, { cache: "no-store" }).catch(() => null),
        fetchJson(`/api/nodes/server/${encodeURIComponent(target.server)}/info`, { cache: "no-store" }).catch(() => null),
        actionConfig?.permissions?.activity_logs
          ? fetchJson(`/api/server/${encodeURIComponent(target.server)}/activity?offset=0&limit=8`, { cache: "no-store" }).catch(() => ({ entries: [] }))
          : Promise.resolve({ entries: [] }),
        actionConfig?.permissions?.console_read
          ? readAssistantLogSnapshot(target.server, { timeoutMs: 1400, maxLines: 8 })
          : Promise.resolve([]),
      ]);

      if (statusResult?.error && /access denied|no access|not authenticated/i.test(String(statusResult.error))) {
        const message = `I cannot inspect ${getReadableTargetLabel(target)} with this account.`;
        setStatus(message, "warning");
        appendAssistantFollowUp(message, { speak: true });
        return;
      }

      const template = String(infoResult?.template || actionConfig?.template || target.template || "").trim().toLowerCase();
      const nodeId = String(nodeInfoResult?.nodeId || infoResult?.nodeId || target.nodeId || "").trim();
      const [nodeStats, eulaResult, historicalLogs] = await Promise.all([
        nodeId
          ? fetchJson(`/api/admin/nodes/${encodeURIComponent(nodeId)}/stats`, { cache: "no-store" }).catch(() => null)
          : Promise.resolve(null),
        template === "minecraft" && actionConfig?.permissions?.files_read
          ? fetchJson(`/api/servers/${encodeURIComponent(target.server)}/files/read?path=${encodeURIComponent("eula.txt")}`, { cache: "no-store" }).catch(() => null)
          : Promise.resolve(null),
        actionConfig?.permissions?.files_read
          ? readAssistantHistoricalLogSnapshot(target.server, template, 18)
          : Promise.resolve({ path: "", lines: [] }),
      ]);
      const eulaAccepted = typeof eulaResult?.content === "string"
        ? /(^|\n)\s*eula\s*=\s*true\s*($|\n)/i.test(eulaResult.content)
        : null;
      const mergedLogLines = Array.isArray(logLines) && logLines.length
        ? logLines
        : (Array.isArray(historicalLogs?.lines) ? historicalLogs.lines : []);

      const diagnosis = buildAssistantDiagnosis(target, {
        status: statusResult,
        info: infoResult,
        nodeInfo: nodeInfoResult,
        nodeStats,
        eulaAccepted,
        noLogAccess: !actionConfig?.permissions?.console_read && !actionConfig?.permissions?.files_read,
        activityEntries: Array.isArray(activityResult?.entries) ? activityResult.entries : [],
        logLines: mergedLogLines,
      });
      setStatus(diagnosis.message, diagnosis.kind);
      appendAssistantFollowUp(diagnosis.message, { speak: true });
    } catch (error) {
      const message = String(error?.message || `Failed to diagnose ${getReadableTargetLabel(target)}.`).trim();
      setStatus(message, "error");
      appendAssistantFollowUp(message, { speak: true });
    }
  }

  async function runBatchMinecraftPropertiesAction(actionConfig) {
    const propertyKey = String(actionConfig?.key || actionConfig?.property || actionConfig?.propertyKey || "").trim();
    const propertyValue = actionConfig?.value ?? actionConfig?.propertyValue ?? "";
    const filePath = String(actionConfig?.path || "server.properties").trim() || "server.properties";
    const onlyIfCurrent = actionConfig?.onlyIfCurrent == null ? "" : String(actionConfig.onlyIfCurrent).trim();
    const restartAfter = !!actionConfig?.restartAfter;
    const expectedValue = extractAssistantPropertiesValue(buildAssistantPropertiesLine(propertyKey, propertyValue));

    if (!propertyKey) {
      const message = "I need the server.properties key to update.";
      setStatus(message, "warning");
      appendAssistantFollowUp(message, { speak: true });
      return;
    }

    try {
      const targets = await resolveMinecraftPropertyTargets(actionConfig);
      if (!targets.length) {
        const message = "I could not find any accessible Minecraft servers.";
        setStatus(message, "warning");
        appendAssistantFollowUp(message, { speak: true });
        return;
      }

      const updated = [];
      const unchanged = [];
      const conditionSkipped = [];
      const failures = [];
      const skipped = (Array.isArray(actionConfig?.skipped) ? actionConfig.skipped : []).map((entry) => getReadableTargetLabel(entry));
      const restartTargets = [];
      const restartSuccesses = [];
      const restartFailures = [];
      const restartSkipped = [];

      for (let index = 0; index < targets.length; index += 1) {
        const target = targets[index];
        const label = getReadableTargetLabel(target);
        setStatus(`Updating ${propertyKey} on ${label} (${index + 1}/${targets.length})...`, "working");

        try {
          const permissions = await fetchServerPermissions(target.server);
          const canRead = !!permissions?.perms?.files_read;
          const canWrite = !!permissions?.perms?.files_create;
          const canRestart = !!permissions?.perms?.server_start && !!permissions?.perms?.server_stop;
          const writeToken = String(permissions?.actionTokens?.fileWrite || "").trim();

          if (!canRead || !canWrite || !writeToken) {
            skipped.push(label);
            continue;
          }

          let currentContent = "";
          try {
            const fileData = await fetchJson(`/api/servers/${encodeURIComponent(target.server)}/files/read?path=${encodeURIComponent(filePath)}`, {
              cache: "no-store",
            });
            currentContent = typeof fileData?.content === "string" ? fileData.content : "";
          } catch (error) {
            const detail = String(error?.message || "").trim().toLowerCase();
            if (!/file not found/.test(detail)) {
              throw error;
            }
          }

          const nextFile = updateAssistantPropertiesContent(currentContent, propertyKey, propertyValue, { onlyIfCurrent });
          if (!nextFile.changed) {
            if (onlyIfCurrent && nextFile.conditionMatched === false) {
              conditionSkipped.push(label);
            } else {
              unchanged.push(label);
            }
            continue;
          }

          await fetchJson(`/api/servers/${encodeURIComponent(target.server)}/files/write`, {
            method: "PUT",
            headers: {
              "Content-Type": "application/json",
              "x-action-token": writeToken,
            },
            body: JSON.stringify({
              path: filePath,
              content: nextFile.content,
            }),
          });

          const readBack = await fetchJson(`/api/servers/${encodeURIComponent(target.server)}/files/read?path=${encodeURIComponent(filePath)}`, {
            cache: "no-store",
          });
          const verifiedContent = typeof readBack?.content === "string" ? readBack.content : "";
          if (getAssistantPropertyValue(verifiedContent, propertyKey) !== expectedValue) {
            throw new Error(`The ${propertyKey} update could not be verified on ${label}.`);
          }

          updated.push(label);
          if (restartAfter) {
            restartTargets.push({ target, label, canRestart });
          }
        } catch (error) {
          failures.push({
            label,
            message: String(error?.message || "Request failed.").trim(),
          });
        }
      }

      if (restartAfter && restartTargets.length) {
        for (let index = 0; index < restartTargets.length; index += 1) {
          const item = restartTargets[index];
          setStatus(`Restarting ${item.label} (${index + 1}/${restartTargets.length})...`, "working");

          if (!item.canRestart) {
            restartSkipped.push({ label: item.label, reason: "permission_denied" });
            continue;
          }

          try {
            await requestAssistantPowerAction(item.target, "restart");
            restartSuccesses.push(item.label);
          } catch (error) {
            restartFailures.push({
              label: item.label,
              message: String(error?.message || "Request failed.").trim(),
            });
          }
        }
      }

      const summary = buildBatchPropertiesSummary(propertyKey, propertyValue, updated, unchanged, failures, skipped, {
        onlyIfCurrent,
        conditionSkipped,
        restartAfter,
        restartSuccesses,
        restartFailures,
        restartSkipped,
      });
      setStatus(summary, failures.length || skipped.length || restartFailures.length || restartSkipped.length ? "warning" : "idle");
      appendAssistantFollowUp(summary, { speak: true });
    } catch (error) {
      const message = String(error?.message || `Failed to update ${propertyKey}.`).trim();
      setStatus(message, "error");
      appendAssistantFollowUp(message, { speak: true });
    }
  }

  async function requestAssistantPowerAction(target, action) {
    const serverName = String(target?.server || target?.name || "").trim();
    if (!serverName) {
      throw new Error("No server was selected.");
    }

    const payload = { cmd: action };

    const response = await fetch(`/api/nodes/server/${encodeURIComponent(serverName)}/action`, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await response.json().catch(() => ({}));

    if (!response.ok || (data && data.ok === false)) {
      throw new Error(data.detail || data.error || data.message || "Power action failed.");
    }

    return data;
  }

  function buildPowerActionSummary({ action, successes, failures, skipped, unconfirmed, pending }) {
    const base = getPowerActionWord(action, "base");
    const successCount = Array.isArray(successes) ? successes.length : 0;
    const failureCount = Array.isArray(failures) ? failures.length : 0;
    const skippedCount = Array.isArray(skipped) ? skipped.length : 0;
    const unconfirmedCount = Array.isArray(unconfirmed) ? unconfirmed.length : 0;
    const pendingCount = Array.isArray(pending) ? pending.length : 0;
    const sentences = [];

    if (successCount > 0) {
      const targetText = successCount === 1 ? successes[0] : `${successCount} servers`;
      sentences.push(`Requested ${base} for ${targetText}.`);
    }

    if (pendingCount > 0) {
      const firstPending = pending[0];
      const detail = firstPending?.label
        ? ` ${firstPending.label} is still updating.`
        : "";
      sentences.push(`Live status is still updating for ${pendingCount === 1 ? "1 server" : `${pendingCount} servers`}.${detail}`);
    }

    if (unconfirmedCount > 0) {
      const firstPending = unconfirmed[0];
      const detail = firstPending?.label && firstPending?.status
        ? ` ${firstPending.label} is still ${firstPending.status}.`
        : "";
      sentences.push(`${unconfirmedCount} did not reflect the requested state yet.${detail}`);
    }

    if (failureCount > 0) {
      const firstFailure = failures[0];
      const detail = firstFailure?.label
        ? ` First failed: ${firstFailure.label}${firstFailure.message ? ` (${firstFailure.message})` : ""}.`
        : "";
      sentences.push(`${failureCount} failed.${detail}`);
    }

    if (skippedCount > 0) {
      const permissionSkipped = skipped.some((item) => item?.reason === "permission_denied");
      const nodeSkipped = skipped.some((item) => item?.reason === "not_attached_to_node");
      let reasonText = "";
      if (permissionSkipped && nodeSkipped) reasonText = " due to permissions or missing node links";
      else if (permissionSkipped) reasonText = " due to permissions";
      else if (nodeSkipped) reasonText = " because they are not attached to nodes";
      sentences.push(`${skippedCount} skipped${reasonText}.`);
    }

    if (!sentences.length) {
      return `I couldn't ${base} any servers.`;
    }

    return sentences.join(" ");
  }

  async function runPowerServerClientAction(actionConfig) {
    const action = normalizeAssistantPowerAction(actionConfig?.action);
    const targets = Array.isArray(actionConfig?.targets) ? actionConfig.targets.filter((item) => item?.server || item?.name) : [];
    const skipped = Array.isArray(actionConfig?.skipped) ? actionConfig.skipped : [];

    if (!action || !targets.length) {
      const summary = buildPowerActionSummary({ action, successes: [], failures: [], skipped });
      setStatus(summary, "warning");
      if (skipped.length) {
        appendAssistantFollowUp(summary, { speak: true });
      }
      return;
    }

    const successes = [];
    const failures = [];
    const unconfirmed = [];
    const pending = [];

    for (let index = 0; index < targets.length; index += 1) {
      const target = targets[index];
      const label = getPowerTargetLabel(target);
      setStatus(`${capitalizeText(getPowerActionWord(action, "progressive"))} ${label} (${index + 1}/${targets.length})...`, "working");
      try {
        await requestAssistantPowerAction(target, action);
        successes.push(label);
        const observation = await waitForAssistantPowerObservation(target.server, action);
        if (!observation.confirmed) {
          if (observation.pending) {
            pending.push({
              label,
              status: observation.status,
            });
          } else {
            unconfirmed.push({
              label,
              status: observation.status,
            });
          }
        }
      } catch (error) {
        failures.push({
          label,
          message: String(error?.message || "Request failed.").trim(),
        });
      }
    }

    const summary = buildPowerActionSummary({ action, successes, failures, skipped, unconfirmed, pending });
    const shouldAppendFollowUp = !!(failures.length || skipped.length || unconfirmed.length);
    if (shouldAppendFollowUp) {
      appendAssistantFollowUp(summary, { speak: true });
    }

    if (failures.length || skipped.length || unconfirmed.length) {
      setStatus(summary, "warning");
      return;
    }

    if (pending.length) {
      setStatus(summary, "idle");
      return;
    }

    if (targets.length === 1 && successes.length === 1) {
      const message = `Requested ${getPowerActionWord(action, "base")} for ${successes[0]}.`;
      setStatus(message, "idle");
      return;
    }

    setStatus(summary, "idle");
  }

  async function runResolveAndPowerServerClientAction(actionConfig) {
    const action = normalizeAssistantPowerAction(actionConfig?.action);
    const query = String(actionConfig?.query || actionConfig?.server || "").trim();
    if (!action || !query) {
      const message = "I need a server name for that power action.";
      setStatus(message, "warning");
      appendAssistantFollowUp(message, { speak: true });
      return;
    }

    setStatus(`Finding the matching server for ${getPowerActionWord(action, "base")}...`, "working");
    const servers = await fetchAccessibleServerDirectory();
    const resolved = resolveAssistantServerFromDirectory(servers, query);
    if (!resolved.entry) {
      const candidates = Array.isArray(resolved.candidates) && resolved.candidates.length
        ? ` Try ${resolved.candidates.join(", ")}.`
        : "";
      const message = `${resolved.error || "I could not match that server."}${candidates}`.trim();
      setStatus(message, "warning");
      appendAssistantFollowUp(message, { speak: true });
      return;
    }

    try {
      const permissions = await fetchServerPermissions(resolved.entry.server);
      if (!canAssistantRunPowerAction(permissions?.perms, action)) {
        const message = `You do not have permission to ${getPowerActionWord(action, "base")} ${getReadableTargetLabel(resolved.entry)}.`;
        setStatus(message, "warning");
        appendAssistantFollowUp(message, { speak: true });
        return;
      }
    } catch {
      const message = `I could not verify power permissions for ${getReadableTargetLabel(resolved.entry)}.`;
      setStatus(message, "warning");
      appendAssistantFollowUp(message, { speak: true });
      return;
    }

    await runPowerServerClientAction({
      action,
      targets: [resolved.entry],
      skipped: [],
    });
  }

  async function runOpenServerPageClientAction(actionConfig) {
    const query = String(actionConfig?.query || actionConfig?.server || "").trim();
    const section = normalizeAssistantServerPageSectionValue(actionConfig?.section);
    if (!query) {
      const message = "I can't help with that server link right now.";
      setStatus(message, "warning");
      appendAssistantFollowUp(message, { speak: false });
      return false;
    }

    const cachedServers = getCachedAccessibleServerDirectory();
    const cachedResolved = cachedServers.length
      ? resolveAssistantServerFromDirectory(cachedServers, query)
      : { entry: null };

    if (cachedResolved.entry) {
      const label = getReadableTargetLabel(cachedResolved.entry);
      const url = buildAssistantServerPageUrl(cachedResolved.entry.server, section);
      const sectionLabel = getAssistantServerPageSectionLabel(section);
      const linkLabel = section === "console"
        ? `Open ${label}`
        : `Open ${sectionLabel} for ${label}`;
      appendAssistantNavigationLink(linkLabel, url, {
        prefix: "Tap the link below to open it in a new tab.",
        speak: false,
      });
      return true;
    }

    setStatus(`Finding the matching server for ${getAssistantServerPageSectionLabel(section)}...`, "working");
    const servers = await fetchAccessibleServerDirectory();
    const resolved = resolveAssistantServerFromDirectory(servers, query);
    if (!resolved.entry) {
      const candidates = Array.isArray(resolved.candidates) && resolved.candidates.length
        ? ` Try ${resolved.candidates.join(", ")}.`
        : "";
      const message = `I can't help with that server link right now.${candidates}`.trim();
      setStatus(message, "warning");
      appendAssistantFollowUp(message, { speak: false });
      return false;
    }

    const label = getReadableTargetLabel(resolved.entry);
    const url = buildAssistantServerPageUrl(resolved.entry.server, section);
    if (!url) {
      const message = `I could not open ${label} right now.`;
      setStatus(message, "error");
      appendAssistantFollowUp(message, { speak: true });
      return false;
    }

    const sectionLabel = getAssistantServerPageSectionLabel(section);
    const linkLabel = section === "console"
      ? `Open ${label}`
      : `Open ${sectionLabel} for ${label}`;
    appendAssistantNavigationLink(linkLabel, url, {
      prefix: "Tap the link below to open it in a new tab.",
      speak: false,
    });
    return true;
  }

  async function runOpenSettingsDestinationClientAction(actionConfig) {
    const action = normalizeAssistantSettingsActionValue(actionConfig?.action);
    const panel = normalizeAssistantSettingsPanelValue(actionConfig?.panel) || inferAssistantSettingsPanelFromActionValue(action);
    if (!state.isAdmin) {
      const message = "Only administrators can open settings tools.";
      setStatus(message, "warning");
      appendAssistantFollowUp(message, { speak: true });
      return false;
    }
    if (!panel && !action) {
      const message = "I can't help with that settings link right now.";
      setStatus(message, "warning");
      appendAssistantFollowUp(message, { speak: false });
      return false;
    }

    const url = buildAssistantSettingsUrl(panel, action);
    const linkLabel = action
      ? `Open ${getAssistantSettingsActionLabel(action)}`
      : `Open ${getAssistantSettingsPanelLabel(panel)}`;
    appendAssistantNavigationLink(linkLabel, url, {
      prefix: "Tap the link below to open it in a new tab.",
      speak: false,
    });
    return true;
  }

  async function runPowerAccessibleServersClientAction(actionConfig) {
    const action = normalizeAssistantPowerAction(actionConfig?.action);
    if (!action) {
      const message = "I need a valid power action.";
      setStatus(message, "warning");
      appendAssistantFollowUp(message, { speak: true });
      return;
    }

    setStatus(`Finding accessible servers to ${getPowerActionWord(action, "base")}...`, "working");
    const { targets, skipped } = await buildAccessibleAssistantPowerTargets(action);
    await runPowerServerClientAction({
      action,
      targets,
      skipped,
    });
  }

  async function runClientActions(actions) {
    const report = {
      handled: 0,
      unknown: 0,
      openedTabs: 0,
      errors: [],
    };
    if (!Array.isArray(actions) || !actions.length) return report;

    for (const action of actions) {
      try {
        if (action?.type === "power_servers") {
          report.handled += 1;
          await runPowerServerClientAction(action);
          continue;
        }
        if (assistantActionMatches(action, ["assistant_power_accessible_servers", "power_accessible_servers"])) {
          report.handled += 1;
          await runPowerAccessibleServersClientAction(action);
          continue;
        }
        if (assistantActionMatches(action, ["assistant_resolve_power_server", "resolve_power_server"])) {
          report.handled += 1;
          await runResolveAndPowerServerClientAction(action);
          continue;
        }
        if (assistantActionMatches(action, ["assistant_open_server_page", "open_server_page"])) {
          report.handled += 1;
          await runOpenServerPageClientAction(action);
          continue;
        }
        if (assistantActionMatches(action, ["assistant_open_settings_destination", "open_settings_destination"])) {
          report.handled += 1;
          await runOpenSettingsDestinationClientAction(action);
          continue;
        }
        if (assistantActionMatches(action, ["find_busiest_node", "assistant_find_busiest_node", "busiest_node"])) {
          report.handled += 1;
          await runFindBusiestNodeClientAction(action);
          continue;
        }
        if (assistantActionMatches(action, ["find_busiest_server", "assistant_find_busiest_server", "busiest_server"])) {
          report.handled += 1;
          await runFindBusiestServerClientAction(action);
          continue;
        }
        if (assistantActionMatches(action, ["check_server_console", "assistant_check_server_console", "server_console_check"])) {
          report.handled += 1;
          await runCheckServerConsoleClientAction(action);
          continue;
        }
        if (assistantActionMatches(action, ["check_server_limits", "assistant_check_server_limits", "server_limits_check"])) {
          report.handled += 1;
          await runCheckServerLimitsClientAction(action);
          continue;
        }
        if (assistantActionMatches(action, ["diagnose_server", "assistant_diagnose_server", "server_diagnosis"])) {
          report.handled += 1;
          await runDiagnoseServerClientAction(action);
          continue;
        }
        if (assistantActionMatches(action, [
          "update_minecraft_server_property",
          "assistant_update_minecraft_server_property",
          "update_minecraft_server_properties",
          "assistant_update_minecraft_server_properties",
          "batch_update_server_properties",
          "batch_update_minecraft_property",
        ])) {
          report.handled += 1;
          await runBatchMinecraftPropertiesAction(action);
          continue;
        }
        if (action?.type === "open_account_flow") {
          report.handled += 1;
          const helper = window.ADPanelDashboardAccount;
          if (!helper || typeof helper.openAccountFlow !== "function") {
            setStatus("The account flow could not be opened here.", "error");
            report.errors.push("The account flow could not be opened here.");
            continue;
          }
          await helper.openAccountFlow(action.flow);
          continue;
        }
        if (action?.type === "open_create_server_modal") {
          report.handled += 1;
          const helper = window.ADPanelDashboardCreate;
          if (!helper || typeof helper.openCreateModal !== "function") {
            setStatus("The create server window could not be opened here.", "error");
            report.errors.push("The create server window could not be opened here.");
            continue;
          }
          closeMenu();
          await helper.openCreateModal();
          continue;
        }
        if (action?.type === "confirm_delete_server") {
          report.handled += 1;
          openDeleteModal(action);
          continue;
        }

        report.unknown += 1;
      } catch (error) {
        const message = String(error?.message || "Browser action failed.").trim();
        report.errors.push(message);
        setStatus(message, "error");
        appendAssistantFollowUp(message, { speak: true });
      }
    }

    return report;
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
        allowNormalUsers: state.allowNormalUsers,
        provider: data.provider || state.provider,
        providers: data.providers || state.providers,
        chat: data.chat,
        messages: Array.isArray(data.messages) ? data.messages : [],
        actionTokens: state.actionTokens,
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
    if (messageLooksLikeAssistantServerPrefetchIntent(message)) {
      warmAccessibleServerDirectory();
    }
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
      const clientActions = Array.isArray(data.clientActions) ? data.clientActions : [];
      const hasExecutionActions = clientActions.some((action) => isExecutionClientAction(action));
      const serverMessages = Array.isArray(data.messages) ? data.messages : [];
      const lastServerAssistant = [...serverMessages].reverse().find((message) => message && message.role === "assistant");
      const serverAssistantReply = String(lastServerAssistant?.content || "").trim();
      const hasRenderedAssistant = !hasExecutionActions && !!serverAssistantReply && (!reply || serverAssistantReply === reply);
      applyBootstrap({
        configured: state.configured,
        canConfigure: state.canConfigure,
        allowNormalUsers: state.allowNormalUsers,
        provider: data.provider || state.provider,
        providers: data.providers || state.providers,
        chat: data.chat,
        messages: hasRenderedAssistant ? serverMessages : [],
        actionTokens: state.actionTokens,
      }, { preserveRenderedMessages: hasExecutionActions || !hasRenderedAssistant });
      if (!hasRenderedAssistant && reply) {
        appendMessageToUi({ role: "assistant", content: reply, created_at: new Date().toISOString() });
        state.lastAssistantReply = reply;
      }
      setTranscriptPreview("");
      const assistantMessagesBeforeActions = countAssistantMessagesInUi();
      const clientActionReport = await runClientActions(clientActions);
      if (hasExecutionActions) {
        if (clientActionReport.handled < 1) {
          const message = clientActionReport.errors[0] || "Browser action could not start.";
          setStatus(message, "error");
          appendAssistantFollowUp(message, { speak: true });
        } else if (reply && countAssistantMessagesInUi() === assistantMessagesBeforeActions) {
          const spoken = speakAssistantReply(reply);
          if (!spoken) {
            setVoiceState("Voice playback could not start automatically.");
          }
        }
        return true;
      }
      setStatus("Assistant ready.", "idle");
      if (reply) {
        const spoken = speakAssistantReply(reply);
        if (!spoken) {
          setVoiceState("Voice playback could not start automatically.");
        }
      }
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
      updateButtons();
      positionMenu();
    });
    els.providerSelect?.addEventListener("change", () => {
      state.provider = String(els.providerSelect.value || state.provider || "groq").trim().toLowerCase() || "groq";
      syncProviderUi();
      syncStoredTokenField(true);
      updateTokenStateText();
    });
    els.userAccessToggle?.addEventListener("change", () => {
      void saveAssistantAccessSetting(!!els.userAccessToggle?.checked);
    });
    els.tokenForm?.addEventListener("submit", (event) => { void saveToken(event); });
    els.tokenInput?.addEventListener("focus", () => {
      if (state.tokenFieldUsesStoredValue) {
        els.tokenInput.select();
      }
    });
    els.tokenInput?.addEventListener("keydown", (event) => {
      const key = String(event.key || "").toLowerCase();
      if ((event.ctrlKey || event.metaKey) && (key === "c" || key === "x")) {
        event.preventDefault();
        return;
      }
      if (
        state.tokenFieldUsesStoredValue
        && !event.ctrlKey
        && !event.metaKey
        && !event.altKey
        && (event.key.length === 1 || event.key === "Backspace" || event.key === "Delete")
      ) {
        clearStoredTokenFieldIfNeeded();
      }
    });
    els.tokenInput?.addEventListener("paste", () => {
      clearStoredTokenFieldIfNeeded();
    });
    els.tokenInput?.addEventListener("beforeinput", (event) => {
      if (!state.tokenFieldUsesStoredValue) return;
      const inputType = String(event.inputType || "");
      if (!inputType || inputType === "insertCompositionText") return;
      clearStoredTokenFieldIfNeeded();
    });
    els.tokenInput?.addEventListener("input", () => {
      const storedValue = getStoredTokenFieldValue(state.provider);
      state.tokenFieldUsesStoredValue = !!storedValue && els.tokenInput.value === storedValue;
      if (els.tokenInput) {
        els.tokenInput.dataset.storedValue = state.tokenFieldUsesStoredValue ? "1" : "";
      }
    });
    els.tokenInput?.addEventListener("copy", (event) => {
      event.preventDefault();
    });
    els.tokenInput?.addEventListener("cut", (event) => {
      event.preventDefault();
    });
    els.tokenInput?.addEventListener("blur", () => {
      if (!String(els.tokenInput?.value || "").trim()) {
        syncStoredTokenField(false);
      }
    });
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

    let isSettingPtt = false;
    let currentPttKey = localStorage.getItem("dashboardAssistantPttKey") || "None";
    if (els.pttBtn) {
      els.pttBtn.textContent = currentPttKey === " " ? "Space" : currentPttKey;
      els.pttBtn.addEventListener("click", () => {
        isSettingPtt = true;
        els.pttBtn.textContent = "Listening...";
      });
    }

    document.addEventListener("keydown", (event) => {
      if (isSettingPtt) {
        event.preventDefault();
        event.stopPropagation();
        currentPttKey = event.code;
        localStorage.setItem("dashboardAssistantPttKey", currentPttKey);
        els.pttBtn.textContent = event.code === "Space" ? "Space" : event.code;
        isSettingPtt = false;
        return;
      }

      if (event.key === "Escape" && state.open && !isSettingPtt) return;

      if (currentPttKey && currentPttKey !== "None" && event.code === currentPttKey) {
        const tagName = document.activeElement?.tagName?.toLowerCase();
        if (tagName === "input" || tagName === "textarea") return;
        event.preventDefault();
        if (!state.recording && !state.requestInFlight && state.configured && !event.repeat && !state.transcribing) {
           void toggleRecording();
        }
      }
    });

    document.addEventListener("keyup", (event) => {
      if (currentPttKey && currentPttKey !== "None" && event.code === currentPttKey) {
        if (!isSettingPtt && state.recording) {
           void toggleRecording();
        }
      }
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
    els.composer = $("dashboardAssistantComposer");
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
    els.userAccessToggle = $("dashboardAssistantAllowUsersToggle");
    els.deleteModal = $("dashboardAssistantDeleteModal");
    els.deleteClose = $("dashboardAssistantDeleteClose");
    els.deleteCancel = $("dashboardAssistantDeleteCancel");
    els.deleteConfirm = $("dashboardAssistantDeleteConfirm");
    els.deleteName = $("dashboardAssistantDeleteName");
    els.deleteSubtitle = $("dashboardAssistantDeleteSubtitle");
    els.pttBtn = $("dashboardAssistantPttKeyBtn");

    if (!els.trigger || !els.menu) return;

    bindEvents();
    syncProviderUi();
    syncStoredTokenField(true);
    updateTokenStateText();
    syncAssistantAccessUi();
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
