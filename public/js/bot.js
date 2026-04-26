(() => {
  const bot = document.documentElement?.dataset?.bot;
  if (!bot) {
    console.error("BOT not injected (data-bot missing)");
    return;
  }
  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, function (m) {
      return ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m]);
    });
  }
  let htmlEntityDecoder = null;
  function decodeHtmlEntities(raw) {
    const value = String(raw || '');
    if (!value || !/[&](?:[a-z]+|#\d+|#x[0-9a-f]+);/i.test(value)) return value;
    if (!htmlEntityDecoder) htmlEntityDecoder = document.createElement('textarea');
    htmlEntityDecoder.innerHTML = value;
    return htmlEntityDecoder.value;
  }

  const ANSI_SGR_RE = /\x1B\[[0-9:;]*m/g;

  function skipAnsiOscSequence(str, idx) {
    let i = idx;
    while (i < str.length) {
      const code = str.charCodeAt(i);
      if (code === 0x07) return i + 1;
      if (code === 0x1b && str.charCodeAt(i + 1) === 0x5c) return i + 2;
      i += 1;
    }
    return str.length;
  }

  function skipAnsiEscTerminatedSequence(str, idx) {
    let i = idx;
    while (i < str.length) {
      if (str.charCodeAt(i) === 0x1b && str.charCodeAt(i + 1) === 0x5c) return i + 2;
      i += 1;
    }
    return str.length;
  }

  function skipAnsiCsiSequence(str, idx) {
    let i = idx;
    while (i < str.length) {
      const code = str.charCodeAt(i);
      if (code >= 0x40 && code <= 0x7e) {
        return { next: i + 1, keep: str[i] === 'm' };
      }
      i += 1;
    }
    return { next: str.length, keep: false };
  }

  function sanitizeAnsiForConsole(input) {
    const str = String(input || '');
    if (!str) return '';
    let out = '';
    for (let i = 0; i < str.length;) {
      const code = str.charCodeAt(i);
      if (code === 0x1b || code === 0x9b) {
        const start = i;
        if (code === 0x9b) {
          const parsed = skipAnsiCsiSequence(str, i + 1);
          if (parsed.keep) out += str.slice(start, parsed.next);
          i = parsed.next;
          continue;
        }
        const next = str.charCodeAt(i + 1);
        if (next === 0x5b) {
          const parsed = skipAnsiCsiSequence(str, i + 2);
          if (parsed.keep) out += str.slice(start, parsed.next);
          i = parsed.next;
          continue;
        }
        if (next === 0x5d) {
          i = skipAnsiOscSequence(str, i + 2);
          continue;
        }
        if (next === 0x50 || next === 0x58 || next === 0x5e || next === 0x5f) {
          i = skipAnsiEscTerminatedSequence(str, i + 2);
          continue;
        }
        if (next >= 0x40 && next <= 0x5f) {
          i += 2;
          continue;
        }
        i += 1;
        continue;
      }
      if ((code < 32 || code === 127) && code !== 10 && code !== 13 && code !== 9) {
        i += 1;
        continue;
      }
      out += str[i];
      i += 1;
    }
    return out;
  }

  function stripRenderableAnsi(text) {
    return String(text || '').replace(ANSI_SGR_RE, '');
  }

  function visibleConsoleText(text) {
    return stripRenderableAnsi(text).replace(/…+/g, '');
  }

  function hasRenderableAnsi(text) {
    return /\x1B\[[0-9:;]*m/.test(String(text || ''));
  }

  function isLikelyMinecraftInfoLine(text) {
    const visible = visibleConsoleText(text).trim();
    if (!visible) return false;
    if (/\b(?:WARN|WARNING|ERROR|SEVERE|FATAL)\b/i.test(visible)) return false;
    return (
      /^\[\d{2}:\d{2}:\d{2}\]\s+\[[^\]]*\/INFO\]:/i.test(visible) ||
      /^\[\d{2}:\d{2}:\d{2}\s+INFO\]:/i.test(visible) ||
      /\[[^\]]*\/INFO\]:\s+\[Not Secure\]\s+\[Server\]/i.test(visible)
    );
  }

  function normalizeAnsiForConsoleRender(text) {
    return String(text || '')
      .split('\n')
      .map((line) => {
        if (!hasRenderableAnsi(line)) return line;
        if (!isLikelyMinecraftInfoLine(line)) return line;
        return stripRenderableAnsi(line);
      })
      .join('\n');
  }

  function stripDockerStreamFrames(raw) {
    const value = String(raw || '');
    if (value.length < 8) return value;
    let out = '';
    let usedFrames = false;
    for (let i = 0; i < value.length;) {
      const c0 = value.charCodeAt(i);
      if (
        (c0 === 1 || c0 === 2 || c0 === 3) &&
        value.charCodeAt(i + 1) === 0 &&
        value.charCodeAt(i + 2) === 0 &&
        value.charCodeAt(i + 3) === 0 &&
        i + 8 <= value.length
      ) {
        const frameLen = (
          ((value.charCodeAt(i + 4) & 0xff) << 24) |
          ((value.charCodeAt(i + 5) & 0xff) << 16) |
          ((value.charCodeAt(i + 6) & 0xff) << 8) |
          (value.charCodeAt(i + 7) & 0xff)
        ) >>> 0;
        if (frameLen === 0) {
          usedFrames = true;
          i += 8;
          continue;
        }
        if (i + 8 + frameLen <= value.length) {
          out += value.slice(i + 8, i + 8 + frameLen);
          i += 8 + frameLen;
          usedFrames = true;
          continue;
        }
      }
      out += value[i];
      i += 1;
    }
    return usedFrames ? out : value;
  }

  function normalizeAiServerNameCandidate(raw) {
    let name = String(raw || '').trim();
    if (!name) return '';
    if (name.includes('..') || /[\/\\]/.test(name) || name.length > 120) return '';
    name = name.replace(/\s+/g, '-').replace(/[^\w\-_.]/g, '').replace(/^-+|-+$/g, '');
    return name;
  }

  function formatFileSize(bytes) {
    const size = Number(bytes);
    if (!Number.isFinite(size) || size < 0) return 'Unknown size';
    if (size < 1024) return `${size} B`;
    const units = ['KB', 'MB', 'GB', 'TB'];
    let value = size / 1024;
    let unitIndex = 0;
    while (value >= 1024 && unitIndex < units.length - 1) {
      value /= 1024;
      unitIndex += 1;
    }
    const precision = value >= 100 ? 0 : value >= 10 ? 1 : 2;
    return `${value.toFixed(precision).replace(/\.0+$|(\.\d*[1-9])0+$/, '$1')} ${units[unitIndex]}`;
  }

  function flashBar(barId) {
    const fill = document.getElementById(barId);
    if (!fill) return;
    fill.classList.remove('complete');
    fill.style.width = '0%';
    void fill.offsetWidth;
    fill.style.width = '70%';
    setTimeout(() => { fill.style.width = '90%'; }, 150);
    setTimeout(() => { fill.classList.add('complete'); }, 300);
    setTimeout(() => { fill.classList.remove('complete'); fill.style.width = '0%'; }, 1100);
  }
  function flashFilesBar() { flashBar('filesFlashFill'); }
  function flashEditorBar() { flashBar('editorFlashFill'); }

  const sio = window.io;
  let socket;

  if (typeof sio !== "function") {
    console.error("Socket.IO client not loaded. Console features will be disabled.");
    socket = {
      on: () => { },
      off: () => { },
      emit: () => { },
      connected: false
    };
  } else {
    try {
      socket = sio();
    } catch (err) {
      console.error("Failed to initialize Socket.IO:", err);
      socket = {
        on: () => { },
        off: () => { },
        emit: () => { },
        connected: false
      };
    }
  }
  const bootstrapUser = window.__ADPANEL_BOOTSTRAP__ && window.__ADPANEL_BOOTSTRAP__.currentUser
    ? window.__ADPANEL_BOOTSTRAP__.currentUser
    : null;
  const currentUser = bootstrapUser || null;

  function getRequestedAssistantBotAction() {
    const params = new URLSearchParams(window.location.search || "");
    return String(params.get("assistantAction") || "").trim();
  }

  let assistantActionHandlers = null;
  let assistantRequestedActionConsumed = false;

  function clearRequestedAssistantBotAction() {
    const url = new URL(window.location.href);
    if (!url.searchParams.has("assistantAction")) return;
    url.searchParams.delete("assistantAction");
    const nextSearch = url.searchParams.toString();
    const nextUrl = `${url.pathname}${nextSearch ? `?${nextSearch}` : ""}${url.hash || ""}`;
    try {
      window.history.replaceState({}, "", nextUrl);
    } catch {
    }
  }

  function runRequestedAssistantBotAction(actionHandlers) {
    if (assistantRequestedActionConsumed) return;
    const requestedAction = getRequestedAssistantBotAction();
    if (!requestedAction) return;
    const handler = actionHandlers?.[requestedAction];
    if (typeof handler !== "function") return;
    assistantRequestedActionConsumed = true;

    window.setTimeout(() => {
      try {
        handler();
        clearRequestedAssistantBotAction();
      } catch (error) {
        console.error("[assistant-bot-action] failed:", error);
      }
    }, 160);
  }

  const sidebarToggle = document.getElementById('sidebarToggle');
  const floatingSidebar = document.getElementById('floatingSidebar');
  const profileCard = document.getElementById('profileCard');
  const statusPopover = document.getElementById('statusPopover');
  const statusInput = document.getElementById('statusInput');
  const statusDuration = document.getElementById('statusDuration');
  const saveStatus = document.getElementById('saveStatus');
  const cancelStatus = document.getElementById('cancelStatus');
  const profileStatusLabel = document.getElementById('profileStatusLabel');
  const statusDot = document.querySelector('.status-dot');
  let profileEmailEl = null;
  const STATUS_KEY = 'adpanel-profile-status';
  function bindUiActions() {
    function setupCustomSelect(wrapper) {
      const select = wrapper.querySelector('select');
      const customSelect = wrapper.querySelector('.custom-select');
      const customOptions = wrapper.querySelector('.custom-options');
      const currentSpan = customSelect.querySelector('.current');

      if (!select || !customSelect || !customOptions) return;

      if (customOptions.children.length === 0 && select.options.length > 0) {
        Array.from(select.options).forEach(opt => {
          const div = document.createElement('div');
          div.className = 'option';
          div.dataset.value = opt.value;
          div.textContent = opt.textContent;
          if (opt.selected) div.classList.add('selected');
          customOptions.appendChild(div);
        });
        if (select.selectedIndex >= 0) {
          currentSpan.textContent = select.options[select.selectedIndex].textContent;
        }
      }

      customSelect.addEventListener('click', (e) => {
        e.stopPropagation();
        document.querySelectorAll('.custom-select').forEach(el => {
          if (el !== customSelect) {
            el.classList.remove('open');
            const otherWrapper = el.closest('.custom-select-wrapper');
            if (otherWrapper) otherWrapper.querySelector('.custom-options')?.classList.remove('show');
          }
        });

        customSelect.classList.toggle('open');
        customOptions.classList.toggle('show');
      });

      customOptions.addEventListener('click', (e) => {
        if (!e.target.classList.contains('option') && !e.target.classList.contains('custom-option')) return;
        const value = e.target.dataset.value;
        const text = e.target.textContent;

        currentSpan.textContent = text;
        wrapper.querySelectorAll('.option, .custom-option').forEach(el => el.classList.remove('selected'));
        e.target.classList.add('selected');

        select.value = value;
        select.dispatchEvent(new Event('change'));

        customSelect.classList.remove('open');
        customOptions.classList.remove('show');
      });
    }

    document.querySelectorAll('.custom-select-wrapper').forEach(setupCustomSelect);

    document.addEventListener('click', (e) => {
      if (!e.target.closest('.custom-select-wrapper')) {
        document.querySelectorAll('.custom-select').forEach(el => el.classList.remove('open'));
        document.querySelectorAll('.custom-options').forEach(el => el.classList.remove('show'));
      }
    });

    const actionHandlers = {
      "open-files": () => openFilesModal(),

      "open-task": () => openTaskModal(),
      "open-store": () => openStoreModal(),
      "open-info": () => openInfoModal(),
      "open-activity": () => openActivityModal(),
      "open-backups": () => openBackupsModal(),
      "open-scheduler": () => openSchedulerModal(),
      "open-planner": () => openPlannerModal(),
      "open-resource-stats": () => openResourceStatsPopup(),
      "open-ai-help": () => openAiHelpModal(),
      "open-reinstall": () => openReinstallModal(),
      "run": () => run(),
      "stop": () => stop(),
      "kill": () => killServer(),
      "send": () => sendCommand(),
      "clear": () => clearConsole(),
      "create-new": () => createNew(),
      "go-home": () => { window.location = "/"; },
      "open-subdomains": () => openSubdomainsModal(),
    };

    document.querySelectorAll('[data-action]').forEach((el) => {
      if (el.dataset.actionBound) return;
      const action = actionHandlers[el.dataset.action];
      if (!action) return;
      el.dataset.actionBound = "1";
      el.addEventListener('click', (e) => {
        e.preventDefault();
        if (
          el.classList.contains('disabled') ||
          el.getAttribute('aria-disabled') === 'true' ||
          (typeof el.disabled === 'boolean' && el.disabled)
        ) return;
        action();
      });
    });
    assistantActionHandlers = actionHandlers;

    const listEl = document.getElementById("list");
    if (listEl && !listEl.dataset.versionBound) {
      listEl.dataset.versionBound = "1";
      listEl.addEventListener("click", (e) => {
        const btn = e.target.closest("button[data-version]");
        if (!btn) return;
        e.preventDefault();
        const version = btn.getAttribute("data-version") || "";
        const kind = btn.getAttribute("data-kind") || "";
        if (!version) return;
        if (kind === "node") changeNodeVersion(version).catch((err) => alert(err?.message || err));
        else changeVersion(version).catch((err) => alert(err?.message || err));
      });
    }

    const breadcrumbsEl = document.getElementById("breadcrumbs");
    if (breadcrumbsEl && !breadcrumbsEl.dataset.bound) {
      breadcrumbsEl.dataset.bound = "1";
      breadcrumbsEl.addEventListener("click", (e) => {
        const link = e.target.closest("a[data-path]");
        if (!link) return;
        e.preventDefault();
        const path = link.getAttribute("data-path") || "";
        loadExplorer(path);
      });
    }
  }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bindUiActions);
  } else {
    bindUiActions();
  }
  function applyStatus(statusObj) {
    if (!statusObj || (statusObj.expiresAt && Date.now() > statusObj.expiresAt)) {
      profileStatusLabel.textContent = 'Available';
      if (statusDot) statusDot.style.background = '#22c55e';
      localStorage.removeItem(STATUS_KEY);
      return;
    }
    profileStatusLabel.textContent = statusObj.text || 'Available';
    if (statusDot) statusDot.style.background = '#38bdf8';
  }
  function loadStatus() {
    fetch('/api/me/status')
      .then(res => res.json())
      .then(data => {
        if (data.ok && data.status) {
          applyStatus(data.status);
        } else {
          applyStatus(null);
        }
      })
      .catch(() => applyStatus(null));
  }
  function persistStatus(text, duration) {
    let expiresAt = null;
    if (duration !== 'never') {
      const ms = parseInt(duration, 10);
      if (!Number.isNaN(ms)) expiresAt = Date.now() + ms;
    }
    const payload = { text: text || 'Available', expiresAt };
    fetch('/api/me/status', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    })
      .then(res => res.json())
      .then(data => {
        if (data.ok) {
          applyStatus(payload);
        }
      })
      .catch(err => console.error('Failed to save status:', err));
  }
  function setSidebar(open) {
    if (!floatingSidebar || !sidebarToggle) return;
    if (open) {
      floatingSidebar.classList.add('open');
      sidebarToggle.classList.add('active');
      return;
    }
    floatingSidebar.classList.remove('open');
    sidebarToggle.classList.remove('active');
    if (statusPopover) statusPopover.classList.remove('show');
  }
  function toggleSidebar() {
    const isOpen = floatingSidebar && floatingSidebar.classList.contains('open');
    setSidebar(!isOpen);
  }
  setSidebar(true);
  if (sidebarToggle) {
    sidebarToggle.addEventListener('click', (e) => {
      e.stopPropagation();
      toggleSidebar();
    });
  }
  if (profileCard) {
    profileCard.addEventListener('click', (e) => {
      e.stopPropagation();
      if (!floatingSidebar || !floatingSidebar.classList.contains('open')) return;
      if (statusPopover) {
        const showing = statusPopover.classList.toggle('show');
        if (showing && statusInput) statusInput.focus();
      }
    });
  }
  if (saveStatus) {
    saveStatus.addEventListener('click', () => {
      const text = statusInput ? statusInput.value.trim() : '';
      const duration = statusDuration ? statusDuration.value : 'never';
      persistStatus(text, duration);
      if (statusPopover) statusPopover.classList.remove('show');
    });
  }
  if (cancelStatus) {
    cancelStatus.addEventListener('click', () => {
      if (statusPopover) statusPopover.classList.remove('show');
    });
  }
  document.addEventListener('click', (e) => {
    if (statusPopover && statusPopover.classList.contains('show')) {
      const clickInsidePopover = statusPopover.contains(e.target);
      const clickOnProfile = profileCard && profileCard.contains(e.target);
      if (!clickInsidePopover && !clickOnProfile) statusPopover.classList.remove('show');
    }
  });
  window.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      setSidebar(false);
      if (statusPopover) statusPopover.classList.remove('show');
    }
  });
  loadStatus();
  let REMOTE_MODE = false;
  let NODE_ONLINE = true;
  function setNodeOnline(isOnline) {
    NODE_ONLINE = !!isOnline;
    updateOfflineState(!NODE_ONLINE);
  }
  let NODE_ID = null;
  let NODE_API_BASE = `/api/nodes/server/${encodeURIComponent(bot)}`;
  const remoteLogTransportMode = String(new URLSearchParams(window.location.search || '').get('logTransport') || '').trim().toLowerCase();
  const PREFER_SOCKET_REMOTE_LOGS = remoteLogTransportMode === 'socket';
  let logSource = null;
  let activeRemoteLogUrl = '';
  let remoteLogTransport = 'none';
  let remoteLogFallbackTimer = null;
  let topoInfoPromise = null;
  function clearRemoteLogFallbackTimer() {
    if (remoteLogFallbackTimer) {
      clearTimeout(remoteLogFallbackTimer);
      remoteLogFallbackTimer = null;
    }
  }
  function ensureSocketConsoleJoin() {
    if (!socket || !socket.connected) return;
    socket.emit("join", bot);
  }
  function enableRemoteSocketLogFallback() {
    remoteLogTransport = 'socket';
    clearRemoteLogFallbackTimer();
    try {
      if (logSource) {
        logSource.close();
        logSource = null;
      }
    } catch { }
    activeRemoteLogUrl = '';
    ensureSocketConsoleJoin();
  }
  function closeRemoteLogEventSource() {
    clearRemoteLogFallbackTimer();
    try {
      if (logSource) {
        logSource.close();
        logSource = null;
      }
    } catch { }
    activeRemoteLogUrl = '';
  }
  function getRemoteLogTailCount(preferredTail = null) {
    const explicitTail = Number(preferredTail);
    if (Number.isFinite(explicitTail) && explicitTail >= 0) {
      return Math.max(0, Math.trunc(explicitTail));
    }
    return 0;
  }
  function buildRemoteLogsUrl(tailCount = null) {
    const params = new URLSearchParams();
    params.set('tail', String(getRemoteLogTailCount(tailCount)));
    return `${NODE_API_BASE}/logs?${params.toString()}`;
  }
  function attachLogStreamIfRemote(options = {}) {
    const { forceReconnect = false, tail = null } = options || {};
    if (!REMOTE_MODE || !hasConsoleReadAccess()) {
      remoteLogTransport = 'none';
      closeRemoteLogEventSource();
      return;
    }
    if (PREFER_SOCKET_REMOTE_LOGS) {
      if (forceReconnect || remoteLogTransport !== 'socket') {
        enableRemoteSocketLogFallback();
      } else {
        ensureSocketConsoleJoin();
      }
      return;
    }
    const url = buildRemoteLogsUrl(tail);
    if (!forceReconnect && remoteLogTransport === 'eventsource' && logSource && activeRemoteLogUrl) {
      return;
    }
    remoteLogTransport = 'eventsource';
    closeRemoteLogEventSource();
    const es = new EventSource(url);
    logSource = es;
    activeRemoteLogUrl = url;
    remoteLogFallbackTimer = window.setTimeout(() => {
      if (remoteLogTransport === 'eventsource') {
        console.debug('[remote-logs] SSE did not become active in time, falling back to socket transport');
        enableRemoteSocketLogFallback();
      }
    }, 8000);
    es.addEventListener('hello', (e) => { });
    es.addEventListener('keepalive', () => { });
    es.onmessage = (ev) => {
      if (remoteLogTransport !== 'eventsource') return;
      clearRemoteLogFallbackTimer();
      try {
        const j = JSON.parse(ev.data);
        if (j && typeof j.line === 'string') {
          appendToConsole(j.line, { live: true });
        }
      } catch {
        appendToConsole(ev.data || "", { live: true });
      }
    };
    es.onerror = () => {
      if (remoteLogTransport === 'eventsource') {
        console.warn('[remote-logs] SSE stream failed, falling back to socket transport');
        enableRemoteSocketLogFallback();
      }
    };
  }
  window.addEventListener('beforeunload', () => {
    closeRemoteLogEventSource();
  });
  function updateOfflineState(isOffline) {
    const overlay = document.getElementById('offlineOverlay');
    if (!overlay) return;
    if (isOffline) overlay.classList.add('show');
    else overlay.classList.remove('show');
  }
  async function topoInfo() {
    try {
      const r = await fetch(`${NODE_API_BASE}/info`);
      if (r.status === 429 || r.status === 401 || r.status === 403) {
        return null;
      }
      if (!r.ok) throw new Error('nodes info not ok');
      const j = await r.json();
      REMOTE_MODE = !!j.remote; NODE_ID = j.nodeId || null;
      return j;
    } catch (e) {
      REMOTE_MODE = false; NODE_ID = null;
      return null;
    }
  }
  topoInfoPromise = topoInfo();

  async function apiExplore(path = "") {
    const url = REMOTE_MODE
      ? `${NODE_API_BASE}/entries?path=${encodeURIComponent(path)}`
      : `/api/servers/${encodeURIComponent(bot)}/files/list?path=${encodeURIComponent(path)}`;
    const r = await fetch(url);
    if (!r.ok) throw new Error('explore failed');
    return await r.json();
  }
  async function apiReadFile(relPath) {
    if (!REMOTE_MODE) {
      socket.emit("readFile", { bot, path: relPath });
      return null;
    }
    const r = await fetch(`${NODE_API_BASE}/file?path=${encodeURIComponent(relPath)}`);
    if (!r.ok) throw new Error('read file failed');
    const j = await r.json();
    return j && typeof j.content === 'string' ? j.content : '';
  }
  async function apiWriteFile(relPath, content) {
    if (!REMOTE_MODE) {
      socket.emit("writeFile", { bot, path: relPath, content });
      return true;
    }
    const r = await fetch(`${NODE_API_BASE}/file`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path: relPath, content })
    });
    if (r.status === 507) {
      const j = await r.json().catch(() => null);
      throw new Error((j && j.message) || 'Disk space limit exceeded. Free up space or increase the storage limit.');
    }
    if (!r.ok) throw new Error('write file failed');
    return true;
  }
  async function apiDelete(relPath, isDir) {
    if (!REMOTE_MODE) {
      socket.emit("deleteFile", { bot, path: relPath, isDir });
      return true;
    }
    const r = await fetch(`${NODE_API_BASE}/delete`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path: relPath, isDir: !!isDir })
    });
    if (!r.ok) throw new Error('delete failed');
    return true;
  }
  async function apiRename(oldPath, newName) {
    const url = REMOTE_MODE ? `${NODE_API_BASE}/rename` : '/rename';
    const payload = REMOTE_MODE
      ? { path: oldPath, newName }
      : { bot, oldPath, newName };
    const r = await fetch(url, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    if (!r.ok) throw new Error(await r.text().catch(() => r.statusText));
    return true;
  }
  async function apiExtract(relPath) {
    const url = REMOTE_MODE ? `${NODE_API_BASE}/extract` : '/extract';
    const payload = REMOTE_MODE ? { path: relPath } : { bot, path: relPath };
    const requestOptions = {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    };
    const r = REMOTE_MODE
      ? await fetch(url, requestOptions)
      : await fetchWithServerActionToken(url, requestOptions, 'fileExtract');
    const j = await r.json().catch(() => ({}));
    if (r.status === 507 || (j && j.error === 'disk_limit_exceeded')) {
      throw new Error(j.message || 'This server has exceeded its disk space limit. Delete files to free space before extracting.');
    }
    if (!r.ok || !j.ok) throw new Error((j && j.error) ? j.error : 'extract failed');
    return j;
  }
  async function apiArchive(paths, destDir) {
    const url = REMOTE_MODE ? `${NODE_API_BASE}/archive` : '/archive';
    const payload = REMOTE_MODE ? { paths, destDir } : { bot, paths, destDir };
    const requestOptions = {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    };
    const r = REMOTE_MODE
      ? await fetch(url, requestOptions)
      : await fetchWithServerActionToken(url, requestOptions, 'fileArchive');
    const j = await r.json().catch(() => ({}));
    if (r.status === 507 || (j && j.error === 'disk_limit_exceeded')) {
      throw new Error(j.message || 'This server has exceeded its disk space limit. Delete files to free space before archiving.');
    }
    if (!r.ok || !j.ok) throw new Error((j && j.error) ? j.error : 'archive failed');
    return j;
  }
  function apiDownloadFile(relPath) {
    const url = REMOTE_MODE
      ? `${NODE_API_BASE}/download?path=${encodeURIComponent(relPath)}`
      : `/api/servers/${encodeURIComponent(bot)}/files/download?path=${encodeURIComponent(relPath)}`;
    const a = document.createElement('a');
    a.href = url;
    a.download = relPath.split('/').pop() || 'download';
    a.style.display = 'none';
    document.body.appendChild(a);
    a.click();
    setTimeout(() => a.remove(), 500);
  }
  const FILE_SIZE_EDITOR_LIMIT = 5000 * 1024;
  let currentUserPreferences = {};
  const FILE_BACKUPS_STORAGE_KEY = `adpanel:file-backups:${bot}`;
  const fileBackupDrafts = new Map();

  function normalizeFileBackupPath(filePath) {
    return String(filePath || '').replace(/\\/g, '/').trim();
  }

  function loadFileBackupsFromStorage() {
    fileBackupDrafts.clear();
    try {
      const raw = localStorage.getItem(FILE_BACKUPS_STORAGE_KEY);
      if (!raw) return fileBackupDrafts;
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return fileBackupDrafts;
      for (const [path, value] of Object.entries(parsed)) {
        const cleanPath = normalizeFileBackupPath(path);
        if (!cleanPath) continue;
        if (typeof value === 'string') {
          fileBackupDrafts.set(cleanPath, value);
          continue;
        }
        if (value && typeof value === 'object' && typeof value.content === 'string') {
          fileBackupDrafts.set(cleanPath, value.content);
        }
      }
    } catch (err) {
      console.warn('[FileBackups] Failed to load draft backups from storage:', err);
      try { localStorage.removeItem(FILE_BACKUPS_STORAGE_KEY); } catch { }
    }
    return fileBackupDrafts;
  }

  async function loadMeIntoProfileEmail() {
    if (!profileEmailEl) profileEmailEl = document.getElementById('profileEmail');

    if (currentUser && typeof currentUser === 'object') {
      const email = String(currentUser.email || '').trim();
      if (email && profileEmailEl) profileEmailEl.textContent = email;
      if (currentUser.preferences && typeof currentUser.preferences === 'object') {
        currentUserPreferences = currentUser.preferences;
      }
      if (email) return;
    }

    try {
      const res = await fetch('/api/me', { credentials: 'include' });
      if (!res.ok) return;

      const data = await res.json().catch(() => null);
      if (data && data.user) {
        const email = data.user.email;
        if (email && profileEmailEl) profileEmailEl.textContent = email;
        if (data.user.preferences) currentUserPreferences = data.user.preferences;
      }
    } catch (err) {
      console.warn('Fetch error in /api/me:', err);
    }
  }
  function apiUploadWithProgress(file, currentPath, onProgress, baseMsg) {
    return new Promise((resolve, reject) => {
      (async () => {
        try {
          const fd = new FormData();
          fd.append('file', file);
          if (!REMOTE_MODE) { fd.append('bot', bot); }
          fd.append('path', currentPath || '');

          const xhr = new XMLHttpRequest();
          xhr.open('POST', REMOTE_MODE ? `${NODE_API_BASE}/upload` : '/upload');

          if (!REMOTE_MODE) {
            const uploadToken = await ensureServerActionToken('fileUpload', { refresh: true });
            if (uploadToken) {
              xhr.setRequestHeader('x-action-token', uploadToken);
            }
          }

          xhr.upload.onprogress = (e) => { if (e.lengthComputable) { const pct = Math.round((e.loaded / e.total) * 100); onProgress(pct, `${baseMsg} (${pct}%)`); } else onProgress(6, `${baseMsg}...`); };
          xhr.onload = () => {
            if (xhr.status >= 200 && xhr.status < 300) {
              try { const j = JSON.parse(xhr.responseText || "{}"); onProgress(100, j && j.msg ? j.msg : "Finalizing..."); } catch { }
              resolve();
            } else if (xhr.status === 413) {
              try {
                const j = JSON.parse(xhr.responseText || "{}");
                reject(new Error(j.message || `File too large. Maximum upload size for this node is ${j.limit_mb ? j.limit_mb + ' MB' : 'exceeded'}.`));
                return;
              } catch { }
              reject(new Error('File too large for this node. Increase the upload limit in node settings.'));
            } else if (xhr.status === 507) {
              try {
                const j = JSON.parse(xhr.responseText || "{}");
                if (j.error === 'disk_limit_exceeded') {
                  reject(new Error(j.message || 'Disk space limit exceeded. Free up space or increase the storage limit.'));
                  return;
                }
              } catch { }
              reject(new Error('Disk space limit exceeded'));
            } else {
              reject(new Error(xhr.responseText || xhr.statusText || ('status ' + xhr.status)));
            }
          };
          xhr.onerror = () => reject(new Error('Network error'));
          xhr.send(fd);
        } catch (error) {
          reject(error);
        }
      })();
    });
  }
  async function apiCreate(type, name, currentPath) {
    const url = REMOTE_MODE ? `${NODE_API_BASE}/create` : '/create';
    const payload = REMOTE_MODE
      ? { type, name, path: currentPath || '' }
      : { bot, type, name, path: currentPath || '' };
    const r = await fetch(url, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    if (r.status === 507) {
      const j = await r.json().catch(() => null);
      throw new Error((j && j.message) || 'Disk space limit exceeded. Free up space or increase the storage limit.');
    }
    if (!r.ok) throw new Error(await r.text().catch(() => r.statusText));
    return true;
  }
  function normalizeServerPowerAction(value) {
    const action = String(value || '').trim().toLowerCase();
    if (!action) return '';
    if (action === 'run') return 'start';
    if (action === 'shutdown') return 'stop';
    return action;
  }
  function createServerActionError(code, message, extra = {}) {
    const err = new Error(message || 'Server action failed.');
    err.code = code || 'server_action_failed';
    Object.assign(err, extra);
    return err;
  }
  function buildServerActionError(action, serverName, payload = null, status = null) {
    const target = normalizeAiServerNameCandidate(serverName) || normalizeAiServerNameCandidate(bot) || 'server';
    const info = payload && typeof payload === 'object' ? payload : {};
    const errorCode = String(info.error || '').trim().toLowerCase();
    const detail = String(info.detail || info.message || '').trim();
    if (errorCode === 'permission denied') {
      return createServerActionError('permission_denied', `You do not have permission to ${action} ${target}.`, {
        action,
        serverName: target,
        permission: info.permission || null,
        status
      });
    }
    if (errorCode === 'no access to server') {
      return createServerActionError('access_denied', `You do not have access to ${target}.`, {
        action,
        serverName: target,
        status
      });
    }
    if (errorCode === 'server not found') {
      return createServerActionError('server_not_found', `${target} was not found.`, {
        action,
        serverName: target,
        status
      });
    }
    if (errorCode === 'not_remote') {
      return createServerActionError('not_remote', `${target} is not attached to a node.`, {
        action,
        serverName: target,
        status
      });
    }
    if (status === 403) {
      return createServerActionError('permission_denied', `You do not have permission to ${action} ${target}.`, {
        action,
        serverName: target,
        permission: info.permission || null,
        status
      });
    }
    return createServerActionError(errorCode || 'server_action_failed', detail || info.error || `Request failed${status ? ` (HTTP ${status})` : '.'}`, {
      action,
      serverName: target,
      status
    });
  }
  function socketRequest(eventName, payload, timeoutMs = 12000) {
    return new Promise((resolve, reject) => {
      if (!socket || typeof socket.emit !== 'function') {
        reject(createServerActionError('socket_unavailable', 'Realtime connection unavailable.'));
        return;
      }
      let settled = false;
      const timer = setTimeout(() => {
        if (settled) return;
        settled = true;
        reject(createServerActionError('timeout', 'No reply from the panel.'));
      }, timeoutMs);
      try {
        socket.emit(eventName, payload, (response) => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          resolve(response || {});
        });
      } catch (err) {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        reject(err instanceof Error ? err : createServerActionError('socket_emit_failed', String(err || 'Socket request failed.')));
      }
    });
  }
  async function requestSocketServerAction(serverName, action, hostPort = null) {
    const target = normalizeAiServerNameCandidate(serverName) || normalizeAiServerNameCandidate(bot);
    const normalizedAction = normalizeServerPowerAction(action);
    if (!target) throw createServerActionError('missing_server', 'No server selected.');
    if (!normalizedAction) throw createServerActionError('invalid_action', 'Invalid server action.');
    const ack = await socketRequest('action', {
      bot: target,
      cmd: normalizedAction === 'start' ? 'run' : normalizedAction,
      ...(hostPort != null ? { port: hostPort } : {})
    });
    if (!ack || ack.ok === false) {
      throw buildServerActionError(normalizedAction, target, ack);
    }
    return { confirmed: true, via: 'socket' };
  }
  async function requestHttpServerAction(serverName, action, hostPort = null) {
    const target = normalizeAiServerNameCandidate(serverName) || normalizeAiServerNameCandidate(bot);
    const normalizedAction = normalizeServerPowerAction(action);
    if (!target) throw createServerActionError('missing_server', 'No server selected.');
    if (!normalizedAction) throw createServerActionError('invalid_action', 'Invalid server action.');
    try {
      const response = await fetch(`/api/nodes/server/${encodeURIComponent(target)}/action`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          cmd: normalizedAction,
          ...(hostPort != null ? { hostPort } : {})
        })
      });
      const data = await response.json().catch(() => null);
      if (!response.ok) {
        if (response.status === 400 && data && data.error === 'not_remote') {
          return requestSocketServerAction(target, normalizedAction, hostPort);
        }
        throw buildServerActionError(normalizedAction, target, data, response.status);
      }
      if (data && data.ok === false) {
        throw buildServerActionError(normalizedAction, target, data, response.status);
      }
      return { confirmed: true, via: 'http' };
    } catch (err) {
      if (err && err.code) throw err;
      const message = String((err && err.message) || '');
      if (/Failed to fetch|NetworkError|Load failed/i.test(message)) {
        return requestSocketServerAction(target, normalizedAction, hostPort);
      }
      throw createServerActionError('server_action_failed', message || 'Server action failed.', {
        action: normalizedAction,
        serverName: target
      });
    }
  }
  async function apiAction(data) {
    const target = normalizeAiServerNameCandidate(data && data.bot) || normalizeAiServerNameCandidate(bot);
    const action = normalizeServerPowerAction(data && data.cmd);
    const rawHostPort = data && (data.hostPort ?? data.port);
    const hostPort = rawHostPort == null || rawHostPort === '' ? null : Number(rawHostPort);
    if (REMOTE_MODE) {
      await requestHttpServerAction(target, action, Number.isFinite(hostPort) ? hostPort : null);
      return true;
    }
    await requestSocketServerAction(target, action, Number.isFinite(hostPort) ? hostPort : null);
    return true;
  }
  async function apiCommand(command, sentAt = Date.now()) {
    const trimmed = String(command || '').trim();
    if (!trimmed) throw new Error('empty_command');

    if (!REMOTE_MODE) {
      const ack = await socketRequest('command', { bot, command: trimmed });
      if (!ack || ack.ok === false) {
        const msg = String((ack && (ack.error || ack.detail)) || 'command_failed');
        throw new Error(msg);
      }
      scheduleConsoleHistoryResync(trimmed, sentAt);
      return true;
    }

    const r = await fetch(`${NODE_API_BASE}/command`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ command: trimmed })
    });

    const payload = await r.json().catch(async () => {
      const text = await r.text().catch(() => '');
      return text ? { error: text } : {};
    });

    if (!r.ok || (payload && payload.ok === false)) {
      const message = String((payload && (payload.detail || payload.error)) || r.statusText || 'Command failed');
      throw new Error(message);
    }
    scheduleConsoleHistoryResync(trimmed, sentAt);
    return true;
  }
  let IS_ADMIN = false;
  let AGENT_ACCESS = false;
  let SERVER_ACTION_TOKENS = {};
  let serverActionTokensLoadedAt = 0;
  const SERVER_ACTION_TOKEN_REFRESH_MS = 4 * 60 * 1000;
  const INVALID_ACTION_TOKEN_ERROR = 'invalid or missing action token';
  const ONE_TIME_SERVER_ACTION_TOKENS = new Set(['fileDelete', 'fileRename', 'changeTemplate', 'applyVersion']);
  let PERMS = {
    files_read: false, files_delete: false, files_rename: false, files_archive: false, console_read: false, console_write: false,
    server_stop: false, server_start: false, files_upload: false, files_create: false,
    activity_logs: false, backups_view: false, backups_create: false, backups_delete: false,
    scheduler_access: false, scheduler_create: false, scheduler_delete: false, store_access: false,
    server_reinstall: false, agent_settings: false
  };
  function hasPerm(k) { return IS_ADMIN || !!PERMS[k]; }
  function hasConsoleReadAccess() { return hasPerm('console_read'); }
  function hasAgentAccess() { return IS_ADMIN || AGENT_ACCESS; }
  function getServerActionToken(tokenKey) {
    return String(SERVER_ACTION_TOKENS?.[tokenKey] || '').trim();
  }
  function shouldRefreshServerActionToken(tokenKey) {
    if (!tokenKey) return false;
    if (ONE_TIME_SERVER_ACTION_TOKENS.has(tokenKey)) return true;
    if (!serverActionTokensLoadedAt) return true;
    return (Date.now() - serverActionTokensLoadedAt) >= SERVER_ACTION_TOKEN_REFRESH_MS;
  }
  async function readJsonResponseSafe(response) {
    try {
      return await response.json();
    } catch {
      return {};
    }
  }
  function isInvalidActionTokenErrorPayload(payload) {
    return String(payload?.error || '').trim().toLowerCase() === INVALID_ACTION_TOKEN_ERROR;
  }
  async function ensureServerActionToken(tokenKey, opts = {}) {
    const { refresh = false } = opts;
    if (!tokenKey) return '';

    const existingToken = getServerActionToken(tokenKey);
    if (!refresh && existingToken && !shouldRefreshServerActionToken(tokenKey)) {
      return existingToken;
    }

    const refreshed = await fetchPerms({ preserveCurrentState: true });
    if (refreshed && refreshed.actionTokens && typeof refreshed.actionTokens === 'object') {
      return String(refreshed.actionTokens[tokenKey] || '').trim();
    }

    return getServerActionToken(tokenKey);
  }
  async function fetchWithServerActionToken(url, options = {}, tokenKey, opts = {}) {
    const { refreshBefore = false, retryOnInvalidToken = true } = opts;

    const runRequest = async (refreshToken) => {
      const headers = new Headers(options.headers || {});
      const token = await ensureServerActionToken(tokenKey, { refresh: refreshToken });
      if (token) headers.set('x-action-token', token);
      return fetch(url, { ...options, headers });
    };

    let response = await runRequest(refreshBefore);
    if (retryOnInvalidToken && response.status === 403) {
      const errData = await readJsonResponseSafe(response.clone());
      if (isInvalidActionTokenErrorPayload(errData)) {
        response = await runRequest(true);
      }
    }

    if (response.ok && ONE_TIME_SERVER_ACTION_TOKENS.has(tokenKey)) {
      delete SERVER_ACTION_TOKENS[tokenKey];
    }

    return response;
  }
  async function writeServerFileWithActionToken(serverName, relPath, content) {
    return fetchWithServerActionToken(`/api/servers/${encodeURIComponent(serverName)}/files/write`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path: relPath, content })
    }, 'fileWrite');
  }
  async function createServerFolderWithActionToken(serverName, relPath) {
    return fetchWithServerActionToken(`/api/servers/${encodeURIComponent(serverName)}/files/mkdir`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path: relPath })
    }, 'fileMkdir');
  }
  async function deleteServerPathWithActionToken(serverName, relPath, isDir = false) {
    return fetchWithServerActionToken(`/api/servers/${encodeURIComponent(serverName)}/files/delete`, {
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path: relPath, isDir: isDir ? '1' : '0' })
    }, 'fileDelete', { refreshBefore: true });
  }
  async function changeServerTemplateWithActionToken(serverName, templateId) {
    return fetchWithServerActionToken(`/api/servers/${encodeURIComponent(serverName)}/template`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ template: templateId })
    }, 'changeTemplate', { refreshBefore: true });
  }
  async function applyServerVersionWithActionToken(serverName, payload) {
    return fetchWithServerActionToken(`/api/servers/${encodeURIComponent(serverName)}/versions/apply`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload || {})
    }, 'applyVersion', { refreshBefore: true });
  }
  const consoleEl = document.getElementById("console");
  let consoleContentEl = document.getElementById('consoleContent');
  if (!consoleContentEl) {
    consoleContentEl = document.createElement('div');
    consoleContentEl.id = 'consoleContent';
    consoleEl.appendChild(consoleContentEl);
  }
  const cmdRow = document.getElementById("cmdRow");
  const cmdInput = document.getElementById("cmdInput");
  const entriesEl = document.getElementById("entries");
  const breadcrumbs = document.getElementById("breadcrumbs");
  const filesModal = document.getElementById("filesModal");
  const filesModalClose = document.getElementById("filesModalClose");
  const fileUploadModal = document.getElementById("fileUploadModal");
  const uploadLabel = document.getElementById("uploadLabel");
  const newBtn = document.getElementById("newBtn");
  const selectAllToggle = document.getElementById("selectAllToggle");
  const bulkDeleteBtn = document.getElementById("bulkDeleteBtn");
  const bulkArchiveBtn = document.getElementById("bulkArchiveBtn");
  const taskModal = document.getElementById("taskModal");
  const taskModalClose = document.getElementById("taskModalClose");
  const taskStartupSelect = document.getElementById("taskStartupSelect");
  const statusLabel = document.getElementById("statusLabel");
  const editorModalClose = document.getElementById("editorModalClose");
  const editorPath = document.getElementById("editorPath");
  const codeShell = document.getElementById("codeShell");
  let editorOriginalContent = "";
  const codePane = document.querySelector('.code-pane');
  const codeOverlay = document.getElementById("codeOverlay");
  const codeInput = document.getElementById("codeInput");
  const lineGutter = document.getElementById("lineGutter");
  const codeSpacer = document.getElementById("codeSpacer");
  const textArea = document.getElementById("textArea");
  const editorSave = document.getElementById("editorSave");
  const editorDiscard = document.getElementById("editorDiscard");
  const editorModal = document.getElementById("editorModal");
  const monacoContainer = document.getElementById("monacoContainer");
  const aceContainer = document.getElementById("aceContainer");
  let aceEditor = null;
  let useAceFallback = null;
  let monacoEditor = null;
  let monacoLoaderPromise = null;
  let monacoFindWidgetObserver = null;
  const progressWheel = document.getElementById("progressWheel");
  const progressFill = document.getElementById("progressFill");
  const progressPopup = document.getElementById("progressPopup");
  const popupPercent = document.getElementById("popupPercent");
  const popupBar = document.getElementById("popupBar");
  const popupMsg = document.getElementById("popupMsg");
  const popupTime = document.getElementById("popupTime");
  const popupClose = document.getElementById("popupClose");
  const dropOverlay = document.getElementById("dropOverlay");
  const dropTitle = document.getElementById("dropTitle");
  const dropSubtitle = document.getElementById("dropSubtitle");
  const infoModal = document.getElementById("infoModal");
  const infoModalClose = document.getElementById("infoModalClose");
  const infoIp = document.getElementById("infoIp");
  const infoPort = document.getElementById("infoPort");
  const infoAddress = document.getElementById("infoAddress");
  const copyAddressBtn = document.getElementById("copyAddressBtn");

  const infoSftpSection = document.getElementById("infoSftpSection");
  const infoSftpNoAccess = document.getElementById("infoSftpNoAccess");
  const infoSftpHost = document.getElementById("infoSftpHost");
  const infoSftpPort = document.getElementById("infoSftpPort");
  const infoSftpUser = document.getElementById("infoSftpUser");
  const infoSftpPass = document.getElementById("infoSftpPass");
  const infoSftpPassMask = infoSftpPass?.querySelector(".info-sftp-pass-mask");
  const infoSftpPassText = infoSftpPass?.querySelector(".info-sftp-pass-text");
  const infoSftpTogglePass = document.getElementById("infoSftpTogglePass");
  const infoSftpRegenBtn = document.getElementById("infoSftpRegenBtn");
  let currentSftpPassword = null;
  let sftpPasswordVisible = false;

  const filesBtn = document.getElementById("filesBtn");
  const taskBtn = document.getElementById("taskBtn");
  const runBtn = document.getElementById("runBtn");
  const stopBtn = document.getElementById("stopBtn");
  const miniRunBtn = document.getElementById("miniRunBtn");
  const miniStopBtn = document.getElementById("miniStopBtn");
  const miniKillBtn = document.getElementById("miniKillBtn");
  const storeBtn = document.getElementById("storeBtn");
  let monacoIdleSuggestTimer = null;
  const storeModal = document.getElementById("storeModal");
  const storeModalClose = document.getElementById("storeModalClose");
  const storeContent = document.getElementById("storeContent");
  const storeTabs = document.getElementById("storeTabs");
  const storeTabVersions = document.getElementById("storeTabVersions");
  const storeTabPlugins = document.getElementById("storeTabPlugins");
  const storeSubtitle = document.getElementById("storeSubtitle");
  const storePrefModal = document.getElementById("storePrefModal");
  const storePrefOptions = document.getElementById("storePrefOptions");
  const storePrefClose = document.getElementById("storePrefClose");
  const dockFiles = document.getElementById("dockFiles");
  const dockTask = document.getElementById("dockTask");
  const dockRun = document.getElementById("dockRun");
  const dockStop = document.getElementById("dockStop");
  const dockNew = document.getElementById("dockNew");
  const dockStore = document.getElementById("dockStore");
  const mrPluginModal = document.getElementById("mrPluginModal");
  const mrPluginModalClose = document.getElementById("mrPluginModalClose");
  const mrPluginName = document.getElementById("mrPluginName");
  const mrPlatformGrid = document.getElementById("mrPlatformGrid");
  const mrVersionGrid = document.getElementById("mrVersionGrid");
  const mrPluginInstallBtn = document.getElementById("mrPluginInstallBtn");
  let currentEditorPath = "";
  let selectedEntries = new Set();
  let serverInfo = null;
  let SERVER_NODE_ID = null;
  let currentStoreTab = 'versions';
  let storeProvidersCache = null;
  let storeCurrentProvider = null;
  let papermcVersionsCache = null;
  const STORE_PREF_STORAGE_KEY = 'store-preference-map';
  const STORE_PREF_OPTIONS = [
    {
      id: 'discord-bot',
      title: 'Node.js & Bots',
      description: 'Show Node.js runtimes and container versions optimized for Discord or generic bots.',
      badge: 'Node.js runtime',
      icon: 'fa-brands fa-node-js'
    },
    {
      id: 'minecraft',
      title: 'Minecraft Servers',
      description: 'Choose Minecraft cores and browse plugins/mods like Paper, Spigot, or Purpur.',
      badge: 'Minecraft',
      icon: 'fa-solid fa-cube'
    }
  ];
  let pendingStoreOpen = false;
  const JUMP_THRESHOLD = 8;
  const MR_PLUGIN_LOADERS = ["paper", "purpur", "spigot", "bukkit", "velocity", "waterfall", "folia"];
  const MR_MOD_LOADERS = ["fabric", "forge", "neoforge", "quilt"];
  const MR_VALID_LOADERS = Array.from(new Set([...MR_PLUGIN_LOADERS, ...MR_MOD_LOADERS]));
  let mrPluginsState = {
    offset: 0,
    limit: 24,
    query: "",
    loading: false,
    more: true
  };
  let mrAllProjectVersions = [];
  let mrCurrentProjectId = null;
  let mrSelectedPlatform = null;
  let mrSelectedMcVersion = null;
  const POWER_STATE = Object.freeze({
    UNKNOWN: "unknown",
    STOPPED: "stopped",
    RUNNING: "running",
    STARTING: "starting",
    STOPPING: "stopping"
  });
  const POWER_STATE_LABEL = Object.freeze({
    [POWER_STATE.UNKNOWN]: "idle",
    [POWER_STATE.STOPPED]: "stopped",
    [POWER_STATE.RUNNING]: "running",
    [POWER_STATE.STARTING]: "running",
    [POWER_STATE.STOPPING]: "stopped"
  });
  const POWER_POLL_IDLE_MS = 6000;
  const POWER_POLL_TRANSITION_MS = 2000;
  const POWER_TRANSITION_TIMEOUT_MS = 30000;
  const powerRunControls = Array.from(document.querySelectorAll('[data-action="run"]'));
  const powerStopControls = Array.from(document.querySelectorAll('[data-action="stop"]'));
  const powerKillControls = Array.from(document.querySelectorAll('[data-action="kill"]'));
  let powerState = POWER_STATE.UNKNOWN;
  let powerTransition = null;
  let powerTransitionSince = 0;
  let powerNodeOnline = true;
  let powerPollTimer = null;
  let powerPollInFlight = false;
  function normalizePowerState(raw) {
    const v = String(raw || "").trim().toLowerCase();
    if (!v) return POWER_STATE.UNKNOWN;
    if (v.includes("starting")) return POWER_STATE.STARTING;
    if (v.includes("stopping")) return POWER_STATE.STOPPING;
    if (v.includes("running") || v.includes("online") || v.includes("up") || v.includes("healthy")) return POWER_STATE.RUNNING;
    if (v.includes("stopped") || v.includes("offline") || v.includes("down") || v.includes("exit") || v.includes("dead")) return POWER_STATE.STOPPED;
    return POWER_STATE.UNKNOWN;
  }
  function isTransitionPowerState(state) {
    return state === POWER_STATE.STARTING || state === POWER_STATE.STOPPING;
  }
  function setPowerControlDisabled(el, disabled) {
    if (!el) return;
    el.classList.toggle("disabled", disabled);
    el.setAttribute("aria-disabled", disabled ? "true" : "false");
    if (typeof el.disabled === "boolean") {
      el.disabled = disabled;
    }
  }
  function canRunNow() {
    return hasPerm("server_start") && powerNodeOnline && powerState === POWER_STATE.STOPPED;
  }
  function canStopNow() {
    return hasPerm("server_stop") && powerNodeOnline && powerState === POWER_STATE.RUNNING;
  }
  function isPowerActionAllowed(action) {
    if (action === "run") return canRunNow();
    if (action === "stop") return canStopNow();
    if (action === "kill") return canStopNow();
    return false;
  }
  function updatePowerStatusLabel() {
    if (!statusLabel) return;
    statusLabel.textContent = POWER_STATE_LABEL[powerState] || "idle";
  }
  function updatePowerControlsInteractivity() {
    const enableRun = canRunNow();
    const enableStop = canStopNow();
    powerRunControls.forEach((el) => setPowerControlDisabled(el, !enableRun));
    powerStopControls.forEach((el) => setPowerControlDisabled(el, !enableStop));
    powerKillControls.forEach((el) => setPowerControlDisabled(el, !enableStop));
  }
  function setPowerState(nextState, opts = {}) {
    const normalized = normalizePowerState(nextState);
    const now = Date.now();
    if (normalized === POWER_STATE.STARTING || normalized === POWER_STATE.STOPPING) {
      if (powerTransition !== normalized) {
        powerTransition = normalized;
        powerTransitionSince = now;
      }
    } else if (normalized === POWER_STATE.RUNNING || normalized === POWER_STATE.STOPPED) {
      powerTransition = null;
      powerTransitionSince = 0;
    } else if (!opts.keepTransition) {
      powerTransition = null;
      powerTransitionSince = 0;
    }
    if (powerState !== normalized || opts.force) {
      powerState = normalized;
      updatePowerStatusLabel();
    }
    updatePowerControlsInteractivity();
  }
  function applyPolledPowerState(rawState, nodeOnline) {
    powerNodeOnline = nodeOnline !== false;
    const polled = normalizePowerState(rawState);
    const now = Date.now();
    if (polled === POWER_STATE.UNKNOWN && !powerTransition) {
      if (powerState === POWER_STATE.RUNNING || powerState === POWER_STATE.STOPPED) {
        updatePowerControlsInteractivity();
        return;
      }
      const seeded = normalizePowerState(serverInfo && serverInfo.status ? serverInfo.status : "");
      if (seeded === POWER_STATE.RUNNING || seeded === POWER_STATE.STOPPED) {
        setPowerState(seeded, { force: true });
        return;
      }
    }
    if (powerTransition === POWER_STATE.STARTING) {
      if (polled === POWER_STATE.RUNNING) {
        setPowerState(POWER_STATE.RUNNING, { force: true });
        return;
      }
      if (polled === POWER_STATE.STOPPED && now - powerTransitionSince > 5000) {
        setPowerState(POWER_STATE.STOPPED, { force: true });
        return;
      }
      if (now - powerTransitionSince > POWER_TRANSITION_TIMEOUT_MS) {
        setPowerState(polled === POWER_STATE.UNKNOWN ? POWER_STATE.STOPPED : polled, { force: true });
        return;
      }
      setPowerState(POWER_STATE.STARTING, { force: true, keepTransition: true });
      return;
    }
    if (powerTransition === POWER_STATE.STOPPING) {
      if (polled === POWER_STATE.STOPPED) {
        setPowerState(POWER_STATE.STOPPED, { force: true });
        return;
      }
      if (now - powerTransitionSince > POWER_TRANSITION_TIMEOUT_MS) {
        setPowerState(polled === POWER_STATE.UNKNOWN ? POWER_STATE.RUNNING : polled, { force: true });
        return;
      }
      setPowerState(POWER_STATE.STOPPING, { force: true, keepTransition: true });
      return;
    }
    setPowerState(polled, { force: true });
  }
  async function fetchPowerSnapshot() {
    const r = await fetch(`/api/server/${encodeURIComponent(bot)}/node-status`, { cache: "no-store" });
    if (!r.ok) throw new Error(`power-status-${r.status}`);
    const j = await r.json().catch(() => ({}));
    return {
      state: (j && (j.status || j.state)) || "",
      nodeOnline: j && typeof j.nodeOnline !== "undefined" ? j.nodeOnline !== false : true
    };
  }
  function nextPowerPollDelay() {
    return isTransitionPowerState(powerState) ? POWER_POLL_TRANSITION_MS : POWER_POLL_IDLE_MS;
  }
  function schedulePowerStatePoll(delayMs = nextPowerPollDelay()) {
    if (powerPollTimer) clearTimeout(powerPollTimer);
    powerPollTimer = setTimeout(() => {
      refreshPowerState().catch(() => { });
    }, delayMs);
  }
  async function refreshPowerState(opts = {}) {
    const { force = false } = opts;
    if (powerPollInFlight && !force) return;
    powerPollInFlight = true;
    try {
      const snapshot = await fetchPowerSnapshot();
      applyPolledPowerState(snapshot.state, snapshot.nodeOnline);
    } catch {
      if (powerTransition && (Date.now() - powerTransitionSince > POWER_TRANSITION_TIMEOUT_MS)) {
        powerTransition = null;
        powerTransitionSince = 0;
      }
      updatePowerControlsInteractivity();
    } finally {
      powerPollInFlight = false;
      schedulePowerStatePoll();
    }
  }
  function startPowerStatePolling() {
    schedulePowerStatePoll(120);
  }
  function stopPowerStatePolling() {
    if (powerPollTimer) {
      clearTimeout(powerPollTimer);
      powerPollTimer = null;
    }
  }
  function syncPowerStateFromPanelMessage(rawOutput) {
    const msg = String(rawOutput || "").toLowerCase();
    if (!msg.includes("[adpanel]")) return;
    if (msg.includes("server starting")) setPowerState(POWER_STATE.STARTING);
    else if (msg.includes("server stopping")) setPowerState(POWER_STATE.STOPPING);
    else if (msg.includes("server killing")) setPowerState(POWER_STATE.STOPPING);
    else if (msg.includes("server started")) setPowerState(POWER_STATE.RUNNING);
    else if (msg.includes("server stopped")) setPowerState(POWER_STATE.STOPPED);
    else if (msg.includes("server killed")) setPowerState(POWER_STATE.STOPPED);
  }
  updatePowerControlsInteractivity();
  window.addEventListener('load', () => {
    ensureJumpBtn();
    updateJumpBtnVisibility();
  });
  window.addEventListener('beforeunload', stopPowerStatePolling);
  window.addEventListener('resize', updateJumpBtnVisibility);
  async function fetchPerms(opts = {}) {
    const { preserveCurrentState = false } = opts;
    try {
      const r = await fetch(`/api/servers/${encodeURIComponent(bot)}/permissions`, { cache: 'no-store' });
      if (!r.ok) {
        const status = r.status;
        if (status === 401 || status === 403) {
          const offlineOverlay = document.getElementById('offlineOverlay');
          const offlineText = document.querySelector('#offlineOverlay .offline-text');
          if (offlineOverlay && offlineText) {
            offlineText.textContent = status === 401 ? "Not authenticated" : "No Access";
            offlineOverlay.classList.add('show');
          }
        }
        throw new Error('perm fetch failed');
      }
      const j = await r.json();
      IS_ADMIN = !!j.isAdmin;
      AGENT_ACCESS = !!j.agent_access;
      SERVER_ACTION_TOKENS = (j && j.actionTokens && typeof j.actionTokens === 'object') ? { ...j.actionTokens } : {};
      serverActionTokensLoadedAt = Date.now();
      if (j && j.perms && typeof j.perms === 'object') {
        PERMS = Object.assign(PERMS, j.perms);
      }
      if (profileEmailEl && j) {
        const emailFromApi =
          (j.user && j.user.email) ? j.user.email :
            (j.email || null);
        if (emailFromApi) {
          profileEmailEl.textContent = emailFromApi;
        }
      }
      return j;
    } catch (e) {
      if (!preserveCurrentState) {
        IS_ADMIN = false;
        AGENT_ACCESS = false;
        SERVER_ACTION_TOKENS = {};
        serverActionTokensLoadedAt = 0;
        PERMS = {
          files_read: false, files_delete: false, files_rename: false, files_archive: false, console_read: false, console_write: false,
          server_stop: false, server_start: false, files_upload: false, files_create: false,
          activity_logs: false, backups_view: false, backups_create: false, backups_delete: false,
          scheduler_access: false, scheduler_create: false, scheduler_delete: false, store_access: false,
          server_reinstall: false, agent_settings: false
        };
      }
      return null;
    }
  }
  function clientClean(raw) {
    if (!raw) return "";
    let s = decodeHtmlEntities(raw);
    s = stripDockerStreamFrames(s);
    s = s.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
    s = sanitizeAnsiForConsole(s);
    s = s.replace(/…+/g, "");
    s = s.replace(/^\s*(?:stdout|stderr)\s*:\s*/gmi, "");
    const out = [];
    for (let line of s.split("\n")) {
      line = line.replace(/…+/g, "");
      line = line.replace(/^((?:\x1B\[[0-9:;]*m)*)(?:\s*>\.{2,}\s*)/, '$1');
      line = line.replace(/^((?:\x1B\[[0-9:;]*m)*)(?:[>=. ]+)/, '$1');
      line = line.replace(/\s+$/, "");
      const visible = visibleConsoleText(line);
      const t = visible.trim();
      if (!t) continue;
      if (/^[\s>.=…-]+$/.test(t)) continue;
      if (/^\[ADPanel\]\s+Connected\s*$/i.test(t)) continue;
      if (/^>\s*$/.test(t)) continue;
      if (/^>\s*\.{2,}\s*$/.test(t)) continue;
      if (/^>\s+[^@\s]+@[^\s]+\s+\S+/i.test(t)) continue;
      if (/^>\s+(?:node|npm|pnpm|yarn)\b/i.test(t)) continue;
      if (t.includes("Usage:  docker")) continue;
      if (t.includes("Run 'docker COMMAND --help'")) continue;
      if (t.includes("For more help on how to use Docker")) continue;
      if (/^\s*Container started\s*$/i.test(t)) continue;
      if (/^[0-9a-f]{64}(?:\s*Container started)?\s*$/i.test(t)) continue;
      if (/^\s*\[?waiting\]?\s+/i.test(t)) continue;
      if (/container\s+".*"\s+not\s+found\s+yet/i.test(t)) continue;
      if (/Error response from daemon:\s*No such container/i.test(t)) continue;
      if (/^\s*\[init\]\s/i.test(t)) continue;
      if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z,?version=.*$/i.test(t)) continue;
      if (/mc-server-runner/i.test(t) && /(gracefully stopping|Sending.*stop.*Minecraft|Waiting for completion|\bDone\b)/i.test(t)) continue;
      out.push(line);
    }
    let joined = out.join("\n");
    joined = joined
      .replace(/([^\n])(?=20\d{2}-\d{2}-\d{2}T)/g, "$1\n")
      .replace(/([^\n])(?=\[\d{2}:\d{2}:\d{2}\s(?:INFO|WARN|ERROR))/g, "$1\n")
      .replace(/([^\n])(?=Starting org\.bukkit\.craftbukkit\.Main)/g, "$1\n")
      .replace(/([^\n])(?=\*\*\* Warning)/g, "$1\n");
    joined = joined.replace(/\n{2,}/g, "\n");
    if (joined && !joined.endsWith("\n")) joined += "\n";
    return joined;
  }

  let ansiUp = null;
  function getAnsiUp() {
    if (!ansiUp) {
      if (typeof AnsiUp !== 'undefined') {
        ansiUp = new AnsiUp();
        ansiUp.use_classes = false;
      } else {
        return { ansi_to_html: (t) => t };
      }
    }
    return ansiUp;
  }

  function ansiToHtml(text) {
    if (!text) return "";

    const normalizedText = normalizeAnsiForConsoleRender(text);

    let result = getAnsiUp().ansi_to_html(normalizedText);

    result = result.replace(
      /(https?:\/\/[^\s<>"']+)/gi,
      '<a href="$1" class="console-link" target="_blank" rel="noopener noreferrer">$1</a>'
    );

    if (hasRenderableAnsi(normalizedText)) {
      return result;
    }

    const lines = result.split('\n');
    result = lines.map(line => {
      if (/\[\d{2}:\d{2}:\d{2}\]\s+\[[^\]]*\/(WARN|WARNING)\]:/i.test(line)) {
        return line.replace(
          /(\[\d{2}:\d{2}:\d{2}\]\s+\[[^\]]*\/(?:WARN|WARNING)\]:)/gi,
          '<span class="log-line-warn">$1</span>'
        );
      }
      if (/\[\d{2}:\d{2}:\d{2}\]\s+\[[^\]]*\/(ERROR|SEVERE|FATAL)\]:/i.test(line)) {
        return line.replace(
          /(\[\d{2}:\d{2}:\d{2}\]\s+\[[^\]]*\/(?:ERROR|SEVERE|FATAL)\]:)/gi,
          '<span class="log-line-error">$1</span>'
        );
      }
      if (/\[\d{2}:\d{2}:\d{2}\]\s+\[[^\]]*\/INFO\]:/i.test(line)) {
        return line.replace(
          /(\[\d{2}:\d{2}:\d{2}\]\s+\[[^\]]*\/INFO\]:)/gi,
          '<span class="log-line-info">$1</span>'
        );
      }
      if (/\[\d{2}:\d{2}:\d{2}\]\s+\[[^\]]*\/(DEBUG|TRACE)\]:/i.test(line)) {
        return line.replace(
          /(\[\d{2}:\d{2}:\d{2}\]\s+\[[^\]]*\/(?:DEBUG|TRACE)\]:)/gi,
          '<span class="log-line-debug">$1</span>'
        );
      }
      if (/\[\d{2}:\d{2}:\d{2}\s+(WARN|WARNING)\]/i.test(line) || /^(WARN|WARNING):/i.test(line)) {
        return `<span class="log-line-warn">${line}</span>`;
      }
      if (/\[\d{2}:\d{2}:\d{2}\s+(ERROR|SEVERE|FATAL)\]/i.test(line) || /^(ERROR|SEVERE|FATAL):/i.test(line)) {
        return `<span class="log-line-error">${line}</span>`;
      }
      if (/\[\d{2}:\d{2}:\d{2}\s+INFO\]/i.test(line)) {
        return line.replace(
          /(\[\d{2}:\d{2}:\d{2}\s+INFO\])/gi,
          '<span class="log-line-info">$1</span>'
        );
      }
      if (/\[\d{2}:\d{2}:\d{2}\s+CMD\]/i.test(line)) {
        return line.replace(
          /(\[\d{2}:\d{2}:\d{2}\s+CMD\])/gi,
          '<span class="log-line-debug">$1</span>'
        );
      }
      if (/\[\d{2}:\d{2}:\d{2}\s+(DEBUG|TRACE)\]/i.test(line)) {
        return `<span class="log-line-debug">${line}</span>`;
      }
      if (/^\[[\w\-]+\]/i.test(line)) {
        return line.replace(
          /^(\[[\w\-]+\])/i,
          '<span class="log-line-info">$1</span>'
        );
      }
      return line;
    }).join('\n');

    return result;
  }
  let userPinnedBottom = true;
  let suppressAutoScroll = false;
  let commandSendInFlight = false;
  let lastSubmittedCommand = "";
  let lastSubmittedAt = 0;
  let pendingConsoleHistoryResyncTimer = null;
  let lastConsoleLiveAt = 0;
  const CONSOLE_COMMAND_HISTORY_LIMIT = 32;
  const consoleCommandHistoryKey = `adpanel:${bot}:command_history`;
  let consoleCommandHistory = loadConsoleCommandHistory();
  let consoleCommandHistoryIndex = -1;

  function loadConsoleCommandHistory() {
    try {
      const parsed = JSON.parse(localStorage.getItem(consoleCommandHistoryKey) || '[]');
      if (!Array.isArray(parsed)) return [];
      return parsed
        .map((entry) => String(entry || '').trim())
        .filter(Boolean)
        .slice(0, CONSOLE_COMMAND_HISTORY_LIMIT);
    } catch {
      return [];
    }
  }

  function persistConsoleCommandHistory() {
    try {
      localStorage.setItem(consoleCommandHistoryKey, JSON.stringify(consoleCommandHistory.slice(0, CONSOLE_COMMAND_HISTORY_LIMIT)));
    } catch { }
  }

  function rememberConsoleCommandForInput(commandText) {
    const command = String(commandText || '').trim();
    if (!command) return;
    consoleCommandHistory = [
      command,
      ...consoleCommandHistory.filter((entry) => entry !== command)
    ].slice(0, CONSOLE_COMMAND_HISTORY_LIMIT);
    consoleCommandHistoryIndex = -1;
    persistConsoleCommandHistory();
  }

  function recallConsoleCommand(direction) {
    if (!cmdInput || !consoleCommandHistory.length) return;
    if (direction < 0) {
      consoleCommandHistoryIndex = Math.min(consoleCommandHistoryIndex + 1, consoleCommandHistory.length - 1);
    } else {
      consoleCommandHistoryIndex = Math.max(consoleCommandHistoryIndex - 1, -1);
    }
    cmdInput.value = consoleCommandHistory[consoleCommandHistoryIndex] || '';
    const cursor = cmdInput.value.length;
    try { cmdInput.setSelectionRange(cursor, cursor); } catch { }
  }

  function isNearBottom(el) {
    return (el.scrollHeight - el.scrollTop - el.clientHeight) < JUMP_THRESHOLD;
  }
  function canScroll(el) {
    return el && (el.scrollHeight - el.clientHeight) > 2;
  }
  let jumpBtn = null;
  function ensureJumpBtn() {
    if (jumpBtn) return jumpBtn;
    try {
      if (getComputedStyle(consoleEl).position === 'static') {
        consoleEl.style.position = 'relative';
      }
    } catch { }
    jumpBtn = document.createElement('button');
    jumpBtn.type = 'button';
    jumpBtn.className = 'jump-btn';
    jumpBtn.setAttribute('aria-label', 'Jump to bottom');
    jumpBtn.innerHTML = '<i class="fa-solid fa-arrow-down"></i>';
    consoleEl.appendChild(jumpBtn);
    jumpBtn.addEventListener('click', () => {
      scrollConsoleToBottom(true);
      suppressAutoScroll = false;
      userPinnedBottom = true;
      jumpBtn.classList.remove('show');
    });
    return jumpBtn;
  }
  function updateJumpBtnVisibility() {
    const btn = ensureJumpBtn();
    const show = canScroll(consoleEl) && !isNearBottom(consoleEl);
    btn.classList.toggle('show', show);
  }
  function scrollConsoleToBottom(smooth = true) {
    try {
      if (smooth) {
        const start = consoleEl.scrollTop;
        const end = consoleEl.scrollHeight - consoleEl.clientHeight;
        const distance = end - start;
        const duration = Math.min(600, Math.max(200, Math.abs(distance) * 0.5));
        let startTime = null;

        const easeOutCubic = (t) => 1 - Math.pow(1 - t, 3);

        function animateScroll(currentTime) {
          if (!startTime) startTime = currentTime;
          const elapsed = currentTime - startTime;
          const progress = Math.min(elapsed / duration, 1);
          const easedProgress = easeOutCubic(progress);

          consoleEl.scrollTop = start + (distance * easedProgress);

          if (progress < 1) {
            requestAnimationFrame(animateScroll);
          }
        }

        requestAnimationFrame(animateScroll);
      } else {
        consoleEl.scrollTop = consoleEl.scrollHeight;
      }
    } catch { }
  }
  const MAX_CONSOLE_CHARS = 400_000;
  let _pendingConsoleText = "";
  let _flushRaf = 0;
  let _consoleRawChunks = [];
  let _consoleVisibleChars = 0;
  let lastConsoleAppendText = "";
  let lastConsoleAppendAt = 0;
  let lastConsoleHistorySyncTs = 0;
  const seenConsoleHistoryEntries = new Set();

  function getConsoleVisibleLength(text) {
    return visibleConsoleText(text).length;
  }

  function resetConsoleBuffer() {
    if (_flushRaf) {
      cancelAnimationFrame(_flushRaf);
      _flushRaf = 0;
    }
    if (pendingConsoleHistoryResyncTimer) {
      clearTimeout(pendingConsoleHistoryResyncTimer);
      pendingConsoleHistoryResyncTimer = null;
    }
    _pendingConsoleText = "";
    _consoleRawChunks = [];
    _consoleVisibleChars = 0;
    lastConsoleAppendText = "";
    lastConsoleAppendAt = 0;
    lastConsoleHistorySyncTs = 0;
    lastConsoleLiveAt = 0;
    seenConsoleHistoryEntries.clear();
    if (consoleContentEl) {
      consoleContentEl.innerHTML = '';
    }
    updateJumpBtnVisibility();
  }

  function formatConsoleClock(ts = Date.now()) {
    const d = new Date(ts);
    const hh = String(d.getHours()).padStart(2, '0');
    const mm = String(d.getMinutes()).padStart(2, '0');
    const ss = String(d.getSeconds()).padStart(2, '0');
    return `${hh}:${mm}:${ss}`;
  }

  function appendCommandEcho(commandText, sentAt = Date.now()) {
    const safe = String(commandText || '').replace(/[\r\n]+/g, ' ').trim();
    if (!safe) return;
    appendToConsole(`[${formatConsoleClock(sentAt)} CMD] ${safe}\n`);
  }

  function _flushConsole() {
    _flushRaf = 0;
    if (!_pendingConsoleText) return;

    const wasNearBottom = isNearBottom(consoleEl);
    const rawChunk = _pendingConsoleText;
    const htmlContent = ansiToHtml(rawChunk);
    consoleContentEl.innerHTML += htmlContent;
    _pendingConsoleText = "";

    _consoleRawChunks.push(rawChunk);
    _consoleVisibleChars += getConsoleVisibleLength(rawChunk);

    if (_consoleVisibleChars > MAX_CONSOLE_CHARS) {
      while (_consoleVisibleChars > MAX_CONSOLE_CHARS && _consoleRawChunks.length > 1) {
        const dropped = _consoleRawChunks.shift();
        _consoleVisibleChars -= getConsoleVisibleLength(dropped);
      }
      consoleContentEl.innerHTML = ansiToHtml(_consoleRawChunks.join(''));
    }

    if (!suppressAutoScroll && wasNearBottom) {
      scrollConsoleToBottom(false);
    }
    updateJumpBtnVisibility();
  }

  function appendToConsole(rawChunk, options = {}) {
    const cleaned = clientClean(rawChunk);
    if (!cleaned) return;
    if (options && options.live) {
      lastConsoleLiveAt = Date.now();
    }
    const now = Date.now();
    if (cleaned === lastConsoleAppendText && now - lastConsoleAppendAt < 400) {
      return;
    }
    lastConsoleAppendText = cleaned;
    lastConsoleAppendAt = now;

    _pendingConsoleText += cleaned;

    if (!_flushRaf) {
      _flushRaf = requestAnimationFrame(_flushConsole);
    }
  }
  async function fetchConsoleHistory(limit = 80) {
    const r = await fetch(`/api/servers/${encodeURIComponent(bot)}/console-history?limit=${encodeURIComponent(limit)}`);
    const payload = await r.json().catch(() => ({}));
    if (!r.ok || !payload || payload.ok === false || !Array.isArray(payload.lines)) {
      throw new Error((payload && (payload.error || payload.detail)) || `history_http_${r.status}`);
    }
    return payload.lines;
  }
  async function resetServerConsoleHistorySession() {
    try {
      const response = await fetch(`/api/servers/${encodeURIComponent(bot)}/console-history/reset`, { method: 'POST' });
      return response.ok;
    } catch {
      return false;
    }
  }
  async function hydrateInitialConsoleHistory(limit = 120) {
    if (!hasConsoleReadAccess()) return 0;
    if (_consoleVisibleChars > 0 || _pendingConsoleText || (consoleContentEl && consoleContentEl.textContent.trim())) return 0;
    let appendedCount = 0;
    try {
      const lines = await fetchConsoleHistory(limit);
      for (const item of lines) {
        const ts = Number(item && item.ts) || 0;
        const source = String(item && item.source || 'output');
        const line = String(item && item.line || '');
        if (!line) continue;
        const key = `${ts}:${source}:${line}`;
        if (seenConsoleHistoryEntries.has(key)) continue;
        seenConsoleHistoryEntries.add(key);
        if (seenConsoleHistoryEntries.size > 1500) {
          const first = seenConsoleHistoryEntries.values().next().value;
          if (first) seenConsoleHistoryEntries.delete(first);
        }
        appendToConsole(line.endsWith('\n') ? line : `${line}\n`);
        if (ts > lastConsoleHistorySyncTs) lastConsoleHistorySyncTs = ts;
        appendedCount += 1;
      }
    } catch { }
    return appendedCount;
  }
  function scheduleConsoleHistoryResync(commandText, sentAt = Date.now()) {
    if (!hasConsoleReadAccess()) return;
    const baselineTs = sentAt - 250;
    if (pendingConsoleHistoryResyncTimer) {
      clearTimeout(pendingConsoleHistoryResyncTimer);
      pendingConsoleHistoryResyncTimer = null;
    }
    pendingConsoleHistoryResyncTimer = window.setTimeout(async () => {
      pendingConsoleHistoryResyncTimer = null;
      if (remoteLogTransport === 'eventsource' && lastConsoleLiveAt >= sentAt) return;
      try {
        const lines = await fetchConsoleHistory(120);
        for (const item of lines) {
          const ts = Number(item && item.ts) || 0;
          const source = String(item && item.source || 'output');
          const line = String(item && item.line || '');
          if (!line) continue;
          if (ts < baselineTs) continue;
          if (ts <= lastConsoleHistorySyncTs) continue;
          const key = `${ts}:${source}:${line}`;
          if (seenConsoleHistoryEntries.has(key)) continue;
          seenConsoleHistoryEntries.add(key);
          if (seenConsoleHistoryEntries.size > 1500) {
            const first = seenConsoleHistoryEntries.values().next().value;
            if (first) seenConsoleHistoryEntries.delete(first);
          }
          if (source === 'command' && line.trim() === String(commandText || '').trim()) continue;
          appendToConsole(line.endsWith('\n') ? line : `${line}\n`);
          if (ts > lastConsoleHistorySyncTs) lastConsoleHistorySyncTs = ts;
        }
      } catch { }
    }, remoteLogTransport === 'eventsource' ? 1200 : 400);
  }
  consoleEl.addEventListener('scroll', () => {
    userPinnedBottom = isNearBottom(consoleEl);
    suppressAutoScroll = !userPinnedBottom;
    updateJumpBtnVisibility();
  });
  function applyPermissionsToUI() {
    if (!hasPerm('console_write')) { cmdRow.classList.add('hidden'); }
    if (!hasConsoleReadAccess() && consoleContentEl) {
      resetConsoleBuffer();
    }
    if (!hasPerm('files_read')) { filesBtn.classList.add('disabled'); dockFiles.classList.add('disabled'); }
    if (!hasPerm('files_upload')) { uploadLabel.classList.add('hidden'); fileUploadModal.classList.add('hidden'); }
    if (!hasPerm('files_create')) { newBtn.classList.add('hidden'); dockNew.classList.add('disabled'); editorSave.classList.add('disabled'); }
    if (!hasPerm('server_start')) {
      if (taskBtn) taskBtn.classList.add('disabled');
      if (dockTask) dockTask.classList.add('disabled');
    }
    const schedulerBtn = document.getElementById('schedulerBtn');
    if (schedulerBtn && !hasPerm('scheduler_access')) {
      schedulerBtn.classList.add('disabled');
      schedulerBtn.style.display = 'none';
    }
    const activityBtnEl = document.getElementById('activityBtn');
    const dockActivity = document.getElementById('dockActivity');
    if (!hasPerm('activity_logs')) {
      if (activityBtnEl) { activityBtnEl.classList.add('disabled'); activityBtnEl.style.display = 'none'; }
      if (dockActivity) { dockActivity.classList.add('disabled'); dockActivity.style.display = 'none'; }
    }
    const storeBtnEl = document.getElementById('storeBtn');
    const dockStore = document.getElementById('dockStore');
    if (!hasPerm('store_access')) {
      if (storeBtnEl) { storeBtnEl.classList.add('disabled'); }
      if (dockStore) { dockStore.classList.add('disabled'); }
    }
    const backupsBtnEl = document.getElementById('backupsBtn');
    const dockBackups = document.getElementById('dockBackups');
    if (!hasPerm('backups_view')) {
      if (backupsBtnEl) { backupsBtnEl.classList.add('disabled'); }
      if (dockBackups) { dockBackups.classList.add('disabled'); }
    }
    const aiHelpBtnEl = document.getElementById('aiHelpBtn');
    const dockAiHelp = document.getElementById('dockAiHelp');
    if (!hasAgentAccess()) {
      if (aiHelpBtnEl) { aiHelpBtnEl.classList.add('disabled'); }
      if (dockAiHelp) { dockAiHelp.classList.add('disabled'); }
    }
    const reinstallBtnEl = document.getElementById('reinstallBtn');
    const dockReinstall = document.getElementById('dockReinstall');
    if (!hasPerm('server_reinstall')) {
      if (reinstallBtnEl) { reinstallBtnEl.classList.add('disabled'); reinstallBtnEl.style.display = 'none'; }
      if (dockReinstall) { dockReinstall.classList.add('disabled'); dockReinstall.style.display = 'none'; }
    } else {
      if (reinstallBtnEl) { reinstallBtnEl.classList.remove('disabled'); reinstallBtnEl.style.display = ''; }
      if (dockReinstall) { dockReinstall.classList.remove('disabled'); dockReinstall.style.display = ''; }
    }
    const aiHelpSettingsBtnEl = document.getElementById('aiHelpSettingsBtn');
    if (aiHelpSettingsBtnEl) {
      if (typeof IS_ADMIN !== 'undefined' && IS_ADMIN) {
        aiHelpSettingsBtnEl.classList.remove('hidden');
        aiHelpSettingsBtnEl.removeAttribute('hidden');
        aiHelpSettingsBtnEl.style.removeProperty('display');
        aiHelpSettingsBtnEl.style.setProperty('display', 'flex', 'important');
      } else {
        aiHelpSettingsBtnEl.classList.add('hidden');
        aiHelpSettingsBtnEl.setAttribute('hidden', 'true');
        aiHelpSettingsBtnEl.style.setProperty('display', 'none', 'important');
      }
    }
    updatePowerControlsInteractivity();
  }
  socket.off("connect");
  socket.on("connect", () => {
    socket.emit("join", bot);
  });

  socket.off("output");
  socket.on("output", (data) => {
    if (REMOTE_MODE && remoteLogTransport === 'eventsource') return;
    syncPowerStateFromPanelMessage(data);
    if (!hasConsoleReadAccess()) return;
    appendToConsole(data, { live: true });
  });

  socket.off("nodeStatus");
  socket.on("nodeStatus", (data) => {
    if (!data || typeof data.nodeOnline === 'undefined') return;
    const nodeOnline = data.nodeOnline !== false;
    powerNodeOnline = nodeOnline;
    updatePowerControlsInteractivity();
    setNodeOnline(nodeOnline);
    updateOfflineState(!nodeOnline);
  });

  if (socket.connected) socket.emit("join", bot);
  socket.on("fileData", (d) => { openEditorModal(d.path, d.content); });
  async function loadServerInfo() {
    if (!topoInfoPromise) topoInfoPromise = topoInfo();
    const [topoRes, srvRes] = await Promise.allSettled([
      topoInfoPromise.catch(() => null),
      fetch(`/api/server-info/${encodeURIComponent(bot)}`)
        .then(r => (r.ok ? r.json() : null))
        .catch(() => null)
    ]);
    const topo = topoRes.status === 'fulfilled' ? topoRes.value : null;
    const srv = srvRes.status === 'fulfilled' ? srvRes.value : null;
    REMOTE_MODE = !!(topo && topo.remote);
    NODE_ID = (topo && topo.nodeId) || (srv && srv.nodeId) || null;
    const topoInfoObj = (topo && topo.info) ? topo.info : {};
    serverInfo = Object.assign({}, topoInfoObj, srv || {});
    updateStoreVisibility();
  }
  function formatAddress(ip, port) {
    if (!ip) return "";
    const is6 = ip.includes(":") && !ip.includes(".");
    const host = is6 ? `[${ip}]` : ip;
    return port ? `${host}:${port}` : host;
  }
  async function nodeAction(cmd, options = {}) {
    if (!SERVER_NODE_ID) throw new Error('no remote node');
    const r = await fetch(`/api/nodes/${encodeURIComponent(SERVER_NODE_ID)}/server/action`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(Object.assign({ name: bot, cmd }, options))
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || (j && j.error)) {
      const msg = (j && (j.detail || j.error)) || `HTTP ${r.status}`;
      throw new Error(msg);
    }
    return j;
  }
  async function nodeCommand(command) {
    if (!SERVER_NODE_ID) throw new Error('no remote node');
    const r = await fetch(`/api/nodes/${encodeURIComponent(SERVER_NODE_ID)}/server/command`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: bot, command })
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || (j && j.error)) {
      const msg = (j && (j.detail || j.error)) || `HTTP ${r.status}`;
      throw new Error(msg);
    }
    return j;
  }
  function normalizeTemplate(t) {
    const raw = (t ?? '').toString().trim().toLowerCase();
    if (!raw) return '';
    if (["discord", "discord-bot", "discord bot", "node", "nodejs", "python", "bot"].includes(raw)) return 'discord-bot';
    if (["mc", "minecraft"].includes(raw)) return 'minecraft';
    return raw;
  }
  function templateDisplayName(t) {
    const tpl = normalizeTemplate(t);
    if (tpl === 'discord-bot') return 'Node.js';
    if (tpl === 'minecraft') return 'Minecraft';
    return tpl || 'unknown';
  }
  function loadStorePreferenceMap() {
    try {
      const raw = localStorage.getItem(STORE_PREF_STORAGE_KEY) || '{}';
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === 'object' ? parsed : {};
    } catch { return {}; }
  }
  function saveStorePreferenceMap(map) {
    try { localStorage.setItem(STORE_PREF_STORAGE_KEY, JSON.stringify(map || {})); } catch { }
  }
  function getStorePreference() {
    const map = loadStorePreferenceMap();
    return map[bot] || null;
  }
  function setStorePreference(pref) {
    const map = loadStorePreferenceMap();
    if (pref) map[bot] = pref; else delete map[bot];
    saveStorePreferenceMap(map);
  }
  async function persistStoreTemplateChoice(templateId) {
    const tpl = normalizeTemplate(templateId);
    if (!tpl) return false;
    try {
      const r = await changeServerTemplateWithActionToken(bot, tpl);
      if (!r.ok) return false;
      try { await loadServerInfo(); } catch { }
      return true;
    } catch (e) {
      console.warn('Failed to persist template choice', e);
      return false;
    }
  }
  function resolveStoreTemplate() {
    const tpl = serverInfo ? normalizeTemplate(serverInfo.template) : '';
    if (tpl === 'discord-bot' || tpl === 'minecraft') return { template: tpl, source: 'template' };
    const saved = getStorePreference();
    if (saved === 'discord-bot' || saved === 'minecraft') return { template: saved, source: 'preference' };
    return { template: '', source: 'unknown' };
  }
  function storeTemplateQuery() {
    const { template: tpl, source } = resolveStoreTemplate();
    if (source === 'preference' && tpl) return `?template=${encodeURIComponent(tpl)}`;
    return '';
  }
  function renderStorePreferenceOptions() {
    if (!storePrefOptions) return;
    storePrefOptions.innerHTML = '';
    STORE_PREF_OPTIONS.forEach(opt => {
      const card = document.createElement('button');
      card.type = 'button';
      card.className = 'store-pref-card';
      card.innerHTML = `
            <div class="store-pref-head">
              <div class="icon"><i class="${opt.icon}"></i></div>
              <div>
                <div class="store-pref-title">${opt.title}</div>
                <div class="store-pref-badge"><i class="fa-solid fa-wand-magic-sparkles"></i> ${opt.badge}</div>
              </div>
            </div>
            <div class="store-pref-desc">${opt.description}</div>
          `;
      card.addEventListener('click', async () => {
        setStorePreference(opt.id);
        const saved = await persistStoreTemplateChoice(opt.id);
        closeStorePrefModal();
        updateStoreVisibility();
        if (!saved) alert('Saved locally but failed to persist template for this server.');
        if (pendingStoreOpen) {
          pendingStoreOpen = false;
          openStoreModal();
        }
      });
      storePrefOptions.appendChild(card);
    });
  }
  function openStorePrefModal() {
    if (!storePrefModal) return;
    renderStorePreferenceOptions();
    storePrefModal.classList.add('show');
    storePrefModal.style.display = 'flex';
    storePrefModal.setAttribute('aria-hidden', 'false');
  }
  function animateClose(el, onComplete) {
    if (!el) return;
    el.classList.add('closing');
    el.addEventListener('animationend', () => {
      el.classList.remove('closing');
      el.classList.remove('show');
      el.style.display = 'none';
      el.setAttribute('aria-hidden', 'true');
      if (onComplete) onComplete();
    }, { once: true });
  }

  function closeStorePrefModal() {
    if (!storePrefModal) return;
    animateClose(storePrefModal, () => {
      pendingStoreOpen = false;
    });
  }
  function updateStoreVisibility() {
    const btn = document.getElementById('storeBtn');
    const dock = document.getElementById('dockStore');
    const tabs = document.getElementById('storeTabs');
    const { template: tpl } = resolveStoreTemplate();
    const showForDiscord = (tpl === 'discord-bot');
    const showForMinecraft = (tpl === 'minecraft');
    const hasExperience = !!tpl;
    const showVersions = showForDiscord || showForMinecraft;
    const showPlugins = showForMinecraft;
    [btn, dock].forEach(el => { if (el) el.classList.remove('hidden'); });
    if (tabs) tabs.classList.toggle('hidden', !hasExperience);
    if (storeTabVersions) storeTabVersions.classList.toggle('hidden', !showVersions);
    if (storeTabPlugins) storeTabPlugins.classList.toggle('hidden', !showPlugins);
    if (storeSubtitle && !hasExperience) {
      storeSubtitle.textContent = 'Pick what you want to browse: Node.js runtimes or Minecraft cores.';
    }
    const allowedTabs = new Set();
    if (showVersions) allowedTabs.add('versions');
    if (showPlugins) allowedTabs.add('plugins');
    if (allowedTabs.size && !allowedTabs.has(currentStoreTab)) {
      const fallbackTab = showVersions ? 'versions' : (showPlugins ? 'plugins' : currentStoreTab);
      setActiveStoreTab(fallbackTab);
    }
  }
  function setActiveStoreTab(tab) {
    currentStoreTab = tab;
    if (storeTabVersions) storeTabVersions.classList.toggle('active', tab === 'versions');
    if (storeTabPlugins) storeTabPlugins.classList.toggle('active', tab === 'plugins');
    if (storeSubtitle) {
      const { template: tpl } = resolveStoreTemplate();
      if (tab === 'versions') {
        storeSubtitle.textContent = (tpl === 'minecraft')
          ? 'Choose your core distribution and version.'
          : 'Choose the runtime/version for your bot container.';
      } else if (tpl === 'minecraft') {
        storeSubtitle.textContent = 'Browse and install plugins from Modrinth.';
      } else {
        storeSubtitle.textContent = '';
      }
    }
  }
  document.addEventListener('DOMContentLoaded', () => {
    updateStoreVisibility();
  });
  async function openStoreModal() {
    if (!storeModal) return;
    if (!hasPerm('store_access')) { alert('You do not have permission to access the store'); return; }
    if (!serverInfo) await loadServerInfo();
    updateStoreVisibility();
    const { template: tpl } = resolveStoreTemplate();
    if (!tpl) {
      pendingStoreOpen = true;
      openStorePrefModal();
      return;
    }
    storeModal.classList.add('show');
    storeModal.style.display = 'flex';
    storeModal.setAttribute('aria-hidden', 'false');
    setActiveStoreTab('versions');
    storeContent.innerHTML = `<div class="store-loading">Loading providers...</div>`;
    try {
      const r = await fetch(`/api/servers/${encodeURIComponent(bot)}/versions${storeTemplateQuery()}`);
      if (!r.ok) {
        storeContent.innerHTML = `<div class="store-error">Failed to load providers (${r.status})</div>`;
        return;
      }
      const data = await r.json();
      const providers = Array.isArray(data.providers) ? data.providers : [];
      if (!providers.length) {
        storeContent.innerHTML = '<div class="store-empty">No providers configured for this template.</div>';
        return;
      }
      await renderStoreProvidersView(providers);
    } catch (e) {
      storeContent.innerHTML = `<div class="store-error">Failed to load providers</div>`;
    }
  }
  function closeStoreModal() {
    if (!storeModal) return;
    animateClose(storeModal, () => {
      storeCurrentProvider = null;
    });
  }
  function compareMcVersionsDesc(a, b) {
    const pa = String(a).split('.').map(x => parseInt(x, 10) || 0);
    const pb = String(b).split('.').map(x => parseInt(x, 10) || 0);
    const len = Math.max(pa.length, pb.length);
    for (let i = 0; i < len; i++) {
      const va = typeof pa[i] === 'number' ? pa[i] : 0;
      const vb = typeof pb[i] === 'number' ? pb[i] : 0;
      if (va !== vb) return vb - va;
    }
    return 0;
  }

  async function fetchVanillaVersions({ includeSnapshots = false, limit = 200 } = {}) {
    const MANIFEST = 'https://piston-meta.mojang.com/mc/game/version_manifest_v2.json';
    let manifest;
    try {
      const r = await fetch(MANIFEST);
      if (!r.ok) throw new Error('manifest not ok');
      manifest = await r.json();
    } catch (e) {
      return { versions: [] };
    }
    const latestReleaseId = manifest?.latest?.release || null;
    const latestSnapshotId = manifest?.latest?.snapshot || null;
    let list = Array.isArray(manifest?.versions) ? manifest.versions.slice() : [];
    list = list.filter(v => includeSnapshots ? true : String(v.type) === 'release');
    list.sort((a, b) => new Date(b.releaseTime || b.time || 0) - new Date(a.releaseTime || a.time || 0));
    if (limit && Number.isFinite(limit)) list = list.slice(0, limit);
    const results = [];
    const concurrency = Math.min(8, Math.max(1, list.length));
    let idx = 0;
    async function worker() {
      while (idx < list.length) {
        const i = idx++;
        const v = list[i];
        try {
          const res = await fetch(v.url);
          if (!res.ok) continue;
          const verMeta = await res.json();
          const server = verMeta?.downloads?.server;
          if (!server?.url) continue;
          const mc = String(v.id);
          const date = String(v.releaseTime || v.time || '').split('T')[0] || '';
          const tags = [];
          if (String(v.type) === 'snapshot') tags.push('snapshot'); else tags.push('release');
          if (mc === latestReleaseId) tags.push('latest');
          if (mc === latestSnapshotId) tags.push('latest-snapshot');
          results.push({
            id: mc,
            name: mc,
            label: `Vanilla ${mc}`,
            mcVersion: mc,
            releaseDate: date,
            tags,
            downloadUrl: server.url
          });
        } catch {
        }
      }
    }
    await Promise.all(Array.from({ length: concurrency }, worker));
    try {
      results.sort((a, b) => compareMcVersionsDesc(a.mcVersion, b.mcVersion));
    } catch {
      results.sort((a, b) => (b.releaseDate > a.releaseDate ? 1 : -1));
    }
    return { versions: results };
  }
  async function fetchWithTimeout(url, opts = {}, ms = 8000) {
    const ctrl = new AbortController();
    const id = setTimeout(() => ctrl.abort(), ms);
    try {
      return await fetch(url, { ...opts, signal: ctrl.signal });
    } finally {
      clearTimeout(id);
    }
  }
  function compareSemverDesc(a, b) {
    const pa = a.split('.').map(n => parseInt(n, 10) || 0);
    const pb = b.split('.').map(n => parseInt(n, 10) || 0);
    for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
      const da = pa[i] ?? 0, db = pb[i] ?? 0;
      if (da !== db) return db - da;
    }
    return 0;
  }
  function neoForgeToMcVersion(ver) {
    const m = ver.match(/^(\d+)\.(\d+)\.(\d+)$/);
    if (!m) return '';
    const major = Number(m[1]);
    const minor = Number(m[2]);
    return `1.${major}.${minor}`;
  }
  async function addLastModifiedDates(list, urlFn, pool = 6, timeout = 5000) {
    let idx = 0;
    const workers = [];
    async function worker() {
      while (idx < list.length) {
        const i = idx++;
        const u = urlFn(list[i]);
        try {
          const r = await fetchWithTimeout(u, { method: 'HEAD' }, timeout);
          const lm = r.headers.get('last-modified');
          if (lm) list[i].releaseDate = new Date(lm).toISOString().slice(0, 10);
        } catch (_) { }
      }
    }
    for (let i = 0; i < pool; i++) workers.push(worker());
    await Promise.all(workers);
  }
  async function fetchPapermcVersions() {
    if (papermcVersionsCache) return papermcVersionsCache;
    const base = '/api/papermc/projects/paper';
    const dlBase = 'https://api.papermc.io/v2/projects/paper';
    let proj;
    try {
      const projRes = await fetch(base);
      if (!projRes.ok) throw new Error('Failed to load Paper project');
      proj = await projRes.json().catch(() => ({}));
    } catch (e) {
      papermcVersionsCache = { versions: [] };
      return papermcVersionsCache;
    }
    let versions = Array.isArray(proj.versions) ? proj.versions.slice() : [];
    if (!versions.length) {
      papermcVersionsCache = { versions: [] };
      return papermcVersionsCache;
    }
    versions.sort(compareMcVersionsDesc);
    const results = [];
    const concurrency = 8;
    let idx = 0;
    async function worker() {
      while (idx < versions.length) {
        const i = idx++;
        const ver = versions[i];
        try {
          const verRes = await fetch(`${base}/versions/${encodeURIComponent(ver)}`);
          if (!verRes.ok) continue;
          const verData = await verRes.json().catch(() => ({}));
          const builds = Array.isArray(verData.builds) ? verData.builds : [];
          if (!builds.length) continue;
          const lastBuild = builds[builds.length - 1];
          let buildMeta = null;
          try {
            const buildRes = await fetch(`${base}/versions/${encodeURIComponent(ver)}/builds/${lastBuild}`);
            if (buildRes.ok) {
              buildMeta = await buildRes.json().catch(() => null);
            }
          } catch (_) { }
          const jarName = `paper-${ver}-${lastBuild}.jar`;
          const downloadUrl = `${dlBase}/versions/${encodeURIComponent(ver)}/builds/${lastBuild}/downloads/${encodeURIComponent(jarName)}`;
          const entry = {
            id: String(ver),
            name: String(ver),
            label: `Paper ${ver}`,
            mcVersion: String(ver),
            releaseDate: (buildMeta && typeof buildMeta.time === 'string')
              ? buildMeta.time.split('T')[0]
              : '',
            tags: [],
            build: lastBuild,
            downloadUrl
          };
          const tags = entry.tags;
          const channel = buildMeta && typeof buildMeta.channel === 'string'
            ? buildMeta.channel.toLowerCase()
            : '';
          if (channel) {
            if (channel === 'default' || channel === 'stable') {
              if (!tags.includes('stable')) tags.push('stable');
            } else {
              if (!tags.includes('unstable')) tags.push('unstable');
              if (!tags.includes(channel)) tags.push(channel);
            }
          } else {
            if (!tags.includes('stable')) tags.push('stable');
          }
          if (buildMeta && buildMeta.promoted) {
            if (!tags.includes('recommended')) tags.push('recommended');
          }
          results.push(entry);
        } catch (e) { }
      }
    }
    const workers = [];
    for (let i = 0; i < concurrency; i++) {
      workers.push(worker());
    }
    await Promise.all(workers);
    results.sort((a, b) => compareMcVersionsDesc(a.mcVersion, b.mcVersion));
    if (results.length) {
      const latestTags = results[0].tags || (results[0].tags = []);
      if (!latestTags.includes('latest')) latestTags.push('latest');
    }
    papermcVersionsCache = { versions: results };
    return papermcVersionsCache;
  }
  async function fetchVelocityVersions() {
    const base = '/api/papermc/projects/velocity';
    const dlBase = 'https://api.papermc.io/v2/projects/velocity';
    let proj;
    try {
      const res = await fetch(base);
      if (!res.ok) throw new Error('Failed to load Velocity project');
      proj = await res.json().catch(() => ({}));
    } catch (e) {
      return { versions: [] };
    }
    let versions = Array.isArray(proj.versions) ? proj.versions.slice() : [];
    if (!versions.length) return { versions: [] };
    versions.sort(compareMcVersionsDesc);
    const results = [];
    const concurrency = 8;
    let idx = 0;
    async function worker() {
      while (idx < versions.length) {
        const i = idx++;
        const ver = versions[i];
        try {
          const verRes = await fetch(`${base}/versions/${encodeURIComponent(ver)}`);
          if (!verRes.ok) continue;
          const verData = await verRes.json().catch(() => ({}));
          const builds = Array.isArray(verData.builds) ? verData.builds : [];
          if (!builds.length) continue;
          const lastBuild = builds[builds.length - 1];
          let buildMeta = null;
          try {
            const buildRes = await fetch(`${base}/versions/${encodeURIComponent(ver)}/builds/${lastBuild}`);
            if (buildRes.ok) {
              buildMeta = await buildRes.json().catch(() => null);
            }
          } catch (_) { }
          let fileName = null;
          if (buildMeta && buildMeta.downloads) {
            if (Array.isArray(buildMeta.downloads)) {
              const app = buildMeta.downloads.find(d => d && d.name && /\.jar$/i.test(d.name)) || buildMeta.downloads[0];
              fileName = app && app.name;
            } else if (buildMeta.downloads.application && buildMeta.downloads.application.name) {
              fileName = buildMeta.downloads.application.name;
            }
          }
          if (!fileName) fileName = `velocity-${ver}-${lastBuild}.jar`;
          const downloadUrl = `${dlBase}/versions/${encodeURIComponent(ver)}/builds/${lastBuild}/downloads/${encodeURIComponent(fileName)}`;
          const entry = {
            id: String(ver),
            name: String(ver),
            label: `Velocity ${ver}`,
            mcVersion: "",
            releaseDate: (buildMeta && typeof buildMeta.time === 'string') ? buildMeta.time.split('T')[0] : '',
            tags: [],
            build: lastBuild,
            downloadUrl
          };
          const tags = entry.tags;
          const channel = buildMeta && typeof buildMeta.channel === 'string' ? buildMeta.channel.toLowerCase() : '';
          if (channel) {
            if (channel === 'default' || channel === 'stable') {
              if (!tags.includes('stable')) tags.push('stable');
            } else {
              if (!tags.includes('unstable')) tags.push('unstable');
              if (!tags.includes(channel)) tags.push(channel);
            }
          } else {
            if (!tags.includes('stable')) tags.push('stable');
          }
          if (buildMeta && buildMeta.promoted) { if (!tags.includes('recommended')) tags.push('recommended'); }
          results.push(entry);
        } catch (_) { }
      }
    }
    const workers = [];
    for (let i = 0; i < concurrency; i++) workers.push(worker());
    await Promise.all(workers);
    results.sort((a, b) => compareMcVersionsDesc(a.name, b.name));
    if (results.length) {
      const t = results[0].tags || (results[0].tags = []);
      if (!t.includes('latest')) t.push('latest');
    }
    return { versions: results };
  }
  async function fetchPurpurVersions() {
    const base = '/api/purpurmc/purpur';
    const dlBase = 'https://api.purpurmc.org/v2/purpur';
    let proj;
    try {
      const res = await fetch(base);
      if (!res.ok) throw new Error('Failed to load Purpur project');
      proj = await res.json().catch(() => ({}));
    } catch (e) {
      return { versions: [] };
    }
    let versions = Array.isArray(proj.versions) ? proj.versions.slice() : [];
    if (!versions.length) return { versions: [] };
    versions.sort(compareMcVersionsDesc);
    const results = [];
    for (const ver of versions) {
      try {
        const latestRes = await fetch(`${base}/${encodeURIComponent(ver)}/latest`);
        if (!latestRes.ok) continue;
        const latest = await latestRes.json().catch(() => ({}));
        const build = latest.build;
        const time = latest.timestamp || latest.time || '';
        const date = typeof time === 'string' ? time.split('T')[0] : '';
        const downloadUrl = `${dlBase}/${encodeURIComponent(ver)}/${encodeURIComponent(build)}/download`;
        const entry = {
          id: String(ver),
          name: String(ver),
          label: `Purpur ${ver}`,
          mcVersion: String(ver),
          releaseDate: date,
          tags: ['stable'],
          build,
          downloadUrl
        };
        results.push(entry);
      } catch (e) { }
    }
    if (results.length) {
      const t = results[0].tags || (results[0].tags = []);
      if (!t.includes('latest')) t.push('latest');
    }
    return { versions: results };
  }
  async function fetchLeavesVersions() {
    const base = 'https://api.github.com/repos/LeavesMC/Leaves/releases';
    const pages = [1, 2, 3, 4, 5];
    const perPage = 990;
    const headers = { 'Accept': 'application/vnd.github+json' };
    const chunks = await Promise.all(pages.map(async (p) => {
      try {
        const res = await fetch(`${base}?per_page=${perPage}&page=${p}`, { headers });
        if (!res.ok) return [];
        const arr = await res.json();
        return Array.isArray(arr) ? arr : [];
      } catch {
        return [];
      }
    }));
    const all = chunks.flat().filter(x => x && !x.draft && !x.prerelease);
    all.sort((a, b) => {
      const da = new Date(a.published_at || a.created_at || 0).getTime();
      const db = new Date(b.published_at || b.created_at || 0).getTime();
      return db - da;
    });
    const seen = new Set();
    const out = [];
    function extractMcVersionFromRelease(rel) {
      const t = String(rel.tag_name || rel.name || '');
      let m = t.match(/(\d+\.\d+(?:\.\d+)?)/);
      if (m) return m[1];
      if (Array.isArray(rel.assets)) {
        for (const a of rel.assets) {
          const s = String(a.name || a.browser_download_url || '');
          const mm = s.match(/(\d+\.\d+(?:\.\d+)?)/);
          if (mm) return mm[1];
        }
      }
      return null;
    }
    function pickJarAsset(rel) {
      if (!Array.isArray(rel.assets)) return { url: null, sha256: null };
      let pref = rel.assets.find(a => /\.jar$/i.test(a?.name || '') &&
        /leaves-\d+\.\d+(?:\.\d+)?\.jar$/i.test(a?.name || ''));
      if (!pref) pref = rel.assets.find(a => /\.jar$/i.test(a?.name || ''));
      if (!pref) return { url: null, sha256: null };
      let sha = null;
      if (typeof pref.digest === 'string' && pref.digest.startsWith('sha256:')) {
        sha = pref.digest.split(':')[1];
      }
      return { url: pref.browser_download_url || null, sha256: sha };
    }
    for (const rel of all) {
      const mc = extractMcVersionFromRelease(rel);
      if (!mc) continue;
      if (seen.has(mc)) continue;
      const { url: downloadUrl, sha256 } = pickJarAsset(rel);
      const releaseDate = String(rel.published_at || rel.created_at || '').split('T')[0] || '';
      out.push({
        id: mc,
        name: mc,
        label: `Leaves ${mc}`,
        mcVersion: mc,
        releaseDate,
        tags: ['github'],
        downloadUrl,
        tag: rel.tag_name || '',
        url: rel.html_url || '',
        sha256: sha256 || null
      });
      seen.add(mc);
    }
    out.sort((a, b) => compareMcVersionsDesc(a.mcVersion, b.mcVersion));
    if (out.length) {
      const t = out[0].tags || (out[0].tags = []);
      if (!t.includes('latest')) t.push('latest');
    }
    return { versions: out };
  }
  async function fetchNeoForgeVersions() {
    const META_URL = 'https://maven.neoforged.net/releases/net/neoforged/neoforge/maven-metadata.xml';
    const res = await fetch(META_URL);
    if (!res.ok) return { versions: [] };
    const xml = await res.text();
    const doc = new DOMParser().parseFromString(xml, 'application/xml');
    const versions = [...doc.querySelectorAll('versioning > versions > version')]
      .map(v => v.textContent.trim())
      .filter(Boolean)
      .sort(compareSemverDesc);
    function toMcVersion(neoforgeVer) {
      const m = neoforgeVer.match(/^(\d+)\.(\d+)\.(\d+)$/);
      if (!m) return '';
      const major = Number(m[1]);
      const minor = Number(m[2]);
      return `1.${major}.${minor}`;
    }

    async function fetchNode(url, opts = {}, timeoutMs = 3500) {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), timeoutMs);

      try {
        const r = await fetch(url, { ...opts, signal: ctrl.signal, cache: "no-store" });
        setNodeOnline(true);
        return r;
      } catch (e) {
        setNodeOnline(false);
        throw e;
      } finally {
        clearTimeout(t);
      }
    }

    async function getLastModified(url) {
      try {
        const head = await fetch(url, { method: 'HEAD' });
        const lm = head.headers.get('last-modified');
        if (!lm) return '';
        const d = new Date(lm);
        if (isNaN(d.getTime())) return '';
        return d.toISOString().slice(0, 10);
      } catch {
        return '';
      }
    }
    const out = [];
    for (const ver of versions) {
      const mcVersion = toMcVersion(ver);
      const base = `https://maven.neoforged.net/releases/net/neoforged/neoforge/${ver}/neoforge-${ver}`;
      const installerUrl = `${base}-installer.jar`;
      const universalUrl = `${base}-universal.jar`;
      const releaseDate = await getLastModified(installerUrl);
      out.push({
        id: ver,
        name: ver,
        label: `NeoForge ${mcVersion || ver}`,
        mcVersion: mcVersion || '',
        releaseDate: releaseDate || '',
        tags: ['stable'],
        build: ver.split('.').pop() || '',
        downloadUrl: installerUrl,
        altDownloads: { installer: installerUrl, universal: universalUrl }
      });
    }
    if (out[0] && !out[0].tags.includes('latest')) out[0].tags.push('latest');
    return { versions: out };
    function compareSemverDesc(a, b) {
      const pa = a.split('.').map(n => parseInt(n, 10) || 0);
      const pb = b.split('.').map(n => parseInt(n, 10) || 0);
      for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
        const da = pa[i] ?? 0, db = pb[i] ?? 0;
        if (da !== db) return db - da;
      }
      return 0;
    }
  }
  async function fetchSpigotVersions() {
    const BASE = "https://api.github.com/repos/BaldGang/spigot-build/releases?per_page=1";
    function compareDesc(a, b) {
      const pa = String(a).split('.').map(n => parseInt(n, 10) || 0);
      const pb = String(b).split('.').map(n => parseInt(n, 10) || 0);
      const len = Math.max(pa.length, pb.length);
      for (let i = 0; i < len; i++) {
        const va = pa[i] ?? 0, vb = pb[i] ?? 0;
        if (va !== vb) return vb - va;
      }
      return 0;
    }
    const cmp = (typeof compareMcVersionsDesc === "function") ? compareMcVersionsDesc : compareDesc;
    try {
      const res = await fetch(BASE, {
        headers: { "Accept": "application/vnd.github+json" }
      });
      if (!res.ok) throw new Error(`GitHub API ${res.status}`);
      const releases = await res.json();
      if (!Array.isArray(releases) || releases.length === 0) {
        return { versions: [] };
      }
      const rel = releases[0];
      const assets = Array.isArray(rel.assets) ? rel.assets : [];
      const byVersion = new Map();
      for (const a of assets) {
        const name = a?.name ?? "";
        const m = name.match(/^spigot-(\d+\.\d+(?:\.\d+)?).jar$/i);
        if (!m || !a.browser_download_url) continue;
        const ver = m[1];
        const rawDate = a.updated_at || a.created_at || rel.published_at || rel.created_at || "";
        const dateOnly = String(rawDate).split("T")[0] || "";
        const candidate = {
          version: ver,
          url: a.browser_download_url,
          date: dateOnly,
          _ts: Date.parse(a.updated_at || a.created_at || rel.published_at || rel.created_at || 0) || 0
        };
        const existing = byVersion.get(ver);
        if (!existing || candidate._ts > existing._ts) {
          byVersion.set(ver, candidate);
        }
      }
      if (byVersion.size === 0) {
        return { versions: [] };
      }
      const candidates = Array.from(byVersion.values())
        .sort((x, y) => cmp(x.version, y.version));
      const out = candidates.map((c) => ({
        id: c.version,
        name: c.version,
        label: `SpigotMC ${c.version}`,
        mcVersion: c.version,
        releaseDate: c.date,
        tags: ["stable"],
        build: null,
        downloadUrl: c.url
      }));
      out[0].tags = ["latest", "stable"];
      return { versions: out };
    } catch (e) {
      console.error("fetchSpigotVersions failed:", e);
      return { versions: [] };
    }
  }
  async function fetchFoliaVersions() {
    const base = 'https://api.papermc.io/v2/projects/folia';
    let proj;
    try {
      const res = await fetch(base);
      if (!res.ok) throw new Error('Failed to load Folia project');
      proj = await res.json().catch(() => ({}));
    } catch (e) {
      return { versions: [] };
    }
    let versions = Array.isArray(proj.versions) ? proj.versions.slice() : [];
    if (!versions.length) return { versions: [] };
    versions.sort(compareMcVersionsDesc);
    const out = [];
    const concurrency = 8;
    let idx = 0;
    async function worker() {
      while (idx < versions.length) {
        const i = idx++;
        const ver = String(versions[i]);
        try {
          const verRes = await fetch(`${base}/versions/${encodeURIComponent(ver)}`);
          if (!verRes.ok) continue;
          const verData = await verRes.json().catch(() => ({}));
          const builds = Array.isArray(verData.builds) ? verData.builds : [];
          if (!builds.length) continue;
          const lastBuild = builds[builds.length - 1];
          let buildMeta = null;
          try {
            const bRes = await fetch(`${base}/versions/${encodeURIComponent(ver)}/builds/${lastBuild}`);
            if (bRes.ok) buildMeta = await bRes.json().catch(() => null);
          } catch (_) { }
          let jarName = null;
          if (buildMeta && buildMeta.downloads) {
            if (buildMeta.downloads.application && buildMeta.downloads.application.name) {
              jarName = buildMeta.downloads.application.name;
            } else if (Array.isArray(buildMeta.downloads)) {
              const anyJar = buildMeta.downloads.find(d => d && d.name && /\.jar$/i.test(d.name));
              if (anyJar) jarName = anyJar.name;
            }
          }
          if (!jarName) jarName = `folia-${ver}-${lastBuild}.jar`;
          const downloadUrl = `${base}/versions/${encodeURIComponent(ver)}/builds/${lastBuild}/downloads/${encodeURIComponent(jarName)}`;
          const date = (buildMeta && typeof buildMeta.time === 'string') ? buildMeta.time.split('T')[0] : '';
          const entry = {
            id: ver,
            name: ver,
            label: `Folia ${ver}`,
            mcVersion: ver,
            releaseDate: date,
            tags: [],
            build: lastBuild,
            downloadUrl
          };
          const channel = buildMeta && typeof buildMeta.channel === 'string'
            ? buildMeta.channel.toLowerCase()
            : '';
          if (channel) {
            if (channel === 'default' || channel === 'stable') entry.tags.push('stable');
            else entry.tags.push('unstable', channel);
          } else {
            entry.tags.push('stable');
          }
          if (buildMeta && buildMeta.promoted) entry.tags.push('recommended');
          out.push(entry);
        } catch (_) { }
      }
    }
    const workers = [];
    for (let i = 0; i < concurrency; i++) workers.push(worker());
    await Promise.all(workers);
    out.sort((a, b) => compareMcVersionsDesc(a.mcVersion || a.name, b.mcVersion || b.name));
    if (out.length) {
      const t = out[0].tags || (out[0].tags = []);
      if (!t.includes('latest')) t.push('latest');
    }
    return { versions: out };
  }
  async function fetchWaterfallVersions() {
    const base = 'https://api.papermc.io/v2/projects/waterfall';
    let proj;
    try {
      const res = await fetch(base);
      if (!res.ok) throw new Error('Failed to load Waterfall project');
      proj = await res.json().catch(() => ({}));
    } catch (e) {
      return { versions: [] };
    }
    let versions = Array.isArray(proj.versions) ? proj.versions.slice() : [];
    if (!versions.length) return { versions: [] };
    versions.sort(compareMcVersionsDesc);
    const out = [];
    const concurrency = 8;
    let idx = 0;
    async function worker() {
      while (idx < versions.length) {
        const i = idx++;
        const ver = String(versions[i]);
        try {
          const verRes = await fetch(`${base}/versions/${encodeURIComponent(ver)}`);
          if (!verRes.ok) continue;
          const verData = await verRes.json().catch(() => ({}));
          const builds = Array.isArray(verData.builds) ? verData.builds : [];
          if (!builds.length) continue;
          const lastBuild = builds[builds.length - 1];
          let buildMeta = null;
          try {
            const bRes = await fetch(`${base}/versions/${encodeURIComponent(ver)}/builds/${lastBuild}`);
            if (bRes.ok) buildMeta = await bRes.json().catch(() => null);
          } catch (_) { }
          let jarName = null;
          if (buildMeta && buildMeta.downloads) {
            if (buildMeta.downloads.application && buildMeta.downloads.application.name) {
              jarName = buildMeta.downloads.application.name;
            } else if (Array.isArray(buildMeta.downloads)) {
              const anyJar = buildMeta.downloads.find(d => d && d.name && /\.jar$/i.test(d.name));
              if (anyJar) jarName = anyJar.name;
            }
          }
          if (!jarName) jarName = `Waterfall-${ver}-${lastBuild}.jar`;
          const downloadUrl = `${base}/versions/${encodeURIComponent(ver)}/builds/${lastBuild}/downloads/${encodeURIComponent(jarName)}`;
          const date = (buildMeta && typeof buildMeta.time === 'string') ? buildMeta.time.split('T')[0] : '';
          const entry = {
            id: ver,
            name: ver,
            label: `Waterfall ${ver}`,
            mcVersion: ver,
            releaseDate: date,
            tags: [],
            build: lastBuild,
            downloadUrl
          };
          const channel = buildMeta && typeof buildMeta.channel === 'string'
            ? buildMeta.channel.toLowerCase()
            : '';
          if (channel) {
            if (channel === 'default' || channel === 'stable') entry.tags.push('stable');
            else entry.tags.push('unstable', channel);
          } else {
            entry.tags.push('stable');
          }
          if (buildMeta && buildMeta.promoted) entry.tags.push('recommended');
          out.push(entry);
        } catch (_) { }
      }
    }
    const workers = [];
    for (let i = 0; i < concurrency; i++) workers.push(worker());
    await Promise.all(workers);
    out.sort((a, b) => compareMcVersionsDesc(a.mcVersion || a.name, b.mcVersion || b.name));
    if (out.length) {
      const t = out[0].tags || (out[0].tags = []);
      if (!t.includes('latest')) t.push('latest');
    }
    return { versions: out };
  }
  async function fetchStoreProviders() {
    const res = await fetch(`/api/servers/${encodeURIComponent(bot)}/versions${storeTemplateQuery()}`);
    if (!res.ok) throw new Error('Failed to load providers');
    const j = await res.json();
    storeProvidersCache = j.providers || [];
    return j;
  }
  async function fetchStoreVersions(providerId) {
    const pid = (providerId || '').toString().toLowerCase();
    if (pid.includes('vanilla')) return await fetchVanillaVersions();
    if (pid.includes('paper')) return await fetchPapermcVersions();
    if (pid.includes('purpur')) return await fetchPurpurVersions();
    if (pid.includes('velocity')) return await fetchVelocityVersions();
    if (pid.includes('waterfall')) return await fetchWaterfallVersions();
    if (pid.includes('folia')) return await fetchFoliaVersions();
    if (pid.includes('leaves')) return await fetchLeavesVersions();
    if (pid.includes('spigot')) return await fetchSpigotVersions();
    if (pid.includes('python')) return await fetchPythonVersions();
    if (pid.includes('node')) return await loadNodeVersions();
    const res = await fetch(`/api/servers/${encodeURIComponent(bot)}/versions/${encodeURIComponent(providerId)}${storeTemplateQuery()}`);
    if (!res.ok) throw new Error('Failed to load provider versions');
    return await res.json();
  }
  async function fetchPythonVersions() {
    const list = document.getElementById("list");
    const response = await fetch("https://api.github.com/repos/python/cpython/tags");
    if (!response.ok) throw new Error('Failed to load Python versions');
    const tags = await response.json();
    const versions = (tags || []).map(tag => {
      const raw = (tag && tag.name) ? String(tag.name) : '';
      const clean = raw.replace(/^v/, '');
      return {
        id: raw,
        name: clean,
        label: `Python ${clean || raw || 'unknown'}`,
        releaseDate: '',
        tags: ['PYTHON']
      };
    });
    if (list) {
      list.innerHTML = versions.map(v => `
        <div class="item">
            <b>${escapeHtml(v.label)}</b>
            <br><br>
            <button data-version="${escapeHtml(v.id)}" data-kind="python">
                Schimba containerul in versiunea ${escapeHtml(v.id)}
            </button>
        </div>
    `).join("");
    }
    return { versions };
  }
  async function loadNodeVersions() {
    const list = document.getElementById("list");
    const res = await fetch("https://nodejs.org/dist/index.json");
    if (!res.ok) throw new Error('Failed to load Node.js versions');
    const versionsRaw = await res.json();
    const versions = (versionsRaw || [])
      .filter(v => v && typeof v.version === 'string' && /^v?\d+\.\d+\.\d+/.test(v.version))
      .map(v => ({
        id: v.version,
        name: v.version.replace(/^v/, ''),
        label: v.version,
        releaseDate: v.date || '',
        tags: v.lts ? ['LTS'] : ['LATEST']
      }));
    if (list) {
      list.innerHTML = versions.map(v => `
        <div class="item">
            <b>${escapeHtml(v.label)}</b>
            <br><br>
            <button data-version="${escapeHtml(v.id)}" data-kind="node">
                Schimba containerul in ${escapeHtml(v.id)}
            </button>
        </div>
    `).join("");
    }
    return { versions };
  }
  async function submitStoreApplyRequest(payload, opts = {}) {
    const {
      confirmMessage = '',
      skipConfirm = false,
      progressMessage = 'Applying version...',
      successMessage = 'Version applied',
      errorPrefix = 'Apply failed',
      silentSuccess = false
    } = opts;

    if (!hasPerm('store_access')) throw new Error('Unauthorized');
    if (!skipConfirm && confirmMessage && !confirm(confirmMessage)) return { cancelled: true };

    showProgressWheel(3, progressMessage);
    try {
      const response = await applyServerVersionWithActionToken(bot, payload);
      const result = await response.json().catch(() => null);
      if (response.status === 507 || (result && result.error === 'disk_limit_exceeded')) {
        throw new Error((result && result.message) ? result.message : 'This server has exceeded its disk space limit.');
      }
      if (!response.ok || !result?.ok) {
        throw new Error(result?.detail || result?.error || `HTTP ${response.status}`);
      }
      try { await loadServerInfo(); updateStoreVisibility(); } catch { }
      if (!silentSuccess) alert(result.message || successMessage);
      return result;
    } catch (e) {
      throw new Error(`${errorPrefix}: ${e?.message || e}`);
    } finally {
      setTimeout(hideProgressWheel, 450);
    }
  }
  async function changeNodeVersion(version, opts = {}) {
    const payload = { providerId: 'nodejs', versionId: version };
    const tpl = resolveStoreTemplate().template;
    if (tpl) payload.template = tpl;
    return submitStoreApplyRequest(payload, {
      confirmMessage: 'Switching Node.js versions will delete all existing files on this server. Continue?',
      skipConfirm: !!opts.skipConfirm,
      progressMessage: `Applying Node.js ${version}...`,
      successMessage: 'Node.js version changed',
      errorPrefix: 'Failed to change Node.js version',
      silentSuccess: !!opts.silentSuccess
    });
  }
  async function changeVersion(version, opts = {}) {
    const payload = { providerId: 'python', versionId: version };
    const tpl = resolveStoreTemplate().template;
    if (tpl) payload.template = tpl;
    return submitStoreApplyRequest(payload, {
      confirmMessage: 'Switching Python versions will delete all existing files on this server. Continue?',
      skipConfirm: !!opts.skipConfirm,
      progressMessage: `Applying Python ${version}...`,
      successMessage: 'Python version changed',
      errorPrefix: 'Failed to change Python version',
      silentSuccess: !!opts.silentSuccess
    });
  }
  async function renderStoreProvidersView(providersFromServer = null) {
    if (!storeContent) return;
    setActiveStoreTab('versions');
    storeCurrentProvider = null;
    storeContent.innerHTML = '<div class="store-loading">Loading providers...</div>';
    try {
      const providers = providersFromServer || (await fetchStoreProviders(), storeProvidersCache);
      if (!providers || providers.length === 0) {
        storeContent.innerHTML = '<div class="store-empty">No providers configured in versions.json.</div>';
        return;
      }
      const grid = document.createElement('div');
      grid.className = 'store-content-grid';
      providers.forEach(p => {
        const card = document.createElement('button');
        card.className = 'store-provider-card';
        card.type = 'button';
        card.addEventListener('click', () => openProviderDetails(p));
        const logoWrap = document.createElement('div');
        logoWrap.className = 'store-provider-logo';
        if (p.logo) {
          const img = document.createElement('img');
          img.src = p.logo;
          img.alt = p.name || p.id || '';
          logoWrap.appendChild(img);
        } else {
          const span = document.createElement('span');
          span.className = 'store-provider-logo-placeholder';
          span.textContent = (p.name || p.id || '?').slice(0, 2).toUpperCase();
          logoWrap.appendChild(span);
        }
        const body = document.createElement('div');
        body.className = 'store-provider-body';
        const title = document.createElement('div');
        title.className = 'store-provider-name';
        title.textContent = p.name || p.id;
        const desc = document.createElement('div');
        desc.className = 'store-provider-desc';
        desc.textContent = p.description || '';
        body.appendChild(title);
        body.appendChild(desc);
        const chevron = document.createElement('div');
        chevron.className = 'store-provider-chevron';
        chevron.innerHTML = '<i class="fa-solid fa-chevron-right"></i>';
        card.appendChild(logoWrap);
        card.appendChild(body);
        card.appendChild(chevron);
        grid.appendChild(card);
      });
      storeContent.innerHTML = '';
      storeContent.appendChild(grid);
    } catch (err) {
      storeContent.innerHTML = `<div class="store-error">Failed to load providers: ${escapeHtml(err.message || err)}</div>`;
    }
  }
  async function applyVersion(providerId, versionId, displayLabel, downloadUrl, opts = {}) {
    if (!hasPerm('store_access')) throw new Error('Unauthorized');
    const label = displayLabel || versionId;
    const pid = (providerId || '').toString().toLowerCase();
    if (pid === 'python') {
      return changeVersion(versionId, opts);
    }
    if (pid.includes('node')) {
      return changeNodeVersion(versionId, opts);
    }
    const payload = { providerId, versionId };
    if (downloadUrl) payload.url = downloadUrl;
    if (opts && opts.destPath) payload.destPath = opts.destPath;
    const templateOverride = normalizeTemplate(opts?.template);
    if (templateOverride) payload.template = templateOverride;
    else {
      const tpl = resolveStoreTemplate().template;
      if (tpl) payload.template = tpl;
    }
    return submitStoreApplyRequest(payload, {
      confirmMessage: `Install "${label}" on "${bot}"?`,
      skipConfirm: !!opts.skipConfirm,
      progressMessage: `Applying ${label}...`,
      successMessage: `Installed ${label}`,
      errorPrefix: 'Apply failed',
      silentSuccess: !!opts.silentSuccess
    });
  }
  window.applyVersion = applyVersion;
  async function openProviderDetails(provider) {
    if (!storeContent) return;
    storeCurrentProvider = provider;
    storeContent.innerHTML = '<div class="store-loading">Loading versions...</div>';
    try {
      const data = await fetchStoreVersions(provider.id || provider.provider || '');
      const versions = data.versions || [];
      const wrapper = document.createElement('div');
      const header = document.createElement('div');
      header.className = 'store-provider-header';
      const left = document.createElement('div');
      left.className = 'store-provider-header-main';
      const logoWrap = document.createElement('div');
      logoWrap.className = 'store-provider-logo';
      if (provider.logo) {
        const img = document.createElement('img');
        img.src = provider.logo;
        img.alt = provider.name || provider.id || '';
        img.className = 'store-provider-header-logo';
        logoWrap.appendChild(img);
      } else {
        const span = document.createElement('span');
        span.className = 'store-provider-logo-placeholder';
        span.textContent = (provider.name || provider.id || '?').slice(0, 2).toUpperCase();
        logoWrap.appendChild(span);
      }
      const text = document.createElement('div');
      const nameEl = document.createElement('div');
      nameEl.className = 'store-provider-header-name';
      nameEl.textContent = provider.name || data.displayName || provider.id;
      const descEl = document.createElement('div');
      descEl.className = 'store-provider-header-desc';
      descEl.textContent = provider.description || data.description || '';
      text.appendChild(nameEl);
      text.appendChild(descEl);
      left.appendChild(logoWrap);
      left.appendChild(text);
      const backBtn = document.createElement('button');
      backBtn.type = 'button';
      backBtn.className = 'store-back-btn';
      backBtn.innerHTML = '<i class="fa-solid fa-arrow-left"></i> Back';
      backBtn.addEventListener('click', () => renderStoreProvidersView());
      header.appendChild(left);
      header.appendChild(backBtn);
      wrapper.appendChild(header);
      const note = document.createElement('div');
      note.className = 'store-note';
      if ((provider.id || provider.provider || '').toString().toLowerCase() === 'python') {
        note.textContent = 'WARNING: Switching Python runtimes will delete all existing files on this server.';
      } else {
        note.textContent = 'NOTE: Click on a version to install it into the server (ONE-TAP)';
      }
      wrapper.appendChild(note);
      if (!versions.length) {
        const empty = document.createElement('div');
        empty.className = 'store-empty';
        empty.textContent = 'No versions defined for this provider.';
        wrapper.appendChild(empty);
      } else {
        const list = document.createElement('div');
        list.className = 'store-version-list';
        versions.forEach(v => {
          const row = document.createElement('div');
          row.className = 'store-version-row';
          const meta = document.createElement('div');
          meta.className = 'store-version-meta';
          const vn = document.createElement('div');
          vn.className = 'store-version-name';
          vn.textContent = v.label || v.name || v.id;
          const vs = document.createElement('div');
          vs.className = 'store-version-sub';
          const parts = [];
          if (v.mcVersion) parts.push(`MC ${v.mcVersion}`);
          if (v.releaseDate) parts.push(v.releaseDate);
          if (!parts.length && v.name) parts.push(String(v.name));
          const detail = parts.join(' • ');
          vs.textContent = detail || (v.description || '');
          meta.appendChild(vn);
          meta.appendChild(vs);
          const tags = document.createElement('div');
          tags.className = 'store-version-tags';
          (v.tags || []).forEach(tag => {
            if (!tag) return;
            const t = String(tag).toLowerCase();
            const pill = document.createElement('span');
            pill.className = 'pill ' + (t === 'latest' || t === 'recommended' ? 'pill-green' : 'pill-muted');
            pill.textContent = t.toUpperCase();
            tags.appendChild(pill);
          });
          row.appendChild(meta);
          row.appendChild(tags);
          row.addEventListener('click', () => {
            const id = v.id || v.name || v.version || '';
            const label = v.label || v.name || id;
            const downloadUrl = v.downloadUrl || v.url || null;
            const { template: tpl } = resolveStoreTemplate();
            if (tpl) {
              const friendly = templateDisplayName(tpl);
              const proceed = confirm(`Please note your server will switch to ${friendly} template, do you want to continue?`);
              if (!proceed) return;
            }
            applyVersion(provider.id || provider.provider || '', id, label, downloadUrl)
              .catch((err) => alert(err?.message || err));
          });
          list.appendChild(row);
        });
        wrapper.appendChild(list);
      }
      storeContent.innerHTML = '';
      storeContent.appendChild(wrapper);
    } catch (err) {
      storeContent.innerHTML = `<div class="store-error">Failed to load versions: ${escapeHtml(err.message || err)}</div>`;
    }
  }
  function renderPluginsPlaceholder() {
    if (!storeContent) return;
    setActiveStoreTab('plugins');
    storeContent.innerHTML = '<div class="store-placeholder"><strong>Plugins</strong> management UI will live here.</div>';
  }
  function renderPluginsUI() {
    if (!storeContent) return;
    setActiveStoreTab('plugins');
    storeContent.innerHTML = `
          <div class="mr-plugins-root">
            <div class="mr-plugins-header">
              <input id="mrPluginSearch" type="text" placeholder="Search plugins on Modrinth..." />
              <button id="mrPluginReload" class="store-tab"><i class="fa-solid fa-rotate"></i>&nbsp;Refresh</button>
            </div>
            <div id="mrPluginGrid" class="mr-plugin-grid"></div>
            <button id="mrPluginLoadMore" class="store-tab" style="margin-top:10px;align-self:flex-start;"><i class="fa-solid fa-angles-down"></i>&nbsp;Load more</button>
          </div>
        `;
    const searchInput = document.getElementById("mrPluginSearch");
    const reloadBtn = document.getElementById("mrPluginReload");
    const loadMoreBtn = document.getElementById("mrPluginLoadMore");
    mrPluginsState.offset = 0;
    mrPluginsState.more = true;
    mrPluginsState.loading = false;
    mrPluginsState.query = "";
    let searchTimeout = null;
    searchInput.addEventListener("input", () => {
      const q = searchInput.value.trim();
      if (searchTimeout) clearTimeout(searchTimeout);
      searchTimeout = setTimeout(() => {
        mrPluginsState.query = q;
        loadModrinthPlugins(true);
      }, 400);
    });
    reloadBtn.addEventListener("click", () => {
      searchInput.value = "";
      mrPluginsState.query = "";
      loadModrinthPlugins(true);
    });
    loadMoreBtn.addEventListener("click", () => {
      loadModrinthPlugins(false);
    });
    loadModrinthPlugins(true);
  }
  async function loadModrinthPlugins(reset) {
    const grid = document.getElementById("mrPluginGrid");
    const loadMoreBtn = document.getElementById("mrPluginLoadMore");
    if (!grid) return;
    if (mrPluginsState.loading) return;
    if (reset) {
      mrPluginsState.offset = 0;
      mrPluginsState.more = true;
      grid.innerHTML = '<div class="store-loading">Loading plugins from Modrinth...</div>';
    } else if (!mrPluginsState.more) {
      return;
    }
    mrPluginsState.loading = true;
    try {
      const facets = encodeURIComponent('[["project_type:plugin"]]');
      let url = `https://api.modrinth.com/v2/search?limit=${mrPluginsState.limit}&offset=${mrPluginsState.offset}&facets=${facets}`;
      if (mrPluginsState.query) url += `&query=${encodeURIComponent(mrPluginsState.query)}`;
      const res = await fetch(url);
      if (!res.ok) {
        grid.innerHTML = `<div class="store-error">Failed to load plugins (${res.status})</div>`;
        mrPluginsState.loading = false;
        return;
      }
      const data = await res.json();
      const hits = Array.isArray(data.hits) ? data.hits : [];
      if (reset) {
        grid.innerHTML = "";
      }
      if (!hits.length) {
        if (mrPluginsState.offset === 0) {
          grid.innerHTML = '<div class="store-empty">No plugins found on Modrinth for this query.</div>';
        }
        mrPluginsState.more = false;
        if (loadMoreBtn) loadMoreBtn.style.display = "none";
      } else {
        hits.forEach(renderModrinthCard);
        mrPluginsState.offset += hits.length;
        if (loadMoreBtn) loadMoreBtn.style.display = (hits.length < mrPluginsState.limit) ? "none" : "inline-flex";
      }
    } catch (e) {
      grid.innerHTML = `<div class="store-error">Failed to load plugins.</div>`;
    } finally {
      mrPluginsState.loading = false;
    }
  }
  function renderModrinthCard(hit) {
    const grid = document.getElementById("mrPluginGrid");
    if (!grid) return;
    const card = document.createElement("div");
    card.className = "mr-plugin-card";
    const icon = hit.icon_url || "https://via.placeholder.com/200x140?text=No+Image";
    const downloads = typeof hit.downloads === "number" ? hit.downloads : 0;
    const downloadsText = downloads.toLocaleString();
    card.innerHTML = `
          <img src="${escapeHtml(icon)}" class="mr-plugin-icon" alt="">
          <div class="mr-plugin-body">
            <div class="mr-plugin-title">${escapeHtml(hit.title)}</div>
            <div class="mr-plugin-desc">${escapeHtml((hit.description || "").slice(0, 90))}${(hit.description || "").length > 90 ? "..." : ""}</div>
          </div>
          <div class="mr-plugin-footer">
            <span class="pill pill-muted"><i class="fa-solid fa-download"></i>&nbsp;${escapeHtml(downloadsText)}</span>
            <button class="mr-plugin-install-btn">Install</button>
          </div>
        `;
    card.querySelector(".mr-plugin-install-btn").addEventListener("click", (ev) => {
      ev.stopPropagation();
      openModrinthPluginModal(hit.project_id, hit.title);
    });
    card.addEventListener("click", () => openModrinthPluginModal(hit.project_id, hit.title));
    grid.appendChild(card);
  }
  async function openModrinthPluginModal(projectId, name) {
    mrCurrentProjectId = projectId;
    mrSelectedPlatform = null;
    mrSelectedMcVersion = null;
    mrPluginInstallBtn.disabled = true;
    mrPluginName.textContent = name;
    const platformSelect = document.getElementById("mrPlatformSelect");
    const versionSelect = document.getElementById("mrVersionSelect");
    platformSelect.innerHTML = `<option value="" selected disabled>Select a platform</option>`;
    versionSelect.innerHTML = `<option value="" selected disabled>Select a Minecraft version</option>`;
    try {
      const res = await fetch(`https://api.modrinth.com/v2/project/${encodeURIComponent(projectId)}/version`);
      if (!res.ok) {
      } else {
        mrAllProjectVersions = await res.json();
        populateModrinthPlatforms();
      }
    } catch (e) {
    }
    mrPluginModal.classList.add("show");
    mrPluginModal.style.display = "flex";
    mrPluginModal.setAttribute("aria-hidden", "false");
  }
  function closeModrinthPluginModal() {
    if (!mrPluginModal) return;
    animateClose(mrPluginModal, () => {
      mrCurrentProjectId = null;
      mrAllProjectVersions = [];
      mrSelectedPlatform = null;
      mrSelectedMcVersion = null;
      const platformSelect = document.getElementById("mrPlatformSelect");
      const versionSelect = document.getElementById("mrVersionSelect");
      if (platformSelect) platformSelect.innerHTML = `<option value="" selected disabled>Select a platform</option>`;
      if (versionSelect) versionSelect.innerHTML = `<option value="" selected disabled>Select a Minecraft version</option>`;
      mrPluginInstallBtn.disabled = true;
    });
  }
  if (mrPluginModalClose) mrPluginModalClose.addEventListener("click", closeModrinthPluginModal);
  if (mrPluginModal) {
    mrPluginModal.addEventListener("click", (e) => {
      if (e.target === mrPluginModal) closeModrinthPluginModal();
    });
  }
  mrPluginModal.addEventListener("click", (e) => {
    if (e.target === mrPluginModal) closeModrinthPluginModal();
  });
  function populateModrinthPlatforms() {
    const platformSelect = document.getElementById("mrPlatformSelect");
    const versionSelect = document.getElementById("mrVersionSelect");
    if (!platformSelect || !versionSelect) return;
    const loaderSet = new Set();
    (mrAllProjectVersions || []).forEach(v => {
      (v.loaders || []).forEach(l => {
        if (MR_VALID_LOADERS.includes(l)) loaderSet.add(l);
      });
    });
    platformSelect.innerHTML = `<option value="" selected disabled>Select a platform</option>`;
    versionSelect.innerHTML = `<option value="" selected disabled>Select a Minecraft version</option>`;
    mrSelectedPlatform = null;
    mrSelectedMcVersion = null;
    mrPluginInstallBtn.disabled = true;
    if (!loaderSet.size) {
      return;
    }
    Array.from(loaderSet).sort().forEach(loader => {
      const opt = document.createElement('option');
      opt.value = loader;
      opt.textContent = loader.toUpperCase();
      platformSelect.appendChild(opt);
    });
    platformSelect.onchange = () => {
      mrSelectedPlatform = platformSelect.value || null;
      mrSelectedMcVersion = null;
      mrPluginInstallBtn.disabled = true;
      populateModrinthVersions();
    };
    versionSelect.onchange = () => {
      mrSelectedMcVersion = versionSelect.value || null;
      mrPluginInstallBtn.disabled = !(mrSelectedPlatform && mrSelectedMcVersion);
    };
  }
  function populateModrinthVersions() {
    const versionSelect = document.getElementById("mrVersionSelect");
    if (!versionSelect) return;
    versionSelect.innerHTML = `<option value="" selected disabled>Select a Minecraft version</option>`;
    const versionsSet = new Set();
    (mrAllProjectVersions || []).forEach(v => {
      if (!v.loaders || !v.game_versions) return;
      if (!v.loaders.includes(mrSelectedPlatform)) return;
      v.game_versions.forEach(gv => {
        const match = String(gv).match(/^(\d+\.\d+(\.\d+)?)$/);
        if (match) versionsSet.add(match[1]);
      });
    });
    const list = Array.from(versionsSet).sort(compareMcVersionsDesc);
    list.forEach(ver => {
      const opt = document.createElement('option');
      opt.value = ver;
      opt.textContent = ver;
      versionSelect.appendChild(opt);
    });
  }
  mrPluginInstallBtn.addEventListener("click", async () => {
    if (!mrCurrentProjectId || !mrSelectedPlatform || !mrSelectedMcVersion) return;
    const match = (mrAllProjectVersions || []).find(v =>
      v.loaders && v.game_versions &&
      v.loaders.includes(mrSelectedPlatform) &&
      v.game_versions.includes(mrSelectedMcVersion)
    );
    if (!match) {
      alert("No matching Modrinth version found for this platform / Minecraft version.");
      return;
    }
    const files = match.files || [];
    const file = files.find(f => f.primary) || files[0];
    if (!file || !file.url) {
      alert("This Modrinth version does not expose a downloadable file.");
      return;
    }
    const label = `${mrPluginName.textContent} (${mrSelectedPlatform.toUpperCase()} ${mrSelectedMcVersion})`;
    const pluginFileName = file.filename || file.url.split('/').pop() || 'plugin.jar';
    try {
      await applyVersion("modrinth-plugin", match.id || `${mrCurrentProjectId}-${mrSelectedPlatform}-${mrSelectedMcVersion}`, label, file.url, { destPath: `plugins/${pluginFileName}` });
      alert("Plugin installed successfully");
      closeModrinthPluginModal();
    } catch (e) {
      alert("Failed to trigger plugin install: " + (e && e.message ? e.message : e));
    }
  });

  function normalizeStoreComparable(value) {
    return String(value || '').toLowerCase().replace(/[^a-z0-9.]+/g, ' ').trim();
  }
  function buildStoreProviderAliases(provider) {
    const aliases = new Set([
      String(provider?.id || '').toLowerCase(),
      String(provider?.name || '').toLowerCase()
    ]);
    switch (String(provider?.id || '').toLowerCase()) {
      case 'papermc':
        aliases.add('paper');
        aliases.add('papermc');
        break;
      case 'nodejs':
        aliases.add('node');
        aliases.add('node.js');
        break;
      case 'python':
        aliases.add('py');
        break;
      default:
        break;
    }
    return Array.from(aliases).map((alias) => alias.trim()).filter(Boolean);
  }
  function matchStoreProviderFromMessage(message, providers) {
    const raw = String(message || '').toLowerCase();
    let best = null;
    for (const provider of (providers || [])) {
      for (const alias of buildStoreProviderAliases(provider).sort((a, b) => b.length - a.length)) {
        const pattern = new RegExp(`(^|[^a-z0-9])${escapeRegex(alias)}([^a-z0-9]|$)`, 'i');
        if (!pattern.test(raw)) continue;
        const score = alias.length;
        if (!best || score > best.score) {
          best = { provider, score };
        }
        break;
      }
    }
    return best ? best.provider : null;
  }
  function findPreferredTaggedVersion(versions) {
    const list = Array.isArray(versions) ? versions : [];
    if (!list.length) return null;
    const preferredTags = ['recommended', 'latest', 'lts', 'stable'];
    for (const tag of preferredTags) {
      const match = list.find((version) =>
        Array.isArray(version?.tags) &&
        version.tags.some((entry) => String(entry || '').toLowerCase() === tag)
      );
      if (match) return match;
    }
    return list[0] || null;
  }
  function findRequestedVersionFromMessage(message, versions) {
    const raw = String(message || '').toLowerCase();
    const list = Array.isArray(versions) ? versions : [];
    if (!list.length) return null;

    const versionHints = Array.from(new Set(raw.match(/\b\d+(?:\.\d+){0,2}\b/g) || []));
    let best = null;

    list.forEach((version, index) => {
      const candidates = [
        version?.id,
        version?.name,
        version?.label,
        version?.mcVersion
      ].filter(Boolean).map(normalizeStoreComparable);
      const tagSet = new Set((version?.tags || []).map((entry) => String(entry || '').toLowerCase()));
      let score = 0;

      versionHints.forEach((hint) => {
        if (candidates.some((candidate) => candidate === hint)) score += 140;
        else if (candidates.some((candidate) => candidate.endsWith(` ${hint}`) || candidate.startsWith(`${hint} `) || candidate.includes(hint))) score += 70;
      });

      if (/\brecommended\b/.test(raw) && tagSet.has('recommended')) score += 80;
      if (/\blatest\b/.test(raw) && tagSet.has('latest')) score += 80;
      if (/\blts\b/.test(raw) && tagSet.has('lts')) score += 80;
      if (/\bstable\b/.test(raw) && (tagSet.has('stable') || tagSet.has('recommended'))) score += 70;

      if (!versionHints.length && !/\b(recommended|latest|lts|stable)\b/.test(raw)) {
        return;
      }

      if (!best || score > best.score || (score === best.score && index < best.index)) {
        best = { version, score, index };
      }
    });

    if (best && best.score > 0) return best.version;
    if (!versionHints.length) return findPreferredTaggedVersion(list);
    if (list.length === 1) return list[0];
    return null;
  }
  function detectMinecraftProjectType(message) {
    const raw = String(message || '').toLowerCase();
    if (/\bmods?\b/.test(raw)) return 'mod';
    if (/\bplugins?\b/.test(raw)) return 'plugin';
    return null;
  }
  function detectRequestedLoader(message, projectType) {
    const raw = String(message || '').toLowerCase();
    const candidates = projectType === 'mod' ? MR_MOD_LOADERS : MR_PLUGIN_LOADERS;
    return candidates.find((loader) => new RegExp(`(^|[^a-z0-9])${escapeRegex(loader)}([^a-z0-9]|$)`, 'i').test(raw)) || null;
  }
  function detectPreferredServerLoader(projectType) {
    const runtimeHint = [
      serverInfo?.runtime?.providerId,
      serverInfo?.start,
      serverInfo?.template,
      storeCurrentProvider?.id
    ].filter(Boolean).join(' ').toLowerCase();

    if (projectType === 'mod') {
      if (runtimeHint.includes('neoforge')) return 'neoforge';
      if (runtimeHint.includes('forge')) return 'forge';
      if (runtimeHint.includes('fabric')) return 'fabric';
      if (runtimeHint.includes('quilt')) return 'quilt';
      return null;
    }

    if (runtimeHint.includes('purpur')) return 'purpur';
    if (runtimeHint.includes('spigot')) return 'spigot';
    if (runtimeHint.includes('bukkit')) return 'bukkit';
    if (runtimeHint.includes('velocity')) return 'velocity';
    if (runtimeHint.includes('waterfall')) return 'waterfall';
    if (runtimeHint.includes('folia')) return 'folia';
    if (runtimeHint.includes('paper')) return 'paper';
    return null;
  }
  function detectRequestedMinecraftVersion(message) {
    const match = String(message || '').match(/\b1\.\d+(?:\.\d+)?\b/);
    return match ? match[0] : null;
  }
  function extractRequestedProjectName(message, projectType) {
    const quoted = String(message || '').match(/["'`]([^"'`]{2,120})["'`]/);
    if (quoted) return quoted[1].trim();

    const pattern = projectType === 'mod'
      ? /(?:install|add|download|setup|set up|use|put|pune|adauga|instaleaza)\s+(?:the\s+)?(?:mod(?:ul)?\s+)?(.+)/i
      : /(?:install|add|download|setup|set up|use|put|pune|adauga|instaleaza)\s+(?:the\s+)?(?:plugin(?:ul)?\s+)?(.+)/i;

    const match = String(message || '').match(pattern);
    let candidate = match ? match[1] : '';
    candidate = candidate.replace(/\b(?:for|on|pe|la|using|with)\b[\s\S]*$/i, ' ').trim();
    candidate = candidate
      .replace(/\b(?:paper|purpur|spigot|bukkit|velocity|waterfall|folia|fabric|forge|neoforge|quilt)\b/gi, ' ')
      .replace(/\b1\.\d+(?:\.\d+)?\b/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
    return candidate || null;
  }
  async function searchModrinthProjects(query, projectType) {
    const facets = encodeURIComponent(JSON.stringify([[`project_type:${projectType}`]]));
    const url = `https://api.modrinth.com/v2/search?limit=8&query=${encodeURIComponent(query)}&facets=${facets}`;
    const response = await fetch(url);
    if (!response.ok) throw new Error(`Modrinth search failed (${response.status})`);
    const data = await response.json().catch(() => ({}));
    return Array.isArray(data?.hits) ? data.hits : [];
  }
  async function fetchModrinthVersions(projectId) {
    const response = await fetch(`https://api.modrinth.com/v2/project/${encodeURIComponent(projectId)}/version`);
    if (!response.ok) throw new Error(`Modrinth versions failed (${response.status})`);
    const data = await response.json().catch(() => []);
    return Array.isArray(data) ? data : [];
  }
  function chooseModrinthSearchHit(hits, query) {
    const needle = normalizeStoreComparable(query);
    if (!needle) return hits[0] || null;
    const exact = hits.find((hit) => {
      const fields = [hit?.title, hit?.slug, hit?.project_id].filter(Boolean).map(normalizeStoreComparable);
      return fields.some((field) => field === needle);
    });
    return exact || hits[0] || null;
  }
  function chooseModrinthInstallVersion(allVersions, opts = {}) {
    let loader = opts.loader || null;
    let mcVersion = opts.mcVersion || null;
    const versions = Array.isArray(allVersions) ? allVersions : [];

    const allLoaders = Array.from(new Set(
      versions.flatMap((version) => Array.isArray(version?.loaders) ? version.loaders : [])
        .map((entry) => String(entry || '').toLowerCase())
        .filter(Boolean)
    ));

    if (!loader) {
      if (allLoaders.length === 1) loader = allLoaders[0];
      else return { error: 'missing_loader', loaders: allLoaders.slice(0, 6) };
    }

    const loaderVersions = versions.filter((version) =>
      Array.isArray(version?.loaders) &&
      version.loaders.map((entry) => String(entry || '').toLowerCase()).includes(loader)
    );
    if (!loaderVersions.length) return { error: 'no_loader_match', loader };

    const availableVersions = Array.from(new Set(
      loaderVersions.flatMap((version) => Array.isArray(version?.game_versions) ? version.game_versions : [])
        .map((entry) => String(entry || ''))
        .filter((entry) => /^\d+\.\d+(?:\.\d+)?$/.test(entry))
    )).sort(compareMcVersionsDesc);

    if (!mcVersion) {
      if (availableVersions.length === 1) mcVersion = availableVersions[0];
      else return { error: 'missing_mc_version', loader, versions: availableVersions.slice(0, 6) };
    }

    const exactVersions = loaderVersions.filter((version) =>
      Array.isArray(version?.game_versions) && version.game_versions.includes(mcVersion)
    );
    if (!exactVersions.length) return { error: 'no_version_match', loader, mcVersion, versions: availableVersions.slice(0, 6) };

    exactVersions.sort((a, b) => {
      if (!!b.featured !== !!a.featured) return b.featured ? 1 : -1;
      const aTime = Date.parse(a?.date_published || a?.date_created || 0) || 0;
      const bTime = Date.parse(b?.date_published || b?.date_created || 0) || 0;
      return bTime - aTime;
    });

    const version = exactVersions[0];
    const file = (version?.files || []).find((entry) => entry?.primary) || (version?.files || [])[0];
    if (!file?.url) return { error: 'missing_file', loader, mcVersion };

    return { loader, mcVersion, version, file };
  }
  function collectModrinthLoaders(allVersions, projectType) {
    const allowedLoaders = projectType === 'mod'
      ? MR_MOD_LOADERS
      : projectType === 'plugin'
        ? MR_PLUGIN_LOADERS
        : MR_VALID_LOADERS;

    return Array.from(new Set(
      (Array.isArray(allVersions) ? allVersions : [])
        .flatMap((version) => Array.isArray(version?.loaders) ? version.loaders : [])
        .map((entry) => String(entry || '').toLowerCase())
        .filter((entry) => allowedLoaders.includes(entry))
    )).sort();
  }
  function collectModrinthMinecraftVersions(allVersions, loader = null) {
    const normalizedLoader = loader ? String(loader).toLowerCase() : null;
    return Array.from(new Set(
      (Array.isArray(allVersions) ? allVersions : [])
        .filter((version) => {
          if (!normalizedLoader) return true;
          return Array.isArray(version?.loaders) &&
            version.loaders.map((entry) => String(entry || '').toLowerCase()).includes(normalizedLoader);
        })
        .flatMap((version) => Array.isArray(version?.game_versions) ? version.game_versions : [])
        .map((entry) => String(entry || '').trim())
        .filter((entry) => /^\d+\.\d+(?:\.\d+)?$/.test(entry))
    )).sort(compareMcVersionsDesc);
  }
  function planModrinthInstallRequirements(opts = {}) {
    const projectVersions = Array.isArray(opts.projectVersions) ? opts.projectVersions : [];
    const availableLoaders = collectModrinthLoaders(projectVersions, opts.projectType);
    const selectedLoader = opts.loader ? String(opts.loader).toLowerCase() : (availableLoaders.length === 1 ? availableLoaders[0] : null);
    const availableVersions = collectModrinthMinecraftVersions(projectVersions, selectedLoader);
    const selectedMcVersion = opts.mcVersion || (availableVersions.length === 1 ? availableVersions[0] : null);
    const missing = [];

    if (!selectedLoader) missing.push('loader');
    if (!selectedMcVersion) missing.push('mcVersion');

    return {
      projectType: opts.projectType,
      query: opts.query,
      projectId: opts.projectId,
      projectTitle: opts.projectTitle,
      loader: selectedLoader,
      mcVersion: selectedMcVersion,
      missing,
      availableLoaders: availableLoaders.slice(0, 6),
      availableVersions: availableVersions.slice(0, 6)
    };
  }
  function buildPendingStoreFollowUpPrompt(intent) {
    const label = intent.projectTitle || intent.query || 'this project';
    const loaderLabel = intent.projectType === 'mod' ? 'loader' : 'server fork and loader';
    const loaderWord = intent.projectType === 'mod' ? 'loaders' : 'platforms';
    const exampleLoader = intent.loader || intent.availableLoaders[0] || (intent.projectType === 'mod' ? 'fabric' : 'paper');
    const exampleVersion = intent.mcVersion || intent.availableVersions[0] || '1.21.4';
    const loaderList = intent.availableLoaders.length ? ` Available ${loaderWord}: ${intent.availableLoaders.join(', ')}.` : '';
    const versionList = intent.availableVersions.length ? ` Available versions: ${intent.availableVersions.join(', ')}.` : '';

    if (intent.missing.includes('loader') && intent.missing.includes('mcVersion')) {
      return `I found **${label}**. Reply once with the ${loaderLabel} and Minecraft version, for example: ${exampleLoader} ${exampleVersion}.${loaderList}${versionList}`;
    }
    if (intent.missing.includes('loader')) {
      return `I found **${label}**. Reply once with the ${loaderLabel}, for example: ${exampleLoader}.${loaderList}`;
    }
    return `I found **${label}**. Reply once with the Minecraft version for **${intent.loader}**, for example: ${exampleVersion}.${versionList}`;
  }
  function buildPendingModrinthFollowUpOperation(plan) {
    if (!plan?.missing?.length) return null;
    return {
      type: 'follow_up',
      source: 'modrinth',
      projectType: plan.projectType,
      query: plan.query,
      projectId: plan.projectId,
      projectTitle: plan.projectTitle,
      loader: plan.loader || null,
      mcVersion: plan.mcVersion || null,
      missing: Array.isArray(plan.missing) ? plan.missing.slice() : [],
      availableLoaders: Array.isArray(plan.availableLoaders) ? plan.availableLoaders.slice() : [],
      availableVersions: Array.isArray(plan.availableVersions) ? plan.availableVersions.slice() : [],
      prompt: buildPendingStoreFollowUpPrompt(plan)
    };
  }
  async function resolveModrinthProjectOperation(opts = {}) {
    const projectType = opts.projectType;
    const query = String(opts.query || opts.projectTitle || '').trim();
    let projectId = String(opts.projectId || '').trim();
    let projectTitle = String(opts.projectTitle || query || '').trim();

    if (!projectType) {
      return { type: 'message', message: 'Tell me whether you want a plugin or a mod.' };
    }
    if (!projectId) {
      if (!query) {
        return { type: 'message', message: `Tell me the exact ${projectType} name you want to install.` };
      }
      const hits = await searchModrinthProjects(query, projectType);
      const hit = chooseModrinthSearchHit(hits, query);
      if (!hit?.project_id) {
        return { type: 'message', message: `I could not find a Modrinth ${projectType} matching "${query}".` };
      }
      projectId = hit.project_id;
      projectTitle = hit.title || query;
    }

    const projectVersions = await fetchModrinthVersions(projectId);
    if (!projectVersions.length) {
      return { type: 'message', message: `I found **${projectTitle || query}**, but it has no installable builds on Modrinth right now.` };
    }

    const plan = planModrinthInstallRequirements({
      projectType,
      query,
      projectId,
      projectTitle,
      projectVersions,
      loader: opts.loader,
      mcVersion: opts.mcVersion
    });
    const followUp = buildPendingModrinthFollowUpOperation(plan);
    if (followUp) return followUp;

    const resolved = chooseModrinthInstallVersion(projectVersions, {
      loader: plan.loader,
      mcVersion: plan.mcVersion
    });

    if (resolved.error === 'no_loader_match') {
      return buildPendingModrinthFollowUpOperation(planModrinthInstallRequirements({
        projectType,
        query,
        projectId,
        projectTitle,
        projectVersions,
        loader: null,
        mcVersion: opts.mcVersion
      })) || {
        type: 'message',
        message: `I found **${projectTitle || query}**, but it does not support **${resolved.loader}**.`
      };
    }

    if (resolved.error === 'no_version_match') {
      return buildPendingModrinthFollowUpOperation(planModrinthInstallRequirements({
        projectType,
        query,
        projectId,
        projectTitle,
        projectVersions,
        loader: resolved.loader || plan.loader,
        mcVersion: null
      })) || {
        type: 'message',
        message: `I found **${projectTitle || query}**, but not for Minecraft **${resolved.mcVersion}**.`
      };
    }

    if (resolved.error === 'missing_file') {
      return {
        type: 'message',
        message: `I found **${projectTitle || query}**, but the matched ${projectType} build does not expose a downloadable file.`
      };
    }

    if (resolved.error) {
      return {
        type: 'message',
        message: `I found **${projectTitle || query}**, but I could not match a downloadable ${projectType} build for this request.`
      };
    }

    const fileName = resolved.file.filename || resolved.file.url.split('/').pop() || `${projectType}.jar`;
    const destRoot = projectType === 'mod' ? 'mods' : 'plugins';
    return {
      type: 'modrinth',
      projectType,
      projectId,
      projectTitle: projectTitle || query,
      loader: resolved.loader,
      mcVersion: resolved.mcVersion,
      versionId: resolved.version.id || `${projectId}-${resolved.loader}-${resolved.mcVersion}`,
      label: `${projectTitle || query} (${resolved.loader.toUpperCase()} ${resolved.mcVersion})`,
      downloadUrl: resolved.file.url,
      destPath: `${destRoot}/${fileName}`
    };
  }
  function isLikelyStoreFollowUpAnswer(message, pending) {
    if (!pending || pending.source !== 'modrinth') return false;
    const raw = String(message || '').trim();
    if (!raw) return false;
    if (detectRequestedLoader(raw, pending.projectType)) return true;
    if (detectRequestedMinecraftVersion(raw)) return true;
    return (pending.availableLoaders || []).some((loader) =>
      new RegExp(`(^|[^a-z0-9])${escapeRegex(loader)}([^a-z0-9]|$)`, 'i').test(raw)
    );
  }
  async function continuePendingStoreFollowUp(message) {
    if (!aiPendingStoreFollowUp || aiPendingStoreFollowUp.source !== 'modrinth') return null;
    if (!isLikelyStoreFollowUpAnswer(message, aiPendingStoreFollowUp)) return null;

    const loader = detectRequestedLoader(message, aiPendingStoreFollowUp.projectType) || aiPendingStoreFollowUp.loader || null;
    const mcVersion = detectRequestedMinecraftVersion(message) || aiPendingStoreFollowUp.mcVersion || null;
    const nextOperation = await resolveModrinthProjectOperation({
      projectType: aiPendingStoreFollowUp.projectType,
      query: aiPendingStoreFollowUp.query,
      projectId: aiPendingStoreFollowUp.projectId,
      projectTitle: aiPendingStoreFollowUp.projectTitle,
      loader,
      mcVersion
    });

    if (nextOperation?.type === 'message') {
      clearPendingStoreFollowUp();
    }

    return nextOperation;
  }
  function parseAiStoreInstallCommand(spec) {
    const raw = String(spec || '').trim();
    if (!raw) return null;

    const parts = raw.split(':').map((part) => part.trim()).filter(Boolean);
    if (!parts.length) return null;

    const projectName = parts[0].replace(/^["'`]+|["'`]+$/g, '').trim();
    if (!projectName) return null;

    let loader = null;
    let mcVersion = null;
    let projectType = aiPendingStoreFollowUp?.projectType || null;

    parts.slice(1).forEach((part) => {
      const token = String(part || '').trim().split(/\s+/)[0].replace(/[.,!?;:]+$/g, '');
      const lowerToken = token.toLowerCase();
      if (!token) return;
      if (!mcVersion && /^\d+\.\d+(?:\.\d+)?$/.test(token)) {
        mcVersion = token;
        return;
      }
      if (!loader && MR_VALID_LOADERS.includes(lowerToken)) {
        loader = lowerToken;
        if (!projectType) {
          projectType = MR_MOD_LOADERS.includes(lowerToken) ? 'mod' : 'plugin';
        }
      }
    });

    if (!projectType && loader) {
      projectType = MR_MOD_LOADERS.includes(loader) ? 'mod' : 'plugin';
    }

    return {
      type: 'ai_store_install',
      projectName,
      projectType,
      loader,
      mcVersion
    };
  }
  async function resolveDirectAiStoreOperation(message) {
    const raw = String(message || '').trim();
    const lower = raw.toLowerCase();
    if (!raw) return null;

    const hasActionVerb = /\b(install|add|apply|switch|change|set|use|update|upgrade|downgrade|download|instaleaza|adauga|schimba|seteaza|pune|aplica|trece)\b/i.test(raw);
    const touchesStore = /\b(store|version|runtime|container|paper|papermc|purpur|pufferfish|vanilla|spigot|velocity|waterfall|folia|leaves|node(?:\.js)?|python|plugin|plugins|mod|mods|modrinth)\b/i.test(raw);
    if (!hasActionVerb || !touchesStore) return null;

    if (!hasPerm('store_access')) {
      return { type: 'message', message: 'You do not have permission to use this server store.' };
    }

    if (!serverInfo) {
      try { await loadServerInfo(); } catch { }
    }

    const { template: tpl } = resolveStoreTemplate();
    if (!tpl) {
      return { type: 'message', message: 'Choose the Store template for this server first, then I can install versions, plugins, or mods.' };
    }

    const projectType = detectMinecraftProjectType(raw);
    if (projectType) {
      if (tpl !== 'minecraft') {
        return { type: 'message', message: 'Plugins and mods are available only for Minecraft server templates.' };
      }

      const query = extractRequestedProjectName(raw, projectType);
      const loader = detectRequestedLoader(raw, projectType) || detectPreferredServerLoader(projectType);
      const mcVersion = detectRequestedMinecraftVersion(raw);
      return resolveModrinthProjectOperation({
        projectType,
        query,
        loader,
        mcVersion
      });
    }

    const providersData = await fetchStoreProviders().catch(() => null);
    const providers = Array.isArray(storeProvidersCache) ? storeProvidersCache : (providersData?.providers || []);
    if (!providers.length) {
      return { type: 'message', message: 'I could not load the Store providers for this server.' };
    }

    const provider = matchStoreProviderFromMessage(raw, providers);
    if (!provider) return null;

    const versionData = await fetchStoreVersions(provider.id).catch(() => null);
    const versions = Array.isArray(versionData?.versions) ? versionData.versions : [];
    if (!versions.length) {
      return { type: 'message', message: `I could not find installable versions for ${provider.name || provider.id}.` };
    }

    const version = findRequestedVersionFromMessage(raw, versions);
    if (!version) {
      const examples = versions.slice(0, 5).map((entry) => entry?.label || entry?.name || entry?.id).filter(Boolean).join(', ');
      return {
        type: 'message',
        message: `Tell me the exact ${provider.name || provider.id} version to install. Examples: ${examples}.`
      };
    }

    return {
      type: 'version',
      providerId: provider.id,
      versionId: version.id || version.name || version.version || '',
      label: version.label || version.name || version.id || provider.name || provider.id,
      downloadUrl: version.downloadUrl || version.url || null,
      template: tpl
    };
  }
  async function executeStoreOperation(operation) {
    if (!operation) return false;
    if (operation.type === 'message') {
      await addChatMessage('ai', operation.message);
      return true;
    }

    try {
      if (operation.type === 'follow_up') {
        aiPendingStoreFollowUp = {
          ...operation,
          missing: Array.isArray(operation.missing) ? operation.missing.slice() : [],
          availableLoaders: Array.isArray(operation.availableLoaders) ? operation.availableLoaders.slice() : [],
          availableVersions: Array.isArray(operation.availableVersions) ? operation.availableVersions.slice() : []
        };
        await addChatMessage('ai', operation.prompt || 'Tell me the missing Store details in one reply.');
        return true;
      }

      clearPendingStoreFollowUp();

      if (operation.type === 'ai_store_install') {
        const candidateTypes = operation.projectType ? [operation.projectType] : ['plugin', 'mod'];
        let resolved = null;

        for (const candidateType of candidateTypes) {
          resolved = await resolveModrinthProjectOperation({
            projectType: candidateType,
            query: operation.projectName,
            loader: operation.loader,
            mcVersion: operation.mcVersion
          });
          if (!resolved) continue;
          if (resolved.type !== 'message' || !/could not find a modrinth/i.test(String(resolved.message || ''))) {
            break;
          }
        }

        if (!resolved) {
          await addChatMessage('ai', 'I could not match that Store install request.');
          return true;
        }

        return executeStoreOperation(resolved);
      }

      if (operation.type === 'version') {
        await applyVersion(operation.providerId, operation.versionId, operation.label, operation.downloadUrl, {
          skipConfirm: true,
          silentSuccess: true,
          template: operation.template
        });
        await addChatMessage('ai', `Installed **${operation.label}** on **${bot}**.`);
        return true;
      }

      if (operation.type === 'modrinth') {
        await applyVersion(`modrinth-${operation.projectType}`, operation.versionId, operation.label, operation.downloadUrl, {
          destPath: operation.destPath,
          skipConfirm: true,
          silentSuccess: true,
          template: 'minecraft'
        });
        await addChatMessage('ai', `Installed **${operation.label}** in \`${operation.destPath}\`.`);
        return true;
      }
    } catch (err) {
      clearPendingStoreFollowUp();
      await addChatMessage('ai', err?.message || 'Failed to complete the Store action.');
      return true;
    }

    return false;
  }

  async function openInfoModal() {
    if (!serverInfo) await loadServerInfo();
    const ip = serverInfo && serverInfo.ip ? serverInfo.ip : "—";
    const port = (serverInfo && (serverInfo.port !== undefined && serverInfo.port !== null)) ? serverInfo.port : "—";
    infoIp.textContent = ip;
    infoPort.textContent = String(port);
    infoAddress.value = (ip !== "—") ? formatAddress(ip, (port !== "—") ? port : undefined) : "";

    currentSftpPassword = null;
    sftpPasswordVisible = false;
    if (infoSftpSection) infoSftpSection.style.display = 'none';
    if (infoSftpNoAccess) infoSftpNoAccess.style.display = 'none';

    infoModal.classList.add('show');
    infoModal.style.display = 'flex';
    infoModal.setAttribute('aria-hidden', 'false');

    try {
      const res = await fetch(`/api/server/${encodeURIComponent(bot)}/sftp-credentials`);
      if (res.ok) {
        const data = await res.json();

        if (data.hasFileAccess) {
          if (infoSftpSection) infoSftpSection.style.display = 'block';
          if (infoSftpNoAccess) infoSftpNoAccess.style.display = 'none';

          if (infoSftpHost) infoSftpHost.textContent = data.connection?.host || ip || '—';
          if (infoSftpPort) infoSftpPort.textContent = data.connection?.port || '2022';

          if (data.credentials) {
            if (infoSftpUser) infoSftpUser.textContent = data.credentials.username || '—';

            if (data.credentials.password) {
              currentSftpPassword = data.credentials.password;
              showSftpPassword(true);
            } else {
              currentSftpPassword = null;
              hideSftpPassword();
            }
          }
        } else if (data.hasAccess) {
          if (infoSftpSection) infoSftpSection.style.display = 'none';
          if (infoSftpNoAccess) infoSftpNoAccess.style.display = 'block';
        } else {
          if (infoSftpSection) infoSftpSection.style.display = 'none';
          if (infoSftpNoAccess) infoSftpNoAccess.style.display = 'none';
        }
      }
    } catch (err) {
      console.error('[Info Modal] Error fetching SFTP credentials:', err);
    }
  }

  function showSftpPassword(isNew = false) {
    sftpPasswordVisible = true;
    if (infoSftpPassMask) infoSftpPassMask.style.display = 'none';
    if (infoSftpPassText) {
      infoSftpPassText.style.display = 'inline';
      infoSftpPassText.textContent = currentSftpPassword || '—';
    }
    if (infoSftpTogglePass) {
      const icon = infoSftpTogglePass.querySelector('i');
      if (icon) icon.className = 'fa-solid fa-eye-slash';
    }
    if (isNew && currentSftpPassword) {
      if (infoSftpPass) {
        infoSftpPass.style.background = 'rgba(16, 185, 129, 0.15)';
        setTimeout(() => { if (infoSftpPass) infoSftpPass.style.background = ''; }, 2000);
      }
    }
  }

  function hideSftpPassword() {
    sftpPasswordVisible = false;
    if (infoSftpPassMask) infoSftpPassMask.style.display = 'inline';
    if (infoSftpPassText) infoSftpPassText.style.display = 'none';
    if (infoSftpTogglePass) {
      const icon = infoSftpTogglePass.querySelector('i');
      if (icon) icon.className = 'fa-solid fa-eye';
    }
  }

  if (infoSftpTogglePass) {
    infoSftpTogglePass.addEventListener('click', () => {
      if (currentSftpPassword) {
        if (sftpPasswordVisible) {
          hideSftpPassword();
        } else {
          showSftpPassword();
        }
      } else {
        alert('Password not available. Click "Regenerate Password" to create a new one.');
      }
    });
  }

  if (infoSftpRegenBtn) {
    infoSftpRegenBtn.addEventListener('click', async () => {
      if (!confirm('This will generate a new SFTP password. Your old password will stop working immediately. Continue?')) {
        return;
      }

      infoSftpRegenBtn.disabled = true;
      const originalText = infoSftpRegenBtn.innerHTML;
      infoSftpRegenBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> <span>Generating...</span>';

      try {
        const res = await fetch(`/api/server/${encodeURIComponent(bot)}/sftp-credentials/regenerate`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' }
        });

        if (res.ok) {
          const data = await res.json();
          if (data.success && data.password) {
            currentSftpPassword = data.password;
            if (infoSftpUser) infoSftpUser.textContent = data.username || infoSftpUser.textContent;
            showSftpPassword(true);

            try {
              await navigator.clipboard.writeText(data.password);
              alert('New password generated and copied to clipboard!\n\nMake sure to save it - it won\'t be shown again.');
            } catch {
              alert('New password generated!\n\nPassword: ' + data.password + '\n\nMake sure to save it - it won\'t be shown again.');
            }
          } else {
            alert('Failed to regenerate password');
          }
        } else {
          const err = await res.json().catch(() => ({}));
          alert('Error: ' + (err.error || 'Failed to regenerate password'));
        }
      } catch (err) {
        console.error('[SFTP] Regenerate error:', err);
        alert('Network error while regenerating password');
      } finally {
        infoSftpRegenBtn.disabled = false;
        infoSftpRegenBtn.innerHTML = originalText;
      }
    });
  }

  document.querySelectorAll('.info-card-copy[data-copy]').forEach(btn => {
    btn.addEventListener('click', async () => {
      const targetId = btn.getAttribute('data-copy');
      let value = '';

      if (targetId === 'infoSftpPassValue') {
        value = currentSftpPassword || '';
        if (!value) {
          alert('Password not available. Click "Regenerate Password" to create a new one.');
          return;
        }
      } else {
        const el = document.getElementById(targetId);
        value = el?.textContent || '';
      }

      if (value && value !== '—') {
        try {
          await navigator.clipboard.writeText(value);
          const icon = btn.querySelector('i');
          if (icon) {
            icon.className = 'fa-solid fa-check';
            setTimeout(() => { icon.className = 'fa-solid fa-copy'; }, 1500);
          }
        } catch {
          alert('Failed to copy: ' + value);
        }
      }
    });
  });

  function closeInfoModal() {
    if (!infoModal) return;
    animateClose(infoModal);
    currentSftpPassword = null;
    sftpPasswordVisible = false;
    hideSftpPassword();
  }

  infoModalClose.addEventListener('click', closeInfoModal);

  const activityModal = document.getElementById('activityModal');
  const activityModalClose = document.getElementById('activityModalClose');
  const activityList = document.getElementById('activityList');
  const activitySearchInput = document.getElementById('activitySearchInput');
  const activityLoadMore = document.getElementById('activityLoadMore');
  let activityData = [];
  let activityOffset = 0;
  const ACTIVITY_LIMIT = 50;
  let activityFilter = 'all';

  async function loadActivityLogs(append = false) {
    if (!append) {
      activityOffset = 0;
      activityData = [];
      activityList.innerHTML = '<div class="activity-loading"><div class="activity-loading-spinner"></div><span>Loading activity...</span></div>';
    }
    try {
      const res = await fetch(`/api/server/${encodeURIComponent(bot)}/activity?offset=${activityOffset}&limit=${ACTIVITY_LIMIT}`);
      if (!res.ok) throw new Error('Failed to load activity');
      const data = await res.json();
      const entries = Array.isArray(data.entries) ? data.entries : [];
      activityData = append ? [...activityData, ...entries] : entries;
      activityOffset += entries.length;
      renderActivityList();
      if (activityLoadMore) {
        activityLoadMore.style.display = entries.length < ACTIVITY_LIMIT ? 'none' : 'flex';
      }
    } catch (err) {
      console.error('Activity load error:', err);
      activityList.innerHTML = '<div class="activity-empty"><i class="fa-solid fa-circle-exclamation"></i><p>Failed to load activity logs</p></div>';
    }
  }

  function renderActivityList() {
    const query = (activitySearchInput?.value || '').toLowerCase();
    let filtered = activityData.filter(e => {
      if (activityFilter !== 'all') {
        const action = String(e.action || '').toLowerCase();
        if (activityFilter === 'power' && !['start', 'stop', 'restart', 'kill', 'run'].some(a => action.includes(a))) return false;
        if (activityFilter === 'console' && !action.includes('command')) return false;
        if (activityFilter === 'file' && !['file', 'upload', 'delete', 'rename', 'create', 'extract'].some(a => action.includes(a))) return false;
      }
      if (query) {
        const text = `${e.user || ''} ${e.ip || ''} ${e.action || ''} ${JSON.stringify(e.details || '')}`.toLowerCase();
        if (!text.includes(query)) return false;
      }
      return true;
    });
    if (filtered.length === 0) {
      activityList.innerHTML = '<div class="activity-empty"><i class="fa-solid fa-ghost"></i><p>No activity found</p></div>';
      return;
    }
    activityList.innerHTML = filtered.map(e => {
      const ts = e.ts ? new Date(e.ts).toLocaleString() : '—';
      const action = escapeHtml(e.action || 'unknown');
      const user = escapeHtml(e.user || 'unknown');
      const ip = escapeHtml(e.ip || '');
      const details = e.details ? escapeHtml(typeof e.details === 'string' ? e.details : JSON.stringify(e.details)) : '';
      let icon = 'fa-circle-info';
      const actionLower = action.toLowerCase();
      if (actionLower.includes('start') || actionLower.includes('run')) icon = 'fa-play';
      else if (actionLower.includes('stop') || actionLower.includes('kill')) icon = 'fa-stop';
      else if (actionLower.includes('restart')) icon = 'fa-rotate';
      else if (actionLower.includes('command')) icon = 'fa-terminal';
      else if (actionLower.includes('file') || actionLower.includes('upload') || actionLower.includes('delete')) icon = 'fa-file';
      let iconType = 'console';
      if (actionLower.includes('start') || actionLower.includes('run')) iconType = 'start';
      else if (actionLower.includes('kill')) iconType = 'kill';
      else if (actionLower.includes('stop')) iconType = 'stop';
      else if (actionLower.includes('restart')) iconType = 'restart';
      else if (actionLower.includes('file') || actionLower.includes('upload') || actionLower.includes('delete')) iconType = 'file';
      return `<div class="activity-entry"><div class="activity-entry-icon ${iconType}"><i class="fa-solid ${icon}"></i></div><div class="activity-entry-content"><div class="activity-entry-action">${action}</div>${details ? `<div class="activity-entry-details">${details}</div>` : ''}<div class="activity-entry-meta"><span><i class="fa-solid fa-user"></i> ${user}</span><span><i class="fa-solid fa-clock"></i> ${ts}</span>${ip ? `<span><i class="fa-solid fa-network-wired"></i> ${ip}</span>` : ''}</div></div></div>`;
    }).join('');
  }

  function openActivityModal() {
    if (!activityModal) return;
    if (!hasPerm('activity_logs')) { alert('You do not have permission to view activity logs'); return; }
    activityModal.classList.add('show');
    activityModal.style.display = 'flex';
    activityModal.setAttribute('aria-hidden', 'false');
    loadActivityLogs();
  }

  function closeActivityModal() {
    if (!activityModal) return;
    animateClose(activityModal);
  }

  if (activityModalClose) activityModalClose.addEventListener('click', closeActivityModal);
  if (activitySearchInput) activitySearchInput.addEventListener('input', () => renderActivityList());
  if (activityLoadMore) activityLoadMore.addEventListener('click', () => loadActivityLogs(true));
  document.querySelectorAll('.activity-pill').forEach(pill => {
    pill.addEventListener('click', () => {
      document.querySelectorAll('.activity-pill').forEach(p => p.classList.remove('active'));
      pill.classList.add('active');
      activityFilter = pill.dataset.filter || 'all';
      renderActivityList();
    });
  });

  const backupsModal = document.getElementById('backupsModal');
  const backupsModalClose = document.getElementById('backupsModalClose');
  const backupsList = document.getElementById('backupsList');
  const backupsCreateBtn = document.getElementById('backupsCreateBtn');
  const backupsRefreshBtn = document.getElementById('backupsRefreshBtn');
  const backupCreateModal = document.getElementById('backupCreateModal');
  const backupCreateClose = document.getElementById('backupCreateClose');
  const backupCreateCancel = document.getElementById('backupCreateCancel');
  const backupCreateConfirm = document.getElementById('backupCreateConfirm');
  const backupNameInput = document.getElementById('backupNameInput');
  const backupDescInput = document.getElementById('backupDescInput');

  async function loadBackups() {
    if (!backupsList) return;
    backupsList.innerHTML = '<div class="backups-loading"><i class="fa-solid fa-spinner fa-spin"></i> Loading backups...</div>';
    if (backupsRefreshBtn) backupsRefreshBtn.classList.add('is-loading');
    try {
      const res = await fetch(`/api/nodes/server/${encodeURIComponent(bot)}/backups`);
      if (!res.ok) throw new Error('Failed to load backups');
      const data = await res.json();
      const backups = Array.isArray(data.backups) ? data.backups : [];
      renderBackupsList(backups);
    } catch (err) {
      console.error('Backups load error:', err);
      backupsList.innerHTML = '<div class="backups-empty"><i class="fa-solid fa-box-open"></i><p>Failed to load backups</p></div>';
    } finally {
      if (backupsRefreshBtn) backupsRefreshBtn.classList.remove('is-loading');
    }
  }

  function renderBackupsList(backups) {
    if (!backups || backups.length === 0) {
      const hint = hasPerm('backups_create') ? 'Create your first backup to protect your server data' : 'No backups available';
      backupsList.innerHTML = `<div class="backups-empty"><i class="fa-solid fa-box-open"></i><p>No backups found</p><p class="backups-empty-hint">${hint}</p></div>`;
      return;
    }
    const canDelete = hasPerm('backups_delete');
    backupsList.innerHTML = backups.map(b => {
      const name = escapeHtml(b.name || b.id || 'Unnamed');
      const desc = b.description ? escapeHtml(b.description) : '';
      const size = b.size ? formatBytes(b.size) : '—';
      const created = b.created_at ? new Date(b.created_at).toLocaleString() : '—';
      const deleteBtn = canDelete ? `<button class="backup-action-btn delete" data-id="${escapeHtml(b.id || '')}" title="Delete"><i class="fa-solid fa-trash"></i></button>` : '';
      return `<div class="backup-item" data-id="${escapeHtml(b.id || '')}"><div class="backup-item-main"><div class="backup-item-icon"><i class="fa-solid fa-box-archive"></i></div><div class="backup-item-info"><div class="backup-item-name">${name}</div>${desc ? `<div class="backup-item-desc">${desc}</div>` : ''}<div class="backup-item-meta"><span><i class="fa-solid fa-calendar"></i> ${created}</span><span><i class="fa-solid fa-weight-hanging"></i> ${size}</span></div></div></div><div class="backup-item-actions"><button class="backup-action-btn restore" data-id="${escapeHtml(b.id || '')}" title="Restore"><i class="fa-solid fa-rotate-left"></i> Restore</button>${deleteBtn}</div></div>`;
    }).join('');

    backupsList.querySelectorAll('.backup-action-btn.restore').forEach(btn => {
      btn.addEventListener('click', () => restoreBackup(btn.dataset.id));
    });
    backupsList.querySelectorAll('.backup-action-btn.delete').forEach(btn => {
      btn.addEventListener('click', () => deleteBackup(btn.dataset.id));
    });
  }

  function formatBytes(bytes) {
    if (!bytes || bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  async function createBackup() {
    if (!hasPerm('backups_create')) {
      alert('You do not have permission to create backups.');
      return;
    }
    const name = backupNameInput?.value?.trim() || 'Backup';
    const description = backupDescInput?.value?.trim() || '';
    if (backupCreateConfirm) {
      backupCreateConfirm.disabled = true;
      backupCreateConfirm.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Creating...';
    }
    try {
      const res = await fetch(`/api/nodes/server/${encodeURIComponent(bot)}/backups`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, description })
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        if (res.status === 507 || err.error === 'disk_limit_exceeded') {
          throw new Error(err.message || 'This server has exceeded its disk space limit. Delete files to free space before creating a backup.');
        }
        throw new Error(err.error || 'Failed to create backup');
      }
      closeBackupCreateModal();
      loadBackups();
      alert('Backup created successfully!');
    } catch (err) {
      alert('Failed to create backup: ' + (err.message || err));
    } finally {
      if (backupCreateConfirm) {
        backupCreateConfirm.disabled = false;
        backupCreateConfirm.innerHTML = '<i class="fa-solid fa-box-archive"></i><span>Create Backup</span>';
      }
    }
  }

  async function restoreBackup(id) {
    if (!id) return;
    if (!confirm('Are you sure you want to restore this backup? Current server files will be replaced.')) return;
    try {
      const res = await fetch(`/api/nodes/server/${encodeURIComponent(bot)}/backups/${encodeURIComponent(id)}/restore`, { method: 'POST' });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        if (res.status === 507 || err.error === 'disk_limit_exceeded') {
          throw new Error(err.message || 'This server has exceeded its disk space limit. Delete files to free space before restoring a backup.');
        }
        throw new Error(err.error || 'Failed to restore backup');
      }
      alert('Backup restored successfully!');
      loadBackups();
    } catch (err) {
      alert('Failed to restore backup: ' + (err.message || err));
    }
  }

  async function deleteBackup(id) {
    if (!id) return;
    if (!hasPerm('backups_delete')) {
      alert('You do not have permission to delete backups.');
      return;
    }
    if (!confirm('Are you sure you want to delete this backup? This cannot be undone.')) return;
    try {
      const res = await fetch(`/api/nodes/server/${encodeURIComponent(bot)}/backups/${encodeURIComponent(id)}`, { method: 'DELETE' });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.error || 'Failed to delete backup');
      }
      loadBackups();
    } catch (err) {
      alert('Failed to delete backup: ' + (err.message || err));
    }
  }

  function openBackupsModal() {
    if (!backupsModal) return;
    if (!hasPerm('backups_view')) {
      alert('You do not have permission to view backups.');
      return;
    }
    backupsModal.classList.add('show');
    backupsModal.style.display = 'flex';
    backupsModal.setAttribute('aria-hidden', 'false');
    if (backupsCreateBtn) {
      if (!hasPerm('backups_create')) {
        backupsCreateBtn.style.display = 'none';
      } else {
        backupsCreateBtn.style.display = '';
      }
    }
    loadBackups();
  }

  function closeBackupsModal() {
    if (!backupsModal) return;
    animateClose(backupsModal);
  }

  function openBackupCreateModal() {
    if (!backupCreateModal) return;
    if (backupNameInput) backupNameInput.value = '';
    if (backupDescInput) backupDescInput.value = '';
    backupCreateModal.classList.add('show');
    backupCreateModal.style.display = 'flex';
    backupCreateModal.setAttribute('aria-hidden', 'false');
  }

  function closeBackupCreateModal() {
    if (!backupCreateModal) return;
    animateClose(backupCreateModal);
  }

  if (backupsModalClose) backupsModalClose.addEventListener('click', closeBackupsModal);
  if (backupsCreateBtn) backupsCreateBtn.addEventListener('click', openBackupCreateModal);
  if (backupsRefreshBtn) backupsRefreshBtn.addEventListener('click', loadBackups);
  if (backupCreateClose) backupCreateClose.addEventListener('click', closeBackupCreateModal);
  if (backupCreateCancel) backupCreateCancel.addEventListener('click', closeBackupCreateModal);
  if (backupCreateConfirm) backupCreateConfirm.addEventListener('click', createBackup);

  const schedulerModal = document.getElementById('schedulerModal');
  const schedulerModalClose = document.getElementById('schedulerModalClose');
  const schedulerTasksList = document.getElementById('schedulerTasksList');
  const schedulerRefreshBtn = document.getElementById('schedulerRefreshBtn');
  const schedulerCreateBtn = document.getElementById('schedulerCreateBtn');
  const schedulerTabs = document.querySelectorAll('.scheduler-tab');
  const schedulerTasksTab = document.getElementById('schedulerTasksTab');
  const schedulerCreateTab = document.getElementById('schedulerCreateTab');
  const schedulerStatusIndicator = document.getElementById('schedulerStatusIndicator');

  const schedulerTaskName = document.getElementById('schedulerTaskName');
  const schedulerScheduleOptions = document.querySelectorAll('.scheduler-schedule-option');
  const schedulerOnceSection = document.getElementById('schedulerOnceSection');
  const schedulerRecurringSection = document.getElementById('schedulerRecurringSection');
  const schedulerDate = document.getElementById('schedulerDate');
  const schedulerTime = document.getElementById('schedulerTime');
  const schedulerRecurringType = document.getElementById('schedulerRecurringType');
  const schedulerSecondsConfig = document.getElementById('schedulerSecondsConfig');
  const schedulerMinutesConfig = document.getElementById('schedulerMinutesConfig');
  const schedulerHourlyConfig = document.getElementById('schedulerHourlyConfig');
  const schedulerDailyConfig = document.getElementById('schedulerDailyConfig');
  const schedulerWeeklyConfig = document.getElementById('schedulerWeeklyConfig');
  const schedulerSecondsInterval = document.getElementById('schedulerSecondsInterval');
  const schedulerMinutesInterval = document.getElementById('schedulerMinutesInterval');
  const schedulerHourlyInterval = document.getElementById('schedulerHourlyInterval');
  const schedulerDailyTime = document.getElementById('schedulerDailyTime');
  const schedulerWeeklyDay = document.getElementById('schedulerWeeklyDay');
  const schedulerWeeklyTime = document.getElementById('schedulerWeeklyTime');
  const schedulerActionType = document.getElementById('schedulerActionType');
  const schedulerActionConsole = document.getElementById('schedulerActionConsole');
  const schedulerActionServerControl = document.getElementById('schedulerActionServerControl');
  const schedulerActionFile = document.getElementById('schedulerActionFile');
  const schedulerActionBackup = document.getElementById('schedulerActionBackup');
  const schedulerCommand = document.getElementById('schedulerCommand');
  const schedulerServerControlText = document.getElementById('schedulerServerControlText');
  const schedulerFilePath = document.getElementById('schedulerFilePath');
  const schedulerFileContent = document.getElementById('schedulerFileContent');
  const schedulerBackupName = document.getElementById('schedulerBackupName');
  const schedulerBackupDesc = document.getElementById('schedulerBackupDesc');

  let schedulerCurrentScheduleType = 'once';

  function openSchedulerModal() {
    if (!schedulerModal) return;
    if (!hasPerm('scheduler_access')) {
      alert('You do not have permission to access the scheduler.');
      return;
    }
    schedulerModal.classList.add('show');
    schedulerModal.style.display = 'flex';
    schedulerModal.setAttribute('aria-hidden', 'false');
    loadSchedulerTasks();
    checkSchedulerStatus();
    if (schedulerDate) {
      const today = new Date();
      schedulerDate.value = today.toISOString().split('T')[0];
    }
    const createTab = document.querySelector('.scheduler-tab[data-tab="create"]');
    if (createTab) {
      if (!hasPerm('scheduler_create')) {
        createTab.style.display = 'none';
      } else {
        createTab.style.display = '';
      }
    }
  }

  function closeSchedulerModal() {
    if (!schedulerModal) return;
    animateClose(schedulerModal);
  }

  async function loadSchedulerTasks() {
    if (!schedulerTasksList) return;
    schedulerTasksList.innerHTML = '<div class="scheduler-loading"><div class="scheduler-loading-spinner"></div><span>Loading tasks...</span></div>';

    if (schedulerRefreshBtn) {
      schedulerRefreshBtn.classList.add('spinning');
    }

    try {
      const res = await fetch(`/api/scheduler/${encodeURIComponent(bot)}/tasks`);
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.error || 'Failed to load tasks');
      }
      const data = await res.json();
      renderSchedulerTasks(data.tasks || [], data.recurring || []);
    } catch (err) {
      console.error('Scheduler tasks load error:', err);
      schedulerTasksList.innerHTML = '<div class="scheduler-empty"><i class="fa-solid fa-exclamation-triangle"></i><p>' + escapeHtml(err.message || 'Failed to load tasks') + '</p></div>';
    } finally {
      if (schedulerRefreshBtn) {
        schedulerRefreshBtn.classList.remove('spinning');
      }
    }
  }

  function renderSchedulerTasks(tasks, recurring) {
    const allTasks = [...tasks, ...recurring];
    const canDelete = hasPerm('scheduler_delete');

    if (allTasks.length === 0) {
      schedulerTasksList.innerHTML = '<div class="scheduler-empty"><i class="fa-solid fa-calendar-xmark"></i><p>No scheduled tasks</p><p style="font-size:12px;margin-top:8px;color:rgba(255,255,255,0.3)">Create a task to automate server operations</p></div>';
      return;
    }

    schedulerTasksList.innerHTML = allTasks.map(task => {
      const icon = getActionIcon(task.actionType);
      const statusClass = task.status || 'pending';
      const timeText = task.status === 'recurring'
        ? `Cron: ${task.cron || 'N/A'}`
        : task.scheduledFor
          ? new Date(task.scheduledFor).toLocaleString()
          : 'N/A';

      const deleteBtn = canDelete
        ? `<button class="scheduler-task-delete" data-id="${escapeHtml(task.id || '')}" title="Delete task">
            <i class="fa-solid fa-trash"></i>
          </button>`
        : '';

      return `
        <div class="scheduler-task-item" data-id="${escapeHtml(task.id || '')}">
          <div class="scheduler-task-icon">${icon}</div>
          <div class="scheduler-task-info">
            <div class="scheduler-task-name">${escapeHtml(task.name || 'Unnamed Task')}</div>
            <div class="scheduler-task-meta">
              <span><i class="fa-solid fa-clock"></i> ${escapeHtml(timeText)}</span>
              <span><i class="fa-solid fa-bolt"></i> ${escapeHtml(task.actionType || 'unknown')}</span>
            </div>
          </div>
          <span class="scheduler-task-status ${statusClass}">${statusClass}</span>
          ${deleteBtn}
        </div>
      `;
    }).join('');

    if (canDelete) {
      schedulerTasksList.querySelectorAll('.scheduler-task-delete').forEach(btn => {
        btn.addEventListener('click', (e) => {
          e.stopPropagation();
          deleteSchedulerTask(btn.dataset.id);
        });
      });
    }
  }

  function getActionIcon(actionType) {
    const icons = {
      'console_command': '<i class="fa-solid fa-terminal"></i>',
      'server_start': '<i class="fa-solid fa-play"></i>',
      'server_stop': '<i class="fa-solid fa-stop"></i>',
      'create_file': '<i class="fa-solid fa-file-circle-plus"></i>',
      'modify_file': '<i class="fa-solid fa-file-pen"></i>',
      'backup': '<i class="fa-solid fa-box-archive"></i>'
    };
    return icons[actionType] || '<i class="fa-solid fa-gear"></i>';
  }

  async function deleteSchedulerTask(taskId) {
    if (!taskId) return;
    if (!hasPerm('scheduler_delete')) {
      alert('You do not have permission to delete schedules');
      return;
    }
    if (!confirm('Are you sure you want to delete this scheduled task?')) return;

    try {
      const res = await fetch(`/api/scheduler/${encodeURIComponent(bot)}/tasks/${encodeURIComponent(taskId)}`, {
        method: 'DELETE'
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.error || 'Failed to delete task');
      }
      loadSchedulerTasks();
    } catch (err) {
      alert('Failed to delete task: ' + (err.message || err));
    }
  }

  async function createSchedulerTask() {
    if (!hasPerm('scheduler_create')) {
      alert('You do not have permission to create schedules');
      return;
    }
    const name = schedulerTaskName?.value?.trim() || '';
    const actionType = schedulerActionType?.value;

    if (!actionType) {
      alert('Please select an action type');
      return;
    }

    let scheduleType, scheduleValue, scheduleTime, scheduledTimestamp;
    let payload = {};

    if (schedulerCurrentScheduleType === 'once') {
      scheduleType = 'once';
      const dateVal = schedulerDate?.value;
      const timeVal = schedulerTime?.value || '00:00';
      if (!dateVal) {
        alert('Please select a date');
        return;
      }
      scheduledTimestamp = new Date(`${dateVal}T${timeVal}`).getTime();
      if (scheduledTimestamp < Date.now()) {
        alert('Scheduled time must be in the future');
        return;
      }
    } else {
      const recurringType = schedulerRecurringType?.value || 'seconds';
      scheduleType = recurringType;

      if (recurringType === 'seconds') {
        scheduleValue = schedulerSecondsInterval?.value || '30';
      } else if (recurringType === 'minutes') {
        scheduleValue = schedulerMinutesInterval?.value || '5';
      } else if (recurringType === 'hourly') {
        scheduleValue = schedulerHourlyInterval?.value || '1';
      } else if (recurringType === 'daily') {
        scheduleTime = schedulerDailyTime?.value || '00:00';
      } else if (recurringType === 'weekly') {
        scheduleValue = schedulerWeeklyDay?.value || '0';
        scheduleTime = schedulerWeeklyTime?.value || '00:00';
      }
    }

    if (actionType === 'console_command') {
      const command = schedulerCommand?.value?.trim();
      if (!command) {
        alert('Please enter a command');
        return;
      }
      payload.command = command;
    } else if (actionType === 'create_file' || actionType === 'modify_file') {
      const filePath = schedulerFilePath?.value?.trim();
      if (!filePath) {
        alert('Please enter a file path');
        return;
      }
      payload.filePath = filePath;
      payload.content = schedulerFileContent?.value || '';
    } else if (actionType === 'backup') {
      payload.backupName = schedulerBackupName?.value?.trim() || 'Scheduled Backup';
      payload.description = schedulerBackupDesc?.value?.trim() || '';
    }

    if (schedulerCreateBtn) {
      schedulerCreateBtn.disabled = true;
      schedulerCreateBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Creating...';
    }

    try {
      const res = await fetch(`/api/scheduler/${encodeURIComponent(bot)}/tasks`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: name || `${actionType} task`,
          actionType,
          payload,
          scheduleType,
          scheduleValue,
          scheduleTime,
          scheduledTimestamp
        })
      });

      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.error || 'Failed to create task');
      }

      switchSchedulerTab('tasks');
      loadSchedulerTasks();
      resetSchedulerForm();
      alert('Task created successfully!');
    } catch (err) {
      alert('Failed to create task: ' + (err.message || err));
    } finally {
      if (schedulerCreateBtn) {
        schedulerCreateBtn.disabled = false;
        schedulerCreateBtn.innerHTML = '<i class="fa-solid fa-plus"></i> <span>Create Task</span>';
      }
    }
  }

  function resetSchedulerForm() {
    if (schedulerTaskName) schedulerTaskName.value = '';
    if (schedulerCommand) schedulerCommand.value = '';
    if (schedulerFilePath) schedulerFilePath.value = '';
    if (schedulerFileContent) schedulerFileContent.value = '';
    if (schedulerBackupName) schedulerBackupName.value = '';
    if (schedulerBackupDesc) schedulerBackupDesc.value = '';
    if (schedulerActionType) schedulerActionType.value = '';
    hideAllActionConfigs();
  }

  function switchSchedulerTab(tabName) {
    schedulerTabs.forEach(tab => {
      tab.classList.toggle('active', tab.dataset.tab === tabName);
    });
    if (schedulerTasksTab) schedulerTasksTab.classList.toggle('hidden', tabName !== 'tasks');
    if (schedulerCreateTab) schedulerCreateTab.classList.toggle('hidden', tabName !== 'create');

    if (tabName === 'tasks') {
      loadSchedulerTasks();
    }
  }

  function switchScheduleType(type) {
    schedulerCurrentScheduleType = type;
    schedulerScheduleOptions.forEach(opt => {
      opt.classList.toggle('active', opt.dataset.schedule === type);
    });
    if (schedulerOnceSection) schedulerOnceSection.classList.toggle('hidden', type !== 'once');
    if (schedulerRecurringSection) schedulerRecurringSection.classList.toggle('hidden', type !== 'recurring');
  }

  function updateRecurringConfig() {
    const type = schedulerRecurringType?.value || 'seconds';
    if (schedulerSecondsConfig) schedulerSecondsConfig.classList.toggle('hidden', type !== 'seconds');
    if (schedulerMinutesConfig) schedulerMinutesConfig.classList.toggle('hidden', type !== 'minutes');
    if (schedulerHourlyConfig) schedulerHourlyConfig.classList.toggle('hidden', type !== 'hourly');
    if (schedulerDailyConfig) schedulerDailyConfig.classList.toggle('hidden', type !== 'daily');
    if (schedulerWeeklyConfig) schedulerWeeklyConfig.classList.toggle('hidden', type !== 'weekly');
  }

  function hideAllActionConfigs() {
    [schedulerActionConsole, schedulerActionServerControl, schedulerActionFile, schedulerActionBackup].forEach(el => {
      if (el) el.classList.add('hidden');
    });
  }

  function updateActionConfig() {
    hideAllActionConfigs();
    const action = schedulerActionType?.value;

    if (action === 'console_command' && schedulerActionConsole) {
      schedulerActionConsole.classList.remove('hidden');
    } else if ((action === 'server_start' || action === 'server_stop') && schedulerActionServerControl) {
      schedulerActionServerControl.classList.remove('hidden');
      if (schedulerServerControlText) {
        schedulerServerControlText.textContent = action === 'server_start'
          ? 'The server will be started at the scheduled time.'
          : 'The server will be stopped at the scheduled time.';
      }
    } else if ((action === 'create_file' || action === 'modify_file') && schedulerActionFile) {
      schedulerActionFile.classList.remove('hidden');
    } else if (action === 'backup' && schedulerActionBackup) {
      schedulerActionBackup.classList.remove('hidden');
    }
  }

  async function checkSchedulerStatus() {
    try {
      const res = await fetch('/api/scheduler/status');
      if (!res.ok) throw new Error('Status check failed');
      const data = await res.json();

      if (schedulerStatusIndicator) {
        const dot = schedulerStatusIndicator.querySelector('.scheduler-status-dot');
        const text = schedulerStatusIndicator.querySelector('.scheduler-status-text');

        if (data.available) {
          if (dot) dot.classList.remove('offline');
          const pendingCount = (data.queue?.waiting || 0) + (data.queue?.delayed || 0);
          if (text) text.textContent = `Scheduler Ready (${pendingCount} pending, ${data.queue?.active || 0} active)`;
        } else {
          if (dot) dot.classList.add('offline');
          if (text) {
            if (!data.redisConfigured) {
              text.textContent = 'Scheduler Offline (REDIS_URL not configured)';
            } else {
              text.textContent = 'Scheduler Offline (Connection error)';
            }
          }
        }
      }
    } catch (err) {
      console.error('Scheduler status check failed:', err);
      if (schedulerStatusIndicator) {
        const dot = schedulerStatusIndicator.querySelector('.scheduler-status-dot');
        const text = schedulerStatusIndicator.querySelector('.scheduler-status-text');
        if (dot) dot.classList.add('offline');
        if (text) text.textContent = 'Scheduler Status Unknown';
      }
    }
  }

  if (schedulerModalClose) schedulerModalClose.addEventListener('click', closeSchedulerModal);
  if (schedulerRefreshBtn) schedulerRefreshBtn.addEventListener('click', loadSchedulerTasks);
  if (schedulerCreateBtn) schedulerCreateBtn.addEventListener('click', createSchedulerTask);

  schedulerTabs.forEach(tab => {
    tab.addEventListener('click', () => switchSchedulerTab(tab.dataset.tab));
  });

  schedulerScheduleOptions.forEach(opt => {
    opt.addEventListener('click', () => switchScheduleType(opt.dataset.schedule));
  });

  if (schedulerRecurringType) {
    schedulerRecurringType.addEventListener('change', updateRecurringConfig);
  }

  if (schedulerActionType) {
    schedulerActionType.addEventListener('change', updateActionConfig);
  }

  if (schedulerModal) {
    schedulerModal.addEventListener('click', (e) => {
      if (e.target === schedulerModal || e.target.classList.contains('scheduler-glass-layer')) {
        closeSchedulerModal();
      }
    });
  }

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && schedulerModal?.classList.contains('show')) {
      closeSchedulerModal();
    }
    if (e.key === 'Escape') {
      const clearModal = document.getElementById('clearConsoleModal');
      if (typeof closeClearConsoleModal === 'function' && clearModal && clearModal.style.display !== 'none') {
        closeClearConsoleModal();
      } else if (clearModal && clearModal.style.display !== 'none') {
        clearModal.style.display = 'none';
      }
    }
  });

  const plannerModal = document.getElementById('plannerModal');
  const plannerModalClose = document.getElementById('plannerModalClose');
  const plannerRefreshBtn = document.getElementById('plannerRefreshBtn');
  const plannerTabs = document.querySelectorAll('.planner-tab');
  const plannerBoardTab = document.getElementById('plannerBoardTab');
  const plannerComposeTab = document.getElementById('plannerComposeTab');
  const plannerSummary = document.getElementById('plannerSummary');
  const plannerActiveList = document.getElementById('plannerActiveList');
  const plannerCompletedSection = document.getElementById('plannerCompletedSection');
  const plannerCompletedList = document.getElementById('plannerCompletedList');
  const plannerOpenComposeBtn = document.getElementById('plannerOpenComposeBtn');
  const plannerItemIdInput = document.getElementById('plannerItemId');
  const plannerTitleInput = document.getElementById('plannerTitleInput');
  const plannerPromptInput = document.getElementById('plannerPromptInput');
  const plannerResetBtn = document.getElementById('plannerResetBtn');
  const plannerSaveBtn = document.getElementById('plannerSaveBtn');
  const plannerStatusIndicator = document.getElementById('plannerStatusIndicator');

  const PLANNER_API_BASE = `/api/servers/${encodeURIComponent(bot)}/planner`;

  let plannerItems = [];
  let plannerCurrentTab = 'board';
  let plannerLastActiveOrder = [];

  function setPlannerStatus(text, offline = false) {
    if (!plannerStatusIndicator) return;
    const dot = plannerStatusIndicator.querySelector('.planner-status-dot');
    const label = plannerStatusIndicator.querySelector('.planner-status-text');
    if (dot) dot.classList.toggle('offline', !!offline);
    if (label) label.textContent = text;
  }

  function plannerRequest(path = '', options = {}) {
    return fetch(`${PLANNER_API_BASE}${path}`, options).then(async (response) => {
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.error || err.message || `Planner request failed (${response.status})`);
      }
      return response.json().catch(() => ({}));
    });
  }

  function derivePlannerTitle(prompt) {
    const clean = String(prompt || '').trim().replace(/\s+/g, ' ');
    if (!clean) return 'Untitled Prompt';
    return clean.length > 72 ? `${clean.slice(0, 69)}...` : clean;
  }

  function plannerAuthorLabel(email) {
    const raw = String(email || '').trim();
    if (!raw) return 'Team';
    const base = raw.split('@')[0] || raw;
    return base.slice(0, 28);
  }

  function plannerActiveItems() {
    return plannerItems.filter((item) => !Number(item.is_done));
  }

  function plannerCompletedItems() {
    return plannerItems.filter((item) => Number(item.is_done));
  }

  function switchPlannerTab(tabName) {
    plannerCurrentTab = tabName;
    plannerTabs.forEach((tab) => {
      tab.classList.toggle('active', tab.dataset.tab === tabName);
    });
    if (plannerBoardTab) plannerBoardTab.classList.toggle('hidden', tabName !== 'board');
    if (plannerComposeTab) plannerComposeTab.classList.toggle('hidden', tabName !== 'compose');
    if (tabName === 'compose') {
      setTimeout(() => plannerTitleInput?.focus(), 40);
    }
  }

  function resetPlannerForm() {
    if (plannerItemIdInput) plannerItemIdInput.value = '';
    if (plannerTitleInput) plannerTitleInput.value = '';
    if (plannerPromptInput) plannerPromptInput.value = '';
    if (plannerSaveBtn) {
      plannerSaveBtn.disabled = false;
      plannerSaveBtn.innerHTML = '<i class="fa-solid fa-plus"></i><span>Save Prompt</span>';
    }
  }

  function fillPlannerForm(item) {
    if (!item) return;
    if (plannerItemIdInput) plannerItemIdInput.value = String(item.id || '');
    if (plannerTitleInput) plannerTitleInput.value = item.title || '';
    if (plannerPromptInput) plannerPromptInput.value = item.prompt || '';
    if (plannerSaveBtn) {
      plannerSaveBtn.innerHTML = '<i class="fa-solid fa-floppy-disk"></i><span>Update Prompt</span>';
    }
    switchPlannerTab('compose');
    plannerTitleInput?.focus();
  }

  function renderPlannerSummaryCards(activeItems, completedItems) {
    if (!plannerSummary) return;
    const nextUp = activeItems[0];
    plannerSummary.innerHTML = `
      <div class="planner-summary-card">
        <span class="planner-summary-label">Active</span>
        <span class="planner-summary-value">${activeItems.length}</span>
        <span class="planner-summary-note">Shared prompts ready for the team on this server.</span>
      </div>
      <div class="planner-summary-card">
        <span class="planner-summary-label">Next Up</span>
        <span class="planner-summary-value">${nextUp ? 'P1' : '0'}</span>
        <span class="planner-summary-note">${escapeHtml(nextUp ? (nextUp.title || derivePlannerTitle(nextUp.prompt)) : 'Nothing queued yet')}</span>
      </div>
      <div class="planner-summary-card">
        <span class="planner-summary-label">Completed</span>
        <span class="planner-summary-value">${completedItems.length}</span>
        <span class="planner-summary-note">Finished prompts can be reopened or sent again later.</span>
      </div>
    `;
  }

  function renderPlannerItemCard(item, index, isDone = false) {
    const title = escapeHtml(item.title || derivePlannerTitle(item.prompt));
    const prompt = escapeHtml(item.prompt || '').replace(/\n/g, '<br>');
    const relativeTime = item.updated_at ? formatRelativeTime(item.updated_at) : 'Just now';
    const priorityBadge = isDone
      ? '<span class="planner-priority-badge done">Done</span>'
      : `<span class="planner-priority-badge">${index === 0 ? 'Now' : `P${index + 1}`}</span>`;

    return `
      <article class="planner-item ${isDone ? 'is-done' : ''}" data-id="${escapeHtml(item.id)}" ${isDone ? '' : 'draggable="true"'}>
        <button class="planner-item-main" type="button" data-planner-action="send" title="Send to ADPanel Agent">
          <div class="planner-item-heading">
            <span class="planner-drag-handle" aria-hidden="true"><i class="fa-solid fa-grip-vertical"></i></span>
            <div class="planner-item-title-wrap">
              <div class="planner-item-title">${title}</div>
              <div class="planner-item-meta">
                ${priorityBadge}
                <span><i class="fa-regular fa-clock"></i> ${escapeHtml(relativeTime)}</span>
                <span><i class="fa-regular fa-user"></i> ${escapeHtml(plannerAuthorLabel(item.updated_by_email || item.created_by_email))}</span>
              </div>
            </div>
          </div>
          <div class="planner-item-prompt">${prompt}</div>
        </button>
        <div class="planner-item-actions">
          <button class="planner-item-action send" type="button" data-planner-action="send" title="Send to ADPanel Agent">
            <i class="fa-solid fa-paper-plane"></i>
          </button>
          <button class="planner-item-action edit" type="button" data-planner-action="edit" title="Edit prompt">
            <i class="fa-solid fa-pen"></i>
          </button>
          <button class="planner-item-action toggle" type="button" data-planner-action="toggle" title="${isDone ? 'Move back to active' : 'Mark as done'}">
            <i class="fa-solid ${isDone ? 'fa-rotate-left' : 'fa-check'}"></i>
          </button>
          <button class="planner-item-action delete" type="button" data-planner-action="delete" title="Delete prompt">
            <i class="fa-solid fa-trash"></i>
          </button>
        </div>
      </article>
    `;
  }

  function renderPlannerBoard() {
    if (!plannerActiveList || !plannerCompletedList) return;
    const activeItems = plannerActiveItems();
    const completedItems = plannerCompletedItems();

    renderPlannerSummaryCards(activeItems, completedItems);
    plannerLastActiveOrder = activeItems.map((item) => Number(item.id));

    if (!activeItems.length) {
      plannerActiveList.innerHTML = `
        <div class="planner-empty">
          <i class="fa-solid fa-layer-group"></i>
          <p>No prompts in the queue yet.</p>
          <button class="planner-empty-btn" type="button" id="plannerEmptyComposeBtn">
            <i class="fa-solid fa-plus"></i>
            <span>Create the first prompt</span>
          </button>
        </div>
      `;
      const emptyBtn = document.getElementById('plannerEmptyComposeBtn');
      if (emptyBtn) {
        emptyBtn.addEventListener('click', () => {
          resetPlannerForm();
          switchPlannerTab('compose');
        });
      }
    } else {
      plannerActiveList.innerHTML = activeItems.map((item, index) => renderPlannerItemCard(item, index, false)).join('');
      bindPlannerDragAndDrop();
    }

    if (!completedItems.length) {
      plannerCompletedSection?.classList.add('hidden');
      plannerCompletedList.innerHTML = '';
    } else {
      plannerCompletedSection?.classList.remove('hidden');
      plannerCompletedList.innerHTML = completedItems.map((item, index) => renderPlannerItemCard(item, index, true)).join('');
    }
  }

  async function loadPlannerItems() {
    if (!plannerActiveList || !plannerCompletedList) return;
    plannerActiveList.innerHTML = '<div class="planner-loading"><div class="planner-loading-spinner"></div><span>Loading planner...</span></div>';
    plannerCompletedList.innerHTML = '';
    if (plannerRefreshBtn) plannerRefreshBtn.classList.add('spinning');
    setPlannerStatus('Loading planner...');

    try {
      const data = await plannerRequest();
      plannerItems = Array.isArray(data.items) ? data.items : [];
      renderPlannerBoard();
      const activeCount = plannerActiveItems().length;
      const completedCount = plannerCompletedItems().length;
      setPlannerStatus(`${activeCount} active, ${completedCount} completed`);
    } catch (err) {
      console.error('[planner] Load error:', err);
      plannerItems = [];
      if (plannerSummary) plannerSummary.innerHTML = '';
      plannerActiveList.innerHTML = `<div class="planner-empty"><i class="fa-solid fa-triangle-exclamation"></i><p>${escapeHtml(err.message || 'Failed to load planner')}</p></div>`;
      plannerCompletedSection?.classList.add('hidden');
      setPlannerStatus('Planner unavailable', true);
    } finally {
      if (plannerRefreshBtn) plannerRefreshBtn.classList.remove('spinning');
    }
  }

  function openPlannerModal() {
    if (!plannerModal) return;
    plannerModal.classList.add('show');
    plannerModal.style.display = 'flex';
    plannerModal.setAttribute('aria-hidden', 'false');
    switchPlannerTab('board');
    loadPlannerItems();
  }

  function closePlannerModal() {
    if (!plannerModal) return;
    animateClose(plannerModal);
  }

  function plannerGetDragAfterElement(container, y) {
    const draggableElements = [...container.querySelectorAll('.planner-item:not(.dragging)')];
    return draggableElements.reduce((closest, child) => {
      const box = child.getBoundingClientRect();
      const offset = y - box.top - box.height / 2;
      if (offset < 0 && offset > closest.offset) {
        return { offset, element: child };
      }
      return closest;
    }, { offset: Number.NEGATIVE_INFINITY, element: null }).element;
  }

  function bindPlannerDragAndDrop() {
    if (!plannerActiveList) return;
    plannerActiveList.querySelectorAll('.planner-item').forEach((item) => {
      item.addEventListener('dragstart', () => {
        item.classList.add('dragging');
      });

      item.addEventListener('dragend', async () => {
        item.classList.remove('dragging');
        const orderedIds = [...plannerActiveList.querySelectorAll('.planner-item')]
          .map((entry) => parseInt(entry.dataset.id, 10))
          .filter((id) => !Number.isNaN(id));
        if (!orderedIds.length) return;
        if (JSON.stringify(orderedIds) === JSON.stringify(plannerLastActiveOrder)) return;
        try {
          const data = await plannerRequest('/reorder', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ itemIds: orderedIds })
          });
          plannerItems = Array.isArray(data.items) ? data.items : plannerItems;
          renderPlannerBoard();
          setPlannerStatus('Priority updated');
        } catch (err) {
          console.error('[planner] Reorder error:', err);
          await loadPlannerItems();
          alert(err.message || 'Failed to update planner order');
        }
      });
    });

    if (plannerActiveList.dataset.dragBound === '1') return;
    plannerActiveList.dataset.dragBound = '1';

    plannerActiveList.addEventListener('dragover', (e) => {
      e.preventDefault();
      const dragging = plannerActiveList.querySelector('.planner-item.dragging');
      if (!dragging) return;
      const afterElement = plannerGetDragAfterElement(plannerActiveList, e.clientY);
      if (!afterElement) {
        plannerActiveList.appendChild(dragging);
      } else if (afterElement !== dragging) {
        plannerActiveList.insertBefore(dragging, afterElement);
      }
    });
  }

  async function sendPlannerPromptToAgent(item) {
    if (!item || !String(item.prompt || '').trim()) {
      alert('This planner prompt is empty.');
      return;
    }
    if (!hasAgentAccess()) {
      alert('You do not have permission to access ADPanel Agent.');
      return;
    }

    closePlannerModal();
    await new Promise((resolve) => setTimeout(resolve, 120));
    await openAiHelpModal();

    if (aiChatInput) {
      aiChatInput.value = String(item.prompt || '').trim();
      aiChatInput.style.height = 'auto';
      aiChatInput.style.height = Math.min(aiChatInput.scrollHeight, 120) + 'px';
    }
    updateSendButton();

    if (hasConfiguredProvider() && aiModelSelect?.value && !aiIsGenerating) {
      await sendAiMessage();
      return;
    }

    alert('Planner prompt prepared in ADPanel Agent. Connect an AI provider to send it.');
  }

  async function savePlannerItem() {
    const prompt = String(plannerPromptInput?.value || '').trim();
    const id = parseInt(plannerItemIdInput?.value || '', 10);
    const isEditing = !Number.isNaN(id);
    let restoreAsEdit = isEditing;
    const title = String(plannerTitleInput?.value || '').trim() || derivePlannerTitle(prompt);

    if (!prompt) {
      alert('Write a prompt first.');
      plannerPromptInput?.focus();
      return;
    }

    if (plannerSaveBtn) {
      plannerSaveBtn.disabled = true;
      plannerSaveBtn.innerHTML = `<i class="fa-solid fa-spinner fa-spin"></i><span>${isEditing ? 'Updating...' : 'Saving...'}</span>`;
    }

    try {
      if (!isEditing) {
        await plannerRequest('', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ title, prompt })
        });
      } else {
        await plannerRequest(`/${encodeURIComponent(id)}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ title, prompt })
        });
      }

      resetPlannerForm();
      restoreAsEdit = false;
      switchPlannerTab('board');
      await loadPlannerItems();
      setPlannerStatus('Prompt saved');
    } catch (err) {
      console.error('[planner] Save error:', err);
      alert(err.message || 'Failed to save prompt');
    } finally {
      if (plannerSaveBtn) {
        plannerSaveBtn.disabled = false;
        plannerSaveBtn.innerHTML = restoreAsEdit
          ? '<i class="fa-solid fa-floppy-disk"></i><span>Update Prompt</span>'
          : '<i class="fa-solid fa-plus"></i><span>Save Prompt</span>';
      }
    }
  }

  async function togglePlannerItem(item) {
    if (!item) return;
    try {
      await plannerRequest(`/${encodeURIComponent(item.id)}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          title: item.title || derivePlannerTitle(item.prompt),
          prompt: item.prompt || '',
          isDone: !Number(item.is_done)
        })
      });
      await loadPlannerItems();
      setPlannerStatus(Number(item.is_done) ? 'Prompt moved back to active' : 'Prompt marked as completed');
    } catch (err) {
      console.error('[planner] Toggle error:', err);
      alert(err.message || 'Failed to update prompt');
    }
  }

  async function deletePlannerItem(item) {
    if (!item) return;
    if (!confirm(`Delete "${item.title || derivePlannerTitle(item.prompt)}"?`)) return;
    try {
      await plannerRequest(`/${encodeURIComponent(item.id)}`, { method: 'DELETE' });
      await loadPlannerItems();
      setPlannerStatus('Prompt deleted');
    } catch (err) {
      console.error('[planner] Delete error:', err);
      alert(err.message || 'Failed to delete prompt');
    }
  }

  async function handlePlannerListClick(e) {
    const itemEl = e.target.closest('.planner-item');
    if (!itemEl) return;
    const itemId = parseInt(itemEl.dataset.id, 10);
    if (Number.isNaN(itemId)) return;
    const item = plannerItems.find((entry) => Number(entry.id) === itemId);
    if (!item) return;

    const actionEl = e.target.closest('[data-planner-action]');
    const action = actionEl?.dataset?.plannerAction;
    if (!action) return;

    if (action === 'send') {
      await sendPlannerPromptToAgent(item);
    } else if (action === 'edit') {
      fillPlannerForm(item);
    } else if (action === 'toggle') {
      await togglePlannerItem(item);
    } else if (action === 'delete') {
      await deletePlannerItem(item);
    }
  }

  if (plannerActiveList) {
    plannerActiveList.addEventListener('click', (e) => {
      handlePlannerListClick(e).catch((err) => console.error('[planner] Active list error:', err));
    });
  }

  if (plannerCompletedList) {
    plannerCompletedList.addEventListener('click', (e) => {
      handlePlannerListClick(e).catch((err) => console.error('[planner] Completed list error:', err));
    });
  }

  if (plannerModalClose) plannerModalClose.addEventListener('click', closePlannerModal);
  if (plannerRefreshBtn) plannerRefreshBtn.addEventListener('click', loadPlannerItems);
  if (plannerOpenComposeBtn) {
    plannerOpenComposeBtn.addEventListener('click', () => {
      resetPlannerForm();
      switchPlannerTab('compose');
    });
  }
  if (plannerResetBtn) plannerResetBtn.addEventListener('click', resetPlannerForm);
  if (plannerSaveBtn) plannerSaveBtn.addEventListener('click', savePlannerItem);
  plannerTabs.forEach((tab) => {
    tab.addEventListener('click', () => switchPlannerTab(tab.dataset.tab));
  });

  if (plannerModal) {
    plannerModal.addEventListener('click', (e) => {
      if (e.target === plannerModal || e.target.classList.contains('planner-glass-layer')) {
        closePlannerModal();
      }
    });
  }

  if (plannerPromptInput) {
    plannerPromptInput.addEventListener('keydown', (e) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
        e.preventDefault();
        savePlannerItem();
      }
    });
  }

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && plannerModal?.classList.contains('show')) {
      closePlannerModal();
    }
  });

  const clearConsoleModal = document.getElementById('clearConsoleModal');
  const clearConsoleConfirm = document.getElementById('clearConsoleConfirm');
  const clearConsoleCancel = document.getElementById('clearConsoleCancel');
  const clearConsoleToggleWrapper = document.getElementById('clearConsoleToggleWrapper');
  const dockWrap = document.querySelector('.dock-wrap');

  if (clearConsoleToggleWrapper) {
    clearConsoleToggleWrapper.addEventListener('click', function (e) {
      var cb = document.getElementById('clearConsoleNoAsk');
      if (!cb) return;
      if (e.target === cb) return;
      e.preventDefault();
      cb.checked = !cb.checked;
    });
  }

  function toggleDock(show) {
    if (!dockWrap) return;
    dockWrap.classList.toggle('dock-hidden', !show);
  }

  function closeClearConsoleModal() {
    if (!clearConsoleModal) return;
    clearConsoleModal.classList.remove('show');
    setTimeout(function () {
      if (!clearConsoleModal.classList.contains('show')) {
        clearConsoleModal.style.display = 'none';
        toggleDock(true);
      }
    }, 300);
  }

  function openClearConsoleModal() {
    if (!clearConsoleModal) return;
    clearConsoleModal.style.display = 'flex';
    requestAnimationFrame(function () {
      clearConsoleModal.classList.add('show');
      toggleDock(false);
    });
    var cb = document.getElementById('clearConsoleNoAsk');
    if (cb) cb.checked = false;
  }

  if (clearConsoleConfirm) {
    clearConsoleConfirm.addEventListener('click', async function () {
      resetConsoleBuffer();

      var cb = document.getElementById('clearConsoleNoAsk');
      if (cb && cb.checked) {
        try {
          var resp = await fetch('/api/me/preferences', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify({ preferences: { clear_console_no_ask: true } })
          });
          var data = await resp.json();
          if (data && data.ok) {
            currentUserPreferences.clear_console_no_ask = true;
            console.log('[ClearConsole] Preference saved successfully');
          } else {
            console.error('[ClearConsole] Save failed:', data);
          }
        } catch (err) {
          console.error('[ClearConsole] Failed to save preference:', err);
        }
      }
      closeClearConsoleModal();
    });
  }

  if (clearConsoleCancel) {
    clearConsoleCancel.addEventListener('click', function () {
      closeClearConsoleModal();
    });
  }

  if (clearConsoleModal) {
    clearConsoleModal.addEventListener('click', function (e) {
      if (e.target === clearConsoleModal || e.target.classList.contains('clear-console-glass-layer')) {
        closeClearConsoleModal();
      }
    });
  }

  const resourceStatsPopup = document.getElementById('resourceStatsPopup');
  const resourceStatsClose = document.getElementById('resourceStatsClose');
  const resMemoryLabel = document.getElementById('resMemoryLabel');
  const resMemoryBar = document.getElementById('resMemoryBar');
  const resDiskLabel = document.getElementById('resDiskLabel');
  const resDiskBar = document.getElementById('resDiskBar');
  const resCpuLabel = document.getElementById('resCpuLabel');
  const resCpuBar = document.getElementById('resCpuBar');
  const resUptimeLabel = document.getElementById('resUptimeLabel');

  let resourceStatsUptimeInterval = null;
  let lastFetchedUptime = null;
  let uptimeFetchedAt = null;
  let resourceStatsSubscribed = false;

  function onResourceData(data) {
    if (!data || data.server !== bot) return;
    const isRunning = data.status === 'running' || data.status === 'online';

    if (data.memory && isRunning) {
      let usedMb, limitMb, pct;
      if (typeof data.memory === 'object') {
        usedMb = data.memory.used || 0;
        limitMb = data.memory.total || 0;
        pct = data.memory.percent ?? (limitMb > 0 ? Math.min(100, Math.round((usedMb / limitMb) * 100)) : 0);
      } else {
        usedMb = data.memory; limitMb = 0; pct = 0;
      }
      if (resMemoryLabel) resMemoryLabel.textContent = limitMb > 0
        ? `${formatBytes(usedMb * 1048576)} / ${formatBytes(limitMb * 1048576)} (${Math.round(pct)}%)`
        : `${formatBytes(usedMb * 1048576)} / Unlimited`;
      if (resMemoryBar) resMemoryBar.style.width = `${pct}%`;
    } else {
      if (resMemoryLabel) resMemoryLabel.textContent = '—';
      if (resMemoryBar) resMemoryBar.style.width = '0%';
    }

    if (data.disk) {
      let usedGb, limitGb, pct;
      if (typeof data.disk === 'object') {
        usedGb = data.disk.used || 0;
        limitGb = data.disk.total || 0;
        pct = data.disk.percent ?? (limitGb > 0 ? Math.min(100, Math.round((usedGb / limitGb) * 100)) : 0);
      } else {
        usedGb = data.disk / 1024; limitGb = 0; pct = 0;
      }
      if (resDiskLabel) resDiskLabel.textContent = limitGb > 0
        ? `${formatBytes(usedGb * 1073741824)} / ${formatBytes(limitGb * 1073741824)} (${Math.round(pct)}%)`
        : `${formatBytes(usedGb * 1073741824)} / Unlimited`;
      if (resDiskBar) resDiskBar.style.width = `${pct}%`;
    } else {
      if (resDiskLabel) resDiskLabel.textContent = '—';
      if (resDiskBar) resDiskBar.style.width = '0%';
    }

    if (data.cpu !== undefined && data.cpu !== null && isRunning) {
      const cpuMax = data.cpuLimit || 100;
      const cpuPct = Math.min(cpuMax, Math.round(data.cpu));
      const cpuBarWidth = cpuMax > 0 ? Math.min(100, (cpuPct / cpuMax) * 100) : 0;
      if (resCpuLabel) resCpuLabel.textContent = `${cpuPct}% / ${Math.round(cpuMax)}%`;
      if (resCpuBar) resCpuBar.style.width = `${cpuBarWidth}%`;
    } else {
      if (resCpuLabel) resCpuLabel.textContent = '—';
      if (resCpuBar) resCpuBar.style.width = '0%';
    }

    if (data.uptime && data.uptime > 0 && isRunning) {
      lastFetchedUptime = data.uptime;
      uptimeFetchedAt = Date.now();
      if (resUptimeLabel) resUptimeLabel.textContent = formatUptime(data.uptime);
      startUptimeCounter();
    } else {
      lastFetchedUptime = null;
      uptimeFetchedAt = null;
      stopUptimeCounter();
      if (resUptimeLabel) resUptimeLabel.textContent = isRunning ? 'Starting...' : 'Offline';
    }
  }

  function subscribeResourceStats() {
    if (resourceStatsSubscribed) return;
    resourceStatsSubscribed = true;
    socket.on('resources:data', onResourceData);
    socket.emit('resources:subscribe', { server: bot });
  }

  function unsubscribeResourceStats() {
    if (!resourceStatsSubscribed) return;
    resourceStatsSubscribed = false;
    socket.off('resources:data', onResourceData);
    socket.emit('resources:unsubscribe', { server: bot });
    stopUptimeCounter();
  }

  socket.on('connect', () => {
    if (resourceStatsSubscribed) {
      resourceStatsSubscribed = false;
      if (resourceStatsPopup && resourceStatsPopup.classList.contains('visible')) {
        subscribeResourceStats();
      }
    }
  });

  function startUptimeCounter() {
    stopUptimeCounter();
    if (lastFetchedUptime === null || uptimeFetchedAt === null) return;
    resourceStatsUptimeInterval = setInterval(() => {
      if (lastFetchedUptime === null || uptimeFetchedAt === null) {
        stopUptimeCounter();
        return;
      }
      const elapsed = Math.floor((Date.now() - uptimeFetchedAt) / 1000);
      const currentUptime = lastFetchedUptime + elapsed;
      if (resUptimeLabel) resUptimeLabel.textContent = formatUptime(currentUptime);
    }, 1000);
  }

  function stopUptimeCounter() {
    if (resourceStatsUptimeInterval) {
      clearInterval(resourceStatsUptimeInterval);
      resourceStatsUptimeInterval = null;
    }
  }

  function formatUptime(seconds) {
    if (!seconds || seconds < 0) return '—';
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);
    const parts = [];
    if (days > 0) parts.push(`${days}d`);
    if (hours > 0) parts.push(`${hours}h`);
    if (mins > 0) parts.push(`${mins}m`);
    if (secs > 0 || parts.length === 0) parts.push(`${secs}s`);
    return parts.join(' ');
  }

  function openResourceStatsPopup() {
    if (!resourceStatsPopup) return;
    resourceStatsPopup.classList.add('visible');
    if (floatingSidebar && floatingSidebar.matches(':hover')) {
      resourceStatsPopup.classList.add('sidebar-expanded');
    }
    subscribeResourceStats();
  }

  function closeResourceStatsPopup() {
    if (!resourceStatsPopup) return;
    resourceStatsPopup.classList.remove('visible');
    resourceStatsPopup.classList.remove('sidebar-expanded');
    unsubscribeResourceStats();
    lastFetchedUptime = null;
    uptimeFetchedAt = null;
  }

  if (floatingSidebar && resourceStatsPopup) {
    floatingSidebar.addEventListener('mouseenter', () => {
      if (resourceStatsPopup.classList.contains('visible')) {
        resourceStatsPopup.classList.add('sidebar-expanded');
      }
    });
    floatingSidebar.addEventListener('mouseleave', () => {
      resourceStatsPopup.classList.remove('sidebar-expanded');
    });
  }

  if (resourceStatsClose) resourceStatsClose.addEventListener('click', closeResourceStatsPopup);
  document.addEventListener('click', (e) => {
    if (resourceStatsPopup && resourceStatsPopup.classList.contains('visible')) {
      if (!resourceStatsPopup.contains(e.target) && !e.target.closest('[data-action="open-resource-stats"]')) {
        closeResourceStatsPopup();
      }
    }
  });

  const openResourceStatsTab = document.getElementById("openResourceStatsTab");
  if (openResourceStatsTab) {
    openResourceStatsTab.addEventListener("click", () => {
      window.open(`/server/${encodeURIComponent(bot)}/resources`, 'ResourceStats', 'width=400,height=500,resizable=yes,scrollbars=yes,status=no,toolbar=no,menubar=no,location=no');
      closeResourceStatsPopup();
    });
  }

  copyAddressBtn.addEventListener('click', async () => {
    try {
      if (!infoAddress.value) return;
      await navigator.clipboard.writeText(infoAddress.value);
      const icon = copyAddressBtn.querySelector('i');
      if (icon) {
        icon.className = 'fa-solid fa-check';
        setTimeout(() => { icon.className = 'fa-solid fa-copy'; }, 1200);
      }
    } catch (e) { infoAddress.select(); document.execCommand('copy'); }
  });
  if (storeModalClose) storeModalClose.addEventListener('click', closeStoreModal);
  if (storePrefClose) storePrefClose.addEventListener('click', closeStorePrefModal);
  if (storePrefModal) storePrefModal.addEventListener('click', (e) => { if (e.target === storePrefModal) closeStorePrefModal(); });
  if (storeTabVersions) storeTabVersions.addEventListener('click', () => {
    renderStoreProvidersView();
  });
  if (storeTabPlugins) storeTabPlugins.addEventListener('click', () => {
    if (storeTabPlugins.classList.contains('hidden')) return;
    renderPluginsUI();
  });
  function focusConsole() { openInfoModal(); }
  function clearSelection() {
    selectedEntries.clear();
    if (selectAllToggle) selectAllToggle.checked = false;
    updateSelectionUI();
  }
  function updateSelectionUI() {
    if (!entriesEl) return;
    const entries = Array.from(entriesEl.querySelectorAll('.entry'));
    entries.forEach(li => {
      const rel = li.dataset.relPath || "";
      const isSelected = selectedEntries.has(rel);
      li.classList.toggle('selected', isSelected);
      li.classList.toggle('selectable', entries.length > 0);
    });
    const allSelected = entries.length > 0 && entries.every(li => selectedEntries.has(li.dataset.relPath || ""));
    if (selectAllToggle) selectAllToggle.checked = allSelected;
    if (bulkDeleteBtn) bulkDeleteBtn.classList.toggle('show', selectedEntries.size > 0 && hasPerm('files_delete'));
    if (bulkArchiveBtn) bulkArchiveBtn.classList.toggle('show', selectedEntries.size > 0 && hasPerm('files_archive'));
  }
  function toggleEntrySelection(li) {
    if (!li) return;
    const rel = li.dataset.relPath;
    if (!rel) return;
    if (selectedEntries.has(rel)) selectedEntries.delete(rel); else selectedEntries.add(rel);
    updateSelectionUI();
  }
  function selectAllEntries() {
    if (!entriesEl) return;
    selectedEntries.clear();
    entriesEl.querySelectorAll('.entry').forEach(li => {
      const rel = li.dataset.relPath;
      if (rel) selectedEntries.add(rel);
    });
    updateSelectionUI();
  }
  let currentPath = "";
  let activeInlineRename = null;
  function cancelActiveInlineRename() {
    if (!activeInlineRename || typeof activeInlineRename.cancel !== 'function') return;
    activeInlineRename.cancel();
  }
  function startInlineRename(li, nameNode, currentName, oldPath) {
    if (!li || !nameNode) return;
    if (activeInlineRename && activeInlineRename.li !== li) cancelActiveInlineRename();
    if (li.classList.contains('renaming')) return;

    let submitting = false;
    const input = document.createElement("input");
    input.type = "text";
    input.className = "entry-rename-input";
    input.value = currentName;
    input.setAttribute("aria-label", `Rename ${currentName}`);

    const actions = document.createElement("div");
    actions.className = "entry-rename-actions";

    const saveBtn = document.createElement("button");
    saveBtn.type = "button";
    saveBtn.className = "entry-rename-btn save";
    saveBtn.title = "Save";
    saveBtn.setAttribute("aria-label", "Save rename");
    saveBtn.innerHTML = '<i class="fa-solid fa-check"></i>';

    const cancelBtn = document.createElement("button");
    cancelBtn.type = "button";
    cancelBtn.className = "entry-rename-btn cancel";
    cancelBtn.title = "Cancel";
    cancelBtn.setAttribute("aria-label", "Cancel rename");
    cancelBtn.innerHTML = '<i class="fa-solid fa-xmark"></i>';

    actions.appendChild(saveBtn);
    actions.appendChild(cancelBtn);

    const editor = document.createElement("div");
    editor.className = "entry-rename-inline";
    editor.appendChild(input);
    editor.appendChild(actions);

    const originalText = nameNode.textContent;
    li.classList.add('renaming');
    nameNode.textContent = "";
    nameNode.appendChild(editor);

    const stopClick = (ev) => ev.stopPropagation();
    editor.addEventListener('mousedown', stopClick);
    editor.addEventListener('click', stopClick);

    const cleanup = () => {
      if (activeInlineRename && activeInlineRename.li === li) activeInlineRename = null;
      li.classList.remove('renaming');
      nameNode.textContent = originalText;
    };

    const cancelRename = () => {
      cleanup();
    };

    const setEditingState = (isBusy) => {
      input.disabled = isBusy;
      saveBtn.disabled = isBusy;
      cancelBtn.disabled = isBusy;
    };

    const saveRename = async () => {
      if (submitting) return;
      const newName = input.value.trim();
      if (!newName) { input.focus(); return; }
      if (newName === currentName) { cleanup(); return; }

      submitting = true;
      setEditingState(true);
      try {
        await apiRename(oldPath, newName);
        flashFilesBar();
        await loadExplorer(currentPath);
      } catch (err) {
        submitting = false;
        setEditingState(false);
        flashFilesBar();
        input.focus();
        input.select();
      }
    };

    saveBtn.addEventListener('click', (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      saveRename();
    });
    cancelBtn.addEventListener('click', (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      cancelRename();
    });
    input.addEventListener('keydown', (ev) => {
      ev.stopPropagation();
      if (ev.key === 'Enter') {
        ev.preventDefault();
        saveRename();
      } else if (ev.key === 'Escape') {
        ev.preventDefault();
        cancelRename();
      }
    });

    activeInlineRename = { li, cancel: cancelRename };
    input.focus();
    input.select();
  }
  async function loadExplorer(path = "") {
    try {
      if (!hasPerm('files_read')) { entriesEl.innerHTML = `<li style="color:var(--muted)">Unauthorized</li>`; return; }
      const data = await apiExplore(path);
      currentPath = data.path || "";
      breadcrumbs.innerHTML = `<a href="#" data-path="">Root</a>${currentPath ? " / " + escapeHtml(currentPath) : ""}`;
      cancelActiveInlineRename();
      clearSelection();
      entriesEl.innerHTML = "";
      data.entries.forEach((rawEntry) => {
        const e = { name: rawEntry.name, isDir: !!(rawEntry.isDir || rawEntry.type === "dir"), size: rawEntry.size };
        const li = document.createElement("li"); li.className = "entry selectable";
        const relPath = currentPath ? currentPath + "/" + e.name : e.name;
        li.dataset.relPath = relPath;
        li.dataset.isDir = e.isDir ? "true" : "false";
        const left = document.createElement("div"); left.className = "entry-left";
        const icon = document.createElement("div"); icon.className = `entry-icon ${e.isDir ? 'folder' : 'file'}`; icon.innerHTML = e.isDir ? '<i class="fa-solid fa-folder"></i>' : '<i class="fa-solid fa-file"></i>';
        const nameWrap = document.createElement("div"); nameWrap.className = "entry-text-wrap";
        const nm = document.createElement("div"); nm.className = "entry-name"; nm.textContent = e.name;
        const meta = document.createElement("div"); meta.className = "entry-meta";
        if (e.isDir) {
          meta.textContent = "Folder";
        } else {
          meta.textContent = typeof e.size === "number" ? formatFileSize(e.size) : "File";
        }
        nameWrap.appendChild(nm); nameWrap.appendChild(meta);
        left.appendChild(icon); left.appendChild(nameWrap);
        let longPressTimeout = null;
        let longPressTriggered = false;
        left.addEventListener('mousedown', (ev) => {
          if (ev.button !== 0) return;
          longPressTriggered = false;
          if (longPressTimeout) clearTimeout(longPressTimeout);
          longPressTimeout = setTimeout(() => {
            longPressTriggered = true;
            clearSelection();
            const rel = li.dataset.relPath;
            if (rel) selectedEntries.add(rel);
            updateSelectionUI();
          }, 2000);
        });
        ['mouseup', 'mouseleave', 'mouseout'].forEach(evt => {
          left.addEventListener(evt, () => {
            if (longPressTimeout) {
              clearTimeout(longPressTimeout);
              longPressTimeout = null;
            }
          });
        });
        left.addEventListener('click', (ev) => {
          ev.stopPropagation();
          if (longPressTriggered) { longPressTriggered = false; return; }
          toggleEntrySelection(li);
        });
        const actions = document.createElement("div"); actions.className = "entry-actions";
        const moreBtn = document.createElement("button"); moreBtn.className = "more-btn"; moreBtn.innerHTML = '<i class="fa-solid fa-ellipsis-vertical"></i>';
        const menu = document.createElement("div"); menu.className = "more-menu";
        if (!e.isDir && hasPerm('files_read')) {
          const downloadBtn = document.createElement("button"); downloadBtn.innerHTML = '<i class="fa-solid fa-download" style="margin-right:6px"></i>Download';
          downloadBtn.addEventListener('click', (ev) => {
            ev.stopPropagation(); menu.classList.remove('show');
            const rel = currentPath ? currentPath + "/" + e.name : e.name;
            apiDownloadFile(rel);
          });
          menu.appendChild(downloadBtn);
        }
        if (hasPerm('files_rename')) {
          const renameBtn = document.createElement("button"); renameBtn.innerHTML = '<i class="fa-solid fa-pen-to-square" style="margin-right:6px"></i>Rename';
          renameBtn.addEventListener('click', (ev) => {
            ev.stopPropagation();
            menu.classList.remove('show');
            startInlineRename(li, nm, e.name, relPath);
          });
          menu.appendChild(renameBtn);
        }
        if (hasPerm('files_delete')) {
          const deleteBtn = document.createElement("button"); deleteBtn.innerHTML = '<i class="fa-solid fa-trash" style="margin-right:6px"></i>Delete';
          deleteBtn.addEventListener('click', async (ev) => {
            ev.stopPropagation();
            if (!confirm(`Delete ${e.isDir ? "folder" : "file"} "${e.name}"?`)) { menu.classList.remove('show'); return; }
            try {
              const rel = currentPath ? currentPath + "/" + e.name : e.name;
              await apiDelete(rel, e.isDir);
              setTimeout(() => loadExplorer(currentPath), 200);
            } catch (err) { flashFilesBar(); }
            menu.classList.remove('show');
          });
          menu.appendChild(deleteBtn);
        }
        const isArchive = !e.isDir && /\.(zip|tar|tgz|tar\.gz|tar\.bz2|bz2|rar|7z)$/i.test(e.name);
        if (isArchive && hasPerm('files_create')) {
          const unarchiveBtn = document.createElement("button"); unarchiveBtn.textContent = "Unarchive";
          unarchiveBtn.addEventListener('click', async (ev) => {
            ev.stopPropagation(); menu.classList.remove('show');
            const targetRel = currentPath ? currentPath + "/" + e.name : e.name;
            if (!confirm(`Unarchive "${e.name}" into current folder?`)) return;
            showProgressWheel(6, "Unarchiving...");
            try {
              const j = await apiExtract(targetRel);
              setProgress(100, j.msg || 'Unarchive complete'); await new Promise(r => setTimeout(r, 250));
              loadExplorer(currentPath); flashFilesBar();
            } catch (err) { flashFilesBar(); }
            finally { hideProgressWheel(); }
          });
          menu.appendChild(unarchiveBtn);
        }
        actions.appendChild(moreBtn); actions.appendChild(menu);
        moreBtn.addEventListener('click', (ev) => {
          ev.stopPropagation();
          document.querySelectorAll('.more-menu.show').forEach(m => { if (m !== menu) m.classList.remove('show'); });
          menu.classList.toggle('show');
        });
        li.addEventListener('click', async () => {
          if (li.classList.contains('renaming')) return;
          if (e.isDir) {
            loadExplorer(currentPath ? currentPath + "/" + e.name : e.name);
          } else if (hasPerm('files_read')) {
            const rel = relPath;
            if (typeof e.size === 'number' && e.size > FILE_SIZE_EDITOR_LIMIT) {
              if (confirm(`This file is ${formatFileSize(e.size)} which exceeds the 5 MB editor limit.\n\nDownload instead?`)) {
                apiDownloadFile(rel);
              }
              return;
            }
            if (REMOTE_MODE) {
              try {
                const content = await apiReadFile(rel);
                if (content !== null) openEditorModal(rel, content);
              } catch (err) { flashFilesBar(); }
            } else {
              socket.emit("readFile", { bot, path: rel });
            }
          }
        });
        const arrow = document.createElement("div"); arrow.className = "entry-arrow"; arrow.innerHTML = '<i class="fa-solid fa-chevron-right"></i>';
        const right = document.createElement("div"); right.className = "entry-right"; right.appendChild(actions); right.appendChild(arrow);
        li.appendChild(left); li.appendChild(right); entriesEl.appendChild(li);
      });
      updateSelectionUI();
      const scripts = data.entries.filter(e => !(e.isDir || e.type === "dir")).map(e => e.name);
      if (taskStartupSelect) {
        taskStartupSelect.innerHTML = `<option value="">(none)</option>` + scripts.map(s => `<option value="${escapeHtml(s)}">${escapeHtml(s)}</option>`).join("");
      }
    } catch (err) {
      console.error("[FileManager] Load failed:", err);
      entriesEl.innerHTML = `<li style="color:var(--muted); padding:10px;">
        <i class="fa-solid fa-triangle-exclamation"></i>
        ${hasPerm('files_read') ? ' Failed to load: ' + (err.message || String(err)) : ' Unauthorized'}
      </li>`;
    }
  }
  if (selectAllToggle) {
    selectAllToggle.addEventListener('change', (ev) => {
      if (ev.target.checked) selectAllEntries(); else clearSelection();
    });
  }
  if (bulkDeleteBtn) {
    bulkDeleteBtn.addEventListener('click', async () => {
      if (selectedEntries.size === 0) return;
      if (!confirm(`Delete ${selectedEntries.size} selected item(s)?`)) return;
      const toDelete = Array.from(entriesEl.querySelectorAll('.entry')).filter(li => selectedEntries.has(li.dataset.relPath));
      try {
        showProgressWheel(4, "Deleting...");
        for (const li of toDelete) {
          const rel = li.dataset.relPath;
          const isDir = li.dataset.isDir === 'true';
          if (!rel) continue;
          await apiDelete(rel, isDir);
        }
        setProgress(100, "Delete complete");
        setTimeout(() => hideProgressWheel(), 300);
        loadExplorer(currentPath);
      } catch (err) {
        alert('Delete failed: ' + (err && err.message ? err.message : err));
      } finally {
        clearSelection();
      }
    });
  }
  if (bulkArchiveBtn) {
    bulkArchiveBtn.addEventListener('click', async () => {
      if (selectedEntries.size === 0) return;
      if (!hasPerm('files_archive')) return alert('Unauthorized: missing files_archive permission');
      const paths = Array.from(selectedEntries);
      try {
        showProgressWheel(10, "Creating archive...");
        const result = await apiArchive(paths, currentPath);
        setProgress(100, `Archive created: ${result.name}`);
        setTimeout(() => hideProgressWheel(), 300);
        loadExplorer(currentPath);
        flashFilesBar();
      } catch (err) {
        hideProgressWheel();
        alert('Archive failed: ' + (err && err.message ? err.message : err));
      } finally {
        clearSelection();
      }
    });
  }
  let currentProgress = 0, simInterval = null, isActiveOperation = false, operationStart = null;
  function showProgressWheel(initial = 0, msg = "Processing...") { isActiveOperation = true; operationStart = Date.now(); progressWheel.classList.remove('hidden'); setProgress(initial, msg); }
  function hideProgressWheel() { isActiveOperation = false; setTimeout(() => { progressWheel.classList.add('hidden'); setProgress(0); closeProgressPopup(); operationStart = null; popupTime.textContent = "—"; }, 300); }
  function setProgress(percent, msg = "Processing...") {
    if (typeof percent !== 'number') percent = 0; percent = Math.max(0, Math.min(100, Math.round(percent)));
    currentProgress = percent; progressFill.style.height = percent + "%"; popupPercent.textContent = percent + "%"; popupBar.style.width = percent + "%";
    if (msg) popupMsg.textContent = msg;
    if (operationStart) { const elapsed = Math.floor((Date.now() - operationStart) / 1000); popupTime.textContent = `${elapsed}s elapsed`; } else popupTime.textContent = "—";
    if (percent >= 100 && isActiveOperation) { setTimeout(() => hideProgressWheel(), 450); }
  }

  function openProgressPopup() { progressPopup.style.display = "block"; progressPopup.setAttribute('aria-hidden', 'false'); popupPercent.textContent = currentProgress + "%"; popupBar.style.width = currentProgress + "%"; if (operationStart) { const elapsed = Math.floor((Date.now() - operationStart) / 1000); popupTime.textContent = `${elapsed}s elapsed`; } else popupTime.textContent = "—"; }
  function closeProgressPopup() {
    if (!progressPopup) return;
    animateClose(progressPopup);
  }
  if (progressWheel && progressPopup && popupClose) {
    progressWheel.addEventListener('click', () => {
      if (progressPopup.style.display === 'block') closeProgressPopup();
      else openProgressPopup();
    });
    popupClose.addEventListener('click', () => closeProgressPopup());
  }
  function enableDropHandlers() {
    if (!hasPerm('files_upload')) return;
    const modalInner = document.querySelector('#filesModal .modal'); if (!modalInner) return;
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(evt => { modalInner.addEventListener(evt, (e) => e.preventDefault(), { passive: false }); });
    modalInner.addEventListener('dragenter', onDragEnter);
    modalInner.addEventListener('dragover', onDragOver);
    modalInner.addEventListener('dragleave', onDragLeave);
    modalInner.addEventListener('drop', onDrop);
  }
  function disableDropHandlers() {
    const modalInner = document.querySelector('#filesModal .modal'); if (!modalInner) return;
    modalInner.removeEventListener('dragenter', onDragEnter);
    modalInner.removeEventListener('dragover', onDragOver);
    modalInner.removeEventListener('dragleave', onDragLeave);
    modalInner.removeEventListener('drop', onDrop);
  }
  let dragCounter = 0;
  function onDragEnter(e) { e.preventDefault(); dragCounter++; dropOverlay.classList.add('visible'); dropTitle.textContent = "Drop to upload"; dropSubtitle.textContent = "Release to upload to current folder"; }
  function onDragOver(e) { e.preventDefault(); e.dataTransfer.dropEffect = 'copy'; if (!dropOverlay.classList.contains('visible')) dropOverlay.classList.add('visible'); }
  function onDragLeave(e) { e.preventDefault(); dragCounter = Math.max(0, dragCounter - 1); if (dragCounter === 0) dropOverlay.classList.remove('visible'); }
  async function onDrop(e) { e.preventDefault(); dragCounter = 0; dropOverlay.classList.remove('visible'); const items = e.dataTransfer.files; if (!items || items.length === 0) return; if (!hasPerm('files_upload')) return; const files = Array.from(items); await uploadFilesSequential(files); }
  async function uploadFilesSequential(files) {
    for (let i = 0; i < files.length; i++) {
      const file = files[i], idx = i + 1, total = files.length, msgBase = `Uploading ${idx} of ${total} — ${file.name}`;
      try {
        await apiUploadWithProgress(file, currentPath, (percent, state) => setProgress(percent, state || msgBase), msgBase);
        await new Promise(r => setTimeout(r, 150)); loadExplorer(currentPath);
      } catch (err) { flashFilesBar(); }
    }
    setTimeout(() => { hideProgressWheel(); }, 400);
    flashFilesBar();
  }
  function normalizeInput(raw) {
    if (typeof raw !== 'string') return '';
    return raw.replace(/\r\n?/g, '\n');
  }
  function detectMonacoLanguage(path) {
    const lower = (path || "").toLowerCase();
    if (lower.endsWith(".js")) return "javascript";
    if (lower.endsWith(".ts")) return "typescript";
    if (lower.endsWith(".json")) return "json";
    if (lower.endsWith(".jsonc")) return "jsonc";
    if (lower.endsWith(".yml") || lower.endsWith(".yaml")) return "yaml";
    if (lower.endsWith(".html") || lower.endsWith(".htm")) return "html";
    if (lower.endsWith(".css")) return "css";
    if (lower.endsWith(".java")) return "java";
    if (lower.endsWith(".php")) return "php";
    if (lower.endsWith(".py")) return "python";
    if (lower.endsWith(".rb")) return "ruby";
    if (lower.endsWith(".go")) return "go";
    if (lower.endsWith(".rs")) return "rust";
    if (lower.endsWith(".cs")) return "csharp";
    if (lower.endsWith(".c")) return "c";
    if (lower.endsWith(".cpp") || lower.endsWith(".cc") || lower.endsWith(".cxx")) return "cpp";
    if (lower.endsWith(".xml") || lower.endsWith(".xsd") || lower.endsWith(".svg") || lower.endsWith(".plist")) return "xml";
    if (lower.endsWith(".toml")) return "toml";
    if (lower.endsWith(".sql")) return "sql";
    if (lower.endsWith(".md") || lower.endsWith(".markdown")) return "markdown";
    if (lower.endsWith(".properties") || lower.endsWith(".ini") || lower.endsWith(".cfg") || lower.endsWith(".conf") || lower.endsWith(".env")) return "ini";
    if (lower.endsWith(".sh") || lower.endsWith(".bash") || lower.endsWith(".zsh") || lower.endsWith(".ksh")) return "shell";
    if (lower.endsWith(".ps1")) return "powershell";
    if (lower.endsWith("dockerfile")) return "dockerfile";
    return "plaintext";
  }
  function detectAceLanguage(path) {
    const lower = (path || "").toLowerCase();
    if (lower.endsWith(".js") || lower.endsWith(".mjs") || lower.endsWith(".cjs")) return "ace/mode/javascript";
    if (lower.endsWith(".ts")) return "ace/mode/typescript";
    if (lower.endsWith(".json") || lower.endsWith(".jsonc")) return "ace/mode/json";
    if (lower.endsWith(".yml") || lower.endsWith(".yaml")) return "ace/mode/yaml";
    if (lower.endsWith(".html") || lower.endsWith(".htm")) return "ace/mode/html";
    if (lower.endsWith(".css")) return "ace/mode/css";
    if (lower.endsWith(".java")) return "ace/mode/java";
    if (lower.endsWith(".php")) return "ace/mode/php";
    if (lower.endsWith(".py")) return "ace/mode/python";
    if (lower.endsWith(".rb")) return "ace/mode/ruby";
    if (lower.endsWith(".go")) return "ace/mode/golang";
    if (lower.endsWith(".rs")) return "ace/mode/rust";
    if (lower.endsWith(".cs")) return "ace/mode/csharp";
    if (lower.endsWith(".c") || lower.endsWith(".cpp") || lower.endsWith(".cc") || lower.endsWith(".cxx") || lower.endsWith(".h")) return "ace/mode/c_cpp";
    if (lower.endsWith(".xml") || lower.endsWith(".xsd") || lower.endsWith(".svg") || lower.endsWith(".plist")) return "ace/mode/xml";
    if (lower.endsWith(".toml")) return "ace/mode/toml";
    if (lower.endsWith(".sql")) return "ace/mode/sql";
    if (lower.endsWith(".md") || lower.endsWith(".markdown")) return "ace/mode/markdown";
    if (lower.endsWith(".properties") || lower.endsWith(".ini") || lower.endsWith(".cfg") || lower.endsWith(".conf") || lower.endsWith(".env")) return "ace/mode/ini";
    if (lower.endsWith(".sh") || lower.endsWith(".bash") || lower.endsWith(".zsh") || lower.endsWith(".ksh")) return "ace/mode/sh";
    if (lower.endsWith(".ps1")) return "ace/mode/powershell";
    if (lower.endsWith("dockerfile")) return "ace/mode/dockerfile";
    return "ace/mode/text";
  }
  async function shouldUseAceFallback() {
    if (useAceFallback !== null) return useAceFallback;
    if (typeof ace === "undefined") { useAceFallback = false; return false; }
    const isMobile = /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent)
      || (navigator.maxTouchPoints > 1 && window.innerWidth < 1100);
    if (isMobile) {
      console.log("[Editor] Mobile/tablet detected → Ace fallback");
      useAceFallback = true; return true;
    }
    const cores = navigator.hardwareConcurrency || 2;
    const memory = navigator.deviceMemory || 4;
    if (cores < 2 || memory < 2) {
      console.log("[Editor] Low-spec device detected (cores:", cores, "mem:", memory, "GB) → Ace fallback");
      useAceFallback = true; return true;
    }
    try {
      const conn = navigator.connection || navigator.mozConnection || navigator.webkitConnection;
      if (conn && typeof conn.downlink === "number" && conn.downlink < 2) {
        console.log("[Editor] Slow connection detected (downlink:", conn.downlink, "Mbps) → Ace fallback");
        useAceFallback = true; return true;
      }
      const t0 = performance.now();
      const testUrl = "/style.css?_cb=" + Date.now();
      const resp = await fetch(testUrl, { cache: "no-store" });
      const blob = await resp.blob();
      const elapsed = performance.now() - t0;
      const sizeBits = blob.size * 8;
      const speedMbps = (sizeBits / elapsed) * 1000 / 1_000_000;
      if (speedMbps < 2) {
        console.log("[Editor] Speed test:", speedMbps.toFixed(2), "Mbps → Ace fallback");
        useAceFallback = true; return true;
      }
    } catch (e) {
      console.warn("[Editor] Speed test failed, assuming capable:", e);
    }
    useAceFallback = false;
    return false;
  }
  function loadMonaco() {
    if (window.monaco) {
      return Promise.resolve(window.monaco);
    }
    if (monacoLoaderPromise) return monacoLoaderPromise;
    monacoLoaderPromise = new Promise((resolve, reject) => {
      if (typeof require === "undefined") {
        reject(new Error("Monaco loader (require) nu este disponibil"));
        return;
      }
      require.config({
        paths: {
          vs: "https://cdn.jsdelivr.net/npm/monaco-editor@0.55.1/min/vs"
        },
        waitSeconds: 10
      });
      require(["vs/editor/editor.main"], function (m) {
        try {
          m.editor.setTheme("vs-dark");
        } catch (e) { }
        resolve(m);
      }, function (err) {
        console.error("Failed to load Monaco", err);
        reject(err);
      });
    });
    return monacoLoaderPromise;
  }
  function patchMonacoFindWidgetLayout(root) {
    const host = root || monacoContainer;
    if (!host) return;
    host.querySelectorAll(".find-widget .monaco-inputbox > .wrapper > .input").forEach((el) => {
      el.style.margin = "0";
      el.style.padding = "2px 4px";
      el.style.border = "none";
      el.style.borderRadius = "0";
      el.style.background = "transparent";
      el.style.minHeight = "0";
      el.style.height = "100%";
      el.style.lineHeight = "inherit";
      el.style.fontFamily = "inherit";
      el.style.fontSize = "inherit";
      el.style.boxSizing = "border-box";
      el.style.resize = "none";
      el.style.backdropFilter = "none";
      el.style.webkitBackdropFilter = "none";
      el.style.boxShadow = "none";
    });
    host.querySelectorAll(".find-widget .monaco-inputbox > .wrapper > .mirror").forEach((el) => {
      el.style.padding = "2px 4px";
      el.style.boxSizing = "border-box";
    });
  }
  function ensureMonacoFindWidgetPatch() {
    if (!monacoContainer) return;
    patchMonacoFindWidgetLayout(monacoContainer);
    if (monacoFindWidgetObserver) return;
    monacoFindWidgetObserver = new MutationObserver(() => {
      patchMonacoFindWidgetLayout(monacoContainer);
    });
    monacoFindWidgetObserver.observe(monacoContainer, {
      childList: true,
      subtree: true,
      attributes: true,
      attributeFilter: ["class", "style"]
    });
  }
  async function openAceEditor(content, filePath) {
    if (monacoContainer) monacoContainer.style.display = "none";
    if (aceContainer) aceContainer.style.display = "block";
    codeShell.style.display = "flex";
    textArea.style.display = "none";
    const legacyPane = document.querySelector("#codeShell .code-pane");
    if (legacyPane) legacyPane.style.display = "none";
    if (lineGutter) lineGutter.style.display = "none";
    if (typeof ace === "undefined") {
      console.error("[Editor] Ace not available — using raw textarea");
      codeShell.style.display = "none";
      if (aceContainer) aceContainer.style.display = "none";
      textArea.style.display = "block";
      textArea.value = content;
      textArea.dataset.original = content;
      textArea.focus();
      return;
    }
    if (!aceEditor) {
      ace.config.set("basePath", "https://cdn.jsdelivr.net/npm/ace-builds@1.36.5/src-min-noconflict");
      aceEditor = ace.edit(aceContainer, {
        theme: "ace/theme/one_dark",
        fontSize: 14,
        fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace",
        showPrintMargin: false,
        tabSize: 2,
        useSoftTabs: true,
        wrap: false,
        scrollPastEnd: false,
        animatedScroll: true,
        enableBasicAutocompletion: false,
        enableLiveAutocompletion: false,
        highlightActiveLine: true,
        highlightGutterLine: true,
        showLineNumbers: true,
        showGutter: true,
        displayIndentGuides: true,
        fixedWidthGutter: false
      });
      aceEditor.resize(true);
    }
    const mode = detectAceLanguage(filePath);
    aceEditor.session.setMode(mode);
    aceEditor.setValue(content, -1);
    aceEditor.clearSelection();
    aceEditor.moveCursorTo(0, 0);
    requestAnimationFrame(() => {
      if (aceEditor) aceEditor.resize(true);
    });
    aceEditor.focus();
  }
  async function openEditorModal(path, content) {
    currentEditorPath = path || "";
    editorModal.classList.add("show");
    editorModal.style.display = "flex";
    editorPath.textContent = currentEditorPath;
    const isRich = true;
    const normalized = normalizeInput(content || "");
    editorOriginalContent = normalized;
    if (!hasPerm("files_create")) {
      editorSave.classList.add("disabled");
    } else {
      editorSave.classList.remove("disabled");
    }
    if (isRich) {
      codeShell.style.display = "flex";
      if (lineGutter) lineGutter.style.display = "none";
      const legacyPane = document.querySelector("#codeShell .code-pane");
      if (legacyPane) legacyPane.style.display = "none";
      textArea.style.display = "none";

      const fallback = await shouldUseAceFallback();

      if (!fallback) {
        if (monacoContainer) monacoContainer.style.display = "block";
        if (aceContainer) aceContainer.style.display = "none";
        try {
          const monaco = await loadMonaco();
          if (!monacoContainer) {
            console.error("monacoContainer missing");
            return;
          }
          const lang = detectMonacoLanguage(currentEditorPath);
          if (monacoEditor) {
            const oldModel = monacoEditor.getModel();
            const newModel = monaco.editor.createModel(normalized, lang);
            monacoEditor.setModel(newModel);
            if (oldModel) oldModel.dispose();
            monacoEditor.setScrollTop(0);
            monacoEditor.setScrollLeft(0);
          } else {
            monacoEditor = monaco.editor.create(monacoContainer, {
              value: normalized,
              language: lang,
              theme: "vs-dark",
              automaticLayout: true,
              fontSize: 14,
              minimap: { enabled: false },
              quickSuggestions: false,
              suggestOnTriggerCharacters: false,
              wordBasedSuggestions: "off",
              parameterHints: { enabled: false },
              snippetSuggestions: "none",
              scrollBeyondLastLine: false,
              scrollBeyondLastColumn: 0,
              tabSize: 2,
              insertSpaces: true,
              detectIndentation: false,
              autoIndent: "full",
              formatOnType: true,
              formatOnPaste: true
            });
          }
          setupMonacoIdleSuggestions(monacoEditor);
          ensureMonacoFindWidgetPatch();
          monacoEditor.focus();
          monacoEditor.__originalValue = normalized;
        } catch (err) {
          console.error("Failed to init Monaco, trying Ace fallback:", err);
          useAceFallback = true;
          monacoLoaderPromise = null;
          if (monacoContainer) monacoContainer.style.display = "none";
          await openAceEditor(normalized, currentEditorPath);
        }
      } else {
        await openAceEditor(normalized, currentEditorPath);
      }
    } else {
      codeShell.style.display = "none";
      if (monacoContainer) monacoContainer.style.display = "none";
      textArea.style.display = "block";
      textArea.value = normalized;
      textArea.dataset.original = normalized;
      textArea.focus();
    }
  }
  function closeEditorModal() {
    if (!editorModal) return;
    animateClose(editorModal, () => {
      editorPath.textContent = '';
      if (monacoIdleSuggestTimer) {
        clearTimeout(monacoIdleSuggestTimer);
        monacoIdleSuggestTimer = null;
      }
      if (monacoEditor) {
        const model = monacoEditor.getModel && monacoEditor.getModel();
        if (model) model.dispose();
        monacoEditor.setModel(null);
      }
      if (aceEditor) {
        aceEditor.setValue('', -1);
      }
      if (aceContainer) aceContainer.style.display = 'none';
      if (codeOverlay) codeOverlay.innerHTML = '';
      if (codeInput) {
        codeInput.value = '';
        codeInput.dataset.original = '';
      }
      if (codeShell) codeShell.style.display = 'none';
      if (textArea) {
        textArea.value = '';
        textArea.dataset.original = '';
        textArea.style.display = 'none';
      }
      if (lineGutter) lineGutter.innerHTML = '';
      editorOriginalContent = "";
      currentEditorPath = '';
    });
  }
  async function performSave() {
    if (!hasPerm('files_create')) return alert('Unauthorized');
    const path = document.getElementById('editorPath').textContent;
    if (!path) return alert('No file open');
    let contentToSave = "";
    if (monacoEditor && monacoEditor.getModel && monacoEditor.getModel()) {
      contentToSave = monacoEditor.getValue();
    } else if (aceEditor && aceContainer && aceContainer.style.display !== 'none') {
      contentToSave = aceEditor.getValue();
    } else if (textArea && textArea.style.display !== 'none') {
      contentToSave = textArea.value;
    } else if (codeInput) {
      contentToSave = codeInput.value;
    }
    try {
      await apiWriteFile(path, contentToSave);
      editorOriginalContent = contentToSave;
      setTimeout(() => {
        loadExplorer(currentPath);
        flashEditorBar();
        closeEditorModal();
      }, 150);
    } catch (err) {
      flashEditorBar();
    }
  }
  if (editorSave) {
    editorSave.onclick = (e) => {
      e.preventDefault();
      performSave();
    };
  }
  editorDiscard.addEventListener('click', () => {
    if (monacoEditor && monacoEditor.getModel && monacoEditor.getModel()) {
      monacoEditor.setValue(editorOriginalContent || "");
      monacoEditor.setScrollTop(0);
      monacoEditor.setScrollLeft(0);
      return;
    }
    if (aceEditor && aceContainer && aceContainer.style.display !== 'none') {
      aceEditor.setValue(editorOriginalContent || "", -1);
      aceEditor.clearSelection();
      aceEditor.moveCursorTo(0, 0);
      return;
    }
    if (textArea && textArea.style.display !== 'none') {
      textArea.value = editorOriginalContent || textArea.dataset.original || '';
      return;
    }
    if (codeInput) {
      const original = editorOriginalContent || codeInput.dataset.original || '';
      codeInput.value = original;
      renderHighlightedEditor(original);
    }
  });

  if (editorModalClose) {
    editorModalClose.addEventListener("click", closeEditorModal);
  }
  if (editorModal) {
    editorModal.addEventListener("click", (e) => {
      if (e.target === editorModal) closeEditorModal();
    });
  }

  function appendConsoleNotice(text, color = '#00e5ff') {
    const tone = String(color || '').toLowerCase() === '#ff9b9b' ? '\x1b[91m' : '\x1b[96m';
    appendToConsole(`${tone}${String(text || '')}\x1b[0m\n`);
  }

  function run() {
    if (!hasPerm('server_start')) return alert('Unauthorized');
    if (!isPowerActionAllowed('run')) return;
    resetConsoleBuffer();
    resetServerConsoleHistorySession();
    appendConsoleNotice('[ADaemon] Starting server...');
    setPowerState(POWER_STATE.STARTING);
    attachLogStreamIfRemote({ tail: 0, forceReconnect: true });
    const onRunError = (err) => {
      const message = err && err.message ? err.message : String(err || 'Run failed.');
      appendConsoleNotice(`[ADaemon] ${message}`, '#ff9b9b');
      alert('Run failed: ' + message);
      refreshPowerState({ force: true }).catch(() => { });
    };
    const startRequest = apiAction({ bot, cmd: 'run' });
    appendConsoleNotice('[ADaemon] Start request accepted.');
    window.setTimeout(() => {
      refreshPowerState({ force: true }).catch(() => { });
    }, 250);
    startRequest
      .then(() => {
        return refreshPowerState({ force: true }).catch(() => { });
      })
      .catch(onRunError);
  }
  function stop() {
    if (!hasPerm('server_stop')) return alert('Unauthorized');
    if (!isPowerActionAllowed('stop')) return;
    setPowerState(POWER_STATE.STOPPING);
    appendConsoleNotice('[ADaemon] Stopping server...');
    apiAction({ bot, cmd: 'stop' })
      .then(() => {
        appendConsoleNotice('[ADaemon] Stop request accepted.');
        return refreshPowerState({ force: true }).catch(() => { });
      })
      .catch(err => {
        const message = err && err.message ? err.message : String(err || 'Stop failed.');
        appendConsoleNotice(`[ADaemon] ${message}`, '#ff9b9b');
        alert('Stop failed: ' + message);
        refreshPowerState({ force: true }).catch(() => { });
      });
  }
  function killServer() {
    if (!hasPerm('server_stop')) return alert('Unauthorized');
    if (!isPowerActionAllowed('kill')) return;
    setPowerState(POWER_STATE.STOPPING);
    appendConsoleNotice('[ADaemon] Sending kill signal...', '#ff9b9b');
    apiAction({ bot, cmd: 'kill' })
      .then(() => {
        appendConsoleNotice('[ADaemon] Kill request accepted.', '#ff9b9b');
        return refreshPowerState({ force: true }).catch(() => { });
      })
      .catch(err => {
        const message = err && err.message ? err.message : String(err || 'Kill failed.');
        appendConsoleNotice(`[ADaemon] ${message}`, '#ff9b9b');
        alert('Kill failed: ' + message);
        refreshPowerState({ force: true }).catch(() => { });
      });
  }
  fileUploadModal.addEventListener('change', () => {
    if (!hasPerm('files_upload')) return alert('Unauthorized');
    const f = fileUploadModal.files[0]; if (!f) return;
    showProgressWheel(0, `Uploading 1 of 1 — ${f.name}`);
    apiUploadWithProgress(f, currentPath, (percent, msg) => setProgress(percent, msg), `Uploading 1 of 1 — ${f.name}`)
      .then(() => { loadExplorer(currentPath); setTimeout(() => { hideProgressWheel(); flashFilesBar(); }, 350); })
      .catch((err) => { hideProgressWheel(); flashFilesBar(); })
      .finally(() => { fileUploadModal.value = ''; });
  });
  async function createNew() {
    if (!hasPerm('files_create')) return alert('Unauthorized');
    const type = prompt("Type 'file' to create a file or 'folder' to create a folder:"); if (!type || (type !== 'file' && type !== 'folder')) return alert('Invalid type.');
    const name = prompt(`Enter ${type} name:`); if (!name) return;
    showProgressWheel(4, type === 'file' ? "Creating file..." : "Creating folder..."); startSimulatedProgress(4, 88, 420);
    try {
      await apiCreate(type, name, currentPath);
      setProgress(100, (type === 'file' ? "File created" : "Folder created"));
      loadExplorer(currentPath);
      setTimeout(() => { hideProgressWheel(); flashFilesBar(); }, 350);
    } catch (err) {
      hideProgressWheel(); flashFilesBar();
    }
  }
  async function sendCommand() {
    if (!hasPerm('console_write')) return alert('Unauthorized');
    const cmd = cmdInput.value.trim(); if (cmd === '') return;
    const now = Date.now();
    if (commandSendInFlight && cmd === lastSubmittedCommand) return;
    if (cmd === lastSubmittedCommand && (now - lastSubmittedAt) < 350) return;
    commandSendInFlight = true;
    lastSubmittedCommand = cmd;
    lastSubmittedAt = now;
    cmdInput.value = '';
    rememberConsoleCommandForInput(cmd);
    appendCommandEcho(cmd, now);
    try {
      await apiCommand(cmd, now);
    } catch (err) {
      alert('Command failed: ' + (err.message || err));
    } finally {
      commandSendInFlight = false;
    }
  }
  cmdInput.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowUp') {
      e.preventDefault();
      recallConsoleCommand(-1);
      return;
    }
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      recallConsoleCommand(1);
      return;
    }
    if (e.key === 'Enter') {
      e.preventDefault();
      sendCommand();
    }
  });
  cmdInput.addEventListener('input', () => {
    consoleCommandHistoryIndex = -1;
  });
  function clearConsole() {
    if (currentUserPreferences.clear_console_no_ask) {
      resetConsoleBuffer();
      return;
    }
    if (typeof openClearConsoleModal === 'function') {
      openClearConsoleModal();
    } else {
      const modal = document.getElementById('clearConsoleModal');
      if (modal) {
        modal.style.display = 'flex';
        const cb = document.getElementById('clearConsoleNoAsk');
        if (cb) cb.checked = false;
      }
    }
  }
  function openFilesModal() {
    if (!hasPerm('files_read')) return alert('Unauthorized');
    filesModal.classList.add('show'); filesModal.style.display = 'flex'; loadExplorer(currentPath); enableDropHandlers();
  }
  function closeFilesModal() {
    if (!filesModal) return;
    animateClose(filesModal, () => {
      disableDropHandlers();
    });
  }

  function closeTaskModal() {
    if (!taskModal) return;
    animateClose(taskModal);
  }

  function closeFileUploadModal() {
    if (fileInput) fileInput.value = '';
  }

  const fileInput = document.getElementById('fileInput');
  if (fileUploadModal) {
    const closeBtn = document.getElementById('fileUploadModalClose');
    if (closeBtn) closeBtn.addEventListener('click', closeFileUploadModal);
    fileUploadModal.addEventListener('click', (e) => {
      if (e.target === fileUploadModal) closeFileUploadModal();
    });
  }



  if (filesModalClose) {
    filesModalClose.addEventListener("click", closeFilesModal);
  }
  if (filesModal) {
    filesModal.addEventListener("click", (e) => {
      if (e.target === filesModal) {
        e.stopPropagation();
      }
    });
  }
  if (editorModal) {
    editorModal.addEventListener("click", (e) => {
      if (e.target === editorModal) {
        e.stopPropagation();
      }
    });
  }

  if (taskModalClose) {
    taskModalClose.addEventListener("click", closeTaskModal);
  }
  if (taskModal) {
    taskModal.addEventListener("click", (e) => {
      if (e.target === taskModal) closeTaskModal();
    });
  }

  if (document.getElementById('schedulerModalClose')) {
    document.getElementById('schedulerModalClose').addEventListener('click', closeSchedulerModal);
  }
  function openTaskModal() { if (!hasPerm('server_start')) return alert('Unauthorized'); taskModal.classList.add('show'); taskModal.style.display = 'flex'; loadExplorer(currentPath); }
  if (taskModalClose) taskModalClose.addEventListener('click', closeTaskModal);
  document.addEventListener('keydown', (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 's') {
      if (editorModal && editorModal.classList.contains('show')) {
        e.preventDefault();
        performSave();
        return;
      }
    }
    if (e.key === 'Escape') {
      if (aiHelpModal && aiHelpModal.classList.contains('show')) { closeAiHelpModal(); return; }
      if (editorModal && editorModal.classList.contains('show')) { closeEditorModal(); return; }
      if (filesModal && filesModal.classList.contains('show')) { closeFilesModal(); return; }
      if (taskModal && taskModal.classList.contains('show')) { closeTaskModal(); return; }
      if (infoModal && infoModal.classList.contains('show')) { closeInfoModal(); return; }
      if (storeModal && storeModal.classList.contains('show')) { closeStoreModal(); return; }
      if (mrPluginModal && mrPluginModal.classList.contains('show')) { closeModrinthPluginModal(); return; }
      if (activityModal && activityModal.classList.contains('show')) { closeActivityModal(); return; }
      if (backupsModal && backupsModal.classList.contains('show')) { closeBackupsModal(); return; }
      if (backupCreateModal && backupCreateModal.classList.contains('show')) { closeBackupCreateModal(); return; }
      if (resourceStatsPopup && resourceStatsPopup.classList.contains('show')) { closeResourceStatsPopup(); return; }
      if (progressPopup && progressPopup.style.display === 'block') { closeProgressPopup(); return; }
    }
  });
  document.querySelectorAll('.modal-close').forEach((btn) => {
    btn.addEventListener('click', (e) => {
      e.preventDefault();
      const escEvent = new KeyboardEvent('keydown', {
        key: 'Escape',
        code: 'Escape',
        keyCode: 27,
        which: 27,
        bubbles: true
      });
      document.dispatchEvent(escEvent);
    });
  });
  document.addEventListener('click', () => { document.querySelectorAll('.more-menu.show').forEach(m => m.classList.remove('show')); });
  async function runBootstrapStep(label, fn) {
    try {
      return await fn();
    } catch (err) {
      console.error(`[bot-bootstrap] ${label} failed:`, err);
      return null;
    }
  }
  document.addEventListener('DOMContentLoaded', async () => {
    profileEmailEl = document.getElementById('profileEmail');
    await runBootstrapStep('restore-file-backups', async () => {
      loadFileBackupsFromStorage();
    });
    await runBootstrapStep('fetch-permissions', async () => {
      await fetchPerms();
      applyPermissionsToUI();
    });
    let initialConsoleHistoryCount = 0;
    await runBootstrapStep('hydrate-console-history', async () => {
      initialConsoleHistoryCount = await hydrateInitialConsoleHistory(120);
    });
    startPowerStatePolling();
    await runBootstrapStep('refresh-power-state-initial', async () => {
      await refreshPowerState({ force: true });
    });
    await runBootstrapStep('load-server-info', async () => {
      await loadServerInfo();
    });
    await runBootstrapStep('attach-log-stream', async () => {
      attachLogStreamIfRemote({ tail: initialConsoleHistoryCount > 0 ? 0 : 100 });
    });
    window.setTimeout(() => {
      loadMonaco().catch(() => { });
    }, 1200);
    await runBootstrapStep('refresh-power-state-post-server-info', async () => {
      await refreshPowerState({ force: true });
    });
    if (powerState === POWER_STATE.STOPPED && consoleContentEl && !consoleContentEl.textContent.trim()) {
      appendConsoleNotice('[ADaemon] Server is currently stopped.');
    }
    await runBootstrapStep('load-explorer', async () => {
      await loadExplorer();
    });
    await runBootstrapStep('load-profile', async () => {
      await loadMeIntoProfileEmail();
    });
    runRequestedAssistantBotAction(assistantActionHandlers);
  });
  function escapeHtmlRaw(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }
  function detectLanguageFromPath(path) {
    const lower = (path || '').toLowerCase();
    if (lower.endsWith('.yml') || lower.endsWith('.yaml')) return 'yaml';
    if (lower.endsWith('.json')) return 'json';
    if (lower.endsWith('.html') || lower.endsWith('.htm')) return 'html';
    if (lower.endsWith('.css')) return 'css';
    if (lower.endsWith('.ts')) return 'ts';
    if (lower.endsWith('.js')) return 'js';
    return 'plain';
  }
  function highlightCode(src, path) {
    if (typeof src !== 'string') src = '';
    const lang = detectLanguageFromPath(path);
    const span = (tok, val) => `<span data-token="${tok}">${val}</span>`;
    let out = escapeHtmlRaw(src);
    if (lang === 'yaml') {
      out = out.replace(/(#.*)$/gm, (_, m) => span('com', m));
      out = out.replace(/("([^"\\]|\\.)*"|'([^'\\]|\\.)*')/g, m => span('str', m));
    } else if (lang === 'json') {
      out = out.replace(/"(\\.|[^"\\])*"/g, m => span('str', m));
      out = out.replace(/\b(true|false|null)\b/g, m => span('kw', m));
      out = out.replace(/\b(\d+(\.\d+)?)\b/g, m => span('num', m));
    } else if (lang === 'html') {
      out = out.replace(/()/g, (_, m) => span('com', m));
      out = out.replace(/(&lt;\/?)([a-zA-Z0-9\-]+)([^&gt;]*?)(&gt;)/g, (m, open, tag, attrs, close) => {
        attrs = attrs.replace(/([a-zA-Z0-9\-:]+)=(".*?"|'.*?')/g, (_, k, v) => `${span('attr', k)}=${span('str', v)}`);
        return span('tag', `${open}${tag}${attrs}${close}`);
      });
    } else if (lang === 'css') {
      out = out.replace(/(\/\*[\s\S]*?\*\/)/g, (_, m) => span('com', m));
      out = out.replace(/([a-zA-Z\-]+)(?=\s*:)/g, m => span('prop', m));
      out = out.replace(/("([^"\\]|\\.)*"|'([^'\\]|\\.)*')/g, m => span('str', m));
      out = out.replace(/\b(\d+(\.\d+)?)(px|rem|em|%)?\b/g, m => span('num', m));
    } else {
      out = out.replace(/(\/\*[\s\S]*?\*\/|\/\/[^\n]*)/g, m => span('com', m.replace(/</g, '&lt;')));
      out = out.replace(/("([^"\\]|\\.)*"|'([^'\\]|\\.)*'|`([^`\\]|\\.)*`)/g, m => span('str', m.replace(/</g, '&lt;')));
      out = out.replace(/\b([0-9]+(?:\.[0-9]+)?)\b/g, m => span('num', m));
      out = out.replace(/\b(true|false|null|undefined)\b/g, m => span('bool', m));
      out = out.replace(/\bclass\s+([A-Za-z_$][\w$]*)/g, (_, n) => `class ${span('cls', n)}`);
      out = out.replace(/\bfunction\s+([A-Za-z_$][\w$]*)\s*\(/g, (_, n) => `function ${span('fn', n)}(`);
      out = out.replace(/([A-Za-z_$][\w$]*)\s*=\s*\(.*?\)\s*=>/g, m => m.replace(/([A-Za-z_$][\w$]*)/, p => span('fn', p)));
      out = out.replace(/\b(new|require|module|exports)\b/g, m => span('const', m));
      out = out.replace(/\b([A-Za-z_$][\w$]*)\s*\(/g, m => span('call', m));
      out = out.replace(/:\s*([A-Z][A-Za-z0-9_$]*)/g, (_, n) => `: ${span('type', n)}`);
      const keywords = ['const', 'let', 'var', 'return', 'if', 'else', 'for', 'while', 'switch', 'case', 'break', 'continue', 'class', 'new', 'try', 'catch', 'finally', 'throw', 'await', 'async', 'import', 'from', 'export', 'extends', 'super', 'this', 'typeof', 'instanceof', 'in'];
      const kwRegex = new RegExp('\\b(' + keywords.join('|') + ')\\b', 'g');
      out = out.replace(kwRegex, m => span('kw', m));
    }
    const lines = out.split(/\r\n|\r|\n/);
    return lines.map(line => `<div class="line">${line || '&nbsp;'}</div>`).join('');
  }
  let _monacoIdleSuggestDisposable = null;
  function setupMonacoIdleSuggestions(editor) {
    if (!editor) return;
    if (monacoIdleSuggestTimer) {
      clearTimeout(monacoIdleSuggestTimer);
      monacoIdleSuggestTimer = null;
    }
    if (_monacoIdleSuggestDisposable) {
      _monacoIdleSuggestDisposable.dispose();
      _monacoIdleSuggestDisposable = null;
    }
    _monacoIdleSuggestDisposable = editor.onDidChangeModelContent(() => {
      if (monacoIdleSuggestTimer) {
        clearTimeout(monacoIdleSuggestTimer);
      }
      monacoIdleSuggestTimer = setTimeout(() => {
        if (!editor || (editor.isDisposed && editor.isDisposed()) || (editor.hasTextFocus && !editor.hasTextFocus())) {
          return;
        }
        const action = editor.getAction && editor.getAction("editor.action.triggerSuggest");
        if (action && action.run) {
          action.run().catch(() => { });
        }
      }, 7000);
    });
  }
  function getLineHeightPx() {
    const lh = codeInput ? parseFloat(getComputedStyle(codeInput).lineHeight) : NaN;
    return Number.isFinite(lh) ? lh : 22;
  }
  function getCodePadding() {
    const styleTarget = codeOverlay || codeInput;
    if (!styleTarget) return { top: 16, bottom: 88 };
    const cs = getComputedStyle(styleTarget);
    const top = parseFloat(cs.paddingTop) || 16;
    const bottom = parseFloat(cs.paddingBottom) || 88;
    return { top, bottom };
  }
  function getGutterPadding() {
    if (!lineGutter) return { top: 16, bottom: 88 };
    const cs = getComputedStyle(lineGutter);
    const top = parseFloat(cs.paddingTop) || 16;
    const bottom = parseFloat(cs.paddingBottom) || 88;
    return { top, bottom };
  }
  function measureContentHeight(raw) {
    const normalized = normalizeInput(raw || '');
    const lineCount = Math.max(1, normalized.split('\n').length);
    const lineHeight = getLineHeightPx();
    const { top: paddingTop, bottom: paddingBottom } = getCodePadding();
    return (lineCount * lineHeight) + paddingTop + paddingBottom;
  }
  function renderHighlightedEditor(raw) {
    const normalized = normalizeInput(raw || '');
    if (codeInput && codeInput.value !== normalized) {
      const start = codeInput.selectionStart || 0;
      const end = codeInput.selectionEnd || 0;
      codeInput.value = normalized;
      const clampPos = (pos) => Math.max(0, Math.min(normalized.length, pos));
      codeInput.selectionStart = clampPos(start);
      codeInput.selectionEnd = clampPos(end);
    }
    codeOverlay.innerHTML = highlightCode(normalized, currentEditorPath || '');
    syncLineNumbers(normalized);
    syncCodeSpacer(normalized);
    syncScrollPositions();
  }
  function syncLineNumbers(raw) {
    const count = Math.max(1, (raw.match(/\n/g) || []).length + 1);
    lineGutter.innerHTML = Array.from({ length: count }, (_, i) => `<div>${i + 1}</div>`).join('');
  }
  function syncCodeSpacer(raw) {
    if (!codeSpacer || !codePane) return;
    const measured = measureContentHeight(raw || (codeInput ? codeInput.value : ''));
    const paneHeight = codePane.clientHeight || 0;
    const spacerHeight = Math.max(0, measured - paneHeight);
    codeSpacer.style.height = `${spacerHeight}px`;
    if (codeInput) codeInput.style.minHeight = `${measured}px`;
    if (codeOverlay) codeOverlay.style.minHeight = `${measured}px`;
  }
  function syncScrollPositions() {
    const top = codePane ? codePane.scrollTop : (codeInput ? codeInput.scrollTop : 0);
    if (lineGutter) lineGutter.scrollTop = top;
  }
  if (codeInput) {
    codeInput.addEventListener('input', () => {
      renderHighlightedEditor(codeInput.value);
    });
    codeInput.addEventListener('keydown', (e) => {
      if (e.key === 'Tab') {
        e.preventDefault();
        const start = codeInput.selectionStart;
        const end = codeInput.selectionEnd;
        const val = codeInput.value;
        codeInput.value = `${val.slice(0, start)}  ${val.slice(end)}`;
        codeInput.selectionStart = codeInput.selectionEnd = start + 2;
        renderHighlightedEditor(codeInput.value);
      }
    });
  }
  if (codePane) {
    codePane.addEventListener('scroll', syncScrollPositions);
  }
  if (lineGutter && codePane) {
    lineGutter.addEventListener('wheel', (e) => {
      e.preventDefault();
      codePane.scrollTop += e.deltaY;
      syncScrollPositions();
    }, { passive: false });
    lineGutter.addEventListener('scroll', () => {
      if (Math.abs((codePane.scrollTop || 0) - (lineGutter.scrollTop || 0)) > 1) {
        codePane.scrollTop = lineGutter.scrollTop || 0;
      }
    });
  }
  function highlightJS(src) {
    return highlightCode(src, currentEditorPath || '');
  }
  function startSimulatedProgress(from = 0, to = 90, duration = 400) {
    if (simInterval) { clearInterval(simInterval); simInterval = null; }
    let pct = from; const steps = Math.max(8, Math.floor(duration / 80)); const step = (to - from) / steps;
    simInterval = setInterval(() => { pct = Math.min(99, pct + step); setProgress(Math.round(pct)); if (pct >= to) { clearInterval(simInterval); simInterval = null; } }, Math.max(40, Math.floor(duration / steps)));
  }

  (function () {
    function setDockHidden(hidden) {
      const dock = document.querySelector('.dock');
      if (!dock) return;
      dock.style.display = hidden ? 'none' : '';
    }

    function isAnyModalVisible() {
      const overlays = document.querySelectorAll('.modal-overlay');
      for (const el of overlays) {
        if (el.classList.contains('show')) return true;
        const cs = getComputedStyle(el);
        if (cs.display !== 'none' && cs.visibility !== 'hidden' && cs.opacity !== '0') {
          return true;
        }
      }
      return false;
    }

    function updateDockVisibility() {
      setDockHidden(isAnyModalVisible());
    }

    const modalObserver = new MutationObserver(updateDockVisibility);
    function attachModalObservers() {
      document.querySelectorAll('.modal-overlay').forEach(el => {
        modalObserver.observe(el, {
          attributes: true,
          attributeFilter: ['class', 'style', 'aria-hidden']
        });
      });
    }

    const docObserver = new MutationObserver(muts => {
      let shouldReattach = false;
      for (const m of muts) {
        if (m.type === 'childList' && (m.addedNodes.length || m.removedNodes.length)) {
          shouldReattach = true;
        }
      }
      if (shouldReattach) attachModalObservers();
      updateDockVisibility();
    });

    document.addEventListener('DOMContentLoaded', () => {
      attachModalObservers();
      docObserver.observe(document.body, { childList: true, subtree: true });
      updateDockVisibility();
    });

    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') {
        if (aiHelpModal && aiHelpModal.classList.contains('show')) {
          closeAiHelpModal();
          e.preventDefault();
        }
      }
      setTimeout(updateDockVisibility, 0);
    });

    window.updateDockVisibility = updateDockVisibility;
  })();

  const aiHelpModal = document.getElementById('aiHelpModal');
  const aiHelpSetup = document.getElementById('aiHelpSetup');
  const aiHelpChat = document.getElementById('aiHelpChat');
  const aiProviderGrid = document.getElementById('aiProviderGrid');
  const aiApiKeyForm = document.getElementById('aiApiKeyForm');
  const aiApiKeyInput = document.getElementById('aiApiKeyInput');
  const aiBaseUrlField = document.getElementById('aiBaseUrlField');
  const aiBaseUrlInput = document.getElementById('aiBaseUrlInput');
  const aiKeyHint = document.getElementById('aiKeyHint');
  const aiKeyProviderName = document.getElementById('aiKeyProviderName');
  const aiKeyBack = document.getElementById('aiKeyBack');
  const aiKeyToggle = document.getElementById('aiKeyToggle');
  const aiKeySave = document.getElementById('aiKeySave');
  const aiChatMessages = document.getElementById('aiChatMessages');
  const aiChatInput = document.getElementById('aiChatInput');
  const aiChatSend = document.getElementById('aiChatSend');
  const aiModelSelector = document.getElementById('aiModelSelector');
  const aiModelSelect = document.getElementById('aiModelSelect');
  const aiModelTrigger = document.getElementById('aiModelTrigger');
  const aiModelCurrent = document.getElementById('aiModelCurrent');
  const aiModelOptions = document.getElementById('aiModelOptions');
  const aiChatClear = document.getElementById('aiChatClear');
  const aiHelpModalClose = document.getElementById('aiHelpModalClose');
  const aiHelpSettingsBtn = document.getElementById('aiHelpSettingsBtn');
  const aiFilePermission = document.getElementById('aiFilePermission');
  const aiPermAllow = document.getElementById('aiPermAllow');
  const aiPermDeny = document.getElementById('aiPermDeny');
  const aiPermFileName = document.getElementById('aiPermFileName');
  const aiUndoToast = document.getElementById('aiUndoToast');
  const aiUndoBtn = document.getElementById('aiUndoBtn');
  const aiUndoDismiss = document.getElementById('aiUndoDismiss');
  const aiFileInput = document.getElementById('aiFileInput');
  const aiChatAttach = document.getElementById('aiChatAttach');

  const AI_PROVIDERS = {
    openai: {
      name: 'OpenAI',
      apiKeyPlaceholder: 'Enter your OpenAI API key',
    },
    anthropic: {
      name: 'Anthropic',
      apiKeyPlaceholder: 'Enter your Anthropic API key',
      connectionHint: 'Saved on this ADPanel instance and used server-side through Anthropic Messages API and model discovery.',
    },
    'openai-compatible': {
      name: 'OpenAI Compatible',
      requiresBaseUrl: true,
      keyOptional: true,
      apiKeyPlaceholder: 'Enter your API key',
      baseUrlPlaceholder: 'https://your-provider.example.com/v1',
    },
    google: {
      name: 'Google AI',
      apiKeyPlaceholder: 'Enter your Google AI API key',
    },
    groq: {
      name: 'Groq',
      apiKeyPlaceholder: 'Enter your Groq API key',
    },
    huggingface: {
      name: 'HuggingFace',
      apiKeyPlaceholder: 'Enter your Hugging Face token',
    },
    together: {
      name: 'Together AI',
      apiKeyPlaceholder: 'Enter your Together AI API key',
    },
    cohere: {
      name: 'Cohere',
      apiKeyPlaceholder: 'Enter your Cohere API key',
    },
    openrouter: {
      name: 'OpenRouter',
      apiKeyPlaceholder: 'Enter your OpenRouter API key',
    }
  };

  let aiCurrentProvider = null;
  let aiChatHistory = [];
  let aiFileBackups = [];
  let aiPendingFileOperations = [];
  let aiCurrentPendingOp = null;
  let aiKeysCache = null;
  let aiProviderStateCache = {};
  let aiProviderModelsCache = {};
  let aiLastReadFileContent = null;
  let aiLastReadFilePath = null;
  let aiCurrentChatId = null;
  let aiChatsList = [];
  let aiChatLoadToken = 0;
  let aiAttachedFile = null;
  let aiAbortController = null;
  let aiIsGenerating = false;

  const AI_FILE_BACKUPS_KEY = `ai-file-backups-${bot}`;
  const AI_SELECTED_MODEL_KEY = 'adpanel-ai-selected-model';
  const AI_SELECTED_MODEL_SERVER_KEY = `adpanel-ai-selected-model-${bot}`;

  function loadSelectedAiModelFromStorage() {
    try {
      const serverValue = String(localStorage.getItem(AI_SELECTED_MODEL_SERVER_KEY) || '').trim();
      if (serverValue) return serverValue;
      return String(localStorage.getItem(AI_SELECTED_MODEL_KEY) || '').trim();
    } catch {
      return '';
    }
  }

  function saveSelectedAiModelToStorage(value) {
    try {
      const normalized = String(value || '').trim();
      if (normalized) {
        localStorage.setItem(AI_SELECTED_MODEL_SERVER_KEY, normalized);
        localStorage.setItem(AI_SELECTED_MODEL_KEY, normalized);
      } else {
        localStorage.removeItem(AI_SELECTED_MODEL_SERVER_KEY);
        localStorage.removeItem(AI_SELECTED_MODEL_KEY);
      }
    } catch { }
  }

  function isPersistableAiModelSelection(value) {
    const { providerId, modelName } = decodeAiModelSelection(String(value || '').trim());
    return !!(providerId && modelName);
  }

  function loadFileBackupsFromStorage() {
    try {
      const stored = localStorage.getItem(AI_FILE_BACKUPS_KEY);
      if (stored) {
        aiFileBackups = JSON.parse(stored);
        const oneDayAgo = Date.now() - (24 * 60 * 60 * 1000);
        aiFileBackups = aiFileBackups.filter(b => b.timestamp > oneDayAgo);
        saveFileBackupsToStorage();
      }
    } catch (e) {
      aiFileBackups = [];
    }
    renderFileChangesBar();
  }

  function saveFileBackupsToStorage() {
    try {
      localStorage.setItem(AI_FILE_BACKUPS_KEY, JSON.stringify(aiFileBackups));
    } catch (e) {
      console.warn('[AI] Failed to save backups to localStorage:', e);
    }
    renderFileChangesBar();
  }

  const aiFileChangesBar = document.getElementById('aiFileChangesBar');
  const aiFileChangesList = document.getElementById('aiFileChangesList');
  const aiFileChangesCount = document.getElementById('aiFileChangesCount');
  const aiAcceptAllChanges = document.getElementById('aiAcceptAllChanges');
  const aiUndoAllChanges = document.getElementById('aiUndoAllChanges');
  const aiFileChangesToggle = document.getElementById('aiFileChangesToggle');
  const aiConfirmDialog = document.getElementById('aiConfirmDialog');
  const aiConfirmAction = document.getElementById('aiConfirmAction');
  const aiConfirmPath = document.getElementById('aiConfirmPath');
  const aiConfirmPreview = document.getElementById('aiConfirmPreview');
  const aiConfirmAllow = document.getElementById('aiConfirmAllow');
  const aiConfirmDeny = document.getElementById('aiConfirmDeny');

  let aiActionTokens = {};

  function getStoredAiProviderState(provider) {
    return aiProviderStateCache?.[provider] || {};
  }

  function encodeAiModelSelection(providerId, modelName) {
    return JSON.stringify({ provider: providerId, model: modelName });
  }

  function decodeAiModelSelection(rawValue) {
    if (!rawValue) return { providerId: '', modelName: '' };
    try {
      const parsed = JSON.parse(rawValue);
      return {
        providerId: String(parsed?.provider || '').trim(),
        modelName: String(parsed?.model || '').trim()
      };
    } catch {
      const separatorIdx = rawValue.indexOf(':');
      if (separatorIdx === -1) return { providerId: '', modelName: '' };
      return {
        providerId: rawValue.slice(0, separatorIdx).trim(),
        modelName: rawValue.slice(separatorIdx + 1).trim()
      };
    }
  }

  function buildAiProviderApiKeyPlaceholder(provider, providerState = getStoredAiProviderState(provider)) {
    const config = AI_PROVIDERS[provider];
    if (!config) return 'Enter your API key';
    if (providerState?.maskedKey) {
      return `Leave empty to keep the saved key (${providerState.maskedKey})`;
    }
    return config.apiKeyPlaceholder || 'Enter your API key';
  }

  function updateAiProviderFormState() {
    if (!aiKeySave) return;
    if (!aiCurrentProvider) {
      aiKeySave.disabled = true;
      return;
    }

    const config = AI_PROVIDERS[aiCurrentProvider];
    const stored = getStoredAiProviderState(aiCurrentProvider);
    const hasKey = config?.keyOptional ? true : !!(aiApiKeyInput?.value.trim() || stored?.keyConfigured);
    const hasBaseUrl = !config?.requiresBaseUrl || !!(aiBaseUrlInput?.value.trim() || stored?.baseUrl);
    aiKeySave.disabled = !(hasKey && hasBaseUrl);
  }

  async function loadAiKeysFromBackend() {
    try {
      const response = await fetch('/api/ai/keys');
      if (!response.ok) return {};
      const data = await response.json();
      if (!data.ok) return {};

      if (data.actionTokens) {
        aiActionTokens = data.actionTokens;
      }

      aiProviderStateCache = data.providers || {};
      const keys = {};
      for (const [provider, info] of Object.entries(aiProviderStateCache)) {
        if (info?.configured) {
          keys[provider] = true;
        }
      }
      aiKeysCache = keys;
      return keys;
    } catch {
      return aiKeysCache || {};
    }
  }

  async function saveAiProviderConfig(provider, { key = '', baseUrl = '' } = {}) {
    try {
      if (!aiActionTokens.setKey) {
        await loadAiKeysFromBackend();
      }
      const response = await fetch('/api/ai/keys', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-action-token': aiActionTokens.setKey || ''
        },
        body: JSON.stringify({ provider, key, baseUrl })
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok || !data.ok) {
        return { ok: false, error: data.error || `Failed to connect provider (${response.status})` };
      }

      if (data.providerState) {
        aiProviderStateCache[provider] = data.providerState;
      }
      if (!aiKeysCache) aiKeysCache = {};
      if (data.providerState?.configured) {
        aiKeysCache[provider] = true;
      } else {
        delete aiKeysCache[provider];
      }
      if (Array.isArray(data.models)) {
        aiProviderModelsCache[provider] = data.models;
      }
      loadAiKeysFromBackend().catch(() => { });
      return data;
    } catch {
      return { ok: false, error: 'Failed to connect provider' };
    }
  }

  function hasConfiguredProvider() {
    if (!aiKeysCache) return false;
    return Object.values(aiKeysCache).some(Boolean);
  }

  async function updateProviderStatuses() {
    const keys = await loadAiKeysFromBackend();
    for (const provider of Object.keys(AI_PROVIDERS)) {
      const statusEl = document.getElementById(`status-${provider}`);
      const statusLabelEl = document.getElementById(`status-label-${provider}`);
      const cardEl = document.querySelector(`[data-provider="${provider}"]`);
      const isConnected = !!keys[provider];
      if (statusEl) {
        statusEl.classList.toggle('connected', isConnected);
      }
      if (statusLabelEl) {
        statusLabelEl.textContent = isConnected ? 'Connected' : '';
      }
      if (cardEl) {
        cardEl.classList.toggle('connected', isConnected);
      }
    }
  }

  async function fetchModels(provider, { forceRefresh = false } = {}) {
    const config = AI_PROVIDERS[provider];
    if (!config) return [];
    if (!forceRefresh && Array.isArray(aiProviderModelsCache[provider]) && aiProviderModelsCache[provider].length > 0) {
      return aiProviderModelsCache[provider];
    }

    try {
      const response = await fetch(`/api/ai/models/${encodeURIComponent(provider)}`);
      const data = await response.json().catch(() => ({}));
      if (!response.ok || !data.ok) {
        throw new Error(data.error || `Failed to load models (${response.status})`);
      }

      const models = Array.isArray(data.models) ? data.models : [];
      aiProviderModelsCache[provider] = models;
      if (data.providerState) {
        aiProviderStateCache[provider] = data.providerState;
      }
      return models;
    } catch (err) {
      console.warn(`[AI] Failed to fetch models for ${provider}:`, err);
      return [];
    }
  }

  function closeAiModelDropdown() {
    if (!aiModelSelector || !aiModelTrigger) return;
    aiModelSelector.classList.remove('open');
    aiModelTrigger.setAttribute('aria-expanded', 'false');
    aiModelOptions?.setAttribute('aria-hidden', 'true');
  }

  function updateAiModelSelectorState({ persist = true } = {}) {
    if (!aiModelSelect || !aiModelCurrent || !aiModelOptions) return;

    const selectedOption = aiModelSelect.selectedOptions?.[0] || aiModelSelect.options[aiModelSelect.selectedIndex] || null;
    aiModelCurrent.textContent = selectedOption?.textContent || 'Select model...';
    if (persist && selectedOption && !selectedOption.disabled && isPersistableAiModelSelection(aiModelSelect.value)) {
      saveSelectedAiModelToStorage(aiModelSelect.value);
    }

    const selectedValue = aiModelSelect.value;
    aiModelOptions.querySelectorAll('.ai-model-option').forEach(optionEl => {
      const isSelected = optionEl.dataset.value === selectedValue;
      optionEl.classList.toggle('selected', isSelected);
      optionEl.setAttribute('aria-selected', isSelected ? 'true' : 'false');
    });
  }

  function syncAiModelSelectorFromNative({ persist = false } = {}) {
    if (!aiModelSelect || !aiModelTrigger || !aiModelOptions || !aiModelCurrent || !aiModelSelector) return;

    closeAiModelDropdown();
    aiModelOptions.innerHTML = '';
    let hasSelectableOptions = false;

    const appendModelOption = (option) => {
      if (!option) return;

      if (option.disabled) {
        if (!option.value) {
          const emptyState = document.createElement('div');
          emptyState.className = 'ai-model-empty';
          emptyState.textContent = option.textContent;
          aiModelOptions.appendChild(emptyState);
        }
        return;
      }

      hasSelectableOptions = true;
      const optionButton = document.createElement('button');
      optionButton.type = 'button';
      optionButton.className = 'ai-model-option';
      optionButton.dataset.value = option.value;
      optionButton.setAttribute('role', 'option');
      optionButton.setAttribute('aria-selected', 'false');
      optionButton.innerHTML = `
        <span class="ai-model-option-name">${escapeHtmlAi(option.textContent)}</span>
        <span class="ai-model-option-check"><i class="fa-solid fa-check"></i></span>
      `;
      aiModelOptions.appendChild(optionButton);
    };

    Array.from(aiModelSelect.children).forEach(child => {
      const tagName = child.tagName?.toUpperCase();
      if (tagName === 'OPTGROUP') {
        const groupLabel = document.createElement('div');
        groupLabel.className = 'ai-model-group';
        groupLabel.textContent = child.label;
        aiModelOptions.appendChild(groupLabel);
        Array.from(child.children).forEach(appendModelOption);
        return;
      }

      if (tagName === 'OPTION') {
        appendModelOption(child);
      }
    });

    aiModelTrigger.disabled = !hasSelectableOptions;
    aiModelSelector.classList.toggle('is-disabled', !hasSelectableOptions);
    aiModelOptions.setAttribute('aria-hidden', aiModelSelector.classList.contains('open') ? 'false' : 'true');
    updateAiModelSelectorState({ persist });
  }

  async function populateModelSelector() {
    if (!aiModelSelect) return;
    const previouslySelectedModel = loadSelectedAiModelFromStorage();
    const keys = await loadAiKeysFromBackend();
    aiModelSelect.innerHTML = '<option value="" disabled selected>Loading models...</option>';
    syncAiModelSelectorFromNative({ persist: false });

    let hasOptions = false;

    const promises = Object.entries(AI_PROVIDERS).map(async ([providerId, config]) => {
      if (!keys[providerId]) return null;

      const models = await fetchModels(providerId);
      return { providerId, name: config.name, models };
    });

    const results = await Promise.all(promises);

    aiModelSelect.innerHTML = '';

    for (const result of results) {
      if (!result || result.models.length === 0) continue;

      const optgroup = document.createElement('optgroup');
      optgroup.label = result.name;

      for (const model of result.models) {
        const opt = document.createElement('option');
        opt.value = encodeAiModelSelection(result.providerId, model);
        opt.textContent = model;
        optgroup.appendChild(opt);
      }

      aiModelSelect.appendChild(optgroup);
      hasOptions = true;
    }

    if (!hasOptions) {
      aiModelSelect.innerHTML = '<option value="" disabled selected>No models available</option>';
    } else if (aiModelSelect.options.length > 0) {
      const optionValues = Array.from(aiModelSelect.options).map((option) => option.value);
      if (previouslySelectedModel && optionValues.includes(previouslySelectedModel)) {
        aiModelSelect.value = previouslySelectedModel;
      } else {
        aiModelSelect.selectedIndex = 0;
      }
    }

    syncAiModelSelectorFromNative({ persist: hasOptions });
  }

  async function openAiHelpModal() {
    if (!aiHelpModal) return;
    if (!hasAgentAccess()) { alert('You do not have permission to access ADPanel Agent'); return; }
    aiHelpModal.classList.add('show');
    aiHelpModal.style.display = 'flex';
    aiHelpModal.setAttribute('aria-hidden', 'false');

    await updateProviderStatuses();

    if (hasConfiguredProvider()) {
      if (aiHelpSetup) aiHelpSetup.style.display = 'none';
      if (aiHelpChat) aiHelpChat.style.display = 'flex';
      await populateModelSelector();
      await loadChatHistory();
    } else {
      if (aiHelpSetup) aiHelpSetup.style.display = 'flex';
      if (aiHelpChat) aiHelpChat.style.display = 'none';
      if (aiApiKeyForm) aiApiKeyForm.style.display = 'none';
      if (aiProviderGrid) aiProviderGrid.style.display = 'grid';
    }
  }

  function closeAiHelpModal() {
    if (!aiHelpModal) return;
    animateClose(aiHelpModal, () => {
      saveChatHistory();
    });
  }

  function showApiKeyForm(provider) {
    if (!aiApiKeyForm || !aiProviderGrid) return;
    aiCurrentProvider = provider;
    const config = AI_PROVIDERS[provider];
    if (!config) return;
    const stored = getStoredAiProviderState(provider);

    aiProviderGrid.style.display = 'none';
    aiApiKeyForm.style.display = 'block';
    if (aiKeyProviderName) aiKeyProviderName.textContent = config.name;
    if (aiBaseUrlField) {
      aiBaseUrlField.style.display = config.requiresBaseUrl ? 'block' : 'none';
    }
    if (aiBaseUrlInput) {
      aiBaseUrlInput.value = config.requiresBaseUrl ? String(stored.baseUrl || '') : '';
      aiBaseUrlInput.placeholder = config.baseUrlPlaceholder || 'https://your-provider.example.com/v1';
    }
    if (aiApiKeyInput) {
      aiApiKeyInput.value = '';
      aiApiKeyInput.placeholder = buildAiProviderApiKeyPlaceholder(provider, stored);
      aiApiKeyInput.type = 'password';
    }
    if (aiKeyToggle) {
      aiKeyToggle.innerHTML = '<i class="fa-solid fa-eye"></i>';
    }
    if (aiKeyHint) {
      aiKeyHint.textContent = config.connectionHint || 'Saved on this ADPanel instance and used server-side for model discovery and chat requests.';
    }
    if (config.requiresBaseUrl && aiBaseUrlInput) {
      aiBaseUrlInput.focus();
    } else if (aiApiKeyInput) {
      aiApiKeyInput.focus();
    }
    updateAiProviderFormState();
  }

  function hideApiKeyForm() {
    if (!aiApiKeyForm || !aiProviderGrid) return;
    aiApiKeyForm.style.display = 'none';
    aiProviderGrid.style.display = 'grid';
    aiCurrentProvider = null;
    if (aiApiKeyInput) aiApiKeyInput.value = '';
    if (aiBaseUrlInput) aiBaseUrlInput.value = '';
    updateAiProviderFormState();
  }


  async function loadChatsFromDb() {
    try {
      const response = await fetch('/api/ai/chats');
      if (!response.ok) {
        aiChatsList = [];
        return [];
      }
      const data = await response.json();
      aiChatsList = Array.isArray(data.chats) ? data.chats : [];
      return aiChatsList;
    } catch {
      aiChatsList = [];
      return [];
    }
  }

  async function createNewChat(title = 'New Chat') {
    try {
      const response = await fetch('/api/ai/chats', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title })
      });
      if (!response.ok) return null;
      const data = await response.json();
      return data.chatId;
    } catch {
      return null;
    }
  }

  async function loadChatFromDb(chatId) {
    try {
      const response = await fetch(`/api/ai/chats/${chatId}`);
      if (!response.ok) return null;
      const data = await response.json();
      return data;
    } catch {
      return null;
    }
  }

  async function saveMessageToDb(chatId, role, content, thinkingTimeMs = null, model = null, imageData = null, thinkingContent = null) {
    try {
      const response = await fetch(`/api/ai/chats/${chatId}/messages`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ role, content, thinking_time_ms: thinkingTimeMs, thinking_content: thinkingContent, model, image_data: imageData })
      });
      return response.ok;
    } catch {
      return false;
    }
  }

  async function deleteChatFromDb(chatId) {
    try {
      const response = await fetch(`/api/ai/chats/${chatId}`, { method: 'DELETE' });
      return response.ok;
    } catch {
      return false;
    }
  }

  async function updateChatTitle(chatId, title) {
    try {
      const response = await fetch(`/api/ai/chats/${chatId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title })
      });
      return response.ok;
    } catch {
      return false;
    }
  }

  async function saveChatHistory() {
  }

  async function loadChatHistory() {
    const loadToken = ++aiChatLoadToken;
    await loadChatsFromDb();
    if (loadToken !== aiChatLoadToken) return;

    if (!aiCurrentChatId && aiChatsList.length > 0) {
      aiCurrentChatId = aiChatsList[0].id;
    }

    let nextHistory = [];

    if (aiCurrentChatId) {
      const requestedChatId = aiCurrentChatId;
      const data = await loadChatFromDb(requestedChatId);
      if (loadToken !== aiChatLoadToken || aiCurrentChatId !== requestedChatId) return;
      if (data && data.messages) {
        nextHistory = data.messages.map(m => ({
          role: m.role === 'assistant' ? 'ai' : m.role,
          content: m.content,
          thinking_time_ms: m.thinking_time_ms,
          thinking_content: m.thinking_content,
          model: m.model,
          timestamp: new Date(m.created_at).getTime(),
          attachment: m.image_data ? { isImage: true, data: m.image_data, name: 'Image' } : null
        }));
      }
    }

    if (loadToken !== aiChatLoadToken) return;
    aiChatHistory = nextHistory;
    renderChatHistory();
    renderChatsList();
  }

  function renderChatsList() {
    const chatsList = document.getElementById('aiChatsList');
    if (!chatsList) return;

    if (!Array.isArray(aiChatsList)) {
      aiChatsList = [];
    }

    if (aiChatsList.length === 0) {
      chatsList.innerHTML = `
        <div class="ai-no-chats">
          <i class="fa-regular fa-comments"></i>
          <p>No chats yet</p>
        </div>
      `;
      return;
    }

    chatsList.innerHTML = aiChatsList.map(chat => `
      <div class="ai-chat-item ${chat.id === aiCurrentChatId ? 'active' : ''}" data-chat-id="${chat.id}">
        <i class="fa-regular fa-comment ai-chat-item-icon"></i>
        <div class="ai-chat-item-content">
          <span class="ai-chat-item-title">${escapeHtmlAi(chat.title)}</span>
          <span class="ai-chat-item-date">${formatRelativeTime(chat.updated_at)}</span>
        </div>
        <button class="ai-chat-item-delete" data-chat-id="${chat.id}" title="Delete chat">
          <i class="fa-solid fa-trash"></i>
        </button>
      </div>
    `).join('');

    chatsList.querySelectorAll('.ai-chat-item').forEach(item => {
      item.addEventListener('click', async (e) => {
        if (e.target.closest('.ai-chat-item-delete')) return;
        const chatId = parseInt(item.dataset.chatId, 10);
        await switchToChat(chatId);
      });
    });

    chatsList.querySelectorAll('.ai-chat-item-delete').forEach(btn => {
      btn.addEventListener('click', async (e) => {
        e.stopPropagation();
        const chatId = parseInt(btn.dataset.chatId, 10);
        if (confirm('Delete this chat?')) {
          await deleteChatFromDb(chatId);
          if (aiCurrentChatId === chatId) {
            aiCurrentChatId = null;
            aiChatHistory = [];
          }
          await loadChatHistory();
        }
      });
    });
  }

  function formatRelativeTime(dateStr) {
    const date = new Date(dateStr);
    const now = new Date();
    const diffMs = now - date;
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);

    if (diffMins < 1) return 'Just now';
    if (diffMins < 60) return `${diffMins}m ago`;
    if (diffHours < 24) return `${diffHours}h ago`;
    if (diffDays < 7) return `${diffDays}d ago`;
    return date.toLocaleDateString();
  }

  async function switchToChat(chatId) {
    const loadToken = ++aiChatLoadToken;
    aiCurrentChatId = chatId;
    const data = await loadChatFromDb(chatId);
    if (loadToken !== aiChatLoadToken || aiCurrentChatId !== chatId) return;
    if (data && data.messages) {
      aiChatHistory = data.messages.map(m => ({
        role: m.role === 'assistant' ? 'ai' : m.role,
        content: m.content,
        thinking_time_ms: m.thinking_time_ms,
        thinking_content: m.thinking_content,
        model: m.model,
        timestamp: new Date(m.created_at).getTime(),
        attachment: m.image_data ? { isImage: true, data: m.image_data, name: 'Image' } : null
      }));
    } else {
      aiChatHistory = [];
    }
    renderChatHistory();
    renderChatsList();
  }

  async function startNewChat() {
    const loadToken = ++aiChatLoadToken;
    aiCurrentChatId = null;
    aiChatHistory = [];
    await loadChatsFromDb();
    if (loadToken !== aiChatLoadToken) return;
    renderChatHistory();
    renderChatsList();
  }

  function renderChatHistory(options = {}) {
    if (!aiChatMessages) return;
    const { animateLatest = false } = options;

    if (aiChatHistory.length === 0) {
      const suggestionItems = [];
      if (hasPerm('server_stop')) suggestionItems.push('Stop this server');
      if (hasPerm('server_start')) suggestionItems.push('Restart this server');
      if (hasPerm('files_read')) suggestionItems.push('What files are in my server?');
      if (hasPerm('files_read')) suggestionItems.push('Help me edit config.yml');
      suggestionItems.push(hasPerm('console_write') ? 'Explain server logs' : 'Help me troubleshoot an error');
      const suggestions = suggestionItems.slice(0, 4).map((label) => `<button class="ai-suggestion">${label}</button>`).join('');
      aiChatMessages.innerHTML = `
      <div class="ai-chat-welcome">
        <div class="ai-welcome-emoji">👋</div>
        <h3>Hello there!</h3>
        <p>Ask for a quick action, files help, or troubleshooting.</p>
        <div class="ai-welcome-suggestions">
          ${suggestions}
        </div>
      </div>
    `;
      bindSuggestionButtons();
      return;
    }

    const userAvatarEl = document.querySelector('#profileCard .icon img');
    const userAvatarSrc = userAvatarEl ? userAvatarEl.src : '';

    aiChatMessages.innerHTML = aiChatHistory.map((msg, index) => {
      const thinkingBadge = buildThinkingBadgeHtml(msg.thinking_time_ms, msg.thinking_content);
      const messageClass = `ai-message ${msg.role}${animateLatest && index === aiChatHistory.length - 1 ? ' ai-enter' : ''}`;

      const attachmentHtml = msg.attachment && msg.attachment.isImage
        ? `<div class="ai-message-attachment"><img src="${msg.attachment.data}" alt="${escapeHtmlAi(msg.attachment.name || 'Image')}"></div>`
        : '';

      const avatarContent = msg.role === 'ai'
        ? '<img src="/images/ADPanel.webp" alt="AI" class="ai-message-avatar-img">'
        : (userAvatarSrc
          ? `<img src="${userAvatarSrc}" alt="You" class="ai-user-avatar-img">`
          : '<i class="fa-solid fa-user"></i>');

      return `
      <div class="${messageClass}">
        <div class="ai-msg-avatar ${msg.role}">
          ${avatarContent}
        </div>
        <div class="ai-message-body">
          ${msg.role === 'ai' && thinkingBadge ? `<div class="ai-message-meta">${thinkingBadge}</div>` : ''}
          <div class="ai-message-content">${msg.role === 'ai' ? (msg.isTyping ? '' : renderAiMarkdown(msg.content)) : escapeHtmlAi(msg.content).replace(/\n/g, '<br>')}</div>
          ${attachmentHtml}
        </div>
      </div>
    `;
    }).join('');

    aiChatMessages.scrollTop = aiChatMessages.scrollHeight;

    bindAiCodeCopyButtons(aiChatMessages);
  }

  function escapeHtmlAi(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function aiCopyToClipboard(text, btn) {
    const cleanText = text.trim();
    function onSuccess() {
      btn.innerHTML = '<i class="fa-solid fa-check"></i>';
      btn.classList.add('copied');
      setTimeout(() => { btn.innerHTML = '<i class="fa-solid fa-copy"></i>'; btn.classList.remove('copied'); }, 1500);
    }
    if (navigator.clipboard && window.isSecureContext) {
      navigator.clipboard.writeText(cleanText).then(onSuccess).catch(() => fallbackCopy(cleanText));
    } else {
      fallbackCopy(cleanText);
    }
    function fallbackCopy(t) {
      const ta = document.createElement('textarea');
      ta.value = t; ta.style.position = 'fixed'; ta.style.opacity = '0';
      document.body.appendChild(ta); ta.select();
      try { document.execCommand('copy'); onSuccess(); } catch (e) {   }
      document.body.removeChild(ta);
    }
  }

  function renderAiMarkdown(s) {
    let html = escapeHtmlAi(String(s));

    html = html.replace(/```(\w*)\n([\s\S]*?)```/g, (_, lang, code) => {
      const langLabel = lang || 'bash';
      const codeId = 'code-' + Math.random().toString(36).substr(2, 9);
      const trimmedCode = code.replace(/^\s+|\s+$/g, '');
      return '<div class="ai-code-block">' +
        '<div class="ai-code-header">' +
        '<div class="ai-code-dots"><span class="dot-red"></span><span class="dot-yellow"></span><span class="dot-green"></span></div>' +
        '<span class="ai-code-lang">' + escapeHtmlAi(langLabel) + '</span>' +
        '<button class="ai-code-copy" data-code-id="' + codeId + '" title="Copy"><i class="fa-solid fa-copy"></i></button>' +
        '</div>' +
        '<pre class="ai-code-pre"><code id="' + codeId + '">' + trimmedCode + '</code></pre>' +
        '</div>';
    });

    html = html.replace(/`([^`\n]+)`/g, '<code class="ai-inline-code">$1</code>');

    html = html.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');

    html = html.replace(/\*([^*]+)\*/g, '<em>$1</em>');

    html = html.replace(/~~([^~]+)~~/g, '<del>$1</del>');

    html = html.replace(/^### (.+)$/gm, '<h4 class="ai-md-h4">$1</h4>');
    html = html.replace(/^## (.+)$/gm, '<h3 class="ai-md-h3">$1</h3>');
    html = html.replace(/^# (.+)$/gm, '<h2 class="ai-md-h2">$1</h2>');

    html = html.replace(/^[\s]*[-] (.+)$/gm, '<li class="ai-md-li">$1</li>');

    html = html.replace(/((?:<li class="ai-md-li">.*<\/li>\n?)+)/g, '<ul class="ai-md-ul">$1</ul>');

    html = html.replace(/^(\d+)\. (.+)$/gm, '<li class="ai-md-oli" value="$1">$2</li>');
    html = html.replace(/((?:<li class="ai-md-oli".*<\/li>\n?)+)/g, '<ol class="ai-md-ol">$1</ol>');

    html = html.replace(/^&gt; (.+)$/gm, '<blockquote class="ai-md-quote">$1</blockquote>');

    html = html.replace(/^---$/gm, '<hr class="ai-md-hr">');

    html = html.replace(/\[([^\]]+)\]\(([^)]+)\)/g, (match, text, url) => {
      const trimmed = url.trim().toLowerCase();
      if (trimmed.startsWith('http://') || trimmed.startsWith('https://') || trimmed.startsWith('mailto:')) {
        return '<a href="' + url + '" target="_blank" rel="noopener" class="ai-md-link">' + text + '</a>';
      }
      return text;
    });

    html = html.replace(/\n/g, '<br>');

    html = html.replace(/<br>\s*(<(?:div|ul|ol|h[2-4]|blockquote|hr|pre))/g, '$1');
    html = html.replace(/(<\/(?:div|ul|ol|h[2-4]|blockquote|hr|pre)>)\s*<br>/g, '$1');

    return html;
  }

  function bindSuggestionButtons() {
    document.querySelectorAll('.ai-suggestion').forEach(btn => {
      btn.addEventListener('click', () => {
        if (aiChatInput) {
          aiChatInput.value = btn.textContent;
          aiChatInput.focus();
          updateSendButton();
        }
      });
    });
  }

  async function typewriteHtml(element, htmlContent, msgObj, speedMs = 15) {
    const tempDiv = document.createElement('div');
    tempDiv.innerHTML = htmlContent;
    element.innerHTML = '';

    async function typeNode(node, parent) {
      if (node.nodeType === Node.TEXT_NODE) {
        const text = node.textContent;
        const textNode = document.createTextNode('');
        parent.appendChild(textNode);

        const chunkSize = Math.max(1, Math.floor(text.length / 40));
        for (let i = 0; i < text.length; i += chunkSize) {
          if (!aiChatHistory.includes(msgObj)) return;
          textNode.textContent += text.substr(i, chunkSize);
          if (aiChatMessages) aiChatMessages.scrollTop = aiChatMessages.scrollHeight;
          await new Promise(r => setTimeout(r, speedMs));
        }
      } else if (node.nodeType === Node.ELEMENT_NODE) {
        const el = document.createElement(node.tagName);
        for (let attr of node.attributes) {
          el.setAttribute(attr.name, attr.value);
        }
        parent.appendChild(el);
        for (let child of node.childNodes) {
          await typeNode(child, el);
          if (!aiChatHistory.includes(msgObj)) return;
        }
        if (aiChatMessages) aiChatMessages.scrollTop = aiChatMessages.scrollHeight;
      }
    }

    for (let child of tempDiv.childNodes) {
      await typeNode(child, element);
      if (!aiChatHistory.includes(msgObj)) return;
    }

    bindAiCodeCopyButtons(element);
  }

  function bindAiCodeCopyButtons(scope) {
    if (!scope) return;
    scope.querySelectorAll('.ai-code-copy[data-code-id]').forEach(btn => {
      if (btn.dataset.boundCopy === 'true') return;
      btn.dataset.boundCopy = 'true';
      btn.addEventListener('click', () => {
        const codeEl = document.getElementById(btn.dataset.codeId);
        if (codeEl) aiCopyToClipboard(codeEl.textContent, btn);
      });
    });
  }

  function buildThinkingBadgeHtml(thinkingTimeMs, thinkingContent = null) {
    const cleanThinkingContent = sanitizeAiThinkingText(thinkingContent);
    if (!cleanThinkingContent) return '';

    const durationLabel = formatAiThinkingDuration(thinkingTimeMs);
    const badgeLabel = durationLabel ? `Thought for ${durationLabel}` : 'Thinking';

    return `
      <details class="ai-thinking-shell">
        <summary class="ai-thinking-toggle">
          <span class="ai-thinking-badge">
            <i class="fa-solid fa-wand-magic-sparkles"></i>
            <span class="ai-thinking-label">${escapeHtmlAi(badgeLabel)}</span>
            <span class="ai-thinking-caret" aria-hidden="true"><i class="fa-solid fa-chevron-right"></i></span>
          </span>
        </summary>
        <div class="ai-thinking-panel">${renderAiMarkdown(cleanThinkingContent)}</div>
      </details>
    `;
  }

  function formatAiThinkingDuration(thinkingTimeMs) {
    const ms = Number(thinkingTimeMs);
    if (!Number.isFinite(ms) || ms <= 0) return '';
    if (ms >= 10000) return `${Math.round(ms / 1000)}s`;
    return `${(ms / 1000).toFixed(1)}s`;
  }

  function sanitizeAiThinkingText(text) {
    return String(text || '')
      .replace(/<think(?:ing)?>/gi, '')
      .replace(/<\/think(?:ing)?>/gi, '')
      .replace(/\[thinking\]/gi, '')
      .replace(/\[\/thinking\]/gi, '')
      .trim();
  }

  function claimTypingIndicatorMessageSlot(thinkingTimeMs = null, thinkingContent = null) {
    if (!aiChatMessages) return null;
    const indicator = aiChatMessages.querySelector('.ai-typing-container');
    if (!indicator) return null;

    indicator.classList.remove('ai-typing-container', 'is-exiting');

    const avatar = indicator.querySelector('.ai-message-avatar, .ai-msg-avatar');
    if (avatar) avatar.className = 'ai-msg-avatar ai';

    let body = indicator.querySelector('.ai-message-body');
    let content = indicator.querySelector('.ai-message-content');

    if (!body) {
      body = document.createElement('div');
      body.className = 'ai-message-body';
      indicator.appendChild(body);
    }

    if (content && content.parentElement !== body) {
      body.appendChild(content);
    }

    if (!content) {
      content = document.createElement('div');
      content.className = 'ai-message-content';
      body.appendChild(content);
    }

    const existingMeta = body.querySelector('.ai-message-meta');
    if (existingMeta) existingMeta.remove();

    const thinkingHtml = buildThinkingBadgeHtml(thinkingTimeMs, thinkingContent);
    if (thinkingHtml) {
      const meta = document.createElement('div');
      meta.className = 'ai-message-meta';
      meta.innerHTML = thinkingHtml;
      body.insertBefore(meta, content);
    }

    content.classList.add('ai-streaming-content');
    content.innerHTML = '';
    return content;
  }

  async function addChatMessage(role, content, thinkingTimeMs = null, model = null, attachment = null, doTypewriter = false, thinkingContent = null) {
    const msg = {
      role,
      content,
      thinking_time_ms: thinkingTimeMs,
      thinking_content: sanitizeAiThinkingText(thinkingContent),
      model,
      timestamp: Date.now(),
      attachment,
      isTyping: doTypewriter && role === 'ai'
    };
    aiChatHistory.push(msg);
    const liveAiContent = role === 'ai' ? claimTypingIndicatorMessageSlot(thinkingTimeMs, msg.thinking_content) : null;
    if (!liveAiContent) {
      renderChatHistory({ animateLatest: true });
    }

    if (aiCurrentChatId) {
      const dbRole = role === 'ai' ? 'assistant' : role;
      const imageData = attachment && attachment.isImage ? attachment.data : null;
      saveMessageToDb(aiCurrentChatId, dbRole, content, thinkingTimeMs, model, imageData, msg.thinking_content).catch(console.error);
    }

    if (liveAiContent) {
      if (msg.isTyping) {
        await typewriteHtml(liveAiContent, renderAiMarkdown(content), msg);
      } else {
        liveAiContent.innerHTML = renderAiMarkdown(content);
      }
      bindAiCodeCopyButtons(liveAiContent.closest('.ai-message-body') || liveAiContent);
      liveAiContent.classList.remove('ai-streaming-content');
      msg.isTyping = false;
      if (aiChatMessages) aiChatMessages.scrollTop = aiChatMessages.scrollHeight;
      return;
    }

    if (msg.isTyping) {
      const messages = aiChatMessages.querySelectorAll('.ai-message.ai');
      if (messages.length > 0) {
        const lastMessage = messages[messages.length - 1];
        const contentDiv = lastMessage.querySelector('.ai-message-content');
        if (contentDiv) {
          await typewriteHtml(contentDiv, renderAiMarkdown(content), msg);
        }
      }
      msg.isTyping = false;
    }
  }

  function showTypingIndicator() {
    if (!aiChatMessages) return;
    const existing = aiChatMessages.querySelector('.ai-typing-container');
    if (existing) return;

    const typingHtml = `
    <div class="ai-message ai ai-enter ai-typing-container">
      <div class="ai-msg-avatar ai"><img src="/images/ADPanel.webp" alt="AI" class="ai-message-avatar-img"></div>
      <div class="ai-message-body">
        <div class="ai-message-content ai-typing-shell">
          <div class="ai-typing" aria-hidden="true">
            <span class="ai-typing-dot"></span>
            <span class="ai-typing-dot"></span>
            <span class="ai-typing-dot"></span>
          </div>
        </div>
      </div>
    </div>
  `;
    aiChatMessages.insertAdjacentHTML('beforeend', typingHtml);
    aiChatMessages.scrollTop = aiChatMessages.scrollHeight;
  }

  function hideTypingIndicator() {
    if (!aiChatMessages) return;
    const indicator = aiChatMessages.querySelector('.ai-typing-container');
    if (!indicator) return;
    if (indicator.classList.contains('is-exiting')) return;
    indicator.classList.add('is-exiting');
    const cleanup = () => {
      if (indicator.parentNode) indicator.remove();
    };
    indicator.addEventListener('transitionend', cleanup, { once: true });
    setTimeout(cleanup, 220);
  }

  function updateSendButton() {
    if (aiChatSend && aiChatInput) {
      const hasContent = aiChatInput.value.trim() || aiAttachedFile;
      aiChatSend.disabled = !hasContent && !aiIsGenerating;

      if (aiIsGenerating) {
        aiChatSend.innerHTML = '<i class="fa-solid fa-square"></i>';
        aiChatSend.classList.add('generating');
        aiChatSend.disabled = false;
        aiChatSend.title = 'Stop generating';
      } else {
        aiChatSend.innerHTML = '<i class="fa-solid fa-arrow-up"></i>';
        aiChatSend.classList.remove('generating');
        aiChatSend.title = 'Send message';
      }
    }
  }

  function handleFileAttachment(file) {
    if (!file) return;

    const maxSize = 5 * 1024 * 1024;
    if (file.size > maxSize) {
      alert('File is too large. Maximum size is 5MB.');
      return;
    }

    const reader = new FileReader();
    reader.onload = (e) => {
      aiAttachedFile = {
        name: file.name,
        type: file.type,
        data: e.target.result,
        isImage: file.type.startsWith('image/')
      };
      showAttachedFilePreview();
      updateSendButton();
    };

    if (file.type.startsWith('image/')) {
      reader.readAsDataURL(file);
    } else {
      reader.readAsText(file);
    }
  }

  function showAttachedFilePreview() {
    if (!aiAttachedFile) return;

    const existingPreview = document.querySelector('.ai-attached-preview');
    if (existingPreview) existingPreview.remove();

    const inputBox = document.querySelector('.ai-chat-input-box');
    if (!inputBox) return;

    const previewHtml = aiAttachedFile.isImage
      ? `<div class="ai-attached-preview">
           <img src="${aiAttachedFile.data}" alt="Attached">
           <button class="ai-attached-remove" title="Remove"><i class="fa-solid fa-xmark"></i></button>
         </div>`
      : `<div class="ai-attached-preview ai-attached-file">
           <i class="fa-solid fa-file-code"></i>
           <button class="ai-attached-remove" title="Remove"><i class="fa-solid fa-xmark"></i></button>
         </div>`;

    const inputRow = inputBox.querySelector('.ai-chat-input-row');
    if (inputRow) {
      inputRow.insertAdjacentHTML('beforebegin', previewHtml);
    } else {
      inputBox.insertAdjacentHTML('afterbegin', previewHtml);
    }

    const removeBtn = inputBox.querySelector('.ai-attached-remove');
    if (removeBtn) {
      removeBtn.addEventListener('click', clearAttachedFile);
    }
  }

  function clearAttachedFile() {
    aiAttachedFile = null;
    const preview = document.querySelector('.ai-attached-preview');
    if (preview) preview.remove();
    if (aiFileInput) aiFileInput.value = '';
    updateSendButton();
  }

  function stopAiGeneration() {
    if (aiAbortController) {
      aiAbortController.abort();
      aiAbortController = null;
    }
    aiIsGenerating = false;
    hideTypingIndicator();
    updateSendButton();
  }

  async function sendAiMessage() {
    if (aiIsGenerating) {
      stopAiGeneration();
      return;
    }

    const messageText = aiChatInput ? aiChatInput.value.trim() : '';
    if (!messageText && !aiAttachedFile) return;

    let message = messageText;
    let attachmentInfo = null;

    if (aiAttachedFile) {
      attachmentInfo = { ...aiAttachedFile };
      if (aiAttachedFile.isImage) {
        message = message || 'Please analyze this image.';
      } else {
        message = `[Attached file: ${aiAttachedFile.name}]\n\nFile content:\n\`\`\`\n${aiAttachedFile.data}\n\`\`\`\n\n${message || 'Please review this file.'}`;
      }
    }

    if (aiChatInput) aiChatInput.value = '';
    clearAttachedFile();

    if (!aiCurrentChatId) {
      const title = message.slice(0, 50) + (message.length > 50 ? '...' : '');
      const chatId = await createNewChat(title);
      if (chatId) {
        aiCurrentChatId = chatId;
        await loadChatsFromDb();
        renderChatHistory();
        renderChatsList();
      } else {
        console.warn('[AI Chat] Failed to create chat in database, continuing with local-only mode');
      }
    }

    if (attachmentInfo && attachmentInfo.isImage) {
      await addChatMessage('user', messageText || '(Image attached)', null, null, attachmentInfo);
    } else {
      await addChatMessage('user', message);
    }

    showTypingIndicator();
    aiIsGenerating = true;
    aiAbortController = new AbortController();
    updateSendButton();

    try {
      const result = await callAiApi(message, attachmentInfo, aiAbortController.signal);

      aiIsGenerating = false;
      aiAbortController = null;
      updateSendButton();

      console.log('[AI Chat] Parsed response:', result);

      if (Array.isArray(result.clientActions) && result.clientActions.length > 0) {
        await handleAiClientActions(result.clientActions);
      }

      let handledStructuredAction = false;

      if (result.fileOperations && result.fileOperations.length > 0) {
        handledStructuredAction = true;
        for (const op of result.fileOperations) {
          await executeFileOperation(op);
        }
      } else if (result.fileOperation) {
        handledStructuredAction = true;
        await executeFileOperation(result);
      }

      if (handledStructuredAction) {
        if (result.message && result.message.trim()) {
          await addChatMessage('ai', result.message.trim(), result.thinking_time_ms, null, null, true, result.thinking_content);
        }
      } else {
        await addChatMessage('ai', result.content || 'Sorry, I could not generate a response.', result.thinking_time_ms, null, null, true, result.thinking_content);
      }
    } catch (err) {
      aiIsGenerating = false;
      aiAbortController = null;
      updateSendButton();

      if (err.name === 'AbortError') {
        await addChatMessage('ai', '⏹️ Generation stopped.');
      } else {
        await addChatMessage('ai', `Error: ${err.message || 'Failed to get response. Please check your API key.'}`);
      }
    }
  }

  async function executeFileOperation(op) {
    if (!op || !op.action) {
      await addChatMessage('ai', 'Unable to process file operation.');
      return;
    }

    if (!hasPerm('files_read')) {
      await addChatMessage('ai', 'You don\'t have permission to access server files.');
      return;
    }

    if (op.action === 'list' || op.action === 'read') {
      await executeFileOperationDirect(op);
    } else {
      if ((op.action === 'write' || op.action === 'create' || op.action === 'mkdir') && !hasPerm('files_create')) {
        await addChatMessage('ai', 'You don\'t have permission to create or modify files.');
        return;
      }
      if (op.action === 'delete' && !hasPerm('files_delete')) {
        await addChatMessage('ai', 'You don\'t have permission to delete files.');
        return;
      }
      aiPendingFileOperations.push(op);
      if (aiPendingFileOperations.length === 1) {
        processNextPendingOperation();
      }
    }
  }

  async function executeFileOperationDirect(op) {
    if (!op || !op.action) {
      await addChatMessage('ai', 'Unable to process file operation.');
      return;
    }

    const serverName = document.documentElement.dataset.bot;

    const normalizePath = (p) => {
      if (!p) return '';
      let normalized = String(p).trim();
      while (normalized.startsWith('/')) {
        normalized = normalized.slice(1);
      }
      return normalized;
    };

    const filePath = normalizePath(op.path);

    try {
      if (op.action === 'read') {
        const response = await fetch(`/api/servers/${encodeURIComponent(serverName)}/files/read?path=${encodeURIComponent(filePath)}`);
        if (!response.ok) {
          const errData = await response.json().catch(() => ({}));
          if (response.status === 502 || (errData.error && errData.error.includes('node'))) {
            await addChatMessage('ai', `⚠️ Cannot connect to server node. Please check if the server is online.`);
            return;
          } else if (response.status === 404) {
            await addChatMessage('ai', `⚠️ File not found: **${filePath}**`);
            return;
          }
          throw new Error(errData.error || `Failed to read file: ${filePath}`);
        }
        const data = await response.json();
        const fileContent = data.content || '(empty file)';

        aiLastReadFileContent = fileContent;
        aiLastReadFilePath = filePath;

        await addChatMessage('ai', `📄 **${filePath}**:\n\`\`\`\n${fileContent}\n\`\`\`\n\nWhat would you like me to change?`, null, null, null, true);

      } else if (op.action === 'write' || op.action === 'create') {
        try {
          const readResp = await fetch(`/api/servers/${encodeURIComponent(serverName)}/files/read?path=${encodeURIComponent(filePath)}`);
          if (readResp.ok) {
            const data = await readResp.json();
            aiFileBackups.push({ path: filePath, content: data.content || '', timestamp: Date.now(), action: 'modify' });
          } else {
            aiFileBackups.push({ path: filePath, content: null, timestamp: Date.now(), action: 'create' });
          }
        } catch {
          aiFileBackups.push({ path: filePath, content: null, timestamp: Date.now(), action: 'create' });
        }
        saveFileBackupsToStorage();

        const response = await writeServerFileWithActionToken(serverName, filePath, op.content || '');

        if (!response.ok) {
          const errData = await response.json().catch(() => ({}));
          if (response.status === 507 || errData.error === 'disk_limit_exceeded') {
            await addChatMessage('ai', `⚠️ **Disk limit exceeded.** ${errData.message || 'Free up space or increase the storage limit.'}`);
            return;
          }
          if (response.status === 502 || (errData.error && errData.error.includes('node'))) {
            await addChatMessage('ai', `⚠️ Cannot connect to server node. Please check if the server is online.`);
            return;
          }
          throw new Error(errData.error || `Failed to ${op.action} file: ${filePath}`);
        }

        aiLastReadFileContent = null;
        aiLastReadFilePath = null;

        const actionWord = op.action === 'create' ? 'Created' : 'Modified';
        await addChatMessage('ai', `✅ ${actionWord} **${filePath}**`, op.thinking_time_ms);
        showUndoToast();

      } else if (op.action === 'mkdir') {
        aiFileBackups.push({ path: filePath, content: null, timestamp: Date.now(), action: 'mkdir' });
        saveFileBackupsToStorage();

        const response = await createServerFolderWithActionToken(serverName, filePath);

        if (!response.ok) {
          const errData = await response.json().catch(() => ({}));
          if (response.status === 502 || (errData.error && errData.error.includes('node'))) {
            await addChatMessage('ai', `⚠️ Cannot connect to server node. Please check if the server is online.`);
            return;
          }
          throw new Error(errData.error || `Failed to create folder: ${filePath}`);
        }

        await addChatMessage('ai', `📁 Created folder **${filePath}**`);
        showUndoToast();

      } else if (op.action === 'list') {
        const listPath = filePath || '.';
        try {
          const response = await fetch(`/api/servers/${encodeURIComponent(serverName)}/files/list?path=${encodeURIComponent(listPath)}`);

          if (!response.ok) {
            const errData = await response.json().catch(() => ({}));
            const errorMsg = errData.error || `HTTP ${response.status}`;

            if (response.status === 502 || errorMsg.includes('node') || errorMsg === 'node_offline') {
              await addChatMessage('ai', `⚠️ **Cannot connect to server node**\n\nThe server node appears to be offline or unreachable.\n\n**Please check:**\n1. Is the node agent running?\n2. Can the panel reach the node? (check firewall)\n3. Check the server status in the dashboard\n\n*File operations require an active node connection.*`);
              return;
            } else if (response.status === 404) {
              await addChatMessage('ai', `⚠️ Directory not found: **${listPath}**`);
              return;
            } else if (response.status === 403) {
              await addChatMessage('ai', `⚠️ You don't have permission to access files on this server.`);
              return;
            }
            throw new Error(errorMsg);
          }

          const data = await response.json();

          const files = data.entries || data.files || [];
          if (files.length === 0) {
            await addChatMessage('ai', `📂 **${listPath === '.' ? '/' : listPath}** is empty.`);
          } else {
            const fileList = files.map(f => {
              const isDir = f.isDirectory === true || f.type === 'dir' || f.type === 'directory';
              const icon = isDir ? '📁' : '📄';
              const size = isDir ? '' : (f.size ? ` (${formatFileSize(f.size)})` : '');
              return `${icon} ${f.name}${size}`;
            }).join('\n');
            await addChatMessage('ai', `📂 Contents of **${listPath === '.' ? '/' : listPath}**:\n\n${fileList}`, null, null, null, true);
          }
        } catch (fetchErr) {
          await addChatMessage('ai', `⚠️ Failed to list files: ${fetchErr.message || 'Connection error'}`);
          return;
        }

      } else if (op.action === 'delete') {
        try {
          const readResp = await fetch(`/api/servers/${encodeURIComponent(serverName)}/files/read?path=${encodeURIComponent(filePath)}`);
          if (readResp.ok) {
            const data = await readResp.json();
            aiFileBackups.push({ path: filePath, content: data.content || '', timestamp: Date.now(), action: 'delete' });
            saveFileBackupsToStorage();
          }
        } catch { }

        const response = await deleteServerPathWithActionToken(serverName, filePath);

        if (!response.ok) throw new Error(`Failed to delete: ${filePath}`);

        await addChatMessage('ai', `🗑️ Deleted **${filePath}**`);
        showUndoToast();
      }
    } catch (err) {
      await addChatMessage('ai', `❌ Error: ${err.message}`);
    }
  }

  function formatFileSize(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
  }

  function sanitizeAiVisibleAssistantText(text) {
    let value = String(text || '')
      .replace(/<think>[\s\S]*?<\/think>/gi, ' ')
      .replace(/<thinking>[\s\S]*?<\/thinking>/gi, ' ')
      .replace(/^\s*(?:thought|thinking|reasoning|analysis)\s*:\s*[\s\S]*$/gim, ' ')
      .trim();

    if (!value) return '';
    if (/^[`'"“”‘’()[\]{}<>.,:;!?*_~\\/\-|+=\s]+$/.test(value)) return '';

    const normalized = value.replace(/\s+/g, ' ').trim();
    const leakedInstructionTalk = [
      /\bwait,\s*the prompt says\b/i,
      /\bthe prompt says\b/i,
      /\bprevious reply was invalid\b/i,
      /\bentire reply must be one\b/i,
      /<adpanel_tool_plan>/i,
      /\bcurrent page server is\b/i,
      /\bcurrent page server:\b/i,
      /\bthe user wants to\b/i,
      /\bi have the\b/i,
      /\bi can use\b/i,
      /\bi should use\b/i,
      /\bi need to use\b/i,
      /\blet'?s refine\b/i,
      /\brespond with exactly one\b/i,
      /\bi need to check\b/i,
      /\btool which can be used\b/i
    ].some((pattern) => pattern.test(normalized));
    const leakedToolReference = /\b(?:power_server|inspect_server|query_console|send_console_command|list_backups|create_backup|restore_backup|delete_backup)\b/i.test(normalized);
    const truncatedReasoning = /(?:i should use the|i can use the|i need to use the)\s*`?$/i.test(normalized);

    if (leakedInstructionTalk || (leakedToolReference && /\b(?:the user wants to|i have the|i can use|i should use|i need to use|let'?s refine|i need to check|tool which can be used)\b/i.test(normalized)) || truncatedReasoning) {
      return '';
    }

    return value;
  }

  function compactAiAssistantReply(text) {
    const raw = sanitizeAiVisibleAssistantText(text);
    if (!raw) return raw;
    return raw;
  }

  function normalizeAiServerNameCandidate(value) {
    const normalized = String(value || '')
      .trim()
      .replace(/^["'`]+|["'`]+$/g, '')
      .replace(/[.,!?;:]+$/g, '')
      .trim();
    return normalized;
  }

  function applyAiServerActionState(action, serverName) {
    const target = normalizeAiServerNameCandidate(serverName);
    const current = normalizeAiServerNameCandidate(bot);
    if (!target || !current || target.toLowerCase() !== current.toLowerCase()) return;

    if (action === 'start' || action === 'restart') {
      resetConsoleBuffer();
      resetServerConsoleHistorySession();
    }
    if (action === 'start') setPowerState(POWER_STATE.STARTING);
    if (action === 'stop' || action === 'kill') setPowerState(POWER_STATE.STOPPING);
    if (action === 'restart') setPowerState(POWER_STATE.STARTING);

    if (action === 'start' || action === 'restart') {
      attachLogStreamIfRemote({ tail: 0, forceReconnect: true });
    }

    setTimeout(() => {
      refreshPowerState({ force: true }).catch(() => { });
    }, 250);
  }

  async function handleAiClientActions(actions) {
    const list = Array.isArray(actions) ? actions : [];
    const seenActionKeys = new Set();
    for (const action of list) {
      const type = String(action && action.type || '').trim().toLowerCase();
      const targetServer = normalizeAiServerNameCandidate(action && action.server);
      const actionKey = type === 'refresh_power_state'
        ? `${type}:${targetServer}:${String(action && action.action || '').trim().toLowerCase()}`
        : type === 'console_command_executed'
          ? `${type}:${targetServer}:${String(action && action.command || '').trim()}:${Number(action && action.sentAt) || 0}`
          : `${type}:${targetServer}`;
      if (seenActionKeys.has(actionKey)) continue;
      seenActionKeys.add(actionKey);

      if (type === 'refresh_power_state') {
        if (targetServer && targetServer.toLowerCase() === String(bot || '').trim().toLowerCase()) {
          applyAiServerActionState(action.action, targetServer);
        }
        continue;
      }

      if (type === 'console_command_executed') {
        if (targetServer && targetServer.toLowerCase() === String(bot || '').trim().toLowerCase()) {
          const commandText = String(action.command || '').trim();
          const sentAt = Number(action.sentAt) || Date.now();
          if (commandText && hasConsoleReadAccess()) {
            appendCommandEcho(commandText, sentAt);
          }
          scheduleConsoleHistoryResync(commandText, sentAt);
        }
        continue;
      }

      if (type === 'backups_updated') {
        if (targetServer && targetServer.toLowerCase() === String(bot || '').trim().toLowerCase()) {
          if (backupsModal && backupsModal.classList.contains('show')) {
            await loadBackups().catch(() => { });
          }
        }
      }
    }
  }

  async function callAiApi(message, attachmentInfo = null, abortSignal = null) {
    const modelValue = aiModelSelect?.value || '';
    const { providerId, modelName } = decodeAiModelSelection(modelValue);

    if (!providerId || !modelName) {
      throw new Error('Please select a model first.');
    }
    saveSelectedAiModelToStorage(modelValue);

    const config = AI_PROVIDERS[providerId];
    if (!config) {
      throw new Error('Unknown provider.');
    }

    const filePatterns = [
      /(?:modify|change|edit|update|set|turn|enable|disable|fix|put|add|remove|optimize)\s+(?:in\s+)?(?:the\s+)?(?:file\s+)?([^\s]+\.(yml|yaml|json|properties|txt|conf|cfg|toml|ini))/i,
      /(?:in|at)\s+([^\s]+\.(yml|yaml|json|properties|txt|conf|cfg|toml|ini))/i,
      /([^\s]+\.(yml|yaml|json|properties|txt|conf|cfg|toml|ini))\s+(?:and\s+)?(?:modify|change|edit|update|set|turn|enable|disable|fix|put|add|remove|optimize)/i
    ];

    let targetFile = null;
    for (const pattern of filePatterns) {
      const match = message.match(pattern);
      if (match) {
        targetFile = match[1];
        break;
      }
    }

    if (targetFile && !aiLastReadFileContent && hasPerm('files_read')) {
      try {
        const serverName = document.documentElement.dataset.bot;
        const response = await fetch(`/api/servers/${encodeURIComponent(serverName)}/files/read?path=${encodeURIComponent(targetFile)}`);
        if (response.ok) {
          const data = await response.json();
          if (data.content) {
            aiLastReadFileContent = data.content;
            aiLastReadFilePath = targetFile;
          }
        }
      } catch (e) {
      }
    }

    let augmentedMessage = message;
    if (aiLastReadFileContent && aiLastReadFilePath) {
      const modifyIntent = /(?:set|change|modify|edit|update|enable|disable|fix|optimize|turn|make|add|remove)/i.test(message);
      if (modifyIntent) {
        augmentedMessage = `User request: ${message}

The file "${aiLastReadFilePath}" currently contains:
\`\`\`
${aiLastReadFileContent}
\`\`\`

Apply the user's requested changes. You MUST output the complete modified file using this exact format:
FILE_WRITE:${aiLastReadFilePath}
---FILE_CONTENT_START---
(paste the ENTIRE file with changes applied here)
---FILE_CONTENT_END---`;
      }
    }

    const canReadFiles = hasPerm('files_read');
    const canWriteFiles = hasPerm('files_create');
    const canDeleteFiles = hasPerm('files_delete');
    const canReadConsole = hasPerm('console_read');
    const canWriteConsole = hasPerm('console_write');
    const canStartServer = hasPerm('server_start');
    const canStopServer = hasPerm('server_stop');
    const canViewBackups = hasPerm('backups_view');
    const canCreateBackups = hasPerm('backups_create');
    const canDeleteBackups = hasPerm('backups_delete');
    const currentServerName = String(bot || '').trim();

    let systemPrompt;

    const baseIdentity = `You are ADPanel Agent, an intelligent AI assistant integrated into ADPanel — an enterprise server management panel that lets users host and manage their Docker-based servers (FiveM, Minecraft, and more).

CORE BEHAVIOR:
1. NEVER use diacritical marks, accents, or special unicode characters in ANY language. Write as if you only have a standard US English keyboard.
2. Reply naturally, like a fast voice assistant.
3. Default to one short sentence. Use two short sentences only when needed.
4. Prefer plain text. Use markdown only when it clearly helps.
5. For greetings, answer in 2-6 words.
6. For actions, say what you did or what you need next.
7. Prioritize the user's latest message and the live server context over older chat history if they conflict.
8. Never narrate internal planning, hidden reasoning, or which tool you are about to use.
9. Never say phrases like "The user wants", "I need to", "I should use", "Plan", or "Current page server".`;

    const nativeToolLines = [
      'Sensitive server actions, console actions, and backup actions are executed through native backend tools.',
      `Current server: ${currentServerName || 'current-server'}.`,
      'Do not explain your internal planning or mention internal tool names in the visible reply.',
      'Do not emit SERVER_START, SERVER_STOP, SERVER_RESTART, or SERVER_KILL lines.',
      'If backend tools are needed, let the backend orchestrator handle them; never write planning text such as "I need to use inspect_server".',
      'Use the current server when the user means this server or current server.',
      'Backend permissions are re-checked on every native action, so do not claim success before the tool result confirms it.',
      'For live console checks or crash diagnosis, prefer the native console inspection tool.',
      'For real console execution, only use the native console command tool when the user explicitly asked to run a command.',
      'For backup restore or delete, inspect the available backups first when the reference is unclear.'
    ].filter(Boolean).join('\n');

    if (canReadFiles) {
      systemPrompt = `${baseIdentity}

You have access to this server's file system. You can browse, read, create, edit, and manage files directly.

${nativeToolLines}

Use these commands (one per line, no extra formatting):

FILE_LIST:.
FILE_LIST:config
FILE_READ:config/settings.yml
${canWriteFiles ? `FILE_MKDIR:plugins/myfolder` : ''}
${canDeleteFiles ? `FILE_DELETE:old-file.txt` : ''}

${canWriteFiles ? `To create or modify a file:
FILE_CREATE:path/to/file.txt
---FILE_CONTENT_START---
(file content here)
---FILE_CONTENT_END---

To modify an existing file:
FILE_WRITE:path/to/file.yml
---FILE_CONTENT_START---
(complete modified file content)
---FILE_CONTENT_END---` : ''}

IMPORTANT RULES:
1. Put each command on its own line
2. Path comes immediately after the colon (no spaces)
3. Use . for root directory listing
4. Use relative paths (no leading /)
5. Only list files or perform file operations when the user explicitly asks for it
6. When modifying a file, output the COMPLETE file content
7. After completing a task, briefly describe what you did
8. Do NOT automatically list files or show server files unless the user specifically asks to see them
9. When the user greets you or asks general questions, respond conversationally without performing file operations
10. When asked what ADPanel is, explain it is an enterprise Docker server management panel with file management, console, scheduling, backups, store, and AI agent features`;
    } else {
      systemPrompt = `${baseIdentity}

You do NOT have file system access for this user. Do not attempt to list, read, or modify any server files.

${nativeToolLines}

Instead, provide helpful advice, explain concepts, troubleshoot issues, and guide users on configuring their servers. If the user asks you to manage files, let them know they need file management permissions and suggest contacting their server administrator.`;
    }

    if (canReadConsole) {
      systemPrompt += `\n\nWhen the user shares server logs or console output, analyze them carefully and provide helpful troubleshooting advice. Identify errors, warnings, and suggest specific fixes.`;
    }

    if (attachmentInfo && attachmentInfo.isImage) {
      systemPrompt += `\n\nYou also have vision capabilities. When the user provides an image, analyze it and describe what you see. If the image appears to be a screenshot or configuration, provide helpful insights about it.`;
    }

    const historyMessages = aiChatHistory.slice(-10).map(m => {
      if (m.attachment && m.attachment.isImage) {
        return {
          role: m.role === 'ai' ? 'assistant' : m.role,
          content: m.content,
          image: m.attachment.data
        };
      }
      return { role: m.role === 'ai' ? 'assistant' : m.role, content: m.content };
    });

    if (historyMessages.length > 0 && historyMessages[historyMessages.length - 1].role === 'user') {
      if (attachmentInfo && attachmentInfo.isImage) {
        historyMessages[historyMessages.length - 1] = { role: 'user', content: augmentedMessage, image: attachmentInfo.data };
      } else {
        historyMessages[historyMessages.length - 1] = { role: 'user', content: augmentedMessage };
      }
    }

    const messages = [
      { role: 'system', content: systemPrompt },
      ...historyMessages
    ];

    const result = await callAiBackendProxy(providerId, modelName, messages, abortSignal);
    if (result && typeof result.content === 'string') {
      result.content = compactAiAssistantReply(sanitizeAiVisibleAssistantText(result.content));
    }
    if (result && typeof result.message === 'string') {
      result.message = compactAiAssistantReply(sanitizeAiVisibleAssistantText(result.message));
    }

    if (result.fileOperation && result.action === 'write' && aiLastReadFileContent) {
      const originalContent = aiLastReadFileContent;
      const originalLength = originalContent.length;
      const newContent = result.content || '';
      const newLength = newContent.length;

      if (originalLength > 100 && newLength < originalLength * 0.3) {
        const mergedContent = tryMergePartialChange(originalContent, newContent, aiLastReadFilePath);
        if (mergedContent) {
          result.content = mergedContent;
        } else {
          const savedPath = aiLastReadFilePath;
          aiLastReadFileContent = null;
          aiLastReadFilePath = null;
          return {
            content: `The AI only provided partial content which would corrupt your file. Please try again with a clearer instruction like "in ${savedPath}, change [setting] to [value]".`
          };
        }
      }

      aiLastReadFileContent = null;
      aiLastReadFilePath = null;
    }

    return result;
  }

  function tryMergePartialChange(originalContent, partialContent, filePath) {
    const ext = filePath.split('.').pop().toLowerCase();

    if (['yml', 'yaml', 'properties'].includes(ext)) {
      const lines = partialContent.trim().split('\n');
      let modifiedContent = originalContent;
      let changesMade = false;

      for (const line of lines) {
        const yamlMatch = line.match(/^(\s*)([a-zA-Z0-9_-]+)\s*:\s*(.*)$/);
        const propsMatch = line.match(/^([a-zA-Z0-9_.-]+)\s*=\s*(.*)$/);

        if (yamlMatch) {
          const [, indent, key, value] = yamlMatch;
          const keyPattern = new RegExp(`^(\\s*)${escapeRegex(key)}\\s*:.*$`, 'gm');
          if (keyPattern.test(modifiedContent)) {
            modifiedContent = modifiedContent.replace(keyPattern, `$1${key}: ${value}`);
            changesMade = true;
          }
        } else if (propsMatch) {
          const [, key, value] = propsMatch;
          const keyPattern = new RegExp(`^${escapeRegex(key)}\\s*=.*$`, 'gm');
          if (keyPattern.test(modifiedContent)) {
            modifiedContent = modifiedContent.replace(keyPattern, `${key}=${value}`);
            changesMade = true;
          }
        }
      }

      if (changesMade) {
        return modifiedContent;
      }
    }

    if (ext === 'json') {
      try {
        const originalJson = JSON.parse(originalContent);
        const partialJson = JSON.parse(partialContent);
        const merged = deepMerge(originalJson, partialJson);
        return JSON.stringify(merged, null, 2);
      } catch {
      }
    }

    return null;
  }

  function escapeRegex(str) {
    return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  function deepMerge(target, source) {
    const result = { ...target };
    for (const key in source) {
      if (source[key] && typeof source[key] === 'object' && !Array.isArray(source[key])) {
        result[key] = deepMerge(result[key] || {}, source[key]);
      } else {
        result[key] = source[key];
      }
    }
    return result;
  }

  async function callAiBackendProxy(provider, model, messages, abortSignal = null, onChunk = null) {
    const fetchOptions = {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        provider,
        model,
        messages,
        stream: !!onChunk,
        serverContext: {
          currentServer: String(bot || '').trim()
        }
      })
    };

    if (abortSignal) {
      fetchOptions.signal = abortSignal;
    }

    const response = await fetch('/api/ai/chat', fetchOptions);

    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      throw new Error(error.error || `API error: ${response.status}`);
    }

    if (onChunk) {
      const reader = response.body.getReader();
      const decoder = new TextDecoder('utf-8');
      let done = false;
      let buffer = '';
      let thinkingTimeMs = null;
      let thinkingContent = '';
      let fullContent = '';

      while (!done) {
        const { value, done: readerDone } = await reader.read();
        done = readerDone;
        if (value) {
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop();
          for (const line of lines) {
            const trimmed = line.trim();
            if (trimmed.startsWith('data: ')) {
              if (trimmed === 'data: [DONE]') continue;
              try {
                const parsed = JSON.parse(trimmed.substring(6));
                if (parsed.error) {
                  throw new Error(parsed.error);
                }
                if (parsed.chunk) {
                  fullContent += parsed.chunk;
                  onChunk(parsed.chunk, fullContent);
                }
                if (parsed.done) {
                  thinkingTimeMs = parsed.thinking_time_ms;
                  thinkingContent = typeof parsed.thinking_content === 'string' ? parsed.thinking_content : thinkingContent;
                }
              } catch (e) {
              }
            }
          }
        }
      }

      const parsedContent = parseAiResponse(fullContent);
      parsedContent.thinking_time_ms = thinkingTimeMs;
      parsedContent.thinking_content = thinkingContent;
      parsedContent.clientActions = [];
      return parsedContent;
    } else {
      const data = await response.json();
      if (!data.ok) {
        throw new Error(data.error || 'AI request failed');
      }

      const result = parseAiResponse(data.content || '');
      result.thinking_time_ms = data.thinking_time_ms || null;
      result.thinking_content = typeof data.thinking_content === 'string' ? data.thinking_content : '';
      result.clientActions = Array.isArray(data.clientActions) ? data.clientActions : [];
      return result;
    }
  }


  async function callOpenAiCompatible(endpoint, apiKey, model, messages, providerId) {
    return await callAiBackendProxy(providerId, model, messages);
  }

  async function callGoogleAi(apiKey, model, messages) {
    return await callAiBackendProxy('google', model, messages);
  }

  async function callCohereAi(apiKey, model, messages) {
    return await callAiBackendProxy('cohere', model, messages);
  }

  function parseAiResponse(content) {
    if (!content || typeof content !== 'string') {
      return { content: content || 'No response received.' };
    }

    let trimmed = content.trim().replace(/\\n/g, '\n');
    const operations = [];
    let remainingMessage = trimmed;

    const toolCallPattern = /\[TOOL_CALL\]\s*\{tool\s*=>\s*"([^"]+)",\s*args\s*=>\s*\{([^}]*)\}\s*\}\s*\[\/TOOL_CALL\]/gi;
    let toolMatch;
    while ((toolMatch = toolCallPattern.exec(trimmed)) !== null) {
      const toolName = toolMatch[1].toLowerCase();
      const argsContent = toolMatch[2];

      const pathMatch = argsContent.match(/--path\s+"([^"]+)"/i);
      const contentArgMatch = argsContent.match(/--content\s+"([^"]+)"/i);

      if (pathMatch) {
        const path = pathMatch[1];
        const fileContent = contentArgMatch ? contentArgMatch[1] : null;

        if (toolName === 'file_mkdir' || toolName === 'mkdir') {
          operations.push({ action: 'mkdir', path });
        } else if (toolName === 'file_create' || toolName === 'create') {
          operations.push({ action: 'create', path, content: fileContent || '' });
        } else if (toolName === 'file_write' || toolName === 'write') {
          operations.push({ action: 'write', path, content: fileContent || '' });
        } else if (toolName === 'file_read' || toolName === 'read') {
          operations.push({ action: 'read', path });
        } else if (toolName === 'file_list' || toolName === 'list') {
          operations.push({ action: 'list', path });
        } else if (toolName === 'file_delete' || toolName === 'delete') {
          operations.push({ action: 'delete', path });
        }
      }
      remainingMessage = remainingMessage.replace(toolMatch[0], '');
    }


    const invokePattern = /<invoke>\s*<(FILE_\w+)>([\s\S]*?)<\/\1>\s*<\/invoke>/gi;
    let invokeMatch;
    while ((invokeMatch = invokePattern.exec(trimmed)) !== null) {
      const action = invokeMatch[1].toLowerCase().replace('file_', '');
      const innerContent = invokeMatch[2];
      parseXmlOperation(action, innerContent, operations);
      remainingMessage = remainingMessage.replace(invokeMatch[0], '');
    }

    const directPattern = /<(FILE_\w+)>([\s\S]*?)<\/\1>/gi;
    let directMatch;
    while ((directMatch = directPattern.exec(trimmed)) !== null) {
      if (!operations.some(op => op._rawMatch === directMatch[0])) {
        const action = directMatch[1].toLowerCase().replace('file_', '');
        const innerContent = directMatch[2];
        parseXmlOperation(action, innerContent, operations);
        remainingMessage = remainingMessage.replace(directMatch[0], '');
      }
    }

    const antlPattern = /<invoke\s+name="([^"]+)"[^>]*>([\s\S]*?)<\/antml:invoke>/gi;
    let antlMatch;
    while ((antlMatch = antlPattern.exec(trimmed)) !== null) {
      const action = antlMatch[1].toLowerCase().replace('file_', '');
      const innerContent = antlMatch[2];
      parseXmlOperation(action, innerContent, operations);
      remainingMessage = remainingMessage.replace(antlMatch[0], '');
    }

    const listPattern = /FILE_LIST:([^\s\n]+)/gi;
    let listMatch;
    while ((listMatch = listPattern.exec(trimmed)) !== null) {
      let path = listMatch[1].trim();
      if (path.includes('FILE_')) continue;
      if (!operations.some(op => op.action === 'list' && op.path === path)) {
        operations.push({ action: 'list', path });
      }
      remainingMessage = remainingMessage.replace(listMatch[0], '');
    }

    const readPattern = /FILE_READ:([^\s\n]+)/gi;
    let readMatch;
    while ((readMatch = readPattern.exec(trimmed)) !== null) {
      let path = readMatch[1].trim();
      if (path.includes('FILE_')) continue;
      if (!operations.some(op => op.action === 'read' && op.path === path)) {
        operations.push({ action: 'read', path });
      }
      remainingMessage = remainingMessage.replace(readMatch[0], '');
    }

    const mkdirPattern = /FILE_MKDIR:([^\s\n]+)/gi;
    let mkdirMatch;
    while ((mkdirMatch = mkdirPattern.exec(trimmed)) !== null) {
      let path = mkdirMatch[1].trim();
      if (path.includes('FILE_')) continue;
      if (!operations.some(op => op.action === 'mkdir' && op.path === path)) {
        operations.push({ action: 'mkdir', path });
      }
      remainingMessage = remainingMessage.replace(mkdirMatch[0], '');
    }

    const deletePattern = /FILE_DELETE:([^\s\n]+)/gi;
    let deleteMatch;
    while ((deleteMatch = deletePattern.exec(trimmed)) !== null) {
      let path = deleteMatch[1].trim();
      if (path.includes('FILE_')) continue;
      if (!operations.some(op => op.action === 'delete' && op.path === path)) {
        operations.push({ action: 'delete', path });
      }
      remainingMessage = remainingMessage.replace(deleteMatch[0], '');
    }

    const createPattern = /FILE_CREATE:([^\s\n]+)/i;
    const createMatch = trimmed.match(createPattern);
    if (createMatch) {
      const filePath = createMatch[1].trim();
      if (!filePath.includes('FILE_') && !operations.some(op => op.action === 'create' && op.path === filePath)) {
        const fileContent = extractFileContent(trimmed, createMatch[0]);
        if (fileContent !== null) {
          operations.push({ action: 'create', path: filePath, content: fileContent });
          remainingMessage = remainingMessage.replace(/FILE_CREATE:[^\n]*[\s\S]*?---FILE_CONTENT_END---/i, '');
          remainingMessage = remainingMessage.replace(/FILE_CREATE:[^\n]*[\s\S]*?```[\s\S]*?```/i, '');
        }
      }
    }

    const writePattern = /FILE_WRITE:([^\s\n]+)/i;
    const writeMatch = trimmed.match(writePattern);
    if (writeMatch) {
      const filePath = writeMatch[1].trim();
      if (!filePath.includes('FILE_') && !operations.some(op => op.action === 'write' && op.path === filePath)) {
        const fileContent = extractFileContent(trimmed, writeMatch[0]);
        if (fileContent !== null) {
          operations.push({ action: 'write', path: filePath, content: fileContent });
          remainingMessage = remainingMessage.replace(/FILE_WRITE:[^\n]*[\s\S]*?---FILE_CONTENT_END---/i, '');
          remainingMessage = remainingMessage.replace(/FILE_WRITE:[^\n]*[\s\S]*?```[\s\S]*?```/i, '');
        } else {
          return { content: `I couldn't extract the file content for ${filePath}. Please try again.` };
        }
      }
    }

    remainingMessage = remainingMessage
      .replace(/---FILE_CONTENT_START---|---FILE_CONTENT_END---/g, '')
      .replace(/<\/?antml:[^>]+>/g, '')
      .replace(/<\/?invoke>/g, '')
      .replace(/<\/?FILE_\w+>/g, '')
      .replace(/<path>[^<]*<\/path>/g, '')
      .replace(/<content>[\s\S]*?<\/content>/g, '')
      .replace(/\[TOOL_CALL\][\s\S]*?\[\/TOOL_CALL\]/g, '')
      .replace(/SERVER_(?:START|STOP|RESTART|KILL)(?::[^\s\n]+)?/gi, '')
      .trim();

    if (operations.length > 0) {
      const response = { message: remainingMessage || null };
      if (operations.length === 1) {
        response.fileOperation = true;
        Object.assign(response, operations[0]);
      } else if (operations.length > 1) {
        response.fileOperations = operations;
      }
      return response;
    }

    return { content };
  }

  function parseXmlOperation(action, innerContent, operations) {
    let path = null;
    let fileContent = null;

    const pathMatch = innerContent.match(/<path>([^<]+)<\/path>/i);
    if (pathMatch) {
      path = pathMatch[1].trim();
    }

    const contentMatch = innerContent.match(/<content>([\s\S]*?)<\/content>/i);
    if (contentMatch) {
      fileContent = contentMatch[1].trim();
    }

    if (!path) {
      const plainPath = innerContent.trim().split(/\s+/)[0];
      if (plainPath && !plainPath.startsWith('<')) {
        path = plainPath;
      }
    }

    if (!path) return;

    switch (action) {
      case 'list':
        operations.push({ action: 'list', path });
        break;
      case 'read':
        operations.push({ action: 'read', path });
        break;
      case 'mkdir':
        operations.push({ action: 'mkdir', path });
        break;
      case 'delete':
        operations.push({ action: 'delete', path });
        break;
      case 'create':
        if (fileContent !== null) {
          operations.push({ action: 'create', path, content: fileContent });
        }
        break;
      case 'write':
        if (fileContent !== null) {
          operations.push({ action: 'write', path, content: fileContent });
        }
        break;
    }
  }

  function extractFileContent(fullContent, afterMarker) {
    const afterIdx = fullContent.indexOf(afterMarker);
    if (afterIdx === -1) return null;

    const afterWrite = fullContent.slice(afterIdx + afterMarker.length);

    const startMarker = '---FILE_CONTENT_START---';
    const endMarker = '---FILE_CONTENT_END---';
    let startIdx = afterWrite.indexOf(startMarker);
    let endIdx = afterWrite.indexOf(endMarker);

    if (startIdx !== -1 && endIdx !== -1 && endIdx > startIdx) {
      return afterWrite.slice(startIdx + startMarker.length, endIdx).trim();
    }

    const codeBlockMatch = afterWrite.match(/```(?:\w*\n)?([\s\S]*?)```/);
    if (codeBlockMatch) {
      return codeBlockMatch[1].trim();
    }

    const lines = afterWrite.split('\n');
    const contentLines = [];
    let foundContent = false;
    for (const line of lines) {
      const trimLine = line.trim();
      if (!foundContent && trimLine === '') continue;
      if (trimLine.startsWith('FILE_') || trimLine.startsWith('---')) break;
      foundContent = true;
      contentLines.push(line);
    }

    const result = contentLines.join('\n').trim();
    return result || null;
  }


  function renderFileChangesBar() {
    if (!aiFileChangesBar || !aiFileChangesList || !aiFileChangesCount) return;

    if (aiFileBackups.length === 0) {
      aiFileChangesBar.classList.remove('show');
      return;
    }

    aiFileChangesBar.classList.add('show');
    aiFileChangesCount.textContent = aiFileBackups.length;

    aiFileChangesList.innerHTML = aiFileBackups.map((backup, index) => {
      const actionLabel = backup.action === 'create' ? 'Created' :
        backup.action === 'mkdir' ? 'Folder Created' :
          backup.action === 'delete' ? 'Deleted' : 'Modified';
      const iconClass = backup.action === 'create' ? 'create' :
        backup.action === 'mkdir' ? 'mkdir' :
          backup.action === 'delete' ? 'delete' : 'modify';
      const icon = backup.action === 'create' ? 'fa-file-circle-plus' :
        backup.action === 'mkdir' ? 'fa-folder-plus' :
          backup.action === 'delete' ? 'fa-trash' : 'fa-file-pen';

      return `
        <div class="ai-file-change-item" data-index="${index}">
          <div class="ai-file-change-icon ${iconClass}">
            <i class="fa-solid ${icon}"></i>
          </div>
          <div class="ai-file-change-info">
            <div class="ai-file-change-path">${escapeHtmlAi(backup.path).replace(/<br>/g, '')}</div>
            <div class="ai-file-change-action">${actionLabel}</div>
          </div>
          <div class="ai-file-change-actions">
            <button class="ai-file-change-accept" data-index="${index}" title="Accept (keep change)">
              <i class="fa-solid fa-check"></i>
            </button>
            <button class="ai-file-change-undo" data-index="${index}" title="Undo this change">
              <i class="fa-solid fa-rotate-left"></i>
            </button>
          </div>
        </div>
      `;
    }).join('');

    aiFileChangesList.querySelectorAll('.ai-file-change-accept').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const index = parseInt(btn.dataset.index, 10);
        acceptFileChange(index);
      });
    });

    aiFileChangesList.querySelectorAll('.ai-file-change-undo').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const index = parseInt(btn.dataset.index, 10);
        undoFileChangeByIndex(index);
      });
    });
  }

  function acceptFileChange(index) {
    if (index < 0 || index >= aiFileBackups.length) return;
    const backup = aiFileBackups[index];
    aiFileBackups.splice(index, 1);
    saveFileBackupsToStorage();
    addChatMessage('ai', `✓ Accepted change to **${backup.path}**`);
  }

  async function acceptAllFileChanges() {
    const count = aiFileBackups.length;
    aiFileBackups = [];
    saveFileBackupsToStorage();
    await addChatMessage('ai', `✓ Accepted all ${count} file change(s)`);
    hideUndoToast();
  }

  async function undoFileChangeByIndex(index) {
    if (index < 0 || index >= aiFileBackups.length) return;

    const backup = aiFileBackups[index];
    aiFileBackups.splice(index, 1);
    saveFileBackupsToStorage();

    const serverName = document.documentElement.dataset.bot;

    try {
      if (backup.action === 'create' || backup.action === 'mkdir') {
        const response = await deleteServerPathWithActionToken(serverName, backup.path, backup.action === 'mkdir');
        if (!response.ok) throw new Error('Failed to delete');
        await addChatMessage('ai', `↩️ Undone: Deleted ${backup.path}`);
      } else if (backup.action === 'delete') {
        const response = await writeServerFileWithActionToken(serverName, backup.path, backup.content);
        if (response.status === 507) {
          const errData = await response.json().catch(() => ({}));
          throw new Error(errData.message || 'Disk space limit exceeded. Cannot restore file.');
        }
        if (!response.ok) throw new Error('Failed to restore file');
        await addChatMessage('ai', `↩️ Undone: Restored ${backup.path}`);
      } else {
        const response = await writeServerFileWithActionToken(serverName, backup.path, backup.content);
        if (response.status === 507) {
          const errData = await response.json().catch(() => ({}));
          throw new Error(errData.message || 'Disk space limit exceeded. Cannot restore file.');
        }
        if (!response.ok) throw new Error('Failed to restore file');
        await addChatMessage('ai', `↩️ Undone: Reverted ${backup.path}`);
      }
    } catch (err) {
      await addChatMessage('ai', `❌ Undo failed: ${err.message}`);
    }
  }

  async function undoAllFileChanges() {
    const backupsToUndo = [...aiFileBackups];
    for (let i = backupsToUndo.length - 1; i >= 0; i--) {
      await undoFileChangeByIndex(0);
    }
    hideUndoToast();
  }


  function showConfirmDialog(op) {
    if (!aiConfirmDialog) return;
    aiCurrentPendingOp = op;

    const actionLabels = {
      'write': 'Modify File',
      'create': 'Create File',
      'mkdir': 'Create Folder',
      'delete': 'Delete'
    };

    if (aiConfirmAction) aiConfirmAction.textContent = actionLabels[op.action] || op.action.toUpperCase();
    if (aiConfirmPath) aiConfirmPath.textContent = op.path;

    if (aiConfirmPreview) {
      if ((op.action === 'write' || op.action === 'create') && op.content) {
        const preview = op.content.length > 500 ? op.content.slice(0, 500) + '\n... (truncated)' : op.content;
        aiConfirmPreview.textContent = preview;
        aiConfirmPreview.style.display = 'block';
      } else {
        aiConfirmPreview.style.display = 'none';
      }
    }

    aiConfirmDialog.classList.add('show');
  }

  function hideConfirmDialog() {
    if (!aiConfirmDialog) return;
    aiConfirmDialog.classList.remove('show');
    aiCurrentPendingOp = null;
  }

  async function processNextPendingOperation() {
    if (aiPendingFileOperations.length === 0) return;

    const op = aiPendingFileOperations[0];

    if (op.action === 'list' || op.action === 'read') {
      aiPendingFileOperations.shift();
      await executeFileOperationDirect(op);
      processNextPendingOperation();
    } else {
      showConfirmDialog(op);
    }
  }

  async function handleConfirmAllow() {
    if (!aiCurrentPendingOp) return;
    const op = aiCurrentPendingOp;
    hideConfirmDialog();
    aiPendingFileOperations.shift();
    await executeFileOperationDirect(op);
    processNextPendingOperation();
  }

  async function handleConfirmDeny() {
    if (!aiCurrentPendingOp) return;
    const op = aiCurrentPendingOp;
    hideConfirmDialog();
    aiPendingFileOperations.shift();
    await addChatMessage('ai', `⛔ Operation denied: ${op.action} ${op.path}`);
    processNextPendingOperation();
  }

  let undoToastTimeout = null;

  function showUndoToast() {
    if (!aiUndoToast) return;
    if (undoToastTimeout) {
      clearTimeout(undoToastTimeout);
      undoToastTimeout = null;
    }
    aiUndoToast.classList.add('show');
    undoToastTimeout = setTimeout(() => {
      hideUndoToast();
    }, 8000);
  }

  function hideUndoToast() {
    if (!aiUndoToast) return;
    if (undoToastTimeout) {
      clearTimeout(undoToastTimeout);
      undoToastTimeout = null;
    }
    aiUndoToast.classList.remove('show');
  }

  async function undoLastFileChange() {
    if (aiFileBackups.length === 0) return;

    const backup = aiFileBackups.pop();
    if (!backup) return;

    const serverName = document.documentElement.dataset.bot;

    try {
      if (backup.action === 'create' || backup.action === 'mkdir') {
        const response = await deleteServerPathWithActionToken(serverName, backup.path, backup.action === 'mkdir');
        if (!response.ok) throw new Error('Failed to delete');
        await addChatMessage('ai', `↩️ Undone: Deleted ${backup.path}`);
      } else if (backup.action === 'delete') {
        const response = await writeServerFileWithActionToken(serverName, backup.path, backup.content);
        if (response.status === 507) {
          const errData = await response.json().catch(() => ({}));
          throw new Error(errData.message || 'Disk space limit exceeded. Cannot restore file.');
        }
        if (!response.ok) throw new Error('Failed to restore file');
        await addChatMessage('ai', `↩️ Undone: Restored ${backup.path}`);
      } else {
        const response = await writeServerFileWithActionToken(serverName, backup.path, backup.content);
        if (response.status === 507) {
          const errData = await response.json().catch(() => ({}));
          throw new Error(errData.message || 'Disk space limit exceeded. Cannot restore file.');
        }
        if (!response.ok) throw new Error('Failed to restore file');
        await addChatMessage('ai', `↩️ Undone: Reverted ${backup.path}`);
      }

      hideUndoToast();
    } catch (err) {
      await addChatMessage('ai', `❌ Undo failed: ${err.message}`);
    }
  }

  function clearAiChat() {
    aiChatHistory = [];
    localStorage.removeItem(AI_CHAT_HISTORY);
    renderChatHistory();
  }

  if (aiHelpModalClose) {
    aiHelpModalClose.addEventListener('click', closeAiHelpModal);
  }

  if (aiProviderGrid) {
    aiProviderGrid.addEventListener('click', (e) => {
      const card = e.target.closest('[data-provider]');
      if (card) {
        showApiKeyForm(card.dataset.provider);
      }
    });
  }

  if (aiKeyBack) {
    aiKeyBack.addEventListener('click', hideApiKeyForm);
  }

  if (aiKeyToggle && aiApiKeyInput) {
    aiKeyToggle.addEventListener('click', () => {
      const isPassword = aiApiKeyInput.type === 'password';
      aiApiKeyInput.type = isPassword ? 'text' : 'password';
      aiKeyToggle.innerHTML = `<i class="fa-solid fa-eye${isPassword ? '-slash' : ''}"></i>`;
    });
  }

  if (aiApiKeyInput) {
    aiApiKeyInput.addEventListener('input', () => {
      updateAiProviderFormState();
    });
  }

  if (aiBaseUrlInput) {
    aiBaseUrlInput.addEventListener('input', () => {
      updateAiProviderFormState();
    });
  }

  if (aiKeySave) {
    aiKeySave.addEventListener('click', async () => {
      if (!aiCurrentProvider) return;
      aiKeySave.disabled = true;
      aiKeySave.textContent = 'Connecting...';

      const result = await saveAiProviderConfig(aiCurrentProvider, {
        key: aiApiKeyInput?.value.trim() || '',
        baseUrl: aiBaseUrlInput?.value.trim() || ''
      });

      if (result.ok) {
        await updateProviderStatuses();
        hideApiKeyForm();

        if (hasConfiguredProvider()) {
          if (aiHelpSetup) aiHelpSetup.style.display = 'none';
          if (aiHelpChat) aiHelpChat.style.display = 'flex';
          await populateModelSelector();
          await loadChatHistory();
        }
      } else {
        alert(result.error || 'Failed to connect provider');
      }

      aiKeySave.textContent = 'Save & Connect';
      updateAiProviderFormState();
    });
  }

  if (aiModelTrigger) {
    aiModelTrigger.addEventListener('click', (e) => {
      e.stopPropagation();
      if (aiModelTrigger.disabled) return;
      const willOpen = !aiModelSelector?.classList.contains('open');
      closeAiModelDropdown();
      if (willOpen && aiModelSelector) {
        aiModelSelector.classList.add('open');
        aiModelTrigger.setAttribute('aria-expanded', 'true');
        aiModelOptions?.setAttribute('aria-hidden', 'false');
      }
    });

    aiModelTrigger.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        aiModelTrigger.click();
      } else if (e.key === 'Escape') {
        closeAiModelDropdown();
      }
    });
  }

  if (aiModelOptions && aiModelSelect) {
    const handleAiModelOptionSelect = (optionButton) => {
      if (!optionButton) return;
      if (!aiModelSelector?.classList.contains('open')) return;
      const nextValue = optionButton.dataset.value || '';
      if (!nextValue) return;

      aiModelSelect.value = nextValue;
      aiModelSelect.dispatchEvent(new Event('change', { bubbles: true }));
      updateAiModelSelectorState();
      closeAiModelDropdown();
      aiModelTrigger?.focus();
    };

    aiModelOptions.addEventListener('pointerdown', (e) => {
      const optionButton = e.target.closest('.ai-model-option');
      if (!optionButton) return;
      e.preventDefault();
      handleAiModelOptionSelect(optionButton);
    });

    aiModelOptions.addEventListener('click', (e) => {
      const optionButton = e.target.closest('.ai-model-option');
      if (!optionButton) return;
      handleAiModelOptionSelect(optionButton);
    });
  }

  if (aiModelSelect) {
    aiModelSelect.addEventListener('change', () => updateAiModelSelectorState({ persist: true }));
  }

  document.addEventListener('click', (e) => {
    if (!e.target.closest('#aiModelSelector')) {
      closeAiModelDropdown();
    }
  });

  if (aiChatInput) {
    aiChatInput.addEventListener('input', () => {
      updateSendButton();
      aiChatInput.style.height = 'auto';
      aiChatInput.style.height = Math.min(aiChatInput.scrollHeight, 120) + 'px';
    });

    aiChatInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        sendAiMessage();
      }
    });

    aiChatInput.addEventListener('paste', (e) => {
      const clipboardItems = e.clipboardData?.items;
      if (!clipboardItems) return;

      for (const item of clipboardItems) {
        if (item.type.startsWith('image/')) {
          e.preventDefault();
          const file = item.getAsFile();
          if (file) {
            handleFileAttachment(file);
          }
          return;
        }
      }
    });
  }

  if (aiChatSend) {
    aiChatSend.addEventListener('click', sendAiMessage);
  }

  syncAiModelSelectorFromNative();
  updateSendButton();

  if (aiChatAttach && aiFileInput) {
    aiChatAttach.addEventListener('click', () => {
      aiFileInput.click();
    });

    aiFileInput.addEventListener('change', (e) => {
      const file = e.target.files[0];
      if (file) handleFileAttachment(file);
    });
  }

  if (aiChatClear) {
    aiChatClear.addEventListener('click', () => {
      if (confirm('Start a new chat?')) startNewChat();
    });
  }

  if (aiHelpSettingsBtn) {
    aiHelpSettingsBtn.addEventListener('click', async () => {
      if (aiHelpChat) aiHelpChat.style.display = 'none';
      if (aiHelpSetup) aiHelpSetup.style.display = 'flex';
      if (aiApiKeyForm) aiApiKeyForm.style.display = 'none';
      if (aiProviderGrid) aiProviderGrid.style.display = 'grid';
      await updateProviderStatuses();
    });
  }

  const aiNewChatBtn = document.getElementById('aiNewChatBtn');
  if (aiNewChatBtn) {
    aiNewChatBtn.addEventListener('click', startNewChat);
  }

  const aiSidebarToggle = document.getElementById('aiSidebarToggle');
  if (aiSidebarToggle) {
    aiSidebarToggle.addEventListener('click', () => {
      const sidebar = document.getElementById('aiChatSidebar');
      if (sidebar) {
        sidebar.classList.toggle('collapsed');
      }
    });
  }

  const aiSidebarExpand = document.getElementById('aiSidebarExpand');
  if (aiSidebarExpand) {
    aiSidebarExpand.addEventListener('click', () => {
      const sidebar = document.getElementById('aiChatSidebar');
      if (sidebar) {
        sidebar.classList.remove('collapsed');
      }
    });
  }

  const aiHelpBtn = document.getElementById('aiHelpBtn');
  const dockAiHelp = document.getElementById('dockAiHelp');

  if (aiHelpBtn) {
    aiHelpBtn.addEventListener('click', (e) => {
      e.preventDefault();
      openAiHelpModal();
    });
  }

  if (dockAiHelp) {
    dockAiHelp.addEventListener('click', (e) => {
      e.preventDefault();
      openAiHelpModal();
    });
  }

  if (aiUndoBtn) {
    aiUndoBtn.addEventListener('click', undoLastFileChange);
  }

  if (aiUndoDismiss) {
    aiUndoDismiss.addEventListener('click', hideUndoToast);
  }

  if (aiAcceptAllChanges) {
    aiAcceptAllChanges.addEventListener('click', acceptAllFileChanges);
  }

  if (aiUndoAllChanges) {
    aiUndoAllChanges.addEventListener('click', undoAllFileChanges);
  }

  if (aiFileChangesToggle && aiFileChangesBar) {
    aiFileChangesToggle.addEventListener('click', () => {
      aiFileChangesBar.classList.toggle('collapsed');
    });
  }

  if (aiConfirmAllow) {
    aiConfirmAllow.addEventListener('click', handleConfirmAllow);
  }

  if (aiConfirmDeny) {
    aiConfirmDeny.addEventListener('click', handleConfirmDeny);
  }

  window.openAiHelpModal = openAiHelpModal;
  window.closeAiHelpModal = closeAiHelpModal;

  const reinstallModal = document.getElementById('reinstallModal');
  const reinstallModalClose = document.getElementById('reinstallModalClose');
  const reinstallCancelBtn = document.getElementById('reinstallCancelBtn');
  const reinstallConfirmBtn = document.getElementById('reinstallConfirmBtn');
  const reinstallConfirmNo = document.getElementById('reinstallConfirmNo');
  const reinstallConfirmYes = document.getElementById('reinstallConfirmYes');
  const reinstallDoneBtn = document.getElementById('reinstallDoneBtn');
  const reinstallErrorClose = document.getElementById('reinstallErrorClose');
  const reinstallDefaultView = document.getElementById('reinstallDefaultView');
  const reinstallConfirmView = document.getElementById('reinstallConfirmView');
  const reinstallProgressView = document.getElementById('reinstallProgressView');
  const reinstallSuccessView = document.getElementById('reinstallSuccessView');
  const reinstallErrorView = document.getElementById('reinstallErrorView');
  const reinstallErrorDesc = document.getElementById('reinstallErrorDesc');

  function reinstallShowView(viewId) {
    [reinstallDefaultView, reinstallConfirmView, reinstallProgressView, reinstallSuccessView, reinstallErrorView].forEach(v => {
      if (v) v.style.display = 'none';
    });
    const target = document.getElementById(viewId);
    if (target) target.style.display = '';
  }

  function openReinstallModal() {
    if (!reinstallModal) return;
    if (!hasPerm('server_reinstall')) {
      return;
    }
    reinstallShowView('reinstallDefaultView');
    reinstallModal.classList.add('show');
    reinstallModal.style.display = 'flex';
    reinstallModal.setAttribute('aria-hidden', 'false');
  }

  function closeReinstallModal() {
    if (!reinstallModal) return;
    animateClose(reinstallModal);
  }

  if (reinstallModalClose) reinstallModalClose.addEventListener('click', closeReinstallModal);
  if (reinstallCancelBtn) reinstallCancelBtn.addEventListener('click', closeReinstallModal);
  if (reinstallErrorClose) reinstallErrorClose.addEventListener('click', closeReinstallModal);
  if (reinstallDoneBtn) reinstallDoneBtn.addEventListener('click', closeReinstallModal);

  if (reinstallConfirmBtn) {
    reinstallConfirmBtn.addEventListener('click', () => {
      reinstallShowView('reinstallConfirmView');
    });
  }

  if (reinstallConfirmNo) {
    reinstallConfirmNo.addEventListener('click', () => {
      reinstallShowView('reinstallDefaultView');
    });
  }

  if (reinstallConfirmYes) {
    reinstallConfirmYes.addEventListener('click', async () => {
      if (!hasPerm('server_reinstall')) {
        reinstallShowView('reinstallDefaultView');
        closeReinstallModal();
        return;
      }

      reinstallShowView('reinstallProgressView');

      try {
        const res = await fetch(`/api/servers/${encodeURIComponent(bot)}/reinstall`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({})
        });

        const data = await res.json().catch(() => ({}));

        if (!res.ok) {
          const errMsg = data.error || 'Reinstall failed. Please try again later.';
          if (reinstallErrorDesc) reinstallErrorDesc.textContent = errMsg;
          reinstallShowView('reinstallErrorView');
          return;
        }

        reinstallShowView('reinstallSuccessView');
      } catch (err) {
        console.error('[reinstall] Network error:', err);
        if (reinstallErrorDesc) reinstallErrorDesc.textContent = 'Network error. Please check your connection and try again.';
        reinstallShowView('reinstallErrorView');
      }
    });
  }

  if (reinstallModal) {
    reinstallModal.addEventListener('click', (e) => {
      if (e.target === reinstallModal) closeReinstallModal();
    });
  }

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && reinstallModal && reinstallModal.classList.contains('show')) {
      closeReinstallModal();
    }
  });

  const subdomainsModal = document.getElementById('subdomainsModal');
  const subdomainsModalClose = document.getElementById('subdomainsModalClose');
  const subdomainsList = document.getElementById('subdomainsList');
  const subdomainsAddBtn = document.getElementById('subdomainsAddBtn');
  const subdomainsRefreshBtn = document.getElementById('subdomainsRefreshBtn');

  const subdomainCreateModal = document.getElementById('subdomainCreateModal');
  const subdomainCreateClose = document.getElementById('subdomainCreateClose');
  const subdomainInput = document.getElementById('subdomainInput');
  const subdomainCreateCancel = document.getElementById('subdomainCreateCancel');
  const subdomainVerifyBtn = document.getElementById('subdomainVerifyBtn');
  const subdomainVerificationResult = document.getElementById('subdomainVerificationResult');
  const overlay = document.getElementById('modalOverlay') || document.querySelector('.activity-modal-overlay') || document.createElement('div');

  function openSubdomainsModal() {
    if (!subdomainsModal) return;
    subdomainsModal.classList.add('show');
    subdomainsModal.style.display = 'flex';
    loadSubdomains();
  }

  function closeSubdomainsModal() {
    if (!subdomainsModal) return;
    animateClose(subdomainsModal);
  }

  if (subdomainsModalClose) {
    subdomainsModalClose.addEventListener('click', closeSubdomainsModal);
  }

  if (subdomainsRefreshBtn) {
    subdomainsRefreshBtn.addEventListener('click', loadSubdomains);
  }

  if (subdomainsAddBtn) {
    subdomainsAddBtn.addEventListener('click', () => {
      openSubdomainCreateModal();
    });
  }

  async function loadSubdomains() {
    if (!subdomainsList) return;
    subdomainsList.innerHTML = `<div class="backups-loading"><i class="fa-solid fa-spinner fa-spin"></i> Loading subdomains...</div>`;
    try {
      const res = await fetch(`/api/subdomains/${encodeURIComponent(bot)}`);
      if (!res.ok) {
        if (res.status === 403) {
          subdomainsList.innerHTML = `<div class="activity-empty-state"><i class="fa-solid fa-lock"></i><span>Access Denied</span></div>`;
        } else {
          subdomainsList.innerHTML = `<div class="activity-empty-state"><i class="fa-solid fa-triangle-exclamation"></i><span>Failed to load subdomains</span></div>`;
        }
        return;
      }
      const data = await res.json();
      renderSubdomains(data.subdomains || []);
    } catch (e) {
      console.error(e);
      subdomainsList.innerHTML = `<div class="activity-empty-state"><i class="fa-solid fa-triangle-exclamation"></i><span>Network error</span></div>`;
    }
  }

  function renderSubdomains(list) {
    if (!list || list.length === 0) {
      subdomainsList.innerHTML = `<div class="activity-empty-state"><i class="fa-solid fa-globe"></i><span>No subdomains found</span></div>`;
      return;
    }
    subdomainsList.innerHTML = '';

    list.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));

    list.forEach(sub => {
      const row = document.createElement('div');
      row.className = 'subdomain-card';

      row.addEventListener('mousemove', e => {
        const rect = row.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        row.style.setProperty('--mouse-x', `${x}px`);
        row.style.setProperty('--mouse-y', `${y}px`);
      });

      let statusBadgeClass = 'pending';
      let statusIcon = 'fa-circle-notch fa-spin';
      let statusText = 'Pending';

      if (sub.status === 'approved') {
        statusBadgeClass = 'approved';
        statusIcon = 'fa-check';
        statusText = 'Active';
      } else if (sub.status === 'canceled') {
        statusBadgeClass = 'canceled';
        statusIcon = 'fa-xmark';
        statusText = 'Invalid DNS';
      }

      let actionsHtml = `
           <button class="sub-btn delete delete-subdomain" data-id="${sub.id}" title="Delete">
             <i class="fa-solid fa-trash"></i>
           </button>
      `;

      if (sub.status === 'pending') {
        actionsHtml = `
           <button class="sub-btn verify verify-subdomain" data-id="${sub.id}" title="Retry Verification">
             <i class="fa-solid fa-rotate-right"></i>
           </button>
           ${actionsHtml}
          `;
      }

      row.innerHTML = `
        <div class="sub-card-header">
           <div class="sub-icon">
              <i class="fa-solid fa-globe"></i>
           </div>
           <div class="sub-info">
              <div class="sub-domain">${escapeHtml(sub.domain)}</div>
              <div class="sub-meta">Added ${new Date(sub.created_at).toLocaleString()}</div>
           </div>
           <div class="sub-status-badge ${statusBadgeClass}">
              <i class="fa-solid ${statusIcon}"></i> ${escapeHtml(statusText)}
           </div>
        </div>
        <div class="sub-card-footer">
           <div class="sub-dns-hint">
             <i class="fa-solid fa-arrow-right-long"></i> Point DNS to this node's IP
           </div>
           <div class="sub-actions">
              ${actionsHtml}
           </div>
        </div>
      `;
      const delBtn = row.querySelector('.delete-subdomain');
      if (delBtn) delBtn.addEventListener('click', () => deleteSubdomain(sub.id, sub.domain));

      const verBtn = row.querySelector('.verify-subdomain');
      if (verBtn) verBtn.addEventListener('click', () => verifySubdomain(sub.id));

      subdomainsList.appendChild(row);
    });
  }

  async function verifySubdomain(id) {
    const btn = document.querySelector(`.verify-subdomain[data-id="${id}"] i`);
    if (btn) btn.className = 'fa-solid fa-spinner fa-spin';

    try {
      const res = await fetch(`/api/subdomains/${encodeURIComponent(bot)}/${id}/verify`, { method: 'POST' });
      const data = await res.json();
      if (res.ok) {
        loadSubdomains();
      } else {
        alert(data.error || 'Verification failed');
        loadSubdomains();
      }
    } catch (e) {
      console.error(e);
      loadSubdomains();
    }
  }

  async function deleteSubdomain(id, domain) {
    if (!confirm(`Delete subdomain ${domain}? This cannot be undone.`)) return;
    try {
      const res = await fetch(`/api/subdomains/${encodeURIComponent(bot)}/${id}`, { method: 'DELETE' });
      if (!res.ok) throw new Error('Failed to delete');
      loadSubdomains();
    } catch (e) {
      alert('Error deleting subdomain: ' + e.message);
    }
  }

  function openSubdomainCreateModal() {
    if (!subdomainCreateModal) return;
    subdomainsModal.classList.remove('show');
    subdomainCreateModal.classList.add('show');
    subdomainCreateModal.style.display = 'flex';

    if (subdomainInput) subdomainInput.value = '';
    if (subdomainVerificationResult) {
      subdomainVerificationResult.style.display = 'none';
      subdomainVerificationResult.innerHTML = '';
    }
    if (subdomainVerifyBtn) {
      subdomainVerifyBtn.disabled = false;
      subdomainVerifyBtn.innerHTML = '<i class="fa-solid fa-check" style="color: white;"></i> <span style="color: white;">Verify & Add</span>';
    }
  }

  function closeSubdomainCreateModal() {
    if (!subdomainCreateModal) return;
    animateClose(subdomainCreateModal, () => {
      if (subdomainsModal) {
        subdomainsModal.classList.add('show');
        subdomainsModal.style.display = 'flex';
        subdomainsModal.setAttribute('aria-hidden', 'false');
      }
    });
  }

  if (subdomainCreateClose) subdomainCreateClose.addEventListener('click', closeSubdomainCreateModal);
  if (subdomainCreateCancel) subdomainCreateCancel.addEventListener('click', closeSubdomainCreateModal);

  if (subdomainVerifyBtn) {
    subdomainVerifyBtn.addEventListener('click', async () => {
      const domain = subdomainInput.value.trim();
      if (!domain) return;

      try {
        subdomainVerifyBtn.disabled = true;
        subdomainVerifyBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Verifying...';

        const res = await fetch(`/api/subdomains/${encodeURIComponent(bot)}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ domain })
        });

        const data = await res.json().catch(() => ({}));

        if (!res.ok) {
          let errorMsg = data.error || 'Failed to add subdomain';
          if (data.details) {
            errorMsg += `<br><small>${escapeHtml(data.details)}</small>`;
          }
          showVerificationError(errorMsg);
          subdomainVerifyBtn.disabled = false;
          subdomainVerifyBtn.innerHTML = '<i class="fa-solid fa-check" style="color: white;"></i> <span style="color: white;">Verify & Add</span>';
          return;
        }

        closeSubdomainCreateModal();
        loadSubdomains();

      } catch (e) {
        console.error(e);
        showVerificationError('Network error');
        subdomainVerifyBtn.disabled = false;
        subdomainVerifyBtn.innerHTML = '<i class="fa-solid fa-check" style="color: white;"></i> <span style="color: white;">Verify & Add</span>';
      }
    });
  }

  function showVerificationError(msg) {
    if (!subdomainVerificationResult) return;
    subdomainVerificationResult.style.display = 'block';
    subdomainVerificationResult.innerHTML = `<div style="color:#f87171; background: rgba(248,113,113,0.1); padding: 8px; border-radius: 6px; font-size: 13px;"><i class="fa-solid fa-circle-exclamation"></i> ${escapeHtml(msg)}</div>`;
  }
})();
