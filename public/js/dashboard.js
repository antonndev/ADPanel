if (typeof globalThis === "undefined") window.globalThis = window;
window.addEventListener("securitypolicyviolation", (e) => {
  console.log("[CSP]", e.violatedDirective, "blocked:", e.blockedURI, "sample:", e.sample);
});

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, function (m) {
    return ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m]);
  });
}

const PANEL_EWS_ICON_SIZE = "1.6rem";

function ensurePanelEwsSvgStyles() {
  if (document.getElementById("panel-ews-svg-styles")) return;
  const style = document.createElement("style");
  style.id = "panel-ews-svg-styles";
  style.textContent = `
    .panel-ews-svg {
      width: ${PANEL_EWS_ICON_SIZE};
      height: ${PANEL_EWS_ICON_SIZE};
      display: block;
      flex: 0 0 auto;
      transition: transform 180ms ease;
    }

    .quick-action.qa-anim-rotate:hover .panel-ews-svg {
      transform: rotate(180deg);
    }
  `;
  document.head.appendChild(style);
}

function createPanelEwsSvg() {
  const template = document.createElement("template");
  template.innerHTML = `
    <svg class="panel-ews-svg" viewBox="0 0 64 64" aria-hidden="true" focusable="false">
      <rect x="5" y="5" width="54" height="54" rx="7" fill="#232C67"></rect>
      <rect x="13" y="12" width="14" height="22" rx="4.5" fill="#FFFFFF"></rect>
      <path d="M18 30H24V43C24 49.6274 18.6274 55 12 55H5V49H12C15.3137 49 18 46.3137 18 43V30Z" fill="#FFFFFF"></path>
      <rect x="35" y="12" width="16" height="5" rx="2.5" fill="#FFFFFF"></rect>
      <rect x="35" y="22" width="16" height="5" rx="2.5" fill="#FFFFFF"></rect>
      <rect x="35" y="32" width="16" height="15" rx="4.5" fill="#FFFFFF"></rect>
    </svg>
  `.trim();
  return template.content.firstElementChild;
}

function replaceLegacyPanelEwsIcons(root = document) {
  ensurePanelEwsSvgStyles();
  root.querySelectorAll("i.fa-panel-ews").forEach((icon) => {
    const svg = createPanelEwsSvg();
    const inlineStyle = icon.getAttribute("style");
    if (inlineStyle) svg.setAttribute("style", `${inlineStyle};width:${PANEL_EWS_ICON_SIZE};height:${PANEL_EWS_ICON_SIZE};display:block;`);
    icon.replaceWith(svg);
  });
}

window.addEventListener('DOMContentLoaded', () => {
  replaceLegacyPanelEwsIcons();

  const splash = document.getElementById('splash-screen');
  const splashImage = splash?.dataset.splashImage || window.__dashboardSplashImage || '/images/hello.webp';
  const splashKey = window.__dashboardSplashKey || `dashboard-splash-cache:${window.location.origin}:${splashImage}`;

  const removeSplash = () => {
    if (splash && splash.isConnected) splash.remove();
  };

  const hideSplash = () => {
    if (!splash || !splash.isConnected) return;
    splash.classList.add('hidden');
    window.setTimeout(removeSplash, 600);
  };

  const markSplashCached = () => {
    try {
      localStorage.setItem(splashKey, '1');
    } catch (_) { }
  };

  const isSplashCached = async () => {
    try {
      const response = await fetch(splashImage, {
        cache: 'only-if-cached',
        mode: 'same-origin'
      });
      return response.ok;
    } catch (_) {
      return false;
    }
  };

  const showSplashForFirstCacheMiss = () => {
    if (!splash) return;
    const displayTime = 5000;
    const preload = new Image();
    let hideScheduled = false;

    const scheduleHide = () => {
      if (hideScheduled) return;
      hideScheduled = true;
      markSplashCached();
      window.setTimeout(hideSplash, displayTime);
    };

    preload.decoding = 'async';
    preload.onload = scheduleHide;
    preload.onerror = removeSplash;
    preload.src = splashImage;

    if (preload.complete) {
      scheduleHide();
    }
  };

  if (splash) {
    const skipSplash = document.documentElement.classList.contains('skip-dashboard-splash');

    if (skipSplash) {
      removeSplash();
    } else {
      isSplashCached()
        .then((cached) => {
          if (cached) {
            markSplashCached();
            removeSplash();
            return;
          }
          showSplashForFirstCacheMiss();
        })
        .catch(showSplashForFirstCacheMiss);
    }
  }

  document.querySelectorAll('[data-scroll-target]').forEach(button => {
    button.addEventListener('click', () => {
      const targetId = button.getAttribute('data-scroll-target');
      const section = document.getElementById(targetId);
      if (section) section.scrollIntoView({ behavior: 'smooth' });
    });
  });

  document.querySelectorAll('.bot-card').forEach(card => {
    const overlay = card.querySelector('[data-offline-overlay]');
    if (!overlay) return;
    const nodeOffline = card.classList.contains('node-offline');
    overlay.style.display = nodeOffline ? 'flex' : 'none';
    card.classList.toggle('is-offline', nodeOffline);
  });

  function updateNodeOfflineStatus(name, nodeOnline) {
    const card = document.querySelector(`.bot-card[data-name="${name.toLowerCase()}"]`);
    if (!card) return;
    const overlay = card.querySelector('[data-offline-overlay]');

    if (nodeOnline) {
      card.classList.remove('node-offline');
      card.classList.remove('is-offline');
      if (overlay) overlay.style.display = 'none';
    } else {
      card.classList.add('node-offline');
      card.classList.add('is-offline');
      if (overlay) overlay.style.display = 'flex';
    }
  }
  window.updateNodeOfflineStatus = updateNodeOfflineStatus;


  function formatBytes(mb) {
    if (mb === null || mb === undefined) return null;
    if (mb === 0) return '0 MB';
    if (mb >= 1024) return `${(mb / 1024).toFixed(1)} GB`;
    return `${Math.round(mb)} MB`;
  }

  function formatResourceLabel(usedMb, limitMb) {
    const used = formatBytes(usedMb);
    const limit = formatBytes(limitMb);
    if (limitMb > 0) return `${used || '0 MB'}/${limit}`;
    if (usedMb > 0) return used;
    return 'N/A';
  }

  function updateCardFromResourceData(data) {
    if (!data || !data.server) return;
    const name = data.server.toLowerCase();
    const card = document.querySelector(`.bot-card[data-name="${name}"]`);
    if (!card) return;

    const status = data.status;
    if (status) {
      const isOnline = status === 'running' || status === 'online';
      const isStopped = status === 'stopped' || status === 'exited';
      if (!isOnline && !isStopped && card.classList.contains('status-online')) {
      } else {
        card.classList.remove('status-online', 'status-stopped', 'status-unknown');
        const statusEl = card.querySelector('.bot-card__status');
        if (statusEl) statusEl.classList.remove('status-online', 'status-stopped', 'status-unknown');
        if (isOnline) {
          card.classList.add('status-online');
          if (statusEl) statusEl.classList.add('status-online');
        } else if (isStopped) {
          card.classList.add('status-stopped');
          if (statusEl) statusEl.classList.add('status-stopped');
        } else {
          card.classList.add('status-unknown');
          if (statusEl) statusEl.classList.add('status-unknown');
        }
      }
    }

    if (data.nodeOnline === true && typeof window.updateNodeOfflineStatus === 'function') {
      window.updateNodeOfflineStatus(data.server, true);
    }

    if (data.memory != null) {
      let usedMb = 0, limitMb = 0, pct = 0;
      if (typeof data.memory === 'object') {
        usedMb = data.memory.used || 0;
        limitMb = data.memory.total || 0;
        pct = data.memory.percent ?? (limitMb > 0 ? Math.min(100, Math.round((usedMb / limitMb) * 100)) : 0);
      } else {
        usedMb = data.memory; limitMb = 0; pct = 0;
      }
      const memLabel = card.querySelector('.bot-card__stat-icon--memory')?.closest('.bot-card__stat')?.querySelector('.bot-card__stat-chip span');
      const memProgress = card.querySelector('.bot-card__stat-icon--memory')?.closest('.bot-card__stat')?.querySelector('.bot-card__progress span');
      if (memLabel) memLabel.textContent = formatResourceLabel(usedMb, limitMb);
      if (memProgress) { memProgress.style.width = `${Math.min(100, Math.max(0, pct))}%`; }
    }

    if (data.disk != null) {
      let usedMb = 0, limitMb = 0, pct = 0;
      if (typeof data.disk === 'object') {
        usedMb = (data.disk.used || 0) * 1024;
        limitMb = (data.disk.total || 0) * 1024;
        pct = data.disk.percent ?? (limitMb > 0 ? Math.min(100, Math.round((usedMb / limitMb) * 100)) : 0);
      } else {
        usedMb = data.disk; limitMb = 0; pct = 0;
      }
      const diskLabel = card.querySelector('.bot-card__stat-icon--disk')?.closest('.bot-card__stat')?.querySelector('.bot-card__stat-chip span');
      const diskProgress = card.querySelector('.bot-card__stat-icon--disk')?.closest('.bot-card__stat')?.querySelector('.bot-card__progress span');
      if (diskLabel) diskLabel.textContent = formatResourceLabel(usedMb, limitMb);
      if (diskProgress) { diskProgress.style.width = `${Math.min(100, Math.max(0, pct))}%`; }
    }

    if (data.cpu != null) {
      const cpuMax = data.cpuLimit || 100;
      const cpuPct = Math.min(cpuMax, Math.max(0, data.cpu || 0));
      const cpuBarWidth = cpuMax > 0 ? Math.min(100, (cpuPct / cpuMax) * 100) : 0;
      const cpuLabel = card.querySelector('.bot-card__stat-icon--cpu')?.closest('.bot-card__stat')?.querySelector('.bot-card__stat-chip span');
      const cpuProgress = card.querySelector('.bot-card__stat-icon--cpu')?.closest('.bot-card__stat')?.querySelector('.bot-card__progress span');
      if (cpuLabel) cpuLabel.textContent = cpuPct > 0 ? `${cpuPct.toFixed(1)}%` : 'N/A';
      if (cpuProgress) cpuProgress.style.width = `${cpuBarWidth}%`;
    }
  }

  let _dashResourceSocket = null;
  let _dashResourceSubscribed = false;
  window._dashResourceSocket = null;

  function getDashboardServerNames() {
    return Array.from(document.querySelectorAll('[data-bot-card]'))
      .map(c => c.dataset.name)
      .filter(Boolean);
  }

  function subscribeDashboardResources(socket) {
    if (!socket || !socket.connected) return;
    const names = getDashboardServerNames();
    if (!names.length) return;
    socket.emit('resources:subscribe-bulk', { servers: names });
    _dashResourceSubscribed = true;
  }

  function unsubscribeDashboardResources(socket) {
    if (!socket) return;
    socket.emit('resources:unsubscribe-all');
    _dashResourceSubscribed = false;
  }

  function initDashboardResourceSocket() {
    if (!window.io) return;
    const socket = window.io({
      transports: ['websocket', 'polling'],
      reconnection: true,
      reconnectionDelay: 1000,
      reconnectionDelayMax: 5000
    });
    _dashResourceSocket = socket;
    window._dashResourceSocket = socket;

    socket.on('connect', () => {
      subscribeDashboardResources(socket);
    });

    socket.on('disconnect', () => {
      _dashResourceSubscribed = false;
    });

    socket.on('resources:data', updateCardFromResourceData);

    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'hidden') {
        unsubscribeDashboardResources(socket);
      } else {
        subscribeDashboardResources(socket);
      }
    });

    window.addEventListener('beforeunload', () => {
      unsubscribeDashboardResources(socket);
    });
  }

  initDashboardResourceSocket();

  document.querySelectorAll('[data-href]').forEach(el => {
    el.addEventListener('click', (event) => {
      const href = el.getAttribute('data-href');
      if (!href) return;
      const target = el.getAttribute('data-target');
      if (target === '_blank') {
        window.open(href, '_blank');
      } else {
        window.location.href = href;
      }
      event.preventDefault();
    });
  });

  function ensureSshTerminalModal() {
    let overlay = document.getElementById('sshTerminalOverlay');
    if (overlay) return overlay;

    overlay = document.createElement('div');
    overlay.id = 'sshTerminalOverlay';
    overlay.className = 'ssh-terminal-overlay';
    overlay.innerHTML = `
      <section class="ssh-terminal-modal" role="dialog" aria-modal="true" aria-label="SSH Terminal">
        <button id="sshTerminalCloseBtn" class="ssh-terminal-close" type="button" aria-label="Close terminal">
          <i class="fa-solid fa-xmark"></i>
        </button>
        <div class="ssh-terminal-frame-wrap">
          <iframe id="sshTerminalFrame" title="ADPanel SSH Terminal" loading="eager" referrerpolicy="no-referrer"></iframe>
        </div>
      </section>
    `;
    document.body.appendChild(overlay);

    const closeBtn = overlay.querySelector('#sshTerminalCloseBtn');
    const closeModal = () => {
      overlay.classList.remove('show');
      const frame = overlay.querySelector('#sshTerminalFrame');
      if (frame) frame.src = 'about:blank';
    };

    closeBtn?.addEventListener('click', closeModal);
    overlay.addEventListener('click', (e) => {
      if (e.target === overlay) closeModal();
    });
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && overlay.classList.contains('show')) closeModal();
    });

    return overlay;
  }

  const sshTerminalBtn = document.getElementById('openSshTerminalBtn');
  sshTerminalBtn?.addEventListener('click', async () => {
    sshTerminalBtn.disabled = true;
    try {
      const res = await fetch('/api/ssh-terminal/session', { credentials: 'include' });
      const data = await res.json().catch(() => ({}));
      if (!res.ok || !data?.launchUrl) {
        throw new Error(data?.error || 'Unable to open terminal');
      }
      const overlay = ensureSshTerminalModal();
      const frame = overlay.querySelector('#sshTerminalFrame');
      if (!frame) throw new Error('Terminal frame unavailable');
      frame.src = data.launchUrl;
      overlay.classList.add('show');
    } catch (err) {
      alert(err?.message || 'Failed to open SSH terminal.');
    } finally {
      sshTerminalBtn.disabled = false;
    }
  });

  document.querySelectorAll('[data-bot-card]').forEach(card => {
    const cover = card.dataset.cover;
    if (cover) card.style.setProperty('--bot-cover', `url("${cover.replace(/"/g, '\\"')}")`);
  });

  document.querySelectorAll('.bot-card__progress span[data-pct]').forEach(span => {
    span.style.width = (span.dataset.pct || '0') + '%';
  });

  const searchInput = document.getElementById('searchBots');
  const botGrid = document.querySelector('[data-bot-grid]');
  const emptyStateTemplate = document.getElementById('empty-state');
  const dashPagination = document.getElementById('dashPagination');

  function navigateDashboard(page, search) {
    const params = new URLSearchParams();
    if (page && page > 1) params.set('page', String(page));
    if (search) params.set('search', search);
    const qs = params.toString();
    window.location.href = '/' + (qs ? '?' + qs : '');
  }

  function updateBotVisibility() {
    if (dashPagination) {
      clearTimeout(_searchTimer);
      _searchTimer = setTimeout(() => navigateDashboard(1, (searchInput?.value || '').trim()), 500);
      return;
    }
    const term = (searchInput?.value || '').trim().toLowerCase();
    const botCards = Array.from(botGrid.querySelectorAll('[data-bot-card]'));
    let visible = 0;
    botCards.forEach(card => {
      const name = card.getAttribute('data-name');
      const match = !term || name.includes(term);
      card.classList.toggle('is-hidden', !match);
      if (match) visible++;
    });
    const existing = botGrid.querySelector('.empty-state');
    if (visible === 0) {
      if (!existing) {
        const clone = emptyStateTemplate.content.cloneNode(true);
        botGrid.appendChild(clone);
      }
    } else if (existing) existing.remove();
  }
  let _searchTimer;
  searchInput?.addEventListener('input', () => {
    clearTimeout(_searchTimer);
    _searchTimer = setTimeout(updateBotVisibility, dashPagination ? 500 : 150);
  });
  if (!dashPagination) updateBotVisibility();

  const urlParams = new URLSearchParams(window.location.search);
  if (searchInput && urlParams.get('search')) {
    searchInput.value = urlParams.get('search');
  }

  if (dashPagination) {
    dashPagination.querySelectorAll('button[data-dash-page]').forEach(btn => {
      btn.addEventListener('click', () => {
        const p = parseInt(btn.dataset.dashPage, 10);
        if (p >= 1) navigateDashboard(p, (searchInput?.value || '').trim());
      });
    });
  }

  const topBar = document.getElementById('topBar');
  const spacer = document.getElementById('topbar-spacer');
  function measureExpandedHeight() {
    const hadCompact = topBar.classList.contains('top-bar--compact');
    topBar.classList.remove('top-bar--compact');
    const h = Math.ceil(topBar.getBoundingClientRect().height);
    if (hadCompact) topBar.classList.add('top-bar--compact');
    return h;
  }
  let expandedHeight = measureExpandedHeight();
  spacer.style.height = expandedHeight + 'px';
  let resizeRAF = null;
  window.addEventListener('resize', () => {
    if (resizeRAF) cancelAnimationFrame(resizeRAF);
    resizeRAF = requestAnimationFrame(() => {
      expandedHeight = measureExpandedHeight();
      spacer.style.height = expandedHeight + 'px';
    });
  });
  new ResizeObserver(() => {
    expandedHeight = measureExpandedHeight();
    spacer.style.height = expandedHeight + 'px';
    fitHero();
  }).observe(topBar);

  function fitHero() {
    const hero = document.querySelector('.hero');
    if (!hero) return;

    hero.style.minHeight = '';
    hero.style.marginBottom = '';

    if (document.querySelector('.quick-actions')) return;

    const rect = hero.getBoundingClientRect();
    const scrollTop = window.scrollY || document.documentElement.scrollTop;
    const absTop = rect.top + scrollTop;
    const heroHeight = hero.offsetHeight;
    const totalContentHeight = absTop + heroHeight;
    const viewHeight = window.innerHeight;

    const diff = viewHeight - totalContentHeight;

    if (diff > 0) {
      hero.style.marginBottom = `${diff + 32}px`;
    }
  }

  window.addEventListener('resize', fitHero);
  fitHero();

  let lastY = window.scrollY || 0, ticking = false, pinnedTimeout = null;
  function onScrollTick() {
    const y = window.scrollY || 0, d = y - lastY;
    if (y > 36) topBar.classList.add('top-bar--compact'); else topBar.classList.remove('top-bar--compact');
    if (y <= 24) {
      topBar.classList.remove('top-bar--hidden', 'top-bar--pinned');
      lastY = y; ticking = false; return;
    }
    if (Math.abs(d) > 4) {
      if (d > 0 && y > 120) {
        topBar.classList.add('top-bar--hidden'); topBar.classList.remove('top-bar--pinned');
      } else if (d < 0) {
        topBar.classList.remove('top-bar--hidden'); topBar.classList.add('top-bar--pinned');
        if (pinnedTimeout) clearTimeout(pinnedTimeout);
        pinnedTimeout = setTimeout(() => topBar.classList.remove('top-bar--pinned'), 900);
      }
    }
    lastY = y; ticking = false;
  }
  window.addEventListener('scroll', () => {
    if (!ticking) {
      requestAnimationFrame(onScrollTick);
      ticking = true;
    }
  }, { passive: true });
  if ((window.scrollY || 0) > 36) topBar.classList.add('top-bar--compact'); else topBar.classList.remove('top-bar--compact');
  topBar.classList.remove('top-bar--hidden');

  function deleteAllClientCookies() {
    const cookies = document.cookie ? document.cookie.split('; ') : [];
    for (const cookie of cookies) {
      const eqPos = cookie.indexOf('=');
      const name = eqPos > -1 ? cookie.substr(0, eqPos) : cookie;
      document.cookie = `${name}=;expires=Thu, 01 Jan 1970 00:00:00 GMT;path=/`;
      document.cookie = `${name}=;expires=Thu, 01 Jan 1970 00:00:00 GMT;path=/;domain=${location.hostname}`;
      const parts = location.hostname.split('.');
      for (let i = 0; i < parts.length - 1; i++) {
        const dom = '.' + parts.slice(i).join('.');
        document.cookie = `${name}=;expires=Thu, 01 Jan 1970 00:00:00 GMT;path=/;domain=${dom}`;
      }
    }
  }
  document.getElementById('logoutBtn')?.addEventListener('click', async (e) => {
    e.preventDefault();
    try {
      deleteAllClientCookies();
      await fetch('/logout', { method: 'POST', credentials: 'include' });
      location.href = '/login';
    } catch {
      alert('Failed to log out');
    }
  });

  const accountBtn = document.getElementById('accountBtn');
  const accountBackBtn = document.getElementById('accountBackBtn');
  const accountPage = document.getElementById('accountPage');
  const mainContent = document.getElementById('mainContent');
  const footerEl = document.getElementById('upload');

  async function showAccountPage() {
    if (!accountPage) return;

    if (window.location.hash !== '#account') {
      window.history.pushState(null, null, '#account');
    }

    if (mainContent) mainContent.style.display = 'none';
    if (footerEl) footerEl.style.display = 'none';

    accountPage.style.display = 'block';

    try {
      const res = await fetch('/api/account', { credentials: 'include' });
      if (!res.ok) throw new Error('Failed to load account');
      const data = await res.json();

      const usernameEl = document.getElementById('accountUsername');
      const emailEl = document.getElementById('accountEmail');
      const usernameValEl = document.getElementById('accountUsernameValue');
      const emailValEl = document.getElementById('accountEmailValue');
      const roleValEl = document.getElementById('accountRoleValue');
      const idValEl = document.getElementById('accountIdValue');
      const avatarImgEl = document.getElementById('accountAvatarImg');
      const statusTextEl = document.getElementById('accountStatusText');
      const cooldownEl = document.getElementById('usernameCooldown');

      const badgeClass = data.isAdmin ? 'admin' : 'user';
      const badgeText = data.isAdmin ? 'Admin' : 'User';

      if (usernameEl) usernameEl.innerHTML = `${escapeHtml(data.username)} <span class="account-badge ${badgeClass}">${badgeText}</span>`;
      if (emailEl) emailEl.textContent = data.email;
      if (usernameValEl) usernameValEl.textContent = data.username;
      if (emailValEl) emailValEl.textContent = data.email;
      if (roleValEl) roleValEl.textContent = data.isAdmin ? 'Administrator' : 'Standard User';
      if (idValEl) idValEl.textContent = data.userId || 'N/A';
      if (avatarImgEl && data.avatarUrl) avatarImgEl.src = data.avatarUrl;
      if (statusTextEl) statusTextEl.textContent = data.status || 'Available';

      window._accountData = data;

      if (cooldownEl && data.usernameChangeCooldown) {
        const days = Math.ceil(data.usernameChangeCooldown / (24 * 60 * 60 * 1000));
        cooldownEl.textContent = `You can change your username again in ${days} day${days !== 1 ? 's' : ''}`;
        cooldownEl.style.display = 'block';
      } else if (cooldownEl) {
        cooldownEl.style.display = 'none';
      }
    } catch (err) {
      console.error('[account] Failed to load:', err);
    }

    window.scrollTo({ top: 0, behavior: 'smooth' });
  }

  function hideAccountPage() {
    if (!accountPage) return;

    if (window.location.hash === '#account') {
      history.pushState("", document.title, window.location.pathname + window.location.search);
    }

    accountPage.style.display = 'none';

    if (mainContent) mainContent.style.display = '';
    if (footerEl) footerEl.style.display = '';

    window.scrollTo({ top: 0, behavior: 'smooth' });
  }

  const avatarWrapper = document.getElementById('accountAvatarWrapper');
  const avatarFileInput = document.getElementById('avatarFileInput');

  avatarWrapper?.addEventListener('click', () => {
    avatarFileInput?.click();
  });

  avatarFileInput?.addEventListener('change', async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;

    if (file.size > 5 * 1024 * 1024) {
      alert('File too large. Maximum size is 5MB.');
      return;
    }

    const formData = new FormData();
    formData.append('avatar', file);

    try {
      const res = await fetch('/api/account/avatar/upload', {
        method: 'POST',
        credentials: 'include',
        body: formData
      });

      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Failed to upload avatar');

      const avatarImgEl = document.getElementById('accountAvatarImg');
      if (avatarImgEl) avatarImgEl.src = data.avatarUrl;

      const headerAvatarImg = document.querySelector('.account-btn-avatar');
      if (headerAvatarImg) headerAvatarImg.src = data.avatarUrl;
    } catch (err) {
      alert('Failed to upload avatar: ' + err.message);
    }

    e.target.value = '';
  });

  const usernameEditBtn = document.getElementById('usernameEditBtn');
  const usernameSaveBtn = document.getElementById('usernameSaveBtn');
  const usernameCancelBtn = document.getElementById('usernameCancelBtn');
  const usernameInput = document.getElementById('usernameInput');
  const usernameValEl = document.getElementById('accountUsernameValue');

  usernameEditBtn?.addEventListener('click', () => {
    if (usernameInput && usernameValEl) {
      usernameInput.value = usernameValEl.textContent || '';
      usernameValEl.style.display = 'none';
      usernameInput.style.display = 'block';
      usernameEditBtn.style.display = 'none';
      usernameSaveBtn.style.display = 'inline-flex';
      usernameCancelBtn.style.display = 'inline-flex';
      usernameInput.focus();
    }
  });

  usernameCancelBtn?.addEventListener('click', () => {
    usernameValEl.style.display = 'inline';
    usernameInput.style.display = 'none';
    usernameEditBtn.style.display = 'inline-flex';
    usernameSaveBtn.style.display = 'none';
    usernameCancelBtn.style.display = 'none';
  });

  usernameSaveBtn?.addEventListener('click', async () => {
    const newUsername = usernameInput?.value?.trim();
    if (!newUsername || newUsername.length < 3) {
      alert('Username must be at least 3 characters');
      return;
    }

    usernameSaveBtn.disabled = true;
    try {
      const res = await fetch('/api/account/username', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ username: newUsername })
      });

      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Failed to update username');

      if (usernameValEl) usernameValEl.textContent = newUsername;
      const usernameEl = document.getElementById('accountUsername');
      if (usernameEl && window._accountData) {
        const badgeClass = window._accountData.isAdmin ? 'admin' : 'user';
        const badgeText = window._accountData.isAdmin ? 'Admin' : 'User';
        usernameEl.innerHTML = `${escapeHtml(newUsername)} <span class="account-badge ${badgeClass}">${badgeText}</span>`;
      }

      usernameCancelBtn?.click();
    } catch (err) {
      alert(err.message);
    } finally {
      usernameSaveBtn.disabled = false;
    }
  });

  const statusEditBtn = document.getElementById('statusEditBtn');
  const statusModal = document.getElementById('statusModal');
  const statusModalClose = document.getElementById('statusModalClose');
  const statusCancelBtn = document.getElementById('statusCancelBtn');
  const statusSaveBtn = document.getElementById('statusSaveBtn');
  const statusInput = document.getElementById('statusInput');
  const statusDuration = document.getElementById('statusDuration');
  const statusError = document.getElementById('statusError');
  const statusTextEl = document.getElementById('accountStatusText');

  statusEditBtn?.addEventListener('click', () => {
    if (statusInput) statusInput.value = statusTextEl?.textContent || '';
    statusModal?.classList.add('show');
  });

  statusModalClose?.addEventListener('click', () => {
    statusModal?.classList.remove('show');
  });

  statusCancelBtn?.addEventListener('click', () => {
    statusModal?.classList.remove('show');
  });

  statusModal?.addEventListener('click', (e) => {
    if (e.target === statusModal) statusModal.classList.remove('show');
  });

  statusSaveBtn?.addEventListener('click', async () => {
    const text = statusInput?.value?.trim() || 'Available';
    const durationVal = statusDuration?.value;
    const expiresAt = durationVal === 'never' ? null : Date.now() + parseInt(durationVal || '86400000');

    statusSaveBtn.disabled = true;
    if (statusError) statusError.classList.remove('show');

    try {
      const res = await fetch('/api/me/status', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ text, expiresAt })
      });

      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Failed to update status');

      if (statusTextEl) statusTextEl.textContent = text;

      statusModal?.classList.remove('show');
    } catch (err) {
      if (statusError) {
        statusError.textContent = err.message;
        statusError.classList.add('show');
      }
    } finally {
      statusSaveBtn.disabled = false;
    }
  });

  accountBtn?.addEventListener('click', showAccountPage);
  accountBackBtn?.addEventListener('click', hideAccountPage);

  if (window.location.hash === '#account') {
    showAccountPage();
  }

  window.addEventListener('hashchange', () => {
    if (window.location.hash === '#account') {
      showAccountPage();
    } else {
      hideAccountPage();
    }
  });

  function activateAccountTab(tabName) {
    const normalized = tabName === 'credentials' ? 'credentials' : 'info';
    document.querySelectorAll('.account-tab').forEach((tab) => {
      tab.classList.toggle('active', tab.dataset.accountTab === normalized);
    });
    document.querySelectorAll('.account-tab-content').forEach((content) => {
      content.classList.remove('active');
    });
    const tabId = normalized === 'info' ? 'accountInfoTab' : 'accountCredentialsTab';
    document.getElementById(tabId)?.classList.add('active');
  }

  document.querySelectorAll('.account-tab').forEach(tab => {
    tab.addEventListener('click', () => {
      activateAccountTab(tab.dataset.accountTab);
    });
  });

  const twofaModal = document.getElementById('twofaModal');
  const change2faBtn = document.getElementById('change2faBtn');
  const twofaModalClose = document.getElementById('twofaModalClose');
  const twofaCancelBtn = document.getElementById('twofaCancelBtn');
  const twofaConfirmBtn = document.getElementById('twofaConfirmBtn');
  const currentCodeInput = document.getElementById('currentCodeInput');
  const newCodeInput = document.getElementById('newCodeInput');
  const qrCodeSection = document.getElementById('qrCodeSection');
  const confirmSection = document.getElementById('confirmSection');
  const newQrCode = document.getElementById('newQrCode');
  const newSecretDisplay = document.getElementById('newSecretDisplay');
  const twofaError = document.getElementById('twofaError');
  const twofaSuccess = document.getElementById('twofaSuccess');

  let pendingNewSecret = null;
  let twoFaModalStep = 1;

  function resetTwofaModal() {
    twoFaModalStep = 1;
    pendingNewSecret = null;
    if (currentCodeInput) currentCodeInput.value = '';
    if (newCodeInput) newCodeInput.value = '';
    if (qrCodeSection) qrCodeSection.style.display = 'none';
    if (confirmSection) confirmSection.style.display = 'none';
    if (twofaError) twofaError.classList.remove('show');
    if (twofaSuccess) twofaSuccess.classList.remove('show');
    if (twofaConfirmBtn) {
      twofaConfirmBtn.textContent = 'Verify Current Code';
      twofaConfirmBtn.disabled = false;
      twofaConfirmBtn.style.display = '';
    }
    if (twofaCancelBtn) twofaCancelBtn.textContent = 'Cancel';

    const twofaStep1 = document.getElementById('twofaStep1');
    if (twofaStep1) twofaStep1.style.display = '';
    const twofaForgotLink = document.getElementById('twofaForgotLink');
    if (twofaForgotLink) twofaForgotLink.style.display = '';

    const qrStepNum = qrCodeSection?.querySelector('.twofa-step-num');
    const confirmStepNum = confirmSection?.querySelector('.twofa-step-num');
    if (qrStepNum) qrStepNum.textContent = '2';
    if (confirmStepNum) confirmStepNum.textContent = '3';
  }

  function showTwofaError(msg) {
    if (twofaError) {
      twofaError.textContent = msg;
      twofaError.classList.add('show');
    }
  }

  function hideTwofaError() {
    if (twofaError) twofaError.classList.remove('show');
  }

  change2faBtn?.addEventListener('click', () => {
    resetTwofaModal();
    twofaModal?.classList.add('show');
  });

  twofaModalClose?.addEventListener('click', () => {
    twofaModal?.classList.remove('show');
  });

  twofaCancelBtn?.addEventListener('click', () => {
    twofaModal?.classList.remove('show');
  });

  twofaModal?.addEventListener('click', (e) => {
    if (e.target === twofaModal) twofaModal.classList.remove('show');
  });

  twofaConfirmBtn?.addEventListener('click', async () => {
    hideTwofaError();

    if (twoFaModalStep === 1) {
      const currentCode = currentCodeInput?.value.trim() || '';
      if (!currentCode || currentCode.length !== 6 || !/^\d+$/.test(currentCode)) {
        showTwofaError('Please enter a valid 6-digit code');
        return;
      }

      twofaConfirmBtn.disabled = true;
      twofaConfirmBtn.textContent = 'Verifying...';

      try {
        const res = await fetch('/api/account/2fa/generate', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          credentials: 'include',
          body: JSON.stringify({ currentCode })
        });

        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Verification failed');

        pendingNewSecret = data.newSecret;
        if (newQrCode) newQrCode.src = data.qrCodeUrl;
        if (newSecretDisplay) newSecretDisplay.textContent = data.newSecret;

        if (qrCodeSection) qrCodeSection.style.display = 'block';
        if (confirmSection) confirmSection.style.display = 'block';
        twoFaModalStep = 2;
        twofaConfirmBtn.textContent = 'Confirm New 2FA';
        twofaConfirmBtn.disabled = false;
      } catch (err) {
        showTwofaError(err.message);
        twofaConfirmBtn.textContent = 'Verify Current Code';
        twofaConfirmBtn.disabled = false;
      }
    } else if (twoFaModalStep === 2) {
      const newCode = newCodeInput?.value.trim() || '';
      if (!newCode || newCode.length !== 6 || !/^\d+$/.test(newCode)) {
        showTwofaError('Please enter a valid 6-digit code from your new authenticator');
        return;
      }

      twofaConfirmBtn.disabled = true;
      twofaConfirmBtn.textContent = 'Updating...';

      try {
        const res = await fetch('/api/account/2fa/confirm', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          credentials: 'include',
          body: JSON.stringify({ newCode, newSecret: pendingNewSecret })
        });

        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Failed to update 2FA');

        if (twofaSuccess) twofaSuccess.classList.add('show');
        twofaConfirmBtn.style.display = 'none';
        if (twofaCancelBtn) twofaCancelBtn.textContent = 'Done';

        setTimeout(() => {
          twofaModal?.classList.remove('show');
          resetTwofaModal();
        }, 2000);
      } catch (err) {
        showTwofaError(err.message);
        twofaConfirmBtn.textContent = 'Confirm New 2FA';
        twofaConfirmBtn.disabled = false;
      }
    }
  });

  [currentCodeInput, newCodeInput].forEach(input => {
    input?.addEventListener('input', (e) => {
      e.target.value = e.target.value.replace(/\D/g, '').slice(0, 6);
    });
  });

  const passwordModal = document.getElementById('passwordModal');
  const changePasswordBtn = document.getElementById('changePasswordBtn');
  const passwordModalClose = document.getElementById('passwordModalClose');
  const passwordCancelBtn = document.getElementById('passwordCancelBtn');
  const passwordConfirmBtn = document.getElementById('passwordConfirmBtn');
  const currentPasswordInput = document.getElementById('currentPasswordInput');
  const password2faInput = document.getElementById('password2faInput');
  const newPasswordInput = document.getElementById('newPasswordInput');
  const confirmPasswordInput = document.getElementById('confirmPasswordInput');
  const passwordStep1 = document.getElementById('passwordStep1');
  const passwordStep2 = document.getElementById('passwordStep2');
  const passwordError = document.getElementById('passwordError');
  const passwordSuccess = document.getElementById('passwordSuccess');

  let passwordModalStep = 1;

  function resetPasswordModal() {
    passwordModalStep = 1;
    if (currentPasswordInput) currentPasswordInput.value = '';
    if (password2faInput) password2faInput.value = '';
    if (newPasswordInput) newPasswordInput.value = '';
    if (confirmPasswordInput) confirmPasswordInput.value = '';
    if (passwordStep1) passwordStep1.style.display = 'block';
    if (passwordStep2) passwordStep2.style.display = 'none';
    if (passwordError) passwordError.classList.remove('show');
    if (passwordSuccess) passwordSuccess.classList.remove('show');
    if (passwordConfirmBtn) {
      passwordConfirmBtn.textContent = 'Verify';
      passwordConfirmBtn.disabled = false;
      passwordConfirmBtn.style.display = '';
    }
    if (passwordCancelBtn) passwordCancelBtn.textContent = 'Cancel';
  }

  function showPasswordError(msg) {
    if (passwordError) {
      passwordError.textContent = msg;
      passwordError.classList.add('show');
    }
  }

  function hidePasswordError() {
    if (passwordError) passwordError.classList.remove('show');
  }

  changePasswordBtn?.addEventListener('click', () => {
    resetPasswordModal();
    passwordModal?.classList.add('show');
  });

  passwordModalClose?.addEventListener('click', () => {
    passwordModal?.classList.remove('show');
  });

  passwordCancelBtn?.addEventListener('click', () => {
    passwordModal?.classList.remove('show');
  });

  passwordModal?.addEventListener('click', (e) => {
    if (e.target === passwordModal) passwordModal.classList.remove('show');
  });

  passwordConfirmBtn?.addEventListener('click', async () => {
    hidePasswordError();

    if (passwordModalStep === 1) {
      const currentPw = currentPasswordInput?.value || '';
      const code = password2faInput?.value.trim() || '';

      if (!currentPw) {
        showPasswordError('Please enter your current password');
        return;
      }
      if (!code || code.length !== 6 || !/^\d+$/.test(code)) {
        showPasswordError('Please enter a valid 6-digit 2FA code');
        return;
      }

      passwordConfirmBtn.disabled = true;
      passwordConfirmBtn.textContent = 'Verifying...';

      try {
        const res = await fetch('/api/account/password/verify', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          credentials: 'include',
          body: JSON.stringify({ currentPassword: currentPw, twoFactorCode: code })
        });

        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Verification failed');

        passwordStep1.style.display = 'none';
        passwordStep2.style.display = 'block';
        passwordModalStep = 2;
        passwordConfirmBtn.textContent = 'Change Password';
        passwordConfirmBtn.disabled = false;
      } catch (err) {
        showPasswordError(err.message);
        passwordConfirmBtn.textContent = 'Verify';
        passwordConfirmBtn.disabled = false;
      }
    } else if (passwordModalStep === 2) {
      const newPw = newPasswordInput?.value || '';
      const confirmPw = confirmPasswordInput?.value || '';

      if (!newPw || newPw.length < 8) {
        showPasswordError('Password must be at least 8 characters');
        return;
      }
      if (newPw !== confirmPw) {
        showPasswordError('Passwords do not match');
        return;
      }

      passwordConfirmBtn.disabled = true;
      passwordConfirmBtn.textContent = 'Updating...';

      try {
        const res = await fetch('/api/account/password/change', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          credentials: 'include',
          body: JSON.stringify({
            currentPassword: currentPasswordInput?.value || '',
            twoFactorCode: password2faInput?.value.trim() || '',
            newPassword: newPw
          })
        });

        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Failed to change password');

        if (passwordSuccess) passwordSuccess.classList.add('show');
        passwordConfirmBtn.style.display = 'none';
        if (passwordCancelBtn) passwordCancelBtn.textContent = 'Done';

        setTimeout(() => {
          passwordModal?.classList.remove('show');
          resetPasswordModal();
        }, 2000);
      } catch (err) {
        showPasswordError(err.message);
        passwordConfirmBtn.textContent = 'Change Password';
        passwordConfirmBtn.disabled = false;
      }
    }
  });

  password2faInput?.addEventListener('input', (e) => {
    e.target.value = e.target.value.replace(/\D/g, '').slice(0, 6);
  });

  document.querySelectorAll('.password-toggle-eye').forEach(btn => {
    const targetId = btn.dataset.target;
    const input = document.getElementById(targetId);

    if (input) {
      input.addEventListener('input', () => {
        btn.style.display = input.value.length > 0 ? 'block' : 'none';
      });

      btn.addEventListener('click', () => {
        const isPassword = input.type === 'password';
        input.type = isPassword ? 'text' : 'password';
        btn.querySelector('i').className = isPassword ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye';
      });
    }
  });

  const emailModal = document.getElementById('emailModal');
  const changeEmailBtn = document.getElementById('changeEmailBtn');
  const emailModalClose = document.getElementById('emailModalClose');
  const emailCancelBtn = document.getElementById('emailCancelBtn');
  const emailConfirmBtn = document.getElementById('emailConfirmBtn');
  const emailPasswordInput = document.getElementById('emailPasswordInput');
  const email2faInput = document.getElementById('email2faInput');
  const newEmailInput = document.getElementById('newEmailInput');
  const emailStep1 = document.getElementById('emailStep1');
  const emailStep2 = document.getElementById('emailStep2');
  const emailError = document.getElementById('emailError');
  const emailSuccess = document.getElementById('emailSuccess');

  let emailModalStep = 1;

  function resetEmailModal() {
    emailModalStep = 1;
    if (emailPasswordInput) emailPasswordInput.value = '';
    if (email2faInput) email2faInput.value = '';
    if (newEmailInput) newEmailInput.value = '';
    if (emailStep1) emailStep1.style.display = 'block';
    if (emailStep2) emailStep2.style.display = 'none';
    if (emailError) emailError.classList.remove('show');
    if (emailSuccess) emailSuccess.classList.remove('show');
    if (emailConfirmBtn) {
      emailConfirmBtn.textContent = 'Verify';
      emailConfirmBtn.disabled = false;
      emailConfirmBtn.style.display = '';
    }
    if (emailCancelBtn) emailCancelBtn.textContent = 'Cancel';
  }

  function showEmailError(msg) {
    if (emailError) {
      emailError.textContent = msg;
      emailError.classList.add('show');
    }
  }

  function hideEmailError() {
    if (emailError) emailError.classList.remove('show');
  }

  changeEmailBtn?.addEventListener('click', () => {
    resetEmailModal();
    emailModal?.classList.add('show');
  });

  emailModalClose?.addEventListener('click', () => {
    emailModal?.classList.remove('show');
  });

  emailCancelBtn?.addEventListener('click', () => {
    emailModal?.classList.remove('show');
  });

  emailModal?.addEventListener('click', (e) => {
    if (e.target === emailModal) emailModal.classList.remove('show');
  });

  emailConfirmBtn?.addEventListener('click', async () => {
    hideEmailError();

    if (emailModalStep === 1) {
      const currentPw = emailPasswordInput?.value || '';
      const code = email2faInput?.value.trim() || '';

      if (!currentPw) {
        showEmailError('Please enter your password');
        return;
      }
      if (!code || code.length !== 6 || !/^\d+$/.test(code)) {
        showEmailError('Please enter a valid 6-digit 2FA code');
        return;
      }

      emailConfirmBtn.disabled = true;
      emailConfirmBtn.textContent = 'Verifying...';

      try {
        const res = await fetch('/api/account/email/verify', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          credentials: 'include',
          body: JSON.stringify({ currentPassword: currentPw, twoFactorCode: code })
        });

        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Verification failed');

        emailStep1.style.display = 'none';
        emailStep2.style.display = 'block';
        emailModalStep = 2;
        emailConfirmBtn.textContent = 'Change Email';
        emailConfirmBtn.disabled = false;
      } catch (err) {
        showEmailError(err.message);
        emailConfirmBtn.textContent = 'Verify';
        emailConfirmBtn.disabled = false;
      }
    } else if (emailModalStep === 2) {
      const newEmail = newEmailInput?.value.trim() || '';

      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!newEmail || !emailRegex.test(newEmail)) {
        showEmailError('Please enter a valid email address');
        return;
      }

      emailConfirmBtn.disabled = true;
      emailConfirmBtn.textContent = 'Updating...';

      try {
        const res = await fetch('/api/account/email/change', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          credentials: 'include',
          body: JSON.stringify({
            currentPassword: emailPasswordInput?.value || '',
            twoFactorCode: email2faInput?.value.trim() || '',
            newEmail: newEmail
          })
        });

        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Failed to change email');

        if (emailSuccess) emailSuccess.classList.add('show');
        emailConfirmBtn.style.display = 'none';
        if (emailCancelBtn) emailCancelBtn.textContent = 'Done';

        const accountEmailEl = document.getElementById('accountEmail');
        const accountEmailValueEl = document.getElementById('accountEmailValue');
        if (accountEmailEl) accountEmailEl.textContent = newEmail;
        if (accountEmailValueEl) accountEmailValueEl.textContent = newEmail;

        setTimeout(() => {
          emailModal?.classList.remove('show');
          resetEmailModal();
        }, 2000);
      } catch (err) {
        showEmailError(err.message);
        emailConfirmBtn.textContent = 'Change Email';
        emailConfirmBtn.disabled = false;
      }
    }
  });

  email2faInput?.addEventListener('input', (e) => {
    e.target.value = e.target.value.replace(/\D/g, '').slice(0, 6);
  });

  document.querySelectorAll('.email-toggle-eye').forEach(btn => {
    const targetId = btn.dataset.target;
    const input = document.getElementById(targetId);

    if (input) {
      input.addEventListener('input', () => {
        btn.style.display = input.value.length > 0 ? 'block' : 'none';
      });

      btn.addEventListener('click', () => {
        const isPassword = input.type === 'password';
        input.type = isPassword ? 'text' : 'password';
        btn.querySelector('i').className = isPassword ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye';
      });
    }
  });

  const recoveryModal = document.getElementById('recoveryModal');
  const recoveryModalClose = document.getElementById('recoveryModalClose');
  const recoveryCancelBtn = document.getElementById('recoveryCancelBtn');
  const recoveryConfirmBtn = document.getElementById('recoveryConfirmBtn');
  const recoveryCodeInput = document.getElementById('recoveryCodeInput');
  const recoveryError = document.getElementById('recoveryError');

  let recoveryReturnTo = null;

  function showRecoveryError(msg) {
    if (recoveryError) {
      recoveryError.textContent = msg;
      recoveryError.classList.add('show');
    }
  }

  function hideRecoveryError() {
    if (recoveryError) recoveryError.classList.remove('show');
  }

  function resetRecoveryModal() {
    if (recoveryCodeInput) recoveryCodeInput.value = '';
    hideRecoveryError();
    if (recoveryConfirmBtn) {
      recoveryConfirmBtn.textContent = 'Verify Code';
      recoveryConfirmBtn.disabled = false;
    }
    recoveryReturnTo = null;
  }

  function openRecoveryModal(returnTo) {
    resetRecoveryModal();
    recoveryReturnTo = returnTo;
    recoveryModal?.classList.add('show');
  }

  async function openAccountFlow(flow) {
    await showAccountPage();
    activateAccountTab('credentials');

    switch (flow) {
      case 'change_password':
        resetPasswordModal();
        passwordModal?.classList.add('show');
        return true;
      case 'change_email':
        resetEmailModal();
        emailModal?.classList.add('show');
        return true;
      case 'change_2fa':
        resetTwofaModal();
        twofaModal?.classList.add('show');
        return true;
      case 'recover_password':
        openRecoveryModal('password');
        return true;
      case 'recover_email':
        openRecoveryModal('email');
        return true;
      case 'recover_2fa':
        openRecoveryModal('twofa');
        return true;
      default:
        return false;
    }
  }

  window.ADPanelDashboardAccount = {
    showAccountPage,
    hideAccountPage,
    activateAccountTab,
    openAccountFlow,
  };

  document.getElementById('twofaForgotLink')?.addEventListener('click', () => {
    twofaModal?.classList.remove('show');
    openRecoveryModal('twofa');
  });

  document.getElementById('passwordForgotLink')?.addEventListener('click', () => {
    passwordModal?.classList.remove('show');
    openRecoveryModal('password');
  });

  document.getElementById('emailForgotLink')?.addEventListener('click', () => {
    emailModal?.classList.remove('show');
    openRecoveryModal('email');
  });

  recoveryModalClose?.addEventListener('click', () => {
    recoveryModal?.classList.remove('show');
  });

  recoveryCancelBtn?.addEventListener('click', () => {
    recoveryModal?.classList.remove('show');
  });

  recoveryModal?.addEventListener('click', (e) => {
    if (e.target === recoveryModal) recoveryModal.classList.remove('show');
  });

  recoveryCodeInput?.addEventListener('input', (e) => {
    e.target.value = e.target.value.toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8);
  });

  recoveryConfirmBtn?.addEventListener('click', async () => {
    hideRecoveryError();

    const code = recoveryCodeInput?.value.trim().toUpperCase() || '';
    if (!code || code.length !== 8) {
      showRecoveryError('Please enter a valid 8-character recovery code');
      return;
    }

    recoveryConfirmBtn.disabled = true;
    recoveryConfirmBtn.textContent = 'Verifying...';

    try {
      const res = await fetch('/api/account/recovery/verify', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ recoveryCode: code })
      });

      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Invalid recovery code');

      recoveryModal?.classList.remove('show');

      if (recoveryReturnTo === 'twofa') {
        try {
          const genRes = await fetch('/api/account/2fa/generate-recovery', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include'
          });
          const genData = await genRes.json();
          if (!genRes.ok) throw new Error(genData.error || 'Failed to generate new 2FA');

          resetTwofaModal();
          pendingNewSecret = genData.newSecret;
          if (newQrCode) newQrCode.src = genData.qrCodeUrl;
          if (newSecretDisplay) newSecretDisplay.textContent = genData.newSecret;

          const twofaStep1 = document.getElementById('twofaStep1');
          if (twofaStep1) twofaStep1.style.display = 'none';

          if (qrCodeSection) qrCodeSection.style.display = 'block';
          if (confirmSection) confirmSection.style.display = 'block';

          const qrStepNum = qrCodeSection?.querySelector('.twofa-step-num');
          const confirmStepNum = confirmSection?.querySelector('.twofa-step-num');
          if (qrStepNum) qrStepNum.textContent = '1';
          if (confirmStepNum) confirmStepNum.textContent = '2';

          twoFaModalStep = 2;
          if (twofaConfirmBtn) twofaConfirmBtn.textContent = 'Confirm New 2FA';
          twofaModal?.classList.add('show');
        } catch (err) {
          alert('Failed to generate 2FA: ' + err.message);
        }
      } else if (recoveryReturnTo === 'password') {
        resetPasswordModal();
        if (passwordStep1) passwordStep1.style.display = 'none';
        if (passwordStep2) passwordStep2.style.display = 'block';
        document.getElementById('passwordForgotLink').style.display = 'none';
        passwordModalStep = 2;
        if (passwordConfirmBtn) passwordConfirmBtn.textContent = 'Change Password';
        passwordModal?.classList.add('show');
      } else if (recoveryReturnTo === 'email') {
        resetEmailModal();
        if (emailStep1) emailStep1.style.display = 'none';
        if (emailStep2) emailStep2.style.display = 'block';
        document.getElementById('emailForgotLink').style.display = 'none';
        emailModalStep = 2;
        if (emailConfirmBtn) emailConfirmBtn.textContent = 'Change Email';
        emailModal?.classList.add('show');
      }
    } catch (err) {
      showRecoveryError(err.message);
      recoveryConfirmBtn.textContent = 'Verify Code';
      recoveryConfirmBtn.disabled = false;
    }
  });

  const createBtns = [
    document.getElementById('createBtn'),
    document.getElementById('createBtnHero'),
    document.getElementById('createBtnFooter')
  ].filter(Boolean);
  const createModal = document.getElementById('createModal');
  const createClose = document.getElementById('createClose');
  const createCancel = document.getElementById('createCancel');
  const createConfirm = document.getElementById('createConfirm');
  const createSpinner = document.getElementById('createSpinner');
  const serverNameEl = document.getElementById('serverName');
  const templateGrid = document.getElementById('templateGrid');
  const templateInfo = document.getElementById('templateInfo');
  const nodeGrid = document.getElementById('nodeGrid');
  const summaryNameEl = document.getElementById('summaryServerName');
  const summaryTemplateEl = document.getElementById('summaryTemplateName');
  const summaryNodeEl = document.getElementById('summaryNodeName');
  const templateInfo2 = document.getElementById('templateInfo2');
  const forkModal = document.getElementById('forkModal');
  const forkClose = document.getElementById('forkClose');
  const forkBack = document.getElementById('forkBack');
  const forkConfirm = document.getElementById('forkConfirm');
  const forkSpinner = document.getElementById('forkSpinner');
  const forkGrid = document.getElementById('forkGrid');
  const forkInfo = document.getElementById('forkInfo');
  const portModal = document.getElementById('portModal');
  const portClose = document.getElementById('portClose');
  const portBack = document.getElementById('portBack');
  const portConfirm = document.getElementById('portConfirm');
  const portSpinner = document.getElementById('portSpinner');
  const serverIpEl = document.getElementById('serverIp');
  const serverPortEl = document.getElementById('serverPort');
  const serverPortPreviewEl = document.getElementById('serverPortPreview');
  const portDefaultValueEl = document.getElementById('portDefaultValue');
  const portTitleTextEl = document.getElementById('portTitleText');
  const portSubtitleEl = document.getElementById('portSubtitle');
  const resourcesModal = document.getElementById('resourcesModal');
  const resourcesClose = document.getElementById('resourcesClose');
  const resourcesBack = document.getElementById('resourcesBack');
  const resourcesConfirm = document.getElementById('resourcesConfirm');
  const resourcesSpinner = document.getElementById('resourcesSpinner');
  const resourceRamEl = document.getElementById('resourceRam');
  const resourceCpuCoresEl = document.getElementById('resourceCpuCores');
  const resourceSwapEl = document.getElementById('resourceSwap');
  const resourceStorageEl = document.getElementById('resourceStorage');
  const resourceBackupsEl = document.getElementById('resourceBackups');
  const resourceMaxSchedulesEl = document.getElementById('resourceMaxSchedules');
  const resourceIoWeightEl = document.getElementById('resourceIoWeight');
  const resourceCpuWeightEl = document.getElementById('resourceCpuWeight');
  const resourcePidsLimitEl = document.getElementById('resourcePidsLimit');
  const resourceFileLimitEl = document.getElementById('resourceFileLimit');
  const nodeRamLimitEl = document.getElementById('nodeRamLimit');
  const nodeCpuLimitEl = document.getElementById('nodeCpuLimit');
  const nodeStorageLimitEl = document.getElementById('nodeStorageLimit');
  const liveNodeCapacityCache = new Map();

  function resolveNodeCacheKey(node) {
    return String(node?.id || node?.uuid || node?.name || '').trim();
  }

  function configuredNodeCapacity(node) {
    return {
      ramMb: Math.max(0, Math.trunc(Number(node?.ram_mb || node?.buildConfig?.ram_mb || 0))),
      cpuCores: Math.max(0, Number(node?.cpu_cores || node?.buildConfig?.cpu_cores || node?.buildConfig?.cpuCores || 0)),
      diskGb: Math.max(0, Number(node?.disk_gb || node?.buildConfig?.disk_gb || node?.buildConfig?.diskGb || 0))
    };
  }

  function mergeNodeCapacityValue(configured, live, round) {
    const cfg = Number(configured);
    const detected = Number(live);
    let value = 0;
    if (Number.isFinite(cfg) && cfg > 0 && Number.isFinite(detected) && detected > 0) value = Math.min(cfg, detected);
    else if (Number.isFinite(detected) && detected > 0) value = detected;
    else if (Number.isFinite(cfg) && cfg > 0) value = cfg;
    return round ? Math.trunc(value) : value;
  }

  function resolveEffectiveNodeCapacity(node, liveCapacity) {
    const configured = configuredNodeCapacity(node);
    const live = (liveCapacity && typeof liveCapacity === 'object') ? liveCapacity : {};
    return {
      ramMb: mergeNodeCapacityValue(configured.ramMb, live.ramMb, true),
      cpuCores: mergeNodeCapacityValue(configured.cpuCores, live.cpuCores, false),
      diskGb: mergeNodeCapacityValue(configured.diskGb, live.diskGb, false)
    };
  }

  function applyNodeCapacityToResourceModal(capacity) {
    const ramMb = Number(capacity?.ramMb || 0);
    const cpuCores = Number(capacity?.cpuCores || 0);
    const diskGb = Number(capacity?.diskGb || 0);
    const diskMb = diskGb > 0 ? Math.trunc(diskGb * 1024) : 0;

    if (nodeRamLimitEl) nodeRamLimitEl.textContent = ramMb > 0 ? `${ramMb.toLocaleString()} MB` : 'Unlimited';
    if (nodeCpuLimitEl) nodeCpuLimitEl.textContent = cpuCores > 0 ? `${cpuCores} cores` : 'Unlimited';
    if (nodeStorageLimitEl) nodeStorageLimitEl.textContent = diskMb > 0 ? (diskMb >= 1010 ? `${(diskMb / 1024).toFixed(2).replace(/\.?0+$/, '')} GB` : `${diskMb.toLocaleString()} MB`) : 'Unlimited';

    if (resourceRamEl) {
      if (ramMb > 0) resourceRamEl.max = ramMb;
      else resourceRamEl.removeAttribute('max');
    }
    if (resourceCpuCoresEl) {
      if (cpuCores > 0) resourceCpuCoresEl.max = cpuCores;
      else resourceCpuCoresEl.removeAttribute('max');
    }
    if (resourceStorageEl) {
      if (diskMb > 0) resourceStorageEl.max = diskMb;
      else resourceStorageEl.removeAttribute('max');
    }
  }

  async function fetchLiveNodeCapacity(node) {
    const cacheKey = resolveNodeCacheKey(node);
    if (!cacheKey) return null;
    if (liveNodeCapacityCache.has(cacheKey)) return liveNodeCapacityCache.get(cacheKey);

    try {
      const res = await fetch(`/api/admin/nodes/${encodeURIComponent(cacheKey)}/stats`, { cache: 'no-store' });
      const data = await res.json().catch(() => ({}));
      if (!res.ok || !data.ok || !data.online || !data.stats) return null;

      const liveCapacity = {
        ramMb: Math.max(0, Math.trunc(Number(data.stats?.ram?.totalMb || 0))),
        cpuCores: Math.max(0, Number(data.stats?.cpu?.cores || 0)),
        diskGb: Math.max(0, Number(data.stats?.disk?.totalGb || 0))
      };
      liveNodeCapacityCache.set(cacheKey, liveCapacity);
      return liveCapacity;
    } catch {
      return null;
    }
  }

  async function getEffectiveNodeCapacity(node) {
    const liveCapacity = await fetchLiveNodeCapacity(node);
    return resolveEffectiveNodeCapacity(node, liveCapacity);
  }

  if (resourceStorageEl) {
    resourceStorageEl.addEventListener('input', () => {
      const hint = resourceStorageEl.closest('.enterprise-field')?.querySelector('.enterprise-hint');
      if (!hint) return;
      const val = parseInt(resourceStorageEl.value, 10);
      if (!isNaN(val) && val > 0) {
        const display = val >= 1010 ? (val / 1024).toFixed(2).replace(/\.?0+$/, '') + ' GB' : val + ' MB';
        hint.textContent = `Maximum disk space: ${display}`;
      } else {
        hint.textContent = 'Maximum disk space. Leave empty to use the node allocation.';
      }
    });
  }

  function readOptionalIntegerRange(input, label, min, max) {
    const raw = input && input.value != null ? String(input.value).trim() : '';
    if (!raw) return null;
    if (!/^\d+$/.test(raw)) {
      throw new Error(`${label} must be a whole number.`);
    }
    const value = Number(raw);
    if (!Number.isSafeInteger(value) || value < min || value > max) {
      throw new Error(`${label} must be between ${min} and ${max}.`);
    }
    return value;
  }

  const startupModal = document.getElementById('startupModal');
  const startupClose = document.getElementById('startupClose');
  const startupBack = document.getElementById('startupBack');
  const startupConfirm = document.getElementById('startupConfirm');
  const startupSpinner = document.getElementById('startupSpinner');
  const startupCommandEl = document.getElementById('startupCommand');
  const startupCommandSourceEl = document.getElementById('startupCommandSource');
  const startupCommandErrorEl = document.getElementById('startupCommandError');

  const importModal = document.getElementById('importModal');
  const importClose = document.getElementById('importClose');
  const importBack = document.getElementById('importBack');
  const importSkip = document.getElementById('importSkip');
  const importConfirm = document.getElementById('importConfirm');
  const importSpinner = document.getElementById('importSpinner');
  const importUrlEl = document.getElementById('importUrl');
  const importProgressContainer = document.getElementById('importProgressContainer');
  const importProgressFill = document.getElementById('importProgressFill');
  const importPercent = document.getElementById('importPercent');
  const importDownloaded = document.getElementById('importDownloaded');
  const importTotal = document.getElementById('importTotal');

  const MC_VERSION = "1.21.8";
  const MC_FORKS = [
    { id: "paper", name: "Paper", desc: "Optimized performance with plugin support" },
    { id: "pufferfish", name: "Pufferfish", desc: "TPS-focused optimizations" },
    { id: "vanilla", name: "Vanilla", desc: "Standard Minecraft experience" },
    { id: "purpur", name: "Purpur", desc: "Paper fork with extra settings" }
  ];

  let templates = [];
  let nodes = [];
  let selectedTemplateId = null;
  let selectedForkId = null;
  let selectedNodeId = "local";
  let portBackTarget = "create";
  let selectedStartupCommand = "";
  let selectedImportUrl = "";

  const BUILTIN_TEMPLATES = ['minecraft', 'nodejs'];
  const MAX_SERVER_NAME_LENGTH = 40;

  function isBuiltinTemplate(templateId) {
    return BUILTIN_TEMPLATES.includes((templateId || '').toLowerCase());
  }

  function sanitizeName(s) {
    s = (s || '').trim();
    s = s.replace(/\s+/g, '-').replace(/[^a-zA-Z0-9\-_]/g, '');
    return s.slice(0, MAX_SERVER_NAME_LENGTH) || '';
  }

  function getTemplateMeta(t) {
    const id = ((t && t.id) || '').toString().toLowerCase();
    if (id.includes('minecraft') || id === 'mc') {
      return { icon: 'fa-solid fa-cube', badge: 'Minecraft' };
    }
    if (id.includes('node') || id.includes('discord')) {
      return { icon: 'fa-brands fa-node-js', badge: 'Node.js' };
    }
    if (id.includes('python') || id.includes('py')) {
      return { icon: 'fa-brands fa-python', badge: 'Python' };
    }
    if (id.includes('proxy') || id.includes('web') || id.includes('http')) {
      return { icon: 'fa-solid fa-cloud', badge: 'Web' };
    }
    return { icon: 'fa-solid fa-server', badge: 'Generic' };
  }

  function getDefaultPortForTemplate(templateId) {
    const id = (templateId || '').toString().toLowerCase();
    if (!id) return 25565;
    if (id.includes('minecraft') || id === 'mc') return 25565;
    if (id.includes('node') || id.includes('discord')) return 3000;
    if (id.includes('http') || id.includes('web') || id.includes('proxy')) return 80;
    return 8080;
  }

  function coercePositiveNumber(value) {
    const numeric = Number(value);
    return Number.isFinite(numeric) && numeric > 0 ? numeric : 0;
  }

  function sanitizePortInputValue(value) {
    return String(value ?? '').replace(/[^\d]/g, '').slice(0, 5);
  }

  function parseStrictPortValue(value) {
    const str = String(value ?? '').trim();
    if (!/^[1-9]\d{0,4}$/.test(str)) return null;
    const port = Number(str);
    if (!Number.isInteger(port) || port < 1 || port > 65535) return null;
    return port;
  }

  function bindStrictPortInput(input) {
    if (!input) return;
    input.setAttribute('inputmode', 'numeric');
    input.addEventListener('input', () => {
      const sanitized = sanitizePortInputValue(input.value);
      if (input.value !== sanitized) input.value = sanitized;
    });
  }

  function getDefaultStartupVolumes(templateId, docker) {
    const configured = Array.isArray(docker?.volumes)
      ? docker.volumes.map((entry) => String(entry || '').trim()).filter(Boolean)
      : [];
    if (configured.length) return configured;
    const normalizedTemplateId = String(templateId || '').trim().toLowerCase();
    if (['nodejs', 'discord-bot', 'python', 'runtime'].includes(normalizedTemplateId)) {
      return ['{DATA_DIR}:/app'];
    }
    return ['{DATA_DIR}:/data'];
  }

  function getDefaultStartFileForTemplate(templateId) {
    const normalizedTemplateId = String(templateId || '').trim().toLowerCase();
    if (normalizedTemplateId === 'minecraft') return 'server.jar';
    if (normalizedTemplateId === 'python') return 'main.py';
    if (normalizedTemplateId === 'nodejs' || normalizedTemplateId === 'discord-bot') return 'index.js';
    return '';
  }

  function getDefaultContainerDataDir(templateId, docker) {
    const volumes = getDefaultStartupVolumes(templateId, docker);
    const first = String(volumes[0] || '').trim();
    if (first.includes(':')) {
      const parts = first.split(':');
      if (parts[1]) return parts[1].trim();
    }
    const normalizedTemplateId = String(templateId || '').trim().toLowerCase();
    return ['nodejs', 'discord-bot', 'python', 'runtime'].includes(normalizedTemplateId) ? '/app' : '/data';
  }

  function getDefaultRuntimeCommand(templateId, docker) {
    const explicit = String(docker?.command || '').trim();
    if (explicit) return explicit;
    const normalizedTemplateId = String(templateId || '').trim().toLowerCase();
    const dataDir = getDefaultContainerDataDir(normalizedTemplateId, docker);
    const startFile = getDefaultStartFileForTemplate(normalizedTemplateId);
    if (normalizedTemplateId === 'minecraft') {
      return `java -Xms128M -Xmx{RAM_MB}M -jar ${dataDir}/${startFile || 'server.jar'} nogui`;
    }
    if (normalizedTemplateId === 'python') {
      return `python ${dataDir}/${startFile || 'main.py'}`;
    }
    if (normalizedTemplateId === 'nodejs' || normalizedTemplateId === 'discord-bot') {
      return `node ${dataDir}/${startFile || 'index.js'}`;
    }
    return '';
  }

  const RUNTIME_PROCESS_BLOCKED_EXECUTABLES = new Set([
    'sh', '/bin/sh', 'bash', '/bin/bash', 'ash', '/bin/ash', 'dash', '/bin/dash', 'zsh', '/bin/zsh',
  ]);
  const RUNTIME_PROCESS_BLOCKED_CONTAINER_EXECUTABLES = new Set([
    'docker', 'docker-compose', 'podman', 'podman-compose', 'nerdctl', 'ctr',
  ]);
  const RUNTIME_PROCESS_ENV_WRAPPERS = new Set(['env']);
  const RUNTIME_PROCESS_BLOCKED_TOKENS = new Set([
    '&&', '||', '|', ';', '&', '>', '>>', '<', '<<', '2>', '2>>', '2>&1', '|&',
  ]);

  function parseRuntimeProcessArgsInput(value) {
    const input = String(value || '');
    const args = [];
    let current = '';
    let quote = '';
    let escape = false;
    for (let i = 0; i < input.length; i += 1) {
      const ch = input[i];
      if (escape) {
        current += ch;
        escape = false;
        continue;
      }
      if (ch === '\\') {
        escape = true;
        continue;
      }
      if (quote) {
        if (ch === quote) quote = '';
        else current += ch;
        continue;
      }
      if (ch === '"' || ch === "'") {
        quote = ch;
        continue;
      }
      if (/\s/.test(ch)) {
        if (current) {
          args.push(current);
          current = '';
        }
        continue;
      }
      current += ch;
    }
    if (current) args.push(current);
    return args;
  }

  function getRuntimeProcessExecutableBaseNameInput(value) {
    const raw = String(value || '').trim();
    if (!raw) return '';
    let candidate = raw;
    if (/\s/.test(candidate)) {
      const nestedArgs = parseRuntimeProcessArgsInput(candidate);
      if (nestedArgs.length) candidate = String(nestedArgs[0] || '').trim();
    }
    const normalized = candidate.toLowerCase().replace(/\\/g, '/');
    const parts = normalized.split('/').filter(Boolean);
    return parts.pop() || normalized;
  }

  function isRuntimeEnvAssignmentTokenInput(value) {
    const token = String(value || '').trim();
    if (!token || token.startsWith('-')) return false;
    const eqIdx = token.indexOf('=');
    if (eqIdx <= 0) return false;
    return /^[A-Za-z_][A-Za-z0-9_]*$/.test(token.slice(0, eqIdx));
  }

  function resolveRuntimeProcessExecutableArgsInput(args) {
    const parts = Array.isArray(args) ? args : [];
    if (!parts.length) return { execName: '', index: -1 };
    let index = 0;
    while (index < parts.length) {
      const token = String(parts[index] || '').trim();
      if (!token) {
        index += 1;
        continue;
      }
      const execName = getRuntimeProcessExecutableBaseNameInput(token);
      if (RUNTIME_PROCESS_ENV_WRAPPERS.has(execName)) {
        index += 1;
        while (index < parts.length) {
          const envToken = String(parts[index] || '').trim();
          if (!envToken) {
            index += 1;
            continue;
          }
          if (envToken === '--') {
            index += 1;
            break;
          }
          if (envToken.startsWith('-') || isRuntimeEnvAssignmentTokenInput(envToken)) {
            index += 1;
            continue;
          }
          break;
        }
        continue;
      }
      return { execName, index };
    }
    return { execName: '', index: -1 };
  }

  function validateRuntimeContainerLauncherInput(args) {
    const { execName } = resolveRuntimeProcessExecutableArgsInput(args);
    if (RUNTIME_PROCESS_BLOCKED_CONTAINER_EXECUTABLES.has(execName)) {
      return 'Container runtime CLI commands like "docker run" are not allowed here. Configure image, ports, volumes, env, and the in-container process separately.';
    }
    return null;
  }

  function isBlockedRuntimeShellExecutableInput(value) {
    const execName = getRuntimeProcessExecutableBaseNameInput(value);
    return RUNTIME_PROCESS_BLOCKED_EXECUTABLES.has(execName);
  }

  function validateRuntimeProcessCommandInput(value) {
    const command = String(value || '').trim();
    if (!command) return null;
    if (command.length > 4000) return 'Process command is too long.';
    if (command.includes('\n')) return 'Process command must be a single line.';
    if (command.includes('`') || command.includes('$(')) {
      return 'Shell expansions are not allowed in process commands.';
    }
    const args = parseRuntimeProcessArgsInput(command);
    if (!args.length) return 'Process command is empty.';
    const { execName: resolvedExecName } = resolveRuntimeProcessExecutableArgsInput(args);
    const containerRuntimeError = validateRuntimeContainerLauncherInput(args);
    if (containerRuntimeError) return containerRuntimeError;
    if (isBlockedRuntimeShellExecutableInput(resolvedExecName || args[0])) {
      return 'Shell wrappers like sh -c or bash -lc are not allowed. Provide the executable and arguments directly.';
    }
    for (const arg of args) {
      const token = String(arg || '').trim();
      if (RUNTIME_PROCESS_BLOCKED_TOKENS.has(token)) {
        return `Shell control operator "${token}" is not allowed in process commands.`;
      }
    }
    return null;
  }

  function buildDefaultStartupCommandForTemplate(template) {
    if (!template || !template.docker) return '';
    return getDefaultRuntimeCommand(template.id, template.docker);
  }

  function setStartupCommandSource(message) {
    if (!startupCommandSourceEl) return;
    startupCommandSourceEl.textContent = String(message || 'Using startup command from template.').trim();
  }

  function setStartupCommandValidationError(message) {
    const text = String(message || '').trim();
    if (startupCommandEl) startupCommandEl.setAttribute('aria-invalid', text ? 'true' : 'false');
    if (!startupCommandErrorEl) return;
    startupCommandErrorEl.textContent = text;
    startupCommandErrorEl.hidden = !text;
  }

  function findTemplateById(templateId) {
    const id = String(templateId || '').trim();
    if (!id) return null;
    return (templates || []).find(t => String(t?.id || '').trim() === id) || null;
  }

  function startupCommandSourceLabel(source, imageRef) {
    const normalized = String(source || '').trim().toLowerCase();
    if (normalized === 'image-inspect') {
      return imageRef
        ? `Detected from Docker image inspect (${imageRef}).`
        : 'Detected from Docker image inspect.';
    }
    if (normalized === 'template-command') return 'Using process command defined by this template.';
    if (normalized === 'template-default') return 'Using template default process command.';
    return 'Using auto-detected process command.';
  }

  function quoteStartupArg(arg) {
    const value = String(arg || '');
    if (!value) return '""';
    if (!/[\s"'\\]/.test(value)) return value;
    return `"${value.replace(/(["\\])/g, '\\$1')}"`;
  }

  function joinStartupArgs(args) {
    if (!Array.isArray(args)) return '';
    return args
      .map((item) => String(item || '').trim())
      .filter(Boolean)
      .map(quoteStartupArg)
      .join(' ')
      .trim();
  }

  function isShellEntrypoint(entrypoint) {
    if (!Array.isArray(entrypoint) || entrypoint.length < 2) return false;
    const first = String(entrypoint[0] || '').trim().toLowerCase();
    const base = first.split('/').filter(Boolean).pop() || first;
    const shellLike = ['sh', 'bash', 'ash', 'dash', 'zsh', 'ksh'].includes(base);
    if (!shellLike) return false;
    const flags = String(entrypoint[1] || '').trim();
    return flags.startsWith('-') && flags.includes('c');
  }

  function isLikelyPathOnlyStartup(command) {
    const args = parseRuntimeProcessArgsInput(String(command || '').trim());
    if (args.length !== 1) return false;
    const token = String(args[0] || '').trim();
    if (!token.startsWith('/')) return false;
    const lower = token.toLowerCase();
    if (lower.includes('entrypoint')) return true;
    if (lower.endsWith('.sh') || lower.endsWith('.bash')) return true;
    return false;
  }

  function pickInspectEnvStartupCommand(envObj) {
    if (!envObj || typeof envObj !== 'object' || Array.isArray(envObj)) return '';
    const env = {};
    Object.entries(envObj).forEach(([k, v]) => {
      env[String(k || '').trim().toUpperCase()] = String(v ?? '').trim();
    });
    const candidates = [
      'STARTUP', 'SERVER_START_CMD', 'START_CMD', 'START_COMMAND',
      'RUN_CMD', 'APP_START_CMD', 'LAUNCH_CMD', 'COMMAND', 'CMD'
    ];
    for (const key of candidates) {
      const val = String(env[key] || '').trim();
      if (!val) continue;
      if (val.includes('${') || val.includes('{{')) continue;
      if (val.startsWith('$')) continue;
      return val;
    }
    return '';
  }

  function inferStartupFromInspectMeta(template, inspectMeta, fallbackCommand, options = {}) {
    const allowUnsafe = !!options.allowUnsafe;

    function chooseCandidate(candidate) {
      const value = String(candidate || '').trim();
      if (!value) return '';
      if (allowUnsafe) return value;
      const err = validateRuntimeProcessCommandInput(value);
      if (err) return '';
      if (isLikelyPathOnlyStartup(value)) return '';
      return value;
    }

    const entrypoint = Array.isArray(inspectMeta?.entrypoint)
      ? inspectMeta.entrypoint.map((item) => String(item || '').trim()).filter(Boolean)
      : [];
    const cmd = Array.isArray(inspectMeta?.cmd)
      ? inspectMeta.cmd.map((item) => String(item || '').trim()).filter(Boolean)
      : [];
    const workingDir = String(inspectMeta?.workingDir || '').trim() || '/';

    const envStartup = pickInspectEnvStartupCommand(inspectMeta?.env);
    if (envStartup) {
      const candidate = chooseCandidate(envStartup);
      if (candidate) return candidate;
    }

    if (isShellEntrypoint(entrypoint)) {
      if (cmd.length) {
        const shellCmd = cmd.join(' ').trim();
        const candidate = chooseCandidate(shellCmd);
        if (candidate) return candidate;
      }
      if (entrypoint.length > 2) {
        const inlineShell = entrypoint.slice(2).join(' ').trim();
        const candidate = chooseCandidate(inlineShell);
        if (candidate) return candidate;
      }
    }

    const assembled = joinStartupArgs([...(entrypoint || []), ...(cmd || [])]);
    const assembledCandidate = chooseCandidate(assembled);
    if (assembledCandidate) return assembledCandidate;

    const templateId = String(template?.id || '').trim().toLowerCase();
    const imageName = String(template?.docker?.image || '').trim().toLowerCase();
    const startFile = getDefaultStartFileForTemplate(templateId);
    const normalizedDir = workingDir === '/' ? '/app' : workingDir;

    let heuristic = '';
    if (templateId.includes('node') || imageName.includes('node')) {
      heuristic = `node ${normalizedDir}/${startFile || 'index.js'}`;
    } else if (templateId.includes('python') || imageName.includes('python')) {
      heuristic = `python ${normalizedDir}/${startFile || 'main.py'}`;
    } else if (templateId.includes('minecraft') || imageName.includes('minecraft') || imageName.includes('java')) {
      heuristic = 'java -Xms128M -Xmx{RAM_MB}M -jar /data/server.jar nogui';
    }

    if (heuristic) {
      const candidate = chooseCandidate(heuristic);
      if (candidate) return candidate;
    }

    const fallback = String(fallbackCommand || '').trim();
    const fallbackCandidate = chooseCandidate(fallback);
    if (fallbackCandidate) return fallbackCandidate;

    if (allowUnsafe) {
      if (assembled) return assembled;
      if (entrypoint.length) return joinStartupArgs(entrypoint);
      if (cmd.length) return cmd.join(' ').trim();
      if (fallback) return fallback;
    }

    return '';
  }

  function isWrapperEntrypointOnlyCommand(command, inspectMeta) {
    if (!isLikelyPathOnlyStartup(command)) return false;
    const cmd = Array.isArray(inspectMeta?.cmd)
      ? inspectMeta.cmd.map((item) => String(item || '').trim()).filter(Boolean)
      : [];
    return cmd.length === 0;
  }

  function startupCommandCacheKey(templateId, nodeId) {
    return `${String(templateId || '').trim().toLowerCase()}::${String(nodeId || '').trim().toLowerCase()}`;
  }

  async function resolveStartupCommandForSelection(options = {}) {
    const templateId = String(options.templateId ?? selectedTemplateId ?? '').trim();
    setStartupCommandValidationError('');

    const template = findTemplateById(templateId);
    if (!template) {
      selectedStartupCommand = '';
      if (startupCommandEl && startupModal?.classList.contains('show')) startupCommandEl.value = '';
      setStartupCommandSource('No template selected yet.');
      return '';
    }

    const templateDefinedCommand = String(template?.docker?.command || '').trim();
    const fallbackCommand = buildDefaultStartupCommandForTemplate(template);
    const candidate = String(templateDefinedCommand || fallbackCommand || '').trim();
    const commandError = validateRuntimeProcessCommandInput(candidate);

    selectedStartupCommand = commandError ? '' : candidate;
    if (startupCommandEl && startupModal?.classList.contains('show')) {
      startupCommandEl.value = selectedStartupCommand;
    }

    if (selectedStartupCommand) {
      setStartupCommandSource(templateDefinedCommand
        ? 'Using startup command defined in this template.'
        : 'Using default startup command for this template.');
    } else {
      setStartupCommandSource(commandError
        ? `Template startup command is invalid (${commandError}). Set the process command manually.`
        : 'Set the process command manually.');
    }

    return selectedStartupCommand;
  }

  function openCreate() {
    createModal.classList.add('show');
    createModal.setAttribute('aria-hidden', 'false');

    selectedTemplateId = null;
    selectedForkId = null;
    selectedNodeId = 'local';
    portBackTarget = 'create';
    selectedStartupCommand = '';
    selectedImportUrl = '';

    if (serverNameEl) serverNameEl.value = '';
    if (templateGrid) templateGrid.innerHTML = '<div class="enterprise-hint">Loading templates…</div>';
    if (templateInfo) templateInfo.textContent = 'No template chosen';

    if (nodeGrid) nodeGrid.innerHTML = '<div class="enterprise-hint">Loading nodes…</div>';

    if (summaryNameEl) summaryNameEl.textContent = '—';
    if (summaryTemplateEl) summaryTemplateEl.textContent = '—';
    if (summaryNodeEl) summaryNodeEl.textContent = 'Local node';
    if (templateInfo2) templateInfo2.textContent = '';
    setStartupCommandSource('Using startup command from template.');

    fetchTemplates();
    fetchNodes();
  }

  function closeCreate() {
    createModal.classList.remove('show');
    createModal.setAttribute('aria-hidden', 'true');
  }

  window.ADPanelDashboardCreate = {
    openCreateModal() {
      openCreate();
      return true;
    },
    closeCreateModal() {
      closeCreate();
      return true;
    },
  };

  createBtns.forEach(btn => btn.addEventListener('click', openCreate));
  createClose?.addEventListener('click', closeCreate);
  createCancel?.addEventListener('click', closeCreate);
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && createModal?.classList.contains('show')) closeCreate();
  });

  async function fetchTemplates() {
    try {
      const r = await fetch('/api/templates');
      if (!r.ok) throw new Error('Failed to load templates');
      const data = await r.json();
      templates = (data && data.templates) || [];
    } catch (e) {
      templates = [
        { id: 'minecraft', name: 'Minecraft', description: 'Minecraft server (fork selection in the next step).' },
        { id: 'discord-bot', name: 'Discord Bot', description: 'Node 20 runtime (mount /app).' },
        { id: 'vanilla', name: 'Empty', description: 'Alpine base (sleep).' },
      ];
    }
    renderTemplates();
  }

  async function fetchNodes() {
    try {
      const r = await fetch('/api/nodes');
      const data = await r.json();
      nodes = (data && data.nodes) ? data.nodes : [];
    } catch (e) {
      nodes = [];
    }
    renderNodes();
  }

  function renderTemplates() {
    if (!templateGrid) return;
    templateGrid.innerHTML = '';

    if (!templates.length) {
      const empty = document.createElement('div');
      empty.className = 'enterprise-hint';
      empty.textContent = 'No templates available.';
      templateGrid.appendChild(empty);
      return;
    }

    templates.forEach(t => {
      const card = document.createElement('button');
      card.type = 'button';
      card.className = 'enterprise-template-item';
      card.setAttribute('role', 'option');
      card.setAttribute('aria-selected', 'false');

      const meta = getTemplateMeta(t);

      card.innerHTML = `
        <div class="enterprise-template-icon">
          <i class="${escapeHtml(meta.icon)}"></i>
        </div>
        <div class="enterprise-template-name">${escapeHtml(t.name)}</div>
      `;
      card.addEventListener('click', () => {
        selectedTemplateId = t.id;
        selectedStartupCommand = buildDefaultStartupCommandForTemplate(t) || '';
        [...templateGrid.children].forEach(c => {
          c.classList.remove('selected');
          c.setAttribute('aria-selected', 'false');
        });
        card.classList.add('selected');
        card.setAttribute('aria-selected', 'true');
        if (templateInfo) templateInfo.textContent = t.name;
        if (summaryTemplateEl) summaryTemplateEl.textContent = t.name;
        if (templateInfo2) templateInfo2.textContent = t.description || '';
        if (startupCommandEl && startupModal?.classList.contains('show')) {
          startupCommandEl.value = selectedStartupCommand;
        }
        void resolveStartupCommandForSelection({
          templateId: selectedTemplateId,
          nodeId: selectedNodeId,
          forceRefresh: false,
          showLoading: startupModal?.classList.contains('show')
        });
      });
      templateGrid.appendChild(card);
    });
  }

  function renderNodes() {
    if (!nodeGrid) return;
    nodeGrid.innerHTML = '';

    if (!nodes.length) {
      const empty = document.createElement('div');
      empty.className = 'enterprise-hint';
      empty.textContent = 'No remote nodes configured. Using local node.';
      nodeGrid.appendChild(empty);
      if (summaryNodeEl) summaryNodeEl.textContent = 'Local node';
      return;
    }

    const autoNode = nodes.find(n => n && n.online) || nodes[0];
    if ((!selectedNodeId || selectedNodeId === 'local') && autoNode) {
      selectedNodeId = autoNode.uuid || autoNode.id || autoNode.name || selectedNodeId;
    }

    nodes.forEach(n => {
      const online = !!n.online;
      const nodeValue = n.uuid || n.id || n.name;
      const el = document.createElement('button');
      el.type = 'button';
      el.className = 'enterprise-node-item';
      el.setAttribute('role', 'option');
      el.setAttribute('aria-selected', 'false');
      el.innerHTML = `
        <div class="enterprise-node-icon">
          <i class="fa-solid fa-server"></i>
        </div>
        <div class="enterprise-node-info">
          <div class="enterprise-node-name">${escapeHtml(n.name || n.uuid)}</div>
          <div class="enterprise-node-meta">${online ? '● Online' : '○ Offline'} · ${escapeHtml(n.address || 'No address')}</div>
        </div>
      `;
      el.addEventListener('click', () => {
        selectedNodeId = nodeValue;
        [...nodeGrid.children].forEach(c => {
          c.classList.remove('selected');
          c.setAttribute('aria-selected', 'false');
        });
        el.classList.add('selected');
        el.setAttribute('aria-selected', 'true');
        const label = n.name || n.uuid;
        if (summaryNodeEl) summaryNodeEl.textContent = online ? label : `${label} (offline)`;
      });

      if (nodeValue && selectedNodeId === nodeValue) {
        el.classList.add('selected');
        el.setAttribute('aria-selected', 'true');
        const label = n.name || n.uuid;
        if (summaryNodeEl) summaryNodeEl.textContent = online ? label : `${label} (offline)`;
      }

      nodeGrid.appendChild(el);
    });

  }

  function openForkModal() {
    if (!forkModal) return;
    if (forkGrid) forkGrid.innerHTML = '';
    selectedForkId = null;
    if (forkInfo) forkInfo.textContent = 'Select a fork to continue.';

    MC_FORKS.forEach(f => {
      const card = document.createElement('button');
      card.type = 'button';
      card.className = 'enterprise-fork-item';
      card.setAttribute('role', 'option');
      card.setAttribute('aria-selected', 'false');
      card.innerHTML = `
        <div class="enterprise-fork-icon">
          <i class="fa-solid fa-cubes"></i>
        </div>
        <div class="enterprise-fork-name">${escapeHtml(f.name)}</div>
        <div class="enterprise-fork-desc">${escapeHtml(f.desc)}</div>
      `;
      card.addEventListener('click', () => {
        selectedForkId = f.id;
        [...forkGrid.children].forEach(c => {
          c.classList.remove('selected');
          c.setAttribute('aria-selected', 'false');
        });
        card.classList.add('selected');
        card.setAttribute('aria-selected', 'true');
        if (forkInfo) forkInfo.textContent = f.name + ' selected';
      });
      forkGrid.appendChild(card);
    });

    forkModal.classList.add('show');
    forkModal.setAttribute('aria-hidden', 'false');
  }

  function closeForkModal() {
    forkModal.classList.remove('show');
    forkModal.setAttribute('aria-hidden', 'true');
  }

  forkClose?.addEventListener('click', closeForkModal);
  forkBack?.addEventListener('click', () => {
    closeForkModal();
    createModal.classList.add('show');
    createModal.setAttribute('aria-hidden', 'false');
  });
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && forkModal?.classList.contains('show')) closeForkModal();
  });

  function openPortModal(backFrom) {
    portBackTarget = backFrom || 'create';

    if (serverPortEl) {
      let defPort = getDefaultPortForTemplate(selectedTemplateId);

      if (selectedNodeId && selectedNodeId !== 'local') {
        const node = (nodes || []).find(x =>
          (x.uuid === selectedNodeId) ||
          (x.id === selectedNodeId) ||
          (x.name === selectedNodeId)
        );
        if (node && node.ports) {
          const alloc = node.ports;
          let inRange = true;
          let firstPort = defPort;
          if (alloc.mode === 'range' && alloc.start > 0 && alloc.count > 0) {
            inRange = defPort >= alloc.start && defPort < alloc.start + alloc.count;
            firstPort = alloc.start;
          } else if (alloc.mode === 'list' && Array.isArray(alloc.ports) && alloc.ports.length > 0) {
            inRange = alloc.ports.includes(defPort);
            firstPort = alloc.ports[0];
          }
          if (!inRange) defPort = firstPort;
        }
      }

      serverPortEl.value = String(defPort);
      if (portDefaultValueEl) portDefaultValueEl.textContent = String(defPort);
      if (serverPortPreviewEl) serverPortPreviewEl.textContent = String(defPort);
    }

    if (serverIpEl) {
      serverIpEl.textContent = 'detecting…';
    }

    const ipv4Re = /\b(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)\b/;
    function extractIP(s) {
      if (!s) return null;
      const str = String(s).trim();
      const m = str.match(ipv4Re);
      if (m) return m[0];
      if (str.includes(':')) return str.replace(/^\[|\]$/g, '');
      return null;
    }
    function pickIpFromJSON(j) {
      return extractIP(j?.publicIp) ||
        extractIP(j?.ip) ||
        extractIP(j?.address) ||
        extractIP(j?.host) ||
        extractIP(j?.hostname);
    }
    function setIp(ip) {
      if (serverIpEl) serverIpEl.textContent = ip || 'unknown';
    }

    const selectedTemplate = templates.find(t => t.id === selectedTemplateId);
    const templateLabel = selectedTemplate ? selectedTemplate.name : 'server';
    if (portTitleTextEl) portTitleTextEl.textContent = 'Expose public port';
    if (portSubtitleEl) portSubtitleEl.textContent = `Choose the public port for ${templateLabel}.`;

    if (selectedNodeId && selectedNodeId !== 'local') {
      const node = (nodes || []).find(x =>
        (x.uuid === selectedNodeId) ||
        (x.id === selectedNodeId) ||
        (x.name === selectedNodeId)
      );
      const ip = node && (node.address || node.host || node.hostname);
      setIp(ip);
    } else {
      fetch('/api/server-info')
        .then(r => r.ok ? r.json() : Promise.reject())
        .then(j => setIp(pickIpFromJSON(j)))
        .catch(() => setIp(null));
    }

    portModal.classList.add('show');
    portModal.setAttribute('aria-hidden', 'false');
  }

  function closePortModal() {
    portModal.classList.remove('show');
    portModal.setAttribute('aria-hidden', 'true');
  }

  portClose?.addEventListener('click', closePortModal);
  portBack?.addEventListener('click', () => {
    closePortModal();
    if (portBackTarget === 'fork') {
      forkModal.classList.add('show');
      forkModal.setAttribute('aria-hidden', 'false');
    } else {
      createModal.classList.add('show');
      createModal.setAttribute('aria-hidden', 'false');
    }
  });
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && portModal?.classList.contains('show')) closePortModal();
  });

  bindStrictPortInput(serverPortEl);
  if (serverPortEl && serverPortPreviewEl) {
    serverPortEl.addEventListener('input', () => {
      const raw = sanitizePortInputValue(serverPortEl.value);
      if (serverPortEl.value !== raw) serverPortEl.value = raw;
      serverPortPreviewEl.textContent = raw || (portDefaultValueEl ? portDefaultValueEl.textContent : '—');
    });
  }

  if (serverNameEl && summaryNameEl) {
    serverNameEl.maxLength = MAX_SERVER_NAME_LENGTH;
    serverNameEl.addEventListener('input', () => {
      if (serverNameEl.value.length > MAX_SERVER_NAME_LENGTH) {
        serverNameEl.value = serverNameEl.value.slice(0, MAX_SERVER_NAME_LENGTH);
      }
      const raw = serverNameEl.value.trim();
      summaryNameEl.textContent = raw || '—';
    });
  }

  createConfirm?.addEventListener('click', () => {
    const raw = serverNameEl.value;
    const name = sanitizeName(raw);
    if (!name) {
      alert('Please enter a valid name (letters, numbers, spaces, dashes, underscores).');
      return;
    }
    if (!selectedTemplateId) {
      alert('Please choose a template.');
      return;
    }

    if (selectedNodeId !== 'local') {
      const node = (nodes || []).find(x =>
        (x.uuid === selectedNodeId) ||
        (x.id === selectedNodeId) ||
        (x.name === selectedNodeId)
      );
      if (!node) {
        alert('The chosen node no longer exists.');
        return;
      }
      if (!node.online) {
        alert('The chosen node is offline.');
        return;
      }
    }

    if (selectedTemplateId === 'minecraft') {
      closeCreate();
      openForkModal();
      return;
    }

    closeCreate();
    openPortModal('create');
  });

  forkConfirm?.addEventListener('click', () => {
    if (!selectedForkId) {
      alert('Please choose a fork.');
      return;
    }
    closeForkModal();
    openPortModal('fork');
  });

  portConfirm?.addEventListener('click', async () => {
    const portVal = parseStrictPortValue(serverPortEl?.value);
    if (!portVal) {
      alert('Invalid port. Choose a value between 1 and 65535.');
      return;
    }

    if (selectedNodeId && selectedNodeId !== 'local') {
      const node = (nodes || []).find(x =>
        (x.uuid === selectedNodeId) ||
        (x.id === selectedNodeId) ||
        (x.name === selectedNodeId)
      );
      if (node && node.ports) {
        const alloc = node.ports;
        let allowed = true;
        if (alloc.mode === 'range' && alloc.start > 0 && alloc.count > 0) {
          allowed = portVal >= alloc.start && portVal < alloc.start + alloc.count;
        } else if (alloc.mode === 'list' && Array.isArray(alloc.ports) && alloc.ports.length > 0) {
          allowed = alloc.ports.includes(portVal);
        }
        if (!allowed) {
          let desc = '';
          if (alloc.mode === 'range') desc = `${alloc.start} \u2013 ${alloc.start + alloc.count - 1}`;
          else if (alloc.mode === 'list') desc = alloc.ports.slice(0, 10).join(', ') + (alloc.ports.length > 10 ? '\u2026' : '');
          alert(`Port ${portVal} is not in this node's allocated ports (${desc}). Please choose a port within the node's allocation.`);
          return;
        }
      }
    }

    closePortModal();
    await openStartupModal();
  });

  async function openStartupModal() {
    if (!startupModal) return;

    startupModal.classList.add('show');
    startupModal.setAttribute('aria-hidden', 'false');
    setStartupCommandValidationError('');

    await resolveStartupCommandForSelection({
      templateId: selectedTemplateId
    });
  }

  function closeStartupModal() {
    if (!startupModal) return;
    startupModal.classList.remove('show');
    startupModal.setAttribute('aria-hidden', 'true');
  }

  startupClose?.addEventListener('click', closeStartupModal);
  startupBack?.addEventListener('click', () => {
    closeStartupModal();
    portModal.classList.add('show');
    portModal.setAttribute('aria-hidden', 'false');
  });
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && startupModal?.classList.contains('show')) closeStartupModal();
  });

  startupConfirm?.addEventListener('click', () => {
    selectedStartupCommand = startupCommandEl ? startupCommandEl.value.trim() : '';
    const startupCommandError = validateRuntimeProcessCommandInput(selectedStartupCommand);
    if (startupCommandError && selectedStartupCommand) {
      setStartupCommandValidationError(startupCommandError);
      setStartupCommandSource('This field accepts only the executable and arguments that run inside the container. Docker CLI startup commands are blocked by ADPanel.');
      startupCommandEl?.focus();
      return;
    }

    setStartupCommandValidationError('');
    closeStartupModal();
    openImportModal();
  });

  startupCommandEl?.addEventListener('input', () => {
    const commandError = validateRuntimeProcessCommandInput(startupCommandEl.value);
    setStartupCommandValidationError(commandError || '');
  });

  const ALLOWED_ARCHIVE_EXTENSIONS = ['.zip', '.rar', '.7z', '.tar.gz', '.tgz', '.tar.bz2', '.tbz2'];

  function isValidArchiveUrl(url) {
    if (!url) return true;
    try {
      const parsedUrl = new URL(url);
      if (!['http:', 'https:'].includes(parsedUrl.protocol)) return false;
      const pathname = parsedUrl.pathname.toLowerCase();
      return ALLOWED_ARCHIVE_EXTENSIONS.some(ext => pathname.endsWith(ext));
    } catch {
      return false;
    }
  }

  function formatDownloadBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  function openImportModal() {
    if (!importModal) {
      openResourcesModal();
      return;
    }

    selectedImportUrl = '';
    if (importUrlEl) importUrlEl.value = '';
    if (importProgressContainer) importProgressContainer.style.display = 'none';
    if (importProgressFill) importProgressFill.style.width = '0%';
    if (importPercent) importPercent.textContent = '0%';
    if (importDownloaded) importDownloaded.textContent = '0 MB';
    if (importTotal) importTotal.textContent = '0 MB';

    if (importConfirm) importConfirm.disabled = false;
    if (importBack) importBack.disabled = false;
    if (importSkip) importSkip.disabled = false;
    if (importClose) importClose.disabled = false;

    importModal.classList.add('show');
    importModal.setAttribute('aria-hidden', 'false');
  }

  function closeImportModal() {
    if (!importModal) return;
    importModal.classList.remove('show');
    importModal.setAttribute('aria-hidden', 'true');
  }

  function setImportButtonsDisabled(disabled) {
    if (importConfirm) importConfirm.disabled = disabled;
    if (importBack) importBack.disabled = disabled;
    if (importSkip) importSkip.disabled = disabled;
    if (importClose) importClose.disabled = disabled;
  }

  importClose?.addEventListener('click', () => {
    closeImportModal();
  });

  importBack?.addEventListener('click', () => {
    closeImportModal();
    startupModal.classList.add('show');
    startupModal.setAttribute('aria-hidden', 'false');
  });

  importSkip?.addEventListener('click', () => {
    selectedImportUrl = '';
    closeImportModal();
    openResourcesModal();
  });


  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && importModal?.classList.contains('show')) {
      closeImportModal();
    }
  });

  importConfirm?.addEventListener('click', async () => {
    const url = importUrlEl ? importUrlEl.value.trim() : '';

    if (!url) {
      selectedImportUrl = '';
      closeImportModal();
      openResourcesModal();
      return;
    }

    if (!isValidArchiveUrl(url)) {
      alert('Invalid URL. Please enter a direct link to a .zip, .rar, .7z, or .tar.gz archive.');
      return;
    }

    selectedImportUrl = url;
    closeImportModal();
    openResourcesModal();
  });

  function openResourcesModal() {
    if (resourceRamEl) resourceRamEl.value = '';
    if (resourceCpuCoresEl) resourceCpuCoresEl.value = '';
    if (resourceSwapEl) resourceSwapEl.value = '';
    if (resourceStorageEl) resourceStorageEl.value = '';
    if (resourceBackupsEl) resourceBackupsEl.value = '';
    if (resourceMaxSchedulesEl) resourceMaxSchedulesEl.value = '';
    if (resourceIoWeightEl) resourceIoWeightEl.value = '';
    if (resourceCpuWeightEl) resourceCpuWeightEl.value = '';
    if (resourcePidsLimitEl) resourcePidsLimitEl.value = '';
    if (resourceFileLimitEl) resourceFileLimitEl.value = '';

    const node = (nodes || []).find(x =>
      (x.uuid === selectedNodeId) ||
      (x.id === selectedNodeId) ||
      (x.name === selectedNodeId)
    );

    if (node) {
      const cacheKey = resolveNodeCacheKey(node);
      applyNodeCapacityToResourceModal(resolveEffectiveNodeCapacity(node, null));
      fetchLiveNodeCapacity(node).then((liveCapacity) => {
        if (!liveCapacity) return;
        if (!resourcesModal?.classList.contains('show')) return;
        const selectedNode = (nodes || []).find(x =>
          (x.uuid === selectedNodeId) ||
          (x.id === selectedNodeId) ||
          (x.name === selectedNodeId)
        );
        if (!selectedNode || resolveNodeCacheKey(selectedNode) !== cacheKey) return;
        applyNodeCapacityToResourceModal(resolveEffectiveNodeCapacity(selectedNode, liveCapacity));
      });
    } else {
      applyNodeCapacityToResourceModal({ ramMb: 0, cpuCores: 0, diskGb: 0 });
    }

    resourcesModal.classList.add('show');
    resourcesModal.setAttribute('aria-hidden', 'false');
  }

  function closeResourcesModal() {
    resourcesModal.classList.remove('show');
    resourcesModal.setAttribute('aria-hidden', 'true');
  }

  resourcesClose?.addEventListener('click', closeResourcesModal);
  resourcesBack?.addEventListener('click', () => {
    closeResourcesModal();
    openImportModal();
  });
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && resourcesModal?.classList.contains('show')) closeResourcesModal();
  });

  resourcesConfirm?.addEventListener('click', async () => {
    const raw = serverNameEl.value;
    const name = sanitizeName(raw);
    if (!name) {
      alert('Server name is missing or invalid.');
      return;
    }
    if (!selectedTemplateId) {
      alert('Template is not chosen.');
      return;
    }
    if (selectedTemplateId === 'minecraft' && !selectedForkId) {
      alert('Please choose a fork.');
      return;
    }

    if (selectedNodeId !== 'local') {
      const node = (nodes || []).find(x =>
        (x.uuid === selectedNodeId) ||
        (x.id === selectedNodeId) ||
        (x.name === selectedNodeId)
      );
      if (!node) {
        alert('The chosen node no longer exists.');
        return;
      }
      if (!node.online) {
        alert('The chosen node is offline.');
        return;
      }

      const effectiveCapacity = await getEffectiveNodeCapacity(node);
      const ramMb = effectiveCapacity.ramMb;
      const cpuCores = effectiveCapacity.cpuCores;
      const diskMb2 = effectiveCapacity.diskGb > 0 ? Math.trunc(effectiveCapacity.diskGb * 1024) : 0;

      const inputRam = resourceRamEl && resourceRamEl.value ? parseInt(resourceRamEl.value, 10) : 0;
      const inputCpuCores = resourceCpuCoresEl && resourceCpuCoresEl.value ? parseFloat(resourceCpuCoresEl.value) : 0;
      const inputStorage = resourceStorageEl && resourceStorageEl.value ? parseInt(resourceStorageEl.value, 10) : 0;

      if (ramMb > 0 && inputRam > ramMb) {
        alert(`RAM limit cannot exceed node maximum (${ramMb} MB).`);
        return;
      }
      if (cpuCores > 0 && inputCpuCores > cpuCores) {
        alert(`CPU cores cannot exceed node maximum (${cpuCores} cores).`);
        return;
      }
      if (diskMb2 > 0 && inputStorage > diskMb2) {
        alert(`Storage limit cannot exceed node maximum (${diskMb2.toLocaleString()} MB).`);
        return;
      }
    }

    const portVal = parseStrictPortValue(serverPortEl?.value);
    if (!portVal) {
      alert('Invalid port. Choose a value between 1 and 65535.');
      return;
    }

    if (selectedNodeId && selectedNodeId !== 'local') {
      const node = (nodes || []).find(x =>
        (x.uuid === selectedNodeId) ||
        (x.id === selectedNodeId) ||
        (x.name === selectedNodeId)
      );
      if (node && node.ports) {
        const alloc = node.ports;
        let allowed = true;
        if (alloc.mode === 'range' && alloc.start > 0 && alloc.count > 0) {
          allowed = portVal >= alloc.start && portVal < alloc.start + alloc.count;
        } else if (alloc.mode === 'list' && Array.isArray(alloc.ports) && alloc.ports.length > 0) {
          allowed = alloc.ports.includes(portVal);
        }
        if (!allowed) {
          let desc = '';
          if (alloc.mode === 'range') desc = `${alloc.start} \u2013 ${alloc.start + alloc.count - 1}`;
          else if (alloc.mode === 'list') desc = alloc.ports.slice(0, 10).join(', ') + (alloc.ports.length > 10 ? '\u2026' : '');
          alert(`Port ${portVal} is not in this node's allocated ports (${desc}). Please choose a port within the node's allocation.`);
          return;
        }
      }
    }

    try {
      resourcesSpinner.classList.add('visible');
      resourcesConfirm.disabled = true;

      const displayName = raw.trim().replace(/[^a-zA-Z0-9 \-_.]/g, '').replace(/\s+/g, ' ').slice(0, 120);
      const payload = {
        name,
        displayName: displayName || name,
        templateId: selectedTemplateId,
        nodeId: selectedNodeId,
        hostPort: portVal
      };
      if (selectedStartupCommand) {
        const payloadCommandError = validateRuntimeProcessCommandInput(selectedStartupCommand);
        if (!payloadCommandError) {
          payload.command = selectedStartupCommand;
        }
      }

      if (selectedTemplateId === 'minecraft') {
        payload.mcFork = selectedForkId;
        payload.mcVersion = MC_VERSION;
      }

      if (selectedImportUrl) {
        payload.importUrl = selectedImportUrl;
      }

      const ramLimit = resourceRamEl && resourceRamEl.value ? parseInt(resourceRamEl.value, 10) : null;
      const cpuCoresLimit = resourceCpuCoresEl && resourceCpuCoresEl.value ? parseFloat(resourceCpuCoresEl.value) : null;
      const swapLimit = resourceSwapEl && resourceSwapEl.value !== '' ? parseInt(resourceSwapEl.value, 10) : null;
      const storageLimit = resourceStorageEl && resourceStorageEl.value ? parseInt(resourceStorageEl.value, 10) : null;
      const backupsLimit = resourceBackupsEl && resourceBackupsEl.value ? parseInt(resourceBackupsEl.value, 10) : null;
      const maxSchedulesLimit = resourceMaxSchedulesEl && resourceMaxSchedulesEl.value ? parseInt(resourceMaxSchedulesEl.value, 10) : null;
      const ioWeightLimit = readOptionalIntegerRange(resourceIoWeightEl, 'I/O Priority', 10, 1000);
      const cpuWeightLimit = readOptionalIntegerRange(resourceCpuWeightEl, 'CPU Priority', 1, 1000);
      const pidsLimit = readOptionalIntegerRange(resourcePidsLimitEl, 'Process Limit', 64, 4096);
      const fileLimit = readOptionalIntegerRange(resourceFileLimitEl, 'File Limit', 1024, 1048576);

      payload.resources = {
        ramMb: ramLimit,
        cpuCores: cpuCoresLimit,
        swapMb: swapLimit,
        storageMb: storageLimit,
        backupsMax: backupsLimit,
        maxSchedules: maxSchedulesLimit,
        ioWeight: ioWeightLimit,
        cpuWeight: cpuWeightLimit,
        pidsLimit,
        fileLimit
      };

      const r = await fetch('/api/servers/create', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok || !j.ok) {
        throw new Error(
          (j && (j.detail || j.message || j.error))
            ? (j.detail || j.message || j.error)
            : 'Create failed'
        );
      }

      window.location.href = `/server/${encodeURIComponent(name)}`;
    } catch (e) {
      alert(e.message || 'Create failed');
    } finally {
      resourcesSpinner.classList.remove('visible');
      resourcesConfirm.disabled = false;
    }
  });
});

const serverStartTime = Number(document.body.dataset.serverStartTime) || Date.now();
const uptimeDisplay = document.getElementById('uptimeDisplay');
function formatTime(ms) {
  const totalSeconds = Math.floor(ms / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  return `${hours}h ${minutes}m ${seconds}s`;
}
function updateUptime() {
  if (uptimeDisplay) {
    const now = Date.now();
    uptimeDisplay.textContent = formatTime(now - serverStartTime);
  }
}
setInterval(updateUptime, 1000);
updateUptime();


class DashboardStatusManager {
  constructor() {
    this.socket = null;
    this.serverCards = new Map();
    this.statuses = new Map();
    this.connected = false;
  }

  init() {
    document.querySelectorAll('[data-bot-card]').forEach(card => {
      const name = card.dataset.name;
      if (name) this.serverCards.set(name.toLowerCase(), card);
    });

    if (this.serverCards.size === 0) return;

    if (window._dashResourceSocket) {
      this.socket = window._dashResourceSocket;
      this.connected = window._dashResourceSocket.connected;
      this._attachListeners();
      if (this.connected) this.subscribe();
    } else {
      this.connectSocket();
    }
  }

  connectSocket() {
    if (!window.io) return;

    this.socket = window.io({
      transports: ['websocket', 'polling'],
      reconnection: true,
      reconnectionDelay: 1000,
      reconnectionDelayMax: 5000
    });

    this._attachListeners();
  }

  _attachListeners() {
    if (!this.socket) return;

    this.socket.on('connect', () => {
      this.connected = true;
      this.subscribe();
    });

    this.socket.on('disconnect', () => {
      this.connected = false;
    });

    this.socket.on('dashboard:initial', (data) => {
      if (data.statuses) {
        Object.entries(data.statuses).forEach(([name, status]) => {
          this.updateServerCard(name, status);
        });
      }
    });

    this.socket.on('server:status', (data) => {
      this.updateServerCard(data.name, data);
    });

    this.socket.on('node:status', (data) => {
      this.updateNodeIndicators(data.id, data);
    });
  }

  subscribe() {
    const serverNames = Array.from(this.serverCards.keys());
    this.socket.emit('dashboard:subscribe', { bots: serverNames });
  }

  updateServerCard(name, status) {
    const cardName = String(name || '').toLowerCase();
    const card = this.serverCards.get(cardName);
    if (!card) return;

    this.statuses.set(cardName, status);

    const statusEl = card.querySelector('.bot-card__status');
    if (statusEl) {
      const normalized = this.normalizeStatus(status.status);
      const allStatusClasses = ['status-online', 'status-stopped', 'status-unknown'];
      card.classList.remove(...allStatusClasses);
      statusEl.classList.remove(...allStatusClasses);
      if (normalized === 'online') {
        card.classList.add('status-online');
        statusEl.classList.add('status-online');
      } else if (normalized === 'stopped') {
        card.classList.add('status-stopped');
        statusEl.classList.add('status-stopped');
      } else {
        card.classList.add('status-unknown');
        statusEl.classList.add('status-unknown');
      }
    }

    if ('nodeOnline' in status && typeof window.updateNodeOfflineStatus === 'function') {
      window.updateNodeOfflineStatus(name, status.nodeOnline === true);
    }
  }

  updateNodeIndicators(nodeId, status) {
    document.querySelectorAll(`[data-node-id="${nodeId}"]`).forEach(card => {
      if (typeof window.updateNodeOfflineStatus === 'function') {
        const name = card.dataset.name;
        if (name) window.updateNodeOfflineStatus(name, status.online === true);
      }
    });
  }

  normalizeStatus(status) {
    const raw = String(status || '').toLowerCase();
    if (['online', 'running', 'up', 'healthy'].some(t => raw.includes(t))) return 'online';
    if (raw.includes('unknown')) return null;
    if (['offline', 'exited', 'stopped', 'down', 'dead', 'error'].some(t => raw.includes(t))) return 'stopped';
    if (!raw) return 'stopped';
    return null;
  }

  destroy() {
    if (this.socket) {
      this.socket.emit('dashboard:unsubscribe');
    }
  }
}

window.dashboardStatus = new DashboardStatusManager();
window.dashboardStatus.init();
