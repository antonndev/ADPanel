const BG_KEY = 'ui-background';
const ACCENT_KEY = 'ui-accent';
const DENSITY_KEY = 'ui-density';
const ANIM_KEY = 'ui-animations';
const body = document.body;
const sidebarNav = document.getElementById('sidebarNav');
const sections = document.querySelectorAll('.panel-section');
const closeEscFixed = document.getElementById('closeEscFixed');
const leftCol = document.getElementById('leftCol');
const modalOverlay = document.getElementById('modalOverlay');
const changeModal = document.getElementById('changeModal');
const changePassCard = document.getElementById('changePassCard');
const modalCancel = document.getElementById('modalCancel');
const modalCancelBtn = document.getElementById('modalCancelBtn');
const modalSave = document.getElementById('modalSave');
const accountModalOverlay = document.getElementById('accountModalOverlay');
const accountModal = document.getElementById('accountModal');
const accountModalTitle = document.getElementById('accountModalTitle');
const accountModalBody = document.getElementById('accountModalBody');
const accountModalClose = document.getElementById('accountModalClose');
const permModalOverlay = document.getElementById('permModalOverlay');
const permModal = document.getElementById('permModal');
const permModalTitle = document.getElementById('permModalTitle');
const permModalBody = document.getElementById('permModalBody');
const permCancel = document.getElementById('permCancel');
const permCancelBtn = document.getElementById('permCancelBtn');
const permSave = document.getElementById('permSave');
const templatesFeedbackEl = document.getElementById('templatesFeedback');
const templatesListEl = document.getElementById('templatesList');
const openCreateTemplateBtn = document.getElementById('openCreateTemplate');
const createTemplateOverlay = document.getElementById('createTemplateOverlay');
const createTemplateModal = document.getElementById('createTemplateModal');
const createTemplateCancel = document.getElementById('createTemplateCancel');
const createTemplateClose = document.getElementById('createTemplateClose');
const createTemplateSave = document.getElementById('createTemplateSave');
const createTemplateFeedback = document.getElementById('createTemplateFeedback');
const ct_id = document.getElementById('ct_id');
const ct_name = document.getElementById('ct_name');
const ct_description = document.getElementById('ct_description');
const ct_template_image = document.getElementById('ct_template_image');
const ct_image = document.getElementById('ct_image');
const ct_tag = document.getElementById('ct_tag');
const serversListEl = document.getElementById('serversList');
const createServerBtn = document.getElementById('createServerBtn');
const createServerNameInput = document.getElementById('createServerName');
const serverTemplateOverlay = document.getElementById('serverTemplateOverlay');
const serverTemplateModal = document.getElementById('serverTemplateModal');
const serverTemplateTitle = document.getElementById('serverTemplateTitle');
const serverTemplateSubtitle = document.getElementById('serverTemplateSubtitle');
const serverTemplateList = document.getElementById('serverTemplateList');
const serverTemplateFeedback = document.getElementById('serverTemplateFeedback');
const serverTemplateCancel = document.getElementById('serverTemplateCancel');
const serverTemplateClose = document.getElementById('serverTemplateClose');
const serverTemplateDone = document.getElementById('serverTemplateDone');
const accountsListEl = document.getElementById('accountsList');
const accountsSearchInput = document.getElementById('accountsSearchInput');
const nodesListEl = document.getElementById('nodesList');
const openCreateNodeBtn = document.getElementById('openCreateNode');
const createNodeOverlay = document.getElementById('createNodeOverlay');
const createNodeModal = document.getElementById('createNodeModal');
const createNodeClose = document.getElementById('createNodeClose');
const createNodeCancel = document.getElementById('createNodeCancel');
const createNodeSave = document.getElementById('createNodeSave');
const createNodeFeedback = document.getElementById('createNodeFeedback');
const cn_name = document.getElementById('cn_name');
const cn_address = document.getElementById('cn_address');
const cn_ram_gb = document.getElementById('cn_ram_gb');
const cn_disk_gb = document.getElementById('cn_disk_gb');
const cn_ports_range = document.getElementById('cn_ports_range');
const cn_ports_list = document.getElementById('cn_ports_list');
const cn_port_start = document.getElementById('cn_port_start');
const cn_port_count = document.getElementById('cn_port_count');
const cn_port_list = document.getElementById('cn_port_list');
const cn_cpu_cores = document.getElementById('cn_cpu_cores');
const cn_max_upload_mb = document.getElementById('cn_max_upload_mb');
const cn_daemon_port = document.getElementById('cn_daemon_port');
const cn_sftp_port = document.getElementById('cn_sftp_port');
const cn_ssl_enabled = document.getElementById('cn_ssl_enabled');
const cn_ssl_row = document.getElementById('cn_ssl_row');
const nodeModalOverlay = document.getElementById('nodeModalOverlay');
const nodeModal = document.getElementById('nodeModal');
const nodeModalClose = document.getElementById('nodeModalClose');
const nodeModalCancelBtn = document.getElementById('nodeModalCancelBtn');
const nm_title = document.getElementById('nm_title');
const nm_subtitle = document.getElementById('nm_subtitle');
const nm_tabs = nodeModal.querySelectorAll('.node-tabs .node-tab');
const tab_build = document.getElementById('tab_build');
const tab_linking = document.getElementById('tab_linking');
const tab_deletion = document.getElementById('tab_deletion');
const buildFooter = document.getElementById('buildFooter');
const nm_name = document.getElementById('nm_name');
const nm_address = document.getElementById('nm_address');
const nm_cpu_cores = document.getElementById('nm_cpu_cores');
const nm_ram_gb = document.getElementById('nm_ram_gb');
const nm_disk_gb = document.getElementById('nm_disk_gb');
const nm_ports_range = document.getElementById('nm_ports_range');
const nm_ports_list = document.getElementById('nm_ports_list');
const nm_port_start = document.getElementById('nm_port_start');
const nm_port_count = document.getElementById('nm_port_count');
const nm_port_list = document.getElementById('nm_port_list');
const nm_max_upload_mb = document.getElementById('nm_max_upload_mb');
const nm_daemon_port = document.getElementById('nm_daemon_port');
const nm_sftp_port = document.getElementById('nm_sftp_port');
const nm_save = document.getElementById('nm_save');
const nm_saveFeedback = document.getElementById('nm_saveFeedback');
const nm_configYml = document.getElementById('nm_configYml');
const nm_copyConfig = document.getElementById('nm_copyConfig');
const nm_downloadConfig = document.getElementById('nm_downloadConfig');
const nm_showCmd = document.getElementById('nm_showCmd');
const nm_cmd = document.getElementById('nm_cmd');
const nm_cmdActions = document.getElementById('nm_cmdActions');
const nm_copyCmd = document.getElementById('nm_copyCmd');
const nm_delete = document.getElementById('nm_delete');
const nm_deleteFeedback = document.getElementById('nm_deleteFeedback');
const nm_ssl_enabled = document.getElementById('nm_ssl_enabled');
const nm_ssl_row = document.getElementById('nm_ssl_row');
const PERM_KEYS = [
  "files_read", "files_delete", "files_rename", "files_archive", "console_write", "server_stop", "server_start", "files_upload", "files_create", "activity_logs", "backups_view", "backups_create", "backups_delete", "scheduler_access", "scheduler_create", "scheduler_delete", "store_access", "subdomain_show", "subdomain_add", "server_reinstall"
];
const ACL_PREFIX = 'acl:';
const THEME_KEY = 'ui-theme';
const UNSAVED_MODAL_MESSAGE = 'Ai modificari nesalvate intr-un popup. Daca reincarci pagina, acestea se vor pierde.';
const TRACKED_MODAL_SELECTOR = '.modal-overlay';
const TRACKABLE_MODAL_FIELD_SELECTOR = [
  'input:not([type="hidden"]):not([type="button"]):not([type="submit"]):not([type="reset"]):not([disabled]):not([readonly])',
  'select:not([disabled]):not([readonly])',
  'textarea:not([disabled]):not([readonly])',
  '[contenteditable="true"]'
].join(',');
const MODAL_MUTATION_CLICK_SELECTOR = '.icon-suggestion, .template-option-card, .port-remove, .pf-remove, [data-unsaved-action]';
const modalUnsavedRecords = new WeakMap();
const modalCustomStateReaders = new Map();
let unsavedModalBeforeUnloadBound = false;

function registerModalCustomStateReader(modalId, reader) {
  if (!modalId || typeof reader !== 'function') return;
  modalCustomStateReaders.set(modalId, reader);
  observeTrackedModals();
}

function getModalUnsavedRecord(root) {
  let record = modalUnsavedRecords.get(root);
  if (!record) {
    record = {
      baseline: '',
      dirty: false,
      userInteracted: false,
      pristineSyncTimer: null,
      observer: null
    };
    modalUnsavedRecords.set(root, record);
  }
  return record;
}

function serializeDataset(dataset) {
  if (!dataset) return '';
  const entries = Object.entries(dataset)
    .filter(([, value]) => value !== undefined && value !== null && value !== '')
    .sort(([a], [b]) => a.localeCompare(b));
  return entries.map(([key, value]) => `${key}=${String(value)}`).join('|');
}

function getModalFieldKey(el, index) {
  const explicit = el.getAttribute('data-unsaved-key');
  if (explicit) return explicit;
  if (el.id) return `#${el.id}`;
  if (el.name) return `${el.tagName.toLowerCase()}:${el.name}:${index}`;
  return `${el.tagName.toLowerCase()}:${el.type || 'value'}:${index}`;
}

function hasHiddenAncestor(node, root) {
  let current = node;
  while (current && current !== root) {
    if (current.hidden) return true;
    const style = window.getComputedStyle(current);
    if (style.display === 'none' || style.visibility === 'hidden') return true;
    current = current.parentElement;
  }
  return false;
}

function isTrackableFieldVisible(el, root) {
  if (!el || !root) return false;
  if (el.matches('[disabled], [readonly]')) return false;
  if (el.tagName === 'INPUT' && el.type === 'file') {
    return !!(el.files && el.files.length > 0) || !hasHiddenAncestor(el.parentElement || el, root);
  }
  return !hasHiddenAncestor(el, root);
}

function serializeTrackableField(el, index) {
  const key = getModalFieldKey(el, index);
  const tag = el.tagName.toLowerCase();
  if (tag === 'input') {
    const type = (el.type || 'text').toLowerCase();
    if (type === 'checkbox' || type === 'radio') {
      return `${key}=${el.checked ? '1' : '0'}`;
    }
    if (type === 'file') {
      const files = Array.from(el.files || []).map(file => `${file.name}:${file.size}:${file.lastModified}`);
      return `${key}=${files.join(',')}`;
    }
    return `${key}=${el.value || ''}`;
  }
  if (tag === 'select' || tag === 'textarea') {
    return `${key}=${el.value || ''}`;
  }
  return `${key}=${(el.textContent || '').trim()}`;
}

function serializeMarkedModalState(el, index) {
  const mode = (el.getAttribute('data-unsaved-state') || 'text').toLowerCase();
  const key = el.getAttribute('data-unsaved-key') || el.id || `state:${index}`;
  let value = '';
  if (mode === 'html') {
    value = (el.innerHTML || '').replace(/\s+/g, ' ').trim();
  } else if (mode === 'dataset') {
    value = serializeDataset(el.dataset);
  } else if (mode === 'value') {
    value = typeof el.value === 'string' ? el.value : (el.textContent || '').trim();
  } else {
    value = (el.textContent || '').replace(/\s+/g, ' ').trim();
  }
  return `${key}=${value}`;
}

function hasTrackableModalState(root) {
  if (!root) return false;
  return !!(
    modalCustomStateReaders.has(root.id) ||
    root.querySelector(TRACKABLE_MODAL_FIELD_SELECTOR) ||
    root.querySelector('[data-unsaved-state]')
  );
}

function isModalOpen(root) {
  if (!root) return false;
  if (root.classList.contains('show')) return true;
  if (root.getAttribute('aria-hidden') === 'false') return true;
  const style = window.getComputedStyle(root);
  if (style.display === 'none' || style.visibility === 'hidden') return false;
  return style.opacity !== '0';
}

function serializeModalState(root) {
  if (!root) return '';
  const parts = [];
  const innerModal = root.querySelector('.enterprise-modal');
  const rootDataset = serializeDataset(root.dataset);
  if (rootDataset) parts.push(`root:${rootDataset}`);
  if (innerModal && innerModal !== root) {
    const modalDataset = serializeDataset(innerModal.dataset);
    if (modalDataset) parts.push(`modal:${modalDataset}`);
  }
  Array.from(root.querySelectorAll(TRACKABLE_MODAL_FIELD_SELECTOR))
    .filter(el => isTrackableFieldVisible(el, root))
    .forEach((el, index) => {
      parts.push(serializeTrackableField(el, index));
    });
  Array.from(root.querySelectorAll('[data-unsaved-state]'))
    .forEach((el, index) => {
      parts.push(serializeMarkedModalState(el, index));
    });
  const customReader = modalCustomStateReaders.get(root.id);
  if (typeof customReader === 'function') {
    try {
      parts.push(`custom=${JSON.stringify(customReader())}`);
    } catch (err) {
      console.warn('[unsaved-modal-guard] Failed to serialize custom state for', root.id, err);
    }
  }
  return parts.join('||');
}

function stopModalPristineSync(root) {
  const record = getModalUnsavedRecord(root);
  if (record.pristineSyncTimer) {
    clearInterval(record.pristineSyncTimer);
    record.pristineSyncTimer = null;
  }
}

function getDirtyOpenModals() {
  return Array.from(document.querySelectorAll(TRACKED_MODAL_SELECTOR))
    .filter(root => hasTrackableModalState(root) && isModalOpen(root))
    .filter(root => getModalUnsavedRecord(root).dirty);
}

function modalBeforeUnloadHandler(e) {
  if (!getDirtyOpenModals().length) return;
  e.preventDefault();
  e.returnValue = UNSAVED_MODAL_MESSAGE;
  return UNSAVED_MODAL_MESSAGE;
}

function updateUnsavedModalBeforeUnload() {
  const shouldBind = getDirtyOpenModals().length > 0;
  if (shouldBind && !unsavedModalBeforeUnloadBound) {
    window.addEventListener('beforeunload', modalBeforeUnloadHandler);
    unsavedModalBeforeUnloadBound = true;
  } else if (!shouldBind && unsavedModalBeforeUnloadBound) {
    window.removeEventListener('beforeunload', modalBeforeUnloadHandler);
    unsavedModalBeforeUnloadBound = false;
  }
}

function syncModalBaseline(root) {
  const record = getModalUnsavedRecord(root);
  record.baseline = serializeModalState(root);
  record.dirty = false;
  updateUnsavedModalBeforeUnload();
}

function startModalPristineSync(root) {
  if (!root || !hasTrackableModalState(root)) return;
  const record = getModalUnsavedRecord(root);
  record.userInteracted = false;
  stopModalPristineSync(root);
  syncModalBaseline(root);
  record.pristineSyncTimer = window.setInterval(() => {
    if (!isModalOpen(root)) {
      stopModalPristineSync(root);
      return;
    }
    if (record.userInteracted) {
      stopModalPristineSync(root);
      return;
    }
    record.baseline = serializeModalState(root);
    record.dirty = false;
    updateUnsavedModalBeforeUnload();
  }, 300);
}

function resetModalUnsavedState(root) {
  if (!root) return;
  const record = getModalUnsavedRecord(root);
  stopModalPristineSync(root);
  record.baseline = '';
  record.dirty = false;
  record.userInteracted = false;
  updateUnsavedModalBeforeUnload();
}

function evaluateModalUnsavedState(root) {
  if (!root || !hasTrackableModalState(root)) return false;
  const record = getModalUnsavedRecord(root);
  const current = serializeModalState(root);
  if (!record.baseline) {
    record.baseline = current;
  }
  record.dirty = current !== record.baseline;
  updateUnsavedModalBeforeUnload();
  return record.dirty;
}

function handleModalUserInteraction(target) {
  const root = target?.closest(TRACKED_MODAL_SELECTOR);
  if (!root || !isModalOpen(root) || !hasTrackableModalState(root)) return;
  const record = getModalUnsavedRecord(root);
  record.userInteracted = true;
  stopModalPristineSync(root);
  window.setTimeout(() => {
    if (isModalOpen(root)) evaluateModalUnsavedState(root);
  }, 0);
}

function observeTrackedModals() {
  Array.from(document.querySelectorAll(TRACKED_MODAL_SELECTOR)).forEach(root => {
    const record = getModalUnsavedRecord(root);
    if (record.observer) return;
    const observer = new MutationObserver(() => {
      if (isModalOpen(root)) {
        if (!record.userInteracted) startModalPristineSync(root);
      } else {
        resetModalUnsavedState(root);
      }
    });
    observer.observe(root, {
      attributes: true,
      attributeFilter: ['class', 'style', 'aria-hidden'],
      childList: true,
      subtree: true
    });
    record.observer = observer;
    if (isModalOpen(root)) startModalPristineSync(root);
  });
}

document.addEventListener('input', (e) => handleModalUserInteraction(e.target), true);
document.addEventListener('change', (e) => handleModalUserInteraction(e.target), true);
document.addEventListener('click', (e) => {
  const mutationTarget = e.target.closest(MODAL_MUTATION_CLICK_SELECTOR);
  if (mutationTarget) handleModalUserInteraction(mutationTarget);
}, true);
observeTrackedModals();

function enforceDarkTheme() {
  body.setAttribute('data-theme', 'dark');
  try { localStorage.setItem(THEME_KEY, 'dark'); } catch (e) { }
  const savedThemeEl = document.getElementById('savedTheme');
  if (savedThemeEl) savedThemeEl.textContent = 'Dark';
  const themeDark = document.getElementById('themeDark');
  const themeLight = document.getElementById('themeLight');
  if (themeDark) themeDark.classList.add('active');
  if (themeLight) themeLight.classList.remove('active');
}
enforceDarkTheme();
function aclKey(email, server) { return `${ACL_PREFIX}${email}:${server}`; }
function getSavedPerms(email, server) {
  try { const raw = localStorage.getItem(aclKey(email, server)); if (!raw) return null; const obj = JSON.parse(raw); if (obj && typeof obj === 'object') return obj; } catch (e) { }
  return null;
}
function setSavedPerms(email, server, perms) {
  try { localStorage.setItem(aclKey(email, server), JSON.stringify(perms || {})); } catch (e) { }
}
function summarizePerms(perms) {
  const enabled = PERM_KEYS.filter(k => perms && perms[k]);
  return enabled.length ? enabled.join(', ') : 'none';
}
sidebarNav.addEventListener('click', (e) => {
  const btn = e.target.closest('button'); if (!btn) return;
  sidebarNav.querySelectorAll('button').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  const panel = btn.dataset.panel;
  sections.forEach(s => s.classList.remove('active'));
  const target = document.getElementById(panel); if (target) target.classList.add('active');
  if (panel === 'servers') {
    loadServers();
  }
  if (panel === 'account') {
    loadAccounts();
  }
  if (panel === 'nodes') {
  }
  if (panel === 'templates') {
    loadTemplates();
  }
  if (panel === 'webhooks') {
    if (typeof window._loadWebhooks === 'function') window._loadWebhooks();
  }
  if (panel === 'panelinfo') {
    loadPanelInfo();
  }
});
function deleteAllClientCookies() {
  const cookies = document.cookie.split("; ");
  for (const cookie of cookies) {
    const eqPos = cookie.indexOf("=");
    const name = eqPos > -1 ? cookie.substr(0, eqPos) : cookie;
    document.cookie = name + "=;expires=Thu, 01 Jan 1970 00:00:00 GMT;path=/";
  }
}
document.getElementById("logoutBtn").addEventListener("click", async () => {
  try {
    deleteAllClientCookies();
    await fetch('/logout', { method: 'POST', credentials: 'include' });
    location.href = '/login';
  } catch (err) {
    console.error(err);
    alert('Failed to log out');
  }
});
function goHome() { history.back(); }
closeEscFixed.addEventListener('click', goHome);
document.addEventListener('keydown', (e) => { if (e.key === 'Escape') goHome(); });
const presets = [
  { id: 1, url: '/images/waves-digital.webp' },
  { id: 2, url: '/images/mountains.webp' },
  { id: 3, url: '/images/sunset.webp' },
  { id: 4, url: '/images/waves.png' }
];
const thumbsEl = document.getElementById('thumbs');
const bgPreview = document.getElementById('bgPreview');
const colorPicker = document.getElementById('colorPicker');
const hexInput = document.getElementById('hexInput');
const colorDot = document.getElementById('colorDot');
const applyColorBtn = document.getElementById('applyColorBtn');
const bgHex = document.getElementById('bgHex');
const uploadFile = document.getElementById('uploadFile');
const urlInput = document.getElementById('urlInput');
const useUrlBtn = document.getElementById('useUrlBtn');
const densitySelect = document.getElementById('densitySelect');
const animationsToggle = document.getElementById('animationsToggle');
const accentPicker = document.getElementById('accentPicker');
const savedBg = document.getElementById('savedBg');
async function sendBackgroundToServer(obj) {
  try {
    const res = await fetch('/api/settings/background', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(obj)
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: 'failed' }));
      console.warn("Server returned error:", err);
    } else {
      console.log("Background updated on server");
    }
  } catch (e) {
    console.error("Failed to send background to server", e);
  }
}
presets.forEach(p => {
  const el = document.createElement('div');
  el.className = 'thumb';
  el.dataset.url = p.url;
  el.innerHTML = `<img src="${p.url}" alt="preset-${p.id}">`;
  el.addEventListener('click', () => selectThumb(el));
  thumbsEl.appendChild(el);
});
function selectThumb(el) {
  document.querySelectorAll('.thumb').forEach(t => t.classList.remove('selected'));
  el.classList.add('selected');
  const url = el.dataset.url;
  bgPreview.style.backgroundImage = `url(${url})`;
  bgPreview.style.backgroundSize = 'cover';
  bgPreview.style.backgroundPosition = 'center';
  const obj = { type: 'image', value: url };
  localStorage.setItem(BG_KEY, JSON.stringify(obj));
  updateSavedBgText();
  sendBackgroundToServer(obj);
}
function setColor(hex) {
  colorPicker.value = hex;
  hexInput.value = hex;
  colorDot.style.background = hex;

  hexInput.style.backgroundColor = hex;
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  const brightness = (r * 299 + g * 587 + b * 114) / 1000;
  hexInput.style.color = brightness > 128 ? '#000' : '#fff';
  hexInput.style.border = '1px solid rgba(0,0,0,0.1)';

  bgPreview.style.backgroundImage = 'none';
  bgPreview.style.backgroundColor = hex;
  bgHex.textContent = hex;
  const obj = { type: 'color', value: hex };
  localStorage.setItem(BG_KEY, JSON.stringify(obj));
  updateSavedBgText();
  sendBackgroundToServer(obj);
}


if (colorDot && colorPicker) {
  colorDot.addEventListener('click', () => {
    colorPicker.click();
  });
}

if (hexInput && colorPicker) {
  hexInput.addEventListener('click', () => {
    colorPicker.click();
  });
  hexInput.style.cursor = 'pointer';
}
const bgSaved = localStorage.getItem(BG_KEY);
if (bgSaved) {
  try {
    const v = JSON.parse(bgSaved);
    if (v.type === 'image' || v.type === 'url' || v.type === 'upload') {
      bgPreview.style.backgroundImage = `url(${v.value})`;
      bgPreview.style.backgroundSize = 'cover';
      bgPreview.style.backgroundPosition = 'center';
    } else {
      bgPreview.style.backgroundImage = 'none';
      bgPreview.style.backgroundColor = v.value;
      colorPicker.value = v.value;
      hexInput.value = v.value;
      colorDot.style.background = v.value;
      bgHex.textContent = v.value;
    }
  } catch (e) {
    console.warn('invalid bg saved');
  }
} else { setColor('#ffffff'); }
function updateSavedBgText() {
  const raw = localStorage.getItem(BG_KEY);
  if (!raw) { savedBg.textContent = 'None'; return; }
  try { const v = JSON.parse(raw); savedBg.textContent = v.type + ' — ' + (typeof v.value === 'string' && v.value.length > 40 ? v.value.slice(0, 40) + '...' : v.value); } catch (e) { savedBg.textContent = 'None' }
}
updateSavedBgText();
colorPicker.addEventListener('input', (e) => setColor(e.target.value));
hexInput.addEventListener('change', (e) => { const v = e.target.value.trim(); if (/^#([0-9A-Fa-f]{3}){1,2}$/.test(v)) setColor(v); else alert('Please enter a valid hex, e.g. #282c34'); });
applyColorBtn.addEventListener('click', () => { const h = hexInput.value.trim(); if (/^#([0-9A-Fa-f]{3}){1,2}$/.test(h)) { setColor(h); console.log('Applied color selection:', h); } else alert('Invalid hex'); });
const uploadBtn = document.getElementById('uploadBtn');
if (uploadBtn && uploadFile) {
  uploadBtn.addEventListener('click', () => uploadFile.click());
}
uploadFile.addEventListener('change', async (e) => {
  const file = e.target.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = async () => {
    const dataUrl = reader.result;
    const obj = { type: 'upload', name: file.name, value: dataUrl };
    localStorage.setItem(BG_KEY, JSON.stringify(obj));
    bgPreview.style.backgroundImage = `url(${dataUrl})`;
    bgPreview.style.backgroundSize = 'cover';
    bgPreview.style.backgroundPosition = 'center';
    updateSavedBgText();
    await sendBackgroundToServer({ type: 'upload', value: dataUrl });
  };
  reader.readAsDataURL(file);
});
useUrlBtn.addEventListener('click', async () => {
  const url = urlInput.value.trim();
  if (!url) return alert('Enter a valid link');
  if (!/^https?:\/\//i.test(url)) return alert('URL must start with http:// or https://');
  const obj = { type: 'image', value: url };
  localStorage.setItem(BG_KEY, JSON.stringify(obj));
  bgPreview.style.backgroundImage = `url(${url})`;
  bgPreview.style.backgroundSize = 'cover';
  bgPreview.style.backgroundPosition = 'center';
  updateSavedBgText();
  await sendBackgroundToServer(obj);
});

if (colorDot && colorPicker) {
  colorDot.addEventListener('click', () => {
    colorPicker.click();
  });
}

const initDensity = localStorage.getItem(DENSITY_KEY) || 'comfortable';
function applyDensity(d) { if (d === 'compact') body.classList.add('compact'); else body.classList.remove('compact'); const densityEl = document.getElementById('densitySelect'); if (densityEl) densityEl.value = d; }
applyDensity(initDensity);
const densitySelectEl = document.getElementById('densitySelect');
if (densitySelectEl) densitySelectEl.addEventListener('change', (e) => { localStorage.setItem(DENSITY_KEY, e.target.value); applyDensity(e.target.value); });
const initAnim = localStorage.getItem(ANIM_KEY);
const animationsToggleEl = document.getElementById('animationsToggle');
if (animationsToggleEl) {
  animationsToggleEl.checked = initAnim === null ? true : initAnim === 'true';
  function applyAnimations(enabled) { if (!enabled) body.classList.add('no-animations'); else body.classList.remove('no-animations'); }
  applyAnimations(animationsToggleEl.checked);
  animationsToggleEl.addEventListener('change', (e) => { localStorage.setItem(ANIM_KEY, e.target.checked); applyAnimations(e.target.checked); });
}
const savedAccent = localStorage.getItem(ACCENT_KEY);
if (savedAccent) document.documentElement.style.setProperty('--accent', savedAccent);
if (accentPicker) accentPicker.addEventListener('input', (e) => { document.documentElement.style.setProperty('--accent', e.target.value); localStorage.setItem(ACCENT_KEY, e.target.value); });
if (changePassCard) changePassCard.addEventListener('click', () => { modalOverlay?.classList.add('show'); changeModal?.classList.add('show'); });
const closePasswordModal = () => { modalOverlay?.classList.remove('show'); changeModal?.classList.remove('show'); };
if (modalCancel) modalCancel.addEventListener('click', closePasswordModal);
if (modalCancelBtn) modalCancelBtn.addEventListener('click', closePasswordModal);
if (modalOverlay) modalOverlay.addEventListener('click', (e) => { if (e.target === modalOverlay) closePasswordModal(); });

document.querySelectorAll('.password-toggle').forEach(btn => {
  if (btn.dataset.listenerAdded) return;
  btn.dataset.listenerAdded = 'true';
  btn.addEventListener('click', (e) => {
    e.preventDefault();
    e.stopPropagation();
    const targetId = btn.dataset.target;
    const input = targetId
      ? document.getElementById(targetId)
      : btn.parentElement?.querySelector('input');
    if (!input) return;
    const icon = btn.querySelector('i');
    if (input.type === 'password') {
      input.type = 'text';
      if (icon) icon.classList.replace('fa-eye', 'fa-eye-slash');
      btn.setAttribute('aria-label', 'Hide password');
    } else {
      input.type = 'password';
      if (icon) icon.classList.replace('fa-eye-slash', 'fa-eye');
      btn.setAttribute('aria-label', 'Show password');
    }
  });
});

const newPasswordInput = document.getElementById('modalNew');
const strengthIndicator = document.getElementById('passwordStrength');
if (newPasswordInput && strengthIndicator) {
  newPasswordInput.addEventListener('input', (e) => {
    const val = e.target.value;
    let strength = 'weak';
    if (val.length >= 12 && /[A-Z]/.test(val) && /[0-9]/.test(val) && /[^A-Za-z0-9]/.test(val)) {
      strength = 'strong';
    } else if (val.length >= 10 && /[A-Z]/.test(val) && /[0-9]/.test(val)) {
      strength = 'good';
    } else if (val.length >= 8) {
      strength = 'fair';
    }
    strengthIndicator.innerHTML = val ? `<div class="password-strength-bar ${strength}"></div>` : '';
  });
}

modalSave.addEventListener('click', async () => {
  const cur = document.getElementById('modalCurrent').value;
  const nw = document.getElementById('modalNew').value;
  const cf = document.getElementById('modalConfirm').value;
  const twoFactorCode = document.getElementById('modalTwoFactor')?.value || '';
  const feedbackEl = document.getElementById('changePasswordFeedback');

  const showFeedback = (msg, isError = false) => {
    if (feedbackEl) {
      feedbackEl.textContent = msg;
      feedbackEl.className = 'node-feedback' + (isError ? ' error' : ' success');
      feedbackEl.style.display = msg ? 'block' : 'none';
    }
  };

  if (!cur || !nw || !cf) {
    showFeedback('Please fill all fields', true);
    return;
  }
  if (nw.length < 8) {
    showFeedback('New password must be at least 8 characters', true);
    return;
  }
  if (nw !== cf) {
    showFeedback('Passwords do not match', true);
    return;
  }

  try {
    modalSave.disabled = true;
    modalSave.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Updating...';

    const res = await fetch('/api/settings/change-password', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ current: cur, newPassword: nw, confirm: cf, twoFactorCode: twoFactorCode })
    });
    const data = await res.json().catch(() => ({ error: 'Invalid server response' }));

    if (!res.ok) {
      if (data.requires2FA) {
        const twoFactorField = document.getElementById('twoFactorField');
        if (twoFactorField) twoFactorField.style.display = 'block';
        showFeedback('Please enter your 2FA code', true);
        document.getElementById('modalTwoFactor')?.focus();
      } else {
        showFeedback(data.error || 'Failed to change password', true);
      }
      return;
    }

    showFeedback('Password changed successfully!', false);

    setTimeout(() => {
      document.getElementById('modalCurrent').value = '';
      document.getElementById('modalNew').value = '';
      document.getElementById('modalConfirm').value = '';
      if (document.getElementById('modalTwoFactor')) document.getElementById('modalTwoFactor').value = '';
      if (document.getElementById('twoFactorField')) document.getElementById('twoFactorField').style.display = 'none';
      modalOverlay.classList.remove('show'); changeModal.classList.remove('show');
      window.location.reload();
    }, 1500);
  } catch (err) {
    console.error(err);
    showFeedback('Network error', true);
  } finally {
    modalSave.disabled = false;
    modalSave.innerHTML = '<i class="fa-solid fa-check"></i> Update Password';
  }
});
async function loadServers(page, search) {
  if (typeof page === 'undefined') page = _serversPage;
  if (typeof search === 'undefined') search = _serversSearch;
  _serversPage = page;
  _serversSearch = search;
  const isFirstLoad = !serversListEl.querySelector('.server-row');
  if (isFirstLoad) {
    serversListEl.innerHTML = `<div style="padding:12px;color:var(--muted)">Loading…</div>`;
  }
  try {
    const params = new URLSearchParams({ page: String(page), limit: String(_serversLimit) });
    if (search) params.set('search', search);
    const res = await fetch('/api/settings/servers?' + params.toString());
    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: 'failed' }));
      serversListEl.innerHTML = `<div style="padding:12px;color:var(--muted)">Failed to load servers</div>`;
      console.warn(err);
      return;
    }
    const data = await res.json();
    const items = Array.isArray(data.items)
      ? data.items
      : (Array.isArray(data.names) ? data.names.map(n => ({ name: n, isLocal: true, nodeId: null })) : []);
    const total = Number(data.total || items.length);
    const totalPages = Number(data.totalPages || 1);
    renderServers(items);
    renderServersPagination(page, totalPages, total);
  } catch (e) {
    console.error(e);
    serversListEl.innerHTML = `<div style="padding:12px;color:var(--muted)">Network error</div>`;
  }
}
function shortId(id) {
  const s = String(id || '');
  return s.length > 8 ? s.slice(0, 8) : s || '—';
}
function renderServers(list) {
  if (!Array.isArray(list) || list.length === 0) {
    serversListEl.innerHTML = `<div style="padding:12px;color:var(--muted)">No servers found.</div>`;
    return;
  }
  serversListEl.innerHTML = '';
  list.forEach(item => {
    const name = item.name || String(item);
    const isLocal = (item && typeof item.isLocal === 'boolean') ? item.isLocal : (!item.nodeId);
    const chipText = isLocal ? 'ID: localhost' : `ID: ${item.nodeId}`;
    const templateLabel = displayTemplateName(item?.template);
    const row = document.createElement('div');
    row.className = 'server-row clickable-card';
    row.dataset.name = name;
    row.dataset.template = item?.template || '';
    row.dataset.nodeId = item?.nodeId ? String(item.nodeId) : '';
    row.innerHTML = `
      <div class="left">
        <div class="iconWrap"><i class="fa-solid fa-folder"></i></div>
        <div class="name">${escapeHtml(item.displayName || name)}</div>
        <span class="inline-badge" style="margin-left:8px">${escapeHtml(chipText)}</span>
        <span class="template-chip" title="Current template for this server">Template: ${escapeHtml(templateLabel)}</span>
      </div>
      <div class="actions">
        <button class="btn ghost manage-resources" style="transition: transform(none);" data-name="${encodeURIComponent(name)}" title="Manage resources for ${escapeHtml(name)}">
          <i class="fa-solid fa-gauge-high"></i>
        </button>
        <button class="btn ghost port-forward" style="transition: transform(none);${isLocal ? 'opacity:.45;pointer-events:none;' : ''}" data-name="${encodeURIComponent(name)}" title="${isLocal ? 'Port forwarding requires a remote node' : 'Port forwarding for ' + escapeHtml(name)}">
          <i class="fa-solid fa-shuffle"></i>
        </button>
        <button class="btn ghost transfer-server" style="transition: transform(none);${isLocal ? 'opacity:.45;pointer-events:none;' : ''}" data-name="${encodeURIComponent(name)}" title="Transfer ${escapeHtml(name)} to another node">
          <i class="fa-solid fa-right-left"></i>
        </button>
        <button class="btn ghost change-template" style="transition: transform(none);" data-name="${encodeURIComponent(name)}" title="Change template for ${escapeHtml(name)}">
          <i class="fa-solid fa-wand-magic-sparkles"></i>
        </button>
        <button class="btn ghost delete-server" style="transition: transform(none);" data-name="${encodeURIComponent(name)}" title="Delete ${escapeHtml(name)}">
          <i class="fa-solid fa-trash"></i>
        </button>
      </div>
    `;
    serversListEl.appendChild(row);
  });
}
function renderServersPagination(currentPage, totalPages, total) {
  const el = document.getElementById('serversPagination');
  if (!el) return;
  if (totalPages <= 1) { el.innerHTML = ''; return; }
  const startItem = (currentPage - 1) * _serversLimit + 1;
  const endItem = Math.min(currentPage * _serversLimit, total);
  el.innerHTML = `
    <div class="nodes-pagination-inner">
      <span class="nodes-pagination-info">${startItem}–${endItem} of ${total} servers</span>
      <div class="nodes-pagination-btns">
        <button class="btn ghost btn-sm" ${currentPage <= 1 ? 'disabled' : ''} data-servers-page="${currentPage - 1}"><i class="fa-solid fa-chevron-left"></i></button>
        <span class="nodes-pagination-current">Page ${currentPage} / ${totalPages}</span>
        <button class="btn ghost btn-sm" ${currentPage >= totalPages ? 'disabled' : ''} data-servers-page="${currentPage + 1}"><i class="fa-solid fa-chevron-right"></i></button>
      </div>
    </div>
  `;
  el.querySelectorAll('button[data-servers-page]').forEach(btn => {
    btn.addEventListener('click', () => {
      const p = parseInt(btn.dataset.serversPage, 10);
      if (p >= 1 && p <= totalPages) loadServers(p);
    });
  });
}
function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, function (m) { return ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m]); });
}

const PANEL_EWS_ICON_SIZE = "1.6rem";

function isPanelEwsIcon(iconClass) {
  return String(iconClass || "").split(/\s+/).includes("fa-panel-ews");
}

function ensurePanelEwsSvgStyles() {
  if (document.getElementById("panel-ews-settings-styles")) return;
  const style = document.createElement("style");
  style.id = "panel-ews-settings-styles";
  style.textContent = `
    .panel-ews-svg {
      width: ${PANEL_EWS_ICON_SIZE};
      height: ${PANEL_EWS_ICON_SIZE};
      display: block;
      flex: 0 0 auto;
      transition: transform 180ms ease;
    }

    .quick-action-item-icon .panel-ews-svg,
    .quick-action-preview-card .panel-ews-svg,
    .icon-preview .panel-ews-svg {
      margin: 0 auto;
    }

    .quick-action-preview-card.qa-anim-rotate:hover .panel-ews-svg {
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
    icon.replaceWith(createPanelEwsSvg());
  });
}
const templateNameMap = new Map();
const templatePickerState = { selected: '', selectedNorm: '', server: '', originalTemplate: '' };
registerModalCustomStateReader('serverTemplateOverlay', () => ({
  server: templatePickerState.server || '',
  selected: normalizeTemplateIdClient(templatePickerState.selected || ''),
  original: templatePickerState.originalTemplate || ''
}));
function normalizeTemplateIdClient(tpl) {
  const raw = (tpl || '').toString().trim().toLowerCase();
  if (!raw) return '';
  if (["discord-bot", "discord", "discord bot", "bot"].includes(raw)) return "discord-bot";
  if (["node", "nodejs", "node.js"].includes(raw)) return "nodejs";
  if (["python", "py"].includes(raw)) return "python";
  if (["mc", "minecraft"].includes(raw)) return "minecraft";
  return raw;
}
function updateTemplateNameMap(list) {
  templateNameMap.clear();
  (Array.isArray(list) ? list : []).forEach(tpl => {
    const norm = normalizeTemplateIdClient(tpl?.id);
    if (norm) templateNameMap.set(norm, tpl?.name || tpl?.id || norm);
  });
}
function displayTemplateName(id) {
  const norm = normalizeTemplateIdClient(id);
  if (!norm) return 'No template';
  return templateNameMap.get(norm) || (id ? String(id) : norm);
}
function findServerRowByName(name) {
  return Array.from(serversListEl.children || []).find(r => r.dataset && r.dataset.name === name) || null;
}
async function loadTemplates(page, search) {
  if (!templatesListEl) return;
  if (typeof page === 'undefined') page = _templatesPage;
  if (typeof search === 'undefined') search = _templatesSearch;
  _templatesPage = page;
  _templatesSearch = search;
  if (templatesFeedbackEl) templatesFeedbackEl.textContent = '';
  const isFirstLoad = !templatesListEl.querySelector('.template-card');
  if (isFirstLoad) {
    templatesListEl.innerHTML = `<div style="padding:12px;color:var(--muted)">Loading templates…</div>`;
  }
  try {
    const params = new URLSearchParams({ page: String(page), limit: String(_templatesLimit) });
    if (search) params.set('search', search);
    const res = await fetch('/api/settings/templates?' + params.toString());
    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: 'failed' }));
      templatesListEl.innerHTML = `<div style="padding:12px;color:var(--muted)">Failed to load templates</div>`;
      if (templatesFeedbackEl) templatesFeedbackEl.textContent = err.error || 'Unable to load templates.';
      console.warn(err);
      return;
    }
    const data = await res.json();
    const templates = Array.isArray(data.templates) ? data.templates : [];
    const total = Number(data.total || templates.length);
    const totalPages = Number(data.totalPages || 1);
    updateTemplateNameMap(templates);
    renderTemplates(templates);
    renderTemplatesPagination(page, totalPages, total);
  } catch (e) {
    console.error(e);
    templatesListEl.innerHTML = `<div style="padding:12px;color:var(--muted)">Network error</div>`;
  }
}
function renderTemplates(list) {
  if (!templatesListEl) return;
  if (!Array.isArray(list) || list.length === 0) {
    templatesListEl.innerHTML = `<div style="padding:16px;border:1px dashed rgba(255,255,255,0.08);border-radius:12px;background:rgba(255,255,255,0.02);color:var(--muted)">No templates found. Create one to get started.</div>`;
    return;
  }
  templatesListEl.innerHTML = '';
  list.forEach(tpl => {
    const row = document.createElement('div');
    row.className = 'template-card clickable-card';
    row.innerHTML = `
          <div class="template-card__body">
            <div class="template-card__icon"><i class="fa-solid fa-file"></i></div>
            <div class="template-card__content">
              <div class="template-card__title">
                <div class="name">${escapeHtml(tpl.name || tpl.id)}</div>
                <span class="inline-badge">ID: ${escapeHtml(tpl.id || '—')}</span>
              </div>
              <div class="template-card__description">${escapeHtml(tpl.description || 'No description')}</div>
              <div class="template-card__meta">
                <span><i class="fa-solid fa-cube"></i> ${escapeHtml((tpl.docker && tpl.docker.image) ? tpl.docker.image : 'Custom image')}</span>
                <span><i class="fa-solid fa-tag"></i> ${escapeHtml((tpl.docker && tpl.docker.tag) ? tpl.docker.tag : 'latest')}</span>
              </div>
            </div>
            <div class="template-card__actions">
              <button class="btn ghost delete-template" data-id="${encodeURIComponent(tpl.id || '')}" data-name="${encodeURIComponent(tpl.name || tpl.id || '')}" title="Delete template">
                <i class="fa-solid fa-trash"></i>
              </button>
            </div>
          </div>
        `;
    templatesListEl.appendChild(row);
  });
}
function renderTemplatesPagination(currentPage, totalPages, total) {
  const el = document.getElementById('templatesPagination');
  if (!el) return;
  if (totalPages <= 1) { el.innerHTML = ''; return; }
  const startItem = (currentPage - 1) * _templatesLimit + 1;
  const endItem = Math.min(currentPage * _templatesLimit, total);
  el.innerHTML = `
    <div class="nodes-pagination-inner">
      <span class="nodes-pagination-info">${startItem}–${endItem} of ${total} templates</span>
      <div class="nodes-pagination-btns">
        <button class="btn ghost btn-sm" ${currentPage <= 1 ? 'disabled' : ''} data-tpl-page="${currentPage - 1}"><i class="fa-solid fa-chevron-left"></i></button>
        <span class="nodes-pagination-current">Page ${currentPage} / ${totalPages}</span>
        <button class="btn ghost btn-sm" ${currentPage >= totalPages ? 'disabled' : ''} data-tpl-page="${currentPage + 1}"><i class="fa-solid fa-chevron-right"></i></button>
      </div>
    </div>
  `;
  el.querySelectorAll('button[data-tpl-page]').forEach(btn => {
    btn.addEventListener('click', () => {
      const p = parseInt(btn.dataset.tplPage, 10);
      if (p >= 1 && p <= totalPages) loadTemplates(p);
    });
  });
}
function openTemplateModal() {
  createTemplateOverlay.classList.add('show');
  createTemplateModal.classList.add('show');
  createTemplateFeedback.textContent = '';
}
function closeTemplateModal() {
  createTemplateOverlay.classList.remove('show');
  createTemplateModal.classList.remove('show');
  createTemplateFeedback.textContent = '';
}
[createTemplateCancel, createTemplateClose, createTemplateOverlay].forEach(el => {
  if (!el) return;
  el.addEventListener('click', (e) => {
    if (e.target === el || el === createTemplateCancel || el === createTemplateClose) {
      closeTemplateModal();
    }
  });
});
if (openCreateTemplateBtn) {
  openCreateTemplateBtn.addEventListener('click', () => {
    ct_name.value = '';
    ct_id.value = '';
    ct_description.value = '';
    ct_template_image.value = '';
    ct_image.value = '';
    ct_tag.value = 'latest';
    openTemplateModal();
  });
}
if (createTemplateSave) {
  createTemplateSave.addEventListener('click', async () => {
    const id = (ct_id.value || '').trim();
    const name = (ct_name.value || '').trim();
    const description = (ct_description.value || '').trim();
    const templateImage = (ct_template_image.value || '').trim();
    const image = (ct_image.value || '').trim();
    const tag = (ct_tag.value || '').trim();
    if (!name || !id || !image) {
      createTemplateFeedback.textContent = 'Please fill out name, id and docker image.';
      return;
    }
    try {
      createTemplateSave.disabled = true;
      createTemplateFeedback.textContent = 'Saving template…';
      const res = await fetch('/api/settings/templates', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id, name, description, dockerImage: image, dockerTag: tag, templateImage })
      });
      const data = await res.json().catch(() => ({ error: 'Invalid response' }));
      if (!res.ok) {
        createTemplateFeedback.textContent = data.error || 'Failed to save template';
        createTemplateSave.disabled = false;
        return;
      }
      createTemplateFeedback.textContent = 'Template created!';
      setTimeout(closeTemplateModal, 400);
      loadTemplates();
    } catch (e) {
      console.error(e);
      createTemplateFeedback.textContent = 'Network error while saving template';
    } finally {
      createTemplateSave.disabled = false;
    }
  });
}
async function deleteTemplate(templateId, templateName, btn) {
  if (!templateId) return;
  if (!confirm(`Delete template "${templateName || templateId}"? It cannot be undone.`)) return;
  try {
    if (btn) {
      btn.disabled = true;
      btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>';
    }
    const res = await fetch(`/api/settings/templates/${encodeURIComponent(templateId)}`, { method: 'DELETE' });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      const serversMsg = Array.isArray(data.servers) && data.servers.length ? ` In use by: ${data.servers.join(', ')}` : '';
      if (templatesFeedbackEl) templatesFeedbackEl.textContent = (data.error || 'Failed to delete template.') + serversMsg;
      return;
    }
    if (templatesFeedbackEl) templatesFeedbackEl.textContent = `Template "${templateName || templateId}" deleted.`;
    loadTemplates();
  } catch (err) {
    console.error(err);
    if (templatesFeedbackEl) templatesFeedbackEl.textContent = 'Network error while deleting template.';
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.innerHTML = '<i class="fa-solid fa-trash"></i>';
    }
  }
}
if (templatesListEl) {
  templatesListEl.addEventListener('click', (e) => {
    const btn = e.target.closest('.delete-template');
    if (!btn) return;
    const id = decodeURIComponent(btn.dataset.id || '');
    const name = decodeURIComponent(btn.dataset.name || '') || id;
    deleteTemplate(id, name, btn);
  });
}
function closeServerTemplateModal() {
  if (serverTemplateOverlay) serverTemplateOverlay.classList.remove('show');
  if (serverTemplateModal) serverTemplateModal.classList.remove('show');
  templatePickerState.selected = '';
  templatePickerState.selectedNorm = '';
  templatePickerState.server = '';
  templatePickerState.originalTemplate = '';
  if (serverTemplateModal) {
    serverTemplateModal.dataset.server = '';
    serverTemplateModal.dataset.selected = '';
  }
  if (serverTemplateList) serverTemplateList.innerHTML = '';
  if (serverTemplateFeedback) serverTemplateFeedback.textContent = '';
}
function selectServerTemplate(id) {
  const normalized = normalizeTemplateIdClient(id);
  if (!normalized) return;
  templatePickerState.selected = id;
  templatePickerState.selectedNorm = normalized;
  if (serverTemplateModal) serverTemplateModal.dataset.selected = id;
  Array.from(serverTemplateList.querySelectorAll('.template-option-card')).forEach(card => {
    const normCard = normalizeTemplateIdClient(card.dataset.id);
    card.classList.toggle('active', normCard === normalized);
  });
}
function renderServerTemplateOptions(list, currentTemplate) {
  if (!serverTemplateList) return;
  const seen = new Set();
  const unique = [];
  (Array.isArray(list) ? list : []).forEach(tpl => {
    const norm = normalizeTemplateIdClient(tpl?.id);
    if (!norm || seen.has(norm)) return;
    seen.add(norm);
    const canonicalId = tpl?.id || norm;
    unique.push({ ...tpl, _norm: norm, _id: canonicalId });
  });
  if (unique.length === 0) {
    serverTemplateList.innerHTML = `<div style="padding:12px;color:var(--muted)">No templates available.</div>`;
    return;
  }
  serverTemplateList.innerHTML = '';
  unique.forEach(tpl => {
    const card = document.createElement('div');
    card.className = 'template-option-card';
    card.dataset.id = tpl._id;

    const templateImage = tpl.template_image || tpl.templateImage || '';
    if (templateImage) {
      card.style.setProperty('--template-bg', `url(${templateImage})`);
      card.innerHTML = `
            <div class="template-card-check"><i class="fa-solid fa-check"></i></div>
            <div class="template-option-content">
              <div class="template-option-title"><i class="fa-solid fa-layer-group"></i> ${escapeHtml(tpl.name || tpl.id)}</div>
              <div class="template-option-desc">${escapeHtml(tpl.description || 'No description')}</div>
              <div class="template-option-pill"><i class="fa-brands fa-docker"></i> ${escapeHtml((tpl.docker && tpl.docker.image) ? tpl.docker.image : 'Custom image')}</div>
            </div>
          `;
      card.style.cssText = `--bg-image: url('${templateImage.replace(/'/g, "\\'")}')`;
    } else {
      card.innerHTML = `
            <div class="template-card-check"><i class="fa-solid fa-check"></i></div>
            <div class="template-option-content">
              <div class="template-option-title"><i class="fa-solid fa-layer-group"></i> ${escapeHtml(tpl.name || tpl.id)}</div>
              <div class="template-option-desc">${escapeHtml(tpl.description || 'No description')}</div>
              <div class="template-option-pill"><i class="fa-brands fa-docker"></i> ${escapeHtml((tpl.docker && tpl.docker.image) ? tpl.docker.image : 'Custom image')}</div>
            </div>
          `;
    }

    if (tpl._norm === normalizeTemplateIdClient(currentTemplate || templatePickerState.selected)) {
      card.classList.add('active');
      templatePickerState.selected = tpl._id;
      templatePickerState.selectedNorm = tpl._norm;
      if (serverTemplateModal) serverTemplateModal.dataset.selected = tpl._id;
    }
    serverTemplateList.appendChild(card);
  });
  if (!templatePickerState.selected && unique[0]) {
    selectServerTemplate(unique[0]._id);
  }
}
async function openServerTemplateModal(serverName, currentTemplate) {
  if (!serverTemplateOverlay || !serverTemplateModal) return;
  templatePickerState.server = serverName;
  templatePickerState.selected = currentTemplate;
  templatePickerState.selectedNorm = normalizeTemplateIdClient(currentTemplate);
  templatePickerState.originalTemplate = normalizeTemplateIdClient(currentTemplate);
  serverTemplateModal.dataset.server = serverName;
  serverTemplateModal.dataset.selected = templatePickerState.selected || '';
  serverTemplateTitle.textContent = `Change template for ${serverName}`;
  serverTemplateSubtitle.textContent = currentTemplate
    ? `Current template: ${displayTemplateName(currentTemplate)}`
    : 'Select a template to apply to this server.';
  serverTemplateOverlay.classList.add('show');
  serverTemplateModal.classList.add('show');
  if (serverTemplateList) serverTemplateList.innerHTML = `<div style="padding:12px;color:var(--muted)">Loading templates…</div>`;
  if (serverTemplateFeedback) serverTemplateFeedback.textContent = '';
  try {
    const res = await fetch('/api/settings/templates');
    const data = await res.json().catch(() => ({ templates: [] }));
    if (!res.ok) {
      if (serverTemplateFeedback) serverTemplateFeedback.textContent = data.error || 'Failed to load templates.';
      return;
    }
    const templates = Array.isArray(data.templates) ? data.templates : [];
    updateTemplateNameMap(templates);
    renderServerTemplateOptions(templates, currentTemplate);
  } catch (err) {
    console.error(err);
    if (serverTemplateFeedback) serverTemplateFeedback.textContent = 'Network error while loading templates.';
  }
}
if (serverTemplateList) {
  serverTemplateList.addEventListener('click', (e) => {
    const card = e.target.closest('.template-option-card');
    if (!card) return;
    selectServerTemplate(card.dataset.id);
  });
}
if (serverTemplateCancel) {
  serverTemplateCancel.addEventListener('click', closeServerTemplateModal);
}
if (serverTemplateClose) {
  serverTemplateClose.addEventListener('click', closeServerTemplateModal);
}
if (serverTemplateOverlay) {
  serverTemplateOverlay.addEventListener('click', (e) => { if (e.target === serverTemplateOverlay) closeServerTemplateModal(); });
}
if (serverTemplateDone) {
  serverTemplateDone.addEventListener('click', async () => {
    const server = serverTemplateModal?.dataset?.server || '';
    const selected = templatePickerState.selected || serverTemplateModal?.dataset?.selected || '';
    const originalTemplate = templatePickerState.originalTemplate || '';
    if (!server) {
      closeServerTemplateModal();
      return;
    }
    if (!selected) {
      if (serverTemplateFeedback) serverTemplateFeedback.textContent = 'Select a template first.';
      return;
    }
    const selectedNorm = normalizeTemplateIdClient(selected);
    if (selectedNorm === originalTemplate) {
      closeServerTemplateModal();
      return;
    }
    const confirmMsg = `Changing template will REINSTALL the server "${server}". That means:\n\n` +
      ` - All server files will be permanently deleted.\n\n` +
      ` Are you sure you want to continue?`;
    if (!confirm(confirmMsg)) return;
    try {
      serverTemplateDone.disabled = true;
      if (serverTemplateCancel) serverTemplateCancel.disabled = true;
      if (serverTemplateClose) serverTemplateClose.style.pointerEvents = 'none';
      serverTemplateDone.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Reinstalling…';
      if (serverTemplateFeedback) {
        serverTemplateFeedback.textContent = 'Stopping server and deleting files… This may take a few minutes.';
        serverTemplateFeedback.style.color = 'var(--warning, #f0ad4e)';
      }
      const res = await fetch(`/api/settings/servers/${encodeURIComponent(server)}/template`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ templateId: selected })
      });
      const data = await res.json().catch(() => ({ error: 'Invalid response' }));
      if (!res.ok) {
        if (serverTemplateFeedback) {
          serverTemplateFeedback.textContent = data.error || 'Failed to change template.';
          serverTemplateFeedback.style.color = 'var(--danger, #d9534f)';
        }
        return;
      }
      if (serverTemplateFeedback) {
        serverTemplateFeedback.textContent = 'Template changed and server reinstalled successfully!';
        serverTemplateFeedback.style.color = 'var(--success, #5cb85c)';
      }
      const row = findServerRowByName(server);
      if (row) {
        row.dataset.template = selected;
        const chip = row.querySelector('.template-chip');
        if (chip) chip.textContent = `Template: ${displayTemplateName(selected)}`;
      }
      setTimeout(() => { closeServerTemplateModal(); }, 1200);
    } catch (err) {
      console.error(err);
      if (serverTemplateFeedback) {
        serverTemplateFeedback.textContent = 'Network error while changing template. Please try again.';
        serverTemplateFeedback.style.color = 'var(--danger, #d9534f)';
      }
    } finally {
      serverTemplateDone.disabled = false;
      if (serverTemplateCancel) serverTemplateCancel.disabled = false;
      if (serverTemplateClose) serverTemplateClose.style.pointerEvents = '';
      serverTemplateDone.innerHTML = '<i class="fa-solid fa-floppy-disk"></i> Done';
    }
  });
}
createServerBtn.addEventListener('click', async () => {
  const raw = createServerNameInput.value.trim();
  if (!raw) return alert('Enter a server name');
  if (!/^[\w\-. ]{1,80}$/.test(raw)) return alert('Invalid name — allowed: letters, numbers, -, _, ., space');
  try {
    createServerBtn.disabled = true;
    createServerBtn.innerHTML = 'Creating...';
    const res = await fetch('/api/settings/servers', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: raw })
    });
    const data = await res.json().catch(() => ({ error: 'Invalid response' }));
    if (!res.ok) {
      alert(data.error || 'Failed to create server');
      createServerBtn.disabled = false;
      createServerBtn.innerHTML = '<i class="fa-solid fa-plus"></i> Create';
      return;
    }
    createServerNameInput.value = '';
    loadServers();
  } catch (err) {
    console.error(err);
    alert('Network error while creating server');
  } finally {
    createServerBtn.disabled = false;
    createServerBtn.innerHTML = '<i class="fa-solid fa-plus"></i> Create';
  }
});
serversListEl.addEventListener('click', async (e) => {
  const resourceBtn = e.target.closest('.manage-resources');
  if (resourceBtn) {
    const row = resourceBtn.closest('.server-row');
    const serverName = row?.dataset?.name || (resourceBtn.dataset.name ? decodeURIComponent(resourceBtn.dataset.name) : '');
    if (serverName) openResourcesModal(serverName);
    return;
  }

  const pfBtn = e.target.closest('.port-forward');
  if (pfBtn) {
    const row = pfBtn.closest('.server-row');
    const serverName = row?.dataset?.name || (pfBtn.dataset.name ? decodeURIComponent(pfBtn.dataset.name) : '');
    if (serverName) openPortForwardModal(serverName);
    return;
  }

  const transferBtn = e.target.closest('.transfer-server');
  if (transferBtn) {
    const row = transferBtn.closest('.server-row');
    const serverName = row?.dataset?.name || (transferBtn.dataset.name ? decodeURIComponent(transferBtn.dataset.name) : '');
    const currentNodeId = row?.dataset?.nodeId ? String(row.dataset.nodeId) : '';
    if (serverName) openTransferModal(serverName, currentNodeId);
    return;
  }

  const changeBtn = e.target.closest('.change-template');
  if (changeBtn) {
    const row = changeBtn.closest('.server-row');
    const serverName = row?.dataset?.name || (changeBtn.dataset.name ? decodeURIComponent(changeBtn.dataset.name) : '');
    const currentTemplate = row?.dataset?.template || '';
    if (serverName) openServerTemplateModal(serverName, currentTemplate);
    return;
  }
  const btn = e.target.closest('.delete-server');
  if (!btn) return;
  const encName = btn.dataset.name;
  if (!encName) return;
  const name = decodeURIComponent(encName);
  const ok = confirm(`Delete server "${name}"? This will permanently remove the folder and its contents.${'\n'}If it is on a node, the node container and files will be removed too.`);
  if (!ok) return;
  try {
    btn.disabled = true;
    const res = await fetch(`/api/settings/servers/${encodeURIComponent(name)}`, { method: 'DELETE' });
    const data = await res.json().catch(() => ({ error: 'Invalid response' }));
    if (!res.ok) {
      alert(data.error || 'Failed to delete server');
      btn.disabled = false;
      return;
    }
    const row = btn.closest('.server-row');
    if (row) row.remove();
    if (!serversListEl.children.length) {
      loadServers();
    }
  } catch (err) {
    console.error(err);
    alert('Network error while deleting server');
    btn.disabled = false;
  }
});

const transferModalOverlay = document.getElementById('transferModalOverlay');
const transferModal = document.getElementById('transferModal');
const transferModalTitle = document.getElementById('transferModalTitle');
const transferModalSubtitle = document.getElementById('transferModalSubtitle');
const transferModalClose = document.getElementById('transferModalClose');
const transferCancel = document.getElementById('transferCancel');
const transferFeedback = document.getElementById('transferFeedback');
const transferNodeList = document.getElementById('transferNodeList');
const transferProgress = document.getElementById('transferProgress');
const transferProgressFill = document.getElementById('transferProgressFill');
const transferProgressText = document.getElementById('transferProgressText');

let currentTransferServer = '';
let transferPollTimer = null;

function setTransferFeedback(msg, isError = false) {
  if (!transferFeedback) return;
  transferFeedback.textContent = msg || '';
  transferFeedback.className = 'node-feedback' + (isError ? ' error' : '');
}

function setTransferProgress(job) {
  if (!transferProgress || !transferProgressFill || !transferProgressText) return;
  if (!job) {
    transferProgress.style.display = 'none';
    transferProgressFill.style.width = '0%';
    transferProgressText.textContent = '';
    return;
  }
  transferProgress.style.display = 'block';
  const pct = Math.max(0, Math.min(100, Number(job.percent || 0)));
  transferProgressFill.style.width = `${pct}%`;
  const status = job.status ? String(job.status) : 'running';
  transferProgressText.textContent = `${status}${job.message ? ' — ' + job.message : ''}`;
}

function showTransferModal(show) {
  if (!transferModalOverlay) return;
  if (show) {
    transferModalOverlay.setAttribute('aria-hidden', 'false');
    transferModalOverlay.classList.add('show');
  } else {
    transferModalOverlay.setAttribute('aria-hidden', 'true');
    transferModalOverlay.classList.remove('show');
    currentTransferServer = '';
    setTransferFeedback('');
    setTransferProgress(null);
    if (transferNodeList) transferNodeList.innerHTML = '';
    if (transferPollTimer) {
      clearInterval(transferPollTimer);
      transferPollTimer = null;
    }
  }
}

async function fetchTransferJob(serverName) {
  const res = await fetch(`/api/settings/servers/${encodeURIComponent(serverName)}/transfer`, { cache: 'no-store' });
  if (!res.ok) return null;
  const data = await res.json().catch(() => ({}));
  return data.job || null;
}

async function pollTransfer(serverName) {
  try {
    const job = await fetchTransferJob(serverName);
    if (job) {
      setTransferProgress(job);
      if (job.status === 'error') setTransferFeedback(job.error || job.message || 'Transfer failed', true);
      if (job.finishedAt) {
        if (job.status === 'done') setTransferFeedback('Transfer complete.', false);
        clearInterval(transferPollTimer);
        transferPollTimer = null;
        loadServers();
      }
    }
  } catch { }
}

async function loadTransferNodes(serverName, currentNodeId) {
  if (!transferNodeList) return;
  transferNodeList.innerHTML = `<div style="padding:12px;color:rgba(255,255,255,0.6)">Loading nodes…</div>`;
  try {
    const res = await fetch('/api/nodes', { cache: 'no-store' });
    const data = await res.json().catch(() => ({ nodes: [] }));
    if (!res.ok) throw new Error(data.error || 'Failed to load nodes');
    const nodes = Array.isArray(data.nodes) ? data.nodes : [];
    transferNodeList.innerHTML = '';
    const filtered = nodes.filter(n => {
      const id = String(n.id || '').trim();
      const uuid = String(n.uuid || '').trim();
      const name = String(n.name || '').trim();
      const anyId = String(id || uuid || name || '').trim();
      if (!anyId) return false;
      if (currentNodeId && (String(currentNodeId) === id || String(currentNodeId) === uuid || String(currentNodeId) === name)) return false;
      if (n.online !== true) return false;
      return true;
    });
    if (!filtered.length) {
      transferNodeList.innerHTML = `<div style="padding:12px;color:rgba(255,255,255,0.6)">No nodes available.</div>`;
      return;
    }
    filtered.forEach(n => {
      const id = n.uuid || n.id || n.name;
      const item = document.createElement('div');
      item.className = 'transfer-node-item';
      item.dataset.id = id;
      item.innerHTML = `
        <div class="transfer-node-meta">
          <span class="status-dot ${n.port_ok === true ? 'green' : (n.port_ok === false ? 'red' : 'gray')}"></span>
          <div>
            <div class="transfer-node-title">${escapeHtml(n.name || id)}</div>
            <div class="transfer-node-sub">${escapeHtml(n.address || '')} • API ${Number(n.api_port || 8080)}</div>
          </div>
        </div>
        <div class="transfer-node-actions">
          <button class="enterprise-btn-primary" type="button">Transfer</button>
        </div>
      `;
      item.querySelector('button')?.addEventListener('click', async (ev) => {
        ev.stopPropagation();
        await startTransfer(serverName, id);
      });
      item.addEventListener('click', async () => {
        await startTransfer(serverName, id);
      });
      transferNodeList.appendChild(item);
    });
  } catch (e) {
    console.error(e);
    transferNodeList.innerHTML = `<div style="padding:12px;color:rgba(255,255,255,0.6)">Failed to load nodes.</div>`;
  }
}

async function startTransfer(serverName, targetNodeId) {
  try {
    setTransferFeedback('Starting transfer…');
    setTransferProgress({ status: 'preparing', percent: 0, message: 'Starting…' });
    const res = await fetch(`/api/settings/servers/${encodeURIComponent(serverName)}/transfer`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ targetNodeId })
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || 'Failed to start transfer');
    if (data.job) setTransferProgress(data.job);
    setTransferFeedback('Transfer in progress…');
    if (!transferPollTimer) {
      transferPollTimer = setInterval(() => pollTransfer(serverName), 1500);
    }
  } catch (e) {
    console.error(e);
    setTransferFeedback(e.message || 'Failed to start transfer', true);
  }
}

async function openTransferModal(serverName, currentNodeId = '') {
  currentTransferServer = serverName;
  if (transferModalTitle) transferModalTitle.textContent = `Transfer: ${serverName}`;
  if (transferModalSubtitle) transferModalSubtitle.textContent = 'Pick a destination node.';
  setTransferFeedback('');
  setTransferProgress(null);
  showTransferModal(true);
  await loadTransferNodes(serverName, currentNodeId);
  const existing = await fetchTransferJob(serverName);
  if (existing) {
    setTransferProgress(existing);
    if (!existing.finishedAt && !transferPollTimer) {
      transferPollTimer = setInterval(() => pollTransfer(serverName), 1500);
    }
  }
}

if (transferModalClose) transferModalClose.addEventListener('click', () => showTransferModal(false));
if (transferCancel) transferCancel.addEventListener('click', () => showTransferModal(false));
if (transferModalOverlay) transferModalOverlay.addEventListener('click', (e) => { if (e.target === transferModalOverlay) showTransferModal(false); });

const resourcesModalOverlay = document.getElementById('resourcesModalOverlay');
const resourcesModal = document.getElementById('resourcesModal');
const resourcesModalTitle = document.getElementById('resourcesModalTitle');
const resourcesModalSubtitle = document.getElementById('resourcesModalSubtitle');
const resourcesModalClose = document.getElementById('resourcesModalClose');
const resourcesCancel = document.getElementById('resourcesCancel');
const resourcesSave = document.getElementById('resourcesSave');
const resourcesFeedback = document.getElementById('resourcesFeedback');

const resRamMb = document.getElementById('res_ramMb');
const resCpuCores = document.getElementById('res_cpuCores');
const resStorageMb = document.getElementById('res_storageMb');
const resSwapMb = document.getElementById('res_swapMb');
const resBackupsMax = document.getElementById('res_backupsMax');
const resMaxSchedules = document.getElementById('res_maxSchedules');
const resUsageBar = document.getElementById('res_usageBar');
const resUsageText = document.getElementById('res_usageText');
const resPortsList = document.getElementById('res_portsList');
const resNewPort = document.getElementById('res_newPort');
const resAddPort = document.getElementById('res_addPort');
const resStartupCommand = document.getElementById('res_startupCommand');
const resStartupSection = document.getElementById('res_startupSection');
const resStartupPreview = document.getElementById('res_startupPreview');
const resCopyStartupCmd = document.getElementById('res_copyStartupCmd');
if (resAddPort) resAddPort.setAttribute('data-unsaved-action', 'true');

function formatStorageMb(mb) {
  if (mb == null || isNaN(mb) || mb <= 0) return '';
  if (mb >= 1010) return (mb / 1024).toFixed(2).replace(/\.?0+$/, '') + ' GB';
  return Math.round(mb) + ' MB';
}

if (resStorageMb) {
  resStorageMb.addEventListener('input', () => {
    const hint = resStorageMb.closest('.node-form-field')?.querySelector('.resource-hint');
    if (!hint) return;
    const val = parseInt(resStorageMb.value, 10);
    if (!isNaN(val) && val > 0) {
      const display = formatStorageMb(val);
      hint.textContent = `Disk space limit: ${display}`;
    } else {
      hint.textContent = 'Disk space limit for server files (e.g. 500 = 500 MB, 50000 = ~48.83 GB)';
    }
  });
}

let currentResourcesServer = '';
let currentServerPorts = [];
let currentServerMainPort = null;
let originalServerMainPort = null;
let currentServerTemplate = '';
let originalStartupCommand = '';
registerModalCustomStateReader('resourcesModalOverlay', () => ({
  server: currentResourcesServer || '',
  mainPort: currentServerMainPort ?? null,
  ports: currentServerPorts.slice().map(Number).filter(Number.isFinite).sort((a, b) => a - b)
}));

function showResourcesFeedback(msg, isError = false) {
  if (!resourcesFeedback) return;
  resourcesFeedback.textContent = msg;
  resourcesFeedback.className = 'node-feedback' + (isError ? ' error' : ' success');
  resourcesFeedback.style.display = msg ? 'block' : 'none';
}

function clearResourcesFeedback() {
  if (resourcesFeedback) {
    resourcesFeedback.textContent = '';
    resourcesFeedback.style.display = 'none';
  }
}

function renderPortsList() {
  if (!resPortsList) return;
  resPortsList.innerHTML = '';

  if (currentServerMainPort != null) {
    const mainItem = document.createElement('div');
    mainItem.className = 'port-item port-item-primary';
    mainItem.innerHTML = `
          <i class="fa-solid fa-server"></i>
          <input type="number" class="port-edit-input" value="${currentServerMainPort}" min="1" max="65535" data-role="main-port" title="Main server port" />
          <span class="port-badge-primary">Primary</span>
        `;
    resPortsList.appendChild(mainItem);

    const input = mainItem.querySelector('.port-edit-input');
    input.addEventListener('change', () => {
      const val = parseInt(input.value, 10);
      if (isNaN(val) || val < 1 || val > 65535) {
        showResourcesFeedback('Main port must be between 1 and 65535', true);
        input.value = currentServerMainPort;
        return;
      }
      if (currentServerPorts.includes(val)) {
        showResourcesFeedback(`Port ${val} is already used as an additional port`, true);
        input.value = currentServerMainPort;
        return;
      }
      clearResourcesFeedback();
      currentServerMainPort = val;
    });
  }

  currentServerPorts.forEach((port, index) => {
    const item = document.createElement('div');
    item.className = 'port-item';
    item.innerHTML = `
          <i class="fa-solid fa-plug"></i>
          <span>${port}</span>
          <button type="button" class="port-remove" data-index="${index}" title="Remove port">
            <i class="fa-solid fa-times"></i>
          </button>
        `;
    resPortsList.appendChild(item);
  });
}

function addPort() {
  if (!resNewPort) return;
  const portVal = resNewPort.value.trim();
  if (!portVal) {
    showResourcesFeedback('Please enter a port number', true);
    return;
  }
  const port = parseInt(portVal, 10);
  if (isNaN(port) || port < 1 || port > 65535) {
    showResourcesFeedback('Port must be between 1 and 65535', true);
    return;
  }
  if (currentServerMainPort != null && port === currentServerMainPort) {
    showResourcesFeedback(`Port ${port} is already the primary server port`, true);
    return;
  }
  if (currentServerPorts.includes(port)) {
    showResourcesFeedback(`Port ${port} is already in the list`, true);
    return;
  }
  currentServerPorts.push(port);
  resNewPort.value = '';
  clearResourcesFeedback();
  renderPortsList();
  syncPortsToStartupCommand();
}

function removePort(index) {
  if (index >= 0 && index < currentServerPorts.length) {
    const totalPorts = (currentServerMainPort != null ? 1 : 0) + currentServerPorts.length;
    if (totalPorts <= 1) {
      showResourcesFeedback('Cannot remove the last port — a server must have at least one port assigned.', true);
      return;
    }
    clearResourcesFeedback();
    currentServerPorts.splice(index, 1);
    renderPortsList();
    syncPortsToStartupCommand();
  }
}

function syncPortsToStartupCommand() {
  if (!resStartupCommand) return;
  const cmd = resStartupCommand.value.trim();
  if (!cmd) return;
  const cmdLower = cmd.toLowerCase();
  if (!cmdLower.startsWith('docker run')) return;

  const argsStr = cmd.slice('docker run'.length).trim();
  const args = parseDockerArgs(argsStr);
  const filtered = [];
  let skipNext = false;
  const imageAndRest = [];
  let reachedImage = false;
  const portFlagsWithValue = new Set(['-p', '--publish']);
  const knownFlagsWithValue = new Set([
    '-e', '--env', '-v', '--volume', '-w', '--workdir',
    '--name', '-m', '--memory', '--cpus', '--memory-swap',
    '-u', '--user', '-h', '--hostname', '--network', '--net',
    '--restart', '-l', '--label', '--entrypoint',
    '--cpu-period', '--cpu-quota', '--memory-reservation',
    '--pids-limit', '--shm-size',
  ]);

  for (let i = 0; i < args.length; i++) {
    if (reachedImage) {
      imageAndRest.push(args[i]);
      continue;
    }
    if (skipNext) { skipNext = false; continue; }
    const a = args[i];
    const lower = a.toLowerCase();
    if (portFlagsWithValue.has(a) && i + 1 < args.length) {
      skipNext = true;
      continue;
    }
    if (a.startsWith('-p=') || a.startsWith('--publish=')) {
      continue;
    }
    if (lower.startsWith('-')) {
      if (lower.includes('=')) { filtered.push(a); continue; }
      if (knownFlagsWithValue.has(lower)) {
        filtered.push(a);
        if (i + 1 < args.length) { filtered.push(args[i + 1]); i++; }
        continue;
      }
      filtered.push(a);
      continue;
    }
    reachedImage = true;
    imageAndRest.push(a);
  }

  const portArgs = [];
  if (currentServerMainPort != null && currentServerMainPort > 0) {
    if (originalStartupCommand.includes('{PORT}')) {
      portArgs.push('-p', '{PORT}:{PORT}');
    } else {
      portArgs.push('-p', `${currentServerMainPort}:${currentServerMainPort}`);
    }
  }
  for (const ap of currentServerPorts) {
    if (ap > 0 && ap !== currentServerMainPort) {
      portArgs.push('-p', `${ap}:${ap}`);
    }
  }

  const newCmd = 'docker run ' + [...filtered, ...portArgs, ...imageAndRest].join(' ');
  resStartupCommand.value = newCmd;
  if (resStartupPreview) {
    const escaped = newCmd.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    resStartupPreview.innerHTML = '<code>' + escaped + '</code>';
  }
  originalStartupCommand = newCmd;
}

function parseDockerArgs(argsStr) {
  const args = [];
  let current = '';
  let inQuote = '';
  for (let i = 0; i < argsStr.length; i++) {
    const ch = argsStr[i];
    if (inQuote) {
      if (ch === inQuote) { inQuote = ''; continue; }
      current += ch;
    } else if (ch === '"' || ch === "'") {
      inQuote = ch;
    } else if (ch === ' ' || ch === '\t') {
      if (current) { args.push(current); current = ''; }
    } else {
      current += ch;
    }
  }
  if (current) args.push(current);
  return args;
}

if (resAddPort) resAddPort.addEventListener('click', addPort);
if (resNewPort) {
  resNewPort.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      addPort();
    }
  });
}
if (resPortsList) {
  resPortsList.addEventListener('click', (e) => {
    const removeBtn = e.target.closest('.port-remove');
    if (removeBtn) {
      const index = parseInt(removeBtn.dataset.index, 10);
      removePort(index);
    }
  });
}

async function openResourcesModal(serverName) {
  currentResourcesServer = serverName;
  currentServerPorts = [];
  currentServerMainPort = null;
  originalServerMainPort = null;
  currentServerTemplate = '';
  clearResourcesFeedback();

  if (resourcesModalTitle) resourcesModalTitle.textContent = `Resources: ${serverName}`;
  if (resourcesModalSubtitle) resourcesModalSubtitle.textContent = 'Set limits for this server.';

  if (resRamMb) resRamMb.value = '';
  if (resCpuCores) resCpuCores.value = '';
  if (resStorageMb) resStorageMb.value = '';
  if (resSwapMb) resSwapMb.value = '-1';
  if (resBackupsMax) resBackupsMax.value = '';
  if (resMaxSchedules) resMaxSchedules.value = '';
  if (resUsageBar) resUsageBar.style.width = '0%';
  if (resUsageText) resUsageText.textContent = 'Loading...';
  if (resNewPort) resNewPort.value = '';
  if (resStartupCommand) resStartupCommand.value = '';
  if (resStartupPreview) resStartupPreview.innerHTML = '<code>Loading...</code>';
  renderPortsList();

  if (resourcesModalOverlay) {
    resourcesModalOverlay.setAttribute('aria-hidden', 'false');
    resourcesModalOverlay.classList.add('show');
  }

  try {
    const res = await fetch(`/api/settings/servers/${encodeURIComponent(serverName)}/resources`);
    if (res.ok) {
      const data = await res.json();
      const resources = data.resources || {};
      const stats = data.stats || {};
      currentServerTemplate = data.template || '';

      if (resources.ramMb != null && resRamMb) resRamMb.value = resources.ramMb;
      if (resources.cpuCores != null && resCpuCores) resCpuCores.value = resources.cpuCores;
      if (resources.storageMb != null && resStorageMb) {
        resStorageMb.value = resources.storageMb;
      } else if (resources.storageGb != null && resStorageMb) {
        resStorageMb.value = Math.round(resources.storageGb * 1024);
      }
      if (resources.swapMb != null && resSwapMb) resSwapMb.value = String(resources.swapMb);
      if (resources.backupsMax != null && resBackupsMax) resBackupsMax.value = resources.backupsMax;
      if (resources.maxSchedules != null && resMaxSchedules) resMaxSchedules.value = resources.maxSchedules;

      if (data.hostPort) {
        currentServerMainPort = Number(data.hostPort);
        originalServerMainPort = currentServerMainPort;
      }

      if (Array.isArray(resources.ports)) {
        currentServerPorts = resources.ports.slice();
      }
      renderPortsList();

      if (data.startupCommand && data.startupCommand.trim()) {
        if (resStartupCommand) resStartupCommand.value = data.startupCommand;
        originalStartupCommand = data.startupCommand;
        if (resStartupPreview) {
          const escaped = data.startupCommand.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
          resStartupPreview.innerHTML = '<code>' + escaped + '</code>';
        }
      } else {
        originalStartupCommand = '';
        if (resStartupPreview) resStartupPreview.innerHTML = '<code>No startup command configured</code>';
      }

      if (resUsageText) {
        if (stats && stats.memory) {
          const usedMb = stats.memory.usedMb || 0;
          const limitMb = stats.memory.limitMb || resources.ramMb || 0;
          const percent = limitMb > 0 ? Math.min(100, (usedMb / limitMb) * 100) : 0;

          if (resUsageBar) {
            resUsageBar.style.width = `${percent}%`;
            resUsageBar.className = 'resource-usage-fill' + (percent > 80 ? ' warning' : '');
          }
          resUsageText.textContent = limitMb > 0
            ? `${Math.round(usedMb)} MB / ${Math.round(limitMb)} MB (${percent.toFixed(1)}%)`
            : 'No limit set';
        } else {
          if (resUsageBar) resUsageBar.style.width = '0%';
          resUsageText.textContent = resources.ramMb
            ? `0 MB / ${resources.ramMb} MB (server stopped)`
            : 'No usage data';
        }
      }
    } else {
      if (resUsageBar) resUsageBar.style.width = '0%';
      if (resUsageText) resUsageText.textContent = 'Failed to load resources';
    }
  } catch (err) {
    console.error('[resources] Failed to load:', err);
    if (resUsageBar) resUsageBar.style.width = '0%';
    if (resUsageText) resUsageText.textContent = 'Failed to load';
  }
}

function closeResourcesModal() {
  if (resourcesModalOverlay) {
    resourcesModalOverlay.setAttribute('aria-hidden', 'true');
    resourcesModalOverlay.classList.remove('show');
  }
  currentResourcesServer = '';
  currentServerMainPort = null;
  originalServerMainPort = null;
  clearResourcesFeedback();
}

function extractHostPortsFromDockerCommand(cmdStr) {
  const cmd = (cmdStr || '').trim();
  const cmdLower = cmd.toLowerCase();
  if (!cmdLower.startsWith('docker run')) return [];

  const argsStr = cmd.slice('docker run'.length).trim();
  const args = [];
  let current = '';
  let inQuote = '';
  for (let i = 0; i < argsStr.length; i++) {
    const ch = argsStr[i];
    if (inQuote) {
      if (ch === inQuote) { inQuote = ''; continue; }
      current += ch;
    } else if (ch === '"' || ch === "'") {
      inQuote = ch;
    } else if (ch === ' ' || ch === '\t') {
      if (current) { args.push(current); current = ''; }
    } else {
      current += ch;
    }
  }
  if (current) args.push(current);

  const hostPorts = [];
  const flagsWithValue = new Set([
    '-e', '--env', '-v', '--volume', '-w', '--workdir',
    '--name', '-m', '--memory', '--cpus', '--memory-swap',
    '-u', '--user', '-h', '--hostname', '--network', '--net',
    '--restart', '-l', '--label', '--entrypoint',
  ]);

  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    const lower = a.toLowerCase();

    if ((a === '-p' || a === '--publish') && i + 1 < args.length) {
      const mapping = args[i + 1];
      i++;
      if (mapping.includes('{PORT}')) continue;
      const hp = parseHostPortFromMapping(mapping);
      if (hp > 0) hostPorts.push(hp);
      continue;
    }

    if (a.startsWith('-p=') || a.startsWith('--publish=')) {
      const mapping = a.includes('=') ? a.split('=').slice(1).join('=') : '';
      if (mapping.includes('{PORT}')) continue;
      const hp = parseHostPortFromMapping(mapping);
      if (hp > 0) hostPorts.push(hp);
      continue;
    }

    if (lower.startsWith('-')) {
      if (lower.includes('=')) continue;
      const flagName = lower.startsWith('--') ? lower : lower;
      if (flagsWithValue.has(flagName) || a === '-p' || a === '--publish') {
        i++;
      }
      continue;
    }
    break;
  }
  return hostPorts;
}

function parseHostPortFromMapping(mapping) {
  let s = mapping;
  const slashIdx = s.indexOf('/');
  if (slashIdx > 0) s = s.slice(0, slashIdx);
  const parts = s.split(':');
  if (parts.length === 2) return parseInt(parts[0], 10) || 0;
  if (parts.length === 3) return parseInt(parts[1], 10) || 0;
  return 0;
}

function validateDockerCommandPorts(cmdStr, allocatedPort, additionalPorts, reservedPorts) {
  const reserved = Array.isArray(reservedPorts) ? new Set(reservedPorts) : new Set();
  const allowed = new Set();
  if (allocatedPort > 0) allowed.add(allocatedPort);
  if (Array.isArray(additionalPorts)) {
    for (const p of additionalPorts) {
      if (p > 0) allowed.add(p);
    }
  }
  const hostPorts = extractHostPortsFromDockerCommand(cmdStr);
  for (const hp of hostPorts) {
    if (hp > 0 && !allowed.has(hp)) {
      return 'Port config through docker edit command is not allowed. Add or remove a port for this server by the category "Port Management"';
    }
    if (hp > 0 && reserved.has(hp)) {
      return `Port ${hp} in the Docker command conflicts with a port forwarding rule. Manage forwarded ports through the Port Forwarding panel instead.`;
    }
  }
  return null;
}

async function saveResources() {
  if (!currentResourcesServer) return;

  if (resStartupCommand && resStartupCommand.value.trim()) {
    const cmdText = resStartupCommand.value.trim();
    const allocatedPort = originalServerMainPort || currentServerMainPort;
    if (allocatedPort && allocatedPort > 0) {
      const portViolation = validateDockerCommandPorts(cmdText, allocatedPort, currentServerPorts);
      if (portViolation) {
        showResourcesFeedback(portViolation, true);
        return;
      }
    }
  }

  const resources = {};

  const ramVal = resRamMb?.value?.trim();
  if (ramVal) {
    const ram = parseInt(ramVal, 10);
    if (!isNaN(ram) && ram >= 0) resources.ramMb = ram;
  }

  const cpuVal = resCpuCores?.value?.trim();
  if (cpuVal) {
    const cpu = parseFloat(cpuVal);
    if (!isNaN(cpu) && cpu >= 0) resources.cpuCores = cpu;
  }

  const storageVal = resStorageMb?.value?.trim();
  if (storageVal) {
    const storage = parseInt(storageVal, 10);
    if (!isNaN(storage) && storage >= 0) resources.storageMb = storage;
  }

  const swapVal = resSwapMb?.value;
  if (swapVal != null) {
    const swap = parseInt(swapVal, 10);
    if (!isNaN(swap)) resources.swapMb = swap;
  }

  const backupsVal = resBackupsMax?.value?.trim();
  if (backupsVal) {
    const backups = parseInt(backupsVal, 10);
    if (!isNaN(backups) && backups >= 0) resources.backupsMax = backups;
  }

  const maxSchedulesVal = resMaxSchedules?.value?.trim();
  if (maxSchedulesVal) {
    const maxSched = parseInt(maxSchedulesVal, 10);
    if (!isNaN(maxSched) && maxSched >= 0) resources.maxSchedules = maxSched;
  }

  resources.ports = currentServerPorts.slice();

  const requestBody = { resources };

  if (currentServerMainPort != null && currentServerMainPort !== originalServerMainPort) {
    requestBody.mainPort = currentServerMainPort;
  }

  if (resStartupCommand && resStartupCommand.value.trim()) {
    requestBody.startupCommand = resStartupCommand.value.trim();
  }

  if (resourcesSave) {
    resourcesSave.disabled = true;
    resourcesSave.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Applying...';
  }

  try {
    const res = await fetch(`/api/settings/servers/${encodeURIComponent(currentResourcesServer)}/resources`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(requestBody)
    });

    const data = await res.json().catch(() => ({ error: 'Invalid response' }));

    if (!res.ok) {
      showResourcesFeedback(data.error || 'Failed to update resources', true);
      return;
    }

    showResourcesFeedback('Resources updated! Server is restarting...', false);

    setTimeout(() => {
      closeResourcesModal();
    }, 2000);

  } catch (err) {
    console.error('[resources] Save error:', err);
    showResourcesFeedback('Network error while saving resources', true);
  } finally {
    if (resourcesSave) {
      resourcesSave.disabled = false;
      resourcesSave.innerHTML = '<i class="fa-solid fa-bolt"></i> Apply & Restart';
    }
  }
}

if (resourcesModalClose) resourcesModalClose.addEventListener('click', closeResourcesModal);
if (resourcesCancel) resourcesCancel.addEventListener('click', closeResourcesModal);
if (resourcesSave) resourcesSave.addEventListener('click', saveResources);
if (resourcesModalOverlay) {
  resourcesModalOverlay.addEventListener('click', (e) => {
    if (e.target === resourcesModalOverlay) closeResourcesModal();
  });
}

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && resourcesModalOverlay?.classList.contains('show')) {
    closeResourcesModal();
  }
});

if (resCopyStartupCmd) {
  resCopyStartupCmd.addEventListener('click', () => {
    const cmd = resStartupCommand?.value?.trim() || resStartupPreview?.textContent?.trim() || '';
    if (cmd && cmd !== 'No startup command configured' && cmd !== 'Loading...') {
      navigator.clipboard.writeText(cmd).then(() => {
        const icon = resCopyStartupCmd.querySelector('i');
        if (icon) {
          icon.className = 'fa-solid fa-check';
          setTimeout(() => { icon.className = 'fa-solid fa-copy'; }, 1500);
        }
      }).catch(() => { });
    }
  });
}

if (resStartupCommand) {
  resStartupCommand.addEventListener('input', () => {
    if (!resStartupPreview) return;
    const val = resStartupCommand.value.trim();
    if (val) {
      const escaped = val.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
      resStartupPreview.innerHTML = '<code>' + escaped + '</code>';
    } else {
      resStartupPreview.innerHTML = '<code>No startup command configured</code>';
    }
  });
}

const portForwardModalOverlay = document.getElementById('portForwardModalOverlay');
const portForwardModal = document.getElementById('portForwardModal');
const portForwardModalTitle = document.getElementById('portForwardModalTitle');
const portForwardModalSubtitle = document.getElementById('portForwardModalSubtitle');
const portForwardModalClose = document.getElementById('portForwardModalClose');
const portForwardCancel = document.getElementById('portForwardCancel');
const portForwardSave = document.getElementById('portForwardSave');
const portForwardFeedback = document.getElementById('portForwardFeedback');
const pfRulesList = document.getElementById('pf_rulesList');
const pfNewPublicPort = document.getElementById('pf_newPublicPort');
const pfNewInternalPort = document.getElementById('pf_newInternalPort');
const pfAddRule = document.getElementById('pf_addRule');
if (pfAddRule) pfAddRule.setAttribute('data-unsaved-action', 'true');

let currentPFServer = '';
let currentPFRules = [];
let currentPFNodeAllocation = null;
let currentPFMainPort = null;
let currentPFAllocatedPorts = [];
registerModalCustomStateReader('portForwardModalOverlay', () => ({
  server: currentPFServer || '',
  rules: currentPFRules
    .map(rule => ({
      publicPort: Number(rule?.publicPort || 0),
      internalPort: Number(rule?.internalPort || 0)
    }))
    .sort((a, b) => (a.internalPort - b.internalPort) || (a.publicPort - b.publicPort))
}));

function showPFFeedback(msg, isError = false) {
  if (!portForwardFeedback) return;
  portForwardFeedback.textContent = msg;
  portForwardFeedback.className = 'node-feedback' + (isError ? ' error' : ' success');
  portForwardFeedback.style.display = msg ? 'block' : 'none';
}

function clearPFFeedback() {
  if (portForwardFeedback) {
    portForwardFeedback.textContent = '';
    portForwardFeedback.style.display = 'none';
  }
}

function isPFPortInAllocation(port) {
  if (!currentPFNodeAllocation) return true;
  const p = Number(port);
  if (!Number.isFinite(p) || p < 1 || p > 65535) return false;
  const alloc = currentPFNodeAllocation;
  if (alloc.mode === 'range') {
    const start = Number(alloc.start || 0);
    const count = Number(alloc.count || 0);
    if (start <= 0 || count <= 0) return true;
    return p >= start && p < start + count;
  }
  if (alloc.mode === 'list' && Array.isArray(alloc.ports)) {
    if (alloc.ports.length === 0) return true;
    return alloc.ports.includes(p);
  }
  return true;
}

function populateInternalPortDropdown() {
  if (!pfNewInternalPort) return;
  pfNewInternalPort.innerHTML = '<option value="" disabled selected>Select a port…</option>';
  const usedInternalPorts = new Set(currentPFRules.map(r => r.internalPort));
  for (const port of currentPFAllocatedPorts) {
    if (!usedInternalPorts.has(port)) {
      const opt = document.createElement('option');
      opt.value = port;
      opt.textContent = port === currentPFMainPort ? `${port} (primary)` : String(port);
      pfNewInternalPort.appendChild(opt);
    }
  }
}

function renderPFRules() {
  if (!pfRulesList) return;
  pfRulesList.innerHTML = '';
  if (currentPFRules.length === 0) {
    pfRulesList.innerHTML = '<div class="pf-empty-state"><i class="fa-solid fa-shuffle"></i> No port forward rules configured</div>';
    populateInternalPortDropdown();
    return;
  }
  currentPFRules.forEach((rule, index) => {
    const item = document.createElement('div');
    item.className = 'pf-rule-item';
    const isPrimary = rule.internalPort === currentPFMainPort;
    item.innerHTML = `
      <div class="pf-rule-ports">
        <span class="pf-rule-port pf-internal"><i class="fa-solid fa-server"></i> ${rule.internalPort}${isPrimary ? ' (primary)' : ''}</span>
        <i class="fa-solid fa-arrow-right pf-rule-arrow"></i>
        <span class="pf-rule-port pf-public"><i class="fa-solid fa-globe"></i> ${rule.publicPort}</span>
      </div>
      <button type="button" class="port-remove pf-remove" data-index="${index}" title="Remove rule">
        <i class="fa-solid fa-times"></i>
      </button>
    `;
    pfRulesList.appendChild(item);
  });
  populateInternalPortDropdown();
}

function addPFRule() {
  if (!pfNewPublicPort || !pfNewInternalPort) return;
  const publicVal = pfNewPublicPort.value.trim();
  const internalVal = pfNewInternalPort.value;
  if (!internalVal) {
    showPFFeedback('Please select an internal server port', true);
    return;
  }
  if (!publicVal) {
    showPFFeedback('Please enter a public port', true);
    return;
  }
  const publicPort = parseInt(publicVal, 10);
  const internalPort = parseInt(internalVal, 10);
  if (isNaN(publicPort) || publicPort < 1 || publicPort > 65535) {
    showPFFeedback('Public port must be between 1 and 65535', true);
    return;
  }
  if (isNaN(internalPort)) {
    showPFFeedback('Invalid internal port selected', true);
    return;
  }
  if (currentPFAllocatedPorts.includes(publicPort)) {
    showPFFeedback(`Port ${publicPort} is already allocated to this server and cannot be used as a public forward`, true);
    return;
  }
  if (currentPFRules.some(r => r.publicPort === publicPort)) {
    showPFFeedback(`Public port ${publicPort} is already in use by another rule`, true);
    return;
  }
  if (currentPFRules.some(r => r.internalPort === internalPort)) {
    showPFFeedback(`Internal port ${internalPort} already has a forward rule`, true);
    return;
  }
  currentPFRules.push({ publicPort, internalPort });
  pfNewPublicPort.value = '';
  pfNewInternalPort.value = '';
  clearPFFeedback();
  renderPFRules();
}

function removePFRule(index) {
  if (index >= 0 && index < currentPFRules.length) {
    currentPFRules.splice(index, 1);
    clearPFFeedback();
    renderPFRules();
  }
}

async function openPortForwardModal(serverName) {
  currentPFServer = serverName;
  currentPFRules = [];
  currentPFNodeAllocation = null;
  currentPFMainPort = null;
  currentPFAllocatedPorts = [];
  clearPFFeedback();

  if (portForwardModalTitle) portForwardModalTitle.textContent = `Ports: ${serverName}`;
  if (portForwardModalSubtitle) portForwardModalSubtitle.textContent = 'Map public ports to internal ones.';

  if (pfNewPublicPort) pfNewPublicPort.value = '';
  if (pfNewInternalPort) pfNewInternalPort.value = '';
  renderPFRules();

  if (portForwardModalOverlay) {
    portForwardModalOverlay.setAttribute('aria-hidden', 'false');
    portForwardModalOverlay.classList.add('show');
  }

  try {
    const res = await fetch(`/api/settings/servers/${encodeURIComponent(serverName)}/port-forwards`);
    if (res.ok) {
      const data = await res.json();
      if (Array.isArray(data.portForwards)) {
        currentPFRules = data.portForwards.map(r => ({
          publicPort: Number(r.publicPort),
          internalPort: Number(r.internalPort)
        }));
      }
      if (data.nodeAllocation) {
        currentPFNodeAllocation = data.nodeAllocation;
      }
      if (data.mainPort) {
        currentPFMainPort = Number(data.mainPort);
      }
      if (Array.isArray(data.allocatedPorts)) {
        currentPFAllocatedPorts = data.allocatedPorts.map(Number);
      }
      renderPFRules();
    } else {
      const errData = await res.json().catch(() => ({}));
      showPFFeedback(errData.error || 'Failed to load port forwarding config', true);
    }
  } catch (err) {
    console.error('[port-forward] Failed to load:', err);
    showPFFeedback('Failed to load port forwarding config', true);
  }
}

function closePortForwardModal() {
  if (portForwardModalOverlay) {
    portForwardModalOverlay.setAttribute('aria-hidden', 'true');
    portForwardModalOverlay.classList.remove('show');
  }
  currentPFServer = '';
  currentPFRules = [];
  currentPFNodeAllocation = null;
  currentPFMainPort = null;
  currentPFAllocatedPorts = [];
  clearPFFeedback();
}

async function savePortForwards() {
  if (!currentPFServer) return;

  for (const rule of currentPFRules) {
    if (currentPFAllocatedPorts.includes(rule.publicPort)) {
      showPFFeedback(`Port ${rule.publicPort} is allocated to this server and cannot be used as a public forward`, true);
      return;
    }
  }

  if (portForwardSave) {
    portForwardSave.disabled = true;
    portForwardSave.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Applying...';
  }

  try {
    const res = await fetch(`/api/settings/servers/${encodeURIComponent(currentPFServer)}/port-forwards`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ portForwards: currentPFRules })
    });
    const data = await res.json().catch(() => ({ error: 'Invalid response' }));
    if (!res.ok) {
      showPFFeedback(data.error || 'Failed to update port forwards', true);
      return;
    }
    showPFFeedback('Port forwards updated! Server is restarting...', false);
    setTimeout(() => closePortForwardModal(), 2000);
  } catch (err) {
    console.error('[port-forward] Save error:', err);
    showPFFeedback('Network error while saving port forwards', true);
  } finally {
    if (portForwardSave) {
      portForwardSave.disabled = false;
      portForwardSave.innerHTML = '<i class="fa-solid fa-bolt"></i> Apply & Restart';
    }
  }
}

if (portForwardModalClose) portForwardModalClose.addEventListener('click', closePortForwardModal);
if (portForwardCancel) portForwardCancel.addEventListener('click', closePortForwardModal);
if (portForwardSave) portForwardSave.addEventListener('click', savePortForwards);
if (portForwardModalOverlay) {
  portForwardModalOverlay.addEventListener('click', (e) => {
    if (e.target === portForwardModalOverlay) closePortForwardModal();
  });
}
if (pfAddRule) pfAddRule.addEventListener('click', addPFRule);
if (pfNewPublicPort) {
  pfNewPublicPort.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') { e.preventDefault(); addPFRule(); }
  });
}
if (pfRulesList) {
  pfRulesList.addEventListener('click', (e) => {
    const removeBtn = e.target.closest('.pf-remove');
    if (removeBtn) {
      const index = parseInt(removeBtn.dataset.index, 10);
      removePFRule(index);
    }
  });
}
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && portForwardModalOverlay?.classList.contains('show')) {
    closePortForwardModal();
  }
});

async function loadAccounts() {
  accountsListEl.innerHTML = `<div style="padding:12px;color:var(--muted)">Loading accounts…</div>`;
  try {
    const res = await fetch('/api/settings/accounts');
    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: 'failed' }));
      accountsListEl.innerHTML = `<div style="padding:12px;color:var(--muted)">Failed to load accounts</div>`;
      console.warn(err);
      return;
    }
    const data = await res.json();
    accountsListEl._accounts = data.accounts || [];
    accountsListEl._bots = data.bots || [];
    renderAccounts(accountsListEl._accounts, accountsListEl._bots);
  } catch (e) {
    console.error(e);
    accountsListEl.innerHTML = `<div style="padding:12px;color:var(--muted)">Network error</div>`;
  }
}
function renderAccounts(accounts, bots, searchQuery = '') {
  accountsListEl.innerHTML = '';

  let filtered = accounts;
  if (searchQuery && searchQuery.trim()) {
    const q = searchQuery.trim().toLowerCase();
    filtered = accounts.filter(acc =>
      acc.email && acc.email.toLowerCase().includes(q)
    );
  }

  if (!Array.isArray(filtered) || filtered.length === 0) {
    if (searchQuery && searchQuery.trim()) {
      accountsListEl.innerHTML = `<div style="padding:12px;color:var(--muted)"><i class="fa-solid fa-search"></i> No accounts matching "${escapeHtml(searchQuery)}"</div>`;
    } else {
      accountsListEl.innerHTML = `<div style="padding:12px;color:var(--muted)"><i class="fa-solid fa-face-tired"></i> No accounts found</div>`;
    }
    return;
  }
  filtered.forEach(acc => {
    const div = document.createElement('div');
    div.className = 'account-row';
    const accessLabel = (Array.isArray(acc.servers) && acc.servers.length > 0) ? `${acc.servers.length} access` : 'no access';
    div.innerHTML = `
          <div class="account-left">
            <div class="iconWrap" style="width:40px;height:40px;border-radius:8px;display:flex;align-items:center;justify-content:center;background:rgba(0,0,0,0.04)"><i class="fa-solid fa-user"></i></div>
            <div>
              <div class="account-email">${escapeHtml(acc.email)}</div>
              <div style="color:var(--muted);font-size:13px">${escapeHtml(accessLabel)}</div>
            </div>
          </div>
          <div style="display:flex;gap:12px;align-items:center">
            <div class="agent-access-wrap" style="display:flex;align-items:center;gap:8px;">
              <label class="switch agent-access-switch" title="Toggle ADPanel Agent access">
                <input type="checkbox" class="agent-access-toggle" data-email="${encodeURIComponent(acc.email)}" ${acc.agent_access ? 'checked' : ''}>
                <span class="slider"></span>
              </label>
              <span style="font-size:12px;color:var(--muted);">Agent</span>
            </div>
            <button class="btn ghost manage-access" data-email="${encodeURIComponent(acc.email)}" title="Manage access for ${escapeHtml(acc.email)}"><i class="fa-solid fa-server"></i></button>
            <button class="btn ghost change-password-btn" data-email="${encodeURIComponent(acc.email)}" title="Change password for ${escapeHtml(acc.email)}" style="color:#f5a623;"><i class="fa-solid fa-key"></i></button>
            <button class="btn ghost qr-reset-btn" data-email="${encodeURIComponent(acc.email)}" title="Reset 2FA/Recovery for ${escapeHtml(acc.email)}" style="color:#fff;"><i class="fa-solid fa-qrcode"></i></button>
            <button class="btn ghost delete-user-btn" data-email="${encodeURIComponent(acc.email)}" title="Delete user ${escapeHtml(acc.email)}" style="color:#f44747;"><i class="fa-solid fa-trash"></i></button>
          </div>
        `;
    accountsListEl.appendChild(div);
  });
}

if (accountsSearchInput) {
  accountsSearchInput.addEventListener('input', (e) => {
    const query = e.target.value;
    const accounts = accountsListEl._accounts || [];
    const bots = accountsListEl._bots || [];
    renderAccounts(accounts, bots, query);
  });
}

accountsListEl.addEventListener('click', async (e) => {
  const manageBtn = e.target.closest('.manage-access');
  if (manageBtn) {
    const enc = manageBtn.dataset.email;
    if (!enc) return;
    const email = decodeURIComponent(enc);
    openManageAccessModal(email);
    return;
  }

  const changePassBtn = e.target.closest('.change-password-btn');
  if (changePassBtn) {
    const enc = changePassBtn.dataset.email;
    if (!enc) return;
    const email = decodeURIComponent(enc);
    openAdminChangePasswordModal(email);
    return;
  }

  const deleteBtn = e.target.closest('.delete-user-btn');
  if (deleteBtn) {
    const enc = deleteBtn.dataset.email;
    if (!enc) return;
    const email = decodeURIComponent(enc);

    const confirmed = confirm(`Are you sure you want to permanently delete the user "${email}"?\n\nThis action cannot be undone. All access permissions will be removed.`);
    if (!confirmed) return;

    deleteBtn.disabled = true;
    deleteBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>';

    try {
      const res = await fetch(`/api/settings/accounts/${encodeURIComponent(email)}`, {
        method: 'DELETE'
      });
      const data = await res.json().catch(() => ({}));

      if (!res.ok) {
        alert(data.error || 'Failed to delete user');
        deleteBtn.disabled = false;
        deleteBtn.innerHTML = '<i class="fa-solid fa-trash"></i>';
        return;
      }

      const toast = document.createElement('div');
      toast.className = 'perm-success-toast';
      toast.innerHTML = '<i class="fa-solid fa-circle-check"></i> User deleted successfully';
      document.body.appendChild(toast);
      setTimeout(() => { toast.classList.add('show'); }, 10);
      setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => toast.remove(), 300);
      }, 2000);

      loadAccounts();
    } catch (err) {
      console.error('[delete-user]', err);
      alert('Network error');
      deleteBtn.disabled = false;
      deleteBtn.innerHTML = '<i class="fa-solid fa-trash"></i>';
    }
    return;
  }
});

accountsListEl.addEventListener('change', async (e) => {
  const toggle = e.target.closest('.agent-access-toggle');
  if (!toggle) return;
  const enc = toggle.dataset.email;
  if (!enc) return;
  const email = decodeURIComponent(enc);
  const enabled = toggle.checked;
  try {
    const res = await fetch(`/api/settings/accounts/${encodeURIComponent(email)}/agent-access`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ enabled })
    });
    if (!res.ok) {
      const data = await res.json().catch(() => ({}));
      alert(data.error || 'Failed to update agent access');
      toggle.checked = !enabled;
      return;
    }
    const toast = document.createElement('div');
    toast.className = 'perm-success-toast';
    toast.innerHTML = `<i class="fa-solid fa-circle-check"></i> Agent access ${enabled ? 'enabled' : 'disabled'}`;
    document.body.appendChild(toast);
    setTimeout(() => { toast.classList.add('show'); }, 10);
    setTimeout(() => {
      toast.classList.remove('show');
      setTimeout(() => toast.remove(), 300);
    }, 2000);
  } catch (err) {
    console.error(err);
    alert('Network error');
    toggle.checked = !enabled;
  }
});
async function openManageAccessModal(email) {
  accountModalTitle.textContent = `Manage access for ${email}`;
  accountModalBody.innerHTML = `<div class="enterprise-loading"><div class="enterprise-loading-spinner"></div><span>Loading servers...</span></div>`;
  accountModalOverlay.classList.add('show');
  accountModal.classList.add('show');
  try {
    const res = await fetch('/api/settings/accounts');
    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: 'failed' }));
      accountModalBody.innerHTML = `<div class="enterprise-empty"><i class="fa-solid fa-circle-exclamation"></i><p>Failed to load data</p></div>`;
      console.warn(err);
      return;
    }
    const data = await res.json();
    const accounts = data.accounts || [];
    const bots = data.bots || [];
    const rec = accounts.find(a => String(a.email).toLowerCase() === String(email).toLowerCase());
    const userServers = rec ? (Array.isArray(rec.servers) ? rec.servers : []) : [];
    if (!Array.isArray(bots) || bots.length === 0) {
      accountModalBody.innerHTML = `<div class="enterprise-empty"><i class="fa-solid fa-server"></i><p>No servers found</p></div>`;
      return;
    }
    const list = document.createElement('div');
    list.className = 'access-list';
    bots.forEach(b => {
      const row = document.createElement('div');
      row.className = 'access-server-row';
      const left = document.createElement('div');
      left.className = 'access-server-left';
      left.innerHTML = `<div class="access-server-icon"><i class="fa-solid fa-folder"></i></div><div class="access-server-name">${escapeHtml(b)}</div>`;
      let hasAccess = userServers.includes(b) || userServers.includes('all');
      const actions = document.createElement('div');
      actions.className = 'access-server-actions';
      if (hasAccess) {
        const editBtn = document.createElement('button');
        editBtn.className = 'access-btn access-btn-edit';
        editBtn.title = `Edit permissions for ${escapeHtml(email)} on ${escapeHtml(b)}`;
        editBtn.innerHTML = `<i class="fa-solid fa-pen"></i> Edit`;
        editBtn.addEventListener('click', () => {
          const existing = getSavedPerms(email, b);
          openPermsModal(email, b, () => openManageAccessModal(email), existing);
        });
        const revokeBtn = document.createElement('button');
        revokeBtn.className = 'access-btn access-btn-revoke';
        revokeBtn.title = `Revoke access for ${escapeHtml(email)} on ${escapeHtml(b)}`;
        revokeBtn.textContent = 'Revoke';
        revokeBtn.addEventListener('click', async () => {
          revokeBtn.disabled = true;
          try {
            const r = await fetch(`/api/settings/accounts/${encodeURIComponent(email)}/remove`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ server: b })
            });
            if (!r.ok) {
              const err = await r.json().catch(() => ({ error: 'failed' }));
              alert(err.error || 'Failed to revoke access');
              revokeBtn.disabled = false;
            } else {
              setSavedPerms(email, b, {});
              openManageAccessModal(email);
            }
          } catch (err) {
            console.error(err);
            alert('Network error');
            revokeBtn.disabled = false;
          }
        });
        actions.appendChild(editBtn);
        actions.appendChild(revokeBtn);
      } else {
        const addBtn = document.createElement('button');
        addBtn.className = 'access-btn access-btn-add';
        addBtn.innerHTML = '<i class="fa-solid fa-plus"></i> Grant';
        addBtn.addEventListener('click', () => {
          const remembered = getSavedPerms(email, b);
          openPermsModal(email, b, () => openManageAccessModal(email), remembered);
        });
        actions.appendChild(addBtn);
      }
      row.appendChild(left);
      row.appendChild(actions);
      list.appendChild(row);
    });
    accountModalBody.innerHTML = '';
    accountModalBody.appendChild(list);
  } catch (e) {
    console.error(e);
    accountModalBody.innerHTML = `<div class="enterprise-empty"><i class="fa-solid fa-plug-circle-xmark"></i><p>Network error</p></div>`;
  }
}
accountModalClose.addEventListener('click', () => {
  accountModalOverlay.classList.remove('show');
  accountModal.classList.remove('show');
});
accountModalOverlay.addEventListener('click', (e) => {
  if (e.target === accountModalOverlay) {
    accountModalOverlay.classList.remove('show');
    accountModal.classList.remove('show');
  }
});
async function openPermsModal(email, server, onSaved, existingPerms) {
  let serverPerms = null;
  try {
    const resp = await fetch(`/api/settings/accounts/${encodeURIComponent(email)}/perms?server=${encodeURIComponent(server)}`);
    if (resp.ok) {
      const data = await resp.json();
      if (data.ok && data.permissions) {
        serverPerms = data.permissions;
      }
    }
  } catch (e) {
    console.error('Failed to fetch permissions from server:', e);
  }
  const permsToUse = serverPerms || existingPerms;
  const isEdit = !!permsToUse && Object.keys(permsToUse).some(k => permsToUse[k]);
  permModalTitle.textContent = isEdit
    ? `Edit permissions for ${email} on ${server}`
    : `Grant permissions for ${email} on ${server}`;
  permModalBody.innerHTML = buildPermsFormHtml(permsToUse, !!serverPerms);
  permModalOverlay.classList.add('show');
  permModal.classList.add('show');
  permModal.dataset.email = email;
  permModal.dataset.server = server;
  permModal.dataset.mode = isEdit ? 'edit' : 'add';
  permModal._onSaved = typeof onSaved === 'function' ? onSaved : null;
  const selAll = document.getElementById('perm_all');
  const permInputs = Array.from(permModalBody.querySelectorAll('input[type="checkbox"].perm'));
  selAll.checked = permInputs.length > 0 && permInputs.every(cb => cb.checked);
  selAll.addEventListener('change', () => {
    permInputs.forEach(cb => { cb.checked = selAll.checked; });
  });
  permInputs.forEach(cb => {
    cb.addEventListener('change', () => {
      selAll.checked = permInputs.length > 0 && permInputs.every(x => x.checked);
    });
  });
}
function closePermsModal() {
  permModalOverlay.classList.remove('show');
  permModal.classList.remove('show');
  permModal.dataset.email = '';
  permModal.dataset.server = '';
  permModal.dataset.mode = '';
  permModal._onSaved = null;
}
function buildPermsFormHtml(existing, fromServer) {
  const currentSummary = existing ? summarizePerms(existing) : null;
  const permLabels = {
    files_read: { label: 'Read files', icon: 'fa-file' },
    files_delete: { label: 'Delete files', icon: 'fa-trash' },
    files_rename: { label: 'Rename files', icon: 'fa-pen' },
    files_archive: { label: 'Create archives', icon: 'fa-file-zipper' },
    console_write: { label: 'Write to console', icon: 'fa-terminal' },
    server_stop: { label: 'Stop server', icon: 'fa-stop' },
    server_start: { label: 'Start server', icon: 'fa-play' },
    files_upload: { label: 'Upload files', icon: 'fa-upload' },
    files_create: { label: 'Create files/folders', icon: 'fa-folder-plus' },
    activity_logs: { label: 'View activity logs', icon: 'fa-clock-rotate-left' },
    backups_view: { label: 'View backups', icon: 'fa-box-archive' },
    backups_create: { label: 'Create backups', icon: 'fa-box-archive' },
    backups_delete: { label: 'Delete backups', icon: 'fa-trash-can' },
    scheduler_access: { label: 'Access scheduler', icon: 'fa-calendar-days' },
    scheduler_create: { label: 'Create schedules', icon: 'fa-calendar-plus' },
    scheduler_delete: { label: 'Delete schedules', icon: 'fa-calendar-xmark' },
    store_access: { label: 'Access store', icon: 'fa-store' },
    subdomain_show: { label: 'Subdomain Show', icon: 'fa-globe' },
    subdomain_add: { label: 'Subdomain Add', icon: 'fa-plus-circle' },
    server_reinstall: { label: 'Reinstall server', icon: 'fa-rotate' }
  };
  const rows = PERM_KEYS.map((key) => {
    const checked = existing ? !!existing[key] : (key === 'files_read');
    const perm = permLabels[key] || { label: key, icon: 'fa-circle' };
    return `
          <label class="perm-item">
            <input class="perm-checkbox perm" data-key="${key}" type="checkbox" ${checked ? 'checked' : ''}>
            <span class="perm-label">${perm.label}</span>
          </label>
        `;
  }).join('');
  const info = fromServer
    ? `<div class="perm-info">
             <strong>Current permissions:</strong> ${escapeHtml(currentSummary || 'None assigned')}
           </div>`
    : existing
      ? `<div class="perm-info">
             <strong>Cached:</strong> ${escapeHtml(currentSummary)}. Toggle as needed.
           </div>`
      : `<div class="perm-info">
             Select the permissions you want to grant to this user.
           </div>`;
  return `
        ${info}
        <div class="perm-grid">
          <label class="perm-item select-all">
            <input id="perm_all" class="perm-checkbox" type="checkbox">
            <span class="perm-label">Select all permissions</span>
          </label>
          ${rows}
        </div>
      `;
}
permCancel.addEventListener('click', closePermsModal);
if (permCancelBtn) permCancelBtn.addEventListener('click', closePermsModal);
permModalOverlay.addEventListener('click', (e) => { if (e.target === permModalOverlay) closePermsModal(); });
permSave.addEventListener('click', async () => {
  const email = permModal.dataset.email || '';
  const server = permModal.dataset.server || '';
  if (!email || !server) { closePermsModal(); return; }
  const permInputs = Array.from(permModalBody.querySelectorAll('input[type="checkbox"].perm'));
  const selected = {};
  PERM_KEYS.forEach(k => selected[k] = false);
  permInputs.forEach(cb => {
    const key = cb.dataset.key;
    selected[key] = !!cb.checked;
  });
  const finalPerms = selected;
  try {
    permSave.disabled = true;
    const res = await fetch(`/api/settings/accounts/${encodeURIComponent(email)}/grant-perms`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ server, permissions: finalPerms })
    });
    const data = await res.json().catch(() => ({ error: 'Invalid response' }));
    if (!res.ok) {
      alert(data.error || 'Failed to grant permissions');
      return;
    }
    setSavedPerms(email, server, finalPerms);
    const toast = document.createElement('div');
    toast.className = 'perm-success-toast';
    toast.innerHTML = '<i class="fa-solid fa-circle-check"></i> Permissions saved successfully';
    document.body.appendChild(toast);
    setTimeout(() => { toast.classList.add('show'); }, 10);
    setTimeout(() => {
      toast.classList.remove('show');
      setTimeout(() => toast.remove(), 300);
    }, 2000);
    closePermsModal();
    if (typeof permModal._onSaved === 'function') permModal._onSaved();
  } catch (e) {
    console.error(e);
    alert('Network error');
  } finally {
    permSave.disabled = false;
  }
});
document.addEventListener('DOMContentLoaded', () => {
  const WAIT_INTERVAL = 100;
  const WAIT_TIMEOUT = 3000;
  const sidebarSelector = '#sidebarNav';
  const pathnameDefaults = [
    { match: /^\/servers(?:\/|$)/i, panel: 'servers' },
    { match: /^\/settings(?:\/|$)/i, panel: 'preferences' }
  ];
  function getRequestedPanel() {
    const params = new URLSearchParams(location.search);
    const p = params.get('panel');
    if (p) return p;
    if (location.hash && location.hash.length > 1) return location.hash.slice(1);
    const path = location.pathname || '/';
    for (const r of pathnameDefaults) {
      if (r.match.test(path)) return r.panel;
    }
    return null;
  }
  function tryOpenPanel(panel) {
    if (!panel) return false;
    const btn = document.querySelector(`${sidebarSelector} button[data-panel="${panel}"]`);
    if (btn) {
      try { btn.focus?.(); btn.click(); } catch { btn.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true })); }
      console.log(`[openPanel] opened panel: ${panel}`);
      return true;
    }
    return false;
  }
  async function waitForSidebarAndOpen() {
    const panel = getRequestedPanel();
    if (!panel) { console.log('[openPanel] no panel requested'); return; }
    const start = Date.now();
    if (document.querySelector(sidebarSelector)) { if (tryOpenPanel(panel)) return; }
    while (Date.now() - start < WAIT_TIMEOUT) {
      const side = document.querySelector(sidebarSelector);
      if (side) { if (tryOpenPanel(panel)) return; }
      await new Promise(r => setTimeout(r, WAIT_INTERVAL));
    }
    console.warn(`[openPanel] failed to open panel "${panel}"`);
    const directPanelEl = document.getElementById(panel);
    if (directPanelEl) {
      document.querySelectorAll('.panel-section').forEach(s => s.classList.remove('active'));
      directPanelEl.classList.add('active');
      console.log(`[openPanel] opened panel by id: ${panel}`);
    }
  }
  waitForSidebarAndOpen();
});
async function fetchWithTimeout(url, opts = {}, ms = 2500) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  try {
    const res = await fetch(url, { ...opts, signal: ctrl.signal });
    clearTimeout(t);
    return res;
  } catch (e) {
    clearTimeout(t);
    throw e;
  }
}
function setNodeStatus(row, cls, label) {
  const dot = row.querySelector('.status-dot');
  if (!dot) return;
  dot.classList.remove('green', 'red', 'gray');
  dot.classList.add(cls);
  if (label) dot.title = label;
}
function showCreateNodeModal(show) {
  if (show) { createNodeOverlay.classList.add('show'); createNodeModal.classList.add('show'); }
  else { createNodeOverlay.classList.remove('show'); createNodeModal.classList.remove('show'); createNodeFeedback.textContent = ''; }
}
openCreateNodeBtn.addEventListener('click', () => showCreateNodeModal(true));
createNodeClose.addEventListener('click', () => showCreateNodeModal(false));
createNodeCancel.addEventListener('click', () => showCreateNodeModal(false));
createNodeOverlay.addEventListener('click', (e) => { if (e.target === createNodeOverlay) showCreateNodeModal(false); });
document.querySelectorAll('input[name="cn_ports_mode"]').forEach(r => {
  r.addEventListener('change', () => {
    const mode = document.querySelector('input[name="cn_ports_mode"]:checked').value;
    cn_ports_range.style.display = (mode === 'range') ? 'grid' : 'none';
    cn_ports_list.style.display = (mode === 'list') ? 'block' : 'none';
  });
});
function bindEditPortsRadios() {
  nodeModal.querySelectorAll('input[name="nm_ports_mode"]').forEach(r => {
    r.addEventListener('change', () => {
      const mode = nodeModal.querySelector('input[name="nm_ports_mode"]:checked').value;
      nm_ports_range.style.display = (mode === 'range') ? 'grid' : 'none';
      nm_ports_list.style.display = (mode === 'list') ? 'block' : 'none';
    });
  });
}
function parsePortsInput(mode, startEl, countEl, listEl) {
  if (mode === 'range') {
    const start = parseInt(startEl.value, 10);
    const count = parseInt(countEl.value, 10);
    if (!Number.isFinite(start) || start < 1 || start > 65535) throw new Error('Invalid start port');
    if (!Number.isFinite(count) || count < 1 || count > 1000) throw new Error('Invalid count');
    if (start + count - 1 > 65535) throw new Error('Port range exceeds 65535');
    return { mode: 'range', start, count };
  } else {
    const raw = (listEl.value || '').trim();
    if (!raw) throw new Error('Provide at least one port');
    const parts = raw.split(',').map(s => parseInt(s.trim(), 10)).filter(n => Number.isFinite(n));
    const uniq = Array.from(new Set(parts));
    if (uniq.length === 0) throw new Error('No valid ports');
    if (uniq.length > 1000) throw new Error('Too many ports (max 1000)');
    if (uniq.some(p => p < 1 || p > 65535)) throw new Error('Ports must be 1..65535');
    return { mode: 'list', ports: uniq };
  }
}
function countPorts(alloc) {
  if (!alloc) return 0;
  if (alloc.mode === 'range') return Math.max(0, parseInt(alloc.count || 0, 10));
  if (alloc.mode === 'list' && Array.isArray(alloc.ports)) return alloc.ports.length;
  return 0;
}
function portsSummary(alloc) {
  if (!alloc) return '—';
  if (alloc.mode === 'range') return `range ${alloc.start}..${alloc.start + alloc.count - 1} (${alloc.count})`;
  if (alloc.mode === 'list') return `${alloc.ports.length} ports`;
  return '—';
}
async function loadNodes(page, search) {
  if (typeof page === 'undefined') page = _nodesPage;
  if (typeof search === 'undefined') search = _nodesSearch;
  _nodesPage = page;
  _nodesSearch = search;
  const isFirstLoad = !nodesListEl.querySelector('.node-row');
  if (isFirstLoad) {
    nodesListEl.innerHTML = `<div style="padding:12px;color:var(--muted)">Loading…</div>`;
  }
  try {
    const params = new URLSearchParams({ page: String(page), limit: String(_nodesLimit) });
    if (search) params.set('search', search);
    const res = await fetch('/api/nodes?' + params.toString());
    if (!res.ok) {
      nodesListEl.innerHTML = `<div style="padding:12px;color:var(--muted)">Failed to load nodes</div>`;
      return;
    }
    const data = await res.json().catch(() => ({ nodes: [], total: 0, page: 1, totalPages: 1 }));
    const nodes = Array.isArray(data.nodes) ? data.nodes : [];
    const total = Number(data.total || nodes.length);
    const totalPages = Number(data.totalPages || 1);
    renderNodes(nodes);
    renderNodesPagination(page, totalPages, total);
  } catch (e) {
    console.error(e);
    nodesListEl.innerHTML = `<div style="padding:12px;color:var(--muted)">Network error</div>`;
  }
}
let _nodesPage = 1;
let _nodesSearch = '';
const _nodesLimit = 50;
let _serversPage = 1;
let _serversSearch = '';
const _serversLimit = 50;
let _serversSearchTimer;
let _templatesPage = 1;
let _templatesSearch = '';
const _templatesLimit = 50;
let _templatesSearchTimer;
let _nodesTimer;
let _nodesSearchTimer;
const nodesSearchInput = document.getElementById('nodesSearchInput');
const nodesPaginationEl = document.getElementById('nodesPagination');

function stopNodesPolling() { clearInterval(_nodesTimer); _nodesTimer = null; }
function startNodesPolling() {
  stopNodesPolling();
  _nodesTimer = setInterval(() => loadNodes(), 30000);
}

document.querySelector('button[data-panel="nodes"]').addEventListener('click', () => {
  _nodesPage = 1;
  loadNodes();
  startNodesPolling();
});
sidebarNav.addEventListener('click', (e) => {
  const btn = e.target.closest('button');
  if (btn && btn.dataset.panel !== 'nodes') stopNodesPolling();
});

if (nodesSearchInput) {
  nodesSearchInput.addEventListener('input', () => {
    clearTimeout(_nodesSearchTimer);
    _nodesSearchTimer = setTimeout(() => {
      _nodesPage = 1;
      loadNodes(1, nodesSearchInput.value.trim());
    }, 300);
  });
}

const serversSearchInput = document.getElementById('serversSearchInput');
if (serversSearchInput) {
  serversSearchInput.addEventListener('input', () => {
    clearTimeout(_serversSearchTimer);
    _serversSearchTimer = setTimeout(() => {
      _serversPage = 1;
      loadServers(1, serversSearchInput.value.trim());
    }, 300);
  });
}

const templatesSearchInput = document.getElementById('templatesSearchInput');
if (templatesSearchInput) {
  templatesSearchInput.addEventListener('input', () => {
    clearTimeout(_templatesSearchTimer);
    _templatesSearchTimer = setTimeout(() => {
      _templatesPage = 1;
      loadTemplates(1, templatesSearchInput.value.trim());
    }, 300);
  });
}

function renderNodesPagination(currentPage, totalPages, total) {
  if (!nodesPaginationEl) return;
  if (totalPages <= 1) { nodesPaginationEl.innerHTML = ''; return; }
  const startItem = (currentPage - 1) * _nodesLimit + 1;
  const endItem = Math.min(currentPage * _nodesLimit, total);
  nodesPaginationEl.innerHTML = `
    <div class="nodes-pagination-inner">
      <span class="nodes-pagination-info">${startItem}–${endItem} of ${total} nodes</span>
      <div class="nodes-pagination-btns">
        <button class="btn ghost btn-sm" ${currentPage <= 1 ? 'disabled' : ''} data-page="${currentPage - 1}"><i class="fa-solid fa-chevron-left"></i></button>
        <span class="nodes-pagination-current">Page ${currentPage} / ${totalPages}</span>
        <button class="btn ghost btn-sm" ${currentPage >= totalPages ? 'disabled' : ''} data-page="${currentPage + 1}"><i class="fa-solid fa-chevron-right"></i></button>
      </div>
    </div>
  `;
  nodesPaginationEl.querySelectorAll('button[data-page]').forEach(btn => {
    btn.addEventListener('click', () => {
      const p = parseInt(btn.dataset.page, 10);
      if (p >= 1 && p <= totalPages) loadNodes(p);
    });
  });
}

function renderNodes(nodes) {
  if (!nodes || nodes.length === 0) {
    nodesListEl.innerHTML = `
          <div class="nodes-empty">
            <div><i class="fa-solid fa-circle-info"></i> ${_nodesSearch ? 'No nodes match your search.' : "You haven't created any node yet."}</div>
            ${!_nodesSearch ? '<button class="btn" data-open-create-node><i class="fa-solid fa-circle-plus"></i> Create node</button>' : ''}
          </div>
        `;
    const emptyCreateBtn = nodesListEl.querySelector('[data-open-create-node]');
    if (emptyCreateBtn && openCreateNodeBtn) {
      emptyCreateBtn.addEventListener('click', () => openCreateNodeBtn.click());
    }
    return;
  }
  const frag = document.createDocumentFragment();
  const now = Date.now();
  for (let i = 0; i < nodes.length; i++) {
    const n = nodes[i];
    const row = document.createElement('div');
    row.className = 'node-row';
    const nodeId = n.id || n.uuid || '';
    row.dataset.id = nodeId;
    const lastSeen = Number(n.last_seen || n.lastSeen || n.lastHeartbeat || 0);
    const fresh = lastSeen > 0 && (now - lastSeen) < 120000;
    let statusClass = 'gray';
    let statusLabel = 'No recent heartbeat';
    if (n.online === false) {
      statusClass = 'red';
      statusLabel = 'Offline';
    }
    if (fresh || n.online === true) {
      if (n.port_ok === false) {
        statusClass = 'red';
        statusLabel = 'Heartbeat ok, port unreachable';
      } else if (n.port_ok === true) {
        statusClass = 'green';
        statusLabel = 'Online';
      } else {
        statusClass = 'gray';
        statusLabel = 'Checking…';
      }
    }
    const portCount = countPorts(n.ports);
    row.innerHTML = `
          <div class="node-left">
            <span class="status-dot ${statusClass}" title="${statusLabel}"></span>
            <div class="iconWrap"><i class="fa-solid fa-diagram-project"></i></div>
            <div>
              <div style="font-weight:700">${escapeHtml(n.name || 'unnamed')}</div>
              <div class="kpi">${escapeHtml(n.address || '0.0.0.0')} • API ${Number(n.api_port || 8080)} • SFTP ${Number(n.sftp_port || 2022)} • RAM ${Number(n.ram_mb || 0) / 1024 | 0} GB • Disk ${Number(n.disk_gb || 0)} GB • Ports ${portCount}</div>
            </div>
          </div>
          <div class="row-actions">
            <span class="inline-badge">ID: ${escapeHtml(nodeId.toString())}</span>
            <button class="btn ghost open-stats" title="View node statistics"><i class="fa-solid fa-chart-line"></i> Statistics</button>
            <button class="btn ghost open-node"><i class="fa-solid fa-pen-to-square"></i> Configure</button>
          </div>
        `;
    row.querySelector('.open-stats').addEventListener('click', (ev) => { ev.stopPropagation(); openNodeStatsModal(nodeId, n.name); });
    row.querySelector('.open-node').addEventListener('click', (ev) => { ev.stopPropagation(); openNodeModal(nodeId); });
    row.addEventListener('click', () => openNodeModal(nodeId));
    frag.appendChild(row);
  }
  nodesListEl.innerHTML = '';
  nodesListEl.appendChild(frag);
}
if (typeof window.io === 'function') {
  try {
    const _nodeStatusSocket = window.io();
    _nodeStatusSocket.on('node:status', (data) => {
      if (!data || !data.id) return;
      const row = nodesListEl.querySelector(`.node-row[data-id="${data.id}"]`);
      if (!row) return;
      if (data.online) {
        setNodeStatus(row, 'green', 'Online');
      } else {
        setNodeStatus(row, 'red', 'Offline / unreachable');
      }
    });
    _nodeStatusSocket.emit('dashboard:subscribe', []);
  } catch { }
}

function isIPAddress(addr) {
  if (!addr) return true;
  if (/^\d{1,3}(\.\d{1,3}){3}$/.test(addr)) return true;
  if (/^\[?[0-9a-fA-F:]+\]?$/.test(addr)) return true;
  return false;
}
if (cn_address && cn_ssl_row) {
  cn_address.addEventListener('input', () => {
    const addr = cn_address.value.trim();
    if (addr && !isIPAddress(addr)) {
      cn_ssl_row.style.display = '';
    } else {
      cn_ssl_row.style.display = 'none';
      if (cn_ssl_enabled) cn_ssl_enabled.checked = false;
    }
  });
}

createNodeSave.addEventListener('click', async () => {
  try {
    createNodeFeedback.textContent = '';
    const name = cn_name.value.trim();
    const address = cn_address.value.trim();
    const cpu_cores = cn_cpu_cores ? parseInt(cn_cpu_cores.value, 10) : 0;
    const ram_gb = parseInt(cn_ram_gb.value, 10);
    const disk_gb = parseInt(cn_disk_gb.value, 10);
    const mode = document.querySelector('input[name="cn_ports_mode"]:checked').value;
    if (!name) throw new Error('Name required');
    if (!address) throw new Error('Address required');
    if (!Number.isFinite(ram_gb) || ram_gb < 1) throw new Error('Invalid RAM');
    if (!Number.isFinite(disk_gb) || disk_gb < 10) throw new Error('Invalid Disk');
    const ports = parsePortsInput(mode, cn_port_start, cn_port_count, cn_port_list);
    const max_upload_mb = parseInt(cn_max_upload_mb?.value || '10240', 10);
    if (!Number.isFinite(max_upload_mb) || max_upload_mb < 1 || max_upload_mb > 100000) throw new Error('Upload limit must be between 1 and 100000 MB');
    const api_port = parseInt(cn_daemon_port?.value || '8080', 10);
    if (!Number.isFinite(api_port) || api_port < 1 || api_port > 65535) throw new Error('Daemon port must be between 1 and 65535');
    const sftp_port = parseInt(cn_sftp_port?.value || '2022', 10);
    if (!Number.isFinite(sftp_port) || sftp_port < 1 || sftp_port > 65535) throw new Error('SFTP port must be between 1 and 65535');
    const ssl_enabled = !!(cn_ssl_enabled && cn_ssl_enabled.checked && !isIPAddress(address));
    const payload = {
      name, address,
      cpu_cores,
      ram_mb: ram_gb * 1024,
      disk_gb,
      ports,
      max_upload_mb,
      api_port,
      sftp_port,
      ssl_enabled
    };
    createNodeSave.disabled = true;
    createNodeSave.innerHTML = 'Creating…';
    const res = await fetch('/api/nodes', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
    const data = await res.json().catch(() => ({ error: 'Invalid response' }));
    if (!res.ok) {
      createNodeFeedback.textContent = data.error || 'Failed to create node';
      createNodeSave.disabled = false; createNodeSave.innerHTML = '<i class="fa-solid fa-plus"></i> Create Node';
      return;
    }
    showCreateNodeModal(false);
    await loadNodes(1, '');
    if (data.id || data.uuid) {
      openNodeModal(data.id || data.uuid, true);
    }
  } catch (err) {
    console.error(err);
    createNodeFeedback.textContent = err.message || 'Error';
  } finally {
    createNodeSave.disabled = false;
    createNodeSave.innerHTML = '<i class="fa-solid fa-plus"></i> Create Node';
  }
});
function showNodeModal(show) {
  if (show) { nodeModalOverlay.classList.add('show'); nodeModal.classList.add('show'); }
  else { nodeModalOverlay.classList.remove('show'); nodeModal.classList.remove('show'); }
}
nodeModalClose.addEventListener('click', () => showNodeModal(false));
if (nodeModalCancelBtn) nodeModalCancelBtn.addEventListener('click', () => showNodeModal(false));
nodeModalOverlay.addEventListener('click', (e) => { if (e.target === nodeModalOverlay) showNodeModal(false); });
nm_tabs.forEach(btn => {
  btn.addEventListener('click', () => {
    nm_tabs.forEach(b => b.classList.remove('active')); btn.classList.add('active');
    const tab = btn.dataset.tab;
    [tab_build, tab_linking, tab_deletion].forEach(p => p.classList.remove('active'));
    if (tab === 'build') { tab_build.classList.add('active'); if (buildFooter) buildFooter.style.display = 'flex'; }
    if (tab === 'linking') { tab_linking.classList.add('active'); if (buildFooter) buildFooter.style.display = 'none'; }
    if (tab === 'deletion') { tab_deletion.classList.add('active'); if (buildFooter) buildFooter.style.display = 'none'; }
  });
});
let CURRENT_NODE = null;
async function openNodeModal(id, focusLinking = false) {
  try {
    const res = await fetch(`/api/nodes/${encodeURIComponent(id)}`);
    const node = await res.json();
    if (!res.ok) throw new Error(node.error || 'Failed to load node');
    CURRENT_NODE = node;
    fillNodeModal(node, focusLinking);
    showNodeModal(true);
  } catch (e) {
    alert(e.message || 'Failed to open node');
  }
}
function setRadio(name, value) {
  const el = nodeModal.querySelector(`input[name="${name}"][value="${value}"]`);
  if (el) { el.checked = true; el.dispatchEvent(new Event('change')); }
}
function fillNodeModal(node, focusLinking = false) {
  nm_title.innerHTML = `<i class="fa-solid fa-diagram-project"></i> ${escapeHtml(node.name || 'Node')}`;
  nm_subtitle.textContent = `${escapeHtml(node.address || '0.0.0.0')} • ${Number(node.cpu_cores || 0)} cores • ${(Number(node.ram_mb || 0) / 1024 | 0)} GB RAM`;
  nm_name.value = node.name || '';
  nm_address.value = node.address || '';
  if (nm_cpu_cores) nm_cpu_cores.value = Number(node.cpu_cores || 0) || '';
  nm_ram_gb.value = (Number(node.ram_mb || 0) / 1024 | 0);
  nm_disk_gb.value = Number(node.disk_gb || 0);
  if (nm_max_upload_mb) nm_max_upload_mb.value = Number(node.max_upload_mb || 10240);
  if (nm_daemon_port) nm_daemon_port.value = Number(node.api_port || 8080);
  if (nm_sftp_port) nm_sftp_port.value = Number(node.sftp_port || 2022);
  if (nm_ssl_row && nm_ssl_enabled) {
    const addr = (node.address || '').trim();
    if (addr && !isIPAddress(addr)) {
      nm_ssl_row.style.display = '';
      nm_ssl_enabled.checked = !!node.ssl_enabled;
    } else {
      nm_ssl_row.style.display = 'none';
      nm_ssl_enabled.checked = false;
    }
  }
  const alloc = node.ports || { mode: 'range', start: 25565, count: 10 };
  if (alloc.mode === 'list') {
    setRadio('nm_ports_mode', 'list');
    nm_ports_range.style.display = 'none';
    nm_ports_list.style.display = 'block';
    nm_port_list.value = Array.isArray(alloc.ports) ? alloc.ports.join(',') : '';
  } else {
    setRadio('nm_ports_mode', 'range');
    nm_ports_range.style.display = 'grid';
    nm_ports_list.style.display = 'none';
    nm_port_start.value = alloc.start || '';
    nm_port_count.value = alloc.count || '';
  }
  bindEditPortsRadios();
  nm_configYml.value = generateNodeConfigYml(node);
  nm_cmd.value = '';
  nm_cmd.style.display = 'none';
  nm_cmdActions.style.display = 'none';
  nm_tabs.forEach(b => b.classList.remove('active'));
  [tab_build, tab_linking, tab_deletion].forEach(p => p.classList.remove('active'));
  if (focusLinking) {
    nodeModal.querySelector('.node-tab[data-tab="linking"]').classList.add('active');
    tab_linking.classList.add('active');
    if (buildFooter) buildFooter.style.display = 'none';
  } else {
    nodeModal.querySelector('.node-tab[data-tab="build"]').classList.add('active');
    tab_build.classList.add('active');
    if (buildFooter) buildFooter.style.display = 'flex';
  }
}
function generateNodeConfigYml(node) {
  const panelUrl = location.origin;
  const uuid = node.uuid || node.id || '';
  const token_id = node.token_id || '';
  const token = node.token || '';
  const sslEnabled = !!node.ssl_enabled;
  const yml = [
    `debug: false`,
    `uuid: ${uuid}`,
    `token_id: ${token_id}`,
    `token: ${token}`,
    `api:`,
    `  host: 0.0.0.0`,
    `  port: ${node.api_port || 8080}`,
    `  ssl:`,
    `    enabled: ${sslEnabled}`,
    `    cert: ""`,
    `    key: ""`,
    `  upload_limit: ${node.max_upload_mb || 10240}`,
    `system:`,
    `  data: /var/lib/node`,
    `  sftp:`,
    `    bind_port: ${node.sftp_port || 2022}`,
    `allowed_mounts: []`,
    `panel:`,
    `  url: ${panelUrl}`,
    `  node_id: ${node.id || node.uuid || ''}`
  ].join('\n');
  return yml;
}
function generateOneTimeCommand(node) {
  const yml = generateNodeConfigYml(node);
  const safeYml = yml.replace(/'/g, "'\\''").replace(/\n/g, '\\n');
  return `mkdir -p /var/lib/node && printf '${safeYml}' > /var/lib/node/config.yml`;
}
function copyToClipboard(text) {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    return navigator.clipboard.writeText(text);
  }
  return new Promise((resolve, reject) => {
    const textArea = document.createElement("textarea");
    textArea.value = text;
    textArea.style.position = "fixed";
    textArea.style.left = "-9999px";
    textArea.style.top = "0";
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();
    try {
      const successful = document.execCommand('copy');
      if (successful) resolve();
      else reject(new Error('Copy failed'));
    } catch (err) {
      reject(err);
    } finally {
      document.body.removeChild(textArea);
    }
  });
}

nm_copyConfig.addEventListener('click', () => {
  copyToClipboard(nm_configYml.value || '').then(() => { nm_copyConfig.innerHTML = '<i class="fa-solid fa-check"></i>'; setTimeout(() => nm_copyConfig.innerHTML = '<i class="fa-solid fa-copy"></i>', 1500); });
});
nm_downloadConfig.addEventListener('click', () => {
  const blob = new Blob([nm_configYml.value || ''], { type: 'text/yaml' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = 'config.yml'; a.click();
  URL.revokeObjectURL(url);
});
nm_showCmd.addEventListener('click', () => {
  if (!CURRENT_NODE) return;
  nm_cmd.value = generateOneTimeCommand(CURRENT_NODE);
  nm_cmd.style.display = 'block';
  nm_cmdActions.style.display = 'flex';
});
nm_copyCmd.addEventListener('click', () => {
  copyToClipboard(nm_cmd.value || '').then(() => { nm_copyCmd.innerHTML = '<i class="fa-solid fa-check"></i> Copied'; setTimeout(() => nm_copyCmd.innerHTML = '<i class="fa-solid fa-copy"></i> Copy Command', 1500); });
});
if (nm_address && nm_ssl_row) {
  nm_address.addEventListener('input', () => {
    const addr = nm_address.value.trim();
    if (addr && !isIPAddress(addr)) {
      nm_ssl_row.style.display = '';
    } else {
      nm_ssl_row.style.display = 'none';
      if (nm_ssl_enabled) nm_ssl_enabled.checked = false;
    }
  });
}
nm_save.addEventListener('click', async () => {
  if (!CURRENT_NODE) return;
  try {
    nm_save.disabled = true; nm_save.innerHTML = 'Saving…'; nm_saveFeedback.textContent = '';
    const name = nm_name.value.trim();
    const address = nm_address.value.trim();
    const cpu_cores = nm_cpu_cores ? parseInt(nm_cpu_cores.value, 10) : 0;
    const ram_gb = parseInt(nm_ram_gb.value, 10);
    const disk_gb = parseInt(nm_disk_gb.value, 10);
    const mode = nodeModal.querySelector('input[name="nm_ports_mode"]:checked')?.value || 'range';
    if (!name) throw new Error('Name required');
    if (!address) throw new Error('Address required');
    if (!Number.isFinite(ram_gb) || ram_gb < 1) throw new Error('Invalid RAM');
    if (!Number.isFinite(disk_gb) || disk_gb < 10) throw new Error('Invalid Disk');
    const ports = parsePortsInput(mode, nm_port_start, nm_port_count, nm_port_list);
    const max_upload_mb = parseInt(nm_max_upload_mb?.value || '10240', 10);
    if (!Number.isFinite(max_upload_mb) || max_upload_mb < 1 || max_upload_mb > 100000) throw new Error('Upload limit must be between 1 and 100000 MB');
    const api_port = parseInt(nm_daemon_port?.value || '8080', 10);
    if (!Number.isFinite(api_port) || api_port < 1 || api_port > 65535) throw new Error('Daemon port must be between 1 and 65535');
    const sftp_port = parseInt(nm_sftp_port?.value || '2022', 10);
    if (!Number.isFinite(sftp_port) || sftp_port < 1 || sftp_port > 65535) throw new Error('SFTP port must be between 1 and 65535');
    const payload = { name, address, cpu_cores, ram_mb: ram_gb * 1024, disk_gb, ports, max_upload_mb, api_port, sftp_port, ssl_enabled: !!(nm_ssl_enabled && nm_ssl_enabled.checked && !isIPAddress(address)) };
    const res = await fetch(`/api/nodes/${encodeURIComponent(CURRENT_NODE.id || CURRENT_NODE.uuid)}`, {
      method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || 'Failed to save');
    CURRENT_NODE = Object.assign({}, CURRENT_NODE, data, payload);
    nm_configYml.value = generateNodeConfigYml(CURRENT_NODE);
    nm_saveFeedback.textContent = 'Saved';
    await loadNodes();
  } catch (e) {
    nm_saveFeedback.textContent = e.message || 'Error';
  } finally {
    nm_save.disabled = false; nm_save.innerHTML = '<i class="fa-solid fa-check"></i> Save Changes';
  }
});
nm_delete.addEventListener('click', async () => {
  if (!CURRENT_NODE) return;
  const ok = confirm(`Delete node "${CURRENT_NODE.name}"? This action is permanent.`);
  if (!ok) return;
  try {
    nm_delete.disabled = true;
    nm_deleteFeedback.textContent = '';
    const res = await fetch(`/api/nodes/${encodeURIComponent(CURRENT_NODE.id || CURRENT_NODE.uuid)}`, { method: 'DELETE' });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || 'Failed to delete');
    nm_deleteFeedback.textContent = 'Deleted';
    showNodeModal(false);
    await loadNodes();
  } catch (e) {
    nm_deleteFeedback.textContent = e.message || 'Error';
  } finally {
    nm_delete.disabled = false;
  }
});

document.querySelectorAll('.toggle-pass').forEach(btn => {
  const input = btn.parentElement.querySelector('input');
  const icon = btn.querySelector('i');

  btn.addEventListener('click', () => {
    const isPassword = input.type === 'password';
    input.type = isPassword ? 'text' : 'password';

    icon.classList.toggle('fa-eye', !isPassword);
    icon.classList.toggle('fa-eye-slash', isPassword);
  });
});

(function () {
  const rateLimiterCard = document.getElementById('rateLimiterCard');
  const rateLimiterToggle = document.getElementById('rateLimiterToggle');
  const rateLimiterDetails = document.getElementById('rateLimiterDetails');
  const rateLimiterLimit = document.getElementById('rateLimiterLimit');
  const rateLimiterWindow = document.getElementById('rateLimiterWindow');
  const rateLimiterSave = document.getElementById('rateLimiterSave');
  const rateLimiterFeedback = document.getElementById('rateLimiterFeedback');
  const rateLimiterSummary = document.getElementById('rateLimiterSummary');

  if (!rateLimiterCard) return;

  const rateLimiterSwitchLabel = rateLimiterCard.querySelector('.switch');
  if (rateLimiterSwitchLabel) {
    rateLimiterSwitchLabel.addEventListener('click', (e) => e.stopPropagation());
  }

  let detailsOpen = false;

  function updateSummary() {
    const limit = parseInt(rateLimiterLimit.value, 10) || 5;
    const window = parseInt(rateLimiterWindow.value, 10) || 120;
    rateLimiterSummary.textContent = `${limit} requests per ${window} seconds`;
  }

  function showFeedback(msg, isError = false) {
    rateLimiterFeedback.textContent = msg;
    rateLimiterFeedback.style.color = isError ? '#ef4444' : '#22c55e';
    setTimeout(() => { rateLimiterFeedback.textContent = ''; }, 3000);
  }

  let securityActionTokens = {};

  async function loadSecuritySettings() {
    try {
      const res = await fetch('/api/settings/security');
      if (!res.ok) return;
      const data = await res.json();
      rateLimiterToggle.checked = !!data.rate_limiting;
      rateLimiterLimit.value = data.limit || 5;
      rateLimiterWindow.value = data.window_seconds || 120;
      securityActionTokens = data.actionTokens || {};
      updateSummary();
    } catch (e) {
      console.error('Failed to load security settings:', e);
    }
  }

  async function saveSecuritySettings() {
    const rate_limiting = rateLimiterToggle.checked;
    const limit = parseInt(rateLimiterLimit.value, 10);
    const window_seconds = parseInt(rateLimiterWindow.value, 10);

    if (!limit || limit < 1 || limit > 10000) {
      showFeedback('Limit must be between 1 and 10000', true);
      return;
    }
    if (!window_seconds || window_seconds < 1 || window_seconds > 86400) {
      showFeedback('Window must be between 1 and 86400 seconds', true);
      return;
    }

    rateLimiterSave.disabled = true;
    try {
      const res = await fetch('/api/settings/security', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-action-token': securityActionTokens.updateSecurity || ''
        },
        body: JSON.stringify({ rate_limiting, limit, window_seconds })
      });
      const data = await res.json();
      if (!res.ok) {
        showFeedback(data.error || 'Failed to save', true);
      } else {
        showFeedback('Settings saved successfully!');
        await loadSecuritySettings();
      }
    } catch (e) {
      showFeedback('Network error', true);
    } finally {
      rateLimiterSave.disabled = false;
    }
  }

  rateLimiterCard.addEventListener('click', (e) => {
    if (e.target.closest('.switch') || e.target.closest('input') || e.target.closest('button')) return;
    detailsOpen = !detailsOpen;
    rateLimiterDetails.style.display = detailsOpen ? 'block' : 'none';
  });

  rateLimiterToggle.addEventListener('change', async () => {
    await saveSecuritySettings();
  });

  rateLimiterLimit.addEventListener('input', updateSummary);
  rateLimiterWindow.addEventListener('input', updateSummary);

  rateLimiterSave.addEventListener('click', (e) => {
    e.stopPropagation();
    saveSecuritySettings();
  });

  [rateLimiterLimit, rateLimiterWindow].forEach(el => {
    el.addEventListener('click', (e) => e.stopPropagation());
  });

  loadSecuritySettings();

  document.querySelector('button[data-panel="security"]')?.addEventListener('click', loadSecuritySettings);
})();

(function () {
  const captchaCard = document.getElementById('captchaCard');
  if (!captchaCard) return;

  const captchaToggle = document.getElementById('captchaToggle');
  const captchaDetails = document.getElementById('captchaDetails');
  const captchaModalOverlay = document.getElementById('captchaModalOverlay');
  const captchaModal = document.getElementById('captchaModal');
  const captchaSiteKey = document.getElementById('captchaSiteKey');
  const captchaSecretKey = document.getElementById('captchaSecretKey');
  const captchaSecretToggle = document.getElementById('captchaSecretToggle');
  const captchaModalClose = document.getElementById('captchaModalClose');
  const captchaModalCancel = document.getElementById('captchaModalCancel');
  const captchaModalSave = document.getElementById('captchaModalSave');
  const captchaModalFeedback = document.getElementById('captchaModalFeedback');

  const captchaSwitchLabel = captchaCard.querySelector('.switch');
  if (captchaSwitchLabel) {
    captchaSwitchLabel.addEventListener('click', (e) => e.stopPropagation());
  }

  let captchaEnabled = false;
  let isLoading = false;
  let detailsOpen = false;
  let pendingToggleState = null;

  function showFeedback(msg, isError = false) {
    if (!captchaModalFeedback) return;
    captchaModalFeedback.textContent = msg;
    captchaModalFeedback.style.color = isError ? '#ef4444' : '#22c55e';
    if (msg) {
      setTimeout(() => {
        if (captchaModalFeedback) captchaModalFeedback.textContent = '';
      }, 4000);
    }
  }

  function openModal() {
    if (!captchaModalOverlay || !captchaModal) return;
    captchaSiteKey.value = '';
    captchaSecretKey.value = '';
    captchaSecretKey.type = 'password';
    if (captchaSecretToggle) {
      const icon = captchaSecretToggle.querySelector('i');
      if (icon) icon.className = 'fa-solid fa-eye';
    }
    showFeedback('');
    captchaModalOverlay.setAttribute('aria-hidden', 'false');
    captchaModalOverlay.style.display = 'flex';
    captchaModalOverlay.classList.add('show');
    void captchaModal.offsetWidth;
    captchaModal.classList.add('show');
    setTimeout(() => captchaSiteKey.focus(), 100);
  }

  function closeModal(revertToggle = false) {
    if (!captchaModalOverlay) return;
    const focusTarget = captchaToggle || document.body;
    focusTarget.focus();
    setTimeout(() => {
      captchaModalOverlay.setAttribute('aria-hidden', 'true');
      captchaModalOverlay.classList.remove('show');
      if (captchaModal) captchaModal.classList.remove('show');
      captchaModalOverlay.style.display = 'none';
    }, 10);
    if (revertToggle && pendingToggleState !== null) {
      captchaToggle.checked = !pendingToggleState;
    }
    pendingToggleState = null;
  }

  function updateUI() {
    if (captchaToggle) captchaToggle.checked = captchaEnabled;
    if (captchaDetails) captchaDetails.style.display = captchaEnabled ? 'block' : 'none';
  }

  async function loadCaptchaStatus() {
    if (isLoading) return;
    isLoading = true;
    try {
      const res = await fetch('/api/settings/security');
      if (!res.ok) return;
      const data = await res.json();
      captchaEnabled = !!data.captcha_enabled;
      if (data.actionTokens) securityActionTokens = data.actionTokens;
      updateUI();
    } catch (e) {
      console.error('Failed to load captcha status:', e);
    } finally {
      isLoading = false;
    }
  }

  captchaToggle.addEventListener('change', async () => {
    const newState = captchaToggle.checked;
    pendingToggleState = newState;

    if (newState) {
      openModal();
    } else {
      if (!confirm('Are you sure you want to disable captcha? This will remove your configured keys.')) {
        captchaToggle.checked = true;
        pendingToggleState = null;
        return;
      }

      try {
        const res = await fetch('/api/settings/captcha', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'x-action-token': securityActionTokens.updateCaptcha || ''
          },
          body: JSON.stringify({ enabled: false })
        });
        const data = await res.json();
        if (!res.ok) {
          alert(data.error || 'Failed to disable captcha');
          captchaToggle.checked = true;
        } else {
          captchaEnabled = false;
          updateUI();
          await loadCaptchaStatus();
        }
      } catch (e) {
        alert('Network error');
        captchaToggle.checked = true;
      }
      pendingToggleState = null;
    }
  });

  captchaCard.addEventListener('click', (e) => {
    if (e.target.closest('.switch') || e.target.closest('input') || e.target.closest('button')) return;
    if (!captchaEnabled) return;
    detailsOpen = !detailsOpen;
    if (captchaDetails) captchaDetails.style.display = detailsOpen ? 'block' : 'none';
  });

  captchaModalCancel.addEventListener('click', () => closeModal(true));
  if (captchaModalClose) captchaModalClose.addEventListener('click', () => closeModal(true));

  captchaModalOverlay.addEventListener('click', (e) => {
    if (e.target === captchaModalOverlay) closeModal(true);
  });

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && captchaModalOverlay.style.display === 'flex') {
      closeModal(true);
    }
  });

  if (captchaSecretToggle) {
    captchaSecretToggle.addEventListener('click', () => {
      const isPassword = captchaSecretKey.type === 'password';
      captchaSecretKey.type = isPassword ? 'text' : 'password';
      const icon = captchaSecretToggle.querySelector('i');
      if (icon) icon.className = isPassword ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye';
    });
  }

  captchaModalSave.addEventListener('click', async () => {
    const siteKey = captchaSiteKey.value.trim();
    const secretKey = captchaSecretKey.value.trim();

    if (!siteKey) {
      showFeedback('Please enter your Site Key', true);
      captchaSiteKey.focus();
      return;
    }
    if (siteKey.length < 10) {
      showFeedback('Site Key appears to be invalid (too short)', true);
      captchaSiteKey.focus();
      return;
    }
    if (!secretKey) {
      showFeedback('Please enter your Secret Key', true);
      captchaSecretKey.focus();
      return;
    }
    if (secretKey.length < 10) {
      showFeedback('Secret Key appears to be invalid (too short)', true);
      captchaSecretKey.focus();
      return;
    }

    captchaModalSave.disabled = true;
    showFeedback('Saving...');

    try {
      const res = await fetch('/api/settings/captcha', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-action-token': securityActionTokens.updateCaptcha || ''
        },
        body: JSON.stringify({ site_key: siteKey, secret_key: secretKey, enabled: true })
      });
      const data = await res.json();

      if (!res.ok) {
        showFeedback(data.error || 'Failed to save', true);
      } else {
        captchaEnabled = true;
        updateUI();
        closeModal(false);
        await loadCaptchaStatus();
        if (data.note) {
          setTimeout(() => alert(data.note), 100);
        }
      }
    } catch (e) {
      showFeedback('Network error', true);
    } finally {
      captchaModalSave.disabled = false;
    }
  });

  const securityBtn = document.querySelector('button[data-panel="security"]');
  if (securityBtn) {
    securityBtn.addEventListener('click', loadCaptchaStatus);
  }

  const securityPanel = document.getElementById('security');
  if (securityPanel && securityPanel.classList.contains('active')) {
    loadCaptchaStatus();
  }
})();

(function () {
  const maintenanceCard = document.getElementById('maintenanceCard');
  if (!maintenanceCard) return;

  const maintenanceToggle = document.getElementById('maintenanceToggle');
  const maintenanceDetails = document.getElementById('maintenanceDetails');
  const maintenanceActiveBox = document.getElementById('maintenanceActiveBox');
  const maintenanceActiveText = document.getElementById('maintenanceActiveText');
  const maintenanceScheduledBox = document.getElementById('maintenanceScheduledBox');
  const maintenanceScheduledText = document.getElementById('maintenanceScheduledText');

  const maintenanceModalOverlay = document.getElementById('maintenanceModalOverlay');
  const maintenanceModal = document.getElementById('maintenanceModal');
  const maintenanceModalClose = document.getElementById('maintenanceModalClose');
  const maintenanceModalCancel = document.getElementById('maintenanceModalCancel');
  const maintenanceModalSave = document.getElementById('maintenanceModalSave');
  const maintenanceModalFeedback = document.getElementById('maintenanceModalFeedback');
  const maintenanceImmediate = document.getElementById('maintenanceImmediate');
  const maintenanceScheduledRadio = document.getElementById('maintenanceScheduled');
  const maintenanceScheduleGroup = document.getElementById('maintenanceScheduleGroup');
  const maintenanceScheduleInput = document.getElementById('maintenanceScheduleInput');
  const maintenanceReasonInput = document.getElementById('maintenanceReasonInput');
  const maintenancePreAlertToggle = document.getElementById('maintenancePreAlertToggle');
  const maintenancePreAlertGroup = document.getElementById('maintenancePreAlertGroup');
  const maintenancePreAlertMessage = document.getElementById('maintenancePreAlertMessage');
  const maintenancePreAlertMinutes = document.getElementById('maintenancePreAlertMinutes');

  const maintenanceSwitchLabel = maintenanceCard.querySelector('.switch');
  if (maintenanceSwitchLabel) {
    maintenanceSwitchLabel.addEventListener('click', (e) => e.stopPropagation());
  }

  let maintenanceActionTokens = {};
  let detailsOpen = false;
  let currentState = {};
  let pendingToggleState = null;

  function showFeedback(msg, isError = false) {
    if (!maintenanceModalFeedback) return;
    maintenanceModalFeedback.textContent = msg;
    maintenanceModalFeedback.style.color = isError ? '#ef4444' : '#22c55e';
    if (msg) {
      setTimeout(() => {
        if (maintenanceModalFeedback) maintenanceModalFeedback.textContent = '';
      }, 4000);
    }
  }

  function formatDateTime(isoString) {
    if (!isoString) return '';
    const d = new Date(isoString);
    return d.toLocaleString(undefined, {
      year: 'numeric', month: 'short', day: 'numeric',
      hour: '2-digit', minute: '2-digit'
    });
  }

  function updateUI() {
    const isActive = currentState.is_active || currentState.enabled;
    const isScheduled = !isActive && !!currentState.scheduled_at;

    maintenanceToggle.checked = isActive || isScheduled;

    if (isActive) {
      maintenanceActiveBox.style.display = 'flex';
      const enabledBy = currentState.enabled_by || 'admin';
      const enabledAt = currentState.enabled_at ? formatDateTime(currentState.enabled_at) : '';
      maintenanceActiveText.textContent = 'Maintenance mode is active' + (enabledAt ? ' — enabled ' + enabledAt : '') + (enabledBy ? ' by ' + enabledBy : '');
      maintenanceDetails.style.display = 'block';
      detailsOpen = true;
    } else {
      maintenanceActiveBox.style.display = 'none';
    }

    if (isScheduled) {
      maintenanceScheduledBox.style.display = 'flex';
      maintenanceScheduledText.textContent = 'Scheduled for ' + formatDateTime(currentState.scheduled_at);
      maintenanceDetails.style.display = 'block';
      detailsOpen = true;
    } else {
      maintenanceScheduledBox.style.display = 'none';
    }

    if (!isActive && !isScheduled) {
      maintenanceDetails.style.display = detailsOpen ? 'block' : 'none';
    }
  }

  function openModal() {
    if (!maintenanceModalOverlay || !maintenanceModal) return;
    maintenanceImmediate.checked = true;
    maintenanceScheduleGroup.style.display = 'none';
    maintenanceScheduleInput.value = '';
    maintenanceReasonInput.value = '';
    maintenancePreAlertToggle.checked = false;
    maintenancePreAlertGroup.style.display = 'none';
    maintenancePreAlertMessage.value = '';
    maintenancePreAlertMinutes.value = '30';
    maintenanceModalSave.innerHTML = '<i class="fa-solid fa-wrench"></i> Enable Maintenance';
    showFeedback('');

    const now = new Date();
    now.setMinutes(now.getMinutes() - now.getTimezoneOffset());
    maintenanceScheduleInput.min = now.toISOString().slice(0, 16);

    maintenanceModalOverlay.classList.add('show');
    maintenanceModal.classList.add('show');
  }

  function closeModal(revertToggle = false) {
    if (!maintenanceModalOverlay) return;
    maintenanceModalOverlay.classList.remove('show');
    if (maintenanceModal) maintenanceModal.classList.remove('show');
    if (revertToggle && pendingToggleState !== null) {
      maintenanceToggle.checked = !pendingToggleState;
    }
    pendingToggleState = null;
  }

  async function loadMaintenanceState() {
    try {
      const res = await fetch('/api/settings/maintenance');
      if (!res.ok) return;
      const data = await res.json();
      currentState = data;
      maintenanceActionTokens = data.actionTokens || {};
      updateUI();
    } catch (e) {
      console.error('Failed to load maintenance state:', e);
    }
  }

  maintenanceImmediate.addEventListener('change', () => {
    maintenanceScheduleGroup.style.display = 'none';
    maintenanceModalSave.innerHTML = '<i class="fa-solid fa-wrench"></i> Enable Maintenance';
  });
  maintenanceScheduledRadio.addEventListener('change', () => {
    maintenanceScheduleGroup.style.display = 'block';
    maintenanceModalSave.innerHTML = '<i class="fa-solid fa-calendar-days"></i> Schedule Maintenance';
  });

  maintenancePreAlertToggle.addEventListener('change', () => {
    maintenancePreAlertGroup.style.display = maintenancePreAlertToggle.checked ? 'block' : 'none';
  });

  maintenanceToggle.addEventListener('change', async () => {
    const newState = maintenanceToggle.checked;
    pendingToggleState = newState;

    if (newState) {
      openModal();
    } else {
      if (!confirm('Are you sure you want to disable maintenance mode? All users will regain access immediately.')) {
        maintenanceToggle.checked = true;
        pendingToggleState = null;
        return;
      }

      try {
        const res = await fetch('/api/settings/maintenance', {
          method: 'DELETE',
          headers: {
            'Content-Type': 'application/json',
            'x-action-token': maintenanceActionTokens.disableMaintenance || ''
          }
        });
        const data = await res.json();
        if (!res.ok) {
          alert(data.error || 'Failed to disable maintenance mode');
          maintenanceToggle.checked = true;
        } else {
          await loadMaintenanceState();
        }
      } catch (e) {
        alert('Network error');
        maintenanceToggle.checked = true;
      }
      pendingToggleState = null;
    }
  });

  maintenanceCard.addEventListener('click', (e) => {
    if (e.target.closest('.switch') || e.target.closest('input') || e.target.closest('button')) return;
    detailsOpen = !detailsOpen;
    if (maintenanceDetails) maintenanceDetails.style.display = detailsOpen ? 'block' : 'none';
  });

  maintenanceModalCancel.addEventListener('click', () => closeModal(true));
  if (maintenanceModalClose) maintenanceModalClose.addEventListener('click', () => closeModal(true));
  maintenanceModalOverlay.addEventListener('click', (e) => {
    if (e.target === maintenanceModalOverlay) closeModal(true);
  });
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && maintenanceModalOverlay.classList.contains('show')) {
      closeModal(true);
    }
  });

  maintenanceModalSave.addEventListener('click', async () => {
    const isImmediate = maintenanceImmediate.checked;
    const reason = maintenanceReasonInput.value.trim();
    const preAlertShow = maintenancePreAlertToggle.checked;
    const preAlertMessage = maintenancePreAlertMessage.value.trim();
    const preAlertMinutes = parseInt(maintenancePreAlertMinutes.value, 10);

    if (!isImmediate) {
      if (!maintenanceScheduleInput.value) {
        showFeedback('Please select a date and time for the scheduled maintenance', true);
        return;
      }
      const scheduledTime = new Date(maintenanceScheduleInput.value).getTime();
      if (isNaN(scheduledTime) || scheduledTime <= Date.now()) {
        showFeedback('Scheduled time must be in the future', true);
        return;
      }
    }

    if (preAlertShow) {
      if (!preAlertMessage) {
        showFeedback('Please enter an alert message', true);
        maintenancePreAlertMessage.focus();
        return;
      }
      if (!preAlertMinutes || preAlertMinutes < 1) {
        showFeedback('Please enter a valid number of minutes', true);
        maintenancePreAlertMinutes.focus();
        return;
      }
      if (!isImmediate) {
        const scheduledTime = new Date(maintenanceScheduleInput.value).getTime();
        const alertTime = scheduledTime - (preAlertMinutes * 60 * 1000);
        if (alertTime <= Date.now()) {
          showFeedback('The alert time (scheduled time minus minutes) is already in the past', true);
          return;
        }
      }
    }

    maintenanceModalSave.disabled = true;
    showFeedback('Enabling maintenance mode...');

    try {
      const body = {
        immediate: isImmediate,
        reason: reason || null,
        scheduled_at: !isImmediate ? new Date(maintenanceScheduleInput.value).toISOString() : null,
        pre_alert_show: preAlertShow,
        pre_alert_message: preAlertShow ? preAlertMessage : null,
        pre_alert_minutes_before: preAlertShow ? preAlertMinutes : null
      };

      const res = await fetch('/api/settings/maintenance', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-action-token': maintenanceActionTokens.enableMaintenance || ''
        },
        body: JSON.stringify(body)
      });
      const data = await res.json();

      if (!res.ok) {
        showFeedback(data.error || 'Failed to enable maintenance mode', true);
      } else {
        closeModal(false);
        await loadMaintenanceState();
      }
    } catch (e) {
      showFeedback('Network error', true);
    } finally {
      maintenanceModalSave.disabled = false;
    }
  });

  const securityBtn = document.querySelector('button[data-panel="security"]');
  if (securityBtn) {
    securityBtn.addEventListener('click', loadMaintenanceState);
  }

  const securityPanel = document.getElementById('security');
  if (securityPanel && securityPanel.classList.contains('active')) {
    loadMaintenanceState();
  }
})();

(function () {
  'use strict';

  const DEFAULT_ADMIN_ACTIONS = [
    { id: 'default-admin-1', title: 'Manage account', icon: 'fa-solid fa-list-check', link: '/settings?panel=account', animation: 'default', newTab: false },
    { id: 'default-admin-2', title: 'Manage servers', icon: 'fa-solid fa-bars-progress', link: '/settings?panel=servers', animation: 'default', newTab: false },
    { id: 'default-admin-3', title: 'ADPanel Site', icon: 'fa-solid fa-globe', link: 'https://ad-panel.com', animation: 'default', newTab: true },
  ];

  const DEFAULT_USER_ACTIONS = [
    { id: 'default-user-1', title: 'Manage account', icon: 'fa-solid fa-list-check', link: '/settings?panel=account', animation: 'default', newTab: false },
  ];

  const MAX_QUICK_ACTIONS = 6;
  let adminActions = [];
  let userActions = [];
  let editingActionId = null;
  let editingType = 'admin';

  const quickActionsList = document.getElementById('quickActionsList');
  const userQuickActionsList = document.getElementById('userQuickActionsList');
  const quickActionsEmpty = document.getElementById('quickActionsEmpty');
  const userQuickActionsEmpty = document.getElementById('userQuickActionsEmpty');
  const quickActionsCount = document.getElementById('quickActionsCount');
  const userQuickActionsCount = document.getElementById('userQuickActionsCount');
  const addQuickActionBtn = document.getElementById('addQuickActionBtn');
  const addUserQuickActionBtn = document.getElementById('addUserQuickActionBtn');

  const quickActionModalOverlay = document.getElementById('quickActionModalOverlay');
  const quickActionModal = document.getElementById('quickActionModal');
  const quickActionModalTitle = document.getElementById('quickActionModalTitle');
  const quickActionModalClose = document.getElementById('quickActionModalClose');
  const quickActionCancel = document.getElementById('quickActionCancel');
  const quickActionSave = document.getElementById('quickActionSave');
  const quickActionFeedback = document.getElementById('quickActionFeedback');

  const qaTitle = document.getElementById('qa_title');
  const qaLink = document.getElementById('qa_link');
  const qaIcon = document.getElementById('qa_icon');
  const qaAnimation = document.getElementById('qa_animation');
  const qaScale = document.getElementById('qa_scale');
  const qaScaleWrapper = document.getElementById('qa_scale_wrapper');
  const qaNewTab = document.getElementById('qa_newTab');

  const quickActionPreview = document.getElementById('quickActionPreview');
  const previewIcon = document.getElementById('previewIcon');
  const previewTitle = document.getElementById('previewTitle');
  const iconInputPreview = document.getElementById('iconInputPreview');
  const iconSuggestions = document.getElementById('iconSuggestions');

  if (!quickActionsList) return;

  loadQuickActions();

  if (addQuickActionBtn) {
    addQuickActionBtn.addEventListener('click', () => openQuickActionModal(null, 'admin'));
  }

  if (addUserQuickActionBtn) {
    addUserQuickActionBtn.addEventListener('click', () => openQuickActionModal(null, 'user'));
  }

  if (quickActionModalClose) {
    quickActionModalClose.addEventListener('click', closeQuickActionModal);
  }

  if (quickActionCancel) {
    quickActionCancel.addEventListener('click', closeQuickActionModal);
  }

  if (quickActionModalOverlay) {
    quickActionModalOverlay.addEventListener('click', (e) => {
      if (e.target === quickActionModalOverlay) closeQuickActionModal();
    });
  }

  if (quickActionSave) {
    quickActionSave.addEventListener('click', saveQuickAction);
  }

  if (qaTitle) {
    qaTitle.addEventListener('input', updatePreview);
  }

  if (qaIcon) {
    qaIcon.addEventListener('input', () => {
      updateIconPreview();
      updatePreview();
    });
  }

  if (qaAnimation) {
    qaAnimation.addEventListener('change', () => {
      updateScaleInputVisibility();
      updatePreviewAnimation();
    });
  }

  if (qaScale) {
    qaScale.addEventListener('input', updatePreviewAnimation);
  }

  function updateScaleInputVisibility() {
    if (qaAnimation.value === 'scale') {
      qaScaleWrapper.style.display = 'block';
    } else {
      qaScaleWrapper.style.display = 'none';
    }
  }

  if (iconSuggestions) {
    iconSuggestions.addEventListener('click', (e) => {
      const btn = e.target.closest('.icon-suggestion');
      if (btn && qaIcon) {
        const icon = btn.dataset.icon;
        qaIcon.value = icon;
        updateIconPreview();
        updatePreview();

        iconSuggestions.querySelectorAll('.icon-suggestion').forEach(b => b.classList.remove('selected'));
        btn.classList.add('selected');
      }
    });
  }

  const customizationBtn = document.querySelector('button[data-panel="customization"]');
  if (customizationBtn) {
    customizationBtn.addEventListener('click', loadQuickActions);
  }

  async function loadQuickActions() {
    try {
      const res = await fetch('/api/settings/quick-actions');
      if (res.ok) {
        const data = await res.json();
        if (data.actions && !Array.isArray(data.actions)) {
          adminActions = data.actions.admin || DEFAULT_ADMIN_ACTIONS;
          userActions = data.actions.user || DEFAULT_USER_ACTIONS;
        } else {
          adminActions = Array.isArray(data.actions) ? data.actions : DEFAULT_ADMIN_ACTIONS;
          userActions = DEFAULT_USER_ACTIONS;
        }
      } else {
        adminActions = DEFAULT_ADMIN_ACTIONS;
        userActions = DEFAULT_USER_ACTIONS;
      }
    } catch (err) {
      console.error('[quick-actions] Failed to load:', err);
      adminActions = DEFAULT_ADMIN_ACTIONS;
      userActions = DEFAULT_USER_ACTIONS;
    }
    renderQuickActions('admin');
    renderQuickActions('user');
  }

  function renderQuickActions(type = 'admin') {
    const isUser = type === 'user';
    const actions = isUser ? userActions : adminActions;
    const container = isUser ? userQuickActionsList : quickActionsList;
    const emptyMsg = isUser ? userQuickActionsEmpty : quickActionsEmpty;
    const countDisplay = isUser ? userQuickActionsCount : quickActionsCount;
    const addBtn = isUser ? addUserQuickActionBtn : addQuickActionBtn;

    if (!container) return;

    if (actions.length === 0) {
      container.innerHTML = '';
      if (emptyMsg) emptyMsg.style.display = 'flex';
      if (countDisplay) countDisplay.textContent = '0';
      return;
    }

    if (emptyMsg) emptyMsg.style.display = 'none';
    if (countDisplay) countDisplay.textContent = String(actions.length);

    container.innerHTML = actions.map((action, index) => `
      <div class="quick-action-item" data-id="${action.id || index}">
        <div class="quick-action-item-info">
          <div class="quick-action-item-icon">
            <i class="${escapeHtml(action.icon || 'fa-solid fa-star')}"></i>
          </div>
          <div class="quick-action-item-details">
            <div class="quick-action-item-title">${escapeHtml(action.title || 'Untitled')}</div>
            <div class="quick-action-item-link" title="${escapeHtml(action.link || '')}">${escapeHtml(truncateLink(action.link || ''))}</div>
          </div>
        </div>
        <div class="quick-action-item-actions">
          <button class="quick-action-item-btn edit" title="Edit" data-action="edit" data-id="${action.id || index}">
            <i class="fa-solid fa-pen"></i>
          </button>
          <button class="quick-action-item-btn delete" title="Delete" data-action="delete" data-id="${action.id || index}">
            <i class="fa-solid fa-trash"></i>
          </button>
        </div>
      </div>
    `).join('');

    replaceLegacyPanelEwsIcons(container);

    container.querySelectorAll('.quick-action-item-btn').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const actionType = btn.dataset.action;
        const id = btn.dataset.id;
        if (actionType === 'edit') {
          const action = actions.find(a => (a.id || actions.indexOf(a)) == id);
          if (action) openQuickActionModal(action, type);
        } else if (actionType === 'delete') {
          deleteQuickAction(id, type);
        }
      });
    });

    if (addBtn) {
      addBtn.disabled = actions.length >= MAX_QUICK_ACTIONS;
      if (actions.length >= MAX_QUICK_ACTIONS) {
        addBtn.title = 'Maximum of 6 quick actions reached';
      } else {
        addBtn.title = isUser ? 'Add user action' : 'Add admin action';
      }
    }
  }

  function openQuickActionModal(action = null, type = 'admin') {
    if (!quickActionModalOverlay) return;

    editingType = type;
    editingActionId = action ? (action.id || null) : null;

    if (qaTitle) qaTitle.value = action?.title || '';
    if (qaLink) qaLink.value = action?.link || '';
    if (qaIcon) qaIcon.value = action?.icon || 'fa-solid fa-star';
    if (qaAnimation) qaAnimation.value = action?.animation || 'default';
    if (qaScale) qaScale.value = action?.scaleValue || 1.05;
    if (qaNewTab) qaNewTab.checked = action?.newTab || false;

    updateScaleInputVisibility();

    if (quickActionModalTitle) {
      quickActionModalTitle.textContent = action ? 'Edit Quick Action' : 'Add Quick Action';
    }

    updateIconPreview();
    updatePreview();
    updatePreviewAnimation();

    if (quickActionFeedback) {
      quickActionFeedback.textContent = '';
      quickActionFeedback.className = 'node-feedback';
    }

    if (iconSuggestions) {
      iconSuggestions.querySelectorAll('.icon-suggestion').forEach(b => b.classList.remove('selected'));
      const currentIcon = qaIcon?.value || 'fa-solid fa-star';
      const matchBtn = iconSuggestions.querySelector(`[data-icon="${currentIcon}"]`);
      if (matchBtn) matchBtn.classList.add('selected');
    }

    quickActionModalOverlay.setAttribute('aria-hidden', 'false');
    quickActionModalOverlay.classList.add('show');

    setTimeout(() => qaTitle?.focus(), 100);
  }

  function closeQuickActionModal() {
    if (quickActionModalOverlay) {
      quickActionModalOverlay.setAttribute('aria-hidden', 'true');
      quickActionModalOverlay.classList.remove('show');
    }
    editingActionId = null;
  }

  function updateIconPreview() {
    const iconClass = qaIcon?.value || 'fa-solid fa-star';
    ensurePanelEwsSvgStyles();
    if (iconInputPreview) {
      if (isPanelEwsIcon(iconClass)) {
        iconInputPreview.innerHTML = '';
        iconInputPreview.appendChild(createPanelEwsSvg());
      } else {
        iconInputPreview.innerHTML = `<i class="${escapeHtml(iconClass)}"></i>`;
      }
    }
    if (quickActionPreview) {
      const existingSvg = quickActionPreview.querySelector('.panel-ews-svg');
      let iconEl = quickActionPreview.querySelector('#previewIcon');
      if (isPanelEwsIcon(iconClass)) {
        const svg = createPanelEwsSvg();
        if (iconEl) {
          iconEl.replaceWith(svg);
        } else if (existingSvg) {
          existingSvg.replaceWith(svg);
        } else {
          quickActionPreview.insertBefore(svg, quickActionPreview.firstChild);
        }
      } else {
        if (!iconEl) {
          iconEl = document.createElement('i');
          iconEl.id = 'previewIcon';
          if (existingSvg) {
            existingSvg.replaceWith(iconEl);
          } else {
            quickActionPreview.insertBefore(iconEl, quickActionPreview.firstChild);
          }
        }
        iconEl.className = iconClass;
      }
    }
  }

  function updatePreview() {
    const title = qaTitle?.value || 'Action Title';
    if (previewTitle) {
      previewTitle.textContent = title || 'Action Title';
    }
    updateIconPreview();
  }

  function updatePreviewAnimation() {
    const animation = qaAnimation?.value || 'default';
    if (quickActionPreview) {
      quickActionPreview.className = 'quick-action-preview-card';
      if (animation !== 'default' && animation !== 'none') {
        quickActionPreview.classList.add(`qa-anim-${animation}`);

        if (animation === 'scale') {
          quickActionPreview.style.setProperty('--hover-scale', qaScale?.value || 1.05);
        } else {
          quickActionPreview.style.removeProperty('--hover-scale');
        }
      } else {
        quickActionPreview.style.removeProperty('--hover-scale');
      }
    }
  }
  async function saveQuickAction() {
    const title = qaTitle?.value?.trim();
    const link = qaLink?.value?.trim();
    const icon = qaIcon?.value || 'fa-solid fa-star';
    const animation = qaAnimation?.value || 'default';
    const scaleValue = qaScale?.value || 1.05;
    const newTab = qaNewTab?.checked || false;

    if (!title) return showFeedback('Title is required', true);
    if (!link) return showFeedback('Link is required', true);

    if (!icon) {
      showFeedback('Icon is required', true);
      qaIcon?.focus();
      return;
    }

    if (!link.startsWith('/') && !link.startsWith('http://') && !link.startsWith('https://')) {
      showFeedback('Link must start with / for internal links or http(s):// for external', true);
      qaLink?.focus();
      return;
    }

    const currentActions = editingType === 'user' ? userActions : adminActions;
    if (!editingActionId && currentActions.length >= MAX_QUICK_ACTIONS) {
      showFeedback('Maximum of 6 quick actions reached', true);
      return;
    }

    const actionData = {
      id: editingActionId || `qa-${Date.now()}`,
      title,
      link,
      icon,
      animation,
      scaleValue,
      newTab
    };

    if (editingActionId) {
      const index = currentActions.findIndex(a => a.id === editingActionId);
      if (index !== -1) {
        currentActions[index] = actionData;
      }
    } else {
      currentActions.push(actionData);
    }

    if (quickActionSave) {
      quickActionSave.disabled = true;
      quickActionSave.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Saving...';
    }

    try {
      const res = await fetch('/api/settings/quick-actions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          admin: adminActions,
          user: userActions
        })
      });

      if (!res.ok) {
        const data = await res.json();
        showFeedback(data.error || 'Failed to save', true);
        return;
      }

      closeQuickActionModal();
      renderQuickActions(editingType);
    } catch (err) {
      console.error('[quick-actions] Save failed:', err);
      showFeedback('Network error', true);
    } finally {
      if (quickActionSave) {
        quickActionSave.disabled = false;
        quickActionSave.innerHTML = '<i class="fa-solid fa-check"></i> Save Action';
      }
    }
  }

  async function deleteQuickAction(id, type) {
    const isUser = type === 'user';
    const actions = isUser ? userActions : adminActions;
    const action = actions.find(a => (a.id || actions.indexOf(a)) == id);
    if (!action) return;

    const confirmDelete = confirm(`Delete quick action "${action.title}"?`);
    if (!confirmDelete) return;

    const index = actions.findIndex(a => (a.id || actions.indexOf(a)) == id);
    if (index !== -1) {
      actions.splice(index, 1);
    }

    try {
      const res = await fetch('/api/settings/quick-actions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          admin: adminActions,
          user: userActions
        })
      });

      if (!res.ok) {
        console.error('[quick-actions] Delete save failed');
        loadQuickActions();
        return;
      }

      renderQuickActions(type);
    } catch (err) {
      console.error('[quick-actions] Delete failed:', err);
      loadQuickActions();
    }
  }

  function showFeedback(msg, isError = false) {
    if (quickActionFeedback) {
      quickActionFeedback.textContent = msg;
      quickActionFeedback.className = 'node-feedback' + (isError ? ' error' : ' success');
    }
  }

  function escapeHtml(str) {
    if (!str) return '';
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  function truncateLink(link) {
    if (!link) return '';
    if (link.length <= 25) return link;
    return link.substring(0, 22) + '...';
  }
})();

(function () {
  'use strict';

  const phpmyadminToggle = document.getElementById('phpmyadminToggle');
  const phpmyadminDetails = document.getElementById('phpmyadminDetails');
  const phpmyadminStatus = document.getElementById('phpmyadminStatus');
  const phpmyadminCard = document.getElementById('phpmyadminCard');
  const dbUsersSection = document.getElementById('dbUsersSection');
  const dbUsersList = document.getElementById('dbUsersList');
  const dbTypeSection = document.getElementById('dbTypeSection');
  const dbTypeCurrent = document.getElementById('dbTypeCurrent');
  const phpmyadminAccessSection = document.getElementById('phpmyadminAccessSection');
  const phpmyadminUrl = document.getElementById('phpmyadminUrl');
  const copyPhpmyadminUrl = document.getElementById('copyPhpmyadminUrl');
  const openPhpmyadminUrl = document.getElementById('openPhpmyadminUrl');
  const addDbUserBtn = document.getElementById('addDbUserBtn');
  const changeDbTypeBtn = document.getElementById('changeDbTypeBtn');

  const dbSetupModalOverlay = document.getElementById('dbSetupModalOverlay');
  const dbSetupModal = document.getElementById('dbSetupModal');
  const dbSetupModalClose = document.getElementById('dbSetupModalClose');
  const dbSetupCancel = document.getElementById('dbSetupCancel');
  const dbSetupSave = document.getElementById('dbSetupSave');
  const dbSetupFeedback = document.getElementById('dbSetupFeedback');
  const dbHost = document.getElementById('db_host');
  const dbPort = document.getElementById('db_port');
  const dbUsername = document.getElementById('db_username');
  const dbPassword = document.getElementById('db_password');

  const dbUserModalOverlay = document.getElementById('dbUserModalOverlay');
  const dbUserModal = document.getElementById('dbUserModal');
  const dbUserModalTitle = document.getElementById('dbUserModalTitle');
  const dbUserModalClose = document.getElementById('dbUserModalClose');
  const dbUserCancel = document.getElementById('dbUserCancel');
  const dbUserSave = document.getElementById('dbUserSave');
  const dbUserFeedback = document.getElementById('dbUserFeedback');
  const dbuserUsername = document.getElementById('dbuser_username');
  const dbuserPassword = document.getElementById('dbuser_password');
  const dbUserPasswordStrength = document.getElementById('dbUserPasswordStrength');

  const dbPassModalOverlay = document.getElementById('dbPassModalOverlay');
  const dbPassModal = document.getElementById('dbPassModal');
  const dbPassModalSubtitle = document.getElementById('dbPassModalSubtitle');
  const dbPassModalClose = document.getElementById('dbPassModalClose');
  const dbPassCancel = document.getElementById('dbPassCancel');
  const dbPassSave = document.getElementById('dbPassSave');
  const dbPassFeedback = document.getElementById('dbPassFeedback');
  const dbpassUsername = document.getElementById('dbpass_username');
  const dbpassPassword = document.getElementById('dbpass_password');
  const dbpassConfirm = document.getElementById('dbpass_confirm');
  const dbPassPasswordStrength = document.getElementById('dbPassPasswordStrength');

  const dbChangeTypeModalOverlay = document.getElementById('dbChangeTypeModalOverlay');
  const dbChangeTypeModal = document.getElementById('dbChangeTypeModal');
  const dbChangeTypeModalClose = document.getElementById('dbChangeTypeModalClose');
  const dbChangeTypeCancel = document.getElementById('dbChangeTypeCancel');
  const dbChangeTypeSave = document.getElementById('dbChangeTypeSave');
  const dbChangeTypeFeedback = document.getElementById('dbChangeTypeFeedback');
  const dbChangeHost = document.getElementById('db_change_host');
  const dbChangePort = document.getElementById('db_change_port');
  const dbChangeUsername = document.getElementById('db_change_username');
  const dbChangePassword = document.getElementById('db_change_password');

  let dbConfig = null;
  let dbUsers = [];
  let phpmyadminEnabled = false;
  let dbActionTokens = {};
  window._dbActionTokens = dbActionTokens;

  function escapeHtmlDb(str) {
    if (!str) return '';
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  function checkPasswordStrength(password) {
    if (!password) return '';
    let strength = 'weak';
    if (password.length >= 12 && /[A-Z]/.test(password) && /[0-9]/.test(password) && /[^A-Za-z0-9]/.test(password)) {
      strength = 'strong';
    } else if (password.length >= 10 && /[A-Z]/.test(password) && /[0-9]/.test(password)) {
      strength = 'good';
    } else if (password.length >= 8) {
      strength = 'fair';
    }
    return `<div class="password-strength-bar ${strength}"></div>`;
  }

  function showDbFeedback(el, msg, isError = false) {
    if (!el) return;
    el.textContent = msg;
    el.className = 'node-feedback' + (isError ? ' error' : ' success');
    el.style.display = msg ? 'block' : 'none';
  }

  function clearDbFeedback(el) {
    if (el) {
      el.textContent = '';
      el.style.display = 'none';
    }
  }

  function openModal(overlay, modal) {
    if (overlay) overlay.classList.add('show');
    if (modal) modal.classList.add('show');
  }

  function closeModal(overlay, modal) {
    if (overlay) overlay.classList.remove('show');
    if (modal) modal.classList.remove('show');
  }

  function getDefaultPort(dbType) {
    switch (dbType) {
      case 'mysql': return 3306;
      case 'postgresql': return 5432;
      case 'mongodb': return 27017;
      default: return 3306;
    }
  }

  function getDbTypeIcon(dbType) {
    switch (dbType) {
      case 'mysql': return 'fa-solid fa-database';
      case 'postgresql': return 'fa-solid fa-database';
      case 'mongodb': return 'fa-solid fa-leaf';
      default: return 'fa-solid fa-database';
    }
  }

  async function loadDbConfig() {
    try {
      const res = await fetch('/api/settings/database');
      if (!res.ok) {
        console.warn('[database] Failed to load config');
        return;
      }
      const data = await res.json();
      dbConfig = data.config || null;
      dbUsers = data.users || [];
      phpmyadminEnabled = !!data.enabled;
      dbActionTokens = data.actionTokens || {};
      window._dbActionTokens = dbActionTokens;
      updateDbUI();
    } catch (err) {
      console.error('[database] Load config error:', err);
    }
  }

  function updateDbUI() {
    if (!phpmyadminToggle) return;

    phpmyadminToggle.checked = phpmyadminEnabled;

    if (phpmyadminEnabled && dbConfig) {
      if (phpmyadminDetails) phpmyadminDetails.style.display = 'block';

      if (phpmyadminStatus) {
        phpmyadminStatus.innerHTML = `
          <div class="db-status-success">
            <i class="fa-solid fa-check-circle"></i>
            <span>phpMyAdmin is active and running</span>
          </div>
        `;
      }

      if (dbUsersSection) dbUsersSection.style.display = 'block';
      renderDbUsers();

      if (dbTypeSection) dbTypeSection.style.display = 'block';
      updateDbTypeDisplay();

      if (phpmyadminAccessSection) {
        phpmyadminAccessSection.style.display = 'block';
        if (phpmyadminUrl) phpmyadminUrl.textContent = 'Secure token access (stays active while this tab stays open)';
      }
    } else {
      if (phpmyadminDetails) phpmyadminDetails.style.display = 'none';
      if (dbUsersSection) dbUsersSection.style.display = 'none';
      if (dbTypeSection) dbTypeSection.style.display = 'none';
      if (phpmyadminAccessSection) phpmyadminAccessSection.style.display = 'none';
    }
  }

  function updateDbTypeDisplay() {
    if (!dbTypeCurrent || !dbConfig) return;

    const icon = getDbTypeIcon(dbConfig.type);
    const typeName = (dbConfig.type || 'mysql').charAt(0).toUpperCase() + (dbConfig.type || 'mysql').slice(1);

    dbTypeCurrent.innerHTML = `
      <div class="db-type-icon ${dbConfig.type || 'mysql'}"><i class="${icon}"></i></div>
      <div class="db-type-info">
        <div class="db-type-name">${escapeHtmlDb(typeName)}</div>
        <div class="db-type-host">${escapeHtmlDb(dbConfig.host || 'localhost')}:${dbConfig.port || getDefaultPort(dbConfig.type)}</div>
      </div>
    `;
  }

  function renderDbUsers() {
    if (!dbUsersList) return;

    if (!dbUsers || dbUsers.length === 0) {
      dbUsersList.innerHTML = `
        <div class="db-users-empty">
          <i class="fa-solid fa-user-slash"></i>
          <p>No database users configured</p>
        </div>
      `;
      return;
    }

    dbUsersList.innerHTML = '';
    dbUsers.forEach(user => {
      const row = document.createElement('div');
      row.className = 'db-user-row';
      row.innerHTML = `
        <div class="db-user-left">
          <div class="db-user-icon"><i class="fa-solid fa-user"></i></div>
          <div>
            <div class="db-user-name">${escapeHtmlDb(user.username)}</div>
            <div class="db-user-created">${user.createdAt ? 'Created ' + new Date(user.createdAt).toLocaleDateString() : ''}</div>
          </div>
        </div>
        <div class="db-user-actions">
          <button class="db-user-btn db-user-btn-edit" data-username="${escapeHtmlDb(user.username)}" title="Change password">
            <i class="fa-solid fa-key"></i> Password
          </button>
          <button class="db-user-btn db-user-btn-delete" data-username="${escapeHtmlDb(user.username)}" title="Delete user">
            <i class="fa-solid fa-trash"></i>
          </button>
        </div>
      `;
      dbUsersList.appendChild(row);
    });
  }

  function openSetupModal() {
    if (!dbSetupModalOverlay || !dbSetupModal) return;

    if (dbHost) dbHost.value = 'localhost';
    if (dbPort) dbPort.value = '3306';
    if (dbUsername) dbUsername.value = '';
    if (dbPassword) dbPassword.value = '';

    const mysqlRadio = document.querySelector('input[name="db_type"][value="mysql"]');
    if (mysqlRadio) mysqlRadio.checked = true;

    clearDbFeedback(dbSetupFeedback);
    openModal(dbSetupModalOverlay, dbSetupModal);
  }

  function closeSetupModal() {
    closeModal(dbSetupModalOverlay, dbSetupModal);
  }

  async function saveDbSetup() {
    const dbType = document.querySelector('input[name="db_type"]:checked')?.value || 'mysql';
    const host = dbHost?.value?.trim() || 'localhost';
    const port = parseInt(dbPort?.value || getDefaultPort(dbType), 10);
    const username = dbUsername?.value?.trim();
    const password = dbPassword?.value;

    if (!username) {
      showDbFeedback(dbSetupFeedback, 'Username is required', true);
      return;
    }

    if (!password) {
      showDbFeedback(dbSetupFeedback, 'Password is required', true);
      return;
    }

    if (dbSetupSave) {
      dbSetupSave.disabled = true;
      dbSetupSave.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Setting up...';
    }

    try {
      const res = await fetch('/api/settings/database/setup', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-action-token': dbActionTokens.setup || '' },
        body: JSON.stringify({
          type: dbType,
          host,
          port,
          username,
          password
        })
      });

      const data = await res.json().catch(() => ({ error: 'Invalid response' }));

      if (!res.ok) {
        showDbFeedback(dbSetupFeedback, data.error || 'Setup failed', true);
        return;
      }

      showDbFeedback(dbSetupFeedback, 'phpMyAdmin setup complete!', false);
      phpmyadminEnabled = true;
      await loadDbConfig();
      if (phpmyadminToggle) phpmyadminToggle.checked = true;
      setTimeout(() => {
        closeSetupModal();
      }, 800);

    } catch (err) {
      console.error('[database] Setup error:', err);
      showDbFeedback(dbSetupFeedback, 'Network error', true);
    } finally {
      if (dbSetupSave) {
        dbSetupSave.disabled = false;
        dbSetupSave.innerHTML = '<i class="fa-solid fa-check"></i> Setup & Enable';
      }
    }
  }

  function openUserModal() {
    if (!dbUserModalOverlay || !dbUserModal) return;

    if (dbuserUsername) dbuserUsername.value = '';
    if (dbuserPassword) dbuserPassword.value = '';
    if (dbUserPasswordStrength) dbUserPasswordStrength.innerHTML = '';

    clearDbFeedback(dbUserFeedback);
    openModal(dbUserModalOverlay, dbUserModal);
  }

  function closeUserModal() {
    closeModal(dbUserModalOverlay, dbUserModal);
  }

  async function saveDbUser() {
    const username = dbuserUsername?.value?.trim();
    const password = dbuserPassword?.value;

    if (!username) {
      showDbFeedback(dbUserFeedback, 'Username is required', true);
      return;
    }

    if (!password || password.length < 8) {
      showDbFeedback(dbUserFeedback, 'Password must be at least 8 characters', true);
      return;
    }

    if (dbUserSave) {
      dbUserSave.disabled = true;
      dbUserSave.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Creating...';
    }

    try {
      const res = await fetch('/api/settings/database/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-action-token': dbActionTokens.createUser || '' },
        body: JSON.stringify({ username, password })
      });

      const data = await res.json().catch(() => ({ error: 'Invalid response' }));

      if (!res.ok) {
        showDbFeedback(dbUserFeedback, data.error || 'Failed to create user', true);
        return;
      }

      showDbFeedback(dbUserFeedback, 'User created successfully!', false);

      await loadDbConfig();

      setTimeout(() => {
        closeUserModal();
      }, 600);

    } catch (err) {
      console.error('[database] Create user error:', err);
      showDbFeedback(dbUserFeedback, 'Network error', true);
    } finally {
      if (dbUserSave) {
        dbUserSave.disabled = false;
        dbUserSave.innerHTML = '<i class="fa-solid fa-check"></i> Create User';
      }
    }
  }

  function openPasswordModal(username) {
    if (!dbPassModalOverlay || !dbPassModal) return;

    if (dbpassUsername) dbpassUsername.value = username;
    if (dbPassModalSubtitle) dbPassModalSubtitle.textContent = `Update password for ${username}`;
    if (dbpassPassword) dbpassPassword.value = '';
    if (dbpassConfirm) dbpassConfirm.value = '';
    if (dbPassPasswordStrength) dbPassPasswordStrength.innerHTML = '';

    clearDbFeedback(dbPassFeedback);
    openModal(dbPassModalOverlay, dbPassModal);
  }

  function closePasswordModal() {
    closeModal(dbPassModalOverlay, dbPassModal);
  }

  async function saveDbPassword() {
    const username = dbpassUsername?.value;
    const password = dbpassPassword?.value;
    const confirm = dbpassConfirm?.value;

    if (!password || password.length < 8) {
      showDbFeedback(dbPassFeedback, 'Password must be at least 8 characters', true);
      return;
    }

    if (password !== confirm) {
      showDbFeedback(dbPassFeedback, 'Passwords do not match', true);
      return;
    }

    if (dbPassSave) {
      dbPassSave.disabled = true;
      dbPassSave.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Updating...';
    }

    try {
      const res = await fetch(`/api/settings/database/users/${encodeURIComponent(username)}/password`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-action-token': dbActionTokens[`changePassword_${username}`] || '' },
        body: JSON.stringify({ password })
      });

      const data = await res.json().catch(() => ({ error: 'Invalid response' }));

      if (!res.ok) {
        showDbFeedback(dbPassFeedback, data.error || 'Failed to update password', true);
        return;
      }

      showDbFeedback(dbPassFeedback, 'Password updated successfully!', false);

      setTimeout(() => {
        closePasswordModal();
      }, 600);

    } catch (err) {
      console.error('[database] Update password error:', err);
      showDbFeedback(dbPassFeedback, 'Network error', true);
    } finally {
      if (dbPassSave) {
        dbPassSave.disabled = false;
        dbPassSave.innerHTML = '<i class="fa-solid fa-check"></i> Update Password';
      }
    }
  }

  async function deleteDbUser(username) {
    if (!confirm(`Delete database user "${username}"? This cannot be undone.`)) return;

    try {
      const res = await fetch(`/api/settings/database/users/${encodeURIComponent(username)}`, {
        method: 'DELETE',
        headers: { 'x-action-token': dbActionTokens[`deleteUser_${username}`] || '' }
      });

      const data = await res.json().catch(() => ({ error: 'Invalid response' }));

      if (!res.ok) {
        alert(data.error || 'Failed to delete user');
        return;
      }

      await loadDbConfig();

    } catch (err) {
      console.error('[database] Delete user error:', err);
      alert('Network error');
    }
  }

  function openChangeTypeModal() {
    if (!dbChangeTypeModalOverlay || !dbChangeTypeModal) return;

    if (dbChangeHost) dbChangeHost.value = 'localhost';
    if (dbChangePort) dbChangePort.value = '';
    if (dbChangeUsername) dbChangeUsername.value = '';
    if (dbChangePassword) dbChangePassword.value = '';

    document.querySelectorAll('input[name="db_change_type"]').forEach(r => r.checked = false);

    clearDbFeedback(dbChangeTypeFeedback);
    openModal(dbChangeTypeModalOverlay, dbChangeTypeModal);
  }

  function closeChangeTypeModal() {
    closeModal(dbChangeTypeModalOverlay, dbChangeTypeModal);
  }

  async function saveDbTypeChange() {
    const dbType = document.querySelector('input[name="db_change_type"]:checked')?.value;
    const host = dbChangeHost?.value?.trim() || 'localhost';
    const port = parseInt(dbChangePort?.value || getDefaultPort(dbType), 10);
    const username = dbChangeUsername?.value?.trim();
    const password = dbChangePassword?.value;

    if (!dbType) {
      showDbFeedback(dbChangeTypeFeedback, 'Please select a database type', true);
      return;
    }

    if (!username) {
      showDbFeedback(dbChangeTypeFeedback, 'Username is required', true);
      return;
    }

    if (!password) {
      showDbFeedback(dbChangeTypeFeedback, 'Password is required', true);
      return;
    }

    if (!confirm('This will remove all existing database users and settings. Continue?')) {
      return;
    }

    if (dbChangeTypeSave) {
      dbChangeTypeSave.disabled = true;
      dbChangeTypeSave.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Changing...';
    }

    try {
      const res = await fetch('/api/settings/database/change-type', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-action-token': dbActionTokens.changeType || '' },
        body: JSON.stringify({
          type: dbType,
          host,
          port,
          username,
          password
        })
      });

      const data = await res.json().catch(() => ({ error: 'Invalid response' }));

      if (!res.ok) {
        showDbFeedback(dbChangeTypeFeedback, data.error || 'Failed to change database type', true);
        return;
      }

      showDbFeedback(dbChangeTypeFeedback, 'Database type changed successfully!', false);

      await loadDbConfig();

      setTimeout(() => {
        closeChangeTypeModal();
      }, 800);

    } catch (err) {
      console.error('[database] Change type error:', err);
      showDbFeedback(dbChangeTypeFeedback, 'Network error', true);
    } finally {
      if (dbChangeTypeSave) {
        dbChangeTypeSave.disabled = false;
        dbChangeTypeSave.innerHTML = '<i class="fa-solid fa-rotate"></i> Change Database';
      }
    }
  }

  async function togglePhpmyadmin(enabled) {
    if (enabled && !phpmyadminEnabled) {
      if (phpmyadminToggle) phpmyadminToggle.checked = false;
      openSetupModal();
      return;
    }

    if (!enabled && phpmyadminEnabled) {
      if (!confirm('Disable phpMyAdmin? This will remove all database users and settings.')) {
        if (phpmyadminToggle) phpmyadminToggle.checked = true;
        return;
      }

      const statusEl = document.getElementById('phpmyadminStatus');
      const detailsEl = document.getElementById('phpmyadminDetails');
      const accessSection = document.getElementById('phpmyadminAccessSection');
      const usersSection = document.getElementById('dbUsersSection');
      if (detailsEl) detailsEl.style.display = 'block';
      if (accessSection) accessSection.style.display = 'none';
      if (usersSection) usersSection.style.display = 'none';
      const progress = statusEl ? createDbDisableProgress(statusEl, [
        'Disabling web access',
        'Removing configuration',
        'Saving settings'
      ], 10, '#f89d21') : null;

      if (phpmyadminToggle) phpmyadminToggle.disabled = true;

      try {
        const res = await fetch('/api/settings/database/disable', {
          method: 'POST',
          headers: { 'x-action-token': dbActionTokens.disable || '' }
        });

        const data = await res.json().catch(() => ({ error: 'Invalid response' }));

        if (!res.ok) {
          if (progress) progress.finish(false, data.error || 'Failed to disable phpMyAdmin');
          if (phpmyadminToggle) { phpmyadminToggle.checked = true; phpmyadminToggle.disabled = false; }
          return;
        }

        if (progress) progress.finish(true, 'phpMyAdmin disabled successfully — refresh to update');

        phpmyadminEnabled = false;
        dbConfig = null;
        dbUsers = [];
        if (phpmyadminToggle) { phpmyadminToggle.checked = false; phpmyadminToggle.disabled = false; }

      } catch (err) {
        console.error('[database] Disable error:', err);
        if (progress) progress.finish(false, 'Network error — please try again');
        if (phpmyadminToggle) { phpmyadminToggle.checked = true; phpmyadminToggle.disabled = false; }
      }
    }
  }

  if (phpmyadminToggle) {
    phpmyadminToggle.addEventListener('change', (e) => {
      togglePhpmyadmin(e.target.checked);
    });
  }

  if (dbSetupModalClose) dbSetupModalClose.addEventListener('click', closeSetupModal);
  if (dbSetupCancel) dbSetupCancel.addEventListener('click', closeSetupModal);
  if (dbSetupModalOverlay) {
    dbSetupModalOverlay.addEventListener('click', (e) => {
      if (e.target === dbSetupModalOverlay) closeSetupModal();
    });
  }
  if (dbSetupSave) dbSetupSave.addEventListener('click', saveDbSetup);

  document.querySelectorAll('input[name="db_type"]').forEach(radio => {
    radio.addEventListener('change', (e) => {
      if (dbPort) dbPort.value = getDefaultPort(e.target.value);
    });
  });

  document.querySelectorAll('input[name="db_change_type"]').forEach(radio => {
    radio.addEventListener('change', (e) => {
      if (dbChangePort) dbChangePort.value = getDefaultPort(e.target.value);
    });
  });

  if (addDbUserBtn) addDbUserBtn.addEventListener('click', openUserModal);
  if (dbUserModalClose) dbUserModalClose.addEventListener('click', closeUserModal);
  if (dbUserCancel) dbUserCancel.addEventListener('click', closeUserModal);
  if (dbUserModalOverlay) {
    dbUserModalOverlay.addEventListener('click', (e) => {
      if (e.target === dbUserModalOverlay) closeUserModal();
    });
  }
  if (dbUserSave) dbUserSave.addEventListener('click', saveDbUser);

  if (dbuserPassword) {
    dbuserPassword.addEventListener('input', (e) => {
      if (dbUserPasswordStrength) {
        dbUserPasswordStrength.innerHTML = checkPasswordStrength(e.target.value);
      }
    });
  }

  if (dbPassModalClose) dbPassModalClose.addEventListener('click', closePasswordModal);
  if (dbPassCancel) dbPassCancel.addEventListener('click', closePasswordModal);
  if (dbPassModalOverlay) {
    dbPassModalOverlay.addEventListener('click', (e) => {
      if (e.target === dbPassModalOverlay) closePasswordModal();
    });
  }
  if (dbPassSave) dbPassSave.addEventListener('click', saveDbPassword);

  if (dbpassPassword) {
    dbpassPassword.addEventListener('input', (e) => {
      if (dbPassPasswordStrength) {
        dbPassPasswordStrength.innerHTML = checkPasswordStrength(e.target.value);
      }
    });
  }

  if (changeDbTypeBtn) changeDbTypeBtn.addEventListener('click', openChangeTypeModal);
  if (dbChangeTypeModalClose) dbChangeTypeModalClose.addEventListener('click', closeChangeTypeModal);
  if (dbChangeTypeCancel) dbChangeTypeCancel.addEventListener('click', closeChangeTypeModal);
  if (dbChangeTypeModalOverlay) {
    dbChangeTypeModalOverlay.addEventListener('click', (e) => {
      if (e.target === dbChangeTypeModalOverlay) closeChangeTypeModal();
    });
  }
  if (dbChangeTypeSave) dbChangeTypeSave.addEventListener('click', saveDbTypeChange);

  if (dbUsersList) {
    dbUsersList.addEventListener('click', (e) => {
      const editBtn = e.target.closest('.db-user-btn-edit');
      if (editBtn) {
        const username = editBtn.dataset.username;
        if (username) openPasswordModal(username);
        return;
      }

      const deleteBtn = e.target.closest('.db-user-btn-delete');
      if (deleteBtn) {
        const username = deleteBtn.dataset.username;
        if (username) deleteDbUser(username);
        return;
      }
    });
  }

  // Helper: request a secure access token for database tools
  // Track open db tool tabs for token lifecycle
  const _dbOpenTabs = [];

  async function requestDbAccessToken(service) {
    try {
      const res = await fetch('/api/settings/database/access-token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ service })
      });
      if (!res.ok) throw new Error('Failed to get access token');
      return await res.json();
    } catch (err) {
      console.error('[db-proxy] Token request failed:', err);
      return null;
    }
  }

  function revokeDbToken(token) {
    try {
      const blob = new Blob([JSON.stringify({ token })], { type: 'application/json' });
      navigator.sendBeacon('/api/settings/database/revoke-token', blob);
    } catch {
      fetch('/api/settings/database/revoke-token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token }),
        keepalive: true
      }).catch(() => {});
    }
  }

  function trackDbTab(win, token) {
    const entry = { win, token, interval: null };
    entry.interval = setInterval(() => {
      if (win.closed) {
        clearInterval(entry.interval);
        revokeDbToken(token);
        const idx = _dbOpenTabs.indexOf(entry);
        if (idx !== -1) _dbOpenTabs.splice(idx, 1);
      }
    }, 2000);
    _dbOpenTabs.push(entry);
  }

  async function copyDbAccessUrl(service, buttonEl) {
    const tokenData = await requestDbAccessToken(service);
    if (!tokenData || !tokenData.url) return;
    const fullUrl = window.location.origin + tokenData.url;
    copyToClipboard(fullUrl).then(() => {
      if (!buttonEl) return;
      buttonEl.innerHTML = '<i class="fa-solid fa-check"></i>';
      setTimeout(() => {
        buttonEl.innerHTML = '<i class="fa-solid fa-copy"></i>';
      }, 1500);
    });
  }

  async function openDbAccessWindow(service, buttonEl) {
    if (!buttonEl) return;

    const originalHtml = buttonEl.innerHTML;
    buttonEl.disabled = true;
    buttonEl.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Opening...';

    try {
      const tokenData = await requestDbAccessToken(service);
      if (tokenData && tokenData.url) {
        const w = window.open(tokenData.url, '_blank');
        if (w && tokenData.token) trackDbTab(w, tokenData.token);
      } else {
        alert('Failed to generate secure access token. Please try again.');
      }
    } finally {
      buttonEl.disabled = false;
      buttonEl.innerHTML = originalHtml;
    }
  }

  // Tokens stay alive while the db-access page remains open.
  // The opened tab sends heartbeats, and the parent tab still revokes quickly on close when possible.

  window._requestDbAccessToken = requestDbAccessToken;
  window._trackDbAccessTab = trackDbTab;
  window._copyDbAccessUrl = copyDbAccessUrl;
  window._openDbAccessWindow = openDbAccessWindow;

  if (copyPhpmyadminUrl) {
    copyPhpmyadminUrl.addEventListener('click', () => copyDbAccessUrl('phpmyadmin', copyPhpmyadminUrl));
  }

  if (openPhpmyadminUrl) {
    openPhpmyadminUrl.addEventListener('click', () => openDbAccessWindow('phpmyadmin', openPhpmyadminUrl));
  }

  window.setupPasswordToggle = function (toggleId, inputId) {
    const toggle = document.getElementById(toggleId);
    const input = document.getElementById(inputId);
    if (toggle && input && !toggle.dataset.listenerAdded) {
      toggle.dataset.listenerAdded = 'true';
      toggle.addEventListener('click', (e) => {
        e.preventDefault();
        e.stopPropagation();
        const icon = toggle.querySelector('i');
        if (input.type === 'password') {
          input.type = 'text';
          if (icon) {
            icon.classList.remove('fa-eye');
            icon.classList.add('fa-eye-slash');
          }
          toggle.setAttribute('aria-label', 'Hide password');
        } else {
          input.type = 'password';
          if (icon) {
            icon.classList.remove('fa-eye-slash');
            icon.classList.add('fa-eye');
          }
          toggle.setAttribute('aria-label', 'Show password');
        }
      });
    }
  }

  setupPasswordToggle('pgadmin_password_toggle', 'pgadmin_password');
  setupPasswordToggle('mongodb_password_toggle', 'mongodb_password');

  const observer = new MutationObserver(() => {
    setupPasswordToggle('pgadmin_password_toggle', 'pgadmin_password');
    setupPasswordToggle('mongodb_password_toggle', 'mongodb_password');
  });
  observer.observe(document.body, { childList: true, subtree: true });

  const sidebarNav = document.getElementById('sidebarNav');
  if (sidebarNav) {
    sidebarNav.addEventListener('click', (e) => {
      const btn = e.target.closest('button');
      if (btn && btn.dataset.panel === 'databases') {
        loadDbConfig();
        loadPgAdminStatus();
        loadMongoDBStatus();
      }
    });
  }

  const databasesPanel = document.getElementById('databases');
  if (databasesPanel && databasesPanel.classList.contains('active')) {
    loadDbConfig();
    loadPgAdminStatus();
    loadMongoDBStatus();
  }
})();

var _dbDisableGuard = null;
function _dbBeforeUnload(e) { e.preventDefault(); e.returnValue = ''; }
function _dbDisableGuardOn() {
  if (!_dbDisableGuard) {
    _dbDisableGuard = true;
    window.addEventListener('beforeunload', _dbBeforeUnload);
  }
}
function _dbDisableGuardOff() {
  if (_dbDisableGuard) {
    _dbDisableGuard = null;
    window.removeEventListener('beforeunload', _dbBeforeUnload);
  }
}

function createDbDisableProgress(statusEl, steps, estimatedSecs, brandColor) {
  const color = brandColor || '#f59e0b';
  const stepsHtml = steps.map((s, i) =>
    `<div class="db-disable-step" data-step="${i}"><i class="fa-solid fa-circle"></i><span>${s}</span></div>`
  ).join('');

  statusEl.innerHTML = `
    <div class="db-disable-progress" style="--db-brand:${color}">
      <div class="db-disable-header">
        <i class="fa-solid fa-circle-notch"></i>
        <div class="db-disable-header-text">
          <div class="db-disable-title">Disabling...</div>
          <div class="db-disable-subtitle">Estimated time: ~${estimatedSecs}s</div>
        </div>
      </div>
      <div class="db-disable-bar-track"><div class="db-disable-bar-fill"></div></div>
      <div class="db-disable-steps">${stepsHtml}</div>
      <div class="db-disable-elapsed">Elapsed: 0s</div>
    </div>
  `;

  _dbDisableGuardOn();

  const barFill = statusEl.querySelector('.db-disable-bar-fill');
  const elapsedEl = statusEl.querySelector('.db-disable-elapsed');
  const stepEls = statusEl.querySelectorAll('.db-disable-step');
  let elapsed = 0;
  let currentStep = 0;

  if (stepEls[0]) stepEls[0].classList.add('active');

  const stepInterval = Math.floor(estimatedSecs / steps.length);
  const timer = setInterval(() => {
    elapsed++;
    if (elapsedEl) elapsedEl.textContent = `Elapsed: ${elapsed}s`;

    const pct = Math.min(90, Math.round((elapsed / estimatedSecs) * 90));
    if (barFill) barFill.style.width = pct + '%';

    const expectedStep = Math.min(steps.length - 1, Math.floor(elapsed / stepInterval));
    while (currentStep < expectedStep) {
      if (stepEls[currentStep]) {
        stepEls[currentStep].classList.remove('active');
        stepEls[currentStep].classList.add('done');
        stepEls[currentStep].querySelector('i').className = 'fa-solid fa-check';
      }
      currentStep++;
      if (stepEls[currentStep]) stepEls[currentStep].classList.add('active');
    }
  }, 1000);

  return {
    finish(success, message) {
      clearInterval(timer);
      _dbDisableGuardOff();
      stepEls.forEach(el => {
        el.classList.remove('active');
        if (success) {
          el.classList.add('done');
          el.querySelector('i').className = 'fa-solid fa-check';
        }
      });
      if (barFill) barFill.style.width = '100%';
      if (success) {
        setTimeout(() => {
          statusEl.innerHTML = `<div class="db-disable-done" style="--db-brand:${color}"><i class="fa-solid fa-circle-check"></i><span>${message || 'Disabled successfully'}</span></div>`;
        }, 400);
      } else {
        statusEl.innerHTML = `<div class="db-status-error"><i class="fa-solid fa-exclamation-triangle"></i><span>${message || 'Failed to disable'}</span></div>`;
      }
    }
  };
}

(function () {
  'use strict';

  const pgadminToggle = document.getElementById('pgadminToggle');
  const pgadminStatus = document.getElementById('pgadminStatus');
  const pgadminAccessSection = document.getElementById('pgadminAccessSection');
  const pgadminUrl = document.getElementById('pgadminUrl');
  const copyPgadminUrl = document.getElementById('copyPgadminUrl');
  const openPgadminUrl = document.getElementById('openPgadminUrl');

  const pgadminSetupModalOverlay = document.getElementById('pgadminSetupModalOverlay');
  const pgadminSetupModalClose = document.getElementById('pgadminSetupModalClose');
  const pgadminSetupCancel = document.getElementById('pgadminSetupCancel');
  const pgadminSetupSave = document.getElementById('pgadminSetupSave');
  const pgadminSetupFeedback = document.getElementById('pgadminSetupFeedback');
  const pgadminEmail = document.getElementById('pgadmin_email');
  const pgadminPassword = document.getElementById('pgadmin_password');
  const pgadminPasswordStrength = document.getElementById('pgadminPasswordStrength');

  function showPgAdminFeedback(msg, isError = false) {
    if (pgadminSetupFeedback) {
      pgadminSetupFeedback.textContent = msg;
      pgadminSetupFeedback.className = 'node-feedback' + (isError ? ' error' : ' success');
      pgadminSetupFeedback.style.display = msg ? 'block' : 'none';
    }
  }

  function openPgAdminSetupModal() {
    if (pgadminEmail) pgadminEmail.value = '';
    if (pgadminPassword) pgadminPassword.value = '';
    if (pgadminPasswordStrength) pgadminPasswordStrength.innerHTML = '';
    showPgAdminFeedback('');
    if (pgadminSetupModalOverlay) {
      pgadminSetupModalOverlay.classList.add('show');
      pgadminSetupModalOverlay.setAttribute('aria-hidden', 'false');
    }
  }

  function closePgAdminSetupModal() {
    if (pgadminSetupModalOverlay) {
      pgadminSetupModalOverlay.classList.remove('show');
      pgadminSetupModalOverlay.setAttribute('aria-hidden', 'true');
    }
    if (pgadminToggle && !pgadminToggle.dataset.wasEnabled) {
      pgadminToggle.checked = false;
    }
  }

  window.loadPgAdminStatus = async function () {
    if (!pgadminStatus) return;
    pgadminStatus.innerHTML = '<div class="db-status-loading"><i class="fa-solid fa-spinner fa-spin"></i><span>Checking status...</span></div>';

    try {
      const res = await fetch('/api/settings/database/pgadmin/status');
      if (!res.ok) throw new Error('Failed to fetch status');
      const data = await res.json();

      if (pgadminToggle) {
        pgadminToggle.checked = data.enabled;
        pgadminToggle.dataset.wasEnabled = data.enabled ? 'true' : '';
      }

      if (data.enabled && data.postgresRunning) {
        if (pgadminUrl) pgadminUrl.textContent = 'Secure token access (stays active while this tab stays open)';
        if (pgadminAccessSection) pgadminAccessSection.style.display = 'block';
        pgadminStatus.innerHTML = '<div class="db-status-online"><i class="fa-solid fa-circle-check"></i><span>PostgreSQL & pgAdmin4 running</span></div>';
      } else if (data.installed) {
        pgadminStatus.innerHTML = '<div class="db-status-offline"><i class="fa-solid fa-circle-xmark"></i><span>PostgreSQL installed but not enabled</span></div>';
        if (pgadminAccessSection) pgadminAccessSection.style.display = 'none';
      } else {
        pgadminStatus.innerHTML = '<div class="db-status-not-installed"><i class="fa-solid fa-download"></i><span>Not installed - Enable to install</span></div>';
        if (pgadminAccessSection) pgadminAccessSection.style.display = 'none';
      }
    } catch (err) {
      console.error('[pgadmin] Status error:', err);
      pgadminStatus.innerHTML = '<div class="db-status-error"><i class="fa-solid fa-exclamation-triangle"></i><span>Error checking status</span></div>';
    }
  };

  async function savePgAdminSetup() {
    const email = pgadminEmail?.value?.trim() || '';
    const password = pgadminPassword?.value || '';
    const dbUser = document.getElementById('pgadmin_db_user')?.value?.trim() || 'admin';
    const dbHost = document.getElementById('pgadmin_db_host')?.value?.trim() || '0.0.0.0';
    const dbPort = document.getElementById('pgadmin_db_port')?.value?.trim() || '5432';

    if (!email || !email.includes('@')) {
      showPgAdminFeedback('Please enter a valid email address', true);
      return;
    }

    if (!password || password.length < 8) {
      showPgAdminFeedback('Password must be at least 8 characters', true);
      return;
    }

    if (!dbUser || dbUser.length < 2) {
      showPgAdminFeedback('Please enter a valid database username', true);
      return;
    }

    const progressContainer = document.getElementById('pgadminProgressContainer');
    const progressText = document.getElementById('pgadminProgressText');
    const progressPercent = document.getElementById('pgadminProgressPercent');
    const progressFill = document.getElementById('pgadminProgressFill');

    if (progressContainer) progressContainer.style.display = 'block';
    if (progressText) progressText.textContent = 'Starting installation...';
    if (progressPercent) progressPercent.textContent = '0%';
    if (progressFill) progressFill.style.width = '0%';

    if (pgadminSetupSave) {
      pgadminSetupSave.disabled = true;
      pgadminSetupSave.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Installing...';
    }
    if (pgadminSetupCancel) pgadminSetupCancel.disabled = true;
    showPgAdminFeedback('', false);

    try {
      const res = await fetch('/api/settings/database/pgadmin/setup', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-action-token': (window._dbActionTokens || {}).pgadminSetup || '' },
        body: JSON.stringify({ email, password, dbUser, dbHost, dbPort })
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Setup failed');

      const jobId = data.jobId;
      const accessUrl = data.accessUrl;

      const pollProgress = async () => {
        try {
          const statusRes = await fetch(`/api/settings/database/install-progress/${jobId}`);
          const status = await statusRes.json();

          if (progressText) progressText.textContent = status.message || 'Installing...';
          if (progressPercent) progressPercent.textContent = `${status.progress || 0}%`;
          if (progressFill) progressFill.style.width = `${status.progress || 0}%`;

          if (status.status === 'completed') {
            if (progressContainer) progressContainer.style.display = 'none';
            if (pgadminSetupSave) { pgadminSetupSave.disabled = false; pgadminSetupSave.innerHTML = '<i class="fa-solid fa-download"></i> Install & Setup'; }
            if (pgadminSetupCancel) pgadminSetupCancel.disabled = false;
            showPgAdminFeedback('pgAdmin4 setup complete!', false);

            setTimeout(() => {
              closePgAdminSetupModal();
              loadPgAdminStatus();

              const toast = document.createElement('div');
              toast.className = 'perm-success-toast';
              toast.innerHTML = `<i class="fa-solid fa-circle-check"></i> pgAdmin4 installed! Use the Open button for secure access.`;
              document.body.appendChild(toast);
              setTimeout(() => { toast.classList.add('show'); }, 10);
              setTimeout(() => { toast.classList.remove('show'); setTimeout(() => toast.remove(), 300); }, 4000);
            }, 1000);
            return;
          } else if (status.status === 'failed') {
            throw new Error(status.error || 'Installation failed');
          } else {
            setTimeout(pollProgress, 2000);
          }
        } catch (pollErr) {
          console.error('[pgadmin] Poll error:', pollErr);
          showPgAdminFeedback('Installation failed: ' + pollErr.message, true);
          if (progressContainer) progressContainer.style.display = 'none';
          if (pgadminToggle) pgadminToggle.checked = false;
          if (pgadminSetupSave) { pgadminSetupSave.disabled = false; pgadminSetupSave.innerHTML = '<i class="fa-solid fa-download"></i> Install & Setup'; }
          if (pgadminSetupCancel) pgadminSetupCancel.disabled = false;
        }
      };

      setTimeout(pollProgress, 2000);

    } catch (err) {
      console.error('[pgadmin] Setup error:', err);
      showPgAdminFeedback('Setup failed: ' + err.message, true);
      if (progressContainer) progressContainer.style.display = 'none';
      if (pgadminToggle) pgadminToggle.checked = false;
      if (pgadminSetupSave) { pgadminSetupSave.disabled = false; pgadminSetupSave.innerHTML = '<i class="fa-solid fa-download"></i> Install & Setup'; }
      if (pgadminSetupCancel) pgadminSetupCancel.disabled = false;
    }
  }

  async function disablePgAdmin() {
    const pgadminDetails = document.getElementById('pgadminDetails');
    if (pgadminDetails) pgadminDetails.style.display = 'block';
    const progress = pgadminStatus ? createDbDisableProgress(pgadminStatus, [
      'Stopping PostgreSQL service',
      'Stopping Apache service',
      'Removing Apache config',
      'Cleaning up nginx config',
      'Uninstalling pgAdmin4 packages',
      'Removing data directories',
      'Cleaning up apt sources',
      'Finalizing'
    ], 60, '#336791') : null;

    if (pgadminToggle) pgadminToggle.disabled = true;
    if (pgadminAccessSection) pgadminAccessSection.style.display = 'none';

    try {
      const res = await fetch('/api/settings/database/pgadmin/disable', { method: 'POST', headers: { 'x-action-token': (window._dbActionTokens || {}).pgadminDisable || '' } });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.error || 'Failed to disable');
      }
      if (progress) progress.finish(true, 'pgAdmin4 disabled and uninstalled — refresh to update');
    } catch (err) {
      console.error('[pgadmin] Disable error:', err);
      if (progress) progress.finish(false, err.message || 'Failed to disable pgAdmin4');
      if (pgadminToggle) pgadminToggle.checked = true;
    } finally {
      if (pgadminToggle) pgadminToggle.disabled = false;
    }
  }

  if (pgadminToggle) {
    pgadminToggle.addEventListener('change', (e) => {
      if (e.target.checked) openPgAdminSetupModal();
      else disablePgAdmin();
    });
  }

  if (pgadminSetupModalClose) pgadminSetupModalClose.addEventListener('click', closePgAdminSetupModal);
  if (pgadminSetupCancel) pgadminSetupCancel.addEventListener('click', closePgAdminSetupModal);
  if (pgadminSetupModalOverlay) {
    pgadminSetupModalOverlay.addEventListener('click', (e) => {
      if (e.target === pgadminSetupModalOverlay) closePgAdminSetupModal();
    });
  }
  if (pgadminSetupSave) pgadminSetupSave.addEventListener('click', savePgAdminSetup);

  if (pgadminPassword && pgadminPasswordStrength) {
    pgadminPassword.addEventListener('input', (e) => {
      const val = e.target.value;
      let strength = 'weak';
      if (val.length >= 12 && /[A-Z]/.test(val) && /[0-9]/.test(val) && /[^A-Za-z0-9]/.test(val)) {
        strength = 'strong';
      } else if (val.length >= 10 && /[A-Z]/.test(val) && /[0-9]/.test(val)) {
        strength = 'good';
      } else if (val.length >= 8) {
        strength = 'fair';
      }
      pgadminPasswordStrength.innerHTML = val ? `<div class="password-strength-bar ${strength}"></div>` : '';
    });
  }

  if (copyPgadminUrl) {
    copyPgadminUrl.addEventListener('click', () => {
      const copyDbAccessUrl = window._copyDbAccessUrl;
      if (typeof copyDbAccessUrl === 'function') copyDbAccessUrl('pgadmin', copyPgadminUrl);
    });
  }

  if (openPgadminUrl) {
    openPgadminUrl.addEventListener('click', () => {
      const openDbAccessWindow = window._openDbAccessWindow;
      if (typeof openDbAccessWindow === 'function') openDbAccessWindow('pgadmin', openPgadminUrl);
    });
  }
})();

(function () {
  'use strict';

  const mongodbToggle = document.getElementById('mongodbToggle');
  const mongodbStatus = document.getElementById('mongodbStatus');
  const mongodbAccessSection = document.getElementById('mongodbAccessSection');
  const mongodbConnString = document.getElementById('mongodbConnString');
  const copyMongodbUrl = document.getElementById('copyMongodbUrl');
  const mongodbConnPass = document.getElementById('mongodbConnPass');
  const toggleMongodbPassword = document.getElementById('toggleMongodbPassword');

  const mongodbSetupModalOverlay = document.getElementById('mongodbSetupModalOverlay');
  const mongodbSetupModalClose = document.getElementById('mongodbSetupModalClose');
  const mongodbSetupCancel = document.getElementById('mongodbSetupCancel');
  const mongodbSetupSave = document.getElementById('mongodbSetupSave');
  const mongodbSetupFeedback = document.getElementById('mongodbSetupFeedback');
  const mongodbUsername = document.getElementById('mongodb_username');
  const mongodbPassword = document.getElementById('mongodb_password');
  const mongodbPasswordStrength = document.getElementById('mongodbPasswordStrength');
  const MONGODB_PASSWORD_CACHE_PREFIX = 'adpanel.mongodb.password.';
  let pendingMongoPassword = '';

  function getMongoConnectionInfo() {
    const urlWrapper = document.querySelector('.db-access-url-wrapper');
    const connUser = document.getElementById('mongodbConnUser');
    const connHost = document.getElementById('mongodbConnHost');
    const rawHost = (urlWrapper?.dataset.host || '').trim();
    const rawPort = (urlWrapper?.dataset.port || '').trim();
    const hostText = (connHost?.textContent || '').trim();
    const splitAt = hostText.lastIndexOf(':');
    const hostFromText = splitAt > -1 ? hostText.slice(0, splitAt) : hostText;
    const portFromText = splitAt > -1 ? hostText.slice(splitAt + 1) : '';
    const username = (urlWrapper?.dataset.username || connUser?.textContent || 'admin').trim() || 'admin';
    const host = rawHost || hostFromText || 'localhost';
    const port = rawPort || portFromText || '27017';
    return {
      urlWrapper,
      username,
      host,
      port,
      cacheKey: `${MONGODB_PASSWORD_CACHE_PREFIX}${username}@${host}:${port}`
    };
  }

  function setMongoConnectionPassword(password, persist = true) {
    const safePassword = typeof password === 'string' ? password : '';
    const { urlWrapper, cacheKey } = getMongoConnectionInfo();
    if (mongodbConnPass) mongodbConnPass.dataset.password = safePassword;
    if (urlWrapper) urlWrapper.dataset.password = safePassword;
    if (!persist) return;
    try {
      if (safePassword) localStorage.setItem(cacheKey, safePassword);
      else localStorage.removeItem(cacheKey);
    } catch { }
  }

  function loadCachedMongoConnectionPassword() {
    const { cacheKey } = getMongoConnectionInfo();
    try {
      const cached = localStorage.getItem(cacheKey) || '';
      setMongoConnectionPassword(cached, false);
      return cached;
    } catch {
      return '';
    }
  }

  function ensureMongoConnectionPassword() {
    const existing = mongodbConnPass?.dataset.password || '';
    if (existing) return existing;
    const cached = loadCachedMongoConnectionPassword();
    if (cached) return cached;
    const { username } = getMongoConnectionInfo();
    const entered = prompt(`Enter MongoDB password for "${username}" to use in the connection string:`);
    if (!entered) return '';
    setMongoConnectionPassword(entered, true);
    return entered;
  }

  function hideMongoConnectionPassword() {
    if (!mongodbConnPass) return;
    mongodbConnPass.classList.add('blurred');
    mongodbConnPass.textContent = '••••••••';
    if (toggleMongodbPassword) {
      toggleMongodbPassword.innerHTML = '<i class="fa-solid fa-eye"></i>';
      toggleMongodbPassword.title = 'Show password';
    }
  }

  function showMongoConnectionPassword(password) {
    if (!mongodbConnPass) return;
    mongodbConnPass.classList.remove('blurred');
    mongodbConnPass.textContent = password;
    if (toggleMongodbPassword) {
      toggleMongodbPassword.innerHTML = '<i class="fa-solid fa-eye-slash"></i>';
      toggleMongodbPassword.title = 'Hide password';
    }
  }

  function showMongoDBFeedback(msg, isError = false) {
    if (mongodbSetupFeedback) {
      mongodbSetupFeedback.textContent = msg;
      mongodbSetupFeedback.className = 'node-feedback' + (isError ? ' error' : ' success');
      mongodbSetupFeedback.style.display = msg ? 'block' : 'none';
    }
  }

  function openMongoDBSetupModal() {
    if (mongodbUsername) mongodbUsername.value = 'admin';
    if (mongodbPassword) mongodbPassword.value = '';
    if (mongodbPasswordStrength) mongodbPasswordStrength.innerHTML = '';
    showMongoDBFeedback('');
    if (mongodbSetupModalOverlay) {
      mongodbSetupModalOverlay.classList.add('show');
      mongodbSetupModalOverlay.setAttribute('aria-hidden', 'false');
    }
  }

  function closeMongoDBSetupModal() {
    if (mongodbSetupModalOverlay) {
      mongodbSetupModalOverlay.classList.remove('show');
      mongodbSetupModalOverlay.setAttribute('aria-hidden', 'true');
    }
    if (mongodbToggle && !mongodbToggle.dataset.wasEnabled) {
      mongodbToggle.checked = false;
    }
  }

  window.loadMongoDBStatus = async function () {
    if (!mongodbStatus) return;
    mongodbStatus.innerHTML = '<div class="db-status-loading"><i class="fa-solid fa-spinner fa-spin"></i><span>Checking status...</span></div>';

    try {
      const res = await fetch('/api/settings/database/mongodb/status');
      if (!res.ok) throw new Error('Failed to fetch status');
      const data = await res.json();

      if (mongodbToggle) {
        mongodbToggle.checked = data.enabled;
        mongodbToggle.dataset.wasEnabled = data.enabled ? 'true' : '';
      }

      if (data.enabled && data.mongoRunning) {
        const host = data.host === '0.0.0.0' ? window.location.hostname : (data.host || 'localhost');
        const port = data.port || '27017';

        if (data.users && data.users[0]) {
          const adminUser = data.users[0];
          const connUser = document.getElementById('mongodbConnUser');
          const connPass = document.getElementById('mongodbConnPass');
          const connHost = document.getElementById('mongodbConnHost');

          if (connUser) connUser.textContent = adminUser.username;
          if (connPass) {
            hideMongoConnectionPassword();
          }
          if (connHost) connHost.textContent = `${host}:${port}`;

          const urlWrapper = document.querySelector('.db-access-url-wrapper');
          if (urlWrapper) {
            urlWrapper.dataset.username = adminUser.username || 'admin';
            urlWrapper.dataset.host = host;
            urlWrapper.dataset.port = port;
          }
          if (pendingMongoPassword) {
            setMongoConnectionPassword(pendingMongoPassword, true);
            pendingMongoPassword = '';
          } else {
            loadCachedMongoConnectionPassword();
          }
        }
        if (mongodbAccessSection) mongodbAccessSection.style.display = 'block';
        mongodbStatus.innerHTML = `<div class="db-status-online"><i class="fa-solid fa-circle-check"></i><span>MongoDB running on ${host}:${port}</span></div>`;
        if (typeof window.loadMongoDBUsers === 'function') window.loadMongoDBUsers();
      } else if (data.installed) {
        mongodbStatus.innerHTML = '<div class="db-status-offline"><i class="fa-solid fa-circle-xmark"></i><span>MongoDB installed but not enabled</span></div>';
        if (mongodbAccessSection) mongodbAccessSection.style.display = 'none';
      } else {
        mongodbStatus.innerHTML = '<div class="db-status-not-installed"><i class="fa-solid fa-download"></i><span>Not installed - Enable to install</span></div>';
        if (mongodbAccessSection) mongodbAccessSection.style.display = 'none';
      }
    } catch (err) {
      console.error('[mongodb] Status error:', err);
      mongodbStatus.innerHTML = '<div class="db-status-error"><i class="fa-solid fa-exclamation-triangle"></i><span>Error checking status</span></div>';
    }
  };

  async function saveMongoDBSetup() {
    const username = mongodbUsername?.value?.trim() || 'admin';
    const password = mongodbPassword?.value || '';
    const host = document.getElementById('mongodb_host')?.value?.trim() || '0.0.0.0';
    const port = document.getElementById('mongodb_port')?.value?.trim() || '27017';

    if (!username || !/^[a-zA-Z0-9_-]+$/.test(username)) {
      showMongoDBFeedback('Please enter a valid username (alphanumeric, _, - only)', true);
      return;
    }

    if (!password || password.length < 8) {
      showMongoDBFeedback('Password must be at least 8 characters', true);
      return;
    }

    const progressContainer = document.getElementById('mongodbProgressContainer');
    const progressText = document.getElementById('mongodbProgressText');
    const progressPercent = document.getElementById('mongodbProgressPercent');
    const progressFill = document.getElementById('mongodbProgressFill');

    if (progressContainer) progressContainer.style.display = 'block';
    if (progressText) progressText.textContent = 'Starting installation...';
    if (progressPercent) progressPercent.textContent = '0%';
    if (progressFill) progressFill.style.width = '0%';

    if (mongodbSetupSave) {
      mongodbSetupSave.disabled = true;
      mongodbSetupSave.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Installing...';
    }
    if (mongodbSetupCancel) mongodbSetupCancel.disabled = true;
    showMongoDBFeedback('', false);

    try {
      const res = await fetch('/api/settings/database/mongodb/setup', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-action-token': (window._dbActionTokens || {}).mongodbSetup || '' },
        body: JSON.stringify({ username, password, host, port })
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Setup failed');

      const jobId = data.jobId;

      const pollProgress = async () => {
        try {
          const statusRes = await fetch(`/api/settings/database/install-progress/${jobId}`);
          const status = await statusRes.json();

          if (progressText) progressText.textContent = status.message || 'Installing...';
          if (progressPercent) progressPercent.textContent = `${status.progress || 0}%`;
          if (progressFill) progressFill.style.width = `${status.progress || 0}%`;

          if (status.status === 'completed') {
            if (progressContainer) progressContainer.style.display = 'none';
            if (mongodbSetupSave) { mongodbSetupSave.disabled = false; mongodbSetupSave.innerHTML = '<i class="fa-solid fa-download"></i> Install & Setup'; }
            if (mongodbSetupCancel) mongodbSetupCancel.disabled = false;
            showMongoDBFeedback('MongoDB setup complete!', false);
            pendingMongoPassword = password;

            setTimeout(() => {
              closeMongoDBSetupModal();
              loadMongoDBStatus();

              const toast = document.createElement('div');
              toast.className = 'perm-success-toast';
              toast.innerHTML = '<i class="fa-solid fa-circle-check"></i> MongoDB installed and ready!';
              document.body.appendChild(toast);
              setTimeout(() => { toast.classList.add('show'); }, 10);
              setTimeout(() => { toast.classList.remove('show'); setTimeout(() => toast.remove(), 300); }, 4000);
            }, 1000);
            return;
          } else if (status.status === 'failed') {
            throw new Error(status.error || 'Installation failed');
          } else {
            setTimeout(pollProgress, 2000);
          }
        } catch (pollErr) {
          console.error('[mongodb] Poll error:', pollErr);
          showMongoDBFeedback('Installation failed: ' + pollErr.message, true);
          if (progressContainer) progressContainer.style.display = 'none';
          if (mongodbToggle) mongodbToggle.checked = false;
          if (mongodbSetupSave) { mongodbSetupSave.disabled = false; mongodbSetupSave.innerHTML = '<i class="fa-solid fa-download"></i> Install & Setup'; }
          if (mongodbSetupCancel) mongodbSetupCancel.disabled = false;
        }
      };

      setTimeout(pollProgress, 2000);

    } catch (err) {
      console.error('[mongodb] Setup error:', err);
      showMongoDBFeedback('Setup failed: ' + err.message, true);
      if (progressContainer) progressContainer.style.display = 'none';
      if (mongodbToggle) mongodbToggle.checked = false;
      if (mongodbSetupSave) { mongodbSetupSave.disabled = false; mongodbSetupSave.innerHTML = '<i class="fa-solid fa-download"></i> Install & Setup'; }
      if (mongodbSetupCancel) mongodbSetupCancel.disabled = false;
    }
  }

  async function disableMongoDB() {
    const mongodbStatus = document.getElementById('mongodbStatus');
    const mongodbDetails = document.getElementById('mongodbDetails');
    const mongodbAccessSection = document.getElementById('mongodbAccessSection');
    if (mongodbDetails) mongodbDetails.style.display = 'block';

    const progress = mongodbStatus ? createDbDisableProgress(mongodbStatus, [
      'Stopping MongoDB service',
      'Removing MongoDB packages',
      'Removing data and config',
      'Cleaning up apt sources',
      'Reloading system services',
      'Finalizing'
    ], 90, '#00ed64') : null;

    if (mongodbToggle) mongodbToggle.disabled = true;
    if (mongodbAccessSection) mongodbAccessSection.style.display = 'none';

    try {
      const res = await fetch('/api/settings/database/mongodb/disable', { method: 'POST', headers: { 'x-action-token': (window._dbActionTokens || {}).mongodbDisable || '' } });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.error || 'Failed to disable');
      }
      if (progress) progress.finish(true, 'MongoDB disabled and removed — refresh to update');
    } catch (err) {
      console.error('[mongodb] Disable error:', err);
      if (progress) progress.finish(false, err.message || 'Failed to disable MongoDB');
      if (mongodbToggle) mongodbToggle.checked = true;
    } finally {
      if (mongodbToggle) mongodbToggle.disabled = false;
    }
  }

  if (mongodbToggle) {
    mongodbToggle.addEventListener('change', (e) => {
      if (e.target.checked) openMongoDBSetupModal();
      else disableMongoDB();
    });
  }

  if (mongodbSetupModalClose) mongodbSetupModalClose.addEventListener('click', closeMongoDBSetupModal);
  if (mongodbSetupCancel) mongodbSetupCancel.addEventListener('click', closeMongoDBSetupModal);
  if (mongodbSetupModalOverlay) {
    mongodbSetupModalOverlay.addEventListener('click', (e) => {
      if (e.target === mongodbSetupModalOverlay) closeMongoDBSetupModal();
    });
  }
  if (mongodbSetupSave) mongodbSetupSave.addEventListener('click', saveMongoDBSetup);

  if (mongodbPassword && mongodbPasswordStrength) {
    mongodbPassword.addEventListener('input', (e) => {
      const val = e.target.value;
      let strength = 'weak';
      if (val.length >= 12 && /[A-Z]/.test(val) && /[0-9]/.test(val) && /[^A-Za-z0-9]/.test(val)) {
        strength = 'strong';
      } else if (val.length >= 10 && /[A-Z]/.test(val) && /[0-9]/.test(val)) {
        strength = 'good';
      } else if (val.length >= 8) {
        strength = 'fair';
      }
      mongodbPasswordStrength.innerHTML = val ? `<div class="password-strength-bar ${strength}"></div>` : '';
    });
  }

  if (mongodbConnPass) {
    mongodbConnPass.addEventListener('click', () => {
      const isBlurred = mongodbConnPass.classList.contains('blurred');
      if (isBlurred) {
        const password = ensureMongoConnectionPassword();
        if (!password) return;
        showMongoConnectionPassword(password);
      } else if (!isBlurred) {
        hideMongoConnectionPassword();
      }
    });
  }

  if (toggleMongodbPassword && mongodbConnPass) {
    toggleMongodbPassword.addEventListener('click', () => {
      const isBlurred = mongodbConnPass.classList.contains('blurred');
      if (isBlurred) {
        const password = ensureMongoConnectionPassword();
        if (!password) return;
        showMongoConnectionPassword(password);
      } else if (!isBlurred) {
        hideMongoConnectionPassword();
      }
    });
  }

  if (copyMongodbUrl) {
    copyMongodbUrl.addEventListener('click', () => {
      const urlWrapper = document.querySelector('.db-access-url-wrapper');
      if (!urlWrapper) return;
      const username = urlWrapper.dataset.username || 'admin';
      let password = urlWrapper.dataset.password || '';
      if (!password) {
        password = ensureMongoConnectionPassword();
        if (!password) return;
      }
      const host = urlWrapper.dataset.host || 'localhost';
      const port = urlWrapper.dataset.port || '27017';
      const encodedUser = encodeURIComponent(username);
      const encodedPass = encodeURIComponent(password);
      const fullUrl = `mongodb://${encodedUser}:${encodedPass}@${host}:${port}/?authSource=admin`;

      copyToClipboard(fullUrl).then(() => {
        copyMongodbUrl.innerHTML = '<i class="fa-solid fa-check"></i>';
        setTimeout(() => { copyMongodbUrl.innerHTML = '<i class="fa-solid fa-copy"></i>'; }, 1500);
      });
    });
  }

  let mongodbActionTokens = {};
  window.loadMongoDBUsers = async function () {
    try {
      const res = await fetch('/api/settings/database/mongodb/users');
      const data = await res.json();

      if (!data.ok || !data.enabled) return;

      mongodbActionTokens = data.actionTokens || {};

      const portInfo = document.getElementById('mongodbPortInfo');
      const host = data.host === '0.0.0.0' ? window.location.hostname : (data.host || 'localhost');
      const port = data.port || '27017';
      if (portInfo) {
        portInfo.textContent = `Database is running on ${host}:${port}`;
      }

      const connUser = document.getElementById('mongodbConnUser');
      const connPass = document.getElementById('mongodbConnPass');
      const connHost = document.getElementById('mongodbConnHost');
      const urlWrapper = document.querySelector('.db-access-url-wrapper');

      if (data.users && data.users[0]) {
        const adminUser = data.users[0];
        if (connUser) connUser.textContent = adminUser.username;
        if (connPass) {
          hideMongoConnectionPassword();
        }
        if (connHost) connHost.textContent = `${host}:${port}`;
        if (urlWrapper) {
          urlWrapper.dataset.username = adminUser.username || 'admin';
          urlWrapper.dataset.host = host;
          urlWrapper.dataset.port = port;
        }
        if (pendingMongoPassword) {
          setMongoConnectionPassword(pendingMongoPassword, true);
          pendingMongoPassword = '';
        } else {
          loadCachedMongoConnectionPassword();
        }
      } else {
        if (connUser) connUser.textContent = 'admin';
        if (connPass) {
          hideMongoConnectionPassword();
        }
        if (connHost) connHost.textContent = `${host}:${port}`;
      }

      const usersList = document.getElementById('mongodbUsersList');
      if (!usersList) return;

      if (!data.users || data.users.length === 0) {
        usersList.innerHTML = `
          <div class="db-users-empty">
            <i class="fa-solid fa-users-slash"></i>
            <p>No users found</p>
          </div>
        `;
        return;
      }

      usersList.innerHTML = data.users.map((user, idx) => {
        const createdAt = user.createdAt ? new Date(user.createdAt).toLocaleDateString() : 'N/A';
        const isAdmin = idx === 0;
        return `
          <div class="db-user-row">
            <div class="db-user-left">
              <div class="db-user-icon ${isAdmin ? 'admin' : ''}">
                <i class="fa-solid ${isAdmin ? 'fa-user-shield' : 'fa-user'}"></i>
              </div>
              <div>
                <div class="db-user-name">${user.username}${isAdmin ? ' <span class="db-user-badge">root</span>' : ''}</div>
                <div class="db-user-created">Created ${createdAt}</div>
              </div>
            </div>
            <div class="db-user-actions">
              <button class="db-user-btn db-user-btn-edit" data-action="change-password" data-username="${user.username}" title="Change password">
                <i class="fa-solid fa-key"></i>
              </button>
              ${!isAdmin ? `
              <button class="db-user-btn db-user-btn-delete" data-action="delete-user" data-username="${user.username}" title="Delete user">
                <i class="fa-solid fa-trash"></i>
              </button>` : ''}
            </div>
          </div>
        `;
      }).join('');

      usersList.querySelectorAll('[data-action="change-password"]').forEach(btn => {
        btn.addEventListener('click', () => {
          const username = btn.getAttribute('data-username');
          window.changeMongoDBPassword(username);
        });
      });

      usersList.querySelectorAll('[data-action="delete-user"]').forEach(btn => {
        btn.addEventListener('click', () => {
          const username = btn.getAttribute('data-username');
          window.showDeleteUserModal(username);
        });
      });
    } catch (err) {
      console.error('[mongodb] Load users error:', err);
    }
  }

  const deleteModalOverlay = document.getElementById('mongodbDeleteUserModalOverlay');
  const deleteUsernameEl = document.getElementById('mongodbDeleteUsername');
  const deleteCancelBtn = document.getElementById('mongodbDeleteCancel');
  const deleteConfirmBtn = document.getElementById('mongodbDeleteConfirm');
  let pendingDeleteUsername = null;

  function openDeleteModal(username) {
    pendingDeleteUsername = username;
    if (deleteUsernameEl) deleteUsernameEl.textContent = username;
    if (deleteModalOverlay) {
      deleteModalOverlay.classList.add('show');
      deleteModalOverlay.setAttribute('aria-hidden', 'false');
    }
  }

  function closeDeleteModal() {
    if (deleteModalOverlay) {
      deleteModalOverlay.classList.remove('show');
      deleteModalOverlay.setAttribute('aria-hidden', 'true');
    }
    pendingDeleteUsername = null;
  }

  window.showDeleteUserModal = openDeleteModal;

  if (deleteCancelBtn) {
    deleteCancelBtn.addEventListener('click', closeDeleteModal);
  }

  if (deleteModalOverlay) {
    deleteModalOverlay.addEventListener('click', (e) => {
      if (e.target === deleteModalOverlay) closeDeleteModal();
    });
  }

  if (deleteConfirmBtn) {
    deleteConfirmBtn.addEventListener('click', async () => {
      if (!pendingDeleteUsername) return;

      deleteConfirmBtn.disabled = true;
      deleteConfirmBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Deleting...';

      try {
        const res = await fetch(`/api/settings/database/mongodb/users/${encodeURIComponent(pendingDeleteUsername)}`, {
          method: 'DELETE',
          headers: { 'x-action-token': mongodbActionTokens[`deleteUser_${pendingDeleteUsername}`] || '' }
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Delete failed');

        closeDeleteModal();
        window.loadMongoDBUsers();
      } catch (err) {
        alert('Failed to delete user: ' + err.message);
      } finally {
        deleteConfirmBtn.disabled = false;
        deleteConfirmBtn.innerHTML = '<i class="fa-solid fa-trash"></i> Delete User';
      }
    });
  }

  window.deleteMongoDBUser = function (username) {
    openDeleteModal(username);
  };

  window.changeMongoDBPassword = async function (username) {
    const newPassword = prompt(`Enter new password for "${username}" (min 8 characters):`);
    if (!newPassword) return;
    if (newPassword.length < 8) {
      alert('Password must be at least 8 characters');
      return;
    }

    try {
      const res = await fetch(`/api/settings/database/mongodb/users/${encodeURIComponent(username)}/password`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-action-token': mongodbActionTokens[`changePassword_${username}`] || '' },
        body: JSON.stringify({ password: newPassword })
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Change password failed');
      alert('Password changed successfully');
      const currentConnUser = (document.getElementById('mongodbConnUser')?.textContent || '').trim() || 'admin';
      if (String(username || '').trim() === currentConnUser) {
        setMongoConnectionPassword(newPassword, true);
        hideMongoConnectionPassword();
      }
    } catch (err) {
      alert('Failed to change password: ' + err.message);
    }
  };

  const mongodbUserModalOverlay = document.getElementById('mongodbUserModalOverlay');
  const mongodbUserModalClose = document.getElementById('mongodbUserModalClose');
  const mongodbUserCancel = document.getElementById('mongodbUserCancel');
  const mongodbUserSave = document.getElementById('mongodbUserSave');
  const mongodbNewUsername = document.getElementById('mongodb_new_username');
  const mongodbNewPassword = document.getElementById('mongodb_new_password');
  const mongodbUserFeedback = document.getElementById('mongodbUserFeedback');

  function openMongodbUserModal() {
    if (mongodbNewUsername) mongodbNewUsername.value = '';
    if (mongodbNewPassword) mongodbNewPassword.value = '';
    if (mongodbUserFeedback) mongodbUserFeedback.innerHTML = '';
    if (mongodbUserModalOverlay) {
      mongodbUserModalOverlay.classList.add('show');
      mongodbUserModalOverlay.setAttribute('aria-hidden', 'false');
    }
  }

  function closeMongodbUserModal() {
    if (mongodbUserModalOverlay) {
      mongodbUserModalOverlay.classList.remove('show');
      mongodbUserModalOverlay.setAttribute('aria-hidden', 'true');
    }
  }

  function showMongodbUserFeedback(msg, isError = false) {
    if (mongodbUserFeedback) {
      mongodbUserFeedback.innerHTML = msg ? `<div class="feedback-${isError ? 'error' : 'success'}">${msg}</div>` : '';
    }
  }

  const createMongodbUserBtn = document.getElementById('createMongodbUser');
  if (createMongodbUserBtn) {
    createMongodbUserBtn.addEventListener('click', openMongodbUserModal);
  }

  if (mongodbUserModalClose) mongodbUserModalClose.addEventListener('click', closeMongodbUserModal);
  if (mongodbUserCancel) mongodbUserCancel.addEventListener('click', closeMongodbUserModal);
  if (mongodbUserModalOverlay) {
    mongodbUserModalOverlay.addEventListener('click', (e) => {
      if (e.target === mongodbUserModalOverlay) closeMongodbUserModal();
    });
  }

  if (mongodbUserSave) {
    mongodbUserSave.addEventListener('click', async () => {
      const username = mongodbNewUsername?.value?.trim();
      const password = mongodbNewPassword?.value;

      if (!username || !/^[a-zA-Z0-9_-]+$/.test(username)) {
        showMongodbUserFeedback('Invalid username (alphanumeric, _, - only)', true);
        return;
      }
      if (!password || password.length < 8) {
        showMongodbUserFeedback('Password must be at least 8 characters', true);
        return;
      }

      mongodbUserSave.disabled = true;
      mongodbUserSave.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Creating...';
      showMongodbUserFeedback('', false);

      try {
        const res = await fetch('/api/settings/database/mongodb/users', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'x-action-token': mongodbActionTokens.createUser || '' },
          body: JSON.stringify({ username, password })
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Create failed');

        showMongodbUserFeedback('User created successfully!', false);
        setTimeout(() => {
          closeMongodbUserModal();
          window.loadMongoDBUsers();
        }, 1000);
      } catch (err) {
        showMongodbUserFeedback('Failed: ' + err.message, true);
      } finally {
        mongodbUserSave.disabled = false;
        mongodbUserSave.innerHTML = '<i class="fa-solid fa-user-plus"></i> Create User';
      }
    });
  }

  setupPasswordToggle('mongodb_new_password_toggle', 'mongodb_new_password');

  const originalLoadMongoDBStatus = loadMongoDBStatus;
  loadMongoDBStatus = async function () {
    await originalLoadMongoDBStatus();
    window.loadMongoDBUsers();
  };
})();

const adminChangePasswordOverlay = document.getElementById('adminChangePasswordOverlay');
const adminChangePasswordModal = document.getElementById('adminChangePasswordModal');
const adminChangePasswordTitle = document.getElementById('adminChangePasswordTitle');
const adminChangePasswordSubtitle = document.getElementById('adminChangePasswordSubtitle');
const adminChangePasswordClose = document.getElementById('adminChangePasswordClose');
const adminChangePasswordCancelBtn = document.getElementById('adminChangePasswordCancelBtn');
const adminChangePasswordSave = document.getElementById('adminChangePasswordSave');
const adminChangePasswordFeedback = document.getElementById('adminChangePasswordFeedback');
const adminNewPassword = document.getElementById('adminNewPassword');
const adminConfirmPassword = document.getElementById('adminConfirmPassword');
const adminPasswordStrength = document.getElementById('adminPasswordStrength');

let currentAdminChangePasswordEmail = '';

function showAdminPasswordFeedback(msg, isError = false) {
  if (!adminChangePasswordFeedback) return;
  adminChangePasswordFeedback.textContent = msg;
  adminChangePasswordFeedback.className = 'node-feedback' + (isError ? ' error' : ' success');
  adminChangePasswordFeedback.style.display = msg ? 'block' : 'none';
}

function clearAdminPasswordFeedback() {
  if (adminChangePasswordFeedback) {
    adminChangePasswordFeedback.textContent = '';
    adminChangePasswordFeedback.style.display = 'none';
  }
}

function openAdminChangePasswordModal(email) {
  currentAdminChangePasswordEmail = email;
  if (adminChangePasswordTitle) adminChangePasswordTitle.textContent = `Change password for ${email}`;
  if (adminChangePasswordSubtitle) adminChangePasswordSubtitle.textContent = 'Set a new password for this user';
  if (adminNewPassword) adminNewPassword.value = '';
  if (adminConfirmPassword) adminConfirmPassword.value = '';
  if (adminPasswordStrength) adminPasswordStrength.innerHTML = '';
  clearAdminPasswordFeedback();

  if (adminChangePasswordOverlay) {
    adminChangePasswordOverlay.setAttribute('aria-hidden', 'false');
    adminChangePasswordOverlay.classList.add('show');
  }
}

function closeAdminChangePasswordModal() {
  if (adminChangePasswordOverlay) {
    adminChangePasswordOverlay.setAttribute('aria-hidden', 'true');
    adminChangePasswordOverlay.classList.remove('show');
  }
  currentAdminChangePasswordEmail = '';
  clearAdminPasswordFeedback();
}

async function saveAdminChangePassword() {
  if (!currentAdminChangePasswordEmail) return;

  const newPassword = adminNewPassword?.value || '';
  const confirm = adminConfirmPassword?.value || '';

  if (!newPassword || !confirm) {
    showAdminPasswordFeedback('Please fill in all fields', true);
    return;
  }

  if (newPassword.length < 8) {
    showAdminPasswordFeedback('Password must be at least 8 characters', true);
    return;
  }

  if (newPassword !== confirm) {
    showAdminPasswordFeedback('Passwords do not match', true);
    return;
  }

  if (adminChangePasswordSave) {
    adminChangePasswordSave.disabled = true;
    adminChangePasswordSave.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Updating...';
  }

  try {
    const res = await fetch(`/api/settings/accounts/${encodeURIComponent(currentAdminChangePasswordEmail)}/change-password`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ newPassword, confirm })
    });

    const data = await res.json().catch(() => ({}));

    if (!res.ok) {
      showAdminPasswordFeedback(data.error || 'Failed to change password', true);
      return;
    }

    showAdminPasswordFeedback('Password changed successfully!', false);

    setTimeout(() => {
      closeAdminChangePasswordModal();

      const toast = document.createElement('div');
      toast.className = 'perm-success-toast';
      toast.innerHTML = '<i class="fa-solid fa-circle-check"></i> Password changed successfully';
      document.body.appendChild(toast);
      setTimeout(() => { toast.classList.add('show'); }, 10);
      setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => toast.remove(), 300);
      }, 2000);
    }, 1000);
  } catch (err) {
    console.error('[admin-change-password]', err);
    showAdminPasswordFeedback('Network error', true);
  } finally {
    if (adminChangePasswordSave) {
      adminChangePasswordSave.disabled = false;
      adminChangePasswordSave.innerHTML = '<i class="fa-solid fa-check"></i> Update Password';
    }
  }
}

if (adminChangePasswordClose) adminChangePasswordClose.addEventListener('click', closeAdminChangePasswordModal);
if (adminChangePasswordCancelBtn) adminChangePasswordCancelBtn.addEventListener('click', closeAdminChangePasswordModal);
if (adminChangePasswordSave) adminChangePasswordSave.addEventListener('click', saveAdminChangePassword);
if (adminChangePasswordOverlay) {
  adminChangePasswordOverlay.addEventListener('click', (e) => {
    if (e.target === adminChangePasswordOverlay) closeAdminChangePasswordModal();
  });
}

if (adminChangePasswordModal) {
  adminChangePasswordModal.querySelectorAll('.password-toggle').forEach(btn => {
    if (btn.dataset.listenerAdded) return;
    btn.dataset.listenerAdded = 'true';
    btn.addEventListener('click', () => {
      const wrap = btn.closest('.password-input-wrap');
      if (wrap) {
        const input = wrap.querySelector('input');
        const icon = btn.querySelector('i');
        if (input && icon) {
          if (input.type === 'password') {
            input.type = 'text';
            icon.classList.replace('fa-eye', 'fa-eye-slash');
          } else {
            input.type = 'password';
            icon.classList.replace('fa-eye-slash', 'fa-eye');
          }
        }
      }
    });
  });
}

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && adminChangePasswordOverlay?.classList.contains('show')) {
    closeAdminChangePasswordModal();
  }
});


const qrOptionsModalOverlay = document.getElementById('qrOptionsModalOverlay');
const qrOptionsModal = document.getElementById('qrOptionsModal');
const qrOptionsModalClose = document.getElementById('qrOptionsModalClose');
const qrOptionsUserEmail = document.getElementById('qrOptionsUserEmail');
const resetUser2FABtn = document.getElementById('resetUser2FABtn');
const resetUserRecoveryBtn = document.getElementById('resetUserRecoveryBtn');

const qrResetModalOverlay = document.getElementById('qrResetModalOverlay');
const qrResetModal = document.getElementById('qrResetModal');
const qrResetModalClose = document.getElementById('qrResetModalClose');
const qrResetUserEmail = document.getElementById('qrResetUserEmail');
const qrResetLoading = document.getElementById('qrResetLoading');
const qrResetContent = document.getElementById('qrResetContent');
const qrResetError = document.getElementById('qrResetError');
const qrResetErrorText = document.getElementById('qrResetErrorText');
const qrResetImage = document.getElementById('qrResetImage');
const qrResetSecretText = document.getElementById('qrResetSecretText');
const qrResetDoneBtn = document.getElementById('qrResetDoneBtn');

let currentQRResetEmail = '';
let currentQRResetSecret = '';

function openQROptionsModal(email) {
  if (!qrOptionsModalOverlay) return;
  currentQRResetEmail = email;
  if (qrOptionsUserEmail) qrOptionsUserEmail.textContent = email;
  qrOptionsModalOverlay.setAttribute('aria-hidden', 'false');
  qrOptionsModalOverlay.classList.add('show');
}

function closeQROptionsModal() {
  if (!qrOptionsModalOverlay) return;
  qrOptionsModalOverlay.setAttribute('aria-hidden', 'true');
  qrOptionsModalOverlay.classList.remove('show');
}

function openQRResetModal() {
  if (!qrResetModalOverlay) return;
  if (qrResetUserEmail) qrResetUserEmail.textContent = `New QR for ${currentQRResetEmail}`;
  if (qrResetLoading) qrResetLoading.style.display = 'block';
  if (qrResetContent) qrResetContent.style.display = 'none';
  if (qrResetError) qrResetError.style.display = 'none';
  qrResetModalOverlay.setAttribute('aria-hidden', 'false');
  qrResetModalOverlay.classList.add('show');

  generateNewQRCode();
}

function closeQRResetModal() {
  if (!qrResetModalOverlay) return;
  qrResetModalOverlay.setAttribute('aria-hidden', 'true');
  qrResetModalOverlay.classList.remove('show');
  currentQRResetSecret = '';
}

async function generateNewQRCode() {
  try {
    const res = await fetch('/api/admin/user/reset-2fa', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'include',
      body: JSON.stringify({ email: currentQRResetEmail })
    });

    const data = await res.json();

    if (!res.ok) {
      throw new Error(data.error || 'Failed to generate QR code');
    }

    currentQRResetSecret = data.newSecret;
    console.log('[admin] QR generated, secret stored:', currentQRResetSecret ? 'YES' : 'NO');
    if (qrResetImage) qrResetImage.src = data.qrCodeUrl;
    if (qrResetSecretText) qrResetSecretText.textContent = data.newSecret;

    if (qrResetLoading) qrResetLoading.style.display = 'none';
    if (qrResetContent) qrResetContent.style.display = 'block';
    if (qrResetError) qrResetError.style.display = 'none';

  } catch (err) {
    console.error('[admin] QR generation error:', err);
    if (qrResetLoading) qrResetLoading.style.display = 'none';
    if (qrResetContent) qrResetContent.style.display = 'none';
    if (qrResetError) qrResetError.style.display = 'block';
    if (qrResetErrorText) qrResetErrorText.textContent = err.message || 'Failed to generate QR code';
  }
}

async function confirmQRReset() {
  if (!currentQRResetSecret) {
    closeQRResetModal();
    return;
  }

  try {
    if (qrResetDoneBtn) {
      qrResetDoneBtn.disabled = true;
      qrResetDoneBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Saving...';
    }

    console.log('[admin] Confirming 2FA reset - email:', currentQRResetEmail, 'secret exists:', !!currentQRResetSecret);
    const res = await fetch('/api/admin/user/confirm-2fa-reset', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'include',
      body: JSON.stringify({
        email: currentQRResetEmail,
        newSecret: currentQRResetSecret
      })
    });

    const data = await res.json();

    if (!res.ok) {
      throw new Error(data.error || 'Failed to save 2FA');
    }

    closeQRResetModal();
    console.log('[admin] 2FA reset SUCCESS for', currentQRResetEmail);
    alert(`2FA successfully reset for ${currentQRResetEmail}`);

  } catch (err) {
    console.error('[admin] QR confirm error:', err);
    alert('Error: ' + (err.message || 'Failed to save 2FA'));
  } finally {
    if (qrResetDoneBtn) {
      qrResetDoneBtn.disabled = false;
      qrResetDoneBtn.innerHTML = '<i class="fa-solid fa-check"></i> Done';
    }
  }
}

if (qrOptionsModalClose) qrOptionsModalClose.addEventListener('click', closeQROptionsModal);
if (qrOptionsModalOverlay) {
  qrOptionsModalOverlay.addEventListener('click', (e) => {
    if (e.target === qrOptionsModalOverlay) closeQROptionsModal();
  });
}

if (resetUser2FABtn) {
  resetUser2FABtn.addEventListener('click', () => {
    closeQROptionsModal();
    openQRResetModal();
  });
}

if (qrResetModalClose) qrResetModalClose.addEventListener('click', closeQRResetModal);
if (qrResetModalOverlay) {
  qrResetModalOverlay.addEventListener('click', (e) => {
    if (e.target === qrResetModalOverlay) closeQRResetModal();
  });
}

if (qrResetDoneBtn) {
  qrResetDoneBtn.addEventListener('click', confirmQRReset);
}

if (accountsListEl) {
  accountsListEl.addEventListener('click', (e) => {
    const btn = e.target.closest('.qr-reset-btn');
    if (btn) {
      const email = decodeURIComponent(btn.dataset.email || '');
      if (email) openQROptionsModal(email);
    }
  });
}


const recoveryResetModalOverlay = document.getElementById('recoveryResetModalOverlay');
const recoveryResetModal = document.getElementById('recoveryResetModal');
const recoveryResetModalClose = document.getElementById('recoveryResetModalClose');
const recoveryResetUserEmail = document.getElementById('recoveryResetUserEmail');
const recoveryResetLoading = document.getElementById('recoveryResetLoading');
const recoveryResetContent = document.getElementById('recoveryResetContent');
const recoveryResetError = document.getElementById('recoveryResetError');
const recoveryResetErrorText = document.getElementById('recoveryResetErrorText');
const recoveryCodesList = document.getElementById('recoveryCodesList');
const recoveryResetDoneBtn = document.getElementById('recoveryResetDoneBtn');

function openRecoveryResetModal() {
  if (!recoveryResetModalOverlay) return;
  if (recoveryResetUserEmail) recoveryResetUserEmail.textContent = `New codes for ${currentQRResetEmail}`;
  if (recoveryResetLoading) recoveryResetLoading.style.display = 'block';
  if (recoveryResetContent) recoveryResetContent.style.display = 'none';
  if (recoveryResetError) recoveryResetError.style.display = 'none';
  recoveryResetModalOverlay.setAttribute('aria-hidden', 'false');
  recoveryResetModalOverlay.classList.add('show');

  generateNewRecoveryCodes();
}

function closeRecoveryResetModal() {
  if (!recoveryResetModalOverlay) return;
  recoveryResetModalOverlay.setAttribute('aria-hidden', 'true');
  recoveryResetModalOverlay.classList.remove('show');
}

async function generateNewRecoveryCodes() {
  try {
    const res = await fetch('/api/admin/user/reset-recovery-codes', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'include',
      body: JSON.stringify({ email: currentQRResetEmail })
    });

    const data = await res.json();

    if (!res.ok) {
      throw new Error(data.error || 'Failed to generate recovery codes');
    }

    if (recoveryCodesList) {
      recoveryCodesList.innerHTML = data.recoveryCodes.map((code, idx) => `
        <div style="background: rgba(0,0,0,0.2); padding: 12px 16px; border-radius: 8px; font-family: 'SF Mono', Monaco, monospace; font-size: 14px; letter-spacing: 2px; text-align: center; color: #10b981;">
          ${idx + 1}. ${code}
        </div>
      `).join('');
    }

    if (recoveryResetLoading) recoveryResetLoading.style.display = 'none';
    if (recoveryResetContent) recoveryResetContent.style.display = 'block';
    if (recoveryResetError) recoveryResetError.style.display = 'none';

    console.log('[admin] Recovery codes reset SUCCESS for', currentQRResetEmail);

  } catch (err) {
    console.error('[admin] Recovery codes generation error:', err);
    if (recoveryResetLoading) recoveryResetLoading.style.display = 'none';
    if (recoveryResetContent) recoveryResetContent.style.display = 'none';
    if (recoveryResetError) recoveryResetError.style.display = 'block';
    if (recoveryResetErrorText) recoveryResetErrorText.textContent = err.message || 'Failed to generate recovery codes';
  }
}

if (recoveryResetModalClose) recoveryResetModalClose.addEventListener('click', closeRecoveryResetModal);
if (recoveryResetModalOverlay) {
  recoveryResetModalOverlay.addEventListener('click', (e) => {
    if (e.target === recoveryResetModalOverlay) closeRecoveryResetModal();
  });
}

if (recoveryResetDoneBtn) {
  recoveryResetDoneBtn.addEventListener('click', closeRecoveryResetModal);
}

if (resetUserRecoveryBtn) {
  resetUserRecoveryBtn.addEventListener('click', () => {
    closeQROptionsModal();
    openRecoveryResetModal();
  });
}

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') {
    if (recoveryResetModalOverlay?.classList.contains('show')) {
      closeRecoveryResetModal();
    } else if (qrResetModalOverlay?.classList.contains('show')) {
      closeQRResetModal();
    } else if (qrOptionsModalOverlay?.classList.contains('show')) {
      closeQROptionsModal();
    } else if (createUserModalOverlay?.classList.contains('show')) {
      closeCreateUserModal();
    } else if (userCredentialsModalOverlay?.classList.contains('show')) {
      closeCredentialsModal();
    }
  }
});


const openCreateUserBtn = document.getElementById('openCreateUserBtn');
const createUserModalOverlay = document.getElementById('createUserModalOverlay');
const createUserModal = document.getElementById('createUserModal');
const createUserModalClose = document.getElementById('createUserModalClose');
const createUserCancelBtn = document.getElementById('createUserCancelBtn');
const createUserSubmitBtn = document.getElementById('createUserSubmitBtn');
const createUserEmail = document.getElementById('createUserEmail');
const createUserPassword = document.getElementById('createUserPassword');
const createUserPasswordConfirm = document.getElementById('createUserPasswordConfirm');
const createUserError = document.getElementById('createUserError');

const userCredentialsModalOverlay = document.getElementById('userCredentialsModalOverlay');
const userCredentialsModal = document.getElementById('userCredentialsModal');
const credentialsModalClose = document.getElementById('credentialsModalClose');
const credentialsUserEmail = document.getElementById('credentialsUserEmail');
const credentialsQRCode = document.getElementById('credentialsQRCode');
const credentialsSecret = document.getElementById('credentialsSecret');
const credentialsRecoveryCodes = document.getElementById('credentialsRecoveryCodes');
const downloadCredentialsBtn = document.getElementById('downloadCredentialsBtn');
const credentialsDoneBtn = document.getElementById('credentialsDoneBtn');

let lastCreatedUserCredentials = null;

function openCreateUserModal() {
  if (!createUserModalOverlay) return;
  if (createUserEmail) createUserEmail.value = '';
  if (createUserPassword) createUserPassword.value = '';
  if (createUserPasswordConfirm) createUserPasswordConfirm.value = '';
  if (createUserError) {
    createUserError.style.display = 'none';
    createUserError.textContent = '';
  }
  createUserModalOverlay.setAttribute('aria-hidden', 'false');
  createUserModalOverlay.classList.add('show');
  setTimeout(() => createUserEmail?.focus(), 100);
}

function closeCreateUserModal() {
  if (!createUserModalOverlay) return;
  createUserModalOverlay.setAttribute('aria-hidden', 'true');
  createUserModalOverlay.classList.remove('show');
}

function showCreateUserError(msg) {
  if (createUserError) {
    createUserError.textContent = msg;
    createUserError.style.display = 'block';
  }
}

async function submitCreateUser() {
  const email = createUserEmail?.value?.trim() || '';
  const password = createUserPassword?.value || '';
  const confirmPassword = createUserPasswordConfirm?.value || '';

  if (!email) {
    showCreateUserError('Email is required');
    return;
  }
  if (!password) {
    showCreateUserError('Password is required');
    return;
  }
  if (password.length < 6) {
    showCreateUserError('Password must be at least 6 characters');
    return;
  }
  if (password !== confirmPassword) {
    showCreateUserError('Passwords do not match');
    return;
  }

  if (createUserError) createUserError.style.display = 'none';

  try {
    if (createUserSubmitBtn) {
      createUserSubmitBtn.disabled = true;
      createUserSubmitBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Creating...';
    }

    const res = await fetch('/api/admin/user/create', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'include',
      body: JSON.stringify({ email, password })
    });

    const data = await res.json();

    if (!res.ok) {
      throw new Error(data.error || 'Failed to create user');
    }

    lastCreatedUserCredentials = data;
    closeCreateUserModal();
    openCredentialsModal(data);

    if (typeof loadAccounts === 'function') {
      loadAccounts();
    }

  } catch (err) {
    console.error('[admin] Create user error:', err);
    showCreateUserError(err.message || 'Failed to create user');
  } finally {
    if (createUserSubmitBtn) {
      createUserSubmitBtn.disabled = false;
      createUserSubmitBtn.innerHTML = '<i class="fa-solid fa-plus"></i> Create User';
    }
  }
}

function openCredentialsModal(data) {
  if (!userCredentialsModalOverlay) return;

  if (credentialsUserEmail) credentialsUserEmail.textContent = data.email;
  if (credentialsQRCode) credentialsQRCode.src = data.qrCodeUrl;
  if (credentialsSecret) credentialsSecret.textContent = data.secret;

  if (credentialsRecoveryCodes) {
    credentialsRecoveryCodes.innerHTML = data.recoveryCodes.map((code, idx) => `
      <div style="background: rgba(0,0,0,0.2); padding: 10px 14px; border-radius: 8px; font-family: 'SF Mono', Monaco, monospace; font-size: 13px; letter-spacing: 2px; text-align: center; color: #10b981;">
        ${idx + 1}. ${code}
      </div>
    `).join('');
  }

  userCredentialsModalOverlay.setAttribute('aria-hidden', 'false');
  userCredentialsModalOverlay.classList.add('show');
}

function closeCredentialsModal() {
  if (!userCredentialsModalOverlay) return;
  userCredentialsModalOverlay.setAttribute('aria-hidden', 'true');
  userCredentialsModalOverlay.classList.remove('show');
  lastCreatedUserCredentials = null;
}

function downloadCredentials() {
  if (!lastCreatedUserCredentials) return;

  const data = lastCreatedUserCredentials;
  const content = `ADPanel User Credentials
========================

Email: ${data.email}

2FA Secret: ${data.secret}

Recovery Codes (each can only be used once):
${data.recoveryCodes.map((code, i) => `  ${i + 1}. ${code}`).join('\n')}

IMPORTANT: Keep these credentials secure!
`;

  const blob = new Blob([content], { type: 'text/plain' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `adpanel-credentials-${data.email.replace('@', '_at_')}.txt`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

if (openCreateUserBtn) openCreateUserBtn.addEventListener('click', openCreateUserModal);
if (createUserModalClose) createUserModalClose.addEventListener('click', closeCreateUserModal);
if (createUserCancelBtn) createUserCancelBtn.addEventListener('click', closeCreateUserModal);
if (createUserSubmitBtn) createUserSubmitBtn.addEventListener('click', submitCreateUser);
if (createUserModalOverlay) {
  createUserModalOverlay.addEventListener('click', (e) => {
    if (e.target === createUserModalOverlay) closeCreateUserModal();
  });
}

if (credentialsModalClose) credentialsModalClose.addEventListener('click', closeCredentialsModal);
if (credentialsDoneBtn) credentialsDoneBtn.addEventListener('click', closeCredentialsModal);
if (downloadCredentialsBtn) downloadCredentialsBtn.addEventListener('click', downloadCredentials);
if (userCredentialsModalOverlay) {
  userCredentialsModalOverlay.addEventListener('click', (e) => {
    if (e.target === userCredentialsModalOverlay) closeCredentialsModal();
  });
}

createUserModal?.querySelectorAll('.password-toggle').forEach(btn => {
  if (btn.dataset.listenerAdded) return;
  btn.dataset.listenerAdded = 'true';
  btn.addEventListener('click', () => {
    const targetId = btn.dataset.target;
    const input = document.getElementById(targetId);
    const icon = btn.querySelector('i');
    if (input && icon) {
      if (input.type === 'password') {
        input.type = 'text';
        icon.classList.replace('fa-eye', 'fa-eye-slash');
      } else {
        input.type = 'password';
        icon.classList.replace('fa-eye-slash', 'fa-eye');
      }
    }
  });
});


const openBrandingBtn = document.getElementById('openBrandingBtn');
const brandingModalOverlay = document.getElementById('brandingModalOverlay');
const brandingModal = document.getElementById('brandingModal');
const brandingModalClose = document.getElementById('brandingModalClose');
const brandingCancelBtn = document.getElementById('brandingCancelBtn');
const brandingSaveBtn = document.getElementById('brandingSaveBtn');
const brandingAppName = document.getElementById('brandingAppName');
const brandingLogoFile = document.getElementById('brandingLogoFile');
const brandingLogoUrl = document.getElementById('brandingLogoUrl');
const brandingLogoSelectBtn = document.getElementById('brandingLogoSelectBtn');
const brandingError = document.getElementById('brandingError');
const logoTabUpload = document.getElementById('logoTabUpload');
const logoTabUrl = document.getElementById('logoTabUrl');
const logoUploadSection = document.getElementById('logoUploadSection');
const logoUrlSection = document.getElementById('logoUrlSection');
const logoPreviewUpload = document.getElementById('logoPreviewUpload');
const logoPreviewImg = document.getElementById('logoPreviewImg');
const logoFileName = document.getElementById('logoFileName');

let selectedLogoFile = null;

function openBrandingModal() {
  if (!brandingModalOverlay) return;

  selectedLogoFile = null;
  if (brandingError) {
    brandingError.style.display = 'none';
    brandingError.textContent = '';
  }
  if (logoPreviewUpload) logoPreviewUpload.style.display = 'none';
  if (brandingLogoFile) brandingLogoFile.value = '';
  if (brandingLogoUrl) brandingLogoUrl.value = '';

  loadCurrentBranding();

  brandingModalOverlay.setAttribute('aria-hidden', 'false');
  brandingModalOverlay.classList.add('show');
}

function closeBrandingModal() {
  if (!brandingModalOverlay) return;
  brandingModalOverlay.setAttribute('aria-hidden', 'true');
  brandingModalOverlay.classList.remove('show');
}

async function loadCurrentBranding() {
  try {
    const res = await fetch('/api/admin/branding', { credentials: 'include' });
    if (res.ok) {
      const data = await res.json();
      if (brandingAppName) brandingAppName.value = data.appName || '';
      if (data.logoUrl && brandingLogoUrl) {
        brandingLogoUrl.value = data.logoUrl;
        switchLogoTab('url');
      }
    }
  } catch (e) {
    console.error('[branding] Failed to load current settings:', e);
  }
}

function switchLogoTab(tab) {
  if (tab === 'upload') {
    if (logoTabUpload) {
      logoTabUpload.style.border = '1px solid rgba(99,102,241,0.5)';
      logoTabUpload.style.background = 'rgba(99,102,241,0.15)';
      logoTabUpload.style.color = '#a5b4fc';
    }
    if (logoTabUrl) {
      logoTabUrl.style.border = '1px solid rgba(255,255,255,0.1)';
      logoTabUrl.style.background = 'rgba(255,255,255,0.03)';
      logoTabUrl.style.color = 'rgba(255,255,255,0.6)';
    }
    if (logoUploadSection) logoUploadSection.style.display = 'block';
    if (logoUrlSection) logoUrlSection.style.display = 'none';
  } else {
    if (logoTabUrl) {
      logoTabUrl.style.border = '1px solid rgba(99,102,241,0.5)';
      logoTabUrl.style.background = 'rgba(99,102,241,0.15)';
      logoTabUrl.style.color = '#a5b4fc';
    }
    if (logoTabUpload) {
      logoTabUpload.style.border = '1px solid rgba(255,255,255,0.1)';
      logoTabUpload.style.background = 'rgba(255,255,255,0.03)';
      logoTabUpload.style.color = 'rgba(255,255,255,0.6)';
    }
    if (logoUploadSection) logoUploadSection.style.display = 'none';
    if (logoUrlSection) logoUrlSection.style.display = 'block';
  }
}

function handleLogoFileSelect(e) {
  const file = e.target.files?.[0];
  if (!file) return;

  selectedLogoFile = file;

  const reader = new FileReader();
  reader.onload = (evt) => {
    if (logoPreviewImg) logoPreviewImg.src = evt.target.result;
    if (logoFileName) logoFileName.textContent = file.name;
    if (logoPreviewUpload) logoPreviewUpload.style.display = 'block';
  };
  reader.readAsDataURL(file);
}

async function saveBranding() {
  const appName = brandingAppName?.value?.trim();

  if (!appName) {
    if (brandingError) {
      brandingError.textContent = 'App name is required';
      brandingError.style.display = 'block';
    }
    return;
  }

  if (brandingError) brandingError.style.display = 'none';

  const body = { appName };

  if (selectedLogoFile) {
    const reader = new FileReader();
    const base64 = await new Promise((resolve) => {
      reader.onload = (e) => resolve(e.target.result);
      reader.readAsDataURL(selectedLogoFile);
    });
    body.logoBase64 = base64;
    body.logoFilename = selectedLogoFile.name;
  } else if (brandingLogoUrl?.value?.trim()) {
    body.logoUrl = brandingLogoUrl.value.trim();
  }

  try {
    if (brandingSaveBtn) {
      brandingSaveBtn.disabled = true;
      brandingSaveBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Saving...';
    }

    const res = await fetch('/api/admin/branding/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'include',
      body: JSON.stringify(body)
    });

    const data = await res.json();

    if (!res.ok) {
      throw new Error(data.error || 'Failed to update branding');
    }

    closeBrandingModal();
    alert('Branding updated successfully! Refresh the page to see changes.');

  } catch (err) {
    console.error('[branding] Save error:', err);
    if (brandingError) {
      brandingError.textContent = err.message || 'Failed to save branding';
      brandingError.style.display = 'block';
    }
  } finally {
    if (brandingSaveBtn) {
      brandingSaveBtn.disabled = false;
      brandingSaveBtn.innerHTML = '<i class="fa-solid fa-check"></i> Save Changes';
    }
  }
}

if (openBrandingBtn) openBrandingBtn.addEventListener('click', openBrandingModal);
if (brandingModalClose) brandingModalClose.addEventListener('click', closeBrandingModal);
if (brandingCancelBtn) brandingCancelBtn.addEventListener('click', closeBrandingModal);
if (brandingSaveBtn) brandingSaveBtn.addEventListener('click', saveBranding);
if (brandingModalOverlay) {
  brandingModalOverlay.addEventListener('click', (e) => {
    if (e.target === brandingModalOverlay) closeBrandingModal();
  });
}

if (logoTabUpload) logoTabUpload.addEventListener('click', () => switchLogoTab('upload'));
if (logoTabUrl) logoTabUrl.addEventListener('click', () => switchLogoTab('url'));

if (brandingLogoSelectBtn) brandingLogoSelectBtn.addEventListener('click', () => brandingLogoFile?.click());
if (brandingLogoFile) brandingLogoFile.addEventListener('change', handleLogoFileSelect);

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && brandingModalOverlay?.classList.contains('show')) {
    closeBrandingModal();
  }
});


const DEFAULT_LOGIN_WATERMARK_URL = 'https://stalwart-pegasus-2c2ca4.netlify.app/watermark.webp';
const openLoginWatermarkBtn = document.getElementById('openLoginWatermarkBtn');
const loginWatermarkModalOverlay = document.getElementById('loginWatermarkModalOverlay');
const loginWatermarkModalClose = document.getElementById('loginWatermarkModalClose');
const loginWatermarkCancelBtn = document.getElementById('loginWatermarkCancelBtn');
const loginWatermarkSaveBtn = document.getElementById('loginWatermarkSaveBtn');
const loginWatermarkTabUpload = document.getElementById('loginWatermarkTabUpload');
const loginWatermarkTabUrl = document.getElementById('loginWatermarkTabUrl');
const loginWatermarkUploadSection = document.getElementById('loginWatermarkUploadSection');
const loginWatermarkUrlSection = document.getElementById('loginWatermarkUrlSection');
const loginWatermarkFile = document.getElementById('loginWatermarkFile');
const loginWatermarkSelectBtn = document.getElementById('loginWatermarkSelectBtn');
const loginWatermarkUrlInput = document.getElementById('loginWatermarkUrl');
const loginWatermarkPreviewImg = document.getElementById('loginWatermarkPreviewImg');
const loginWatermarkPreviewBadge = document.getElementById('loginWatermarkPreviewBadge');
const loginWatermarkPreviewMeta = document.getElementById('loginWatermarkPreviewMeta');
const loginWatermarkFeedback = document.getElementById('loginWatermarkFeedback');
const loginWatermarkUploadMeta = document.getElementById('loginWatermarkUploadMeta');
const initialLoginWatermarkSrc = document.querySelector('[data-login-watermark-image]')?.src || DEFAULT_LOGIN_WATERMARK_URL;

let loginWatermarkSourceMode = 'upload';
let selectedLoginWatermarkFile = null;
let loginWatermarkCurrentConfig = {
  mode: 'url',
  watermarkUrl: initialLoginWatermarkSrc,
  externalUrl: initialLoginWatermarkSrc
};

function setLoginWatermarkFeedback(message, type = 'error') {
  if (!loginWatermarkFeedback) return;
  loginWatermarkFeedback.textContent = message || '';
  loginWatermarkFeedback.className = 'login-watermark-feedback';
  if (!message) {
    loginWatermarkFeedback.style.display = 'none';
    return;
  }
  loginWatermarkFeedback.classList.add(type);
  loginWatermarkFeedback.style.display = 'block';
}

function setLoginWatermarkPreview(src, badgeText, metaText) {
  if (loginWatermarkPreviewImg && src) {
    loginWatermarkPreviewImg.src = src;
  }
  if (loginWatermarkPreviewBadge) {
    loginWatermarkPreviewBadge.textContent = badgeText || 'Current image';
  }
  if (loginWatermarkPreviewMeta) {
    loginWatermarkPreviewMeta.textContent = metaText || 'The login watermark keeps its current hardcoded size.';
  }
}

function updateLoginWatermarkImages(src) {
  if (!src) return;
  document.querySelectorAll('[data-login-watermark-image]').forEach((img) => {
    img.src = src;
  });
}

function showLoginWatermarkToast(message) {
  const toast = document.createElement('div');
  toast.className = 'perm-success-toast';
  toast.innerHTML = '<i class="fa-solid fa-circle-check"></i>' + escapeHtml(message || 'Login watermark updated');
  document.body.appendChild(toast);
  requestAnimationFrame(() => toast.classList.add('show'));
  setTimeout(() => {
    toast.classList.remove('show');
    setTimeout(() => toast.remove(), 300);
  }, 2800);
}

function syncLoginWatermarkPreviewToCurrent() {
  const currentUrl = loginWatermarkCurrentConfig.watermarkUrl || DEFAULT_LOGIN_WATERMARK_URL;
  const currentMode = loginWatermarkCurrentConfig.mode === 'upload' ? 'Uploaded image' : 'Current image';
  const currentMeta = loginWatermarkCurrentConfig.mode === 'upload'
    ? 'The login page is currently using an uploaded image. The watermark size and placement stay exactly the same.'
    : 'The login page is currently using an image link. The watermark size and placement stay exactly the same.';
  setLoginWatermarkPreview(currentUrl, currentMode, currentMeta);
}

function switchLoginWatermarkTab(mode) {
  loginWatermarkSourceMode = mode === 'url' ? 'url' : 'upload';
  if (loginWatermarkTabUpload) loginWatermarkTabUpload.classList.toggle('active', loginWatermarkSourceMode === 'upload');
  if (loginWatermarkTabUrl) loginWatermarkTabUrl.classList.toggle('active', loginWatermarkSourceMode === 'url');
  if (loginWatermarkUploadSection) loginWatermarkUploadSection.style.display = loginWatermarkSourceMode === 'upload' ? 'block' : 'none';
  if (loginWatermarkUrlSection) loginWatermarkUrlSection.style.display = loginWatermarkSourceMode === 'url' ? 'block' : 'none';

  if (loginWatermarkSourceMode === 'upload' && selectedLoginWatermarkFile) {
    if (loginWatermarkUploadMeta) {
      loginWatermarkUploadMeta.textContent = `Ready to upload: ${selectedLoginWatermarkFile.name}`;
    }
    return;
  }

  if (loginWatermarkSourceMode === 'url' && loginWatermarkUrlInput?.value?.trim()) {
    setLoginWatermarkPreview(
      loginWatermarkUrlInput.value.trim(),
      'Link preview',
      'Previewing the image from this link. The login watermark dimensions remain untouched.'
    );
    return;
  }

  syncLoginWatermarkPreviewToCurrent();
}

async function loadCurrentLoginWatermark() {
  try {
    const res = await fetch('/api/admin/login-watermark', { credentials: 'include' });
    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.error || 'Failed to load login watermark');
    }

    loginWatermarkCurrentConfig = {
      mode: data.mode || 'url',
      watermarkUrl: data.watermarkUrl || DEFAULT_LOGIN_WATERMARK_URL,
      externalUrl: data.externalUrl || ''
    };

    if (loginWatermarkUrlInput) {
      loginWatermarkUrlInput.value = data.mode === 'upload' ? '' : (data.externalUrl || data.watermarkUrl || '');
    }

    if (loginWatermarkUploadMeta) {
      loginWatermarkUploadMeta.textContent = data.mode === 'upload'
        ? 'Currently using an uploaded image. Choose another file to replace it.'
        : 'No local file selected yet. You can upload one anytime.';
    }

    switchLoginWatermarkTab(data.mode === 'upload' ? 'upload' : 'url');
    setLoginWatermarkFeedback('');
  } catch (err) {
    console.error('[login-watermark] Failed to load current settings:', err);
    loginWatermarkCurrentConfig = {
      mode: 'url',
      watermarkUrl: initialLoginWatermarkSrc,
      externalUrl: initialLoginWatermarkSrc
    };
    if (loginWatermarkUrlInput) loginWatermarkUrlInput.value = initialLoginWatermarkSrc;
    if (loginWatermarkUploadMeta) loginWatermarkUploadMeta.textContent = 'No local file selected yet. You can upload one anytime.';
    switchLoginWatermarkTab('url');
    setLoginWatermarkFeedback('Could not load the current watermark. Saving a new one still works.', 'error');
  }
}

function openLoginWatermarkModal() {
  if (!loginWatermarkModalOverlay) return;
  selectedLoginWatermarkFile = null;
  if (loginWatermarkFile) loginWatermarkFile.value = '';
  if (loginWatermarkUploadMeta) loginWatermarkUploadMeta.textContent = '';
  setLoginWatermarkFeedback('');
  syncLoginWatermarkPreviewToCurrent();
  loginWatermarkModalOverlay.setAttribute('aria-hidden', 'false');
  loginWatermarkModalOverlay.classList.add('show');
  loadCurrentLoginWatermark();
}

function closeLoginWatermarkModal() {
  if (!loginWatermarkModalOverlay) return;
  loginWatermarkModalOverlay.setAttribute('aria-hidden', 'true');
  loginWatermarkModalOverlay.classList.remove('show');
}

function handleLoginWatermarkFileSelect(e) {
  const file = e.target.files?.[0];
  if (!file) return;

  selectedLoginWatermarkFile = file;
  if (loginWatermarkUploadMeta) {
    loginWatermarkUploadMeta.textContent = `Selected file: ${file.name}`;
  }

  const reader = new FileReader();
  reader.onload = (evt) => {
    setLoginWatermarkPreview(
      evt.target?.result || DEFAULT_LOGIN_WATERMARK_URL,
      'New upload',
      'This file will replace the login watermark image while keeping the current hardcoded dimensions.'
    );
  };
  reader.readAsDataURL(file);
}

function handleLoginWatermarkUrlPreview() {
  const url = loginWatermarkUrlInput?.value?.trim();
  if (!url) {
    syncLoginWatermarkPreviewToCurrent();
    return;
  }

  setLoginWatermarkPreview(
    url,
    'Link preview',
    'Previewing the image from this web link. The login watermark dimensions remain exactly as they are now.'
  );
}

async function saveLoginWatermark() {
  if (!loginWatermarkSaveBtn) return;

  const body = {};
  if (loginWatermarkSourceMode === 'upload' && selectedLoginWatermarkFile) {
    const reader = new FileReader();
    const base64 = await new Promise((resolve) => {
      reader.onload = (event) => resolve(event.target?.result || '');
      reader.readAsDataURL(selectedLoginWatermarkFile);
    });
    body.watermarkBase64 = base64;
    body.watermarkFilename = selectedLoginWatermarkFile.name;
  } else if (loginWatermarkSourceMode === 'url' && loginWatermarkUrlInput?.value?.trim()) {
    body.watermarkUrl = loginWatermarkUrlInput.value.trim();
  }

  try {
    setLoginWatermarkFeedback('');
    loginWatermarkSaveBtn.disabled = true;
    loginWatermarkSaveBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Saving...';

    const res = await fetch('/api/admin/login-watermark', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'include',
      body: JSON.stringify(body)
    });

    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.error || 'Failed to update login watermark');
    }

    loginWatermarkCurrentConfig = {
      mode: data.mode || (loginWatermarkSourceMode === 'upload' ? 'upload' : 'url'),
      watermarkUrl: data.watermarkUrl || DEFAULT_LOGIN_WATERMARK_URL,
      externalUrl: data.externalUrl || ''
    };
    updateLoginWatermarkImages(loginWatermarkCurrentConfig.watermarkUrl);
    closeLoginWatermarkModal();
    showLoginWatermarkToast('Login watermark updated');
  } catch (err) {
    console.error('[login-watermark] Save error:', err);
    setLoginWatermarkFeedback(err.message || 'Failed to save login watermark', 'error');
  } finally {
    loginWatermarkSaveBtn.disabled = false;
    loginWatermarkSaveBtn.innerHTML = '<i class="fa-solid fa-check"></i> Save Watermark';
  }
}

if (openLoginWatermarkBtn) openLoginWatermarkBtn.addEventListener('click', openLoginWatermarkModal);
if (loginWatermarkModalClose) loginWatermarkModalClose.addEventListener('click', closeLoginWatermarkModal);
if (loginWatermarkCancelBtn) loginWatermarkCancelBtn.addEventListener('click', closeLoginWatermarkModal);
if (loginWatermarkSaveBtn) loginWatermarkSaveBtn.addEventListener('click', saveLoginWatermark);
if (loginWatermarkTabUpload) loginWatermarkTabUpload.addEventListener('click', () => switchLoginWatermarkTab('upload'));
if (loginWatermarkTabUrl) loginWatermarkTabUrl.addEventListener('click', () => switchLoginWatermarkTab('url'));
if (loginWatermarkSelectBtn) loginWatermarkSelectBtn.addEventListener('click', () => loginWatermarkFile?.click());
if (loginWatermarkFile) loginWatermarkFile.addEventListener('change', handleLoginWatermarkFileSelect);
if (loginWatermarkUrlInput) {
  loginWatermarkUrlInput.addEventListener('input', () => {
    if (loginWatermarkSourceMode === 'url') handleLoginWatermarkUrlPreview();
  });
  loginWatermarkUrlInput.addEventListener('change', handleLoginWatermarkUrlPreview);
}
if (loginWatermarkModalOverlay) {
  loginWatermarkModalOverlay.addEventListener('click', (e) => {
    if (e.target === loginWatermarkModalOverlay) closeLoginWatermarkModal();
  });
}
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && loginWatermarkModalOverlay?.classList.contains('show')) {
    closeLoginWatermarkModal();
  }
});


const DEFAULT_LOGIN_BACKGROUND_URL = '/images/bgvid.webm';
const openLoginBackgroundBtn = document.getElementById('openLoginBackgroundBtn');
const loginBackgroundModalOverlay = document.getElementById('loginBackgroundModalOverlay');
const loginBackgroundModalClose = document.getElementById('loginBackgroundModalClose');
const loginBackgroundCancelBtn = document.getElementById('loginBackgroundCancelBtn');
const loginBackgroundSaveBtn = document.getElementById('loginBackgroundSaveBtn');
const loginBackgroundTabUpload = document.getElementById('loginBackgroundTabUpload');
const loginBackgroundTabUrl = document.getElementById('loginBackgroundTabUrl');
const loginBackgroundUploadSection = document.getElementById('loginBackgroundUploadSection');
const loginBackgroundUrlSection = document.getElementById('loginBackgroundUrlSection');
const loginBackgroundFile = document.getElementById('loginBackgroundFile');
const loginBackgroundSelectBtn = document.getElementById('loginBackgroundSelectBtn');
const loginBackgroundUrlInput = document.getElementById('loginBackgroundUrl');
const loginBackgroundPreviewImg = document.getElementById('loginBackgroundPreviewImg');
const loginBackgroundPreviewVideo = document.getElementById('loginBackgroundPreviewVideo');
const loginBackgroundPreviewBadge = document.getElementById('loginBackgroundPreviewBadge');
const loginBackgroundPreviewMeta = document.getElementById('loginBackgroundPreviewMeta');
const loginBackgroundFeedback = document.getElementById('loginBackgroundFeedback');
const loginBackgroundUploadMeta = document.getElementById('loginBackgroundUploadMeta');

const initialLoginBackgroundType = loginBackgroundPreviewImg?.classList.contains('is-visible') ? 'image' : 'video';
const initialLoginBackgroundSrc = initialLoginBackgroundType === 'image'
  ? (loginBackgroundPreviewImg?.currentSrc || loginBackgroundPreviewImg?.src || DEFAULT_LOGIN_BACKGROUND_URL)
  : (loginBackgroundPreviewVideo?.currentSrc || loginBackgroundPreviewVideo?.src || DEFAULT_LOGIN_BACKGROUND_URL);

let loginBackgroundSourceMode = 'upload';
let selectedLoginBackgroundFile = null;
let loginBackgroundPreviewObjectUrl = null;
let loginBackgroundCurrentConfig = {
  mode: 'upload',
  mediaType: initialLoginBackgroundType,
  backgroundUrl: initialLoginBackgroundSrc,
  externalUrl: '',
  mimeType: initialLoginBackgroundType === 'video' ? 'video/webm' : 'image/webp'
};

function setLoginBackgroundFeedback(message, type = 'error') {
  if (!loginBackgroundFeedback) return;
  loginBackgroundFeedback.textContent = message || '';
  loginBackgroundFeedback.className = 'login-watermark-feedback';
  if (!message) {
    loginBackgroundFeedback.style.display = 'none';
    return;
  }
  loginBackgroundFeedback.classList.add(type);
  loginBackgroundFeedback.style.display = 'block';
}

function clearLoginBackgroundPreviewObjectUrl() {
  if (!loginBackgroundPreviewObjectUrl) return;
  try {
    URL.revokeObjectURL(loginBackgroundPreviewObjectUrl);
  } catch (err) {
    console.warn('[login-background] Failed to revoke preview URL:', err);
  }
  loginBackgroundPreviewObjectUrl = null;
}

function safePlayLoginBackgroundPreview() {
  if (!loginBackgroundPreviewVideo) return;
  const playPromise = loginBackgroundPreviewVideo.play();
  if (playPromise && typeof playPromise.then === 'function') {
    playPromise.catch(() => { });
  }
}

function getLoginBackgroundExtension(value) {
  try {
    const pathname = new URL(value, window.location.origin).pathname.toLowerCase();
    if (!pathname.includes('.')) return '';
    return pathname.slice(pathname.lastIndexOf('.') + 1).replace(/[^a-z0-9]/g, '');
  } catch (err) {
    return '';
  }
}

function detectLoginBackgroundTypeFromUrl(value) {
  const ext = getLoginBackgroundExtension(value);
  if (['png', 'jpg', 'jpeg', 'webp', 'ico', 'svg'].includes(ext)) return 'image';
  if (['webm', 'mp4', 'ogg', 'ogv'].includes(ext)) return 'video';
  return null;
}

function detectLoginBackgroundTypeFromFile(file) {
  const fileType = String(file?.type || '').toLowerCase();
  if (fileType.startsWith('image/')) return 'image';
  if (fileType.startsWith('video/')) return 'video';
  return detectLoginBackgroundTypeFromUrl(file?.name || '');
}

function getLoginBackgroundMimeType(value, mediaType) {
  const ext = getLoginBackgroundExtension(value);
  if (mediaType === 'image') {
    if (ext === 'png') return 'image/png';
    if (ext === 'jpg' || ext === 'jpeg') return 'image/jpeg';
    if (ext === 'webp') return 'image/webp';
    if (ext === 'ico') return 'image/x-icon';
    if (ext === 'svg') return 'image/svg+xml';
    return 'image/webp';
  }

  if (ext === 'mp4') return 'video/mp4';
  if (ext === 'ogg' || ext === 'ogv') return 'video/ogg';
  return 'video/webm';
}

function setLoginBackgroundPreview(src, mediaType, badgeText, metaText, mimeType) {
  if (!loginBackgroundPreviewImg || !loginBackgroundPreviewVideo) return;

  loginBackgroundPreviewImg.classList.toggle('is-visible', mediaType === 'image');
  loginBackgroundPreviewVideo.classList.toggle('is-visible', mediaType === 'video');

  if (mediaType === 'image') {
    loginBackgroundPreviewVideo.pause();
    loginBackgroundPreviewVideo.removeAttribute('src');
    loginBackgroundPreviewVideo.load();
    loginBackgroundPreviewImg.src = src;
  } else {
    loginBackgroundPreviewImg.removeAttribute('src');
    loginBackgroundPreviewVideo.src = src;
    if (mimeType) loginBackgroundPreviewVideo.dataset.mimeType = mimeType;
    loginBackgroundPreviewVideo.load();
    safePlayLoginBackgroundPreview();
  }

  if (loginBackgroundPreviewBadge) {
    loginBackgroundPreviewBadge.textContent = badgeText || (mediaType === 'image' ? 'Image' : 'Video');
  }
  if (loginBackgroundPreviewMeta) {
    loginBackgroundPreviewMeta.textContent = metaText || `Current ${mediaType}`;
  }
}

function showLoginBackgroundToast(message) {
  const toast = document.createElement('div');
  toast.className = 'perm-success-toast';
  toast.innerHTML = '<i class="fa-solid fa-circle-check"></i>' + escapeHtml(message || 'Login background updated');
  document.body.appendChild(toast);
  requestAnimationFrame(() => toast.classList.add('show'));
  setTimeout(() => {
    toast.classList.remove('show');
    setTimeout(() => toast.remove(), 300);
  }, 2800);
}

function syncLoginBackgroundPreviewToCurrent() {
  setLoginBackgroundPreview(
    loginBackgroundCurrentConfig.backgroundUrl || DEFAULT_LOGIN_BACKGROUND_URL,
    loginBackgroundCurrentConfig.mediaType || 'video',
    (loginBackgroundCurrentConfig.mediaType || 'video') === 'image' ? 'Image' : 'Video',
    `Current ${(loginBackgroundCurrentConfig.mediaType || 'video') === 'image' ? 'image' : 'video'}`,
    loginBackgroundCurrentConfig.mimeType || 'video/webm'
  );
}

function switchLoginBackgroundTab(mode) {
  loginBackgroundSourceMode = mode === 'url' ? 'url' : 'upload';
  if (loginBackgroundTabUpload) loginBackgroundTabUpload.classList.toggle('active', loginBackgroundSourceMode === 'upload');
  if (loginBackgroundTabUrl) loginBackgroundTabUrl.classList.toggle('active', loginBackgroundSourceMode === 'url');
  if (loginBackgroundUploadSection) loginBackgroundUploadSection.style.display = loginBackgroundSourceMode === 'upload' ? 'block' : 'none';
  if (loginBackgroundUrlSection) loginBackgroundUrlSection.style.display = loginBackgroundSourceMode === 'url' ? 'block' : 'none';

  if (loginBackgroundSourceMode === 'upload' && selectedLoginBackgroundFile) {
    const mediaType = detectLoginBackgroundTypeFromFile(selectedLoginBackgroundFile) || 'video';
    if (loginBackgroundUploadMeta) {
      const mediaLabel = mediaType === 'image' ? 'Image' : 'Video';
      loginBackgroundUploadMeta.textContent = `${mediaLabel} ready: ${selectedLoginBackgroundFile.name}`;
    }
    if (loginBackgroundPreviewObjectUrl) {
      setLoginBackgroundPreview(
        loginBackgroundPreviewObjectUrl,
        mediaType,
        mediaType === 'image' ? 'Image' : 'Video',
        `New ${mediaType}`,
        selectedLoginBackgroundFile.type || getLoginBackgroundMimeType(selectedLoginBackgroundFile.name, mediaType)
      );
    }
    return;
  }

  if (loginBackgroundSourceMode === 'url' && loginBackgroundUrlInput?.value?.trim()) {
    handleLoginBackgroundUrlPreview();
    return;
  }

  setLoginBackgroundFeedback('');
  syncLoginBackgroundPreviewToCurrent();
}

async function loadCurrentLoginBackground() {
  try {
    const res = await fetch('/api/admin/login-background', { credentials: 'include' });
    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.error || 'Failed to load login background');
    }

    loginBackgroundCurrentConfig = {
      mode: data.mode || 'upload',
      mediaType: data.mediaType || 'video',
      backgroundUrl: data.backgroundUrl || DEFAULT_LOGIN_BACKGROUND_URL,
      externalUrl: data.externalUrl || '',
      mimeType: data.mimeType || 'video/webm'
    };

    if (loginBackgroundUrlInput) {
      loginBackgroundUrlInput.value = data.mode === 'url' ? (data.externalUrl || data.backgroundUrl || '') : '';
    }
    if (loginBackgroundUploadMeta) {
      loginBackgroundUploadMeta.textContent = data.mode === 'upload'
        ? `${data.mediaType === 'image' ? 'Image' : 'Video'} active`
        : 'Ready for a local upload';
    }

    switchLoginBackgroundTab(data.mode || 'upload');
    setLoginBackgroundFeedback('');
    syncLoginBackgroundPreviewToCurrent();
  } catch (err) {
    console.error('[login-background] Failed to load current settings:', err);
    loginBackgroundCurrentConfig = {
      mode: 'upload',
      mediaType: initialLoginBackgroundType,
      backgroundUrl: initialLoginBackgroundSrc,
      externalUrl: '',
      mimeType: initialLoginBackgroundType === 'video' ? 'video/webm' : 'image/webp'
    };
    switchLoginBackgroundTab('upload');
    setLoginBackgroundFeedback('Could not load the current background.', 'error');
    syncLoginBackgroundPreviewToCurrent();
  }
}

function openLoginBackgroundModal() {
  if (!loginBackgroundModalOverlay) return;
  selectedLoginBackgroundFile = null;
  clearLoginBackgroundPreviewObjectUrl();
  if (loginBackgroundFile) loginBackgroundFile.value = '';
  if (loginBackgroundUploadMeta) loginBackgroundUploadMeta.textContent = '';
  setLoginBackgroundFeedback('');
  syncLoginBackgroundPreviewToCurrent();
  loginBackgroundModalOverlay.setAttribute('aria-hidden', 'false');
  loginBackgroundModalOverlay.classList.add('show');
  loadCurrentLoginBackground();
}

function closeLoginBackgroundModal() {
  if (!loginBackgroundModalOverlay) return;
  if (loginBackgroundPreviewVideo) {
    loginBackgroundPreviewVideo.pause();
  }
  clearLoginBackgroundPreviewObjectUrl();
  loginBackgroundModalOverlay.setAttribute('aria-hidden', 'true');
  loginBackgroundModalOverlay.classList.remove('show');
}

function handleLoginBackgroundFileSelect(e) {
  const file = e.target.files?.[0];
  if (!file) return;

  const mediaType = detectLoginBackgroundTypeFromFile(file);
  if (!mediaType) {
    setLoginBackgroundFeedback('Choose a valid image or video file.', 'error');
    if (loginBackgroundFile) loginBackgroundFile.value = '';
    return;
  }

  const maxBytes = mediaType === 'image' ? 5 * 1024 * 1024 : 40 * 1024 * 1024;
  if (file.size > maxBytes) {
    setLoginBackgroundFeedback(mediaType === 'image' ? 'Image must be under 5MB.' : 'Video must be under 40MB.', 'error');
    if (loginBackgroundFile) loginBackgroundFile.value = '';
    return;
  }

  clearLoginBackgroundPreviewObjectUrl();
  loginBackgroundPreviewObjectUrl = URL.createObjectURL(file);
  selectedLoginBackgroundFile = file;

  if (loginBackgroundUploadMeta) {
    loginBackgroundUploadMeta.textContent = `${mediaType === 'image' ? 'Image' : 'Video'} selected: ${file.name}`;
  }

  setLoginBackgroundFeedback('');
  setLoginBackgroundPreview(
    loginBackgroundPreviewObjectUrl,
    mediaType,
    mediaType === 'image' ? 'Image' : 'Video',
    `New ${mediaType}`,
    file.type || getLoginBackgroundMimeType(file.name, mediaType)
  );
}

function handleLoginBackgroundUrlPreview() {
  const url = loginBackgroundUrlInput?.value?.trim();
  if (!url) {
    setLoginBackgroundFeedback('');
    syncLoginBackgroundPreviewToCurrent();
    return;
  }

  const mediaType = detectLoginBackgroundTypeFromUrl(url);
  if (!mediaType) {
    setLoginBackgroundFeedback('Use a direct image or video URL.', 'error');
    return;
  }

  setLoginBackgroundFeedback('');
  setLoginBackgroundPreview(
    url,
    mediaType,
    mediaType === 'image' ? 'Image' : 'Video',
    `${mediaType === 'image' ? 'Image' : 'Video'} from web`,
    getLoginBackgroundMimeType(url, mediaType)
  );
}

async function saveLoginBackground() {
  if (!loginBackgroundSaveBtn) return;

  const body = {};
  if (loginBackgroundSourceMode === 'upload') {
    if (!selectedLoginBackgroundFile) {
      setLoginBackgroundFeedback('Choose an image or video first.', 'error');
      return;
    }

    const reader = new FileReader();
    const base64 = await new Promise((resolve) => {
      reader.onload = (event) => resolve(event.target?.result || '');
      reader.readAsDataURL(selectedLoginBackgroundFile);
    });
    body.backgroundBase64 = base64;
    body.backgroundFilename = selectedLoginBackgroundFile.name;
  } else {
    const url = loginBackgroundUrlInput?.value?.trim();
    if (!url) {
      setLoginBackgroundFeedback('Paste a media URL first.', 'error');
      return;
    }

    const mediaType = detectLoginBackgroundTypeFromUrl(url);
    if (!mediaType) {
      setLoginBackgroundFeedback('Use a direct image or video URL.', 'error');
      return;
    }
    body.backgroundUrl = url;
  }

  try {
    setLoginBackgroundFeedback('');
    loginBackgroundSaveBtn.disabled = true;
    loginBackgroundSaveBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Saving...';

    const res = await fetch('/api/admin/login-background', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'include',
      body: JSON.stringify(body)
    });

    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.error || 'Failed to update login background');
    }

    loginBackgroundCurrentConfig = {
      mode: data.mode || loginBackgroundSourceMode,
      mediaType: data.mediaType || 'video',
      backgroundUrl: data.backgroundUrl || DEFAULT_LOGIN_BACKGROUND_URL,
      externalUrl: data.externalUrl || '',
      mimeType: data.mimeType || 'video/webm'
    };

    closeLoginBackgroundModal();
    showLoginBackgroundToast('Login background updated');
  } catch (err) {
    console.error('[login-background] Save error:', err);
    setLoginBackgroundFeedback(err.message || 'Failed to save login background', 'error');
  } finally {
    loginBackgroundSaveBtn.disabled = false;
    loginBackgroundSaveBtn.innerHTML = '<i class="fa-solid fa-check"></i> Save Background';
  }
}

if (openLoginBackgroundBtn) openLoginBackgroundBtn.addEventListener('click', openLoginBackgroundModal);
if (loginBackgroundModalClose) loginBackgroundModalClose.addEventListener('click', closeLoginBackgroundModal);
if (loginBackgroundCancelBtn) loginBackgroundCancelBtn.addEventListener('click', closeLoginBackgroundModal);
if (loginBackgroundSaveBtn) loginBackgroundSaveBtn.addEventListener('click', saveLoginBackground);
if (loginBackgroundTabUpload) loginBackgroundTabUpload.addEventListener('click', () => switchLoginBackgroundTab('upload'));
if (loginBackgroundTabUrl) loginBackgroundTabUrl.addEventListener('click', () => switchLoginBackgroundTab('url'));
if (loginBackgroundSelectBtn) loginBackgroundSelectBtn.addEventListener('click', () => loginBackgroundFile?.click());
if (loginBackgroundFile) loginBackgroundFile.addEventListener('change', handleLoginBackgroundFileSelect);
if (loginBackgroundUrlInput) {
  loginBackgroundUrlInput.addEventListener('input', () => {
    if (loginBackgroundSourceMode === 'url') handleLoginBackgroundUrlPreview();
  });
  loginBackgroundUrlInput.addEventListener('change', handleLoginBackgroundUrlPreview);
}
if (loginBackgroundModalOverlay) {
  loginBackgroundModalOverlay.addEventListener('click', (e) => {
    if (e.target === loginBackgroundModalOverlay) closeLoginBackgroundModal();
  });
}
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && loginBackgroundModalOverlay?.classList.contains('show')) {
    closeLoginBackgroundModal();
  }
});

const nodeStatsModalOverlay = document.getElementById('nodeStatsModalOverlay');
const statsModalClose = document.getElementById('statsModalClose');
const statsModalBody = document.getElementById('statsModalBody');
const statsNodeNameEl = document.getElementById('statsNodeName');
const statsImage = document.getElementById('nodeStatsImage');

if (statsImage) {
  statsImage.addEventListener('error', () => {
    if (!statsImage.src.includes('server.webp')) {
      statsImage.src = '/images/server.webp';
    }
  });
  if (statsImage.complete && statsImage.naturalHeight === 0) {
    statsImage.src = '/images/server.webp';
  }
}

let statsPollingInterval = null;
let currentStatsNodeId = null;

function openNodeStatsModal(nodeId, nodeName) {
  if (!nodeStatsModalOverlay) return;
  currentStatsNodeId = nodeId;
  if (statsNodeNameEl) statsNodeNameEl.textContent = 'Node name: ' + (nodeName || 'Loading...');
  nodeStatsModalOverlay.classList.add('show');
  document.body.style.overflow = 'hidden';

  statsModalBody.innerHTML = `
    <div class="stats-loading">
      <div class="stats-spinner"></div>
      <span>Loading statistics...</span>
    </div>
  `;

  fetchAndRenderStats(nodeId);

  statsPollingInterval = setInterval(() => {
    if (currentStatsNodeId) fetchAndRenderStats(currentStatsNodeId);
  }, 5000);
}

function closeNodeStatsModal() {
  if (!nodeStatsModalOverlay) return;
  nodeStatsModalOverlay.classList.remove('show');
  document.body.style.overflow = '';

  if (statsPollingInterval) {
    clearInterval(statsPollingInterval);
    statsPollingInterval = null;
  }
  currentStatsNodeId = null;
}

async function fetchAndRenderStats(nodeId) {
  try {
    const res = await fetch(`/api/admin/nodes/${encodeURIComponent(nodeId)}/stats`);
    const data = await res.json();

    if (!data.ok || !data.online) {
      renderOfflineStats(data.nodeName || nodeId, data.error);
      return;
    }

    renderStats(data);
  } catch (err) {
    console.error('[stats] Fetch error:', err);
    renderOfflineStats(currentStatsNodeId, 'Connection error');
  }
}

function renderOfflineStats(nodeName, error) {
  statsNodeNameEl.textContent = 'Node name: ' + (nodeName || 'Node');
  statsModalBody.innerHTML = `
    <div class="stats-offline">
      <div class="stats-offline-icon">
        <i class="fa-solid fa-plug-circle-xmark"></i>
      </div>
      <div class="stats-offline-text">Node Offline</div>
      <div class="stats-offline-desc">${escapeHtml(error || 'Unable to connect to this node')}</div>
    </div>
  `;
}

function renderStats(data) {
  const s = data.stats;
  statsNodeNameEl.textContent = 'Node name: ' + (data.nodeName || 'Node');

  const cpuClass = s.cpu.percent > 90 ? 'warning' : '';
  const ramClass = s.ram.freeMb < 100 ? 'critical' : s.ram.percent > 90 ? 'warning' : '';
  const diskClass = s.disk.freeGb < 1 ? 'critical' : s.disk.percent > 90 ? 'warning' : '';

  let html = '';

  if (data.warnings && data.warnings.length > 0) {
    html += '<div class="stats-warnings">';
    data.warnings.forEach(w => {
      const icon = w.level === 'critical' ? 'fa-triangle-exclamation' : 'fa-circle-exclamation';
      html += `
        <div class="stats-warning-item ${w.level}">
          <i class="fa-solid ${icon} stats-warning-icon"></i>
          <span>${escapeHtml(w.message)}</span>
        </div>
      `;
    });
    html += '</div>';
  }

  html += `
    <div class="stats-card">
      <div class="stats-card-header">
        <div class="stats-card-title">
          <div class="stats-card-icon cpu"><i class="fa-solid fa-microchip"></i></div>
          CPU Usage
        </div>
        <div class="stats-card-value ${cpuClass}">${s.cpu.percent}%</div>
      </div>
      <div class="stats-progress-container">
        <div class="stats-progress-bar">
          <div class="stats-progress-fill cpu ${cpuClass}" style="width: ${Math.min(s.cpu.percent, 100)}%"></div>
        </div>
        <div class="stats-progress-labels">
          <span>0%</span>
          <span>100%</span>
        </div>
      </div>
      <div class="stats-details">
        <div class="stats-detail-item">
          <span class="stats-detail-label">Cores</span>
          <span class="stats-detail-value">${s.cpu.cores || 'N/A'}</span>
        </div>
      </div>
    </div>
  `;

  html += `
    <div class="stats-card">
      <div class="stats-card-header">
        <div class="stats-card-title">
          <div class="stats-card-icon ram"><i class="fa-solid fa-memory"></i></div>
          Memory Usage
        </div>
        <div class="stats-card-value ${ramClass}">${s.ram.percent}%</div>
      </div>
      <div class="stats-progress-container">
        <div class="stats-progress-bar">
          <div class="stats-progress-fill ram ${ramClass}" style="width: ${Math.min(s.ram.percent, 100)}%"></div>
        </div>
        <div class="stats-progress-labels">
          <span>${formatBytes(s.ram.usedMb * 1024 * 1024)} used</span>
          <span>${formatBytes(s.ram.totalMb * 1024 * 1024)} total</span>
        </div>
      </div>
      <div class="stats-details">
        <div class="stats-detail-item">
          <span class="stats-detail-label">Used</span>
          <span class="stats-detail-value">${(s.ram.usedMb / 1024).toFixed(1)} GB</span>
        </div>
        <div class="stats-detail-item">
          <span class="stats-detail-label">Free</span>
          <span class="stats-detail-value ${ramClass}">${(s.ram.freeMb / 1024).toFixed(1)} GB</span>
        </div>
      </div>
    </div>
  `;

  html += `
    <div class="stats-card">
      <div class="stats-card-header">
        <div class="stats-card-title">
          <div class="stats-card-icon disk"><i class="fa-solid fa-hard-drive"></i></div>
          Disk Usage
        </div>
        <div class="stats-card-value ${diskClass}">${s.disk.percent}%</div>
      </div>
      <div class="stats-progress-container">
        <div class="stats-progress-bar">
          <div class="stats-progress-fill disk ${diskClass}" style="width: ${Math.min(s.disk.percent, 100)}%"></div>
        </div>
        <div class="stats-progress-labels">
          <span>${s.disk.usedGb} GB used</span>
          <span>${s.disk.totalGb} GB total</span>
        </div>
      </div>
      <div class="stats-details">
        <div class="stats-detail-item">
          <span class="stats-detail-label">Used</span>
          <span class="stats-detail-value">${s.disk.usedGb} GB</span>
        </div>
        <div class="stats-detail-item">
          <span class="stats-detail-label">Free</span>
          <span class="stats-detail-value ${diskClass}">${s.disk.freeGb} GB</span>
        </div>
      </div>
    </div>
  `;

  html += `
    <div class="stats-card">
      <div class="stats-card-header">
        <div class="stats-card-title">
          <div class="stats-card-icon info"><i class="fa-solid fa-server"></i></div>
          System Information
        </div>
      </div>
      <div class="stats-info-grid">
        <div class="stats-info-item">
          <span class="stats-info-label">Hostname</span>
          <span class="stats-info-value">${escapeHtml(s.hostname || 'N/A')}</span>
        </div>
        <div class="stats-info-item">
          <span class="stats-info-label">OS</span>
          <span class="stats-info-value">${escapeHtml(s.os || 'N/A')}</span>
        </div>
        <div class="stats-info-item">
          <span class="stats-info-label">Uptime</span>
          <span class="stats-info-value">${formatUptime(s.uptime)}</span>
        </div>
        <div class="stats-info-item">
          <span class="stats-info-label">Status</span>
          <span class="stats-info-value" style="color: #34d399;">● Online</span>
        </div>
      </div>
    </div>
  `;

  html += `
    <div class="stats-last-updated">
      <i class="fa-solid fa-clock"></i> Last updated: ${new Date().toLocaleTimeString()} • Auto-refreshing every 5 seconds
    </div>
  `;

  statsModalBody.innerHTML = html;
}

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function formatUptime(seconds) {
  if (!seconds || seconds <= 0) return 'N/A';
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const mins = Math.floor((seconds % 3600) / 60);
  if (days > 0) return `${days}d ${hours}h ${mins}m`;
  if (hours > 0) return `${hours}h ${mins}m`;
  return `${mins}m`;
}

if (statsModalClose) statsModalClose.addEventListener('click', closeNodeStatsModal);
if (nodeStatsModalOverlay) {
  nodeStatsModalOverlay.addEventListener('click', (e) => {
    if (e.target === nodeStatsModalOverlay) closeNodeStatsModal();
  });
}

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && nodeStatsModalOverlay?.classList.contains('show')) {
    closeNodeStatsModal();
  }
});

document.addEventListener('DOMContentLoaded', () => {
  const openAlertModalBtn = document.getElementById('openAlertModalBtn');
  const alertModal = document.getElementById('alertModal');
  const closeAlertModal = document.getElementById('closeAlertModal');
  const closeAlertModalFooter = document.getElementById('closeAlertModalFooter');
  const addAlertBtn = document.getElementById('addAlertBtn');
  const alertContent = document.getElementById('alertContent');
  const alertDate = document.getElementById('alertDate');
  const alertEndDate = document.getElementById('alertEndDate');
  const alertNeverEnds = document.getElementById('alertNeverEnds');
  const alertsList = document.getElementById('alertsList');

  if (openAlertModalBtn) {
    console.log('Setup alert listeners, button:', openAlertModalBtn, 'modal:', alertModal);

    function formatAlertDate(value) {
      if (!value) return 'Not set';
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return 'Invalid date';
      return date.toLocaleString([], {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
      });
    }

    function parseLocalDateTimeInput(value) {
      if (!value) return null;
      const parts = value.split('T');
      if (parts.length !== 2) return null;
      const [y, m, d] = parts[0].split('-').map(Number);
      const [h, min] = parts[1].split(':').map(Number);
      if ([y, m, d, h, min].some((num) => Number.isNaN(num))) return null;
      const localDate = new Date(y, m - 1, d, h, min);
      if (Number.isNaN(localDate.getTime())) return null;
      return localDate;
    }

    function getAlertStatusMeta(alert) {
      const now = Date.now();
      const startAt = alert?.date ? new Date(alert.date).getTime() : 0;
      const endAt = alert?.neverEnds || !alert?.endDate ? null : new Date(alert.endDate).getTime();

      if (Number.isFinite(startAt) && startAt > now) {
        return {
          label: 'Scheduled',
          style: 'color:#fbbf24;font-size:10px;margin-left:6px;background:rgba(251,191,36,0.1);padding:2px 6px;border-radius:4px;'
        };
      }

      if (endAt !== null && Number.isFinite(endAt) && endAt <= now) {
        return {
          label: 'Ended',
          style: 'color:#f87171;font-size:10px;margin-left:6px;background:rgba(248,113,113,0.1);padding:2px 6px;border-radius:4px;'
        };
      }

      return {
        label: 'Active',
        style: 'color:#4ade80;font-size:10px;margin-left:6px;background:rgba(74,222,128,0.1);padding:2px 6px;border-radius:4px;'
      };
    }

    function syncAlertEndDateState() {
      if (!alertEndDate || !alertNeverEnds) return;
      const keepActive = !!alertNeverEnds.checked;
      alertEndDate.disabled = keepActive;
      alertEndDate.required = !keepActive;
      alertEndDate.style.opacity = keepActive ? '0.55' : '1';
    }

    function toggleAlertModal(show) {
      if (show) {
        alertModal.setAttribute('aria-hidden', 'false');
        alertModal.classList.add('show');
        const inner = alertModal.querySelector('.enterprise-modal');
        if (inner) setTimeout(() => inner.classList.add('show'), 10);
        if (alertNeverEnds) alertNeverEnds.checked = true;
        syncAlertEndDateState();
        loadAlerts();
      } else {
        alertModal.setAttribute('aria-hidden', 'true');
        const inner = alertModal.querySelector('.enterprise-modal');
        if (inner) inner.classList.remove('show');
        setTimeout(() => alertModal.classList.remove('show'), inner ? 200 : 0);
      }
    }

    function loadAlerts() {
      if (!alertsList) return;
      alertsList.innerHTML = '<div style="padding:10px;text-align:center;color:#888">Loading...</div>';
      fetch('/api/settings/alert')
        .then(res => res.json())
        .then(data => {
          if (!Array.isArray(data) || data.length === 0) {
            alertsList.innerHTML = '<div style="padding:10px;text-align:center;color:#888">No alerts found.</div>';
            return;
          }
          alertsList.innerHTML = '';
          data.sort((a, b) => new Date(b.date || 0) - new Date(a.date || 0));
          data.forEach(alert => {
            const row = document.createElement('div');
            row.style.background = 'rgba(255,255,255,0.03)';
            row.style.padding = '10px';
            row.style.borderRadius = '8px';
            row.style.display = 'flex';
            row.style.justifyContent = 'space-between';
            row.style.alignItems = 'center';

            const info = document.createElement('div');
            const startDateStr = formatAlertDate(alert.date);
            const endDateStr = alert.neverEnds ? 'Until removed' : formatAlertDate(alert.endDate);
            const statusMeta = getAlertStatusMeta(alert);
            const status = `<span style="${statusMeta.style}">${statusMeta.label}</span>`;

            info.innerHTML = `<div style="font-weight:600; font-size:14px; color:#fff;">${startDateStr} ${status}</div><div style="font-size:12px; color:#7f8aa3; margin-top:4px;">Ends: ${endDateStr}</div><div style="font-size:13px; color:#aaa; margin-top:6px;">${alert.message}</div>`;

            const delBtn = document.createElement('button');
            delBtn.className = 'btn ghost';
            delBtn.innerHTML = '<i class="fa-solid fa-trash"></i>';
            delBtn.style.padding = '6px 10px';
            delBtn.style.color = '#ff6b6b';
            delBtn.style.borderColor = 'rgba(255,107,107,0.2)';
            delBtn.onclick = () => deleteAlert(alert.id);

            row.appendChild(info);
            row.appendChild(delBtn);
            alertsList.appendChild(row);
          });
        })
        .catch(err => {
          console.error(err);
          alertsList.innerHTML = '<div style="color:#ff6b6b;text-align:center">Failed to load alerts</div>';
        });
    }

    function deleteAlert(id) {
      if (!confirm('Delete this alert?')) return;
      fetch('/api/settings/alert/' + id, { method: 'DELETE' })
        .then(res => res.json())
        .then(data => {
          if (data.ok) loadAlerts();
          else alert('Failed: ' + data.error);
        })
        .catch(err => alert('Error: ' + err));
    }

    openAlertModalBtn.addEventListener('click', (e) => {
      e.preventDefault();
      toggleAlertModal(true);
    });

    if (closeAlertModal) {
      closeAlertModal.addEventListener('click', () => toggleAlertModal(false));
    }
    if (closeAlertModalFooter) {
      closeAlertModalFooter.addEventListener('click', () => toggleAlertModal(false));
    }

    window.addEventListener('click', (e) => {
      if (e.target === alertModal) toggleAlertModal(false);
    });

    if (alertNeverEnds) {
      alertNeverEnds.addEventListener('change', syncAlertEndDateState);
      syncAlertEndDateState();
    }

    if (alertDate && alertEndDate) {
      alertDate.addEventListener('change', () => {
        alertEndDate.min = alertDate.value || '';
        if (alertEndDate.value && alertDate.value && alertEndDate.value < alertDate.value) {
          alertEndDate.value = '';
        }
      });
    }

    if (addAlertBtn) {
      addAlertBtn.addEventListener('click', () => {
        const message = alertContent.value.trim();
        const dateStr = alertDate.value;
        const endDateStr = alertEndDate?.value || '';
        const keepActive = !!alertNeverEnds?.checked;

        if (!message) {
          alert('Please enter a message');
          return;
        }

        const startDate = parseLocalDateTimeInput(dateStr);
        if (!startDate) {
          alert('Please select a complete date and time.');
          return;
        }

        let isoEndDate = null;
        if (!keepActive) {
          const endDate = parseLocalDateTimeInput(endDateStr);
          if (!endDate) {
            alert('Please select an end date.');
            return;
          }
          if (endDate.getTime() <= startDate.getTime()) {
            alert('End date must be after start date.');
            return;
          }
          isoEndDate = endDate.toISOString();
        }

        fetch('/api/settings/alert', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            message,
            date: startDate.toISOString(),
            endDate: isoEndDate,
            neverEnds: keepActive
          })
        })
          .then(res => res.json())
          .then(data => {
            if (data.ok) {
              alertContent.value = '';
              alertDate.value = '';
              if (alertEndDate) alertEndDate.value = '';
              if (alertNeverEnds) alertNeverEnds.checked = true;
              syncAlertEndDateState();
              loadAlerts();
            } else {
              alert('Failed: ' + (data.error || 'Unknown'));
            }
          })
          .catch(err => alert('Error: ' + err));
      });
    }
  }

  let webhooksConfig = { mode: 'single', single: null, multiple: {} };
  let webhookCategories = [];
  let webhookEditCategory = null;

  const webhookModeSingle = document.getElementById('webhookModeSingle');
  const webhookModeMultiple = document.getElementById('webhookModeMultiple');
  const webhookSingleSection = document.getElementById('webhookSingleSection');
  const webhookMultipleSection = document.getElementById('webhookMultipleSection');
  const webhookSingleList = document.getElementById('webhookSingleList');
  const webhookSingleEmpty = document.getElementById('webhookSingleEmpty');
  const webhookCategoriesGrid = document.getElementById('webhookCategoriesGrid');
  const openAddWebhookBtn = document.getElementById('openAddWebhookBtn');
  const webhookModalOverlay = document.getElementById('webhookModalOverlay');
  const webhookModalClose = document.getElementById('webhookModalClose');
  const webhookModalCancel = document.getElementById('webhookModalCancel');
  const webhookModalSave = document.getElementById('webhookModalSave');
  const webhookModalTitle = document.getElementById('webhookModalTitle');
  const webhookModalSubtitle = document.getElementById('webhookModalSubtitle');
  const webhookUrlInput = document.getElementById('webhookUrlInput');
  const webhookModalError = document.getElementById('webhookModalError');
  const webhookModeIndicator = document.getElementById('webhookModeIndicator');
  const webhookCategoryLabel = document.getElementById('webhookCategoryLabel');
  const webhookDeleteOverlay = document.getElementById('webhookDeleteOverlay');
  const webhookDeleteClose = document.getElementById('webhookDeleteClose');
  const webhookDeleteCancelBtn = document.getElementById('webhookDeleteCancelBtn');
  const webhookDeleteConfirmBtn = document.getElementById('webhookDeleteConfirmBtn');
  const webhookDeleteText = document.getElementById('webhookDeleteText');

  let pendingDeleteCategory = null;

  function toggleWebhookModal(show, options = {}) {
    if (!webhookModalOverlay) return;
    const resetInput = options.resetInput !== false;
    const innerModal = webhookModalOverlay.querySelector('.enterprise-modal');
    webhookModalOverlay.setAttribute('aria-hidden', show ? 'false' : 'true');
    if (show) {
      webhookModalOverlay.classList.add('show');
      if (innerModal) {
        innerModal.style.animation = 'none';
        innerModal.offsetHeight;
        innerModal.style.animation = '';
        setTimeout(() => innerModal.classList.add('show'), 10);
      }
      if (webhookModalError) {
        webhookModalError.textContent = '';
        webhookModalError.style.display = 'none';
      }
      if (webhookUrlInput && resetInput) webhookUrlInput.value = '';
      setTimeout(() => webhookUrlInput && webhookUrlInput.focus(), 100);
    } else {
      if (innerModal) innerModal.classList.remove('show');
      setTimeout(() => webhookModalOverlay.classList.remove('show'), 200);
    }
  }

  function toggleWebhookDeleteModal(show) {
    if (!webhookDeleteOverlay) return;
    const innerModal = webhookDeleteOverlay.querySelector('.enterprise-modal');
    webhookDeleteOverlay.setAttribute('aria-hidden', show ? 'false' : 'true');
    if (show) {
      webhookDeleteOverlay.classList.add('show');
      if (innerModal) {
        innerModal.style.animation = 'none';
        innerModal.offsetHeight;
        innerModal.style.animation = '';
        setTimeout(() => innerModal.classList.add('show'), 10);
      }
    } else {
      if (innerModal) innerModal.classList.remove('show');
      setTimeout(() => webhookDeleteOverlay.classList.remove('show'), 200);
    }
  }

  async function loadWebhooks() {
    try {
      const res = await fetch('/api/settings/webhooks');
      const data = await res.json();
      if (data.ok) {
        webhooksConfig = data.config || { mode: 'single', single: null, multiple: {} };
        webhookCategories = data.categories || [];
        renderWebhooks();
      }
    } catch (e) {
      console.error('[webhooks] Load error:', e);
    }
  }
  window._loadWebhooks = loadWebhooks;

  function renderWebhooks() {
    if (!webhookModeSingle) return;

    if (webhooksConfig.mode === 'single') {
      webhookModeSingle.classList.add('active');
      webhookModeMultiple.classList.remove('active');
      if (webhookSingleSection) webhookSingleSection.style.display = '';
      if (webhookMultipleSection) webhookMultipleSection.style.display = 'none';
    } else {
      webhookModeSingle.classList.remove('active');
      webhookModeMultiple.classList.add('active');
      if (webhookSingleSection) webhookSingleSection.style.display = 'none';
      if (webhookMultipleSection) webhookMultipleSection.style.display = '';
    }

    const singleConfigured = !!(webhooksConfig.singleConfigured || webhooksConfig.single);
    if (singleConfigured) {
      if (webhookSingleEmpty) webhookSingleEmpty.style.display = 'none';
      if (webhookSingleList) webhookSingleList.innerHTML = renderWebhookItem('All Events', webhooksConfig.single);
    } else {
      if (webhookSingleEmpty) webhookSingleEmpty.style.display = 'flex';
      if (webhookSingleList) webhookSingleList.innerHTML = '';
    }

    if (webhookCategoriesGrid) {
      webhookCategoriesGrid.innerHTML = webhookCategories.map(cat => {
        const maskedUrl = webhooksConfig.multiple && webhooksConfig.multiple[cat.id];
        const isConfigured = !!(webhooksConfig.multipleConfigured && webhooksConfig.multipleConfigured[cat.id]);
        const hasUrl = isConfigured || !!maskedUrl;
        const safeColor = /^#[0-9a-fA-F]{3,8}$/.test(cat.color) ? cat.color : '#888';
        const safeIcon = /^[a-zA-Z0-9\- ]+$/.test(cat.icon) ? cat.icon : 'fa-solid fa-circle';
        return `
          <div class="webhook-category-card">
            <div class="webhook-category-left">
              <div class="webhook-category-icon" style="background: ${safeColor}22;">
                <i class="${safeIcon}" style="color: ${safeColor};"></i>
              </div>
              <div>
                <div class="webhook-category-name">${escapeHtml(cat.label)}</div>
                <div class="webhook-category-desc">${hasUrl ? escapeHtml(maskedUrl || 'Configured') : 'Not configured'}</div>
              </div>
            </div>
            <div class="webhook-category-right">
              ${hasUrl ? '<span class="webhook-category-status active">Active</span>' : '<span class="webhook-category-status">Inactive</span>'}
              ${hasUrl ? `
                <button class="webhook-action-btn test" data-webhook-action="test-category" data-webhook-category="${escapeHtml(cat.id)}" title="Test"><i class="fa-solid fa-paper-plane"></i></button>
                <button class="webhook-action-btn danger" data-webhook-action="delete-category" data-webhook-category="${escapeHtml(cat.id)}" title="Remove"><i class="fa-solid fa-trash"></i></button>
              ` : ''}
              <button class="webhook-category-btn" data-webhook-action="edit-category" data-webhook-category="${escapeHtml(cat.id)}">${hasUrl ? 'Edit' : 'Set'}</button>
            </div>
          </div>`;
      }).join('');
    }
  }

  function renderWebhookItem(label, maskedUrl) {
    return `
      <div class="webhook-item">
        <div class="webhook-item-left">
          <div class="webhook-item-icon"><i class="fa-brands fa-discord"></i></div>
          <div class="webhook-item-info">
            <div class="webhook-item-label">${escapeHtml(label)}</div>
            <div class="webhook-item-url">${escapeHtml(maskedUrl || 'Configured')}</div>
          </div>
        </div>
        <div class="webhook-item-actions">
          <button class="webhook-action-btn test" data-webhook-action="test-single" title="Send test"><i class="fa-solid fa-paper-plane"></i></button>
          <button class="webhook-action-btn" data-webhook-action="edit-single" title="Edit"><i class="fa-solid fa-pen"></i></button>
          <button class="webhook-action-btn danger" data-webhook-action="delete-single" title="Delete"><i class="fa-solid fa-trash"></i></button>
        </div>
      </div>`;
  }

  if (webhookModeSingle) {
    webhookModeSingle.addEventListener('click', async () => {
      if (webhooksConfig.mode === 'single') return;
      try {
        const res = await fetch('/api/settings/webhooks', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ mode: 'single' })
        });
        const data = await res.json();
        if (data.ok) { webhooksConfig.mode = 'single'; renderWebhooks(); }
      } catch (e) { console.error(e); }
    });
  }

  if (webhookModeMultiple) {
    webhookModeMultiple.addEventListener('click', async () => {
      if (webhooksConfig.mode === 'multiple') return;
      try {
        const res = await fetch('/api/settings/webhooks', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ mode: 'multiple' })
        });
        const data = await res.json();
        if (data.ok) { webhooksConfig.mode = 'multiple'; renderWebhooks(); }
      } catch (e) { console.error(e); }
    });
  }

  if (openAddWebhookBtn) {
    openAddWebhookBtn.addEventListener('click', () => {
      webhookEditCategory = null;
      if (webhookModalTitle) webhookModalTitle.textContent = 'Add Single Webhook';
      if (webhookModalSubtitle) webhookModalSubtitle.textContent = 'Enter a Discord webhook URL for all log categories';
      if (webhookModeIndicator) webhookModeIndicator.style.display = 'none';
      toggleWebhookModal(true, { resetInput: true });
    });
  }

  if (webhookSingleList) {
    webhookSingleList.addEventListener('click', (e) => {
      const btn = e.target.closest('button[data-webhook-action]');
      if (!btn || !webhookSingleList.contains(btn)) return;
      const action = btn.getAttribute('data-webhook-action');
      if (action === 'test-single') testWebhookSingle();
      else if (action === 'edit-single') openEditWebhookSingle();
      else if (action === 'delete-single') confirmDeleteWebhookSingle();
    });
  }

  if (webhookCategoriesGrid) {
    webhookCategoriesGrid.addEventListener('click', (e) => {
      const btn = e.target.closest('button[data-webhook-action]');
      if (!btn || !webhookCategoriesGrid.contains(btn)) return;
      const action = btn.getAttribute('data-webhook-action');
      const categoryId = btn.getAttribute('data-webhook-category');
      if (!categoryId) return;
      if (action === 'edit-category') openWebhookCategoryModal(categoryId);
      else if (action === 'delete-category') deleteWebhookCategory(categoryId);
      else if (action === 'test-category') testWebhookCategory(categoryId);
    });
  }

  if (webhookModalClose) webhookModalClose.addEventListener('click', () => toggleWebhookModal(false));
  if (webhookModalCancel) webhookModalCancel.addEventListener('click', () => toggleWebhookModal(false));
  if (webhookModalOverlay) webhookModalOverlay.addEventListener('click', (e) => { if (e.target === webhookModalOverlay) toggleWebhookModal(false); });

  if (webhookModalSave) {
    webhookModalSave.addEventListener('click', async () => {
      const url = (webhookUrlInput.value || '').trim();
      if (!url) {
        webhookModalError.textContent = 'Please enter a webhook URL';
        webhookModalError.style.display = 'block';
        return;
      }
      if (!/^https:\/\/discord\.com\/api\/webhooks\/\d+\/[\w-]+$/.test(url)) {
        webhookModalError.textContent = 'Invalid Discord webhook URL format';
        webhookModalError.style.display = 'block';
        return;
      }
      webhookModalSave.disabled = true;
      webhookModalSave.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Saving...';
      try {
        const body = { url };
        if (webhookEditCategory) body.category = webhookEditCategory;
        const res = await fetch('/api/settings/webhooks', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body)
        });
        const data = await res.json();
        if (data.ok) {
          toggleWebhookModal(false);
          loadWebhooks();
        } else {
          webhookModalError.textContent = data.error || 'Failed to save';
          webhookModalError.style.display = 'block';
        }
      } catch (e) {
        webhookModalError.textContent = 'Network error';
        webhookModalError.style.display = 'block';
      } finally {
        webhookModalSave.disabled = false;
        webhookModalSave.innerHTML = '<i class="fa-solid fa-check"></i> Save Webhook';
      }
    });
  }

  if (webhookDeleteClose) webhookDeleteClose.addEventListener('click', () => toggleWebhookDeleteModal(false));
  if (webhookDeleteCancelBtn) webhookDeleteCancelBtn.addEventListener('click', () => toggleWebhookDeleteModal(false));
  if (webhookDeleteOverlay) webhookDeleteOverlay.addEventListener('click', (e) => { if (e.target === webhookDeleteOverlay) toggleWebhookDeleteModal(false); });

  if (webhookDeleteConfirmBtn) {
    webhookDeleteConfirmBtn.addEventListener('click', async () => {
      webhookDeleteConfirmBtn.disabled = true;
      webhookDeleteConfirmBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Deleting...';
      try {
        const body = {};
        if (pendingDeleteCategory) body.category = pendingDeleteCategory;
        const res = await fetch('/api/settings/webhooks', {
          method: 'DELETE', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body)
        });
        const data = await res.json();
        if (data.ok) { toggleWebhookDeleteModal(false); loadWebhooks(); }
      } catch (e) { console.error(e); }
      finally {
        webhookDeleteConfirmBtn.disabled = false;
        webhookDeleteConfirmBtn.innerHTML = '<i class="fa-solid fa-trash"></i> Delete';
        pendingDeleteCategory = null;
      }
    });
  }

  function openEditWebhookSingle() {
    webhookEditCategory = null;
    if (webhookModalTitle) webhookModalTitle.textContent = 'Replace Webhook';
    if (webhookModalSubtitle) webhookModalSubtitle.textContent = 'Enter a new Discord webhook URL to replace the current one';
    if (webhookModeIndicator) webhookModeIndicator.style.display = 'none';
    toggleWebhookModal(true, { resetInput: true });
  }

  function confirmDeleteWebhookSingle() {
    pendingDeleteCategory = null;
    if (webhookDeleteText) webhookDeleteText.textContent = 'Are you sure you want to remove the webhook? It will stop receiving all panel activity logs.';
    toggleWebhookDeleteModal(true);
  }

  async function testWebhookSingle() {
    try {
      const res = await fetch('/api/settings/webhooks/test', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({})
      });
      const data = await res.json();
      showWebhookToast(data.ok ? 'Test message sent!' : (data.error || 'Failed'), data.ok ? 'success' : 'error');
    } catch (e) { showWebhookToast('Network error', 'error'); }
  }

  function openWebhookCategoryModal(catId) {
    const cat = webhookCategories.find(c => c.id === catId);
    if (!cat) return;
    webhookEditCategory = catId;
    const isConfigured = !!(webhooksConfig.multipleConfigured && webhooksConfig.multipleConfigured[catId]);
    if (webhookModalTitle) webhookModalTitle.textContent = isConfigured ? 'Replace Webhook' : 'Set Webhook';
    if (webhookModalSubtitle) webhookModalSubtitle.textContent = 'Webhook for: ' + cat.label;
    if (webhookModeIndicator) webhookModeIndicator.style.display = 'block';
    if (webhookCategoryLabel) webhookCategoryLabel.textContent = cat.label;
    toggleWebhookModal(true, { resetInput: true });
  }

  function deleteWebhookCategory(catId) {
    const cat = webhookCategories.find(c => c.id === catId);
    pendingDeleteCategory = catId;
    if (webhookDeleteText) webhookDeleteText.textContent = 'Remove the webhook for "' + (cat ? cat.label : catId) + '"?';
    toggleWebhookDeleteModal(true);
  }

  async function testWebhookCategory(catId) {
    try {
      const res = await fetch('/api/settings/webhooks/test', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ category: catId })
      });
      const data = await res.json();
      showWebhookToast(data.ok ? 'Test message sent!' : (data.error || 'Failed'), data.ok ? 'success' : 'error');
    } catch (e) { showWebhookToast('Network error', 'error'); }
  }

  window.openEditWebhookSingle = openEditWebhookSingle;
  window.confirmDeleteWebhookSingle = confirmDeleteWebhookSingle;
  window.testWebhookSingle = testWebhookSingle;
  window.openWebhookCategoryModal = openWebhookCategoryModal;
  window.deleteWebhookCategory = deleteWebhookCategory;
  window.testWebhookCategory = testWebhookCategory;

  function showWebhookToast(msg, type) {
    const existing = document.querySelector('.webhook-toast');
    if (existing) existing.remove();
    const toast = document.createElement('div');
    toast.className = 'webhook-toast';
    toast.style.cssText = 'position:fixed;bottom:24px;right:24px;z-index:10001;padding:12px 20px;border-radius:10px;font-size:14px;font-weight:500;color:white;pointer-events:none;animation:fadeInUp 0.3s ease;' +
      (type === 'success' ? 'background:rgba(34,197,94,0.9);' : 'background:rgba(239,68,68,0.9);');
    toast.innerHTML = '<i class="fa-solid ' + (type === 'success' ? 'fa-check-circle' : 'fa-exclamation-circle') + '" style="margin-right:8px;"></i>' + escapeHtml(msg);
    document.body.appendChild(toast);
    setTimeout(() => { toast.style.opacity = '0'; toast.style.transition = 'opacity 0.3s'; setTimeout(() => toast.remove(), 300); }, 3000);
  }
});
(function () {
  let _panelInfoTokens = {};
  let _latestVersionFound = null;
  let _cachedVersion = '';
  let _cachedArchitecture = '';

  async function loadPanelInfo() {
    const versionEl = document.getElementById('panelInfoVersion');
    const archEl = document.getElementById('panelInfoArchitecture');
    if (!versionEl || !archEl) return;

    versionEl.innerHTML = '<code>Loading...</code>';
    archEl.innerHTML = '<code>Loading...</code>';

    try {
      const res = await fetch('/api/settings/panel-info', { credentials: 'include' });
      if (!res.ok) {
        versionEl.innerHTML = '<code>Error</code>';
        archEl.innerHTML = '<code>Error</code>';
        return;
      }
      const data = await res.json();
      _cachedVersion = data.version || 'unknown';
      _cachedArchitecture = data.architecture || 'unknown';
      versionEl.innerHTML = `<code>${escapeHtml(_cachedVersion)}</code>`;
      archEl.innerHTML = `<code>${escapeHtml(_cachedArchitecture)}</code>`;
      _panelInfoTokens = data.actionTokens || {};
    } catch (e) {
      console.error('[panel-info] Failed to load:', e);
      versionEl.innerHTML = '<code>Network error</code>';
      archEl.innerHTML = '<code>Network error</code>';
    }
  }

  window.loadPanelInfo = loadPanelInfo;

  function handleCopy(btnId, getValue) {
    const btn = document.getElementById(btnId);
    if (!btn) return;
    btn.addEventListener('click', async () => {
      const value = getValue();
      if (!value || value === 'unknown' || value === 'Loading...') return;

      const textarea = document.createElement('textarea');
      textarea.value = value;
      textarea.style.position = 'fixed';
      textarea.style.left = '-9999px';
      textarea.style.top = '0';
      document.body.appendChild(textarea);
      textarea.focus();
      textarea.select();

      try {
        document.execCommand('copy');
        btn.classList.add('copied');
        const icon = btn.querySelector('i');
        if (icon) { icon.className = 'fa-solid fa-check'; }
        setTimeout(() => {
          btn.classList.remove('copied');
          if (icon) { icon.className = 'fa-solid fa-copy'; }
        }, 1500);
      } catch (err) {
        console.error('Copy failed', err);
      }
      document.body.removeChild(textarea);
    });
  }
  handleCopy('copyVersionBtn', () => _cachedVersion);
  handleCopy('copyArchBtn', () => _cachedArchitecture);

  const checkBtn = document.getElementById('checkForUpdatesBtn');
  const spinIcon = document.getElementById('updateSpinIcon');

  function resetCheckBtn() {
    if (!checkBtn) return;
    checkBtn.dataset.mode = '';
    checkBtn.className = 'btn btn-update-check';
    checkBtn.innerHTML = '<i class="fa-solid fa-arrows-rotate" id="updateSpinIcon"></i> Check for Updates';
    checkBtn.disabled = false;
  }

  if (checkBtn) {
    checkBtn.addEventListener('click', async () => {
      const statusEl = document.getElementById('panelUpdateStatus');

      if (checkBtn.dataset.mode === 'install') {
        await doInstall(_latestVersionFound, 'auto');
        return;
      }

      const icon = checkBtn.querySelector('.fa-arrows-rotate');
      if (icon) {
        icon.classList.remove('spinning');
        void icon.offsetWidth;
        icon.classList.add('spinning');
        icon.addEventListener('animationend', () => icon.classList.remove('spinning'), { once: true });
      }

      checkBtn.disabled = true;
      if (statusEl) { statusEl.textContent = 'Checking for updates...'; statusEl.className = 'panelinfo-update-status info'; }

      try {
        const res = await fetch('/api/settings/panel-update/check', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'x-action-token': _panelInfoTokens.checkUpdate || '' },
          credentials: 'include',
          body: JSON.stringify({}),
        });

        const data = await res.json().catch(() => ({}));

        if (!res.ok) {
          if (statusEl) { statusEl.textContent = data.error || 'Failed to check for updates'; statusEl.className = 'panelinfo-update-status error'; }
          checkBtn.disabled = false;
          return;
        }

        if (data.updateAvailable) {
          _latestVersionFound = data.latestVersion;
          if (data.actionTokens) _panelInfoTokens = { ..._panelInfoTokens, ...data.actionTokens };
          if (statusEl) {
            statusEl.innerHTML = `<i class="fa-solid fa-circle-up" style="margin-right:6px;"></i>New version found: <strong>${escapeHtml(data.latestVersion)}</strong> (current: ${escapeHtml(data.currentVersion)})`;
            statusEl.className = 'panelinfo-update-status success';
          }
          checkBtn.dataset.mode = 'install';
          checkBtn.className = 'btn btn-update-install';
          checkBtn.innerHTML = '<i class="fa-solid fa-download"></i> Install Update';
          checkBtn.disabled = false;
        } else {
          if (statusEl) {
            statusEl.innerHTML = `<i class="fa-solid fa-circle-check" style="margin-right:6px;"></i>You are running the latest version (${escapeHtml(data.currentVersion)})`;
            statusEl.className = 'panelinfo-update-status success';
          }
          checkBtn.disabled = false;
          await loadPanelInfo();
        }
      } catch (e) {
        console.error('[panel-update] Check failed:', e);
        if (statusEl) { statusEl.textContent = 'Network error while checking for updates'; statusEl.className = 'panelinfo-update-status error'; }
        checkBtn.disabled = false;
      }
    });
  }

  async function doInstall(version, source) {
    const statusEl = document.getElementById('panelUpdateStatus');
    if (!version) {
      if (statusEl) { statusEl.textContent = 'No version specified.'; statusEl.className = 'panelinfo-update-status error'; }
      return;
    }

    const cleanVersion = String(version).trim();
    if (!/^v?\d+\.\d+\.\d+/.test(cleanVersion)) {
      if (statusEl) { statusEl.textContent = 'Invalid version format. Use format: v1.0.0'; statusEl.className = 'panelinfo-update-status error'; }
      return;
    }

    if (!confirm(`Install ADPanel ${cleanVersion}? The panel will need to be restarted after installation.`)) {
      return;
    }

    const installSourceBtn = source === 'manual' ? document.getElementById('manualInstallBtn') : checkBtn;
    if (checkBtn) {
      checkBtn.className = 'btn btn-update-installing';
      checkBtn.innerHTML = '<i class="fa-solid fa-spinner"></i> Installing...';
      checkBtn.disabled = true;
    }
    const manualBtn = document.getElementById('manualInstallBtn');
    const manualInput = document.getElementById('manualVersionInput');
    if (manualBtn) manualBtn.disabled = true;
    if (manualInput) manualInput.disabled = true;

    if (statusEl) { statusEl.textContent = 'Downloading and installing update...'; statusEl.className = 'panelinfo-update-status info'; }

    try {
      if (!_panelInfoTokens.installUpdate) {
        await loadPanelInfo();
      }

      const res = await fetch('/api/settings/panel-update/install', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-action-token': _panelInfoTokens.installUpdate || '' },
        credentials: 'include',
        body: JSON.stringify({ version: cleanVersion }),
      });

      const data = await res.json().catch(() => ({}));

      if (!res.ok) {
        if (statusEl) { statusEl.textContent = data.error || 'Failed to install update'; statusEl.className = 'panelinfo-update-status error'; }
        resetCheckBtn();
        if (manualBtn) manualBtn.disabled = false;
        if (manualInput) manualInput.disabled = false;
        return;
      }

      if (statusEl) {
        statusEl.innerHTML = `<i class="fa-solid fa-circle-check" style="margin-right:6px;"></i>Update to <strong>${escapeHtml(data.version)}</strong> installed! ${data.filesUpdated} files updated. Please restart the panel.`;
        statusEl.className = 'panelinfo-update-status success';
      }

      if (checkBtn) {
        checkBtn.className = 'btn btn-update-check';
        checkBtn.innerHTML = '<i class="fa-solid fa-check"></i> Update Installed';
        checkBtn.disabled = true;
        checkBtn.dataset.mode = '';
      }
      if (manualBtn) manualBtn.disabled = false;
      if (manualInput) { manualInput.disabled = false; manualInput.value = ''; }

      setTimeout(() => {
        resetCheckBtn();
        loadPanelInfo();
      }, 3000);

    } catch (e) {
      console.error('[panel-update] Install failed:', e);
      if (statusEl) { statusEl.textContent = 'Network error while installing update'; statusEl.className = 'panelinfo-update-status error'; }
      resetCheckBtn();
      if (manualBtn) manualBtn.disabled = false;
      if (manualInput) manualInput.disabled = false;
    }
  }

  const manualInstallBtn = document.getElementById('manualInstallBtn');
  const manualVersionInput = document.getElementById('manualVersionInput');
  if (manualInstallBtn && manualVersionInput) {
    manualInstallBtn.addEventListener('click', async () => {
      const version = manualVersionInput.value.trim();
      if (!version) {
        const statusEl = document.getElementById('panelUpdateStatus');
        if (statusEl) { statusEl.textContent = 'Please enter a version number.'; statusEl.className = 'panelinfo-update-status error'; }
        manualVersionInput.focus();
        return;
      }
      await doInstall(version, 'manual');
    });

    manualVersionInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        manualInstallBtn.click();
      }
    });
  }

  const urlParams = new URLSearchParams(window.location.search);
  if (urlParams.get('panel') === 'panelinfo') {
    loadPanelInfo();
  }
})();
