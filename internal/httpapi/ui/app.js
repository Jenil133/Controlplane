// Controlplane admin UI.
//
// A dependency-free single-page app over the JSON API. The API speaks
// protojson: camelCase field names, 64-bit integers as strings, durations
// such as "30s" and enums by name. The page's Content-Security-Policy
// forbids inline script, inline event handlers and style attributes, so all
// behaviour lives in this file, and everything that comes from users or the
// server is rendered as text, never parsed as HTML.

const TABS = [
  { id: 'configs', label: 'Configs' },
  { id: 'flags', label: 'Flags' },
  { id: 'experiments', label: 'Experiments' },
  { id: 'rate-limits', label: 'Rate limits' },
  { id: 'circuit-breakers', label: 'Circuit breakers' },
  { id: 'history', label: 'History' },
  { id: 'audit', label: 'Audit' },
];

// KINDS are the entity tabs and the snapshot list each one shows.
const KINDS = {
  configs: { noun: 'config', items: (s) => s.configs },
  flags: { noun: 'flag', items: (s) => s.flags },
  experiments: { noun: 'experiment', items: (s) => s.experiments },
  'rate-limits': { noun: 'rate limit', items: (s) => s.rateLimits },
  'circuit-breakers': { noun: 'circuit breaker', items: (s) => s.circuitBreakers },
};

// ENTITY_LABELS names the entity types of audit events and diffs.
const ENTITY_LABELS = {
  namespace: 'namespace',
  config: 'config',
  flag: 'flag',
  experiment: 'experiment',
  rate_limit: 'rate limit',
  circuit_breaker: 'circuit breaker',
};

const ROLLOUT_STATES = {
  ROLLOUT_STATE_ACTIVE: { label: 'active', tone: 'ok' },
  ROLLOUT_STATE_PAUSED: { label: 'paused', tone: 'warn' },
  ROLLOUT_STATE_COMPLETED: { label: 'completed', tone: '' },
  ROLLOUT_STATE_ABORTED: { label: 'aborted', tone: 'danger' },
};

const CHANGE_TYPES = {
  CHANGE_TYPE_ADDED: 'added',
  CHANGE_TYPE_MODIFIED: 'modified',
  CHANGE_TYPE_REMOVED: 'removed',
};
const CHANGE_TONES = { added: 'ok', modified: 'warn', removed: 'danger' };
const ACTION_TONES = { create: 'ok', update: 'info', delete: 'danger', rollback: 'warn' };
const ROLLOUT_DONE = { advance: 'Advanced', pause: 'Paused', resume: 'Resumed', abort: 'Aborted' };

// Stored entries carry these alongside their content; diffs list them last.
const META_FIELDS = ['revision', 'updated_at', 'updated_by'];
// Stored durations are nanoseconds.
const DURATION_FIELDS = new Set(['window', 'open_duration']);

const HINTS = {
  Unauthenticated: 'Enter a valid API token at the top right.',
  PermissionDenied: "Your token's role does not allow this.",
  Aborted: 'Someone changed it after this page loaded it. Refresh and try again.',
  Unavailable: 'The control plane could not be reached.',
};

const PAGE_SIZE = 25;
const POLL_MS = 10000;
const NOTICE_MS = 6000;
// The API token is kept in localStorage so that a reload does not ask for it
// again. That is a trade-off: any script that runs on this origin can read it.
// The Content-Security-Policy (no inline or foreign script) and the rule that
// data is only ever rendered as text are what keep such a script out, and a
// token's role limits what a leak is worth; sessionStorage would only shorten
// its life to the tab. Clearing the field removes the stored copy.
const TOKEN_KEY = 'controlplane.token';
const ACTOR_KEY = 'controlplane.actor';

const els = {};
const state = {
  namespaces: [],
  name: '', // selected namespace
  status: 'idle', // of the selected namespace: loading | ready | failed
  namespace: null, // its Namespace message
  snapshot: null, // its Snapshot, which every entity tab shows
  tab: 'configs',
  editing: null, // open inline form, { tab, key, mode }; key is '' for a new entry
  filters: {}, // key filter text per entity tab
  history: newHistory(),
  audit: newAudit(),
};

// ------------------------------------------------------------------ storage

function load(key) {
  try {
    return localStorage.getItem(key) ?? '';
  } catch {
    return '';
  }
}

function save(key, value) {
  try {
    if (value) localStorage.setItem(key, value);
    else localStorage.removeItem(key);
  } catch {
    // Storage may be disabled; the value then lasts as long as the page.
  }
}

// ---------------------------------------------------------------------- API

class ApiError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}

let inflight = 0;

// rpc calls one API method. Call sites pass the literal method path and an
// object literal request: a Go test checks both against the API definition.
// Background calls do not light up the activity bar.
async function rpc(path, request, { background = false } = {}) {
  const headers = { 'Content-Type': 'application/json' };
  const token = els.token.value.trim();
  if (token) headers.Authorization = `Bearer ${token}`;
  const actor = els.actor.value.trim();
  if (actor) headers['X-Controlplane-Actor'] = actor;

  if (!background) setLoading(1);
  try {
    let response;
    try {
      response = await fetch(path, {
        method: 'POST',
        headers,
        body: JSON.stringify(request),
        cache: 'no-store',
      });
    } catch (err) {
      throw new ApiError('Unavailable', `request failed: ${err.message}`);
    }
    let body = null;
    try {
      body = await response.json();
    } catch {
      // Not JSON, e.g. an error page from a proxy: report the status line.
    }
    if (!response.ok) {
      throw new ApiError(body?.code || 'Unknown', body?.message || `HTTP ${response.status} ${response.statusText}`);
    }
    return body ?? {};
  } finally {
    if (!background) setLoading(-1);
  }
}

function setLoading(delta) {
  inflight += delta;
  document.body.classList.toggle('loading', inflight > 0);
}

// ---------------------------------------------------------------------- DOM

// nodes turns children into nodes; strings become text nodes, so data can
// never turn into markup.
function nodes(children) {
  return children
    .flat(Infinity)
    .filter((c) => c !== null && c !== undefined && c !== false)
    .map((c) => (c instanceof Node ? c : String(c)));
}

// Properties set on the element rather than as attributes.
const DOM_PROPS = new Set(['value', 'checked', 'disabled', 'readOnly', 'required']);

// h builds an element. Children are added before properties so that a
// select's value can pick one of its options.
function h(tag, props, ...children) {
  const node = document.createElement(tag);
  node.append(...nodes(children));
  for (const [key, value] of Object.entries(props ?? {})) {
    if (value === null || value === undefined || value === false) continue;
    if (key === 'class') node.className = value;
    else if (key === 'dataset') Object.assign(node.dataset, value);
    else if (key.startsWith('on')) node.addEventListener(key.slice(2), value);
    else if (DOM_PROPS.has(key)) node[key] = value;
    else node.setAttribute(key, value === true ? '' : String(value));
  }
  return node;
}

function fill(parent, ...children) {
  parent.replaceChildren(...nodes(children));
}

// --------------------------------------------------------------- formatting

// num reads an int64, which protojson sends as a string.
const num = (v) => Number(v ?? 0);

const decimal = (x) => String(Number(x.toFixed(9)));

function fmtPercent(p) {
  return `${Number(Number(p ?? 0).toFixed(2))}%`;
}

function fmtTime(ts) {
  const d = new Date(ts);
  if (!ts || Number.isNaN(d.getTime())) return '—';
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function relTime(ts) {
  const ms = new Date(ts).getTime() - Date.now();
  const secs = Math.abs(ms) / 1000;
  if (secs < 45) return ms < 0 ? 'just now' : 'in a moment';
  const [value, unit] = secs < 3600 ? [secs / 60, 'm'] : secs < 86400 ? [secs / 3600, 'h'] : [secs / 86400, 'd'];
  const text = `${Math.round(value)}${unit}`;
  return ms < 0 ? `${text} ago` : `in ${text}`;
}

// ago shows a relative time with the exact local time on hover.
function ago(ts) {
  if (!ts) return h('span', { class: 'muted' }, '—');
  return h('time', { datetime: ts, title: fmtTime(ts) }, relTime(ts));
}

// at shows an exact local time with the relative time on hover.
function at(ts) {
  if (!ts) return h('span', { class: 'muted' }, '—');
  return h('time', { datetime: ts, title: relTime(ts) }, fmtTime(ts));
}

const UNIT_NS = { ns: 1, us: 1e3, 'µs': 1e3, 'μs': 1e3, ms: 1e6, s: 1e9, m: 60e9, h: 3600e9 };

// parseDuration reads a Go duration such as "90s", "10m" or "1h30m" into
// nanoseconds; cpctl takes the same syntax.
function parseDuration(text) {
  const s = text.trim();
  if (s === '') throw new Error('a duration is required, e.g. 30s');
  if (s === '0') return 0;
  const part = /(\d+\.?\d*|\.\d+)(ns|us|µs|μs|ms|s|m|h)/y;
  let ns = 0;
  for (let pos = 0; pos < s.length; pos = part.lastIndex) {
    part.lastIndex = pos;
    const m = part.exec(s);
    if (!m) throw new Error(`invalid duration "${text}": use e.g. 30s, 10m or 1h30m`);
    ns += Number(m[1]) * UNIT_NS[m[2]];
  }
  return Math.round(ns);
}

// protoDuration formats nanoseconds as a protojson Duration, e.g. "1.5s".
function protoDuration(ns) {
  const seconds = Math.floor(ns / 1e9);
  const nanos = ns - seconds * 1e9;
  if (nanos === 0) return `${seconds}s`;
  return `${seconds}.${String(nanos).padStart(9, '0').replace(/0+$/, '')}s`;
}

// durationNs reads a protojson Duration such as "600s" or "0.250s".
function durationNs(d) {
  const m = /^(\d+)(?:\.(\d{1,9}))?s$/.exec(d ?? '');
  return m ? Number(m[1]) * 1e9 + Number((m[2] ?? '').padEnd(9, '0')) : 0;
}

// formatDuration renders nanoseconds as a Go duration, e.g. "1h30m".
function formatDuration(ns) {
  if (!ns) return '0s';
  if (ns < 1e3) return `${ns}ns`;
  if (ns < 1e6) return `${decimal(ns / 1e3)}µs`;
  if (ns < 1e9) return `${decimal(ns / 1e6)}ms`;
  const hours = Math.floor(ns / 3600e9);
  const minutes = Math.floor((ns % 3600e9) / 60e9);
  const seconds = (ns % 60e9) / 1e9;
  return (hours ? `${hours}h` : '') + (minutes ? `${minutes}m` : '') + (seconds ? `${decimal(seconds)}s` : '');
}

// parseStages reads rollout stages written as "1:10m,5:10m,25:30m,100":
// percent[:duration], where a stage without a duration advances manually.
function parseStages(text) {
  const parts = text
    .split(',')
    .map((p) => p.trim())
    .filter(Boolean);
  if (parts.length === 0) throw new Error('enter at least one stage, e.g. 1:10m,5:10m,25:30m,100');
  return parts.map((part) => {
    const [percentText, durationText, ...rest] = part.split(':');
    // Plain decimals only: Number() would also take 0x10, 1e1 and Infinity.
    const digits = percentText.trim().replace(/%$/, '');
    const percent = Number(digits);
    if (rest.length > 0 || !/^(\d+\.?\d*|\.\d+)$/.test(digits)) {
      throw new Error(`invalid stage "${part}": use percent or percent:duration, e.g. 5:10m`);
    }
    return { percent, duration: durationText === undefined ? 0 : parseDuration(durationText) };
  });
}

function describeStages(stages) {
  return stages
    .map((s, i) => {
      if (i === stages.length - 1) return fmtPercent(s.percent);
      return s.duration ? `${fmtPercent(s.percent)} for ${formatDuration(s.duration)}` : `${fmtPercent(s.percent)} until advanced`;
    })
    .join(' → ');
}

function planOwns(plan) {
  return plan?.state === 'ROLLOUT_STATE_ACTIVE' || plan?.state === 'ROLLOUT_STATE_PAUSED';
}

// describePlan summarizes a flag's rollout for notices.
function describePlan(flag) {
  const plan = flag.rollout;
  if (!plan) return `rollout ${fmtPercent(flag.rolloutPercent)}`;
  const label = ROLLOUT_STATES[plan.state]?.label ?? 'unknown';
  return `stage ${plan.currentStage + 1}/${plan.stages.length} at ${fmtPercent(flag.rolloutPercent)}, ${label}`;
}

// describeStoredPlan summarizes a rollout plan as stored (snake_case, with
// durations in nanoseconds), as audit events and diffs carry it.
function describeStoredPlan(plan) {
  const stages = plan.stages ?? [];
  const current = plan.current_stage ?? 0;
  const steps = stages.map((s) => fmtPercent(s.percent)).join(' → ');
  return `${plan.state}, stage ${current + 1}/${stages.length} (${steps})`;
}

// splitList reads unit IDs, one per line, dropping blanks and repeats. Commas
// do not separate: an ID may contain one, and saving a flag must never split
// an allowlist entry it was not asked to change.
function splitList(text) {
  return [
    ...new Set(
      text
        .split('\n')
        .map((s) => s.trim())
        .filter(Boolean),
    ),
  ];
}

// ------------------------------------------------------ banners and notices

let noticeTimer = 0;

function showError(err) {
  const code = err instanceof ApiError ? err.code : '';
  const hint = HINTS[code];
  fill(els.bannerText, code ? h('strong', {}, `${code}: `) : null, err.message, hint ? h('span', { class: 'hint' }, hint) : null);
  els.notice.hidden = true;
  els.banner.hidden = false;
}

function notify(message) {
  els.banner.hidden = true;
  els.notice.textContent = message;
  els.notice.hidden = false;
  clearTimeout(noticeTimer);
  noticeTimer = setTimeout(() => {
    els.notice.hidden = true;
  }, NOTICE_MS);
}

// act runs a button's action with the button disabled; errors go to the
// banner.
async function act(button, action) {
  button.disabled = true;
  try {
    await action();
  } catch (err) {
    showError(err);
  } finally {
    button.disabled = false;
  }
}

// ------------------------------------------------------- routing and loading

function link(name, tab = 'configs') {
  return `#/ns/${encodeURIComponent(name)}/${tab}`;
}

function currentRoute() {
  const m = /^#\/ns\/([^/]+)(?:\/([a-z-]+))?$/.exec(location.hash);
  if (!m) return { name: '', tab: 'configs' };
  try {
    return { name: decodeURIComponent(m[1]), tab: TABS.some((t) => t.id === m[2]) ? m[2] : 'configs' };
  } catch {
    return { name: '', tab: 'configs' };
  }
}

// route shows the namespace and tab the location names. Every navigation
// reloads them, so a tab never shows older state than the history next to
// it, and a rollback is checked against the revision on screen.
async function route() {
  const { name, tab } = currentRoute();
  state.tab = tab;
  state.editing = null;
  els.banner.hidden = true;
  if (name !== state.name) {
    state.name = name;
    state.status = name ? 'loading' : 'idle';
    state.namespace = null;
    state.snapshot = null;
    state.history = newHistory();
    state.audit = newAudit();
    els.stale.hidden = true;
  }
  render();
  try {
    if (name) await refresh();
    else await loadNamespaces();
  } catch (err) {
    showError(err);
  }
}

async function loadNamespaces() {
  const res = await rpc('/api/v1/AdminService/ListNamespaces', {});
  state.namespaces = res.namespaces ?? [];
  renderSidebar();
}

async function loadNamespace() {
  const name = state.name;
  try {
    const [nsRes, snapRes] = await Promise.all([
      rpc('/api/v1/AdminService/GetNamespace', { name }),
      rpc('/api/v1/DistributionService/GetSnapshot', { namespace: name }),
    ]);
    if (name !== state.name) return;
    state.namespace = nsRes.namespace;
    state.snapshot = snapRes.snapshot;
    state.status = 'ready';
  } catch (err) {
    if (name === state.name && !state.snapshot) state.status = 'failed';
    throw err;
  }
}

// refresh reloads the selected namespace and, on the history and audit tabs,
// their first page: entity tabs show the snapshot. Audit filters are kept.
async function refresh() {
  try {
    await Promise.all([loadNamespace(), loadNamespaces()]);
    els.stale.hidden = true;
    state.history = newHistory();
    state.audit = { ...newAudit(), filters: state.audit.filters };
    if (state.tab === 'history') await loadHistory();
    if (state.tab === 'audit') await loadAudit();
  } finally {
    render();
  }
}

// poll notices when someone else changes the namespace, for instance the
// rollout controller advancing a stage. It only offers to reload, so that an
// open form is never thrown away.
async function poll() {
  const name = state.name;
  if (!name || !state.snapshot || document.hidden) return;
  try {
    const res = await rpc('/api/v1/AdminService/GetNamespace', { name }, { background: true });
    if (name !== state.name || !state.snapshot) return;
    const latest = num(res.namespace.revision);
    const shown = num(state.snapshot.revision);
    els.staleText.textContent = `${name} is now at revision ${latest}; this page shows revision ${shown}.`;
    els.stale.hidden = latest <= shown;
  } catch {
    // The next action the user takes reports the problem.
  }
}

// ------------------------------------------------------------------- render

const RENDERERS = {
  configs: renderConfigs,
  flags: renderFlags,
  experiments: renderExperiments,
  'rate-limits': renderRateLimits,
  'circuit-breakers': renderBreakers,
  history: renderHistory,
  audit: renderAudit,
};

function render() {
  renderSidebar();
  els.welcome.hidden = state.name !== '';
  els.view.hidden = state.name === '';
  if (state.name === '') return;
  renderHeader();
  renderTabs();
  renderPanel();
}

function renderSidebar() {
  fill(
    els.nsList,
    state.namespaces.map((ns) => {
      const active = ns.name === state.name;
      return h(
        'li',
        {},
        h(
          'a',
          { href: link(ns.name, state.tab), class: active ? 'active' : null, 'aria-current': active ? 'page' : null, title: ns.description || null },
          h('span', { class: 'ns-name' }, ns.name),
          h('span', { class: 'ns-rev' }, `r${ns.revision}`),
        ),
      );
    }),
  );
  els.nsEmpty.hidden = state.namespaces.length > 0;
}

function renderHeader() {
  const ns = state.namespace;
  const snap = state.snapshot;
  els.title.textContent = state.name;
  els.description.textContent = ns?.description ?? '';
  els.description.hidden = !ns?.description;
  fill(
    els.meta,
    snap ? h('span', { class: 'badge info' }, `revision ${snap.revision}`) : null,
    snap ? h('span', {}, 'updated ', ago(snap.updatedAt)) : null,
    ns ? h('span', {}, `created ${fmtTime(ns.createdAt)}`, ns.createdBy ? ` by ${ns.createdBy}` : '') : null,
  );
}

function renderTabs() {
  fill(
    els.tabs,
    TABS.map((t) => {
      const kind = KINDS[t.id];
      const count = kind && state.snapshot ? (kind.items(state.snapshot) ?? []).length : null;
      return h(
        'a',
        { href: link(state.name, t.id), class: 'tab', role: 'tab', 'aria-selected': String(t.id === state.tab) },
        t.label,
        count === null ? null : h('span', { class: 'count' }, count),
      );
    }),
  );
}

function renderPanel() {
  els.panel.replaceChildren();
  if (!state.snapshot) {
    els.panel.append(
      state.status === 'failed'
        ? h(
            'div',
            { class: 'empty' },
            h('p', {}, 'This namespace could not be loaded.'),
            h('button', { type: 'button', onclick: (e) => act(e.currentTarget, refresh) }, 'Try again'),
          )
        : h('p', { class: 'empty' }, 'Loading…'),
    );
    return;
  }
  RENDERERS[state.tab]();
}

// --------------------------------------------------------------- entity tabs

function isEditing(tab, key) {
  return state.editing?.tab === tab && state.editing.key === key;
}

function openEditor(tab, key, mode) {
  state.editing = { tab, key, mode };
  renderPanel();
  els.panel.querySelector('.editor input:not([readonly]):not([type="checkbox"]), .editor textarea')?.focus();
}

function toggleEditor(tab, key, mode) {
  if (isEditing(tab, key) && state.editing.mode === mode) closeEditor();
  else openEditor(tab, key, mode);
}

function closeEditor() {
  state.editing = null;
  renderPanel();
}

// renderEntityTab draws one entity kind: a toolbar, the form for a new
// entry when it is open, and a table whose rows open their own inline form.
function renderEntityTab(tab, columns, form, rowActions = () => []) {
  const kind = KINDS[tab];
  const items = kind.items(state.snapshot) ?? [];
  const body = h('tbody');
  const filter = h('input', {
    type: 'search',
    class: 'filter',
    placeholder: 'Filter by key',
    value: state.filters[tab] ?? '',
    'aria-label': `Filter ${kind.noun}s by key`,
  });
  filter.addEventListener('input', () => {
    state.filters[tab] = filter.value;
    applyFilter(body, filter.value);
  });
  els.panel.append(
    h(
      'div',
      { class: 'toolbar' },
      filter,
      h('span', { class: 'spacer' }),
      h('button', { type: 'button', class: 'primary', onclick: () => toggleEditor(tab, '', 'edit') }, `New ${kind.noun}`),
    ),
  );
  if (isEditing(tab, '')) els.panel.append(h('div', { class: 'card' }, form(null, 'edit')));
  if (items.length === 0) {
    els.panel.append(h('p', { class: 'empty' }, `No ${kind.noun}s in this namespace yet.`));
    return;
  }

  for (const item of items) {
    body.append(
      h(
        'tr',
        { dataset: { key: item.key } },
        columns.map((c) => h('td', { class: c.class ?? null }, c.cell(item))),
        h(
          'td',
          { class: 'actions' },
          rowActions(item),
          h('button', { type: 'button', onclick: () => toggleEditor(tab, item.key, 'edit') }, 'Edit'),
          h('button', { type: 'button', class: 'danger', onclick: (e) => act(e.currentTarget, () => removeEntry(tab, item)) }, 'Delete'),
        ),
      ),
    );
    if (isEditing(tab, item.key)) {
      body.append(
        h('tr', { class: 'editor-row', dataset: { key: item.key } }, h('td', { colspan: columns.length + 1 }, form(item, state.editing.mode))),
      );
    }
  }
  els.panel.append(
    h(
      'div',
      { class: 'table-wrap' },
      h(
        'table',
        { class: 'grid' },
        h('thead', {}, h('tr', {}, columns.map((c) => h('th', { class: c.class ?? null }, c.title)), h('th', { class: 'actions' }))),
        body,
      ),
    ),
  );
  applyFilter(body, filter.value);
}

function applyFilter(body, text) {
  const needle = text.trim().toLowerCase();
  for (const row of body.rows) row.hidden = needle !== '' && !row.dataset.key.toLowerCase().includes(needle);
}

function keyCell(item) {
  return [h('div', { class: 'key' }, item.key), item.description ? h('div', { class: 'sub' }, item.description) : null];
}

function updatedCell(item) {
  return [h('div', { class: 'num' }, `r${item.revision}`), h('div', { class: 'sub' }, item.updatedBy || 'unknown', ' · ', ago(item.updatedAt))];
}

// editorForm builds an inline form. submit returns the notice to show;
// when it fails the form stays open with its input intact.
function editorForm(title, fields, submit, submitLabel = 'Save') {
  const fieldset = h(
    'fieldset',
    {},
    h('div', { class: 'form-grid' }, fields),
    h(
      'div',
      { class: 'form-actions' },
      h('button', { type: 'submit', class: 'primary' }, submitLabel),
      h('button', { type: 'button', onclick: closeEditor }, 'Cancel'),
    ),
  );
  const form = h('form', { class: 'editor', autocomplete: 'off' }, h('h3', {}, title), fieldset);
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    fieldset.disabled = true;
    try {
      const message = await submit();
      state.editing = null;
      notify(message);
      await refresh();
    } catch (err) {
      showError(err);
    } finally {
      fieldset.disabled = false;
    }
  });
  return form;
}

function field(label, control, { hint = null, wide = false } = {}) {
  return h(
    'label',
    { class: wide ? 'field wide' : 'field' },
    h('span', { class: 'label' }, label),
    control,
    hint === null || hint instanceof Node ? hint : h('small', { class: 'hint' }, hint),
  );
}

// group is a field made of several controls, which a label must not wrap.
function group(label, content) {
  return h('div', { class: 'field wide' }, h('span', { class: 'label' }, label), content);
}

function checkField(label, control) {
  return h('label', { class: 'field check' }, control, h('span', {}, label));
}

function textInput(name, value, props = {}) {
  return h('input', { type: 'text', name, value: value ?? '', spellcheck: 'false', ...props });
}

function numberInput(name, value, props = {}) {
  return h('input', { type: 'number', name, value: value === null || value === undefined ? '' : String(value), ...props });
}

function checkbox(name, checked) {
  return h('input', { type: 'checkbox', name, checked });
}

function keyInput(item, placeholder) {
  return textInput('key', item?.key, { required: true, readOnly: item !== null, maxlength: 256, placeholder });
}

// enabledSwitch flips an entry's enabled state in place.
function enabledSwitch(tab, item) {
  const input = h('input', {
    type: 'checkbox',
    class: 'switch',
    checked: item.enabled,
    'aria-label': `${item.key} enabled`,
    title: item.enabled ? 'Enabled: click to disable' : 'Disabled: click to enable',
  });
  input.addEventListener('change', async () => {
    input.disabled = true;
    try {
      await setEnabled(tab, item, input.checked);
      notify(`${input.checked ? 'Enabled' : 'Disabled'} ${KINDS[tab].noun} ${item.key}.`);
      await refresh();
    } catch (err) {
      input.checked = item.enabled;
      showError(err);
    } finally {
      input.disabled = false;
    }
  });
  return input;
}

// setEnabled rewrites an entry with only its enabled state changed. A put
// replaces the whole entry, so every other field goes back as it was, and the
// entry's revision makes the write fail rather than overwrite a change made
// in the meantime.
function setEnabled(tab, item, enabled) {
  const namespace = state.name;
  switch (tab) {
    case 'flags':
      // Leaving out rolloutPercent and salt keeps them as they are.
      return rpc('/api/v1/AdminService/PutFlag', {
        namespace,
        key: item.key,
        enabled,
        description: item.description,
        allowlist: item.allowlist,
        expectedRevision: item.revision,
      });
    case 'experiments':
      return rpc('/api/v1/AdminService/PutExperiment', {
        namespace,
        key: item.key,
        enabled,
        description: item.description,
        salt: item.salt,
        // An unset payload arrives as null; sent back, it would be stored
        // as a JSON null payload.
        variants: item.variants.map((v) => ({ name: v.name, weight: v.weight, payload: v.payload ?? undefined })),
        expectedRevision: item.revision,
      });
    case 'rate-limits':
      return rpc('/api/v1/AdminService/PutRateLimit', {
        namespace,
        key: item.key,
        enabled,
        description: item.description,
        requestsPerSecond: item.requestsPerSecond,
        burst: item.burst,
        expectedRevision: item.revision,
      });
    case 'circuit-breakers':
      return rpc('/api/v1/AdminService/PutCircuitBreaker', {
        namespace,
        key: item.key,
        enabled,
        description: item.description,
        failureRateThreshold: item.failureRateThreshold,
        minRequests: item.minRequests,
        window: item.window,
        openDuration: item.openDuration,
        halfOpenMaxRequests: item.halfOpenMaxRequests,
        expectedRevision: item.revision,
      });
    default:
      throw new Error(`${tab} have no enabled state`);
  }
}

async function removeEntry(tab, item) {
  const noun = KINDS[tab].noun;
  if (!confirm(`Delete ${noun} "${item.key}" from ${state.name}?\n\nClients stop seeing it as soon as the change reaches them.`)) return;
  const res = await deleteEntry(tab, item);
  if (isEditing(tab, item.key)) state.editing = null;
  notify(`Deleted ${noun} ${item.key} at revision ${res.revision}.`);
  await refresh();
}

// deleteEntry deletes an entry unless it changed since it was loaded.
function deleteEntry(tab, item) {
  const namespace = state.name;
  const key = item.key;
  const expectedRevision = item.revision;
  switch (tab) {
    case 'configs':
      return rpc('/api/v1/AdminService/DeleteConfig', { namespace, key, expectedRevision });
    case 'flags':
      return rpc('/api/v1/AdminService/DeleteFlag', { namespace, key, expectedRevision });
    case 'experiments':
      return rpc('/api/v1/AdminService/DeleteExperiment', { namespace, key, expectedRevision });
    case 'rate-limits':
      return rpc('/api/v1/AdminService/DeleteRateLimit', { namespace, key, expectedRevision });
    case 'circuit-breakers':
      return rpc('/api/v1/AdminService/DeleteCircuitBreaker', { namespace, key, expectedRevision });
    default:
      throw new Error(`cannot delete ${tab}`);
  }
}

// ------------------------------------------------------------------ configs

function renderConfigs() {
  renderEntityTab(
    'configs',
    [
      { title: 'Key', cell: keyCell },
      { title: 'Value', cell: (c) => h('pre', { class: 'json' }, JSON.stringify(c.value, null, 2)) },
      { title: 'Updated', cell: updatedCell },
    ],
    configForm,
  );
}

function configForm(c) {
  const key = keyInput(c, 'payments.timeout_ms');
  const value = h('textarea', {
    name: 'value',
    rows: 6,
    class: 'mono',
    spellcheck: 'false',
    required: true,
    value: c ? JSON.stringify(c.value, null, 2) : '',
  });
  const description = textInput('description', c?.description, { maxlength: 1024 });
  return editorForm(
    c ? `Edit config ${c.key}` : 'New config',
    [
      field('Key', key),
      field('Value', value, { hint: 'Any JSON value: 250, "text", true, [1, 2] or {"a": 1}.', wide: true }),
      field('Description', description, { wide: true }),
    ],
    async () => {
      let parsed;
      try {
        parsed = JSON.parse(value.value);
      } catch (err) {
        throw new Error(`The value is not valid JSON: ${err.message}`);
      }
      const res = await rpc('/api/v1/AdminService/PutConfig', {
        namespace: state.name,
        key: key.value.trim(),
        value: parsed,
        description: description.value,
        expectedRevision: c ? c.revision : undefined,
      });
      return `Saved config ${res.config.key} at revision ${res.config.revision}.`;
    },
  );
}

// -------------------------------------------------------------------- flags

function renderFlags() {
  renderEntityTab(
    'flags',
    [
      { title: 'Flag', cell: keyCell },
      { title: 'Enabled', cell: (f) => enabledSwitch('flags', f) },
      { title: 'Rollout', cell: rolloutCell },
      { title: 'Staged rollout', cell: planCell },
      { title: 'Allowlist', cell: allowlistCell },
      { title: 'Updated', cell: updatedCell },
    ],
    (f, mode) => (mode === 'rollout' ? rolloutForm(f) : flagForm(f)),
    rolloutActions,
  );
}

function rolloutCell(f) {
  return h(
    'div',
    { class: 'percent' },
    h('meter', { min: 0, max: 100, value: f.rolloutPercent, 'aria-label': `${f.key} rollout` }),
    h('span', {}, fmtPercent(f.rolloutPercent)),
  );
}

function planCell(f) {
  const plan = f.rollout;
  if (!plan) return h('span', { class: 'muted' }, '—');
  const { label, tone } = ROLLOUT_STATES[plan.state] ?? { label: 'unknown', tone: '' };
  const stages = plan.stages ?? [];
  const current = plan.currentStage ?? 0;
  return [
    h('div', {}, h('span', { class: `badge ${tone}` }, label), ` stage ${current + 1}/${stages.length}`),
    h(
      'div',
      { class: 'stages small' },
      stages.map((s, i) => {
        const cls = i === current ? 'stage current' : i < current ? 'stage done' : 'stage';
        return h('span', { class: cls, title: s.duration ? `for ${formatDuration(durationNs(s.duration))}` : null }, fmtPercent(s.percent));
      }),
    ),
    nextStage(plan),
  ];
}

// nextStage says when an active plan moves on.
function nextStage(plan) {
  const stages = plan.stages ?? [];
  if (plan.state !== 'ROLLOUT_STATE_ACTIVE' || plan.currentStage >= stages.length - 1) return null;
  const wait = durationNs(stages[plan.currentStage].duration);
  if (!wait) return h('div', { class: 'sub' }, 'advances manually');
  const due = new Date(new Date(plan.stageStartedAt).getTime() + wait / 1e6);
  if (due <= new Date()) return h('div', { class: 'sub' }, 'next stage due now');
  return h('div', { class: 'sub' }, 'next stage ', h('time', { datetime: due.toISOString(), title: fmtTime(due.toISOString()) }, relTime(due)));
}

function allowlistCell(f) {
  const units = f.allowlist ?? [];
  if (units.length === 0) return h('span', { class: 'muted' }, '—');
  const shown = units.slice(0, 3);
  return h(
    'div',
    { class: 'chips', title: units.join('\n') },
    shown.map((u) => h('span', { class: 'chip' }, u)),
    units.length > shown.length ? h('span', { class: 'chip muted' }, `+${units.length - shown.length}`) : null,
  );
}

function rolloutActions(f) {
  const plan = f.rollout;
  if (!planOwns(plan)) {
    return h('button', { type: 'button', onclick: () => toggleEditor('flags', f.key, 'rollout') }, 'Start rollout');
  }
  return [
    h('button', { type: 'button', onclick: (e) => act(e.currentTarget, () => rolloutStep(f, 'advance')) }, 'Advance'),
    plan.state === 'ROLLOUT_STATE_PAUSED'
      ? h('button', { type: 'button', onclick: (e) => act(e.currentTarget, () => rolloutStep(f, 'resume')) }, 'Resume')
      : h('button', { type: 'button', onclick: (e) => act(e.currentTarget, () => rolloutStep(f, 'pause')) }, 'Pause'),
    h('button', { type: 'button', class: 'danger', onclick: (e) => act(e.currentTarget, () => rolloutStep(f, 'abort')) }, 'Abort'),
  ];
}

function rolloutCall(verb, flag) {
  const namespace = state.name;
  switch (verb) {
    case 'advance':
      return rpc('/api/v1/AdminService/AdvanceRollout', { namespace, flag: flag.key });
    case 'pause':
      return rpc('/api/v1/AdminService/PauseRollout', { namespace, flag: flag.key });
    case 'resume':
      return rpc('/api/v1/AdminService/ResumeRollout', { namespace, flag: flag.key });
    case 'abort':
      return rpc('/api/v1/AdminService/AbortRollout', { namespace, flag: flag.key });
    default:
      throw new Error(`unknown rollout action ${verb}`);
  }
}

async function rolloutStep(flag, verb) {
  const plan = flag.rollout;
  if (verb === 'advance') {
    const next = plan.stages[plan.currentStage + 1];
    const target = next ? ` (${fmtPercent(next.percent)})` : '';
    if (!confirm(`Advance the rollout of ${flag.key} to stage ${plan.currentStage + 2} of ${plan.stages.length}${target}?`)) return;
  }
  if (verb === 'abort' && !confirm(`Abort the rollout of ${flag.key}?\n\nIts rollout drops to 0%: only allowlisted units keep the flag on.`)) {
    return;
  }
  const res = await rolloutCall(verb, flag);
  notify(`${ROLLOUT_DONE[verb]} the rollout of ${flag.key}: ${describePlan(res.flag)}.`);
  await refresh();
}

function flagForm(f) {
  const owned = planOwns(f?.rollout);
  const key = keyInput(f, 'new-checkout');
  const enabled = checkbox('enabled', f ? f.enabled : false);
  const percent = numberInput('rolloutPercent', f ? f.rolloutPercent : 100, {
    required: true,
    min: 0,
    max: 100,
    step: 0.01,
    disabled: owned,
  });
  const salt = textInput('salt', f?.salt, { maxlength: 256, placeholder: 'defaults to the key' });
  const allowlist = h('textarea', {
    name: 'allowlist',
    rows: 3,
    class: 'mono',
    spellcheck: 'false',
    placeholder: 'one unit ID per line',
    value: (f?.allowlist ?? []).join('\n'),
  });
  const description = textInput('description', f?.description, { maxlength: 1024 });
  return editorForm(
    f ? `Edit flag ${f.key}` : 'New flag',
    [
      field('Key', key),
      field('Rollout %', percent, { hint: owned ? 'Set by the staged rollout.' : 'Share of units that see the flag on, 0 to 100.' }),
      field('Salt', salt, { hint: 'Changing it reshuffles which units are in the rollout.' }),
      checkField('Enabled', enabled),
      field('Allowlist', allowlist, { hint: 'Units that always see the flag on while it is enabled.', wide: true }),
      field('Description', description, { wide: true }),
    ],
    async () => {
      const value = Number(percent.value);
      // Only a changed percentage is sent: an unset one keeps the current
      // value, and a rollout in progress refuses any other.
      const changed = !owned && (f === null || value !== f.rolloutPercent);
      const res = await rpc('/api/v1/AdminService/PutFlag', {
        namespace: state.name,
        key: key.value.trim(),
        enabled: enabled.checked,
        description: description.value,
        rolloutPercent: changed ? value : undefined,
        salt: salt.value.trim(),
        allowlist: splitList(allowlist.value),
        expectedRevision: f ? f.revision : undefined,
      });
      return `Saved flag ${res.flag.key} at revision ${res.flag.revision}.`;
    },
  );
}

function rolloutForm(f) {
  const stages = textInput('stages', '', { required: true, class: 'mono', placeholder: '1:10m,5:10m,25:30m,100' });
  const preview = h(
    'small',
    { class: 'hint' },
    'Stages as percent:duration, comma-separated. A stage without a duration waits for a manual advance; the last stage completes the rollout.',
  );
  stages.addEventListener('input', () => {
    try {
      preview.textContent = describeStages(parseStages(stages.value));
      preview.classList.remove('invalid');
    } catch (err) {
      preview.textContent = err.message;
      preview.classList.add('invalid');
    }
  });
  return editorForm(
    `Start a staged rollout of ${f.key}`,
    [field('Stages', stages, { hint: preview, wide: true })],
    async () => {
      const parsed = parseStages(stages.value);
      const res = await rpc('/api/v1/AdminService/StartRollout', {
        namespace: state.name,
        flag: f.key,
        stages: parsed.map((s) => ({ percent: s.percent, duration: s.duration ? protoDuration(s.duration) : undefined })),
      });
      return `Started the rollout of ${f.key}: ${describePlan(res.flag)}.`;
    },
    'Start rollout',
  );
}

// -------------------------------------------------------------- experiments

function renderExperiments() {
  renderEntityTab(
    'experiments',
    [
      { title: 'Experiment', cell: keyCell },
      { title: 'Enabled', cell: (e) => enabledSwitch('experiments', e) },
      { title: 'Variants', cell: variantsCell },
      { title: 'Salt', cell: (e) => h('code', {}, e.salt) },
      { title: 'Updated', cell: updatedCell },
    ],
    experimentForm,
  );
}

function variantsCell(e) {
  const variants = e.variants ?? [];
  const total = variants.reduce((sum, v) => sum + v.weight, 0);
  return h(
    'div',
    { class: 'chips' },
    variants.map((v) =>
      h(
        'span',
        { class: 'chip', title: v.payload === null || v.payload === undefined ? null : `payload ${JSON.stringify(v.payload)}` },
        v.name,
        h('span', { class: 'muted' }, total ? fmtPercent((v.weight / total) * 100) : '0%'),
      ),
    ),
  );
}

function experimentForm(e) {
  const key = keyInput(e, 'checkout-button');
  const enabled = checkbox('enabled', e ? e.enabled : false);
  const salt = textInput('salt', e?.salt, { maxlength: 256, placeholder: 'defaults to the key' });
  const description = textInput('description', e?.description, { maxlength: 1024 });
  const variants = variantsEditor(
    e
      ? e.variants
      : [
          { name: 'control', weight: 50 },
          { name: 'treatment', weight: 50 },
        ],
  );
  return editorForm(
    e ? `Edit experiment ${e.key}` : 'New experiment',
    [
      field('Key', key),
      field('Salt', salt, { hint: 'Changing it reassigns every unit.' }),
      checkField('Enabled', enabled),
      field('Description', description, { wide: true }),
      group('Variants', variants.node),
    ],
    async () => {
      const list = variants.read();
      const res = await rpc('/api/v1/AdminService/PutExperiment', {
        namespace: state.name,
        key: key.value.trim(),
        enabled: enabled.checked,
        description: description.value,
        salt: salt.value.trim(),
        variants: list.map((v) => ({ name: v.name, weight: v.weight, payload: v.payload })),
        expectedRevision: e ? e.revision : undefined,
      });
      return `Saved experiment ${res.experiment.key} at revision ${res.experiment.revision}.`;
    },
  );
}

// variantsEditor edits variants as rows of name, weight and optional JSON
// payload, showing each variant's share as the weights change.
function variantsEditor(initial) {
  const rows = [];
  const list = h(
    'div',
    { class: 'variants' },
    h(
      'div',
      { class: 'variant-row variant-head' },
      h('span', {}, 'Name'),
      h('span', {}, 'Weight'),
      h('span', { class: 'share' }, 'Share'),
      h('span', {}, 'Payload (JSON, optional)'),
      h('span', {}),
    ),
  );
  const updateShares = () => {
    const total = rows.reduce((sum, r) => sum + (Number(r.weight.value) || 0), 0);
    for (const r of rows) r.share.textContent = total ? fmtPercent(((Number(r.weight.value) || 0) / total) * 100) : '—';
  };
  const add = (v) => {
    const row = {
      name: textInput('variant', v.name, { required: true, maxlength: 64, placeholder: 'control', 'aria-label': 'Variant name' }),
      weight: numberInput('weight', v.weight, { required: true, min: 0, step: 1, 'aria-label': 'Variant weight' }),
      share: h('span', { class: 'share' }),
      payload: textInput('payload', v.payload === null || v.payload === undefined ? '' : JSON.stringify(v.payload), {
        class: 'mono',
        placeholder: '{"color": "green"}',
        'aria-label': 'Variant payload',
      }),
    };
    const remove = h('button', { type: 'button', class: 'ghost small', 'aria-label': 'Remove variant' }, 'Remove');
    row.node = h('div', { class: 'variant-row' }, row.name, row.weight, row.share, row.payload, remove);
    remove.addEventListener('click', () => {
      rows.splice(rows.indexOf(row), 1);
      row.node.remove();
      updateShares();
    });
    row.weight.addEventListener('input', updateShares);
    rows.push(row);
    list.append(row.node);
    updateShares();
  };
  initial.forEach(add);
  return {
    node: h('div', {}, list, h('button', { type: 'button', class: 'small', onclick: () => add({ name: '', weight: 1 }) }, 'Add variant')),
    read: () =>
      rows.map((r) => {
        const text = r.payload.value.trim();
        let payload;
        if (text !== '') {
          try {
            payload = JSON.parse(text);
          } catch (err) {
            throw new Error(`The payload of variant "${r.name.value}" is not valid JSON: ${err.message}`);
          }
        }
        return { name: r.name.value.trim(), weight: Number(r.weight.value), payload };
      }),
  };
}

// -------------------------------------------------------------- rate limits

function renderRateLimits() {
  renderEntityTab(
    'rate-limits',
    [
      { title: 'Rate limit', cell: keyCell },
      { title: 'Enabled', cell: (r) => enabledSwitch('rate-limits', r) },
      { title: 'Rate', class: 'num', cell: (r) => `${r.requestsPerSecond} req/s` },
      { title: 'Burst', class: 'num', cell: (r) => String(r.burst) },
      { title: 'Updated', cell: updatedCell },
    ],
    rateLimitForm,
  );
}

function rateLimitForm(r) {
  const key = keyInput(r, 'checkout');
  const enabled = checkbox('enabled', r ? r.enabled : true);
  const rate = numberInput('requestsPerSecond', r ? r.requestsPerSecond : 100, { required: true, min: 0, step: 'any' });
  const burst = numberInput('burst', r ? r.burst : 200, { required: true, min: 1, step: 1 });
  const description = textInput('description', r?.description, { maxlength: 1024 });
  return editorForm(
    r ? `Edit rate limit ${r.key}` : 'New rate limit',
    [
      field('Key', key, { hint: 'The route or method clients limit.' }),
      field('Requests per second', rate, { hint: 'Sustained rate; fractions such as 0.5 work.' }),
      field('Burst', burst, { hint: 'Requests allowed at once after a quiet period.' }),
      checkField('Enabled', enabled),
      field('Description', description, { wide: true }),
    ],
    async () => {
      const res = await rpc('/api/v1/AdminService/PutRateLimit', {
        namespace: state.name,
        key: key.value.trim(),
        enabled: enabled.checked,
        description: description.value,
        requestsPerSecond: Number(rate.value),
        burst: Number(burst.value),
        expectedRevision: r ? r.revision : undefined,
      });
      return `Saved rate limit ${res.rateLimit.key} at revision ${res.rateLimit.revision}.`;
    },
  );
}

// --------------------------------------------------------- circuit breakers

function renderBreakers() {
  renderEntityTab(
    'circuit-breakers',
    [
      { title: 'Circuit breaker', cell: keyCell },
      { title: 'Enabled', cell: (b) => enabledSwitch('circuit-breakers', b) },
      {
        title: 'Opens at',
        cell: (b) => [
          h('div', {}, `${fmtPercent(b.failureRateThreshold * 100)} failures`),
          h('div', { class: 'sub' }, `once ${b.minRequests} calls are in the window`),
        ],
      },
      { title: 'Window', class: 'num', cell: (b) => formatDuration(durationNs(b.window)) },
      { title: 'Open for', class: 'num', cell: (b) => formatDuration(durationNs(b.openDuration)) },
      { title: 'Trial calls', class: 'num', cell: (b) => String(b.halfOpenMaxRequests) },
      { title: 'Updated', cell: updatedCell },
    ],
    breakerForm,
  );
}

function breakerForm(b) {
  const key = keyInput(b, 'payments');
  const enabled = checkbox('enabled', b ? b.enabled : true);
  const threshold = numberInput('failureRateThreshold', b ? b.failureRateThreshold : 0.5, { required: true, min: 0, max: 1, step: 'any' });
  const minRequests = numberInput('minRequests', b ? b.minRequests : 20, { required: true, min: 1, step: 1 });
  const windowInput = textInput('window', b ? formatDuration(durationNs(b.window)) : '10s', { required: true, placeholder: '10s' });
  const openFor = textInput('openDuration', b ? formatDuration(durationNs(b.openDuration)) : '30s', { required: true, placeholder: '30s' });
  const trials = numberInput('halfOpenMaxRequests', b ? b.halfOpenMaxRequests : 5, { required: true, min: 1, step: 1 });
  const description = textInput('description', b?.description, { maxlength: 1024 });
  return editorForm(
    b ? `Edit circuit breaker ${b.key}` : 'New circuit breaker',
    [
      field('Key', key, { hint: 'The dependency clients protect, e.g. payments.' }),
      field('Failure rate threshold', threshold, { hint: 'Fraction of failed calls, above 0 and at most 1, that opens it.' }),
      field('Minimum calls', minRequests, { hint: 'Calls in the window before the rate counts.' }),
      field('Window', windowInput, { hint: 'Rolling window, 1s to 1h.' }),
      field('Open for', openFor, { hint: 'Time before trial calls, 100ms to 1h.' }),
      field('Trial calls', trials, { hint: 'Half-open calls that must all succeed to close it.' }),
      checkField('Enabled', enabled),
      field('Description', description, { wide: true }),
    ],
    async () => {
      const res = await rpc('/api/v1/AdminService/PutCircuitBreaker', {
        namespace: state.name,
        key: key.value.trim(),
        enabled: enabled.checked,
        description: description.value,
        failureRateThreshold: Number(threshold.value),
        minRequests: Number(minRequests.value),
        window: protoDuration(parseDuration(windowInput.value)),
        openDuration: protoDuration(parseDuration(openFor.value)),
        halfOpenMaxRequests: Number(trials.value),
        expectedRevision: b ? b.revision : undefined,
      });
      return `Saved circuit breaker ${res.circuitBreaker.key} at revision ${res.circuitBreaker.revision}.`;
    },
  );
}

// ------------------------------------------------------------------ history

function newHistory() {
  // cursors[i] is the beforeRevision of page i; 0 starts at the latest.
  return { cursors: [0], page: 0, revisions: null, next: 0, failed: false, open: 0, detail: null };
}

async function loadHistory() {
  const hist = state.history;
  hist.failed = false;
  try {
    const res = await rpc('/api/v1/AdminService/ListRevisions', {
      namespace: state.name,
      pageSize: PAGE_SIZE,
      beforeRevision: hist.cursors[hist.page],
    });
    if (hist !== state.history) return;
    hist.revisions = res.revisions ?? [];
    hist.next = num(res.nextBeforeRevision);
  } catch (err) {
    hist.failed = true;
    throw err;
  }
}

async function pageHistory(step) {
  const hist = state.history;
  if (step > 0) hist.cursors[hist.page + 1] = hist.next;
  hist.page += step;
  hist.open = 0;
  hist.detail = null;
  try {
    await loadHistory();
  } finally {
    renderPanel();
  }
}

function pager(newer, older, go) {
  return h(
    'div',
    { class: 'pager' },
    h('button', { type: 'button', disabled: !newer, onclick: (e) => act(e.currentTarget, () => go(-1)) }, '← Newer'),
    h('button', { type: 'button', disabled: !older, onclick: (e) => act(e.currentTarget, () => go(1)) }, 'Older →'),
  );
}

function renderHistory() {
  const hist = state.history;
  els.panel.append(
    h(
      'div',
      { class: 'toolbar' },
      h('p', { class: 'muted' }, 'Every change writes a new revision. Open one to see what it changed, compare it with now, or roll back to it.'),
      h('span', { class: 'spacer' }),
      pager(hist.page > 0, hist.next > 0, pageHistory),
    ),
  );
  if (!hist.revisions) {
    els.panel.append(h('p', { class: 'empty' }, hist.failed ? 'The history could not be loaded.' : 'Loading…'));
    return;
  }
  if (hist.revisions.length === 0) {
    els.panel.append(h('p', { class: 'empty' }, 'No revisions are recorded for this namespace.'));
    return;
  }
  const current = num(state.snapshot.revision);
  const body = h('tbody');
  for (const rev of hist.revisions) {
    const r = num(rev.revision);
    const open = hist.open === r;
    body.append(
      h(
        'tr',
        { class: open ? 'selected' : null },
        h('td', { class: 'num' }, `r${r}`, r === current ? h('span', { class: 'badge info' }, 'current') : null),
        h('td', { class: 'num' }, at(rev.createdAt)),
        h('td', {}, rev.actor || '—'),
        h('td', {}, rev.summary),
        h(
          'td',
          { class: 'actions' },
          h('button', { type: 'button', 'aria-expanded': String(open), onclick: () => toggleRevision(r) }, open ? 'Hide' : 'Changes'),
        ),
      ),
    );
    if (open) body.append(h('tr', { class: 'detail-row' }, h('td', { colspan: 5 }, revisionDetail(r, current))));
  }
  els.panel.append(
    h(
      'div',
      { class: 'table-wrap' },
      h(
        'table',
        { class: 'grid' },
        h('thead', {}, h('tr', {}, h('th', {}, 'Revision'), h('th', {}, 'When'), h('th', {}, 'Actor'), h('th', {}, 'Summary'), h('th', { class: 'actions' }))),
        body,
      ),
    ),
  );
}

function toggleRevision(r) {
  const hist = state.history;
  if (hist.open === r) {
    hist.open = 0;
    hist.detail = null;
    renderPanel();
    return;
  }
  showRevision(r, 'changes');
}

// showRevision opens one view of a revision: what it changed, what changed
// since, or the whole state it recorded.
async function showRevision(r, view) {
  const hist = state.history;
  hist.open = r;
  hist.detail = { revision: r, view, data: null, error: null };
  renderPanel();
  const detail = hist.detail;
  try {
    if (view === 'state') {
      const res = await rpc('/api/v1/AdminService/GetRevision', { namespace: state.name, revision: r });
      detail.data = res.revision.snapshot;
    } else if (view === 'since') {
      const res = await rpc('/api/v1/AdminService/DiffRevisions', { namespace: state.name, fromRevision: r, toRevision: 0 });
      detail.data = res.changes ?? [];
    } else if (r > 1) {
      const res = await rpc('/api/v1/AdminService/DiffRevisions', { namespace: state.name, fromRevision: r - 1, toRevision: r });
      detail.data = res.changes ?? [];
    } else {
      // Revision 1 is the creation of the empty namespace.
      detail.data = [];
    }
  } catch (err) {
    detail.error = err;
  }
  if (hist.detail === detail) renderPanel();
}

function revisionDetail(r, current) {
  const detail = state.history.detail;
  const views = [
    ['changes', 'What it changed'],
    ['since', 'Changes since'],
    ['state', 'Full state'],
  ];
  return [
    h(
      'div',
      { class: 'detail-bar' },
      h(
        'div',
        { class: 'segmented', role: 'group', 'aria-label': `Revision ${r}` },
        views.map(([id, label]) =>
          h(
            'button',
            { type: 'button', class: detail.view === id ? 'active' : null, 'aria-pressed': String(detail.view === id), onclick: () => showRevision(r, id) },
            label,
          ),
        ),
      ),
      h('span', { class: 'spacer' }),
      r === current
        ? h('span', { class: 'muted small' }, 'This is the current revision.')
        : h('button', { type: 'button', class: 'danger', onclick: (e) => act(e.currentTarget, () => rollback(r)) }, 'Roll back to this revision'),
    ),
    revisionView(detail, r, current),
  ];
}

function revisionView(detail, r, current) {
  if (detail.error) {
    const code = detail.error instanceof ApiError ? `${detail.error.code}: ` : '';
    return h('p', { class: 'error-text' }, code, detail.error.message);
  }
  if (!detail.data) return h('p', { class: 'muted' }, 'Loading…');
  switch (detail.view) {
    case 'state':
      return snapshotView(detail.data);
    case 'since':
      return [
        h('p', { class: 'caption' }, `From revision ${r} to the current state (revision ${current}). Rolling back to revision ${r} reverts these changes.`),
        changesList(detail.data, 'Nothing has changed since this revision.'),
      ];
    default:
      return changesList(detail.data, r === 1 ? 'Revision 1 created the namespace.' : 'No entry changed in this revision.');
  }
}

async function rollback(r) {
  const current = num(state.snapshot.revision);
  const ok = confirm(
    `Roll back ${state.name} to revision ${r}?\n\n` +
      `This writes a new revision that restores every config, flag, experiment and policy to how it was at revision ${r}. ` +
      `It is refused if anything changed after revision ${current}, the state this page shows.`,
  );
  if (!ok) return;
  const res = await rpc('/api/v1/AdminService/Rollback', { namespace: state.name, toRevision: r, expectedRevision: current });
  const n = (res.changes ?? []).length;
  notify(
    n === 0
      ? `Nothing to roll back: revision ${r} matches the current state.`
      : `Rolled back to revision ${r}: revision ${res.revision} changed ${n} ${n === 1 ? 'entry' : 'entries'}.`,
  );
  await refresh();
}

function snapshotView(snap) {
  const sections = [
    ['Configs', snap.configs, (c) => JSON.stringify(c.value)],
    ['Flags', snap.flags, (f) => `${f.enabled ? 'on' : 'off'}, ${fmtPercent(f.rolloutPercent)}${(f.allowlist ?? []).length ? `, ${f.allowlist.length} allowlisted` : ''}`],
    ['Experiments', snap.experiments, (e) => `${e.enabled ? 'on' : 'off'}, ${(e.variants ?? []).map((v) => `${v.name}:${v.weight}`).join(' ')}`],
    ['Rate limits', snap.rateLimits, (r) => `${r.enabled ? 'on' : 'off'}, ${r.requestsPerSecond} req/s, burst ${r.burst}`],
    [
      'Circuit breakers',
      snap.circuitBreakers,
      (b) => `${b.enabled ? 'on' : 'off'}, opens at ${fmtPercent(b.failureRateThreshold * 100)} over ${formatDuration(durationNs(b.window))}`,
    ],
  ].filter(([, items]) => (items ?? []).length > 0);
  if (sections.length === 0) return h('p', { class: 'muted' }, 'The namespace was empty at this revision.');
  return sections.map(([title, items, describe]) =>
    h(
      'div',
      { class: 'snapshot-section' },
      h('h4', {}, `${title} (${items.length})`),
      h('table', { class: 'fields' }, h('tbody', {}, items.map((item) => h('tr', {}, h('th', { scope: 'row' }, item.key), h('td', {}, describe(item)))))),
    ),
  );
}

// -------------------------------------------------------------------- diffs

function changesList(changes, emptyText) {
  if (changes.length === 0) return h('p', { class: 'muted' }, emptyText);
  return h(
    'div',
    { class: 'changes' },
    changes.map((c) => changeCard(CHANGE_TYPES[c.type] ?? 'modified', c.entityType, c.key, c.before, c.after)),
  );
}

// changeCard shows how one entry changed, field by field, as stored.
function changeCard(type, entityType, key, before, after) {
  return h(
    'div',
    { class: `change ${type}` },
    h(
      'div',
      { class: 'change-head' },
      h('span', { class: `badge ${CHANGE_TONES[type]}` }, type),
      h('span', { class: 'entity' }, ENTITY_LABELS[entityType] ?? entityType),
      h('strong', { class: 'key' }, key),
    ),
    fieldTable(type, before, after),
  );
}

function fieldTable(type, before, after) {
  const b = isObject(before) ? before : {};
  const a = isObject(after) ? after : {};
  const names = [...new Set([...Object.keys(b), ...Object.keys(a)])];
  const ordered = [...names.filter((n) => !META_FIELDS.includes(n)), ...names.filter((n) => META_FIELDS.includes(n))];
  const rows = ordered
    .filter((n) => type !== 'modified' || JSON.stringify(b[n]) !== JSON.stringify(a[n]))
    .map((n) =>
      h(
        'tr',
        { class: META_FIELDS.includes(n) ? 'meta' : null },
        h('th', { scope: 'row' }, n),
        type === 'added' ? null : h('td', { class: 'before' }, fieldText(n, b[n])),
        type === 'removed' ? null : h('td', { class: 'after' }, fieldText(n, a[n])),
      ),
    );
  return h(
    'table',
    { class: 'fields' },
    h('thead', {}, h('tr', {}, h('th', {}, 'Field'), type === 'added' ? null : h('th', {}, 'Before'), type === 'removed' ? null : h('th', {}, 'After'))),
    h('tbody', {}, rows),
  );
}

function isObject(v) {
  return v !== null && typeof v === 'object' && !Array.isArray(v);
}

function fieldText(name, value) {
  if (value === undefined) return '—';
  if (DURATION_FIELDS.has(name) && typeof value === 'number') return formatDuration(value);
  if (name === 'rollout' && isObject(value)) return describeStoredPlan(value);
  return JSON.stringify(value);
}

// -------------------------------------------------------------------- audit

function newAudit() {
  return {
    filters: { entityType: '', entityKey: '', actor: '', since: '', until: '' },
    tokens: [''], // tokens[i] is the pageToken of page i
    page: 0,
    events: null,
    next: '',
    failed: false,
    open: '',
  };
}

async function loadAudit() {
  const audit = state.audit;
  const f = audit.filters;
  audit.failed = false;
  try {
    const res = await rpc('/api/v1/AdminService/ListAuditEvents', {
      namespace: state.name,
      entityType: f.entityType,
      entityKey: f.entityKey,
      actor: f.actor,
      since: f.since ? new Date(f.since).toISOString() : undefined,
      until: f.until ? new Date(f.until).toISOString() : undefined,
      pageSize: PAGE_SIZE,
      pageToken: audit.tokens[audit.page],
    });
    if (audit !== state.audit) return;
    audit.events = res.events ?? [];
    audit.next = res.nextPageToken ?? '';
  } catch (err) {
    audit.failed = true;
    throw err;
  }
}

async function pageAudit(step) {
  const audit = state.audit;
  if (step > 0) audit.tokens[audit.page + 1] = audit.next;
  audit.page += step;
  audit.open = '';
  try {
    await loadAudit();
  } finally {
    renderPanel();
  }
}

async function filterAudit(filters) {
  state.audit = { ...newAudit(), filters };
  renderPanel();
  try {
    await loadAudit();
  } catch (err) {
    showError(err);
  }
  renderPanel();
}

function renderAudit() {
  const audit = state.audit;
  const f = audit.filters;
  const type = h(
    'select',
    { name: 'entityType', value: f.entityType },
    h('option', { value: '' }, 'Any'),
    Object.entries(ENTITY_LABELS).map(([value, label]) => h('option', { value }, label)),
  );
  const key = textInput('entityKey', f.entityKey, { placeholder: 'Any key' });
  const actor = textInput('actor', f.actor, { placeholder: 'Anyone' });
  const since = h('input', { type: 'datetime-local', name: 'since', value: f.since, step: 1 });
  const until = h('input', { type: 'datetime-local', name: 'until', value: f.until, step: 1 });
  const form = h(
    'form',
    { class: 'filters', autocomplete: 'off' },
    field('Entity type', type),
    field('Key', key),
    field('Actor', actor),
    field('Since', since),
    field('Until', until),
    h(
      'div',
      { class: 'form-actions' },
      h('button', { type: 'submit', class: 'primary' }, 'Apply'),
      h('button', { type: 'button', onclick: () => filterAudit(newAudit().filters) }, 'Clear'),
    ),
  );
  form.addEventListener('submit', (event) => {
    event.preventDefault();
    filterAudit({ entityType: type.value, entityKey: key.value.trim(), actor: actor.value.trim(), since: since.value, until: until.value });
  });
  els.panel.append(form, h('div', { class: 'toolbar' }, h('span', { class: 'spacer' }), pager(audit.page > 0, audit.next !== '', pageAudit)));

  if (!audit.events) {
    els.panel.append(h('p', { class: 'empty' }, audit.failed ? 'The audit log could not be loaded.' : 'Loading…'));
    return;
  }
  if (audit.events.length === 0) {
    els.panel.append(h('p', { class: 'empty' }, 'No audit events match.'));
    return;
  }
  const body = h('tbody');
  for (const ev of audit.events) {
    const open = audit.open === ev.id;
    body.append(
      h(
        'tr',
        { class: open ? 'selected' : null },
        h('td', { class: 'num' }, at(ev.time)),
        h('td', {}, ev.actor || '—'),
        h('td', {}, h('span', { class: `badge ${ACTION_TONES[ev.action] ?? 'info'}` }, ev.action)),
        h('td', {}, h('div', { class: 'key' }, ev.entityKey), h('div', { class: 'sub' }, ENTITY_LABELS[ev.entityType] ?? ev.entityType)),
        h('td', { class: 'num' }, `r${ev.revision}`),
        h('td', {}, ev.message),
        h(
          'td',
          { class: 'actions' },
          h(
            'button',
            {
              type: 'button',
              'aria-expanded': String(open),
              onclick: () => {
                audit.open = open ? '' : ev.id;
                renderPanel();
              },
            },
            open ? 'Hide' : 'Details',
          ),
        ),
      ),
    );
    if (open) {
      const kind = ev.before === null ? 'added' : ev.after === null ? 'removed' : 'modified';
      body.append(h('tr', { class: 'detail-row' }, h('td', { colspan: 7 }, changeCard(kind, ev.entityType, ev.entityKey, ev.before, ev.after))));
    }
  }
  els.panel.append(
    h(
      'div',
      { class: 'table-wrap' },
      h(
        'table',
        { class: 'grid' },
        h(
          'thead',
          {},
          h(
            'tr',
            {},
            h('th', {}, 'When'),
            h('th', {}, 'Actor'),
            h('th', {}, 'Action'),
            h('th', {}, 'Entry'),
            h('th', {}, 'Revision'),
            h('th', {}, 'Message'),
            h('th', { class: 'actions' }),
          ),
        ),
        body,
      ),
    ),
  );
}

// --------------------------------------------------------------- namespaces

async function createNamespace(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const fieldset = form.querySelector('fieldset');
  const name = form.elements.namedItem('name').value.trim();
  const description = form.elements.namedItem('description').value;
  fieldset.disabled = true;
  try {
    const res = await rpc('/api/v1/AdminService/CreateNamespace', { name, description });
    form.reset();
    form.closest('details').open = false;
    notify(`Created namespace ${res.namespace.name}.`);
    await loadNamespaces();
    location.hash = link(res.namespace.name);
  } catch (err) {
    showError(err);
  } finally {
    fieldset.disabled = false;
  }
}

// --------------------------------------------------------------------- start

function start() {
  const ids = {
    token: 'token',
    actor: 'actor',
    nsList: 'ns-list',
    nsEmpty: 'ns-empty',
    nsReload: 'ns-reload',
    nsCreate: 'ns-create',
    banner: 'banner',
    bannerText: 'banner-text',
    bannerClose: 'banner-close',
    notice: 'notice',
    welcome: 'welcome',
    view: 'ns-view',
    title: 'ns-title',
    description: 'ns-description',
    meta: 'ns-meta',
    refresh: 'ns-refresh',
    stale: 'ns-stale',
    staleText: 'ns-stale-text',
    staleReload: 'ns-stale-reload',
    tabs: 'tabs',
    panel: 'panel',
  };
  for (const [name, id] of Object.entries(ids)) els[name] = document.getElementById(id);

  els.token.value = load(TOKEN_KEY);
  els.actor.value = load(ACTOR_KEY);
  els.token.addEventListener('change', async () => {
    save(TOKEN_KEY, els.token.value.trim());
    els.banner.hidden = true;
    try {
      if (state.name) await refresh();
      else await loadNamespaces();
    } catch (err) {
      showError(err);
    }
  });
  els.actor.addEventListener('change', () => save(ACTOR_KEY, els.actor.value.trim()));
  els.bannerClose.addEventListener('click', () => {
    els.banner.hidden = true;
  });
  els.nsReload.addEventListener('click', (e) => act(e.currentTarget, loadNamespaces));
  els.refresh.addEventListener('click', (e) => act(e.currentTarget, refresh));
  els.staleReload.addEventListener('click', (e) => act(e.currentTarget, refresh));
  els.nsCreate.addEventListener('submit', createNamespace);
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && state.editing) closeEditor();
  });
  document.addEventListener('visibilitychange', poll);
  window.addEventListener('hashchange', route);
  setInterval(poll, POLL_MS);

  route();
}

start();
