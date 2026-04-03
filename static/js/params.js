(function () {
  'use strict';

  const AE_FROM_MONITOR_KEY = 'ae_opened_params_from_monitor';

  /** 从监控进入参数页时由 app.js 写入；异常来源则清除，避免误用 history.back */
  function reconcileParamsEntryFlag() {
    const ref = document.referrer;
    if (!ref) {
      try {
        sessionStorage.removeItem(AE_FROM_MONITOR_KEY);
      } catch (_) {}
      return;
    }
    try {
      const u = new URL(ref);
      if (u.origin !== location.origin) {
        sessionStorage.removeItem(AE_FROM_MONITOR_KEY);
        return;
      }
      const path = (u.pathname || '/').replace(/\/+$/, '') || '/';
      const fromHome = path === '' || path === '/';
      const fromParams = /\/params$/.test(path);
      if (!fromHome && !fromParams) {
        sessionStorage.removeItem(AE_FROM_MONITOR_KEY);
      }
    } catch (_) {
      try {
        sessionStorage.removeItem(AE_FROM_MONITOR_KEY);
      } catch (__) {}
    }
  }

  function bindReturnToMonitorLinks() {
    document.querySelectorAll('a.ae-nav-monitor').forEach((a) => {
      a.addEventListener('click', (e) => {
        let canBack = false;
        try {
          canBack = sessionStorage.getItem(AE_FROM_MONITOR_KEY) === '1';
        } catch (_) {}
        if (!canBack || window.history.length <= 1) return;
        e.preventDefault();
        try {
          sessionStorage.removeItem(AE_FROM_MONITOR_KEY);
        } catch (_) {}
        window.history.back();
      });
    });
  }

  reconcileParamsEntryFlag();
  bindReturnToMonitorLinks();

  const $ = (id) => document.getElementById(id);
  const statusEl = $('params-status');

  function setStatus(msg, isError) {
    if (!statusEl) return;
    statusEl.textContent = msg || '';
    statusEl.classList.toggle('params-status-error', !!isError);
    statusEl.classList.toggle('params-status-ok', !!msg && !isError);
  }

  async function api(method) {
    const opts = { method, credentials: 'same-origin', headers: {} };
    if (method === 'POST') {
      opts.headers['Content-Type'] = 'application/json';
      opts.body = JSON.stringify(collectPayload());
    }
    const res = await fetch('/api/config_editable', opts);
    let data = null;
    const text = await res.text();
    try {
      data = text ? JSON.parse(text) : null;
    } catch {
      data = { _raw: text };
    }
    return { res, data };
  }

  function boolFromIni(s) {
    const v = String(s || '').trim().toLowerCase();
    return v === 'true' || v === '1' || v === 'yes' || v === 'on';
  }

  function fillForm(cfg) {
    if (!cfg) return;
    const boolIds = ['STRATEGY_enable_hourly_entry_limit', 'RISK_enable_max_gain_24h_exit'];
    for (const sec of ['STRATEGY', 'SIGNAL', 'RISK']) {
      const block = cfg[sec];
      if (!block || typeof block !== 'object') continue;
      for (const [k, v] of Object.entries(block)) {
        const id = `${sec}_${k}`;
        const el = $(id);
        if (!el) continue;
        if (boolIds.includes(id)) {
          el.checked = boolFromIni(v);
        } else {
          el.value = v;
        }
      }
    }
  }

  function collectPayload() {
    return {
      STRATEGY: {
        leverage: $('STRATEGY_leverage').value,
        position_size_ratio: $('STRATEGY_position_size_ratio').value,
        max_positions: $('STRATEGY_max_positions').value,
        max_daily_entries: $('STRATEGY_max_daily_entries').value,
        enable_hourly_entry_limit: $('STRATEGY_enable_hourly_entry_limit').checked,
        max_opens_per_scan: $('STRATEGY_max_opens_per_scan').value,
      },
      SIGNAL: {
        sell_surge_threshold: $('SIGNAL_sell_surge_threshold').value,
        sell_surge_max: $('SIGNAL_sell_surge_max').value,
      },
      RISK: {
        strong_coin_tp_pct: $('RISK_strong_coin_tp_pct').value,
        medium_coin_tp_pct: $('RISK_medium_coin_tp_pct').value,
        weak_coin_tp_pct: $('RISK_weak_coin_tp_pct').value,
        stop_loss_pct: $('RISK_stop_loss_pct').value,
        enable_max_gain_24h_exit: $('RISK_enable_max_gain_24h_exit').checked,
        max_gain_24h_threshold: $('RISK_max_gain_24h_threshold').value,
        max_hold_hours: $('RISK_max_hold_hours').value,
      },
    };
  }

  async function load() {
    setStatus('加载中…', false);
    const { res, data } = await api('GET');
    if (!res.ok) {
      setStatus(data && data.error ? String(data.error) : `加载失败 (${res.status})`, true);
      return;
    }
    if (!data || !data.success || !data.config) {
      setStatus('返回数据无效', true);
      return;
    }
    fillForm(data.config);
    setStatus('');
  }

  document.getElementById('form-config-editable').addEventListener('submit', async (e) => {
    e.preventDefault();
    setStatus('保存中…', false);
    const { res, data } = await api('POST');
    if (!res.ok) {
      const err = (data && (data.error || data.message)) || `保存失败 (${res.status})`;
      setStatus(String(err), true);
      return;
    }
    if (data && data.success) {
      setStatus(data.message || '已保存', false);
    } else {
      setStatus('保存失败', true);
    }
  });

  /* —— Binance API —— */
  const binanceStatusEl = $('binance-cred-status');
  const maskHintEl = $('binance-mask-hint');
  const envNoteEl = $('binance-env-note');
  const keyInput = $('binance-api-key-input');
  const secInput = $('binance-api-secret-input');
  let lastKeyTailHint = '';

  function setBinanceStatus(msg, kind) {
    if (!binanceStatusEl) return;
    binanceStatusEl.textContent = msg || '';
    binanceStatusEl.classList.remove('params-binance-ok', 'params-binance-err');
    if (kind === 'ok') binanceStatusEl.classList.add('params-binance-ok');
    if (kind === 'err') binanceStatusEl.classList.add('params-binance-err');
  }

  async function fetchJson(url, opts) {
    const res = await fetch(url, { credentials: 'same-origin', ...opts });
    let data = null;
    const text = await res.text();
    try {
      data = text ? JSON.parse(text) : null;
    } catch {
      data = { _raw: text };
    }
    return { res, data };
  }

  async function loadBinanceMeta() {
    if (!maskHintEl) return;
    const { res, data } = await fetchJson('/api/binance_credentials', { method: 'GET' });
    if (!res.ok || !data || !data.success) {
      maskHintEl.textContent = '无法加载密钥状态';
      return;
    }
    if (data.env_overrides_file && data.note) {
      envNoteEl.hidden = false;
      envNoteEl.textContent = data.note;
    } else {
      envNoteEl.hidden = true;
      envNoteEl.textContent = '';
    }
    lastKeyTailHint = data.copyable_key_hint || '';
    if (data.effective_configured) {
      const tail = data.key_tail ? `（尾号 …${data.key_tail}）` : '';
      maskHintEl.textContent = `已配置 · Key ********${tail} · Secret ******** · 来源: ${data.effective_source === 'env' ? '环境变量' : 'config.ini'}`;
      if (keyInput) {
        keyInput.value = '';
        keyInput.placeholder = '********';
      }
      if (secInput) {
        secInput.value = '';
        secInput.placeholder = '********';
      }
    } else {
      maskHintEl.textContent = '未配置有效密钥（仅界面模式）。请在下方填写并保存，或设置环境变量。';
      if (keyInput) keyInput.placeholder = 'API Key';
      if (secInput) secInput.placeholder = 'API Secret';
    }
  }

  async function copyInput(el, label) {
    const v = (el && el.value) || '';
    if (!v.trim()) {
      setBinanceStatus(`${label} 输入框为空；已配置时完整密钥不会显示，请填写新密钥后再复制。`, 'err');
      return;
    }
    try {
      await navigator.clipboard.writeText(v);
      setBinanceStatus(`已复制 ${label}（当前输入框内容）`, 'ok');
    } catch {
      setBinanceStatus('复制失败，请手动选择文本复制', 'err');
    }
  }

  $('btn-copy-api-key')?.addEventListener('click', () => copyInput(keyInput, 'API Key'));
  $('btn-copy-api-secret')?.addEventListener('click', () => copyInput(secInput, 'Secret'));

  $('btn-copy-key-tail')?.addEventListener('click', async () => {
    const t = lastKeyTailHint.trim();
    if (!t) {
      setBinanceStatus('暂无尾号说明（可能未配置 Key）', 'err');
      return;
    }
    try {
      await navigator.clipboard.writeText(t);
      setBinanceStatus('已复制尾号说明', 'ok');
    } catch {
      setBinanceStatus('复制失败', 'err');
    }
  });

  $('btn-binance-test')?.addEventListener('click', async () => {
    setBinanceStatus('测试中…', null);
    const body = {};
    const k = keyInput?.value?.trim();
    const s = secInput?.value?.trim();
    if (k && s) {
      body.api_key = k;
      body.api_secret = s;
    }
    const { res, data } = await fetchJson('/api/binance_test', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      setBinanceStatus((data && (data.error || data.message)) || `请求失败 ${res.status}`, 'err');
      return;
    }
    if (data.ok) {
      const ms = data.latency_ms != null ? ` ${data.latency_ms}ms` : '';
      setBinanceStatus((data.message || '连通正常') + ms, 'ok');
    } else {
      setBinanceStatus(data.message || '连通失败', 'err');
    }
  });

  $('btn-binance-save')?.addEventListener('click', async () => {
    const k = keyInput?.value?.trim();
    const s = secInput?.value?.trim();
    if (!k || !s) {
      setBinanceStatus('请填写完整的 API Key 与 Secret 再保存（界面不预填明文）', 'err');
      return;
    }
    setBinanceStatus('保存中…', null);
    const { res, data } = await fetchJson('/api/binance_credentials', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ api_key: k, api_secret: s }),
    });
    if (!res.ok) {
      setBinanceStatus((data && data.error) || `保存失败 ${res.status}`, 'err');
      return;
    }
    if (data.warning) {
      setBinanceStatus(data.message || data.warning, 'err');
    } else {
      setBinanceStatus(data.message || '已保存', 'ok');
    }
    keyInput.value = '';
    secInput.value = '';
    await loadBinanceMeta();
  });

  const formTablist = document.querySelector('.params-form-tablist');
  function activateFormPanel(panelId) {
    if (!panelId) return;
    formTablist?.querySelectorAll('.params-tab[data-panel]').forEach((t) => {
      const on = t.getAttribute('data-panel') === panelId;
      t.classList.toggle('is-active', on);
      t.setAttribute('aria-selected', on ? 'true' : 'false');
    });
    document.querySelectorAll('.params-form-panels .params-panel').forEach((p) => {
      p.classList.toggle('is-active', p.id === panelId);
    });
  }
  formTablist?.querySelectorAll('.params-tab[data-panel]').forEach((tab) => {
    tab.addEventListener('click', () => activateFormPanel(tab.getAttribute('data-panel')));
  });

  const leftCol = document.getElementById('params-col-left');
  const rightCol = document.getElementById('params-col-right');
  const btnNarrowLeft = document.getElementById('params-narrow-left');
  const btnNarrowRight = document.getElementById('params-narrow-right');

  function setNarrowPanel(which) {
    if (!leftCol || !rightCol) return;
    if (which === 'left') {
      leftCol.classList.add('is-narrow-active');
      rightCol.classList.remove('is-narrow-active');
      btnNarrowLeft?.classList.add('is-active');
      btnNarrowRight?.classList.remove('is-active');
    } else {
      leftCol.classList.remove('is-narrow-active');
      rightCol.classList.add('is-narrow-active');
      btnNarrowLeft?.classList.remove('is-active');
      btnNarrowRight?.classList.add('is-active');
    }
  }

  btnNarrowLeft?.addEventListener('click', () => setNarrowPanel('left'));
  btnNarrowRight?.addEventListener('click', () => setNarrowPanel('right'));

  const mqNarrow = window.matchMedia('(max-width: 1100px)');
  mqNarrow.addEventListener('change', () => {
    if (mqNarrow.matches) setNarrowPanel('left');
  });

  load();
  loadBinanceMeta();
})();
