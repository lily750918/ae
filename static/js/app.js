/**
 * AE Monitor 前端：与 ae_server.py API 对接
 */
(function () {
  'use strict';

  const $ = (id) => document.getElementById(id);

  async function api(path, options = {}) {
    const opts = {
      credentials: 'same-origin',
      headers: { ...(options.headers || {}) },
      ...options,
    };
    if (opts.body && typeof opts.body === 'object' && !(opts.body instanceof FormData)) {
      opts.body = JSON.stringify(opts.body);
      opts.headers['Content-Type'] = 'application/json';
    }
    const res = await fetch(path, opts);
    let data = null;
    const text = await res.text();
    try {
      data = text ? JSON.parse(text) : null;
    } catch {
      data = { _raw: text };
    }
    return { res, data };
  }

  function setPnlEl(el, val) {
    if (!el) return;
    const n = Number(val);
    el.textContent = isNaN(n) ? '—' : '$' + n.toFixed(2);
    el.classList.remove('pnl-positive', 'pnl-negative');
    if (!isNaN(n)) {
      if (n > 0) el.classList.add('pnl-positive');
      else if (n < 0) el.classList.add('pnl-negative');
    }
  }

  async function loadStatus() {
    const { res, data } = await api('/api/status');
    if (!data || data.error) {
      console.warn('status', data);
      return;
    }

    const tb = $('total-balance');
    const ab = $('available-balance');
    if (data.total_balance != null) tb.textContent = Number(data.total_balance).toFixed(2);
    else if (data.balance != null) tb.textContent = Number(data.balance).toFixed(2);
    else tb.textContent = '—';

    if (data.available_balance != null) ab.textContent = Number(data.available_balance).toFixed(2);
    else ab.textContent = tb.textContent;

    $('positions-count').textContent = data.positions_count ?? '0';
    $('today-entries').textContent = data.today_entries ?? '0';

    setPnlEl($('unrealized-pnl'), data.unrealized_pnl);
    setPnlEl($('daily-pnl'), data.daily_pnl);

    const dot = $('running-status');
    const txt = $('running-text');
    const run = !!data.running;
    if (dot) {
      dot.classList.toggle('running', run);
    }
    if (txt) txt.textContent = run ? '运行中' : '已停止';

    const exDot = $('exchange-status-dot');
    const exTxt = $('exchange-status-text');
    if (exDot && exTxt) {
      const ex = data.exchange_status || {};
      exDot.classList.remove('exchange-ok', 'exchange-down', 'exchange-unknown');
      if (data.api_configured === false) {
        exDot.classList.add('exchange-unknown');
        exTxt.textContent = '交易所 — 未配置 API';
        exTxt.title = '';
      } else if (ex.ok) {
        exDot.classList.add('exchange-ok');
        const lat = ex.latency_ms != null ? ` · ${ex.latency_ms}ms` : '';
        const st = ex.server_time_iso ? ` · 币安时 ${ex.server_time_iso}` : '';
        exTxt.textContent = `${ex.exchange || 'Binance'} ${ex.market || ''} 正常${lat}`;
        exTxt.title = (ex.message || '') + st;
      } else {
        exDot.classList.add('exchange-down');
        exTxt.textContent = `交易所异常: ${ex.message || '未知'}`;
        exTxt.title = ex.message || '';
      }
    }

    const btnStart = $('btn-start');
    const btnStop = $('btn-stop');
    if (btnStart) btnDisabled(btnStart, run || data.api_configured === false);
    if (btnStop) btnDisabled(btnStop, !run);
    if (data.api_configured === false && btnStart) {
      btnStart.title = '未配置 API，无法启动';
    }
  }

  function btnDisabled(btn, disabled) {
    btn.disabled = !!disabled;
  }

  async function loadPositions() {
    const container = $('positions-container');
    const { res, data } = await api('/api/positions');

    if (res.status === 503 || (data && data.api_configured === false)) {
      container.innerHTML =
        '<div class="empty-state">' +
        (data && data.error ? data.error : '当前无法加载持仓（仅界面模式或未配置 API）') +
        '</div>';
      return;
    }

    if (!data || !data.success) {
      container.innerHTML =
        '<div class="empty-state">' + (data && data.error ? data.error : '加载失败') + '</div>';
      return;
    }

    const list = data.positions || [];
    if (!list.length) {
      container.innerHTML = '<div class="empty-state">暂无持仓</div>';
      return;
    }

    const rows = list
      .map((p) => {
        const payload = encodeURIComponent(
          JSON.stringify({
            symbol: p.symbol,
            position_id: p.position_id,
            entry_price: p.entry_price,
            mark_price: p.mark_price,
            tp_price: p.tp_price,
            sl_price: p.sl_price,
          })
        );
        return `
      <tr>
        <td><strong>${escapeHtml(p.symbol)}</strong></td>
        <td><span class="badge ${p.direction === 'long' ? 'badge-long' : 'badge-short'}">${escapeHtml(
          p.direction || ''
        )}</span></td>
        <td>${fmtNum(p.entry_price)}</td>
        <td>${fmtNum(p.mark_price)}</td>
        <td class="${pnlClass(p.pnl)}">${p.pnl != null && !isNaN(Number(p.pnl)) ? Number(p.pnl).toFixed(2) : '—'}</td>
        <td class="${pnlClass(p.pnl_pct)}">${p.pnl_pct != null ? Number(p.pnl_pct).toFixed(2) + '%' : '—'}</td>
        <td class="row-actions">
          <button type="button" class="btn btn-secondary" data-action="tpsl" data-payload="${payload}">止盈止损</button>
        </td>
      </tr>`;
      })
      .join('');

    container.innerHTML = `
      <table>
        <thead>
          <tr>
            <th>合约</th><th>方向</th><th>开仓价</th><th>标记价</th><th>盈亏</th><th>盈亏%</th><th>操作</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>`;

    container.querySelectorAll('[data-action="tpsl"]').forEach((btn) => {
      btn.addEventListener('click', () => {
        try {
          openTpslModal(JSON.parse(decodeURIComponent(btn.getAttribute('data-payload'))));
        } catch (e) {
          console.error(e);
        }
      });
    });
  }

  function pnlClass(v) {
    const n = Number(v);
    if (isNaN(n)) return '';
    if (n > 0) return 'pnl-positive';
    if (n < 0) return 'pnl-negative';
    return '';
  }

  function fmtNum(v) {
    if (v == null || v === '') return '—';
    const n = Number(v);
    return isNaN(n) ? '—' : n.toFixed(6);
  }

  function escapeHtml(s) {
    if (!s) return '';
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  async function loadLogs() {
    const el = $('logs-content');
    const { res, data } = await api('/api/logs?lines=80');
    if (res.status === 503) {
      el.textContent = (data && data.error) || '仅界面模式下无日志接口';
      return;
    }
    if (data && data.success && data.logs) {
      el.textContent = data.logs.join('\n');
    } else {
      el.textContent = data && data.error ? data.error : JSON.stringify(data || {}, null, 2);
    }
  }

  let modalCtx = null;

  function openTpslModal(ctx) {
    modalCtx = ctx;
    $('modal-symbol').textContent = ctx.symbol || '';
    $('modal-entry-price').textContent = ctx.entry_price != null ? String(ctx.entry_price) : '';
    $('modal-current-price').textContent = ctx.mark_price != null ? String(ctx.mark_price) : '';
    const tp = $('input-tp-price');
    const sl = $('input-sl-price');
    const tpS = ctx.tp_price != null && ctx.tp_price !== 'N/A' ? String(parseFloat(ctx.tp_price) || '') : '';
    const slS = ctx.sl_price != null && ctx.sl_price !== 'N/A' ? String(parseFloat(ctx.sl_price) || '') : '';
    tp.value = tpS;
    sl.value = slS;
    updatePercentage();
    $('modal-tpsl').classList.add('open');
  }

  window.closeModal = function () {
    $('modal-tpsl').classList.remove('open');
    modalCtx = null;
  };

  window.updatePercentage = function () {
    const entry = parseFloat($('modal-entry-price').textContent);
    const tp = parseFloat($('input-tp-price').value);
    const sl = parseFloat($('input-sl-price').value);
    const hintTp = $('tp-percentage');
    const hintSl = $('sl-percentage');
    if (!isNaN(entry) && entry > 0) {
      if (!isNaN(tp))
        hintTp.textContent =
          '相对开仓价: ' + ((((tp - entry) / entry) * 100).toFixed(2) + '%');
      else hintTp.textContent = '';
      if (!isNaN(sl))
        hintSl.textContent =
          '相对开仓价: ' + ((((sl - entry) / entry) * 100).toFixed(2) + '%');
      else hintSl.textContent = '';
    }
  };

  window.submitTPSL = async function () {
    if (!modalCtx) return;
    const tp = $('input-tp-price').value;
    const sl = $('input-sl-price').value;
    const body = {
      symbol: modalCtx.symbol,
      position_id: modalCtx.position_id,
      tp_price: tp === '' ? null : parseFloat(tp),
      sl_price: sl === '' ? null : parseFloat(sl),
    };
    const { res, data } = await api('/api/update_tp_sl', { method: 'POST', body });
    alert((data && (data.message || data.error)) || (res.ok ? '已提交' : '失败'));
    window.closeModal();
    await refreshData();
  };

  window.refreshData = async function () {
    await loadStatus();
    await loadPositions();
    await loadLogs();
  };

  window.startTrading = async function () {
    const { data } = await api('/api/start_trading', { method: 'POST' });
    alert((data && data.message) || 'done');
    await refreshData();
  };

  window.stopTrading = async function () {
    const { data } = await api('/api/stop_trading', { method: 'POST' });
    alert((data && data.message) || 'done');
    await refreshData();
  };

  window.manualScan = async function () {
    const { data } = await api('/api/manual_scan', { method: 'POST' });
    alert((data && (data.message || data.error)) || 'done');
    await refreshData();
  };

  window.closeConfirm = function () {
    $('modal-confirm').classList.remove('open');
  };

  document.addEventListener('DOMContentLoaded', refreshData);
})();
