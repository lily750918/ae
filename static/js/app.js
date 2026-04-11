/**
 * AE Monitor 前端：与 ae_server.py API 对接
 */
(function () {
  'use strict';

  const $ = (id) => document.getElementById(id);

  async function api(path, options = {}) {
    const { headers: extraHeaders, ...restOptions } = options;
    const opts = {
      credentials: 'same-origin',
      ...restOptions,
      headers: { ...(extraHeaders || {}) },
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

  let _positionsStreamRunning = false;

  /** 订阅后端 SSE：开仓/平仓/清幽灵仓/改止盈止损时推送，再拉持仓（监控逻辑仍在服务端 monitor_loop） */
  function startPositionsStream() {
    if (_positionsStreamRunning) return;
    _positionsStreamRunning = true;
    (async function streamLoop() {
      while (_positionsStreamRunning) {
        try {
          const res = await fetch('/api/positions/stream', { credentials: 'same-origin' });
          if (!res.ok || !res.body) {
            await new Promise((r) => setTimeout(r, 5000));
            continue;
          }
          const reader = res.body.getReader();
          const dec = new TextDecoder();
          let buf = '';
          while (_positionsStreamRunning) {
            const { done, value } = await reader.read();
            if (done) break;
            buf += dec.decode(value, { stream: true });
            const blocks = buf.split('\n\n');
            buf = blocks.pop() || '';
            for (const block of blocks) {
              for (const line of block.split('\n')) {
                const t = line.trim();
                if (!t.startsWith('data:')) continue;
                try {
                  const j = JSON.parse(t.slice(5).trim());
                  if (j && j.type === 'positions_changed') {
                    const anyModalOpen = document.querySelector('.modal.open, .modal.is-open');
                    if (!anyModalOpen) {
                      await loadPositions();
                      await loadStatus();
                      await loadLogs();
                      updateLastUpdated();
                    }
                  }
                } catch (_) {}
              }
            }
          }
        } catch (_) {
          /* 断线重连 */
        }
        await new Promise((r) => setTimeout(r, 3000));
      }
    })();
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
      _currentPositions = [];
      setPositionChart(null);
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
        const tpDisplay = p.tp_price && p.tp_price !== 'N/A' ? fmtNum(p.tp_price) : '—';
        const slDisplay = p.sl_price && p.sl_price !== 'N/A' ? fmtNum(p.sl_price) : '—';
        const entryTime = p.entry_time ? p.entry_time.replace('T', ' ').slice(0, 19) : '—';
        return `
      <tr class="position-row" data-action="chart" data-symbol="${escapeHtml(p.symbol)}">
        <td><strong>${escapeHtml(p.symbol)}</strong></td>
        <td><span class="badge ${p.direction === 'long' ? 'badge-long' : 'badge-short'}">${escapeHtml(
          p.direction || ''
        )}</span></td>
        <td>${fmtNum(p.entry_price)}</td>
        <td>${fmtNum(p.mark_price)}</td>
        <td class="${pnlClass(p.pnl)}">${p.pnl != null && !isNaN(Number(p.pnl)) ? Number(p.pnl).toFixed(2) : '—'}</td>
        <td class="${pnlClass(p.pnl_pct)}">${p.pnl_pct != null ? Number(p.pnl_pct).toFixed(2) + '%' : '—'}</td>
        <td class="pnl-positive">${tpDisplay}</td>
        <td class="pnl-negative">${slDisplay}</td>
        <td>${entryTime}</td>
        <td class="row-actions">
          <button type="button" class="btn btn-secondary" data-action="tpsl" data-payload="${payload}">止盈止损</button>
          <button type="button" class="btn btn-danger" data-action="close" data-symbol="${escapeHtml(p.symbol)}">市价平仓</button>
        </td>
      </tr>`;
      })
      .join('');

    container.innerHTML = `
      <table>
        <thead>
          <tr>
            <th>合约</th><th>方向</th><th>开仓价</th><th>标记价</th><th>盈亏</th><th>盈亏%</th><th>止盈价</th><th>止损价</th><th>开仓时间</th><th>操作</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>`;

    // 若当前图表的 symbol 已不在持仓中，切换到第一个；否则保持不变
    _currentPositions = list;
    const symbols = list.map((p) => p.symbol);
    if (!symbols.includes(_currentChartSymbol)) {
      setPositionChart(list[0] || null);
    }
    highlightActiveRow();
  }

  let _currentChartSymbol = null;
  let _currentChartInterval = '1h';
  let _currentChartPos = null;
  let _currentPositions = [];
  let _lwChart = null;
  let _priceStream = null;       // 当前价格 SSE 连接
  let _chartCandleSeries = null; // 当前 K 线 series 引用（供价格流更新末根 K 线）

  // ── 全局价格流：页面加载后订阅一次，推送所有活跃持仓价格 ──────────────────────
  function startPriceStream() {
    if (_priceStream) return;
    (async function priceStreamLoop() {
      while (true) {
        try {
          const res = await fetch('/api/price/stream', { credentials: 'same-origin' });
          if (!res.ok || !res.body) { await new Promise((r) => setTimeout(r, 5000)); continue; }
          _priceStream = res;
          const reader = res.body.getReader();
          const dec = new TextDecoder();
          let buf = '';
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            buf += dec.decode(value, { stream: true });
            const blocks = buf.split('\n\n');
            buf = blocks.pop() || '';
            for (const block of blocks) {
              for (const line of block.split('\n')) {
                const t = line.trim();
                if (!t.startsWith('data:')) continue;
                try {
                  const msg = JSON.parse(t.slice(5).trim());
                  if (msg.type !== 'prices') continue;
                  _applyPriceUpdate(msg.data);
                } catch (_) {}
              }
            }
          }
        } catch (_) {}
        _priceStream = null;
        await new Promise((r) => setTimeout(r, 3000));
      }
    })();
  }

  function _applyPriceUpdate(prices) {
    const ivSeconds = _currentChartInterval === '4h' ? 14400 : _currentChartInterval === '1d' ? 86400 : 3600;
    for (const item of prices) {
      const symbol = item.symbol;
      const price = parseFloat(item.price);
      if (isNaN(price)) continue;
      // 更新持仓表格行
      const pos = _currentPositions.find((p) => p.symbol === symbol);
      document.querySelectorAll(`.position-row[data-symbol="${symbol}"]`).forEach((row) => {
        const cells = row.querySelectorAll('td');
        if (cells[3]) cells[3].textContent = fmtNum(price);
        if (pos && cells[4] && cells[5]) {
          const ep = parseFloat(pos.entry_price);
          const qty = parseFloat(pos.quantity);
          const dir = pos.direction;
          if (!isNaN(ep) && ep > 0 && !isNaN(qty)) {
            const pnl = dir === 'long' ? (price - ep) * qty : (ep - price) * qty;
            const pnlPct = dir === 'long' ? (price - ep) / ep * 100 : (ep - price) / ep * 100;
            cells[4].textContent = pnl.toFixed(2);
            cells[4].className = pnlClass(pnl);
            cells[5].textContent = pnlPct.toFixed(2) + '%';
            cells[5].className = pnlClass(pnlPct);
          }
        }
      });
      // 更新当前展开的 K 线末根 close
      if (_chartCandleSeries && symbol === _currentChartSymbol) {
        const now = Math.floor(Date.now() / 1000);
        const barTime = now - (now % ivSeconds);
        try { _chartCandleSeries.update({ time: barTime, close: price }); } catch (_) {}
      }
    }
  }

  function setPositionChart(pos, interval) {
    const wrap = $('position-chart-widget');
    const label = $('position-chart-symbol');
    if (!wrap) return;

    if (_lwChart) { _lwChart.remove(); _lwChart = null; _chartCandleSeries = null; }

    if (!pos) {
      _currentChartSymbol = null;
      _currentChartPos = null;
      if (label) label.textContent = '—';
      wrap.innerHTML = '<div class="empty-state">暂无持仓</div>';
      return;
    }

    const symbol = pos.symbol;
    const iv = interval || _currentChartInterval;
    if (symbol === _currentChartSymbol && iv === _currentChartInterval) return;
    _currentChartSymbol = symbol;
    _currentChartInterval = iv;
    _currentChartPos = pos;
    if (label) label.textContent = symbol;

    if (typeof LightweightCharts === 'undefined') {
      wrap.innerHTML = '<div class="empty-state">图表库加载中，请稍候…</div>';
      setTimeout(() => { _currentChartSymbol = null; setPositionChart(pos, iv); }, 1000);
      return;
    }

    const entry = parseFloat(pos.entry_price);
    const tp    = parseFloat(pos.tp_price);
    const sl    = parseFloat(pos.sl_price);

    // Price tag helpers
    const tag = (cls, lbl, val) =>
      !isNaN(val) && val > 0
        ? `<span class="lwc-tag lwc-tag-${cls}">${lbl} ${fmtNum(val)}</span>`
        : '';

    wrap.innerHTML = `
      <div class="lwc-toolbar">
        <div class="lwc-interval-btns">
          <button class="lwc-btn${iv==='1h'?' lwc-btn-active':''}" data-iv="1h">1H</button>
          <button class="lwc-btn${iv==='4h'?' lwc-btn-active':''}" data-iv="4h">4H</button>
          <button class="lwc-btn${iv==='1d'?' lwc-btn-active':''}" data-iv="1d">1D</button>
        </div>
        <div class="lwc-price-tags">
          ${tag('entry','入场',entry)}${tag('tp','止盈',tp)}${tag('sl','止损',sl)}
        </div>
      </div>
      <div class="lwc-inner"></div>`;

    wrap.querySelectorAll('.lwc-btn[data-iv]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const newIv = btn.getAttribute('data-iv');
        if (newIv !== _currentChartInterval) {
          _currentChartSymbol = null;
          setPositionChart(_currentChartPos, newIv);
        }
      });
    });

    const inner = wrap.querySelector('.lwc-inner');

    _lwChart = LightweightCharts.createChart(inner, {
      autoSize: true,
      layout: { background: { type: 'solid', color: '#121824' }, textColor: '#8a9bb0' },
      grid: {
        vertLines: { color: 'rgba(94,123,168,0.08)' },
        horzLines: { color: 'rgba(94,123,168,0.08)' },
      },
      crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
      rightPriceScale: { borderColor: 'rgba(94,123,168,0.22)' },
      timeScale: { borderColor: 'rgba(94,123,168,0.22)', timeVisible: true, secondsVisible: false },
    });

    const candles = _lwChart.addCandlestickSeries({
      upColor: '#34d399', downColor: '#f87171',
      borderVisible: false,
      wickUpColor: '#34d399', wickDownColor: '#f87171',
      priceFormat: { type: 'custom', formatter: fmtChartPrice },
    });

    const volSeries = _lwChart.addHistogramSeries({
      priceFormat: { type: 'volume' },
      priceScaleId: 'vol',
    });
    _lwChart.priceScale('vol').applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });

    api('/api/klines?symbol=' + symbol + '&interval=' + iv + '&limit=100').then(({ data }) => {
      if (!data || !data.success || !_lwChart) return;
      candles.setData(data.data);
      volSeries.setData(data.data.map((d) => ({
        time: d.time,
        value: d.volume,
        color: d.close >= d.open ? 'rgba(52,211,153,0.35)' : 'rgba(248,113,113,0.35)',
      })));
      _lwChart.timeScale().fitContent();

      if (!isNaN(entry) && entry > 0) {
        candles.createPriceLine({ price: entry, color: '#fbbf24', lineWidth: 2,
          lineStyle: LightweightCharts.LineStyle.Dashed, axisLabelVisible: true, title: '入场' });
      }
      if (!isNaN(tp) && tp > 0) {
        candles.createPriceLine({ price: tp, color: '#34d399', lineWidth: 1,
          lineStyle: LightweightCharts.LineStyle.Solid, axisLabelVisible: true, title: '止盈' });
      }
      if (!isNaN(sl) && sl > 0) {
        candles.createPriceLine({ price: sl, color: '#f87171', lineWidth: 1,
          lineStyle: LightweightCharts.LineStyle.Solid, axisLabelVisible: true, title: '止损' });
      }
      // 记录当前 series 引用供全局价格流更新末根 K 线
      _chartCandleSeries = candles;
    });

    // autoSize: true 已自动处理容器尺寸变化，无需手动 ResizeObserver
  }

  // ── BTC 主图（服务端 Binance K 线，无 TradingView 外网依赖）──────────────
  let _btcChart = null;
  let _btcCandles = null;
  let _btcVol = null;
  let _btcInterval = '1d';
  let _btcRefreshTimer = null;

  function initBtcChart(interval) {
    const wrap = document.getElementById('btc-main-chart');
    if (!wrap) return;
    if (typeof LightweightCharts === 'undefined') {
      wrap.innerHTML = '<div class="empty-state">图表库加载中…</div>';
      setTimeout(() => initBtcChart(interval), 1000);
      return;
    }

    _btcInterval = interval || _btcInterval;

    if (_btcChart) { _btcChart.remove(); _btcChart = null; _btcCandles = null; _btcVol = null; }
    clearInterval(_btcRefreshTimer);

    wrap.innerHTML = `
      <div class="lwc-toolbar">
        <div class="lwc-interval-btns">
          <button class="lwc-btn${_btcInterval==='1h'?' lwc-btn-active':''}" data-btc-iv="1h">1H</button>
          <button class="lwc-btn${_btcInterval==='4h'?' lwc-btn-active':''}" data-btc-iv="4h">4H</button>
          <button class="lwc-btn${_btcInterval==='1d'?' lwc-btn-active':''}" data-btc-iv="1d">1D</button>
        </div>
        <span class="lwc-tag lwc-tag-entry" style="font-size:11px;opacity:.6">BTCUSDT · Binance</span>
      </div>
      <div class="lwc-inner"></div>`;

    wrap.querySelectorAll('.lwc-btn[data-btc-iv]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const iv = btn.getAttribute('data-btc-iv');
        if (iv !== _btcInterval) initBtcChart(iv);
      });
    });

    const inner = wrap.querySelector('.lwc-inner');
    _btcChart = LightweightCharts.createChart(inner, {
      autoSize: true,
      layout: { background: { type: 'solid', color: '#121824' }, textColor: '#8a9bb0' },
      grid: {
        vertLines: { color: 'rgba(94,123,168,0.08)' },
        horzLines: { color: 'rgba(94,123,168,0.08)' },
      },
      crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
      rightPriceScale: { borderColor: 'rgba(94,123,168,0.22)' },
      timeScale: { borderColor: 'rgba(94,123,168,0.22)', timeVisible: _btcInterval !== '1d', secondsVisible: false },
    });

    _btcCandles = _btcChart.addCandlestickSeries({
      upColor: '#34d399', downColor: '#f87171',
      borderVisible: false,
      wickUpColor: '#34d399', wickDownColor: '#f87171',
      priceFormat: { type: 'custom', formatter: fmtChartPrice },
    });

    _btcVol = _btcChart.addHistogramSeries({
      priceFormat: { type: 'volume' },
      priceScaleId: 'vol',
    });
    _btcChart.priceScale('vol').applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });

    refreshBtcChart();
    // 每 5 分钟自动刷新最新 K 线
    _btcRefreshTimer = setInterval(refreshBtcChart, 5 * 60 * 1000);
  }

  // 每个周期默认可见 bar 数（决定初始缩放）
  const _btcVisibleBars = { '1h': 48, '4h': 60, '1d': 90 };

  async function refreshBtcChart() {
    if (!_btcChart || !_btcCandles) return;
    const limit = _btcInterval === '1d' ? 180 : 100;
    const { data } = await api('/api/klines?symbol=BTCUSDT&interval=' + _btcInterval + '&limit=' + limit);
    if (!data || !data.success || !_btcCandles) return;
    _btcCandles.setData(data.data);
    _btcVol.setData(data.data.map((d) => ({
      time: d.time,
      value: d.volume,
      color: d.close >= d.open ? 'rgba(52,211,153,0.35)' : 'rgba(248,113,113,0.35)',
    })));
    // 只显示最近 N 根，其余可左滑查看
    const total = data.data.length;
    const visible = _btcVisibleBars[_btcInterval] || 60;
    _btcChart.timeScale().setVisibleLogicalRange({ from: total - visible, to: total + 2 });
  }

  function highlightActiveRow() {
    document.querySelectorAll('.position-row').forEach((row) => {
      row.classList.toggle(
        'position-row-active',
        row.getAttribute('data-symbol') === _currentChartSymbol
      );
    });
  }

  // 事件委托：绑定在不会被替换的 container 上，避免 DOM 重建后事件失效
  document.addEventListener('click', async (e) => {
    const btn = e.target.closest('[data-action]');
    if (!btn) return;
    const action = btn.getAttribute('data-action');

    if (action === 'chart') {
      const symbol = btn.getAttribute('data-symbol');
      const pos = _currentPositions.find((p) => p.symbol === symbol);
      if (pos) {
        setPositionChart(pos);
        highlightActiveRow();
      }
      return;
    }

    if (action === 'tpsl') {
      try {
        openTpslModal(JSON.parse(decodeURIComponent(btn.getAttribute('data-payload'))));
      } catch (err) {
        console.error(err);
      }
    }

    if (action === 'close') {
      const symbol = btn.getAttribute('data-symbol');
      openConfirm(
        '⚠️ 确认市价平仓',
        `确定要对 ${symbol} 执行市价平仓吗？此操作不可撤销。`,
        async () => {
          btn.disabled = true;
          btn.textContent = '平仓中…';
          const { res, data } = await api('/api/close_position', {
            method: 'POST',
            body: { symbol },
          });
          const msg = (data && (data.message || data.error)) || (res.ok ? '平仓已提交' : '平仓失败');
          alert(msg);
          await refreshData();
        }
      );
    }
  });

  function pnlClass(v) {
    const n = Number(v);
    if (isNaN(n)) return '';
    if (n > 0) return 'pnl-positive';
    if (n < 0) return 'pnl-negative';
    return '';
  }

  function fmtNum(v) {
    if (v == null || v === '' || v === 'N/A') return '—';
    const n = Number(v);
    if (isNaN(n) || n === 0) return '—';
    if (n >= 10)  return n.toFixed(2);
    if (n >= 0.1) return n.toFixed(4);
    return n.toPrecision(4);
  }

  function fmtChartPrice(price) {
    if (price >= 10)  return price.toFixed(2);
    if (price >= 0.1) return price.toFixed(4);
    return price.toPrecision(4);
  }

  function escapeHtml(s) {
    if (!s) return '';
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  async function loadStrategyParams() {
    const container = $('strategy-params-container');
    if (!container) return;
    const { res, data } = await api('/api/strategy_params');
    if (!data || !data.success) {
      container.innerHTML = '<div style="font-size:0.72rem;color:#888">' + (data && data.error ? data.error : '加载失败') + '</div>';
      return;
    }
    const groups = data.params || {};
    const cols = Object.values(groups)
      .map((group) => {
        const rowHtml = (item) =>
          `<tr>
          <td class="hparam-key">${escapeHtml(item.key)}</td>
          <td class="hparam-val">${escapeHtml(String(item.value))}</td>
        </tr>`;
        if (Array.isArray(group.columns) && group.columns.length) {
          const inner = group.columns
            .map(
              (colItems) =>
                `<table class="hparam-table">${(colItems || []).map(rowHtml).join('')}</table>`
            )
            .join('');
          return `<div class="hparam-col hparam-col-split">
        <div class="hparam-group-label">${escapeHtml(group.label)}</div>
        <div class="hparam-split-inner">${inner}</div>
      </div>`;
        }
        const rows = (group.items || []).map(rowHtml).join('');
        return `<div class="hparam-col">
        <div class="hparam-group-label">${escapeHtml(group.label)}</div>
        <table class="hparam-table">${rows}</table>
      </div>`;
      })
      .join('');
    container.innerHTML = `<div class="hparam-grid">${cols}</div>`;
  }

  function toDatetimeLocalValue(d) {
    const p = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}T${p(d.getHours())}:${p(d.getMinutes())}`;
  }

  function initExportRangeDefaults() {
    const now = new Date();
    const weekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
    [
      ['trade-export-start', weekAgo],
      ['trade-export-end', now],
      ['signal-export-start', weekAgo],
      ['signal-export-end', now],
    ].forEach(([id, d]) => {
      const el = $(id);
      if (el) el.value = toDatetimeLocalValue(d);
    });
  }

  async function exportHistoryCsv(kind) {
    const startId = kind === 'trade' ? 'trade-export-start' : 'signal-export-start';
    const endId = kind === 'trade' ? 'trade-export-end' : 'signal-export-end';
    const startEl = $(startId);
    const endEl = $(endId);
    const startVal = startEl && startEl.value ? startEl.value : '';
    const endVal = endEl && endEl.value ? endEl.value : '';
    if (startVal && endVal) {
      if (new Date(startVal).getTime() > new Date(endVal).getTime()) {
        alert('开始时间不能晚于结束时间');
        return;
      }
    }
    const qs = new URLSearchParams();
    if (startVal) qs.set('start', new Date(startVal).toISOString());
    if (endVal) qs.set('end', new Date(endVal).toISOString());
    const path = kind === 'trade' ? '/api/trade_history_export' : '/api/signal_history_export';
    const url = path + (qs.toString() ? '?' + qs.toString() : '');
    try {
      const res = await fetch(url, { credentials: 'same-origin' });
      const ct = (res.headers.get('content-type') || '').toLowerCase();
      if (!res.ok) {
        let msg = '导出失败 ' + res.status;
        try {
          const j = await res.json();
          if (j && j.error) msg = j.error;
        } catch (_) {}
        alert(msg);
        return;
      }
      if (!ct.includes('csv')) {
        try {
          const j = await res.json();
          alert((j && j.error) || '导出失败');
        } catch (_) {
          alert('导出失败');
        }
        return;
      }
      const blob = await res.blob();
      const dispo = res.headers.get('content-disposition') || '';
      const m = /filename="([^"]+)"/i.exec(dispo);
      const fname =
        m && m[1] ? m[1] : kind === 'trade' ? 'trade_history.csv' : 'signal_history.csv';
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = fname;
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(a.href), 2500);
    } catch (e) {
      alert('导出失败: ' + e.message);
    }
  }

  async function downloadDailyReport() {
    const url = '/api/daily_report_download';
    try {
      const res = await fetch(url, { credentials: 'same-origin' });
      if (!res.ok) {
        let msg = '下载失败 ' + res.status;
        try {
          const j = await res.json();
          if (j && j.error) msg = j.error;
        } catch (_) {}
        alert(msg);
        return;
      }
      const ct = (res.headers.get('content-type') || '').toLowerCase();
      if (ct.includes('application/json')) {
        try {
          const j = await res.json();
          alert((j && j.error) || '下载失败');
        } catch (_) {
          alert('下载失败');
        }
        return;
      }
      const blob = await res.blob();
      const dispo = res.headers.get('content-disposition') || '';
      const m = /filename="([^"]+)"/i.exec(dispo);
      const fname = m && m[1] ? m[1] : 'daily_report.txt';
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = fname;
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(a.href), 2500);
    } catch (e) {
      alert('下载失败: ' + e.message);
    }
  }

  window.downloadDailyReport = downloadDailyReport;

  const TRADE_PAGE_SIZE = 8;
  let _tradeAllRows = [];
  let _tradePage = 0;

  /** 不足一页时用占位行垫满，保证表格高度恒为 pageSize 行 */
  function padHistoryTableRows(rowHtmlFragments, pageSize, colCount) {
    const rows = rowHtmlFragments.slice();
    while (rows.length < pageSize) {
      let cells = '';
      for (let c = 0; c < colCount; c += 1) cells += '<td class="history-pad-cell">&nbsp;</td>';
      rows.push(`<tr class="history-pad-row" aria-hidden="true">${cells}</tr>`);
    }
    return rows.join('');
  }

  function renderTradeHistoryPage() {
    const container = $('trade-history-container');
    if (!container || !_tradeAllRows.length) return;
    const total = _tradeAllRows.length;
    const totalPages = Math.ceil(total / TRADE_PAGE_SIZE);
    _tradePage = Math.max(0, Math.min(_tradePage, totalPages - 1));
    const start = _tradePage * TRADE_PAGE_SIZE;
    const sliceRows = _tradeAllRows.slice(start, start + TRADE_PAGE_SIZE);
    const pageRows = padHistoryTableRows(sliceRows, TRADE_PAGE_SIZE, 10);

    const prevDis = _tradePage === 0 ? ' disabled' : '';
    const nextDis = _tradePage >= totalPages - 1 ? ' disabled' : '';

    container.innerHTML = `
      <div class="positions-container history-table-fixed">
        <table class="trade-history-table">
          <thead><tr>
            <th class="trade-col-symbol">合约</th>
            <th class="trade-col-dir">方向</th>
            <th class="trade-col-price">开仓价</th>
            <th class="trade-col-price">平仓价</th>
            <th class="trade-col-pnl">盈亏($)</th>
            <th class="trade-col-pnl-pct">盈亏%</th>
            <th class="trade-col-reason">原因</th>
            <th class="trade-col-dur">持仓时长</th>
            <th class="history-time-col">开仓时间</th>
            <th class="history-time-col">平仓时间</th>
          </tr></thead>
          <tbody>${pageRows}</tbody>
        </table>
      </div>
      <div class="trade-pagination">
        <button class="btn btn-secondary trade-pg-btn" data-dir="-1"${prevDis}>‹ 上一页</button>
        <span class="trade-pg-info">第 ${_tradePage + 1} / ${totalPages} 页（共 ${total} 条）</span>
        <button class="btn btn-secondary trade-pg-btn" data-dir="1"${nextDis}>下一页 ›</button>
      </div>`;
  }

  async function loadTradeHistory() {
    const container = $('trade-history-container');
    if (!container) return;
    const { res, data } = await api('/api/trade_history?limit=200');
    if (!data || !data.success) {
      container.innerHTML = '<div class="empty-state">' + (data && data.error ? data.error : '加载失败') + '</div>';
      return;
    }
    const trades = data.trades || [];
    if (!trades.length) {
      container.innerHTML = '<div class="empty-state">暂无交易记录</div>';
      _tradeAllRows = [];
      return;
    }
    _tradeAllRows = trades.map((t) => {
      const pnlVal = Number(t.pnl_value);
      const pnlPct = Number(t.pnl_pct);
      const cls = pnlVal > 0 ? 'pnl-positive' : pnlVal < 0 ? 'pnl-negative' : '';
      const entryTime = t.entry_time ? t.entry_time.replace('T', ' ').slice(0, 19) : '—';
      const closeTime = t.close_time ? t.close_time.replace('T', ' ').slice(0, 19) : '—';
      const elapsed = t.elapsed_hours != null ? Number(t.elapsed_hours).toFixed(1) + 'h' : '—';
      return `<tr>
        <td class="trade-cell-symbol"><strong>${escapeHtml(t.symbol)}</strong></td>
        <td class="trade-cell-dir"><span class="badge ${t.direction === 'long' ? 'badge-long' : 'badge-short'}">${escapeHtml(t.direction || '')}</span></td>
        <td class="trade-cell-price">${fmtNum(t.entry_price)}</td>
        <td class="trade-cell-price">${fmtNum(t.exit_price)}</td>
        <td class="trade-cell-pnl ${cls}">${isNaN(pnlVal) ? '—' : (pnlVal >= 0 ? '+' : '') + pnlVal.toFixed(2)}</td>
        <td class="trade-cell-pnl-pct ${cls}">${isNaN(pnlPct) ? '—' : (pnlPct >= 0 ? '+' : '') + pnlPct.toFixed(2) + '%'}</td>
        <td class="trade-cell-reason">${escapeHtml(t.reason_cn || t.reason || '—')}</td>
        <td class="trade-cell-dur">${elapsed}</td>
        <td class="history-time-cell">${entryTime}</td>
        <td class="history-time-cell">${closeTime}</td>
      </tr>`;
    });
    renderTradeHistoryPage();
  }

  // 翻页点击（事件委托）
  document.addEventListener('click', (e) => {
    const btn = e.target.closest('.trade-pg-btn');
    if (!btn || btn.disabled) return;
    _tradePage += Number(btn.getAttribute('data-dir'));
    renderTradeHistoryPage();
  });

  async function loadLogs() {
    const el = $('logs-content');
    const { res, data } = await api('/api/logs?lines=100');
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
      tp_price: tp === '' ? null : parseFloat(tp),
      sl_price: sl === '' ? null : parseFloat(sl),
    };
    const pid = modalCtx.position_id;
    if (pid && pid !== 'N/A') body.position_id = pid;
    try {
      const res = await fetch('/api/update_tp_sl', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await res.json();
      alert((data && (data.message || data.error)) || (res.ok ? '已提交' : '失败'));
    } catch (e) {
      alert('请求失败: ' + e.message);
    }
    window.closeModal();
    await refreshData();
  };

  function updateLastUpdated() {
    const el = $('last-updated');
    if (el) {
      const now = new Date();
      const hh = String(now.getHours()).padStart(2, '0');
      const mm = String(now.getMinutes()).padStart(2, '0');
      const ss = String(now.getSeconds()).padStart(2, '0');
      el.textContent = `更新于 ${hh}:${mm}:${ss}`;
    }
  }

  window.refreshData = async function () {
    await loadStatus();
    await loadPositions();
    await loadTradeHistory();
    await loadSignalHistory();
    await loadLogs();
    updateLastUpdated();
  };

  // —— 信号历史 ——
  const SIGNAL_PAGE_SIZE = 9;
  let _signalAllRows = [];
  let _signalPage = 0;

  /** 与后端一致：信号时间降序，同时间按卖量倍数降序，再按合约名稳定排序 */
  function parseSignalTimeMs(sig) {
    if (!sig) return 0;
    let s = String(sig).trim();
    if (s.endsWith(' UTC')) s = s.slice(0, -4).trim();
    if (!s.includes('T')) s = s.replace(' ', 'T');
    const iso = s.endsWith('Z') || /[+-]\d{2}:?\d{2}$/.test(s) ? s : `${s}Z`;
    const ms = Date.parse(iso);
    return Number.isFinite(ms) ? ms : 0;
  }

  function compareSignalHistory(a, b) {
    const ta = parseSignalTimeMs(a.signal_time);
    const tb = parseSignalTimeMs(b.signal_time);
    if (tb !== ta) return tb - ta;
    const ra = Number(a.surge_ratio) || 0;
    const rb = Number(b.surge_ratio) || 0;
    if (rb !== ra) return rb - ra;
    return String(a.symbol || '').localeCompare(String(b.symbol || ''));
  }

  function renderSignalPage() {
    const container = $('signal-history-container');
    if (!container || !_signalAllRows.length) return;
    const total = _signalAllRows.length;
    const totalPages = Math.ceil(total / SIGNAL_PAGE_SIZE);
    _signalPage = Math.max(0, Math.min(_signalPage, totalPages - 1));
    const start = _signalPage * SIGNAL_PAGE_SIZE;
    const sliceRows = _signalAllRows.slice(start, start + SIGNAL_PAGE_SIZE);
    const pageRows = padHistoryTableRows(sliceRows, SIGNAL_PAGE_SIZE, 6);
    const prevDis = _signalPage === 0 ? ' disabled' : '';
    const nextDis = _signalPage >= totalPages - 1 ? ' disabled' : '';
    container.innerHTML = `
      <div class="positions-container history-table-fixed">
        <table>
          <thead><tr>
            <th>合约</th><th>信号时间</th><th>卖量倍数</th><th>买量倍数</th><th>价格</th><th>建仓</th>
          </tr></thead>
          <tbody>${pageRows}</tbody>
        </table>
      </div>
      <div class="trade-pagination">
        <button class="btn btn-secondary signal-pg-btn" data-dir="-1"${prevDis}>‹ 上一页</button>
        <span class="trade-pg-info">第 ${_signalPage + 1} / ${totalPages} 页（共 ${total} 条）</span>
        <button class="btn btn-secondary signal-pg-btn" data-dir="1"${nextDis}>下一页 ›</button>
      </div>`;
  }

  async function loadSignalHistory() {
    const container = $('signal-history-container');
    if (!container) return;
    const { res, data } = await api('/api/signal_history?limit=500');
    if (!data || !data.success) {
      container.innerHTML = '<div class="empty-state">' + (data && data.error ? data.error : '加载失败') + '</div>';
      return;
    }
    const signals = (data.signals || []).slice().sort(compareSignalHistory);
    if (!signals.length) {
      container.innerHTML = '<div class="empty-state">暂无信号记录</div>';
      _signalAllRows = [];
      return;
    }
    _signalAllRows = signals.map((s) => {
      const sigTime = s.signal_time ? s.signal_time.replace(' UTC', '') : '—';
      const openedBadge = s.opened
        ? '<span class="badge badge-long">已建仓</span>'
        : '<span style="color:var(--text-muted);font-size:0.75rem">—</span>';
      return `<tr>
        <td><strong>${escapeHtml(s.symbol)}</strong></td>
        <td>${escapeHtml(sigTime)}</td>
        <td class="pnl-positive">${Number(s.surge_ratio).toFixed(2)}x</td>
        <td>${s.intraday_buy_ratio > 0 ? Number(s.intraday_buy_ratio).toFixed(2) + 'x' : '—'}</td>
        <td>${fmtNum(s.price)}</td>
        <td>${openedBadge}</td>
      </tr>`;
    });
    renderSignalPage();
  }

  // 信号翻页
  document.addEventListener('click', (e) => {
    const btn = e.target.closest('.signal-pg-btn');
    if (!btn || btn.disabled) return;
    _signalPage += Number(btn.getAttribute('data-dir'));
    renderSignalPage();
  });

  // —— Tab 切换 ——
  document.addEventListener('click', (e) => {
    const btn = e.target.closest('.tab-btn');
    if (!btn) return;
    const tab = btn.getAttribute('data-tab');
    document.querySelectorAll('.tab-btn').forEach((b) => b.classList.remove('tab-btn-active'));
    btn.classList.add('tab-btn-active');
    document.querySelectorAll('[id^="tab-panel-"]').forEach((p) => {
      p.style.display = 'none';
    });
    const panel = $('tab-panel-' + tab);
    if (panel) {
      panel.style.display = 'flex';
      panel.style.flexDirection = 'column';
    }
  });

  window.startTrading = async function () {
    const { data } = await api('/api/start_trading', { method: 'POST' });
    if (data && data.error) alert(data.error);
    await refreshData();
  };

  window.stopTrading = async function () {
    const { data } = await api('/api/stop_trading', { method: 'POST' });
    if (data && data.error) alert(data.error);
    await refreshData();
  };

  window.manualScan = async function () {
    const { data } = await api('/api/manual_scan', { method: 'POST' });
    if (data && data.error) {
      alert(data.error);
    } else {
      // 无论是否有信号或建仓成功，均静默处理，仅在控制台打印
      const msg = (data && data.message) || '手动扫描完成';
      console.log(`[Manual Scan] ${msg}`);
    }
    await refreshData();
  };

  let confirmCallback = null;

  function openConfirm(title, message, onConfirm) {
    $('confirm-title').textContent = title;
    $('confirm-message').textContent = message;
    confirmCallback = onConfirm;
    $('modal-confirm').classList.add('open');
    $('confirm-yes').onclick = async () => {
      const cb = confirmCallback;
      window.closeConfirm();
      if (cb) await cb();
    };
  }

  window.closeConfirm = function () {
    $('modal-confirm').classList.remove('open');
    confirmCallback = null;
  };

  document.addEventListener('DOMContentLoaded', () => {
    initExportRangeDefaults();
    const bt = $('btn-export-trade');
    const bs = $('btn-export-signal');
    if (bt) bt.addEventListener('click', () => exportHistoryCsv('trade'));
    if (bs) bs.addEventListener('click', () => exportHistoryCsv('signal'));
    const dr = $('btn-download-daily-report');
    if (dr) dr.addEventListener('click', () => downloadDailyReport());

    refreshData();
    loadStrategyParams();
    startPositionsStream();
    startPriceStream();
    initBtcChart();
    // 状态栏约每 10 秒刷新；持仓列表由 SSE 在结构变化时刷新，此处仅定时补拉盈亏/价格（约 45 秒）
    setInterval(async () => {
      const anyModalOpen = document.querySelector('.modal.open, .modal.is-open');
      if (anyModalOpen) return;
      await loadStatus();
      updateLastUpdated();
    }, 10000);
    setInterval(async () => {
      const anyModalOpen = document.querySelector('.modal.open, .modal.is-open');
      if (anyModalOpen) return;
      await loadPositions();
    }, 45000);
    // 交易记录 + 信号记录 + 日志 每 30 秒自动刷新
    setInterval(async () => {
      const anyModalOpen = document.querySelector('.modal.open, .modal.is-open');
      if (anyModalOpen) return;
      await loadTradeHistory();
      await loadSignalHistory();
    }, 30000);
    setInterval(async () => {
      const anyModalOpen = document.querySelector('.modal.open, .modal.is-open');
      if (anyModalOpen) return;
      await loadLogs();
    }, 12000);

    document.querySelectorAll('a[href]').forEach((a) => {
      try {
        const u = new URL(a.href, window.location.origin);
        if (u.origin !== window.location.origin) return;
        if (!/\/params\/?$/.test(u.pathname)) return;
        a.addEventListener('click', () => {
          try {
            sessionStorage.setItem('ae_opened_params_from_monitor', '1');
          } catch (_) {}
        });
      } catch (_) {}
    });
  });

  /** 从参数页 history.back() 回到监控时若命中 bfcache，补拉数据与顶栏策略参数 */
  window.addEventListener('pageshow', (e) => {
    if (!e.persisted) return;
    refreshData();
    loadStrategyParams();
  });
})();
