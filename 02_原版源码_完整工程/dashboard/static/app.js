(() => {
  const $ = id => document.getElementById(id);
  const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const tag = (text, kind) => `<span class="tag ${kind}">${esc(text)}</span>`;
  const n = v => Number(v ?? 0);

  let dashboardData = null;
  let rows = [];
  let progressData = { companies: [], summary: {} };
  let logEventSource = null;
  let currentDetailCompany = null;
  const states = { page: 1, size: 20 };

  const chinese = {
    phase: {
      WEB_ICP:'备案载体查询', WEB_CRAWL:'官网抓取', IMAGE_DOWNLOAD:'图片下载', OCR:'图片文字识别',
      KEYWORD:'关键词筛查', ACCOUNT_DISCOVERY:'社媒账号发现', ACCOUNT_RANK:'账号筛选排序',
      ACCOUNT_REVIEW:'账号人工复核', ACCOUNT_VERIFY:'账号身份核验', POST_CRAWL:'社媒内容抓取',
      RISK_ANALYSIS:'风险分析研判', RISK_EVIDENCE:'风险证据整理'
    },
    companyStatus: { CLOSED:'已完成', OPEN:'进行中' },
    taskStatus: { OK:'成功', FAILED:'失败', RETRY_WAIT:'等待重试', RUNNING:'运行中', PENDING:'排队中', TERMINAL:'已结束' },
    error: { social_login_wait_timeout:'社媒登录等待超时', process_error:'处理过程异常', deterministic_dns_error:'域名无法解析', target_identity_mismatch:'账号身份不匹配', login_required:'需要重新登录' },
    file: { 'final.json':'任务结论文件', 'result.json':'任务原始结果', '截图':'证据截图', '过程文本':'过程文本', '其他':'其他过程文件' }
  };
  const label = (group, value) => chinese[group]?.[value] || value || '其他';

  async function fetchJson(url, opts) {
    try {
      const res = await fetch(url, opts);
      return await res.json();
    } catch (e) {
      return { ok: false, error: '网络请求失败：' + e.message };
    }
  }

  function showAlert(level, text) {
    const el = $(level === 'critical' ? 'alert-crit' : 'alert-warn');
    const txt = $(level === 'critical' ? 'alert-crit-text' : 'alert-warn-text');
    if (!text) {
      el.classList.remove('show');
      return;
    }
    txt.textContent = text;
    el.classList.add('show');
  }

  function renderMetrics() {
    if (!dashboardData) return;
    const status = dashboardData.status || {};
    const companyStatus = status.companies_by_status || {};
    const taskMap = status.tasks_by_phase_status || {};
    const audit = dashboardData.audit || {};
    const exec = (status.manifest || {}).execution_summary || {};
    const rowsLocal = dashboardData.companies?.rows || [];
    const closed = n(companyStatus.CLOSED), open = n(companyStatus.OPEN);
    const taskTotal = Object.values(taskMap).reduce((a,b)=>a+n(b),0);
    const failed = n((audit.by_status || {}).FAILED);
    const retry = n((audit.by_status || {}).RETRY_WAIT);
    const running = n((audit.by_status || {}).RUNNING);
    const pending = n((audit.by_status || {}).PENDING);
    const fallback = rowsLocal.filter(r => r['分析模式'] === 'conservative_local_fallback').length;

    const metrics = [
      ['企业完成', `${closed} / ${closed + open}`, open ? `${open} 家进行中` : '全部闭环'],
      ['任务终态', `${taskTotal - retry - running - pending} / ${taskTotal}`, `${retry + running + pending} 个未终态`],
      ['成功任务', taskTotal ? `${Math.round((taskTotal - failed - retry - running - pending) / taskTotal * 100)}%` : '—', `${taskTotal - failed - retry - running - pending} 个成功`],
      ['异常任务', failed + retry, `${failed} 失败 · ${retry} 重试`, '请关注错误归因'],
      ['历史复用', n(exec.HISTORY_REUSE), `本次实际执行 ${n(exec.PROCESS)}`]
    ];
    $('metrics').innerHTML = metrics.map(m => `<div class="metric"><div class="label">${m[0]}</div><div class="value">${m[1]}</div><div class="hint">${m[2]}</div></div>`).join('');
  }

  function mergeCompanyRows() {
    // 优先使用 extract_batch_table 的完整数据；若企业正在运行且暂无 export，
    // 则用 /api/runs/latest/progress 的实时数据补充。
    const progressMap = new Map(progressData.companies.map(c => [c['企业'], c]));
    const completeMap = new Map(rows.map(r => [r['企业'], r]));
    const allNames = [...new Set([...completeMap.keys(), ...progressMap.keys()])].sort();
    return allNames.map(name => {
      const full = completeMap.get(name) || {};
      const live = progressMap.get(name) || {};
      // 实时状态优先
      const status = live['状态'] || full['状态'] || '未知';
      return {
        ...full,
        '企业': name,
        '状态': status,
        '_live': live,
        '_hasFullData': !!completeMap.has(name),
      };
    });
  }

  function filtered() {
    const q = $('search').value.trim().toLowerCase(), status = $('status-filter').value, risk = $('risk-filter').value;
    return mergeCompanyRows().filter(r => {
      const hay = [r['企业'], r['风险词'], r['活跃域名']].join(' ').toLowerCase();
      if (q && !hay.includes(q)) return false;
      if (status && r['状态'] !== status) return false;
      if (risk === 'risk' && !(r['有风险'] === true || (r['风险词'] && String(r['风险词']).trim()))) return false;
      return true;
    });
  }

  function renderTable() {
    const merged = mergeCompanyRows();
    const all = filtered(), pages = Math.max(1, Math.ceil(all.length/states.size));
    states.page = Math.min(states.page, pages);
    const pageRows = all.slice((states.page-1)*states.size, states.page*states.size);
    $('result-count').textContent = `显示 ${all.length} / ${merged.length} 家企业`;
    $('company-body').innerHTML = pageRows.length ? pageRows.map(r => {
      const live = r._live || {};
      const riskCell = r['有风险'] === true ? tag('有风险','bad') : r['有风险'] === false ? tag('未发现','ok') : tag('待判定','warn');
      let stageProgress;
      if (r['状态'] === 'CLOSED') {
        stageProgress = tag('已完成', 'ok');
      } else if (live['当前阶段'] && live['当前阶段'] !== '-' && live['当前阶段'] !== '待判定') {
        stageProgress = tag(label('phase', live['当前阶段']), 'warn');
      } else {
        stageProgress = tag('待判定', 'warn');
      }
      return `<tr class="company-row" data-name="${esc(r['企业'])}"><td class="company-name">${esc(r['企业'])}</td><td>${tag(label('companyStatus', r['状态'] || '未知'), r['状态']==='CLOSED'?'ok':'warn')}</td><td>${stageProgress}</td><td>${esc(r['风险词'] || '—')}</td><td>${riskCell}</td></tr>`;
    }).join('') : '<tr><td colspan="5" class="empty">没有符合条件的企业</td></tr>';
    $('page-label').textContent = `${states.page} / ${pages} 页`;
    $('prev').disabled = states.page === 1; $('next').disabled = states.page === pages;
  }

  function switchTab(tab) {
    document.querySelectorAll('#detail-tabs .tab').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
    loadCompanySection(tab);
  }

  async function loadCompanySection(tab) {
    if (!currentDetailCompany) return;
    const runId = dashboardData?.run_id || 'latest';
    const content = $('detail-content');
    if (tab === 'overview') {
      const row = rows.find(r => r['企业'] === currentDetailCompany) || {};
      const live = (progressData.companies || []).find(c => c['企业'] === currentDetailCompany) || {};
      const files = dashboardData?.process_files?.by_company?.[currentDetailCompany] || [];
      const shots = files.filter(f => f.kind === '截图');
      const carriers = n(row['ICP网站数']) + n(row['ICP小程序数']) + n(row['ICP应用数']) + n(row['ICP快应用数']);
      const stats = live['任务统计'] || {};
      const list = shots.length ? shots.map(f => `<li><span>${esc(label('file', f.kind))}</span><a href="/api/file?path=${encodeURIComponent(f.path)}" target="_blank">查看截图</a></li>`).join('') : '<li class="muted">暂无截图</li>';
      const progressInfo = stats.total
        ? `<div class="detail-stat"><span class="muted">任务进度</span><br><strong>${stats.ok}成功 / ${stats.failed}失败 / ${stats.running}运行中</strong></div>`
        : '';
      content.innerHTML = `<div class="detail-grid"><div class="detail-stat"><span class="muted">备案载体</span><br><strong>${row._hasFullData!==false?carriers:'—'}</strong></div><div class="detail-stat"><span class="muted">官网 / 社媒</span><br><strong>${row._hasFullData!==false?n(row['官网域名数']):'—'} / ${row._hasFullData!==false?n(row['社媒账号数']):'—'}</strong></div><div class="detail-stat"><span class="muted">风险词</span><br><strong>${esc(row['风险词'] || '—')}</strong></div>${progressInfo}</div><h3>截图（本企业已索引 ${shots.length} 张）</h3><ul class="files">${list}</ul>`;
      return;
    }
    const sectionMap = { icp: 'icp', website: 'website', social: 'social', risk: 'risk' };
    const section = sectionMap[tab];
    const sectionPhaseMap = { icp: 'WEB_ICP', website: 'WEB_CRAWL', social: 'ACCOUNT_VERIFY', risk: 'RISK_ANALYSIS' };
    content.innerHTML = '<p class="empty">正在加载...</p>';
    const res = await fetchJson(`/api/company?run=${encodeURIComponent(runId)}&company=${encodeURIComponent(currentDetailCompany)}&section=${section}`);
    if (!res.ok) {
      // 企业尚未导出时，按标签回退到对应阶段产物
      const phase = sectionPhaseMap[tab];
      if (phase) {
        const phaseRes = await fetchJson(`/api/phase?run=${encodeURIComponent(runId)}&company=${encodeURIComponent(currentDetailCompany)}&phase=${phase}&kind=summary`);
        if (phaseRes.ok) {
          content.innerHTML = renderPhaseSection(phase, phaseRes.data);
          return;
        }
      }
      content.innerHTML = `<p class="empty">${esc(res.error)}</p>${res.hint?`<p class="empty" style="padding-top:0">提示：${esc(res.hint)}</p>`:''}`;
      return;
    }
    content.innerHTML = renderCompanySection(tab, res.data);
  }

  function renderCompanySection(tab, data) {
    if (tab === 'icp') {
      const counts = data.icp_counts || {};
      const items = [];
      ['web','mapp','app','kapp'].forEach(k => {
        const list = (data.icp_carriers || {})[k] || [];
        list.forEach(r => items.push(`[${k}] ${esc(r.domain || r.name || JSON.stringify(r))}`));
      });
      return `<div class="detail-grid"><div class="detail-stat"><span class="muted">网站</span><br><strong>${counts.web || 0}</strong></div><div class="detail-stat"><span class="muted">小程序</span><br><strong>${counts.mapp || 0}</strong></div><div class="detail-stat"><span class="muted">APP/快应用</span><br><strong>${(counts.app || 0) + (counts.kapp || 0)}</strong></div></div><h3>备案载体明细</h3>${items.length?`<pre class="phase-detail">${items.join('\n')}</pre>`:'<p class="empty">暂无 ICP 数据</p>'}`;
    }
    if (tab === 'website') {
      const ws = data.website_search || {};
      const wc = data.website_crawl || {};
      const candidates = (ws.candidates || []).map(c => esc(c.domain || JSON.stringify(c))).join('\n');
      const urls = (wc.seen_urls || []).map(u => esc(u)).join('\n');
      return `<div class="detail-grid"><div class="detail-stat"><span class="muted">搜索候选</span><br><strong>${(ws.candidates || []).length}</strong></div><div class="detail-stat"><span class="muted">抓取页面</span><br><strong>${(wc.seen_urls || []).length}</strong></div><div class="detail-stat"><span class="muted">研判方式</span><br><strong>${data.analysis_mode==='conservative_local_fallback'?'智能研判降级':'智能研判正常'}</strong></div></div><h3>搜索候选官网</h3>${candidates?`<pre class="phase-detail">${candidates}</pre>`:'<p class="empty">暂无候选官网</p>'}<h3>已抓取页面</h3>${urls?`<pre class="phase-detail">${urls}</pre>`:'<p class="empty">暂无抓取页面</p>'}`;
    }
    if (tab === 'social') {
      const s = data.social_account_verifications || {};
      const accounts = (s.accounts || []).map(a => `[${a.platform}] ${esc(a.name || '—')} · ${esc(a.decision || '—')} · ${esc(a.official_status || '—')}`).join('\n');
      return `<div class="detail-grid"><div class="detail-stat"><span class="muted">社媒账号</span><br><strong>${s.count || 0}</strong></div><div class="detail-stat"><span class="muted">研判方式</span><br><strong>${data.analysis_mode==='conservative_local_fallback'?'智能研判降级':'智能研判正常'}</strong></div><div class="detail-stat"><span class="muted"></span><br><strong></strong></div></div><h3>账号列表</h3>${accounts?`<pre class="phase-detail">${accounts}</pre>`:'<p class="empty">暂无社媒账号数据</p>'}`;
    }
    if (tab === 'risk') {
      const r = data.risk || {};
      const keywords = (r.risk_keyword_findings || []).map(f => (f.keywords || []).join('、')).filter(Boolean).join('；');
      const scenarios = (r.risk_scenarios || []).map(s => `${esc(s.scenario || '—')}: ${esc(JSON.stringify(s))}`).join('\n');
      return `<div class="detail-grid"><div class="detail-stat"><span class="muted">有风险</span><br><strong>${r.overall_risk_found===true?'是':r.overall_risk_found===false?'否':'待判定'}</strong></div><div class="detail-stat"><span class="muted">回购线索</span><br><strong>${r.gold_buyback?.present===true?'有':r.gold_buyback?.present===false?'无':'—'}</strong></div><div class="detail-stat"><span class="muted">研判方式</span><br><strong>${r.analysis_mode==='conservative_local_fallback'?'智能研判降级':'智能研判正常'}</strong></div></div><h3>风险摘要</h3><pre class="phase-detail">${esc(r.executive_summary || '无')}</pre><h3>风险词</h3><p>${keywords||'<span class="empty">无</span>'}</p><h3>风险场景</h3>${scenarios?`<pre class="phase-detail">${scenarios}</pre>`:'<p class="empty">暂无风险场景</p>'}`;
    }
    return '<p class="empty">未知标签</p>';
  }

  function renderPhaseSection(phase, data) {
    const tasks = data?.tasks || [];
    if (!tasks.length) return `<p class="empty">阶段 ${esc(label('phase', phase))} 暂无任务产物</p>`;
    const rows = tasks.map(t => {
      const summary = t.summary || {};
      const title = summary.title || summary.url || summary.domain || summary.name || t.task_dir || '未命名任务';
      const status = esc(label('taskStatus', t.status));
      const text = JSON.stringify(summary, null, 2).slice(0, 1000);
      return `<div class="phase-card"><div class="phase-card-title"><span>${esc(title)}</span><small>${status}</small></div><pre class="phase-detail">${esc(text)}</pre></div>`;
    }).join('');
    return `<div class="phase-list-compact">${rows}</div>`;
  }

  function showDetailByName(name) {
    currentDetailCompany = name;
    const live = (progressData.companies || []).find(c => c['企业'] === name) || {};
    $('detail-name').textContent = name;
    $('detail-note').textContent = `状态：${label('companyStatus', live['状态'] || '未知')} · 点击下方标签切换视图`;
    // 重置标签为概览
    document.querySelectorAll('#detail-tabs .tab').forEach(b => b.classList.toggle('active', b.dataset.tab === 'overview'));
    $('detail-dialog').showModal();
    loadCompanySection('overview');
  }

  async function loadDashboard() {
    const res = await fetchJson('/api/runs/latest');
    if (!res.ok) {
      showAlert('warning', '加载批次数据失败：' + res.error);
      return;
    }
    dashboardData = res.data;
    rows = dashboardData.companies?.rows || [];
    renderMetrics();
    await loadProgress();
  }

  async function loadProgress() {
    const res = await fetchJson('/api/runs/latest/progress');
    if (!res.ok) {
      // 不弹警告，静默失败，避免主数据加载失败时满屏报错
      return;
    }
    progressData = res;
    // 状态筛选器固定为「全部 / 已完成」，不再动态填充
    renderTable();
  }

  async function updateStatus() {
    const res = await fetchJson('/api/agent/status');
    const dot = $('status-dot');
    const text = $('status-text');
    const btnStart = $('btn-start');
    const btnStop = $('btn-stop');
    const btnReplay = $('btn-replay');
    const info = $('run-info');

    if (!res.ok) {
      dot.className = 'dot err';
      text.textContent = '状态获取失败';
      showAlert('warning', '无法连接看板后端：' + res.error);
      return;
    }

    showAlert(res.error_level === 'critical' ? 'critical' : 'warning', res.error_message);

    if (res.running) {
      dot.className = 'dot run';
      text.textContent = '运行中';
      btnStart.disabled = true;
      btnStop.disabled = false;
      btnReplay.disabled = true;
      info.textContent = `PID: ${res.pid || '-'} · 启动: ${res.start_time || '-'}`;
      $('log-status').textContent = '实时接收中...';
    } else {
      dot.className = 'dot stop';
      text.textContent = '已停止';
      btnStart.disabled = false;
      btnStop.disabled = true;
      btnReplay.disabled = false;
      info.textContent = res.pid ? `上次 PID: ${res.pid}` : '';
      $('log-status').textContent = '等待启动...';
    }
  }

  function appendLog(text) {
    const panel = $('log-panel');
    const lines = text.split('\n').filter(Boolean);
    lines.forEach(line => {
      const div = document.createElement('div');
      div.className = 'log-line';
      div.textContent = line;
      panel.appendChild(div);
    });
    panel.scrollTop = panel.scrollHeight;
  }

  function connectLog() {
    if (logEventSource) { logEventSource.close(); }
    logEventSource = new EventSource('/api/agent/log');
    logEventSource.onmessage = e => {
      try {
        const data = JSON.parse(e.data);
        if (data.text) appendLog(data.text);
      } catch (err) {}
    };
    logEventSource.onerror = () => {};
  }

  async function startAgent() {
    $('btn-start').disabled = true;
    const res = await fetchJson('/api/agent/start', { method: 'POST' });
    if (!res.ok) {
      showAlert(res.error?.includes('API key') || res.error?.includes('companies') ? 'critical' : 'warning', res.error);
      $('btn-start').disabled = false;
      return;
    }
    appendLog(`[看板] main_agent 已启动，PID=${res.pid}，日志=${res.log_file}`);
    connectLog();
    updateStatus();
  }

  let replayCompanies = [];
  const replaySelected = new Set();

  function renderReplayList(query = '') {
    const list = $('replay-list');
    const q = query.trim().toLowerCase();
    const filtered = replayCompanies.filter(name => name.toLowerCase().includes(q));
    $('replay-count').textContent = `已选 ${replaySelected.size} / ${filtered.length} 家`;
    list.innerHTML = filtered.length
      ? filtered.map(name => `<label class="replay-item"><input type="checkbox" value="${esc(name)}" ${replaySelected.has(name) ? 'checked' : ''}><span>${esc(name)}</span></label>`).join('')
      : '<p class="empty">没有匹配企业</p>';
    const allVisible = list.querySelectorAll('input');
    $('replay-select-all').checked = allVisible.length > 0 && [...allVisible].every(cb => cb.checked);
  }

  function openReplayDialog() {
    replayCompanies = mergeCompanyRows().map(r => r['企业']).filter(Boolean);
    replaySelected.clear();
    $('replay-search').value = '';
    $('replay-select-all').checked = false;
    renderReplayList();
    $('replay-dialog').showModal();
  }

  async function confirmReplay() {
    if (!replaySelected.size) {
      showAlert('warning', '请至少选择一家企业');
      return;
    }
    $('btn-replay-confirm').disabled = true;
    $('replay-dialog').close();
    $('btn-replay').disabled = true;
    const res = await fetchJson('/api/agent/replay', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ companies: [...replaySelected] }),
    });
    $('btn-replay-confirm').disabled = false;
    if (!res.ok) {
      showAlert(res.error?.includes('API key') || res.error?.includes('replay_companies') ? 'critical' : 'warning', res.error);
      $('btn-replay').disabled = false;
      return;
    }
    appendLog(`[看板] 复跑排查已启动，PID=${res.pid}，日志=${res.log_file}`);
    connectLog();
    updateStatus();
  }

  async function startReplay() {
    openReplayDialog();
  }

  async function stopAgent() {
    $('btn-stop').disabled = true;
    const res = await fetchJson('/api/agent/stop', { method: 'POST' });
    if (!res.ok) {
      showAlert('warning', res.error);
      $('btn-stop').disabled = false;
      return;
    }
    appendLog('[看板] main_agent 已停止');
    if (logEventSource) { logEventSource.close(); logEventSource = null; }
    updateStatus();
  }

  function bindEvents() {
    $('btn-start').addEventListener('click', startAgent);
    $('btn-stop').addEventListener('click', stopAgent);
    $('btn-replay').addEventListener('click', startReplay);
    $('company-body').addEventListener('click', e => { const tr = e.target.closest('tr[data-name]'); if (tr) showDetailByName(tr.dataset.name); });
    ['search','status-filter','risk-filter','page-size'].forEach(id => $(id).addEventListener(id==='search'?'input':'change', e => { if(id==='page-size') states.size=n(e.target.value); states.page=1; renderTable(); }));
    $('prev').onclick = () => { states.page--; renderTable(); };
    $('next').onclick = () => { states.page++; renderTable(); };
    $('close-dialog').onclick = () => $('detail-dialog').close();
    $('detail-dialog').addEventListener('click', e => { if(e.target === $('detail-dialog')) $('detail-dialog').close(); });
    $('detail-tabs').addEventListener('click', e => { const b = e.target.closest('.tab'); if (b) switchTab(b.dataset.tab); });
    $('close-replay-dialog').onclick = () => $('replay-dialog').close();
    $('replay-dialog').addEventListener('click', e => { if(e.target === $('replay-dialog')) $('replay-dialog').close(); });
    $('replay-search').addEventListener('input', e => renderReplayList(e.target.value));
    $('replay-select-all').addEventListener('change', e => {
      const visible = [...$('replay-list').querySelectorAll('input')];
      visible.forEach(cb => {
        if (e.target.checked) replaySelected.add(cb.value);
        else replaySelected.delete(cb.value);
      });
      renderReplayList($('replay-search').value);
    });
    $('replay-list').addEventListener('change', e => {
      if (e.target.tagName === 'INPUT') {
        if (e.target.checked) replaySelected.add(e.target.value);
        else replaySelected.delete(e.target.value);
        renderReplayList($('replay-search').value);
      }
    });
    $('btn-replay-confirm').onclick = confirmReplay;
  }

  async function init() {
    bindEvents();
    await loadDashboard();
    await updateStatus();
    connectLog();
    setInterval(updateStatus, 3000);
    setInterval(loadProgress, 3000);
    setInterval(loadDashboard, 30000);
  }

  init();
})();
