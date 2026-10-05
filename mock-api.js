/* GoldGuard AI · 静态站点 API 垫片（仅供 GitHub Pages 等静态托管使用）
 *
 * 作用：把前端对 /api/* 的 fetch 与 EventSource 请求，路由到预生成的
 * window.__GG_BUNDLE__（见 data/api-bundle.js），并在浏览器内模拟“开始排查 / 人工复核”。
 * 本地服务版由 run.py 提供真实接口，二者返回结构一致。
 */
(function () {
  var B = window.__GG_BUNDLE__ || {};
  var KEY = 'gg_reviews_v1';
  var load = function () { try { return JSON.parse(localStorage.getItem(KEY) || '{}'); } catch (e) { return {}; } };
  var save = function (r) { try { localStorage.setItem(KEY, JSON.stringify(r)); } catch (e) {} };

  var PHASES = ['WEB_ICP', 'WEB_CRAWL', 'IMAGE_DOWNLOAD', 'OCR', 'KEYWORD', 'ACCOUNT_DISCOVERY',
    'ACCOUNT_RANK', 'ACCOUNT_REVIEW', 'ACCOUNT_VERIFY', 'POST_CRAWL', 'RISK_ANALYSIS', 'RISK_EVIDENCE'];
  var PHASE_ZH = {
    WEB_ICP: '备案载体查询', WEB_CRAWL: '官网抓取', IMAGE_DOWNLOAD: '图片下载', OCR: '图片文字识别',
    KEYWORD: '关键词筛查', ACCOUNT_DISCOVERY: '社媒账号发现', ACCOUNT_RANK: '账号筛选排序',
    ACCOUNT_REVIEW: '账号人工复核', ACCOUNT_VERIFY: '账号身份核验', POST_CRAWL: '社媒内容抓取',
    RISK_ANALYSIS: '风险分析研判', RISK_EVIDENCE: '风险证据整理'
  };

  var state = { running: false, phaseIdx: 0, startTime: null, pid: null };
  var listeners = [];
  var simTimer = null;

  function emit(text) { listeners.forEach(function (fn) { fn({ data: JSON.stringify({ text: text }) }); }); }
  function J(obj) { return Promise.resolve({ ok: true, status: 200, json: function () { return Promise.resolve(obj); } }); }
  function clone(o) { return JSON.parse(JSON.stringify(o)); }

  function mergeReview(name, data) {
    if (!data) return data;
    var r = load()[name];
    if (!r) return data;
    if (data.human_review) data.human_review = r;
    if (data.risk && data.risk.human_review) data.risk.human_review = r;
    return data;
  }

  function progressList() {
    var base = B.progress;
    if (!state.running) return base;
    var copy = clone(base);
    copy.companies.forEach(function (c) {
      if (c['状态'] === 'OPEN') {
        c['当前阶段'] = PHASES[Math.min(state.phaseIdx, PHASES.length - 1)];
        var total = 36, ok = Math.max(1, Math.round(total * (state.phaseIdx + 1) / PHASES.length));
        c['任务统计'] = { total: total, ok: ok, failed: 0, running: total - ok };
      }
    });
    return copy;
  }

  function startSim() {
    if (state.running) return J({ ok: false, error: '已有排查任务在运行中' });
    state.running = true; state.phaseIdx = 0;
    state.pid = 40000 + Math.floor(Math.random() * 1000);
    state.startTime = new Date().toLocaleString('zh-CN');
    emit('[看板] 启动离线演示排查（静态模式）');
    clearInterval(simTimer);
    simTimer = setInterval(function () {
      if (state.phaseIdx >= PHASES.length) {
        clearInterval(simTimer); state.running = false;
        emit('全部企业排查完成；示例企业G 标记“待核实”，示例企业H 触发降级'); return;
      }
      var p = PHASES[state.phaseIdx];
      emit('[' + (state.phaseIdx + 1) + '/12] ' + p + '（' + PHASE_ZH[p] + '）完成');
      state.phaseIdx++;
    }, 1100);
    return J({ ok: true, pid: state.pid, log_file: 'logs/demo_agent.log' });
  }

  function FakeES(url) {
    this.url = url; this.onmessage = null; this.onerror = null;
    var self = this;
    listeners.push(function (e) { if (self.onmessage) self.onmessage(e); });
  }
  FakeES.prototype.close = function () {};
  window.EventSource = FakeES;

  var orig = window.fetch ? window.fetch.bind(window) : null;
  window.fetch = function (input, opts) {
    var url = String(input && input.url ? input.url : input);
    var parts = url.split('?');
    var path = parts[0];
    var q = new URLSearchParams(parts[1] || '');
    var method = ((opts && opts.method) || 'GET').toUpperCase();

    if (path.indexOf('/api/') !== 0) return orig ? orig(input, opts) : Promise.reject(new Error('offline'));

    if (method === 'POST') {
      if (path === '/api/agent/start' || path === '/api/agent/replay') return startSim();
      if (path === '/api/agent/stop') { state.running = false; clearInterval(simTimer); emit('收到停止指令'); return J({ ok: true }); }
      if (path === '/api/review') {
        var body = {}; try { body = JSON.parse(opts.body); } catch (e) {}
        var name = body.company;
        var all = load();
        all[name] = { status: '已复核', reviewer: '复核人（演示）', decision: body.decision || '采纳',
          note: body.note || '', reviewed_at: new Date().toLocaleString('zh-CN') };
        save(all); emit('[人工复核] ' + name + ' → ' + all[name].decision);
        return J({ ok: true, data: all[name] });
      }
      return J({ ok: false, error: '静态演示未实现该接口' });
    }

    if (path === '/api/runs/latest') return J(B.dashboard);
    if (path === '/api/runs/latest/progress') return J(progressList());
    if (path === '/api/agent/status') return J({ ok: true, running: state.running, pid: state.pid,
      start_time: state.startTime, error_message: null, error_level: null });
    if (path === '/api/meta') return J(B.meta);
    if (path === '/api/company') {
      var cn = q.get('company'), sec = q.get('section') || 'risk';
      var byC = (B.company || {})[cn];
      if (!byC || !byC[sec]) return J({ ok: false, error: '未找到该企业或该栏目' });
      return J({ ok: true, data: mergeReview(cn, clone(byC[sec])) });
    }
    if (path === '/api/phase') {
      var pn = q.get('company'), pp = q.get('phase') || 'RISK_ANALYSIS';
      var byP = (B.phase || {})[pn];
      if (!byP || !byP[pp]) return J({ ok: false, error: '未找到阶段产物' });
      return J(byP[pp]);
    }
    return J({ ok: false, error: '静态演示未实现的接口' });
  };

  // 截图链接：拦截 a[href^="/api/file"]，用生成的 SVG 占位打开
  document.addEventListener('click', function (e) {
    var a = e.target && e.target.closest ? e.target.closest('a[href^="/api/file"]') : null;
    if (!a) return;
    e.preventDefault();
    var u = new URL(a.getAttribute('href'), location.href);
    var label = (u.searchParams.get('path') || 'evidence').split('/').pop();
    var svg = '<svg xmlns="http://www.w3.org/2000/svg" width="640" height="360">' +
      '<rect width="100%" height="100%" fill="#f4f7f4"/>' +
      '<text x="50%" y="46%" text-anchor="middle" font-size="20" fill="#147a5a">证据截图（构造占位）</text>' +
      '<text x="50%" y="56%" text-anchor="middle" font-size="14" fill="#69756f">' + label + '</text></svg>';
    window.open(URL.createObjectURL(new Blob([svg], { type: 'image/svg+xml' })), '_blank');
  });
})();
