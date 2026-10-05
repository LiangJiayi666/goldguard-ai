/* 静态站点 API 垫片冒烟测试（Node，无浏览器）
 * 校验 mock-api.js 的路由与返回结构，确保 GitHub Pages 静态版可用。
 * 运行：node tools/test_mock_api.cjs
 */
const fs = require('fs');
const path = require('path');

const REPO = path.resolve(__dirname, '..', '..');
const SITE = path.join(REPO, 'site');

// ---- 最小浏览器环境 ----
global.window = global;
const store = {};
global.localStorage = {
  getItem: (k) => (k in store ? store[k] : null),
  setItem: (k, v) => { store[k] = String(v); },
  removeItem: (k) => { delete store[k]; },
};
global.document = { addEventListener: () => {} };
global.location = { href: 'https://example.github.io/goldguard-ai/' };
global.URLSearchParams = URLSearchParams;

// ---- 加载 bundle 与垫片 ----
eval(fs.readFileSync(path.join(SITE, 'data', 'api-bundle.js'), 'utf8'));
eval(fs.readFileSync(path.join(SITE, 'mock-api.js'), 'utf8'));

const results = [];
const check = (cond, name, detail = '') => results.push([!!cond, cond ? name : `${name} —— ${detail}`]);
const get = async (u) => (await fetch(u)).json();
const post = async (u, body) => (await fetch(u, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body || {}) })).json();

(async () => {
  const dash = await get('/api/runs/latest');
  check(dash.ok && dash.data.companies.rows.length === 8, '驾驶舱返回 8 家企业');
  const name = dash.data.companies.rows[0]['企业'];

  const ev = await get('/api/company?company=' + encodeURIComponent(name) + '&section=evidence');
  check(ev.ok && ev.data.evidence.length >= 3, '企业证据链可返回事实证据');
  check(ev.data.evidence.every((e) => e.source_id in ev.data.sources), '证据可回溯来源');

  const rv = await post('/api/review', { company: name, decision: '标记误报', note: 'node 测试' });
  check(rv.ok && rv.data.decision === '标记误报', '人工复核 POST 生效');
  const ev2 = await get('/api/company?company=' + encodeURIComponent(name) + '&section=evidence');
  check(ev2.data.human_review.status === '已复核', '复核结果写回证据卡片');

  const st = await post('/api/agent/start');
  check(st.ok, '可启动排查（静态模拟）');
  const status = await get('/api/agent/status');
  check(status.running === true, '启动后状态为运行中');
  const prog = await get('/api/runs/latest/progress');
  check(prog.ok && prog.companies.length === 8, '进度接口返回 8 家企业');
  check(prog.companies.some((c) => c['状态'] === 'OPEN' && c['当前阶段']), '运行中 OPEN 企业有当前阶段');

  check((await post('/api/agent/stop')).ok, '可停止排查');
  check((await get('/api/agent/status')).running === false, '停止后状态为已停止');

  check(typeof window.EventSource === 'function', 'EventSource 已垫片');

  const passed = results.filter((r) => r[0]).length;
  console.log(`\n静态站点垫片测试：${passed}/${results.length} 通过\n` + '-'.repeat(50));
  results.forEach((r) => console.log((r[0] ? '  [PASS] ' : '  [FAIL] ') + r[1]));
  process.exit(passed === results.length ? 0 : 1);
})();
