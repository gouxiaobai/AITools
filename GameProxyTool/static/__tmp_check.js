
// ================================================================== //
//  Known action dictionary (extend as needed)                            //
// ================================================================== //
const ACTIONS = {
  // VIP
  19600: '领取VIP每日免费宝箱奖励'
  19601: '领取VIP免费礼包奖励',
  19602: '购买VIP点数或时间'
  // Month card
  19660: 'MonthCardReceiveDailyReward',
  19661: 'MonthAndWeekCardAllReward',
  // Week card
  19650: 'WeekCardSyncInfo',
  19651: 'WeekCardReceiveFreeBox',
  19652: 'WeekCardReceiveDailyReward',
  19653: 'WeekCardSyncItems',
  19654: 'WeekCardReceiveAllReward',
  // Mail
  50001: 'RewardMail',
  50002: 'DeleteMails',
  50005: 'CollectMail',
  50006: 'SyncMailInfo',
  // Common
  0: 'VoidResponse',
  1: 'Sync',
  209: 'CamelPaymentReceipt',
  301: 'CrossForward',
  902: 'TestCmd',
  3000: 'ChatSend',
  3001: 'ChatOpen',
  10042: 'MapTcpLogin',
  10043: 'MapTcpRelogin',
  20000: 'SyncMapData',
};

// ================================================================== //
//  Template definitions                                                   //
// ================================================================== //
const TEMPLATES = [
  { action: 19660, name: 'Month Card Daily Reward', params: {} },
  { action: 19661, name: 'Month+Week Card Claim All', params: { activityId: 0 } },
  { action: 19650, name: 'Week Card Sync', params: { activityId: 0 } },
  { action: 19651, name: 'Week Card Free Box', params: { activityId: 0 } },
  { action: 19652, name: 'Week Card Daily Reward', params: { activityId: 0, cardId: 0 } },
  { action: 19654, name: 'Week Card Claim All', params: { activityId: 0 } },
  { action: 50001, name: 'Mail Claim Reward', params: { mailIdList: [0] } },
  { action: 50002, name: 'Delete Mails', params: { mails: [0] } },
  { action: 50005, name: 'Mail Favorite Toggle', params: { mailId: 0 } },
  { action: 50006, name: 'Sync Mail Info', params: { maxMailId: 0, mailSyncVersion: 0 } },
];

// ================================================================== //
//  State                                                                  //
// ================================================================== //
let allPackets = [];
let visiblePackets = [];
let selectedId = null;
let currentTab = 'view';
let currentRunId = null;
let currentRunData = null;
let testPollTimer = null;

// ================================================================== //
//  Initialization                                                         //
// ================================================================== //
function init() {
  buildTemplates();
  refreshTestRuns();
  fetchStatus();
  setInterval(fetchStatus, 3000);

  // Initial load for existing packets.
  fetch('/api/packets').then(r => r.json()).then(pkts => {
    pkts.forEach(addPacket);
  });

  // Live stream via SSE.
  const es = new EventSource('/api/stream');
  es.onmessage = e => {
    addPacket(JSON.parse(e.data));
  };
  es.onerror = () => {
    // Fallback poll after disconnect.
    setTimeout(() => {
      const after = allPackets.length ? allPackets[allPackets.length - 1].id : -1;
      fetch(`/api/packets?after=${after}`).then(r => r.json()).then(pkts => pkts.forEach(addPacket));
    }, 2000);
  };
}

function defaultSuiteObj() {
  return {
    name: 'ui_suite',
    cases: [
      {
        name: 'case_1',
        steps: [
          {
            action: 50006,
            params: { maxMailId: 0, mailSyncVersion: 0 },
            timeout_ms: 5000,
            expect: [{ type: 'exists', path: 'commResp', value: true }]
          }
        ]
      }
    ]
  };
}

function initSuiteEditor() {
  const txt = document.getElementById('inlineSuite');
  if (!txt.value.trim()) {
    txt.value = JSON.stringify(defaultSuiteObj(), null, 2);
  }
  loadEditorFromInline();
}

function loadEditorFromInline() {
  let suite;
  try {
    suite = JSON.parse(document.getElementById('inlineSuite').value);
  } catch (e) {
    toast('JSON 解析失败: ' + e.message, true);
    return;
  }
  if (!suite.cases && Array.isArray(suite.steps)) {
    suite = { name: suite.name || 'inline_suite', cases: [{ name: suite.name || 'case_1', steps: suite.steps }] };
  }
  if (!Array.isArray(suite.cases)) suite.cases = [];
  document.getElementById('suiteNameInput').value = suite.name || 'ui_suite';
  const box = document.getElementById('suiteCases');
  box.innerHTML = '';
  suite.cases.forEach(c => box.appendChild(createCaseEl(c)));
  document.getElementById('useInlineSuite').checked = true;
}

function createCaseEl(caseData = {}) {
  const caseEl = document.createElement('div');
  caseEl.className = 'case-card';
  caseEl.innerHTML = `
    <div class="case-head">
      <label style="color:var(--muted)">Case:</label>
      <input type="text" class="case-name mono" style="min-width:220px" value="${escapeHtml(caseData.name || 'case_new')}">
      <button class="secondary add-step-btn">Add Step</button>
      <button class="danger del-case-btn">Delete Case</button>
    </div>
    <div class="steps-box"></div>
  `;
  const stepsBox = caseEl.querySelector('.steps-box');
  (caseData.steps || []).forEach(s => stepsBox.appendChild(createStepEl(s)));
  if (!stepsBox.children.length) stepsBox.appendChild(createStepEl());

  caseEl.querySelector('.add-step-btn').onclick = () => stepsBox.appendChild(createStepEl());
  caseEl.querySelector('.del-case-btn').onclick = () => caseEl.remove();
  return caseEl;
}

function createStepEl(step = {}) {
  const stepEl = document.createElement('div');
  stepEl.className = 'step-card';
  const action = step.action ?? '';
  const responseAction = step.response_action ?? '';
  const timeoutMs = step.timeout_ms ?? 5000;
  const paramsText = JSON.stringify(step.params || {}, null, 2);
  stepEl.innerHTML = `
    <div class="step-head">
      <label style="color:var(--muted)">Action</label>
      <input type="number" class="step-action mono" value="${action}" style="width:100px">
      <label style="color:var(--muted)">RespAction</label>
      <input type="number" class="step-resp-action mono" value="${responseAction}" style="width:100px" placeholder="default: same as request action">
      <label style="color:var(--muted)">Timeout(ms)</label>
      <input type="number" class="step-timeout mono" value="${timeoutMs}" style="width:110px">
      <button class="secondary add-expect-btn">Add Expect</button>
      <button class="danger del-step-btn">Delete Step</button>
    </div>
    <div>
      <label style="color:var(--muted)">Params (JSON)</label>
      <textarea class="step-params mono" rows="4">${escapeHtml(paramsText)}</textarea>
    </div>
    <div class="expects-box"></div>
  `;
  const expectsBox = stepEl.querySelector('.expects-box');
  (step.expect || []).forEach(ex => expectsBox.appendChild(createExpectEl(ex)));
  if (!expectsBox.children.length) expectsBox.appendChild(createExpectEl({ type: 'exists', path: 'commResp', value: true }));

  stepEl.querySelector('.add-expect-btn').onclick = () => expectsBox.appendChild(createExpectEl());
  stepEl.querySelector('.del-step-btn').onclick = () => stepEl.remove();
  return stepEl;
}

function createExpectEl(exp = {}) {
  const exEl = document.createElement('div');
  exEl.className = 'expect-row';
  const type = exp.type || 'exists';
  const path = exp.path || '';
  const valueText = exp.value === undefined ? '' : JSON.stringify(exp.value);
  exEl.innerHTML = `
    <select class="expect-type mono">
      <option value="exists"${type === 'exists' ? ' selected' : ''}>exists</option>
      <option value="equals"${type === 'equals' ? ' selected' : ''}>equals</option>
      <option value="contains"${type === 'contains' ? ' selected' : ''}>contains</option>
    </select>
    <input type="text" class="expect-path mono" value="${escapeHtml(path)}" placeholder="commResp.code">
    <input type="text" class="expect-value mono" value="${escapeHtml(valueText)}" placeholder='true / 0 / "ok" / {"k":1}'>
    <button class="danger del-expect-btn">Delete</button>
  `;
  exEl.querySelector('.del-expect-btn').onclick = () => exEl.remove();
  return exEl;
}

function addCaseUI() {
  document.getElementById('suiteCases').appendChild(createCaseEl());
  document.getElementById('useInlineSuite').checked = true;
}

function syncEditorToInline() {
  let suite;
  try {
    suite = collectSuiteFromEditor();
  } catch (e) {
    toast(e.message || String(e), true);
    return;
  }
  document.getElementById('inlineSuite').value = JSON.stringify(suite, null, 2);
  document.getElementById('useInlineSuite').checked = true;
  toast('已从编辑器生成 JSON。');
}

function collectSuiteFromEditor() {
  const suiteName = document.getElementById('suiteNameInput').value.trim() || 'ui_suite';
  const caseEls = [...document.querySelectorAll('#suiteCases .case-card')];
  if (!caseEls.length) {
    throw new Error('请至少添加一个 Case。');
  }
  const cases = caseEls.map((caseEl, ci) => {
    const caseName = caseEl.querySelector('.case-name').value.trim() || `case_${ci + 1}`;
    const stepEls = [...caseEl.querySelectorAll('.step-card')];
    if (!stepEls.length) throw new Error(`Case[${ci + 1}] has no step.`);
    const steps = stepEls.map((stepEl, si) => {
      const actionRaw = stepEl.querySelector('.step-action').value;
      const action = parseInt(actionRaw, 10);
      if (!action) throw new Error(`Case[${ci + 1}] Step[${si + 1}] action is invalid.`);

      const respRaw = stepEl.querySelector('.step-resp-action').value.trim();
      const timeoutRaw = stepEl.querySelector('.step-timeout').value;
      const timeoutMs = parseInt(timeoutRaw || '5000', 10);
      if (!timeoutMs || timeoutMs < 1) throw new Error(`Case[${ci + 1}] Step[${si + 1}] timeout_ms is invalid.`);

      const paramsText = stepEl.querySelector('.step-params').value.trim();
      let params = {};
      if (paramsText) {
        try { params = JSON.parse(paramsText); }
        catch (e) { throw new Error(`Case[${ci + 1}] Step[${si + 1}] params JSON invalid: ${e.message}`); }
      }
      const expectEls = [...stepEl.querySelectorAll('.expect-row')];
      const expect = expectEls.map((exEl, ei) => {
        const type = exEl.querySelector('.expect-type').value;
        const path = exEl.querySelector('.expect-path').value.trim();
        const valText = exEl.querySelector('.expect-value').value.trim();
        let value;
        if (!valText && type === 'exists') {
          value = true;
        } else if (!valText) {
          value = '';
        } else {
          try { value = JSON.parse(valText); }
          catch (_) { value = valText; }
        }
        return { type, path, value };
      });
      const step = { action, params, timeout_ms: timeoutMs, expect };
      if (respRaw) step.response_action = parseInt(respRaw, 10);
      return step;
    });
    return { name: caseName, steps };
  });
  return { name: suiteName, cases };
}

function escapeHtml(s) {
  return String(s)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function downloadSuiteTemplate() {
  window.location.href = '/api/test/template.xlsx';
}

function startTestRun() {
  const conn = parseInt(document.getElementById('testConn').value) || 0;
  const suiteFile = document.getElementById('suiteXlsxFile').files[0];
  if (!suiteFile) {
    toast('请先上传 .xlsx 测试用例。', true);
    return;
  }

  const fd = new FormData();
  fd.append('file', suiteFile);
  fd.append('conn', String(conn));
  fetch('/api/test/upload-run', {
    method: 'POST',
    body: fd
  }).then(r => r.json()).then(res => {
    if (!res.ok) {
      toast('启动失败: ' + (res.error || 'unknown'), true);
      return;
    }
    currentRunId = res.run.run_id;
    toast(`已启动运行: ${currentRunId}`);
    renderRunStatus(res.run);
    refreshTestRuns();
    startRunPolling();
  }).catch(e => toast('启动失败: ' + e.message, true));
}

function stopTestRun() {
  if (!currentRunId) {
    toast('当前没有运行任务。', true);
    return;
  }
  fetch(`/api/test/stop/${currentRunId}`, { method: 'POST' })
    .then(r => r.json())
    .then(res => {
      if (res.ok) {
        toast('已发送停止请求。');
        renderRunStatus(res.run);
      } else {
        toast('停止失败: ' + (res.error || 'unknown'), true);
      }
    })
    .catch(e => toast('停止失败: ' + e.message, true));
}

function startRunPolling() {
  if (testPollTimer) clearInterval(testPollTimer);
  testPollTimer = setInterval(() => {
    if (!currentRunId) return;
    fetch(`/api/test/run/${currentRunId}`).then(r => r.json()).then(run => {
      renderRunStatus(run);
      if (run.status !== 'running') {
        clearInterval(testPollTimer);
        testPollTimer = null;
        refreshTestRuns();
      }
    }).catch(() => {});
  }, 1000);
}

function refreshTestRuns() {
  fetch('/api/test/runs').then(r => r.json()).then(runs => {
    const box = document.getElementById('runsList');
    if (!runs.length) {
      box.innerHTML = '<div class="run-row"><span style="color:var(--muted)">暂无历史</span><span></span><span></span><span></span><span></span></div>';
      return;
    }
    box.innerHTML = '';
    runs.forEach(run => {
      const row = document.createElement('div');
      row.className = 'run-row';
      const ended = run.ended_at ? new Date(run.ended_at * 1000).toLocaleTimeString() : '-';
      row.innerHTML = `
        <span><b>${run.run_id}</b> ${run.suite_name || run.suite_path || '-'}</span>
        <span>${run.status}</span>
        <span>${run.progress.completed_cases || 0}/${run.progress.total_cases || 0}</span>
        <span>${run.progress.completed_steps || 0}/${run.progress.total_steps || 0}</span>
        <span>${ended}</span>
      `;
      row.querySelector('b').onclick = () => {
        currentRunId = run.run_id;
        fetch(`/api/test/run/${run.run_id}`).then(r => r.json()).then(renderRunStatus);
      };
      box.appendChild(row);
    });
  }).catch(e => toast('刷新历史失败: ' + e.message, true));
}

function renderRunStatus(run) {
  currentRunData = run;
  const p = run.progress || {};
  const result = run.result || {};
  const lines = [];
  lines.push(`run_id: ${run.run_id}`);
  lines.push(`状态: ${run.status}`);
  lines.push(`套件: ${run.suite_name || run.suite_path || '-'}`);
  lines.push(`Case 进度: ${p.completed_cases || 0}/${p.total_cases || 0}`);
  lines.push(`Step 进度: ${p.completed_steps || 0}/${p.total_steps || 0}`);
  lines.push(`当前 Case: ${p.current_case || '-'}`);
  if (run.error) lines.push(`错误: ${run.error}`);
  if (result.suite_status) {
    lines.push(`结果: ${result.suite_status}`);
    lines.push(`通过 Case: ${result.passed_cases}/${result.total_cases}`);
    lines.push(`通过 Step: ${result.passed_steps}/${result.total_steps}`);
  }
  if (run.report_path) lines.push(`报告: ${run.report_path}`);
  document.getElementById('testStatus').textContent = lines.join('\n');
}

function exportCurrentRun() {
  if (!currentRunData || !currentRunData.result) {
    toast('当前没有可导出的结果。', true);
    return;
  }
  const run = currentRunData;
  const data = JSON.stringify(run.result, null, 2);
  const blob = new Blob([data], { type: 'application/json' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = `test_run_${run.run_id}.json`;
  a.click();
}

function buildTemplates() {
  const grid = document.getElementById('templateGrid');
  TEMPLATES.forEach(t => {
    const btn = document.createElement('button');
    btn.className = 'tpl-btn';
    btn.innerHTML = `<span class="tpl-action">${t.action}</span>${t.name}<br><span class="tpl-name">${ACTIONS[t.action] || ''}</span>`;
    btn.onclick = () => {
      document.getElementById('sendAction').value = t.action;
      document.getElementById('sendParams').value = JSON.stringify(t.params, null, 2);
      switchTab('send');
    };
    grid.appendChild(btn);
  });
}

// ================================================================== //
//  Packet handling                                                        //
// ================================================================== //
function addPacket(pkt) {
  allPackets.push(pkt);
  document.getElementById('pktCount').textContent = allPackets.length;

  if (matchesFilter(pkt)) {
    visiblePackets.push(pkt);
    renderRow(pkt);
    if (document.getElementById('autoScroll').checked) {
      const list = document.getElementById('pktList');
      list.scrollTop = list.scrollHeight;
    }
  }
}

function matchesFilter(pkt) {
  const af = document.getElementById('filterAction').value.trim();
  const df = document.getElementById('filterDir').value;
  const sf = document.getElementById('searchBox').value.trim().toLowerCase();

  if (af && String(pkt.action) !== af) return false;
  if (df && pkt.direction !== df) return false;
  if (sf) {
    const str = JSON.stringify(pkt.decoded).toLowerCase();
    if (!str.includes(sf)) return false;
  }
  return true;
}

function renderRow(pkt) {
  const list = document.getElementById('pktList');
  if (list.querySelector('.empty-hint')) list.innerHTML = '';

  const row = document.createElement('div');
  row.className = 'pkt-row' + (pkt.id === selectedId ? ' selected' : '');
  row.id = `row-${pkt.id}`;
  row.onclick = () => selectPacket(pkt.id);

  const ts = new Date(pkt.timestamp * 1000);
  const timeStr = ts.toTimeString().slice(0, 8);
  const actionName = ACTIONS[pkt.action] || '';

  row.innerHTML = `
    <span class="pkt-id">${pkt.id}</span>
    <span class="dir-badge dir-${pkt.direction.toLowerCase()}">${pkt.direction}</span>
    <span class="pkt-action">${pkt.action ?? '-'}</span>
    <span class="pkt-name" title="${actionName}">${actionName}${pkt.note ? `<span class="pkt-note">*</span>` : ''}</span>
    <span class="pkt-time">${timeStr}</span>
  `;
  list.appendChild(row);
}

function applyFilter() {
  visiblePackets = allPackets.filter(matchesFilter);
  const list = document.getElementById('pktList');
  list.innerHTML = visiblePackets.length ? '' : '<div class="empty-hint">无匹配数据包</div>';
  visiblePackets.forEach(renderRow);
}

// ================================================================== //
//  Selection & details                                                    //
// ================================================================== //
function selectPacket(id) {
  selectedId = id;
  document.querySelectorAll('.pkt-row').forEach(r => r.classList.remove('selected'));
  const row = document.getElementById(`row-${id}`);
  if (row) row.classList.add('selected');

  // Get packet detail (includes full hex).
  fetch(`/api/packets/${id}`).then(r => r.json()).then(pkt => {
    renderDetail(pkt);
    populateEditor(pkt);
    renderHex(pkt);
  });
}

function renderDetail(pkt) {
  const actionName = ACTIONS[pkt.action] || '';
  const ts = new Date(pkt.timestamp * 1000).toLocaleString();
  document.getElementById('detailHeader').innerHTML = `
    <div class="detail-meta">
      <span class="kv"><span>#</span>${pkt.id}</span>
      <span class="dir-badge dir-${pkt.direction.toLowerCase()}">${pkt.direction}</span>
      <span class="kv"><span>Action </span><b style="color:var(--yellow)">${pkt.action ?? '-'}</b></span>
      <span class="kv" style="color:var(--accent)">${actionName}</span>
      <span class="kv"><span>${ts}</span></span>
    </div>
    <button class="secondary" style="margin-left:auto" onclick="promptNote(${pkt.id})">备注</button>
    <button onclick="loadReplay(${pkt.id})" title="切换到编辑页">编辑重放</button>
  `;
  document.getElementById('jsonView').innerHTML = syntaxHighlight(pkt.decoded);
}

function syntaxHighlight(obj) {
  const str = JSON.stringify(obj, null, 2);
  return str.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, match => {
    let cls = 'json-num';
    if (/^"/.test(match)) {
      cls = /:$/.test(match) ? 'json-key' : 'json-str';
    } else if (/true|false/.test(match)) {
      cls = 'json-bool';
    } else if (/null/.test(match)) {
      cls = 'json-null';
    }
    return `<span class="${cls}">${match}</span>`;
  });
}

function renderHex(pkt) {
  const hex = pkt.raw_hex || '';
  let out = '';
  for (let i = 0; i < hex.length; i += 2) {
    out += hex.slice(i, i+2) + ' ';
    if ((i / 2 + 1) % 16 === 0) out += '\n';
  }
  document.getElementById('hexView').textContent = out;
}

// ================================================================== //
//  Edit / replay                                                          //
// ================================================================== //
function populateEditor(pkt) {
  const copy = Object.assign({}, pkt.decoded);
  // commReq is re-generated during send.
  delete copy.commReq;
  document.getElementById('editArea').value = JSON.stringify(copy, null, 2);
}

function loadReplay(id) {
  fetch(`/api/packets/${id}`).then(r => r.json()).then(pkt => {
    populateEditor(pkt);
    switchTab('edit');
  });
}

function replayEdited() {
  let msg;
  try { msg = JSON.parse(document.getElementById('editArea').value); }
  catch (e) { toast('JSON 格式错误: ' + e.message, true); return; }

  if (!msg.action) { toast('缺少 action 字段。', true); return; }
  const conn = parseInt(document.getElementById('replayConn').value) || 0;

  const pid = selectedId;
  const selected = allPackets.find(p => p.id === pid);
  if (selected && selected.direction === 'C2S') {
    fetch(`/api/replay/${pid}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ conn, overrides: msg })
    }).then(r => r.json()).then(res => {
      if (res.ok) toast(`已重放 (msgId=${res.msg_id ?? '-'})`);
      else toast('失败: ' + res.error, true);
    });
    return;
  }

  const action = parseInt(msg.action);
  const params = Object.assign({}, msg);
  delete params.action;
  fetch('/api/send', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action, params, conn })
  }).then(r => r.json()).then(res => {
    if (res.ok) toast(`已发送 (msgId=${res.msg_id ?? '-'})`);
    else toast('失败: ' + res.error, true);
  });
}

function formatEditor() {
  try {
    const obj = JSON.parse(document.getElementById('editArea').value);
    document.getElementById('editArea').value = JSON.stringify(obj, null, 2);
  } catch (e) {
    toast('JSON 格式错误', true);
  }
}

// ================================================================== //
//  Custom send                                                            //
// ================================================================== //
function sendCustom() {
  const action = parseInt(document.getElementById('sendAction').value);
  if (!action) { toast('请填写 Action。', true); return; }

  let params = {};
  const raw = document.getElementById('sendParams').value.trim();
  if (raw) {
    try { params = JSON.parse(raw); }
    catch (e) { toast('参数 JSON 格式错误: ' + e.message, true); return; }
  }
  const conn = parseInt(document.getElementById('sendConn').value) || 0;

  fetch('/api/send', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action, params, conn })
  }).then(r => r.json()).then(res => {
    if (res.ok) {
      toast(`已发送 (msgId=${res.msg_id})`);
      document.getElementById('sendResult').textContent = `发送成功，消息 ID: ${res.msg_id}`;
    } else {
      toast('失败: ' + res.error, true);
      document.getElementById('sendResult').textContent = '错误: ' + res.error;
    }
  });
}

// ================================================================== //
//  Misc actions                                                           //
// ================================================================== //
function clearAll() {
  if (!confirm('确定清空所有已捕获的数据包吗？')) return;
  fetch('/api/clear', { method: 'POST' }).then(() => {
    allPackets = [];
    visiblePackets = [];
    selectedId = null;
    document.getElementById('pktList').innerHTML = '<div class="empty-hint">等待数据包...</div>';
    document.getElementById('jsonView').innerHTML = '';
    document.getElementById('hexView').textContent = '选择数据包后显示原始 HEX...';
    document.getElementById('pktCount').textContent = '0';
  });
}

function setToken() {
  const token = document.getElementById('tokenInput').value.trim();
  fetch('/api/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ token })
  }).then(() => {
    document.getElementById('tokenDisp').textContent = token.slice(0, 12) + '...';
    toast('Token 已设置。');
  });
}

function promptNote(id) {
  const note = prompt('为此数据包添加备注:');
  if (note === null) return;
  fetch(`/api/packets/${id}/note`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ note })
  }).then(() => {
    const pkt = allPackets.find(p => p.id === id);
    if (pkt) pkt.note = note;
    applyFilter();
    toast('备注已保存。');
  });
}

function exportJSON() {
  const data = JSON.stringify(visiblePackets, null, 2);
  const blob = new Blob([data], { type: 'application/json' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = `packets_${Date.now()}.json`;
  a.click();
}

function fetchStatus() {
  fetch('/api/status').then(r => r.json()).then(s => {
    document.getElementById('connCount').textContent = s.active_connections;
    if (s.token) document.getElementById('tokenDisp').textContent = String(s.token).slice(0, 12) + '...';
    renderSetupGuide(s.local_ip, s.http_port, s.active_connections);
  });
}

function renderSetupGuide(localIp, httpPort, connCount) {
  const el = document.getElementById('setupGuide');
  if (connCount > 0) {
    el.innerHTML = `<div class="setup-card"><h4>已连接: ${connCount} 条活跃连接</h4></div>`;
    return;
  }
  el.innerHTML = `
    <div class="setup-card">
      <h4>手机代理配置（仅需一次）</h4>
      <div class="setup-steps">
        <div class="setup-step"><div class="step-num">1</div><div class="step-txt">手机与电脑连接到<b>同一 Wi-Fi</b>。</div></div>
        <div class="setup-step"><div class="step-num">2</div><div class="step-txt">手机 Wi-Fi 代理改为<b>手动</b><br>主机: <b>${localIp}</b> 端口: <b>${httpPort}</b></div></div>
        <div class="setup-step"><div class="step-num">3</div><div class="step-txt">重启游戏，SSO 请求会自动被拦截。</div></div>
      </div>
    </div>`;
}

function switchTab(name) {
  currentTab = name;
  document.querySelectorAll('.tab-btn').forEach((b, i) => {
    const tabs = ['view', 'edit', 'hex', 'send', 'test'];
    b.classList.toggle('active', tabs[i] === name);
  });
  document.querySelectorAll('.tab-content').forEach(el => {
    el.classList.toggle('active', el.id === `tab-${name}`);
  });
}

let _toastTimer;
function toast(msg, err = false) {
  const el = document.createElement('div');
  el.className = 'toast' + (err ? ' err' : '');
  el.textContent = msg;
  document.body.appendChild(el);
  clearTimeout(_toastTimer);
  _toastTimer = setTimeout(() => el.remove(), 2500);
}

init();

