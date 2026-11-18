import {DOMAIN} from '../constants/domain.js';
import {store,loadLarge} from '../stores/research.js';
import {largeRuns} from '../api/large.js';
import {studySelect} from '../components/common/study-select.js';
import {largeRunList} from '../components/common/large-run-list.js';
import {largeStatus,progressBar} from '../components/common/large-status.js';
import {bindForm} from '../hooks/forms.js';
import {esc} from '../utils/format.js';

const BATCH_SIZE=5000;
const BATCH_SIZE_LABEL=BATCH_SIZE.toLocaleString()+' 行';

export async function render(){
  const writable=localStorage.getItem('research-role')!=='viewer';
  const active=store.largeRuns.find(r=>r.id===store.largeRunId);
  return `<div class="eyebrow">05 / LARGE-BATCH PIPELINE</div>
  <h1>大批量分布研究 <small style="font-size:13px">外部排序 · 精确分位数 · 完整 ECDF · 无抽样</small></h1>
  <p class="lead">分批接收观测并立即冻结；分段处理与归并可中断恢复，已落妥片段不重做。损坏输入即停止，拒绝拼用其他输入的片段。内存预算 256MB。</p>
  ${writable?`<form id="large-create" class="worksheet">
    ${studySelect()}
    <label>研究名称<input name="name" required minlength="2" value="${esc(DOMAIN.names[0])} · 大批量"></label>
    <div class="inline-form">
      <label>指标列<select name="field">${DOMAIN.fields.map(f=>`<option value="${f[0]}">${f[1]}</option>`).join('')}</select></label>
      <label>声明观测总数（可选，用于发布前核验）<input name="expected_count" type="number" min="1" step="1" placeholder="如 2000000"></label>
    </div>
    <button type="submit">创建大批量研究 →</button>
  </form>`:'<p class="method-note">观察员可查看研究；分析师与负责人可分批接收观测。</p>'}

  <h2>当前接收 <small>选择研究后分批推送，冻结前可查看接收进度</small></h2>
  <div id="large-active">${active?activePanel(active):'<p class="method-note">尚未选择大批量研究。</p>'}</div>

  <h2>大批量研究记录</h2>${largeRunList()}`;
}

function activePanel(r){
  const p=r.progress||{};
  const canIngest=r.status==='ingesting';
  return `<div class="worksheet">
    <p><strong>#${r.id} ${esc(r.name)}</strong> ${largeStatus(r.status)}
    &nbsp;已冻结 <b>${(r.input_total||0).toLocaleString()}</b> 条${r.expected_count?` / 声明 ${r.expected_count.toLocaleString()} 条`:''}</p>
    <div class="checksum">输入链 SHA256 ${esc(r.input_chain||'（首批接收后生成）')}</div>
    ${progressBar(p)}
    ${canIngest?`
    <label>粘贴 JSON 数组或 JSONL（每行一个 {"source_key","values":{"moisture":..}}）
      <textarea id="large-paste" placeholder='[{"source_key":"t-0001","values":{"moisture":12.4}}]'></textarea></label>
    <label>或选择本地文件（.jsonl / .json），前端按每批 ${BATCH_SIZE_LABEL} 自动切分推送
      <input id="large-file" type="file" accept=".jsonl,.json,application/json"></label>
    <div class="toolbar">
      <button id="large-send-paste" type="button">推送粘贴批次 →</button>
      <button id="large-freeze" type="button" class="secondary">冻结输入并排队计算 →</button>
    </div>
    <div id="large-send-progress" class="method-note" style="display:none"></div>
    `:<p class="method-note">输入已冻结或运行结束，不能再追加批次。<a href="#large-results/${r.id}">查看计算进度与结果 →</a></p>`}
  </div>`;
}

export function bind(refresh){
  bindForm('large-create',async data=>{
    const payload={study_id:+data.study_id,name:data.name,field:data.field};
    if(data.expected_count) payload.expected_count=+data.expected_count;
    const r=await largeRuns.create(payload);
    store.largeRunId=r.id;await loadLarge();refresh();
  });
  const sendPaste=document.getElementById('large-send-paste');
  if(sendPaste) sendPaste.onclick=()=>sendRows(parsePaste(),refresh);
  const file=document.getElementById('large-file');
  if(file) file.onchange=()=>handleFile(file.files[0],refresh);
  const freeze=document.getElementById('large-freeze');
  if(freeze) freeze.onclick=async()=>{
    await largeRuns.freeze(store.largeRunId);
    await loadLarge();refresh();
    location.hash='large-results/'+store.largeRunId;
  };
}

function parsePaste(){
  const text=document.getElementById('large-paste').value.trim();
  if(!text) throw new Error('请先粘贴观测数据');
  try{
    const parsed=JSON.parse(text);
    if(Array.isArray(parsed)) return parsed;
    throw new Error('JSON 顶层必须是观测数组');
  }catch(e){
    if(e instanceof SyntaxError){
      return text.split(/\r?\n/).filter(Boolean).map(line=>JSON.parse(line));
    }
    throw e;
  }
}

async function sendRows(rows,refresh){
  if(!store.largeRunId) throw new Error('请先创建并选择大批量研究');
  const note=document.getElementById('large-send-progress');
  note.style.display='block';
  let sent=0;
  for(let i=0;i<rows.length;i+=BATCH_SIZE){
    const chunk=rows.slice(i,i+BATCH_SIZE);
    await largeRuns.addBatch(store.largeRunId,{rows:chunk});
    sent+=chunk.length;
    note.textContent=`已推送 ${sent.toLocaleString()} / ${rows.length.toLocaleString()} 行…`;
    await loadLarge();refresh();
  }
  note.textContent=`全部 ${sent.toLocaleString()} 行已冻结到磁盘。`;
}

async function handleFile(file,refresh){
  if(!file) return;
  const text=await file.text();
  let rows;
  if(file.name.endsWith('.jsonl')){
    rows=text.split(/\r?\n/).filter(Boolean).map(line=>JSON.parse(line));
  }else{
    rows=JSON.parse(text);
    if(!Array.isArray(rows)) throw new Error('JSON 顶层必须是观测数组');
  }
  await sendRows(rows,refresh);
}
