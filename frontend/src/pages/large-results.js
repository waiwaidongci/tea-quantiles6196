import {store} from '../stores/research.js';
import {largeRuns} from '../api/large.js';
import {esc,fmt} from '../utils/format.js';
import {table} from '../components/common/table.js';
import {largeStatus,progressBar} from '../components/common/large-status.js';
import {download} from '../utils/download.js';

export async function render(id){
  if(!id) return `<h1>大批量结果档案</h1><p class="method-note">选择一次大批量研究查看进度、核验项与完整 ECDF。</p>`;
  const r=await largeRuns.get(id);store.largeRunId=r.id;
  const head=`<div class="eyebrow">06 / LARGE RESULTS · RUN ${r.id}</div>
    <h1>${esc(r.name)} ${largeStatus(r.status)}</h1>
    <div class="checksum">输入链 SHA256 ${esc(r.input_chain)}</div>
    <p>冻结观测 ${(r.input_total||0).toLocaleString()} 条 · ${esc(r.created_at)} · ${r.review?'负责人已复核':'尚未复核'}</p>`;

  if(r.status==='failed'){
    return `${head}<div class="error">运行已停止：${esc(r.error||'输入或片段损坏')}。已落妥片段之外的处理全部拒绝，结果不可见。</div>`;
  }
  if(r.status!=='completed'){
    return `${head}${progressBar(r.progress)}
      <p class="method-note">页面每 2 秒刷新，展示“接收到可用”的完整进度（接收/排序/归并/终遍/核验）。未核验通过前结果不可见。</p>`;
  }
  const res=r.result;
  const page=store.largeEcdfPage||0;
  const ecdf=await largeRuns.ecdf(id,page).catch(()=>null);
  const checks=(res.verification&&res.verification.checks)||[];
  return `${head}
    <section class="result-intro"><strong>${esc(res.interpretation)}</strong><p>${esc(res.caveat)}</p></section>
    <h2>发布前核验 <small>${checks.length} 项全部通过后结果才可见</small></h2>
    ${table(['核验项','结论','明细'],checks.map(c=>[c.name,c.ok?'通过':'失败',c.detail||'']))}
    <h2>核心数值</h2>
    ${table(['业务指标','计算值'],Object.entries(res.metrics).map(([k,v])=>[k,fmt(v)]))}
    <h2>${esc(res.table_title)}</h2>
    ${table(res.columns,res.rows)}
    <h2>完整 ECDF <small>共 ${res.ecdf_summary.unique.toLocaleString()} 个唯一点 · ${res.ecdf_summary.pages} 页 · 单页读取，不装载全量</small></h2>
    ${ecdfPanel(id,page,ecdf)}
    <div class="toolbar">
      <button id="large-export">下载核算 CSV</button>
      <a class="button secondary" href="#large-review/${r.id}">打开业务复核 →</a>
    </div>`;
}

function ecdfPanel(id,page,ecdf){
  if(!ecdf) return '<p class="method-note">ECDF 页面读取失败。</p>';
  const rows=ecdf.rows.map(x=>[x.value,x.cum,x.fraction]);
  const nav=`<div class="toolbar">
    <button type="button" class="secondary" data-page="${page-1}" ${page===0?'disabled':''}>← 上一页</button>
    <span>第 ${page+1} / ${ecdf.pages} 页（样本量 N=${ecdf.n.toLocaleString()}，唯一点 ${ecdf.unique.toLocaleString()}）</span>
    <button type="button" class="secondary" data-page="${page+1}" ${page>=ecdf.pages-1?'disabled':''}>下一页 →</button>
  </div>`;
  return `${nav}${table(['取值','累计数 cum','累计比例 P(X≤v)'],rows)}${nav}`;
}

export function bind(){
  document.querySelectorAll('[data-page]').forEach(btn=>{
    btn.onclick=async()=>{
      const p=Math.max(0,+btn.dataset.page);
      store.largeEcdfPage=p;location.hash='large-results/'+store.largeRunId;
    };
  });
  const exp=document.getElementById('large-export');
  if(exp) exp.onclick=async()=>{
    try{download(await largeRuns.export(store.largeRunId),'large-research-'+store.largeRunId+'.csv');}
    catch(e){document.getElementById('notice').textContent=e.message;}
  };
}
