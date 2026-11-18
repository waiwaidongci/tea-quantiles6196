import {esc,fmt} from '../../utils/format.js';
import {LARGE_STATE_LABELS} from '../../types/enums.js';

const PHASE_LABELS={ingesting:'分批接收与冻结',sorting:'分段排序',merging:'流式归并',
  finalizing:'终遍统计与 ECDF',verified:'核验通过可发布',failed:'已停止'};

export function largeStatus(x){
  return `<span class="status ${x}">${LARGE_STATE_LABELS[x]||x}</span>`;
}

export function progressBar(p){
  if(!p) return '';
  const pct=p.total?Math.min(100,Math.round(p.done/p.total*100)):(p.phase==='verified'?100:0);
  const phase=PHASE_LABELS[p.phase]||LARGE_STATE_LABELS[p.phase]||p.phase||'';
  const failed=p.failed?' failed':'';
  return `<div class="progress-block${failed}">
    <div class="progress-head"><b>${esc(phase)}</b><span>${fmt(p.done||0)} / ${fmt(p.total||0)} · ${pct}%</span></div>
    <div class="progress-track"><i style="width:${pct}%"></i></div>
    <small>${esc(p.detail||'')}${p.segments?` · 已落段 ${p.segments} · 批次 ${p.batches||0}`:''}</small>
    ${p.failed?`<div class="error">${esc(p.failure||'运行已停止')}</div>`:''}
  </div>`;
}
