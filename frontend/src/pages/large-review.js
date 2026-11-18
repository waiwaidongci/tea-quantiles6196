import {store,loadLarge} from '../stores/research.js';
import {largeRuns} from '../api/large.js';
import {table} from '../components/common/table.js';
import {largeStatus} from '../components/common/large-status.js';
import {esc} from '../utils/format.js';
import {bindForm} from '../hooks/forms.js';

export async function render(id){
  const complete=(store.largeRuns||[]).filter(r=>r.status==='completed');
  const r=id?await largeRuns.get(id):null;
  return `<div class="eyebrow">07 / LARGE RELEASE</div>
  <h1>大批量研究复核与交付</h1>
  <p>仅核验通过的研究可复核；复核不可覆盖。观察员只能导出已复核结果。</p>
  ${r?`<h2>#${r.id} ${esc(r.name)} ${largeStatus(r.status)}</h2>
    ${r.review?`<div class="method-note"><strong>已复核</strong><p>${esc(r.review.note)}</p><small>${esc(r.review.author)} · ${esc(r.review.created_at)}</small></div>`
      :localStorage.getItem('research-role')==='admin'
        ?`<form id="large-review-form"><label>复核意见与后续动作<textarea name="note" minlength="5" maxlength="2000" required placeholder="写明数据范围、精确口径、内存与恢复边界及复测建议"></textarea></label><button type="submit">确认复核并归档</button></form>`
        :'<div class="method-note">等待负责人复核；分析师可以查看全部计算依据与核验项。</div>'}`:''}
  ${table(['大批量运行','复核状态','动作'],complete.map(x=>[x.name,x.review?x.review.note:'待复核',{html:`<a href="#large-review/${x.id}">查看 / 复核 →</a>`}]))}`;
}

export function bind(refresh){
  bindForm('large-review-form',async data=>{
    const id=Number(location.hash.split('/')[1]);
    await largeRuns.review(id,data.note);
    await loadLarge();refresh();
  });
}
