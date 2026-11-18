import {store} from '../../stores/research.js';
import {table} from './table.js';
import {largeStatus} from './large-status.js';
import {esc} from '../../utils/format.js';

export function largeRunList(){
  const runs=store.largeRuns||[];
  return table(['编号 / 大批量研究','状态','已冻结观测','输入链 / 复核'],
    runs.map(r=>[
      {html:`<a href="#large-results/${r.id}">#${r.id} ${esc(r.name)}</a>`},
      {html:largeStatus(r.status)},
      `${(r.input_total||0).toLocaleString()} 条 · ${r.field}`,
      `${(r.input_chain||'').slice(0,12)} · ${r.review?'已复核':'待复核'}`
    ]));
}
