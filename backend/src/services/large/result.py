"""大批量结果组装：把核验通过的清单转成对外结果结构（不依赖 worker 运行时）。"""
from src.services.large.merging import QUANTILE_POINTS


def build_result(run, m: dict) -> dict:
    f = m['finalize']
    q = f['quantiles']
    field_label = {'moisture': '含水率(%)', 'extract': '水浸出率(%)'}[m['field']]
    metrics = {'样本量': f['n'], '均值': f['mean'], '样本标准差': f['std'],
               '最小值': f['min'], 'P25': q['P25'], '中位数': q['中位数'],
               'P75': q['P75'], '最大值': f['max'], 'IQR': f['iqr'],
               '唯一值数': f['unique'], 'ECDF页数': len(f['ecdf_pages']),
               '输入批次': len(m['batches']),
               '内存峰值字节(排序缓冲)': m.get('peak_bytes_seen', 0),
               '内存预算字节': m.get('memory_budget_bytes', 0)}
    return {
        'metrics': metrics,
        'interpretation': (f"大批量精确研究：{f['n']} 份茶样的{field_label}中位数为"
                           f"{q['中位数']:.3f}，P25/P75 为 {q['P25']:.3f}/{q['P75']:.3f}；"
                           f"全量外部排序归并，无抽样。"),
        'caveat': ('结果经发布前独立核验（输入数/累计数/分位点/ECDF 逐项一致）后才可见；'
                   'ECDF 为完整唯一点序列，按页读取，单次仅装载一页。'),
        'columns': ['分位点', '线性插值结果'],
        'rows': [[label, q[label]] for label, _ in QUANTILE_POINTS],
        'table_title': '精确分位数（线性插值，与小批研究同口径）',
        'ecdf_summary': {'unique': f['unique'], 'pages': len(f['ecdf_pages']),
                         'page_route': f'/large-runs/{run.id}/ecdf?page='},
        'input_sha256': m['input_chain'],
        'verification': m.get('verification'),
    }
