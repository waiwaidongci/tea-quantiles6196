"""流式 k 路归并、终遍统计与发布前独立核验。

归并全程堆中只保留每路一个值（heapq.merge），每路读取器持有有界字节缓冲，
输出分块追加；任何参与归并的段在使用前都要通过 SHA256 与输入链核验，
已落妥的归并段在恢复时直接跳过，不重做。
"""
import hashlib, heapq, json, math, os
import numpy as np
from . import layout
from .layout import (LargeRunError, segment_path, segment_meta_path,
                     ecdf_page_path, write_json_atomic, read_json, file_sha256)
from .sorting import iter_segment
from .inputs import save_manifest

DT = np.dtype('<f8')
WRITE_CHUNK = 8192          # 归并输出块（float 个数），远小于字节预算
QUANTILE_POINTS = (('最小值', 0.0), ('P25', 0.25), ('中位数', 0.5),
                   ('P75', 0.75), ('最大值', 1.0))


def _level_metas(m: dict, level: int):
    if level == 0:
        return sorted(m['segments'], key=lambda x: x['seq'])
    return sorted(m['levels'].get(str(level), []), key=lambda x: x['seq'])


def _set_level(m: dict, level: int, metas):
    m['levels'][str(level)] = sorted(metas, key=lambda x: x['seq'])


def _check_meta(root, m: dict, level: int, meta: dict):
    if meta.get('input_chain') != m['input_chain']:
        raise LargeRunError(f'段 L{level}/#{meta["seq"]} 属于其他输入，拒绝拼用')
    p = segment_path(root, level, meta['seq'])
    if not p.exists() or file_sha256(p) != meta['sha256']:
        raise LargeRunError(f'段 L{level}/#{meta["seq"]} 缺失或损坏，停止运行')


def _write_merged(root, m: dict, level: int, seq: int, values) -> dict:
    """流式消费归并迭代器，分块写二进制段并实时计算校验和。"""
    path = segment_path(root, level, seq)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.bin.tmp')
    h = hashlib.sha256()
    rows = 0
    prev = None
    chunk = np.empty(WRITE_CHUNK, dtype=DT)
    used = 0
    with open(tmp, 'wb') as f:
        def drain():
            nonlocal used
            if used == 0:
                return
            block = chunk[:used].tobytes()
            f.write(block)
            h.update(block)
            used = 0
        for v in values:
            if prev is not None and v < prev:
                raise LargeRunError('归并结果违反有序性，停止运行')
            prev = v
            chunk[used] = v
            used += 1
            rows += 1
            if used >= WRITE_CHUNK:
                drain()
        drain()
    digest = h.hexdigest()
    os.replace(tmp, path)
    meta = {'seq': seq, 'level': level, 'rows': rows,
            'sha256': digest, 'input_chain': m['input_chain']}
    write_json_atomic(segment_meta_path(root, level, seq), meta)
    return meta


def merge_levels(root, m: dict) -> dict:
    """层级归并，直到只剩一个总段；每层每段可独立恢复。"""
    if m['stage'] == 'finalizing' or m['stage'] == 'verified':
        return m
    if m['stage'] != 'merging':
        raise LargeRunError(f'当前阶段 {m["stage"]} 不能执行归并')

    level = 0
    while len(_level_metas(m, level)) > 1:
        metas = _level_metas(m, level)
        for meta in metas:
            _check_meta(root, m, level, meta)
        out_level = level + 1
        done = list(_level_metas(m, out_level))
        groups = [metas[i:i + layout.MERGE_FANOUT]
                  for i in range(0, len(metas), layout.MERGE_FANOUT)]
        for seq, group in enumerate(groups):
            if any(x['seq'] == seq for x in done):
                meta = next(x for x in done if x['seq'] == seq)
                _check_meta(root, m, out_level, meta)
                continue  # 已落妥的归并段不重做
            streams = [iter_segment(root, level, meta) for meta in group]
            merged = heapq.merge(*streams)
            meta = _write_merged(root, m, out_level, seq, merged)
            done.append(meta)
            _set_level(m, out_level, done)
            m['progress'] = {
                'phase': 'merging',
                'done': sum(x['rows'] for x in done),
                'total': m['input_total'],
                'detail': f'归并 L{level}→L{out_level}：{len(done)}/{len(groups)} 段'}
            save_manifest(root, m)
        _set_level(m, out_level, done)
        if sum(x['rows'] for x in done) != m['input_total']:
            from .inputs import fail
            fail(root, m, f'L{out_level} 归并累计数与输入数不一致')
        level += 1
        save_manifest(root, m)

    m['final_level'] = level
    m['stage'] = 'finalizing'
    m['progress'] = {'phase': 'finalizing', 'done': 0, 'total': m['input_total'],
                     'detail': '归并完成，执行终遍：精确分位数与完整 ECDF'}
    save_manifest(root, m)
    return m


def _final_segment(root, m: dict):
    level = m.get('final_level', 0)
    metas = _level_metas(m, level)
    if len(metas) != 1:
        raise LargeRunError('最终有序段不唯一，无法终遍')
    meta = metas[0]
    _check_meta(root, m, level, meta)
    return level, meta


def _quantile_ranks(n: int):
    targets = {}
    for label, q in QUANTILE_POINTS:
        h = (n - 1) * q
        i = int(math.floor(h))
        j = int(math.ceil(h))
        targets.setdefault(i, set()).add((label, 'lo', h))
        if j != i:
            targets.setdefault(j, set()).add((label, 'hi', h))
    return targets


def _linear_quantile(n: int, captured: dict) -> dict:
    out = {}
    for label, q in QUANTILE_POINTS:
        h = (n - 1) * q
        i = int(math.floor(h))
        j = int(math.ceil(h))
        lo = captured[i]
        hi = captured[j]
        out[label] = lo + (hi - lo) * (h - i)
    return out


def finalize(root, m: dict) -> dict:
    """单遍终遍：精确分位数、完整 ECDF 分页落盘、有界统计量。"""
    level, meta = _final_segment(root, m)
    n = meta['rows']
    rank_targets = _quantile_ranks(n)
    need_ranks = set(rank_targets)
    captured = {}

    total = 0.0          # Neumaier 补偿求和
    comp = 0.0
    sumsq = 0.0
    comp2 = 0.0
    prev = None
    unique = 0
    pages, page_rows, page_h = [], [], hashlib.sha256()
    page_no = 0
    ecdf_dir = root / 'ecdf'
    ecdf_dir.mkdir(parents=True, exist_ok=True)
    cur_value = None
    cur_count = 0

    def flush_ecdf_page():
        nonlocal page_no, page_rows, page_h
        if not page_rows:
            return
        p = ecdf_page_path(root, page_no)
        text = ''.join(json.dumps(r, sort_keys=True, allow_nan=False) + '\n'
                       for r in page_rows)
        tmp = p.with_suffix('.jsonl.tmp')
        tmp.write_text(text, encoding='utf-8')
        digest = file_sha256(tmp)
        os.replace(tmp, p)
        pages.append({'page': page_no, 'rows': len(page_rows), 'sha256': digest})
        page_no += 1
        page_rows, page_h = [], hashlib.sha256()

    rank = 0
    for v in iter_segment(root, level, meta):
        if prev is not None and v < prev:
            raise LargeRunError('终遍发现有序性破坏，停止运行')
        if rank in need_ranks:
            captured[rank] = v
        # Neumaier 补偿求和（括号不可省：+= 优先级低于条件表达式）。
        t = total + v
        comp += ((total - t) + v) if abs(total) >= abs(v) else ((v - t) + total)
        total = t
        vv = v * v
        t2 = sumsq + vv
        comp2 += ((sumsq - t2) + vv) if abs(sumsq) >= abs(vv) else ((vv - t2) + sumsq)
        sumsq = t2

        if cur_value is None or v != cur_value:
            if cur_value is not None:
                page_rows.append({'value': cur_value, 'cum': cur_count,
                                  'fraction': cur_count / n})
                if len(page_rows) >= layout.ECDF_PAGE_ROWS:
                    flush_ecdf_page()
            cur_value, cur_count, unique = v, rank + 1, unique + 1
        else:
            cur_count = rank + 1
        prev = v
        rank += 1
    # 末值入页
    if cur_value is not None:
        page_rows.append({'value': cur_value, 'cum': cur_count,
                          'fraction': cur_count / n})
        flush_ecdf_page()

    if rank != n:
        from .inputs import fail
        fail(root, m, f'终遍计数 {rank} 与段行数 {n} 不一致')
    if set(captured) != need_ranks:
        from .inputs import fail
        fail(root, m, '分位点采集不完整')

    quantiles = _linear_quantile(n, captured)
    # Neumaier 求和：返回值为 total+comp。
    mean = (total + comp) / n
    # 与小批研究 np.std(ddof=1) 同口径：样本标准差。
    ss = (sumsq + comp2) - n * mean * mean
    var = ss / (n - 1) if n > 1 else 0.0
    if -1e-9 < var < 0:
        var = 0.0
    if var < 0:
        from .inputs import fail
        fail(root, m, '方差为负且超出数值容差，核验失败')
    std = math.sqrt(var)
    iqr = quantiles['P75'] - quantiles['P25']

    m['finalize'] = {
        'n': n, 'quantiles': quantiles, 'mean': mean,
        'std': std, 'min': quantiles['最小值'], 'max': quantiles['最大值'],
        'iqr': iqr, 'unique': unique,
        'ecdf_pages': pages,
        'neumaier_sum': total + comp,
    }
    m['ecdf'] = {'pages': len(pages), 'unique': unique, 'rows': unique}
    m['progress'] = {'phase': 'finalizing', 'done': n, 'total': m['input_total'],
                     'detail': '终遍完成，等待发布前独立核验'}
    save_manifest(root, m)
    return m


def verify(root, m: dict) -> dict:
    """发布前独立核验：不引用终遍结果，重新流式计算并逐项比对。"""
    if 'finalize' not in m:
        raise LargeRunError('终遍结果缺失，不能核验')
    level, meta = _final_segment(root, m)
    n = meta['rows']
    rank_targets = _quantile_ranks(n)
    need_ranks = set(rank_targets)
    captured = {}
    order_ok = True
    prev = None
    rank = 0
    check_total = 0.0
    check_comp = 0.0
    unique = 0
    last_cum = 0
    cur_value = None
    for v in iter_segment(root, level, meta):  # 同时重算段 SHA256
        if prev is not None and v < prev:
            order_ok = False
        if rank in need_ranks:
            captured[rank] = v
        t = check_total + v
        check_comp += ((check_total - t) + v) if abs(check_total) >= abs(v) else ((v - t) + check_total)
        check_total = t
        if cur_value is None or v != cur_value:
            cur_value, unique = v, unique + 1
        prev = v
        rank += 1
        last_cum = rank

    f = m['finalize']
    checks = []

    def check(name, ok, detail=''):
        checks.append({'name': name, 'ok': bool(ok), 'detail': detail})
        if not ok:
            raise LargeRunError(f'发布核验失败：{name} {detail}')

    # 1) 输入数 = 段累计数 = 批次累计数
    batch_total = sum(b['rows'] for b in m['batches'])
    seg_total = sum(s['rows'] for s in m['segments'])
    check('输入数/批次累计/分段累计一致',
          n == rank == m['input_total'] == batch_total == seg_total,
          f'{n}/{rank}/{m["input_total"]}/{batch_total}/{seg_total}')
    check('有序性', order_ok)
    # 2) 分位点与终遍完全一致（独立重算）
    q2 = _linear_quantile(n, captured)
    check('分位点一致', q2 == f['quantiles'], f'{q2} != {f["quantiles"]}')
    check('均值一致', (check_total + check_comp) == f['neumaier_sum'])
    # 3) ECDF：唯一点数、页数、末点累计=n=分母，逐页校验和
    check('ECDF 唯一点数一致', unique == f['unique'] == m['ecdf']['unique'])
    check('ECDF 末点累计等于样本量', last_cum == n)
    page_rows_total = 0
    for p in m['finalize']['ecdf_pages']:
        path = ecdf_page_path(root, p['page'])
        check(f'ECDF 第{p["page"]}页存在', path.exists())
        check(f'ECDF 第{p["page"]}页校验和', file_sha256(path) == p['sha256'])
        page_rows_total += p['rows']
    check('ECDF 页面行数合计等于唯一点数', page_rows_total == unique)
    check('ECDF 页数登记一致', len(m['finalize']['ecdf_pages']) == m['ecdf']['pages'])
    # 4) 输入链仍与冻结时一致（seal 已核，这里再核一次链式哈希）
    chain = ''
    for b in sorted(m['batches'], key=lambda x: x['no']):
        chain = hashlib.sha256(
            (chain + b['sha256']).encode('utf-8')).hexdigest()
    check('输入链哈希一致', chain == m['input_chain'])

    m['stage'] = 'verified'
    m['verification'] = {'checks': checks, 'all_ok': True}
    m['progress'] = {'phase': 'verified', 'done': n, 'total': m['input_total'],
                     'detail': f'核验通过（{len(checks)} 项），结果可发布'}
    save_manifest(root, m)
    return m


def read_ecdf_page(root, m: dict, page_no: int):
    """分页读取完整 ECDF，单次只装载一页，不触碰全量结果。"""
    if m['stage'] != 'verified':
        raise LargeRunError('结果尚未核验，ECDF 不可见')
    pages = m['finalize']['ecdf_pages']
    if not (0 <= page_no < len(pages)):
        raise LargeRunError('ECDF 页码超出范围')
    rec = pages[page_no]
    path = ecdf_page_path(root, page_no)
    if not path.exists() or file_sha256(path) != rec['sha256']:
        raise LargeRunError('ECDF 页面损坏，停止读取')
    rows = [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines() if line]
    return {'page': page_no, 'pages': len(pages),
            'unique': m['ecdf']['unique'], 'n': m['input_total'], 'rows': rows}
