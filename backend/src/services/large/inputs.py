"""输入分批接收与冻结。

- 每批观测在写入前逐行校验（字段完整、有限业务数值），损坏即拒绝整批。
- 批次以 JSONL 原子落盘，冻结后永不改写；每批带 SHA256，并形成链式哈希。
- 批次号由服务端单调分配，重复提交相同批次号按幂等处理，内容不同则硬失败。
- 输入冻结（seal）后再发批次一律拒绝；恢复时重新核验已落批次的校验和。
"""
import json, math, os
from . import layout
from .layout import LargeRunError, write_json_atomic, read_json, file_sha256, sha256_text, batch_path

VALID_FIELDS = ('moisture', 'extract')


def new_manifest(run_id: int, field: str, expected_count) -> dict:
    return {
        'run_id': run_id,
        'version': 1,
        'field': field,
        'expected_count': int(expected_count) if expected_count is not None else None,
        'stage': 'ingesting',            # ingesting -> sorting -> merging -> finalizing -> verified
        'failed': False,
        'failure': '',
        'input_chain': '',               # 空输入链的初始值
        'input_total': 0,
        'batches': [],                   # [{'no','rows','sha256','chain_after'}]
        'sealed': False,
        'segments': [],                  # level0 段 [{'seq','rows','sha256','input_chain'}]
        'levels': {},                    # level -> [{'seq','rows','sha256'}]
        'ecdf': {'pages': 0, 'unique': 0, 'rows': 0},
        'progress': {'phase': 'ingesting', 'done': 0,
                     'total': int(expected_count) if expected_count is not None else 0,
                     'detail': '等待分批接收观测'},
        'memory_budget_bytes': max(layout.SEGMENT_BYTES, layout.MERGE_FANOUT * layout.READER_BYTES * 4),
        'peak_bytes_seen': 0,
    }


def load_manifest(root) -> dict:
    path = root / layout.MANIFEST
    if not path.exists():
        raise LargeRunError('运行清单缺失，无法恢复')
    try:
        m = read_json(path)
    except (json.JSONDecodeError, OSError) as e:
        raise LargeRunError(f'运行清单损坏：{e}')
    if m.get('failed'):
        raise LargeRunError(m.get('failure') or '运行此前已失败')
    return m


def save_manifest(root, m: dict) -> None:
    write_json_atomic(root / layout.MANIFEST, m)


def fail(root, m: dict, message: str) -> None:
    m['failed'] = True
    m['failure'] = message
    m['progress']['detail'] = '已停止：' + message
    try:
        save_manifest(root, m)
    except OSError:
        pass
    raise LargeRunError(message)


def validate_row(row, field: str):
    if not isinstance(row, dict) or 'source_key' not in row or 'values' not in row:
        raise LargeRunError('批次观测必须含 source_key 与 values')
    if not isinstance(row['source_key'], str) or not row['source_key']:
        raise LargeRunError('source_key 必须为非空字符串')
    values = row['values']
    if not isinstance(values, dict) or field not in values:
        raise LargeRunError(f'观测字段不完整，缺少 {field}')
    if isinstance(values[field], bool):
        raise LargeRunError('观测必须为数值')
    try:
        v = float(values[field])
    except (TypeError, ValueError):
        raise LargeRunError('观测必须为数值')
    if not math.isfinite(v) or v < 0 or v > 100:
        raise LargeRunError('观测必须为 0–100 之间的有限数值')
    return {'source_key': row['source_key'], 'value': v}


def append_batch(root, m: dict, rows, batch_no: int = None) -> dict:
    """接收一批观测并冻结落盘。返回更新后的清单（同进程内对象被原地修改）。"""
    if m['sealed']:
        raise LargeRunError('输入已冻结，拒绝追加批次')
    if m['stage'] != 'ingesting':
        raise LargeRunError('运行已进入计算阶段，拒绝追加批次')
    if not isinstance(rows, list) or not rows:
        raise LargeRunError('批次必须为非空观测数组')
    clean = [validate_row(r, m['field']) for r in rows]

    existing_nos = {b['no'] for b in m['batches']}
    if batch_no is None:
        batch_no = (max(existing_nos) + 1) if existing_nos else 1
    if batch_no in existing_nos:
        # 幂等：同号重传必须与已冻结内容一致，否则属于输入冲突，拒绝拼用。
        rec = next(b for b in m['batches'] if b['no'] == batch_no)
        if rec['sha256'] == sha256_text(_serialize(clean)):
            return m
        raise LargeRunError(f'批次 {batch_no} 已冻结为不同内容，拒绝拼用冲突输入')
    if batch_no < 1:
        raise LargeRunError('批次号必须为正整数')
    expected_next = (max(existing_nos) + 1) if existing_nos else 1
    if batch_no != expected_next:
        raise LargeRunError(f'批次号必须连续，下一批应为 {expected_next}')

    text = _serialize(clean)
    path = batch_path(root, batch_no)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.jsonl.tmp')
    tmp.write_text(text, encoding='utf-8')
    digest = file_sha256(tmp)
    if digest != sha256_text(text):  # 防御：落盘前后一致
        tmp.unlink(missing_ok=True)
        raise LargeRunError('批次落盘校验失败')
    os.replace(tmp, path)

    chain = sha256_text((m['input_chain'] or '') + digest)
    rec = {'no': batch_no, 'rows': len(clean), 'sha256': digest, 'chain_after': chain}
    m['batches'].append(rec)
    m['batches'].sort(key=lambda b: b['no'])
    m['input_chain'] = chain
    m['input_total'] += len(clean)
    m['progress'] = {'phase': 'ingesting', 'done': m['input_total'],
                     'total': m['expected_count'] or 0,
                     'detail': f'已冻结 {len(m["batches"])} 批 / {m["input_total"]} 条'}
    save_manifest(root, m)
    return m


def _serialize(clean) -> str:
    # 规范化 JSONL，allow_nan=False 确保 NaN/Inf 不会污染冻结输入。
    return ''.join(json.dumps(x, sort_keys=True, ensure_ascii=False, allow_nan=False) + '\n'
                   for x in clean)


def seal(root, m: dict) -> dict:
    if m['sealed']:
        return m
    if not m['batches']:
        raise LargeRunError('没有任何已接收批次，无法冻结')
    # 恢复式核验：重新读取每个冻结批次并比对校验和与链式哈希。
    chain = ''
    total = 0
    for rec in sorted(m['batches'], key=lambda b: b['no']):
        p = batch_path(root, rec['no'])
        if not p.exists():
            fail(root, m, f'冻结批次 {rec["no"]} 文件缺失，输入不完整')
        digest = file_sha256(p)
        if digest != rec['sha256']:
            fail(root, m, f'冻结批次 {rec["no"]} 校验和不一致，输入已损坏，停止运行')
        chain = sha256_text(chain + digest)
        if chain != rec['chain_after']:
            fail(root, m, f'批次 {rec["no"]} 链式哈希断裂，输入链损坏')
        total += rec['rows']
    if chain != m['input_chain'] or total != m['input_total']:
        fail(root, m, '输入链或累计数核验失败，停止运行')
    if m['expected_count'] is not None and total != m['expected_count']:
        fail(root, m, f'输入数 {total} 与声明观测数 {m["expected_count"]} 不一致')
    m['sealed'] = True
    m['stage'] = 'sorting'
    m['progress'] = {'phase': 'sorting', 'done': 0, 'total': total,
                     'detail': '输入已冻结，开始分段排序'}
    save_manifest(root, m)
    return m


def iter_batch_values(root, m: dict):
    """流式读取全部冻结批次（仅在排序阶段使用，单批读取、不装载全量）。"""
    field = m['field']
    for rec in sorted(m['batches'], key=lambda b: b['no']):
        p = batch_path(root, rec['no'])
        if not p.exists():
            fail(root, m, f'批次 {rec["no"]} 文件缺失')
        digest = file_sha256(p)
        if digest != rec['sha256']:
            fail(root, m, f'批次 {rec["no"]} 校验失败，输入损坏，停止运行')
        n = 0
        with open(p, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    fail(root, m, f'批次 {rec["no"]} 含空行，输入损坏')
                try:
                    obj = json.loads(line)
                    v = float(obj['value'])
                except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                    fail(root, m, f'批次 {rec["no"]} 存在无法解析的观测，输入损坏')
                if not math.isfinite(v):
                    fail(root, m, f'批次 {rec["no"]} 含非有限值，输入损坏')
                n += 1
                yield v
        if n != rec['rows']:
            fail(root, m, f'批次 {rec["no"]} 行数 {n} 与冻结记录 {rec["rows"]} 不符')
