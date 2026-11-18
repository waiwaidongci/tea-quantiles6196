"""分段外部排序：流式读取冻结输入，按字节预算分块排序落盘。

内存约束：任一时刻内存中只有一个不超过 SEGMENT_BYTES 的 float64 数组；
已落妥且校验通过的段在恢复时不重做。段文件带 SHA256 与输入链标记，
损坏的派生段只允许从同一冻结输入确定性重生成，绝不接受其他输入的段。
"""
import hashlib, os
import numpy as np
from . import layout
from .layout import (LargeRunError, segment_path, segment_meta_path,
                     write_json_atomic, read_json, file_sha256)
from .inputs import iter_batch_values, save_manifest

DT = np.dtype('<f8')


def _write_segment(root, level: int, seq: int, arr: np.ndarray, input_chain: str) -> dict:
    path = segment_path(root, level, seq)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.bin.tmp')
    h = hashlib.sha256()
    with open(tmp, 'wb') as f:
        block = arr.astype(DT, copy=False).tobytes()
        f.write(block)
        h.update(block)
    digest = h.hexdigest()
    os.replace(tmp, path)
    meta = {'seq': seq, 'level': level, 'rows': int(arr.size),
            'sha256': digest, 'input_chain': input_chain}
    write_json_atomic(segment_meta_path(root, level, seq), meta)
    return meta


def _read_segment_meta(root, level: int, seq: int):
    p = segment_meta_path(root, level, seq)
    if not p.exists():
        return None
    return read_json(p)


def _segment_ok(root, level: int, meta: dict) -> bool:
    p = segment_path(root, level, meta['seq'])
    if not p.exists() or file_sha256(p) != meta['sha256']:
        return False
    if meta.get('input_chain') != _manifest_chain(root):
        return False
    return True


def _manifest_chain(root) -> str:
    return read_json(root / layout.MANIFEST)['input_chain']


def spill_segments(root, m: dict) -> dict:
    """把冻结输入切成有序 level-0 段；中断后继续时跳过已落妥段。"""
    if m['stage'] not in ('sorting', 'merging'):
        if m['stage'] in ('finalizing', 'verified'):
            return m
        raise LargeRunError(f'当前阶段 {m["stage"]} 不能执行分段排序')

    budget = layout.SEGMENT_BYTES
    capacity = max(1, budget // 8)
    existing = list(m.get('segments', []))

    # 恢复：逐个核验已落段；发现坏段则从该段起重生成（边界由字节预算确定，可精确复现）。
    verified_rows = 0
    good_seqs = set()
    first_bad = None
    for meta in existing:
        if _segment_ok(root, 0, meta):
            good_seqs.add(meta['seq'])
        elif first_bad is None:
            first_bad = meta['seq']
    if first_bad is not None:
        # 已进入归并阶段后发现 level-0 坏段：回退到排序阶段，并清理全部下游归并产物，
        # 因为它们可能基于坏段生成。level-0 已落妥前缀保留、不重做。
        m['stage'] = 'sorting'
        m['levels'] = {}
        m.pop('final_level', None)
        m.pop('finalize', None)
        m.pop('verification', None)
        # 清除坏段及其后全部 level-0 产物，保留已落妥的前缀。
        keep = {s for s in good_seqs if s < first_bad}
        for meta in existing:
            if meta['seq'] not in keep:
                for p in (segment_path(root, 0, meta['seq']),
                          segment_meta_path(root, 0, meta['seq'])):
                    p.unlink(missing_ok=True)
        # 清理下游层级文件。
        import shutil
        for d in root.glob('level-*'):
            if d.name != 'level-0':
                shutil.rmtree(d, ignore_errors=True)
        existing = [meta for meta in existing if meta['seq'] in keep]
        verified_rows = sum(meta['rows'] for meta in existing)
        m['segments'] = existing
        save_manifest(root, m)
        good_seqs = keep

    next_seq = (max(good_seqs) + 1) if good_seqs else 0
    skipped = verified_rows
    buf = np.empty(capacity, dtype=DT)
    used = 0
    peak = m.get('peak_bytes_seen', 0)

    def flush():
        nonlocal buf, used, next_seq, peak
        if used == 0:
            return
        arr = np.sort(buf[:used], kind='mergesort')
        meta = _write_segment(root, 0, next_seq, arr, m['input_chain'])
        # 恢复场景下用新 meta 替换同 seq 的旧（已删除/损坏）记录，再排序去重。
        existing[:] = [x for x in existing if x['seq'] != meta['seq']]
        existing.append(meta)
        m['segments'] = sorted(existing, key=lambda x: x['seq'])
        next_seq += 1
        used = 0
        buf = np.empty(capacity, dtype=DT)
        m['progress'] = {'phase': 'sorting', 'done': skipped + _rows_so_far(m),
                         'total': m['input_total'],
                         'detail': f'分段排序：已落 {len(m["segments"])} 段'}
        save_manifest(root, m)

    # 按全局索引切分，保证中断恢复时段边界与首次完全一致（确定性重生成）。
    stream = iter_batch_values(root, m)
    for idx, v in enumerate(stream):
        if idx < skipped:
            continue  # 已落妥前缀不重做，但其全局位置仍由 idx 保持
        buf[used] = v
        used += 1
        peak = max(peak, used * 8)
        # 段边界由全局容量决定：每 capacity 个输入值一段。
        if (idx + 1) % capacity == 0:
            flush()
    flush()
    m['peak_bytes_seen'] = peak

    total_rows = sum(s['rows'] for s in m['segments'])
    if total_rows != m['input_total']:
        from .inputs import fail
        fail(root, m, f'分段累计数 {total_rows} 与输入数 {m["input_total"]} 不一致')
    # 段必须连续编号。
    seqs = sorted(s['seq'] for s in m['segments'])
    if seqs != list(range(len(seqs))):
        from .inputs import fail
        fail(root, m, '排序段编号不连续，拒绝继续')
    m['stage'] = 'merging'
    m['progress'] = {'phase': 'merging', 'done': 0, 'total': m['input_total'],
                     'detail': f'分段完成（{len(seqs)} 段），开始流式归并'}
    save_manifest(root, m)
    return m


def _rows_so_far(m: dict) -> int:
    return sum(s['rows'] for s in m.get('segments', []))


def iter_segment(root, level: int, meta: dict):
    """以有界缓冲流式读取一个二进制段（float64 小端），并核验校验和。"""
    path = segment_path(root, level, meta['seq'])
    if not path.exists():
        raise LargeRunError(f'归并输入段 L{level}/#{meta["seq"]} 缺失，拒绝用其他片段拼用')
    h = hashlib.sha256()
    expected_rows = 0
    with open(path, 'rb') as f:
        while True:
            block = f.read(layout.READER_BYTES)
            if not block:
                break
            h.update(block)
            if len(block) % 8 != 0:
                raise LargeRunError(f'段 L{level}/#{meta["seq"]} 字节损坏')
            arr = np.frombuffer(block, dtype=DT)
            expected_rows += arr.size
            for v in arr.tolist():
                yield v
    if h.hexdigest() != meta['sha256']:
        raise LargeRunError(f'段 L{level}/#{meta["seq"]} 校验和不一致，片段损坏，停止运行')
    if expected_rows != meta['rows']:
        raise LargeRunError(f'段 L{level}/#{meta["seq"]} 行数与元数据不符')
