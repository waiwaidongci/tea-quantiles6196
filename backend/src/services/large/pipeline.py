"""大批量流水线编排：分阶段执行，任何硬错误都让运行永久失败并落盘原因。"""
import logging
from . import layout
from .layout import LargeRunError
from .inputs import load_manifest, save_manifest, fail, seal
from .sorting import spill_segments
from .merging import merge_levels, finalize, verify


def run_pipeline(root, until: str = 'verified') -> dict:
    """按清单阶段继续推进；可恢复，已完成阶段/片段不重做。"""
    m = load_manifest(root)
    try:
        if not m['sealed']:
            seal(root, m)
        if m['stage'] == 'sorting':
            spill_segments(root, m)
        if m['stage'] == 'merging':
            merge_levels(root, m)
        if m['stage'] == 'finalizing':
            finalize(root, m)
        if m['stage'] == 'finalizing' and until == 'verified':
            verify(root, m)
        if m['stage'] != 'verified' and until == 'verified':
            raise LargeRunError(f'流水线停在阶段 {m["stage"]}')
    except LargeRunError as e:
        fail(root, m, str(e))
        raise
    except Exception as e:  # 意外异常同样冻结为失败态，避免半成品可见
        logging.exception('large pipeline unexpected error')
        fail(root, m, f'内部错误：{e}')
        raise
    return m


def progress(root) -> dict:
    try:
        m = load_manifest(root)
    except LargeRunError as e:
        return {'phase': 'failed', 'done': 0, 'total': 0, 'detail': str(e),
                'failed': True, 'failure': str(e)}
    p = dict(m.get('progress', {}))
    p['failed'] = m.get('failed', False)
    p['failure'] = m.get('failure', '')
    p['stage'] = m.get('stage')
    p['sealed'] = m.get('sealed', False)
    p['batches'] = len(m.get('batches', []))
    p['segments'] = len(m.get('segments', []))
    p['peak_bytes_seen'] = m.get('peak_bytes_seen', 0)
    p['memory_budget_bytes'] = m.get('memory_budget_bytes', 0)
    return p
