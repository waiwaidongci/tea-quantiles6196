"""大批量分布研究 · 磁盘存储布局。

所有大批量中间产物都在独立的运行目录内，目录名含 run id 与输入冻结校验和；
不同运行（不同输入）的片段物理隔离，任何片段都带 input_chain 与文件 SHA256，
归并/恢复时只接受与本运行输入链一致的片段，拒绝拼用其他输入的片段。
"""
import hashlib, json, os
from pathlib import Path

ARTIFACT_ROOT = Path(os.environ.get('LARGE_ARTIFACT_DIR', '/app/artifacts/large'))
# 单个内存排序段的目标字节预算（8字节/值的 float64 计入）。
SEGMENT_BYTES = int(os.environ.get('LARGE_SEGMENT_BYTES', 8 * 1024 * 1024))
# 归并时每路读取缓冲；ECDF 页面缓冲同样有界。
READER_BYTES = 256 * 1024
ECDF_PAGE_ROWS = int(os.environ.get('LARGE_ECDF_PAGE_ROWS', 500))
# 归并层级的扇出。
MERGE_FANOUT = int(os.environ.get('LARGE_MERGE_FANOUT', 8))

MANIFEST = 'manifest.json'


class LargeRunError(Exception):
    """输入损坏/片段损坏/核验失败等硬错误：运行必须停止。"""


def run_dir(run_id: int, input_chain: str) -> Path:
    safe = ''.join(c for c in (input_chain or '') if c.isalnum() or c in '-_')[:16]
    return ARTIFACT_ROOT / f'run-{run_id}-{safe}'


def batch_path(root: Path, batch_no: int) -> Path:
    return root / 'input' / f'batch-{batch_no:06d}.jsonl'


def segment_path(root: Path, level: int, seq: int) -> Path:
    return root / f'level-{level}' / f'seg-{seq:06d}.bin'


def segment_meta_path(root: Path, level: int, seq: int) -> Path:
    return root / f'level-{level}' / f'seg-{seq:06d}.meta.json'


def ecdf_page_path(root: Path, page_no: int) -> Path:
    return root / 'ecdf' / f'page-{page_no:06d}.jsonl'


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def write_json_atomic(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, allow_nan=False), encoding='utf-8')
    os.replace(tmp, path)


def read_json(path: Path):
    return json.loads(path.read_text(encoding='utf-8'))
