"""测试夹具：隔离的磁盘目录与受限内存预算。

关键：小内存预算的环境变量必须在 src.services.large 任何模块导入前设置
（layout 在导入时读取常量，其他模块以名字绑定这些常量）。
因此本文件顶部直接设置环境变量，测试模块只从单一的已配置副本导入。
"""
import os
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

# 每段约128个float，确保小例也跨多段、多层归并；读取缓冲同样压小。
os.environ['LARGE_SEGMENT_BYTES'] = '1024'
os.environ['LARGE_MERGE_FANOUT'] = '3'
os.environ['LARGE_ECDF_PAGE_ROWS'] = '4'
os.environ['LARGE_READER_BYTES'] = '256'
_DEFAULT_ART = BACKEND / 'tests' / '.tmp-artifacts'
_DEFAULT_ART.mkdir(parents=True, exist_ok=True)
os.environ.setdefault('LARGE_ARTIFACT_DIR', str(_DEFAULT_ART))


@pytest.fixture()
def tiny_env(tmp_path):
    # 只切换产物根目录；段/页大小常量在首次导入时已按小预算固定。
    from src.services.large import layout
    saved = layout.ARTIFACT_ROOT
    layout.ARTIFACT_ROOT = tmp_path / 'artifacts'
    try:
        root = tmp_path / 'run-root'
        root.mkdir()
        yield tmp_path, root
    finally:
        layout.ARTIFACT_ROOT = saved
