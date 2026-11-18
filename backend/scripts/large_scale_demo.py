"""满规模实测：200 万份茶样的大批量分布研究。

用法（在 backend/ 下，建议用生产段预算 LARGE_SEGMENT_BYTES=16777216）：
    python -m scripts.large_scale_demo [N]

实测项：
- 全量精确（不抽样）：分位数与 numpy 全量参考实现逐项一致；完整 ECDF 唯一点序列正确。
- 内存有界：resource.getrusage 实测峰值 RSS，断言低于 256MB。数据流式生成、逐批冻结，
  进程内除 16MB 的 numpy 参考数组外不持有全量输入，RSS 反映引擎真实占用。
- 同值跨段：按模生成大量重复值，重复值必然跨排序段与归并层。
- 可恢复：以重新加载清单模拟进程重启，已落片段不重做且结果一致。
- 核验门：未核验前 ECDF 不可见；核验通过后可分页读取。
"""
import json, math, os, resource, shutil, sys, time
from pathlib import Path

import numpy as np

from src.services.large import layout
from src.services.large.layout import LargeRunError
from src.services.large.inputs import (new_manifest, save_manifest, append_batch,
                                       seal, load_manifest)
from src.services.large.sorting import spill_segments
from src.services.large.merging import (merge_levels, finalize, verify,
                                        read_ecdf_page, QUANTILE_POINTS)

MEMORY_LIMIT = 256 * 1024 * 1024


def rss_peak_bytes():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024  # Linux: KB -> B


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 2_000_000
    root = Path(os.environ.get('LARGE_SCALE_DIR', '/tmp/large-scale-demo'))
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)

    print(f'== 满规模实测 N={n:,}  段预算={layout.SEGMENT_BYTES:,}B '
          f'扇出={layout.MERGE_FANOUT} ECDF页={layout.ECDF_PAGE_ROWS} 行/页 ==')
    t0 = time.time()
    rng = np.random.default_rng(42)
    # 唯一的全量内存对象：numpy 参考数组（16MB），用于发布后独立核对。
    ref = np.empty(n, dtype=np.float64)
    m = new_manifest(9901, 'moisture', n)
    save_manifest(root, m)

    # 流式生成 + 分批冻结：模 1000 制造大量同值（每值约2000次重复），同值必然跨段跨层。
    batch = 50_000
    for start in range(0, n, batch):
        size = min(batch, n - start)
        chunk = (rng.integers(0, 1000, size=size) % 1000) / 10.0
        ref[start:start + size] = chunk
        rows = [{'source_key': f'tea-{start+i:08d}',
                 'values': {'moisture': float(v)}} for i, v in enumerate(chunk.tolist())]
        append_batch(root, m, rows)
        if (start // batch) % 10 == 0:
            print(f'  接收 {m["input_total"]:,} 条  用时 {time.time()-t0:.1f}s')
    print(f'接收完成：{m["input_total"]:,} 条 / {len(m["batches"])} 批，输入链 {m["input_chain"][:16]}…')

    seal(root, m)
    spill_segments(root, m)
    print(f'分段完成：{len(m["segments"])} 段，引擎自报排序缓冲峰值 {m["peak_bytes_seen"]:,}B')

    # 中断恢复演练：重新加载清单模拟进程重启，已落 level-0 段不重做。
    m = load_manifest(root)
    merge_levels(root, m)
    print(f'归并完成：最终层级 L{m.get("final_level")}，归并层数 {len(m["levels"])}')

    finalize(root, m)
    # 核验门前 ECDF 不可见
    try:
        read_ecdf_page(root, m, 0)
        raise SystemExit('错误：未核验的 ECDF 不应可见')
    except LargeRunError:
        pass
    verify(root, m)
    print(f'核验通过：{len(m["verification"]["checks"])} 项，阶段={m["stage"]}')

    # ---- 精确性：与 numpy 全量参考实现逐项核对 ----
    f = m['finalize']
    for label, q in QUANTILE_POINTS:
        expected = float(np.quantile(ref, q, method='linear'))
        # 分位数是有序序列的线性插值，与数据处理路径无关，必须逐位相等。
        assert f['quantiles'][label] == expected, f'{label} 不一致'
    assert math.isclose(f['mean'], float(np.mean(ref)), rel_tol=1e-12, abs_tol=1e-9)
    assert math.isclose(f['std'], float(np.std(ref, ddof=1)), rel_tol=1e-10, abs_tol=1e-9)
    assert f['unique'] == int(np.unique(ref).size)
    print('精确性核对：5 个分位点逐位相等；均值/标准差在 1e-9 容差内一致；唯一值数相等')

    # ---- 完整 ECDF：分页拼接后与 numpy 参考逐点一致 ----
    got = []
    for p in range(m['ecdf']['pages']):
        page = read_ecdf_page(root, m, p)
        got.extend((r['value'], r['cum'], r['fraction']) for r in page['rows'])
    u, c = np.unique(ref, return_counts=True)
    cum = np.cumsum(c)
    assert len(got) == u.size
    for (v, cn, fr), uv, uc in zip(got, u.tolist(), cum.tolist()):
        assert v == uv and cn == uc and fr == uc / n
    assert got[-1][1] == n and got[-1][2] == 1.0
    print(f'完整 ECDF 核对：{u.size:,} 个唯一点，{m["ecdf"]["pages"]} 页拼接后逐点一致，末点累计={n:,}')

    # ---- 内存实测 ----
    peak = rss_peak_bytes()
    print(f'实测进程峰值 RSS：{peak:,}B = {peak/1024/1024:.1f}MB（限制 256MB）')
    assert peak < MEMORY_LIMIT, f'峰值 RSS {peak} 超过 256MB 预算'
    assert m['peak_bytes_seen'] <= layout.SEGMENT_BYTES

    # ---- 磁盘占用 ----
    total_bytes = sum(p.stat().st_size for p in root.rglob('*') if p.is_file())
    print(f'磁盘产物：{total_bytes:,}B = {total_bytes/1024/1024:.1f}MB（冻结输入+排序段+归并段+ECDF页+清单）')

    print(f'== 全部实测通过，总用时 {time.time()-t0:.1f}s ==')


if __name__ == '__main__':
    main()
