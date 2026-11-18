"""中断恢复实测：在分段/归并中途杀掉进程，重启后已落片段不重做、结果一致。

两个检查点：
1. 杀掉后重启：清单阶段回退/续跑正确，最终核验通过；
2. 已落段文件在重启后内容不变（确定性恢复，不重写已落妥片段）。
"""
import math, os, shutil, subprocess, sys, time
from pathlib import Path

import numpy as np

ROOT = Path('/tmp/large-restart-demo')
ENV = dict(os.environ,
           LARGE_SEGMENT_BYTES='2097152', LARGE_MERGE_FANOUT='8',
           LARGE_READER_BYTES='262144', LARGE_ECDF_PAGE_ROWS='500',
           LARGE_ARTIFACT_DIR=str(ROOT / 'art'),
           PYTHONPATH='/workspace/backend')
N = 500_000

WORKER = r'''
import os,time
from pathlib import Path
from src.services.large.inputs import new_manifest,save_manifest,append_batch,seal,load_manifest
from src.services.large.sorting import spill_segments
from src.services.large.merging import merge_levels,finalize,verify
import numpy as np
root=Path(os.environ['DEMO_ROOT'])
rng=np.random.default_rng(42); n=int(os.environ['DEMO_N'])
m=new_manifest(7717,'moisture',n); save_manifest(root,m)
b=50000
for s in range(0,n,b):
    size=min(b,n-s); chunk=(rng.integers(0,1000,size=size)%1000)/10.0
    append_batch(root,m,[{'source_key':f'tea-{s+i:08d}','values':{'moisture':float(v)}} for i,v in enumerate(chunk.tolist())])
seal(root,m)
spill_segments(root,m)
# 检查点 A：分段完成后停住，等待被 kill
Path(os.environ['DEMO_ROOT'],'checkpoint-a').write_text(str(len(m['segments'])))
time.sleep(6)
merge_levels(root, load_manifest(root))
finalize(root, load_manifest(root))
verify(root, load_manifest(root))
Path(os.environ['DEMO_ROOT'],'done').write_text('ok')
'''


def run(cmd, **kw):
    return subprocess.run(cmd, env=dict(ENV, DEMO_ROOT=str(ROOT), DEMO_N=str(N)),
                          capture_output=True, text=True, cwd='/workspace/backend', **kw)


def main():
    if ROOT.exists():
        shutil.rmtree(ROOT)
    ROOT.mkdir(parents=True)
    env = dict(ENV, DEMO_ROOT=str(ROOT), DEMO_N=str(N))
    script = ROOT / 'worker.py'
    script.write_text(WORKER)

    proc = subprocess.Popen([sys.executable, str(script)], env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd='/workspace/backend')
    # 等分段检查点
    for _ in range(100):
        if (ROOT / 'checkpoint-a').exists():
            break
        time.sleep(0.2)
    assert (ROOT / 'checkpoint-a').exists(), '分段检查点未出现'
    nseg = int((ROOT / 'checkpoint-a').read_text())
    print(f'检查点 A：已落 {nseg} 个排序段，进程在归并前被 kill')

    # 记录已落段的 mtime/sha，恢复后必须保持不变
    from src.services.large.layout import segment_path, file_sha256
    before = {}
    for seq in range(nseg):
        p = segment_path(ROOT, 0, seq)
        before[seq] = (p.stat().st_mtime_ns, file_sha256(p))

    proc.kill()
    proc.wait()
    assert not (ROOT / 'done').exists(), '进程应在完成前被杀'

    # 重启：从清单恢复，直接推进到核验
    RESUME = r'''
import os
from pathlib import Path
from src.services.large.pipeline import run_pipeline
m=run_pipeline(Path(os.environ['DEMO_ROOT']))
assert m['stage']=='verified'
print('RESUME_OK', m['input_total'], len(m['segments']), m['ecdf']['unique'])
'''
    rscript = ROOT / 'resume.py'
    rscript.write_text(RESUME)
    out = run([sys.executable, str(rscript)])
    assert 'RESUME_OK' in out.stdout, out.stderr
    print('恢复进程输出：', out.stdout.strip().splitlines()[-1])

    # 已落 level-0 段未被重做
    for seq, (mt, sha) in before.items():
        p = segment_path(ROOT, 0, seq)
        assert p.stat().st_mtime_ns == mt and file_sha256(p) == sha, f'段 {seq} 被重做'
    print('已落妥的', nseg, '个排序段全部保持原样（mtime 与 SHA256 不变），未重做')

    # 结果与独立 oracle 一致
    from src.services.large.inputs import load_manifest
    from src.services.large.merging import read_ecdf_page
    m = load_manifest(ROOT)
    rng = np.random.default_rng(42)
    ref = (rng.integers(0, 1000, size=N) % 1000) / 10.0
    f = m['finalize']
    for label, q in [('最小值', 0), ('P25', .25), ('中位数', .5), ('P75', .75), ('最大值', 1)]:
        assert f['quantiles'][label] == float(np.quantile(ref, q, method='linear')), label
    got = []
    for p in range(m['ecdf']['pages']):
        got.extend(r['value'] for r in read_ecdf_page(ROOT, m, p)['rows'])
    assert got == np.unique(ref).tolist()
    print('恢复后结果与 numpy 全量参考一致（5 分位点逐位相等，ECDF 唯一点序列一致）')
    print('== 中断恢复实测通过 ==')


if __name__ == '__main__':
    main()
