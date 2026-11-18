"""大批量引擎测试：同值跨段、可恢复、损坏停止、拒绝异输入片段、分页ECDF、oracle核对。"""
import json,math,os
from src.services.large import layout
from src.services.large.layout import LargeRunError
from src.services.large.inputs import (new_manifest,append_batch,seal,
                                       load_manifest,save_manifest)
from src.services.large.sorting import spill_segments
from src.services.large.merging import (merge_levels,finalize,verify,
                                        read_ecdf_page,QUANTILE_POINTS)


def oracle_quantile(xs,q):
    xs=sorted(xs)
    h=(len(xs)-1)*q;i=math.floor(h);j=math.ceil(h)
    return xs[i]+(xs[j]-xs[i])*(h-i)


def oracle_ecdf(xs):
    xs=sorted(xs);n=len(xs);out=[];i=0
    while i<n:
        j=i+1
        while j<n and xs[j]==xs[i]: j+=1
        out.append((xs[i],j/n));i=j
    return out


def make_rows(values,field='moisture'):
    return [{'source_key':f'k{i:05d}','values':{field:float(v)}} for i,v in enumerate(values)]


def run_all(root,m):
    seal(root,m)
    spill_segments(root,m)
    merge_levels(root,m)
    finalize(root,m)
    verify(root,m)
    return m


def test_tie_values_across_segments_and_layers(tiny_env):
    _,root=tiny_env
    # 大量同值 + 少量异值：同值必然跨段、跨归并层（每段仅128个float）
    values=[7.5]*300+[1.0,2.0,9.0,5.5,5.5,5.5]+[7.5]*120+[3.25]*200
    m=new_manifest(1,'moisture',len(values))
    save_manifest(root,m)
    # 分4批接收，制造分段边界
    for i in range(0,len(values),100):
        append_batch(root,m,make_rows(values[i:i+100]))
    run_all(root,m)
    f=m['finalize']
    assert m['stage']=='verified'
    assert f['n']==len(values)
    for label,q in QUANTILE_POINTS:
        assert f['quantiles'][label]==oracle_quantile(values,q),label
    # 完整 ECDF 经分页重组后与 oracle 逐点一致
    got=[]
    for p in range(m['ecdf']['pages']):
        got.extend([(r['value'],r['fraction']) for r in read_ecdf_page(root,m,p)['rows']])
    assert got==oracle_ecdf(values)
    # 同值点的累计数必须精确：排序后 7.5 共420个，且其 ECDF 累计为 625（含205个更小值）
    idx75=next(i for i,(v,_) in enumerate(got) if v==7.5)
    assert got[idx75]==(7.5,625/len(values))
    prev_cum=got[idx75-1][1]*len(values) if idx75>0 else 0
    assert got[idx75][1]*len(values)-prev_cum==420  # 该值的重数精确为420
    assert len(m['segments'])>1            # 确实跨了多段
    assert int(m.get('final_level',0))>=1  # 确实发生了多层归并


def test_bad_rows_rejected_at_ingest(tiny_env):
    _,root=tiny_env
    m=new_manifest(2,'moisture',None);save_manifest(root,m)
    bad_batches=[
        [{'source_key':'a','values':{'extract':1.0}}],          # 缺字段
        [{'source_key':'b','values':{'moisture':float('nan')}}],
        [{'source_key':'c','values':{'moisture':120.0}}],
        [{'source_key':'','values':{'moisture':1.0}}],
        [{'values':{'moisture':1.0}}],
    ]
    for bad in bad_batches:
        try:
            append_batch(root,m,bad);assert False,'应拒绝'
        except LargeRunError:
            pass
    assert m['batches']==[]  # 全部未冻结


def test_conflicting_batch_number_rejected(tiny_env):
    _,root=tiny_env
    m=new_manifest(3,'moisture',None);save_manifest(root,m)
    append_batch(root,m,make_rows([1.0,2.0]))
    # 同号重传不同内容 -> 拒绝拼用
    try:
        append_batch(root,m,make_rows([3.0,4.0]),batch_no=1);assert False
    except LargeRunError as e:
        assert '拒绝拼用' in str(e)
    # 同号重传相同内容 -> 幂等
    before=dict(m)
    append_batch(root,m,make_rows([1.0,2.0]),batch_no=1)
    assert len(m['batches'])==1 and m['input_total']==2
    # 跳号拒绝
    try:
        append_batch(root,m,make_rows([5.0]),batch_no=9);assert False
    except LargeRunError:
        pass


def test_corrupt_frozen_input_stops_pipeline(tiny_env):
    _,root=tiny_env
    values=[float(i%7) for i in range(500)]
    m=new_manifest(4,'moisture',len(values));save_manifest(root,m)
    for i in range(0,len(values),50):
        append_batch(root,m,make_rows(values[i:i+50]))
    seal(root,m)
    # 破坏最后一个冻结批次（追加一行）：流式重读时校验和失配，必须停止并标记失败。
    last_no=max(b['no'] for b in m['batches'])
    bp=layout.batch_path(root,last_no)
    bp.write_text(bp.read_text()+'{"source_key":"x","value":1.0}\n')
    try:
        spill_segments(root,m);assert False
    except LargeRunError as e:
        assert '损坏' in str(e)
    m2=json.loads((root/'manifest.json').read_text())
    assert m2['failed'] is True
    # 失败态被持久化：重新加载清单即在入口被拒绝，任何后续推进都不可能。
    try:
        load_manifest(root);assert False
    except LargeRunError as e:
        assert '已失败' in str(e) or '损坏' in str(e)
    # 直接读原始 JSON 确认失败标记与无结果字段。
    assert 'finalize' not in m2


def test_corrupt_segment_recovered_not_reused(tiny_env):
    _,root=tiny_env
    values=[math.sin(i)*10+50 for i in range(600)]
    m=new_manifest(5,'moisture',len(values));save_manifest(root,m)
    append_batch(root,m,make_rows(values))
    seal(root,m)
    spill_segments(root,m)
    nseg=len(m['segments']);assert nseg>=3
    # 破坏第2个排序段：恢复时只重生成坏段及之后，第1段保留
    first=m['segments'][0];victim=m['segments'][1]
    p=layout.segment_path(root,0,victim['seq'])
    with open(p,'r+b') as f:
        f.seek(0);f.write(b'\x00'*16)
    # 第1段的 mtime 应保持不变（不重做）
    mt_before=os.stat(layout.segment_path(root,0,first['seq'])).st_mtime_ns
    spill_segments(root,load_manifest(root))
    mt_after=os.stat(layout.segment_path(root,0,first['seq'])).st_mtime_ns
    assert mt_before==mt_after,'已落妥片段不应重做'
    m2=load_manifest(root)
    assert len(m2['segments'])==nseg
    merge_levels(root,m2);finalize(root,m2);verify(root,m2)
    for label,q in QUANTILE_POINTS:
        assert m2['finalize']['quantiles'][label]==oracle_quantile(values,q)


def test_foreign_segment_refused(tiny_env):
    _,root=tiny_env
    # 运行A正常落段
    va=[1.0,2.0,3.0]*100
    ma_root=root/'a';ma_root.mkdir(exist_ok=True)
    ma=new_manifest(6,'moisture',len(va));save_manifest(ma_root,ma)
    append_batch(ma_root,ma,make_rows(va));seal(ma_root,ma);spill_segments(ma_root,ma)
    # 运行B：把A的段复制过来并登记，input_chain 不同 -> 必须拒绝
    vb=[4.0,5.0]*150
    mb_root=root/'b';mb_root.mkdir(exist_ok=True)
    mb=new_manifest(7,'moisture',len(vb));save_manifest(mb_root,mb)
    append_batch(mb_root,mb,make_rows(vb));seal(mb_root,mb)
    spill_segments(mb_root,mb)
    foreign=ma['segments'][0]
    import shutil
    target_seq=99
    shutil.copy(layout.segment_path(ma_root,0,foreign['seq']),
                layout.segment_path(mb_root,0,target_seq))
    fake=dict(foreign);fake['seq']=target_seq
    from src.services.large.layout import write_json_atomic,segment_meta_path
    write_json_atomic(segment_meta_path(mb_root,0,target_seq),fake)
    mb['segments'].append(fake)
    try:
        merge_levels(mb_root,mb);assert False
    except LargeRunError as e:
        assert '其他输入' in str(e) or '损坏' in str(e)


def test_resume_skips_finished_merge_segments(tiny_env):
    _,root=tiny_env
    values=[float((i*37)%101)/10 for i in range(900)]
    m=new_manifest(8,'moisture',len(values));save_manifest(root,m)
    for i in range(0,len(values),90):
        append_batch(root,m,make_rows(values[i:i+90]))
    seal(root,m);spill_segments(root,m)
    merge_levels(root,m)
    # 模拟中断：进程重启后重新推进，全部段已落妥 -> 直接到终遍/核验
    m2=load_manifest(root)
    assert m2['stage']=='finalizing'
    finalize(root,m2);verify(root,m2)
    got=[]
    for p in range(m2['ecdf']['pages']):
        got.extend([r['value'] for r in read_ecdf_page(root,m2,p)['rows']])
    assert got==sorted(set(values))
    for label,q in QUANTILE_POINTS:
        assert m2['finalize']['quantiles'][label]==oracle_quantile(values,q)


def test_ecdf_invisible_before_verification(tiny_env):
    _,root=tiny_env
    values=[1.0,2.0,2.0,3.0]*40
    m=new_manifest(9,'moisture',len(values));save_manifest(root,m)
    append_batch(root,m,make_rows(values));seal(root,m);spill_segments(root,m)
    merge_levels(root,m);finalize(root,m)
    assert m['stage']=='finalizing'
    try:
        read_ecdf_page(root,m,0);assert False
    except LargeRunError as e:
        assert '不可见' in str(e)
    verify(root,m)
    page=read_ecdf_page(root,m,0)
    assert page['pages']==m['ecdf']['pages'] and page['n']==len(values)


def test_expected_count_mismatch_stops(tiny_env):
    _,root=tiny_env
    m=new_manifest(10,'moisture',10);save_manifest(root,m)
    append_batch(root,m,make_rows([1.0,2.0,3.0]))
    try:
        seal(root,m);assert False
    except LargeRunError as e:
        assert '不一致' in str(e)


def test_corrupt_ecdf_page_detected(tiny_env):
    _,root=tiny_env
    values=[float(i) for i in range(50)]
    m=new_manifest(11,'moisture',len(values));save_manifest(root,m)
    append_batch(root,m,make_rows(values));run_all(root,m)
    p=layout.ecdf_page_path(root,0)
    p.write_text(p.read_text()+'\n')  # 破坏字节但保持可解析
    try:
        read_ecdf_page(root,load_manifest(root),0);assert False
    except LargeRunError as e:
        assert '损坏' in str(e)


def test_constant_batch_quantiles(tiny_env):
    _,root=tiny_env
    values=[42.0]*257
    m=new_manifest(12,'extract',len(values));save_manifest(root,m)
    append_batch(root,m,make_rows(values,field='extract'))
    run_all(root,m)
    f=m['finalize']
    assert all(f['quantiles'][k]==42.0 for k,_ in QUANTILE_POINTS)
    assert f['unique']==1 and f['std']==0.0
    got=[]
    for p in range(m['ecdf']['pages']):
        got.extend(read_ecdf_page(root,m,p)['rows'])
    assert got==[{'value':42.0,'cum':257,'fraction':1.0}] or \
           got[0]['value']==42.0 and got[0]['cum']==257
