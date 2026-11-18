"""大批量服务层集成测试：SQLite 元数据库 + 磁盘流水线，绕过 Redis 直接驱动。

覆盖：分批接收/冻结、损坏停止、未核验不可见、worker 流水线核验、ECDF 分页、
复核与导出权限、与小批研究互不干扰。
"""
import json,math,os,types
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config.database import Base
from src.models import user,study,observation,run,review,audit,large_run
from src.models.study import Study
from src.models.user import User
from src.models.large_run import LargeRun,LargeBatch
from src.schemas.large_run import LargeRunInput,LargeBatchInput
from src.services import large_runs
from src.services.large import layout
from src.services.large.pipeline import run_pipeline
from src.services.large.merging import read_ecdf_page
from src.services.large.inputs import load_manifest


@pytest.fixture()
def db_env(tmp_path,monkeypatch):
    engine=create_engine(f'sqlite:///{tmp_path}/test.db')
    Base.metadata.create_all(engine)
    Session=sessionmaker(bind=engine,expire_on_commit=False)
    monkeypatch.setattr('src.config.database.Session',Session)
    monkeypatch.setattr(large_runs,'Session',Session)
    monkeypatch.setattr(layout,'ARTIFACT_ROOT',tmp_path/'artifacts')
    # 绕过 Redis：freeze 不实际入队，测试直接驱动流水线。
    class FakeRedis:
        def rpush(self,*a): return 1
    monkeypatch.setattr(large_runs,'Redis',types.SimpleNamespace(from_url=lambda u:FakeRedis()))
    db=Session()
    db.add(Study(name='青禾·大批量一号',source_filename='tea-2m.csv',description=''))
    db.add(User(email='admin@demo.local',password_hash='x',role='admin'))
    db.add(User(email='analyst@demo.local',password_hash='x',role='analyst'))
    db.add(User(email='viewer@demo.local',password_hash='x',role='viewer'))
    db.commit()
    admin=db.query(User).filter_by(role='admin').one()
    analyst=db.query(User).filter_by(role='analyst').one()
    viewer=db.query(User).filter_by(role='viewer').one()
    yield types.SimpleNamespace(db=db,Session=Session,admin=admin,analyst=analyst,
                                viewer=viewer,tmp=tmp_path)
    db.close()


def _rows(values,field='moisture'):
    return [{'source_key':f'k{i:06d}','values':{field:float(v)}}
            for i,v in enumerate(values)]


def _create_and_ingest(env,values,batch_size=100,field='moisture',expected=True):
    data=LargeRunInput(study_id=1,name='200万级研究',field=field,
                       expected_count=len(values) if expected else None)
    rec=large_runs.create(env.db,data,env.analyst)
    run_id=rec['id']
    for i in range(0,len(values),batch_size):
        chunk=values[i:i+batch_size]
        large_runs.add_batch(env.db,run_id,
                             LargeBatchInput(rows=_rows(chunk,field)),env.analyst)
    return run_id


def test_full_service_flow_and_verification_gate(db_env):
    env=db_env
    values=[float((i*13)%97)/3.0 for i in range(1237)]  # 含大量同值、跨多段
    run_id=_create_and_ingest(env,values)
    # 未冻结前结果不可见
    rec=large_runs.get(env.db,run_id)
    assert rec['status']=='ingesting' and rec['result'] is None
    large_runs.freeze(env.db,run_id,env.analyst)
    lr=env.db.get(LargeRun,run_id)
    assert lr.status=='queued'
    # 模拟 worker 执行
    m=run_pipeline(Path(lr.artifact_dir))
    from src.services.large.result import build_result as _build_result
    result=_build_result(lr,m)
    lr.result=result;lr.verification=m['verification'];lr.status='completed'
    env.db.commit()
    final=large_runs.get(env.db,run_id)
    assert final['status']=='completed'
    # 核验项全部通过
    assert all(c['ok'] for c in final['verification']['checks'])
    # 分位数与独立 oracle 一致
    def oracle(xs,q):
        xs=sorted(xs);h=(len(xs)-1)*q;i=math.floor(h);j=math.ceil(h)
        return xs[i]+(xs[j]-xs[i])*(h-i)
    for label,q in [('最小值',0),('P25',.25),('中位数',.5),('P75',.75),('最大值',1)]:
        assert final['result']['metrics'][label]==oracle(values,q)
    # ECDF 分页读取，拼接后等于完整唯一序列
    got=[]
    pages=final['result']['metrics']['ECDF页数']
    for p in range(pages):
        got.extend(large_runs.ecdf(env.db,run_id,p)['rows'])
    uniq=sorted(set(values))
    assert [r['value'] for r in got]==uniq
    assert got[-1]['cum']==len(values) and got[-1]['fraction']==1.0


def test_ecdf_invisible_before_completed(db_env):
    env=db_env
    values=[1.0,2.0,2.0,3.0]*50
    run_id=_create_and_ingest(env,values)
    large_runs.freeze(env.db,run_id,env.analyst)
    # queued 状态读取 ECDF 被拒绝
    with pytest.raises(Exception) as ei:
        large_runs.ecdf(env.db,run_id,0)
    assert '不可见' in str(ei.value)


def test_corrupt_batch_marks_run_failed(db_env):
    env=db_env
    values=[float(i%5) for i in range(400)]
    run_id=_create_and_ingest(env,values)
    lr=env.db.get(LargeRun,run_id)
    # 冻结后破坏磁盘批次
    large_runs.freeze(env.db,run_id,env.analyst)
    m=load_manifest(Path(lr.artifact_dir))
    bp=layout.batch_path(Path(lr.artifact_dir),max(b['no'] for b in m['batches']))
    bp.write_text(bp.read_text()+'{"source_key":"z","value":9.0}\n')
    with pytest.raises(layout.LargeRunError):
        run_pipeline(Path(lr.artifact_dir))
    env.db.refresh(lr)
    raw=json.loads((Path(lr.artifact_dir)/'manifest.json').read_text())
    assert raw['failed'] is True
    # 服务层把失败同步到 DB（模拟 worker 异常处理）
    lr.status='failed';lr.error=raw['failure'];env.db.commit()
    assert large_runs.get(env.db,run_id)['status']=='failed'


def test_foreign_input_segment_refused(db_env):
    env=db_env
    # 两个不同输入的运行，B 不能使用 A 的片段
    va=[1.0,2.0,3.0]*80
    ra=_create_and_ingest(env,va)
    large_runs.freeze(env.db,ra,env.analyst)
    la=env.db.get(LargeRun,ra)
    run_pipeline(Path(la.artifact_dir))
    vb=[4.0,5.0]*120
    rb=_create_and_ingest(env,vb)
    large_runs.freeze(env.db,rb,env.analyst)
    lb=env.db.get(LargeRun,rb)
    root_b=Path(lb.artifact_dir)
    # 先让 B 正常完成分段，确保 level-0 目录与合法段存在。
    from src.services.large.sorting import spill_segments
    mb=load_manifest(root_b)
    spill_segments(root_b,mb)
    # 把 A 的段文件拷进 B 的目录并伪造 meta（input_chain 仍属于 A）。
    import shutil
    from src.services.large.layout import segment_path,segment_meta_path,write_json_atomic
    root_a=Path(la.artifact_dir)
    ma=load_manifest(root_a)
    foreign=ma['segments'][0]
    shutil.copy(segment_path(root_a,0,foreign['seq']),segment_path(root_b,0,77))
    fake=dict(foreign);fake['seq']=77  # input_chain 仍是 A 的
    write_json_atomic(segment_meta_path(root_b,0,77),fake)
    mb['segments'].append(fake)
    from src.services.large.inputs import save_manifest
    save_manifest(root_b,mb)
    with pytest.raises(layout.LargeRunError) as ei:
        run_pipeline(root_b)
    assert '其他输入' in str(ei.value) or '损坏' in str(ei.value)


def test_review_and_export_permissions(db_env):
    env=db_env
    values=[float(i%7) for i in range(333)]
    run_id=_create_and_ingest(env,values)
    large_runs.freeze(env.db,run_id,env.analyst)
    lr=env.db.get(LargeRun,run_id)
    m=run_pipeline(Path(lr.artifact_dir))
    from src.services.large.result import build_result as _build_result
    lr.result=_build_result(lr,m);lr.verification=m['verification'];lr.status='completed'
    env.db.commit()
    # viewer 未复核不能导出
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as ei:
        large_runs.export_metrics(env.db,run_id,env.viewer)
    assert ei.value.status_code==403
    # analyst 不能复核
    with pytest.raises(HTTPException) as ei:
        large_runs.approve(env.db,run_id,'核验无误，可发布',env.analyst)
    assert ei.value.status_code==403
    # admin 复核
    large_runs.approve(env.db,run_id,'核验无误，可发布',env.admin)
    # 复核不可覆盖
    with pytest.raises(HTTPException) as ei:
        large_runs.approve(env.db,run_id,'再次复核',env.admin)
    assert ei.value.status_code==409
    # viewer 复核后可导出
    csv_text=large_runs.export_metrics(env.db,run_id,env.viewer)
    assert '大批量运行编号' in csv_text and str(run_id) in csv_text
    # 复核记录与小批隔离
    rv=large_runs.get(env.db,run_id)['review']
    assert rv['run_kind']=='large' and rv['note']=='核验无误，可发布'


def test_batch_idempotent_and_conflict_rejected(db_env):
    env=db_env
    data=LargeRunInput(study_id=1,name='冲突研究',field='moisture',expected_count=None)
    run_id=large_runs.create(env.db,data,env.analyst)['id']
    large_runs.add_batch(env.db,run_id,LargeBatchInput(rows=_rows([1.0,2.0])),env.analyst)
    # 同内容重传幂等
    large_runs.add_batch(env.db,run_id,
                         LargeBatchInput(batch_no=1,rows=_rows([1.0,2.0])),env.analyst)
    assert env.db.query(LargeBatch).filter_by(run_id=run_id).count()==1
    # 不同内容同号拒绝
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as ei:
        large_runs.add_batch(env.db,run_id,
                             LargeBatchInput(batch_no=1,rows=_rows([9.0])),env.analyst)
    assert ei.value.status_code==422
    lr=env.db.get(LargeRun,run_id)
    assert lr.status=='failed'  # 冲突输入导致运行失败
