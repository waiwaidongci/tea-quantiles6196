"""大批量研究服务：DB 元数据与磁盘清单的唯一协调点。

规则：
- 输入只落磁盘冻结文件与链式哈希，DB 不保存全量观测；
- 只有核验通过（stage=verified）后才写 result 并转 completed，未核验结果不可见；
- 任何输入/片段损坏都把运行置 failed 并保留原因，拒绝拼用其他输入的片段。
"""
import json,os
from pathlib import Path
from redis import Redis
from fastapi import HTTPException
from sqlalchemy import select
from src.config.database import Session
from src.models.large_run import LargeRun,LargeBatch
from src.models.review import Review
from src.services import studies
from src.services.audits import log
from src.services.large import layout
from src.services.large.inputs import (new_manifest,append_batch,load_manifest,
                                       save_manifest,seal,VALID_FIELDS)
from src.services.large.merging import read_ecdf_page
from src.services.large.pipeline import progress as read_progress
from src.utils.serializers import record,now

QUEUE='large-analysis-queue'


def _root(run: LargeRun) -> Path:
    return Path(run.artifact_dir)


def create(db,data,user):
    studies.require(db,data.study_id)
    if data.field not in VALID_FIELDS:
        raise HTTPException(422,'未知指标，仅支持 moisture/extract')
    run=LargeRun(study_id=data.study_id,name=data.name,status='ingesting',
                 field=data.field,parameters={'field':data.field,'mode':'large_external_sort'},
                 expected_count=data.expected_count,input_total=0,input_chain='',
                 artifact_dir='',result=None,error='',created_at=now())
    db.add(run);db.flush()
    root=layout.run_dir(run.id,'init')
    root.mkdir(parents=True,exist_ok=True)
    m=new_manifest(run.id,data.field,data.expected_count)
    save_manifest(root,m)
    # 目录名补上输入链（首批接收后才有非空链）；先用占位目录，首批后改名。
    run.artifact_dir=str(root)
    log(db,user,'largerun.create',str(run.id));db.commit()
    return record(run)


def add_batch(db,run_id,data,user):
    run=db.get(LargeRun,run_id)
    if not run: raise HTTPException(404,'大批量运行不存在')
    if run.status!='ingesting': raise HTTPException(409,'运行已冻结或结束，不能再接收批次')
    root=_root(run)
    try:
        m=load_manifest(root)
        append_batch(root,m,data.rows,data.batch_no)
    except layout.LargeRunError as e:
        run.status='failed';run.error=str(e)
        log(db,user,'largerun.batch_reject',f'{run_id}: {e}');db.commit()
        raise HTTPException(422,str(e))
    rec=m['batches'][-1] if data.batch_no is None else \
        next(b for b in m['batches'] if b['no']==(data.batch_no or len(m['batches'])))
    exists=db.scalar(select(LargeBatch).where(LargeBatch.run_id==run_id,
                                              LargeBatch.batch_no==rec['no']))
    if not exists:
        db.add(LargeBatch(run_id=run_id,batch_no=rec['no'],rows=rec['rows'],
                          sha256=rec['sha256'],chain_after=rec['chain_after'],
                          created_at=now()))
    run.input_total=m['input_total'];run.input_chain=m['input_chain']
    # 首个批次后把目录名补全为含输入链的规范目录。
    target=layout.run_dir(run_id,m['input_chain'])
    if Path(run.artifact_dir)!=target and Path(run.artifact_dir).exists():
        target.parent.mkdir(parents=True,exist_ok=True)
        Path(run.artifact_dir).rename(target)
        run.artifact_dir=str(target)
    run.progress=read_progress(target)
    log(db,user,'largerun.batch',f'{run_id}#{rec["no"]} rows={rec["rows"]}');db.commit()
    return record(run)


def freeze(db,run_id,user):
    run=db.get(LargeRun,run_id)
    if not run: raise HTTPException(404,'大批量运行不存在')
    if run.status!='ingesting': raise HTTPException(409,'运行不在接收状态')
    root=_root(run)
    try:
        m=load_manifest(root)
        seal(root,m)
    except layout.LargeRunError as e:
        run.status='failed';run.error=str(e);db.commit()
        raise HTTPException(422,str(e))
    run.status='queued';run.sealed_at=now();run.input_chain=m['input_chain']
    run.input_total=m['input_total'];run.progress=read_progress(root)
    log(db,user,'largerun.freeze',f'{run_id} rows={m["input_total"]}');db.commit()
    Redis.from_url(os.environ['REDIS_URL']).rpush(QUEUE,run_id)
    return record(run)


def get(db,run_id):
    run=db.get(LargeRun,run_id)
    if not run: raise HTTPException(404,'大批量运行不存在')
    out=record(run)
    rv=db.scalar(select(Review).where(Review.run_id==run_id,Review.run_kind=='large'))
    out['review']=record(rv) if rv else None
    if run.status in ('ingesting','queued','running') and Path(run.artifact_dir).exists():
        try: out['progress']=read_progress(Path(run.artifact_dir))
        except layout.LargeRunError as e:
            out['progress']={'failed':True,'detail':str(e)}
    return out


def list_runs(db):
    return [get(db,i) for i in db.scalars(select(LargeRun.id).order_by(LargeRun.id.desc()))]


def ecdf(db,run_id,page):
    run=db.get(LargeRun,run_id)
    if not run: raise HTTPException(404,'大批量运行不存在')
    if run.status!='completed':
        raise HTTPException(409,'结果尚未核验通过，ECDF 不可见')
    try:
        m=load_manifest(Path(run.artifact_dir))
        return read_ecdf_page(Path(run.artifact_dir),m,page)
    except layout.LargeRunError as e:
        raise HTTPException(409,str(e))


def approve(db,run_id,note,user):
    if user.role!='admin':
        raise HTTPException(403,'此动作需要负责人')
    run=db.get(LargeRun,run_id)
    if not run: raise HTTPException(404,'大批量运行不存在')
    if run.status!='completed': raise HTTPException(409,'只能复核已核验完成的运行')
    if db.scalar(select(Review).where(Review.run_id==run_id,Review.run_kind=='large')):
        raise HTTPException(409,'已复核，记录不可覆盖')
    rv=Review(run_id=run_id,run_kind='large',note=note,
              author=user.email,created_at=now())
    db.add(rv);log(db,user,'largerun.review',f'{run_id}: {note}');db.commit()
    return record(rv)


def export_metrics(db,run_id,user):
    import csv,io
    run=db.get(LargeRun,run_id)
    if not run: raise HTTPException(404,'大批量运行不存在')
    if run.status!='completed': raise HTTPException(409,'尚无核验通过的结果')
    r=get(db,run_id)
    if user.role=='viewer' and not r['review']:
        raise HTTPException(403,'观察员只能导出已复核结果')
    stream=io.StringIO();w=csv.writer(stream)
    w.writerow(['大批量运行编号','输入链SHA256','指标','值'])
    for k,v in run.result['metrics'].items():
        w.writerow([run_id,run.input_chain,k,
                    json.dumps(v,ensure_ascii=False) if isinstance(v,(dict,list)) else v])
    log(db,user,'largerun.export',run_id);db.commit()
    return '\ufeff'+stream.getvalue()
