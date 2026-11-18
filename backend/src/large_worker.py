"""大批量分布研究独立 worker。

- 启动时把 queued/running 的大批量运行重新入队，中断后从磁盘清单恢复，已落片段不重做。
- 流水线在工作线程执行，主线程每秒把磁盘进度同步到 DB，页面可见“接收到可用”的进度。
- 只有核验通过（stage=verified）后才把结果写入 DB；未核验结果不可见。
- 输入/片段损坏等硬错误把运行置 failed 并保留原因。
"""
import json,logging,os,time
from pathlib import Path
from threading import Thread
from redis import Redis
from sqlalchemy import select
from src.config.database import Session
from src.models.large_run import LargeRun
from src.services.large import layout
from src.services.large.layout import LargeRunError,read_json
from src.services.large.pipeline import run_pipeline
from src.services.large.result import build_result as _build_result
from src.utils.serializers import now

QUEUE='large-analysis-queue'
ARTIFACT_ROOT=Path(os.environ.get('ARTIFACT_DIR','/app/artifacts'))
redis=Redis.from_url(os.environ['REDIS_URL'])
ARTIFACT_ROOT.mkdir(parents=True,exist_ok=True)

with Session() as db:
    pending=list(db.scalars(select(LargeRun).where(LargeRun.status.in_(['queued','running']))))
    for r in pending:
        redis.rpush(QUEUE,r.id)


def _execute(run_id:int):
    with Session() as db:
        run=db.get(LargeRun,run_id)
        if not run or run.status in ('completed','failed'):
            return
        run.status='running';db.commit()
        root=Path(run.artifact_dir)
        error=None
        try:
            m=run_pipeline(root)
            result=_build_result(run,m)
            (root/'result.json').write_text(
                json.dumps(result,ensure_ascii=False,allow_nan=False),encoding='utf-8')
            run.result=result
            run.verification=m.get('verification')
            run.progress=m.get('progress')
            run.status='completed'
        except Exception as e:
            logging.exception('large analysis failed')
            error=str(e)
            run.status='failed';run.error=error
            try: run.progress=read_json(root/layout.MANIFEST).get('progress')
            except Exception: pass
        finally:
            db.commit()


while True:
    Path('/tmp/large-worker-heartbeat').touch()
    job=redis.blpop(QUEUE,timeout=2)
    if not job:
        continue
    run_id=int(job[1])
    worker=Thread(target=_execute,args=(run_id,),daemon=True)
    worker.start()
    # 进度同步：流水线线程落盘清单，主线程每秒把进度写回 DB。
    while worker.is_alive():
        Path('/tmp/large-worker-heartbeat').touch()
        try:
            with Session() as db:
                run=db.get(LargeRun,run_id)
                if run and run.status=='running':
                    root=Path(run.artifact_dir)
                    mp=root/layout.MANIFEST
                    if mp.exists():
                        m=read_json(mp)
                        run.progress=m.get('progress')
                        db.commit()
        except Exception:
            logging.exception('progress sync failed')
        time.sleep(1)
    worker.join(timeout=1)
