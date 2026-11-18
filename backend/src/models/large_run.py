from sqlalchemy import Column,Integer,String,JSON,Text,ForeignKey,UniqueConstraint
from src.config.database import Base

class LargeDistributionRun(Base):
    """大批量分布研究：DB 只保存元数据与核验后的结果，全量输入与中间片段在磁盘。"""
    __tablename__='large_runs'
    id=Column(Integer,primary_key=True)
    study_id=Column(Integer,ForeignKey('studies.id'),nullable=False)
    name=Column(String(120),nullable=False)
    status=Column(String(20),default='ingesting')  # ingesting/queued/running/completed/failed
    field=Column(String(20),nullable=False)
    parameters=Column(JSON,nullable=False)
    expected_count=Column(Integer,nullable=True)
    input_total=Column(Integer,default=0)
    input_chain=Column(String(64),default='')
    artifact_dir=Column(String(500),nullable=False)
    result=Column(JSON,nullable=True)            # 仅在核验通过后写入，未核验不可见
    verification=Column(JSON,nullable=True)
    progress=Column(JSON,nullable=True)
    error=Column(Text,default='')
    created_at=Column(String(40),nullable=False)
    sealed_at=Column(String(40),nullable=True)

class LargeBatch(Base):
    __tablename__='large_batches'
    __table_args__=(UniqueConstraint('run_id','batch_no'),)
    id=Column(Integer,primary_key=True)
    run_id=Column(Integer,ForeignKey('large_runs.id'),nullable=False)
    batch_no=Column(Integer,nullable=False)
    rows=Column(Integer,nullable=False)
    sha256=Column(String(64),nullable=False)
    chain_after=Column(String(64),nullable=False)
    created_at=Column(String(40),nullable=False)
LargeRun=LargeDistributionRun
