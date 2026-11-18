from sqlalchemy import Column,Integer,String,Text,ForeignKey
from src.config.database import Base
class ReleaseNote(Base):
    __tablename__='reviews'
    id=Column(Integer,primary_key=True)
    # 小批运行引用 runs.id；大批量运行引用 large_runs.id。
    # 应用层保证 run_kind+run_id 的唯一性，DB 不再硬绑单一外键。
    run_id=Column(Integer,nullable=False)
    run_kind=Column(String(10),default='small',nullable=False)
    note=Column(Text,nullable=False)
    author=Column(String(120),nullable=False)
    created_at=Column(String(40),nullable=False)
Review=ReleaseNote
