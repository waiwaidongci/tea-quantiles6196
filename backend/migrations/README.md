Schema v1 is defined by the SQLAlchemy models and created at startup via Base.metadata.create_all. For this local small application no destructive migration is run. Back up named db-data before modifying schemas.

Schema v2 (大批量分布研究): 新增 `large_runs` 与 `large_batches` 两张表；`reviews` 表移除指向
`runs(id)` 的硬外键并新增 `run_kind`（small/large，默认 small），由应用层保证 run_kind+run_id 唯一，
使小批与大批量复核互不干扰。由于本应用以 `create_all` 在建库时直接创建最新结构，全新环境无需手工迁移；
对已存在的旧库需先备份 db-data，再手动执行：
  ALTER TABLE reviews ADD COLUMN run_kind VARCHAR(10) NOT NULL DEFAULT 'small';
  ALTER TABLE reviews DROP CONSTRAINT reviews_run_id_fkey;
大批量全量观测与排序/归并片段不入库，仅存于共享产物卷 large 运行目录，DB 只保留元数据、进度与核验后结果。
