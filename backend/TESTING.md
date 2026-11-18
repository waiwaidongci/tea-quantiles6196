# 测试与实测记录（大批量分布研究）

本目录记录大批量分布研究（外部排序 + 流式归并，256MB 内存，精确不抽样）的实际验证。
测试使用独立 venv（`numpy / sqlalchemy / fastapi / pydantic / redis / pyjwt / pytest / pytest-env / httpx`），
段预算经 `pytest.ini` 的环境变量压到 1024B（每段约 128 个 float），使小例也跨多段、多层归并。

## 一、自动化测试：`pytest tests/ -q` → 19 passed

### 引擎测试 `tests/test_large_engine.py`（11 项，纯磁盘清单，无 DB/Redis）
- `test_tie_values_across_segments_and_layers`：同值跨段小例（420 个同值 + 异值，跨多段多层归并），
  分位点与线性插值 oracle 逐点相等；完整 ECDF 分页拼接后与 oracle 相等；同值重数精确（625/626）。
- `test_bad_rows_rejected_at_ingest`：缺字段 / NaN / 越界 / 空 source_key / 结构错误，接收即拒且不冻结。
- `test_conflicting_batch_number_rejected`：同号不同内容拒绝拼用；同号相同内容幂等；跳号拒绝。
- `test_corrupt_frozen_input_stops_pipeline`：破坏冻结批次 → 校验和失配 → 运行 failed 持久化，重载即停止，无结果字段。
- `test_corrupt_segment_recovered_not_reused`：破坏排序段 → 阶段回退、确定性重生成坏段，已落妥段 mtime/SHA 不变（不重做）。
- `test_foreign_segment_refused`：把另一运行（不同输入链）的段拷入 → 归并前拒绝。
- `test_resume_skips_finished_merge_segments`：模拟进程重启，已落归并段直接跳过，结果与 oracle 一致。
- `test_ecdf_invisible_before_verification`：终遍完成但未核验时 ECDF 不可见；核验后分页可读。
- `test_expected_count_mismatch_stops`：声明观测数与实际不符，seal 即停止。
- `test_corrupt_ecdf_page_detected`：破坏 ECDF 页字节 → 分页读取校验失败。
- `test_constant_batch_quantiles`：常量批次各分位点相等、标准差 0、唯一值 1。

### 服务层测试 `tests/test_large_service.py`（6 项，SQLite 元数据库 + 磁盘流水线）
- 全链路：创建→分批接收→冻结→流水线→核验门→分位数 oracle→ECDF 分页拼接→复核/导出权限。
- 未完成时 ECDF 不可见；坏批次使运行 failed；异输入片段拒绝；
  viewer 未复核禁导出、analyst 禁复核、admin 复核不可覆盖、复核与小批隔离（`run_kind`）。

### HTTP 端到端 `tests/test_large_api.py`（2 项，FastAPI TestClient + 真实鉴权/路由）
- 大批量运行完整 HTTP 生命周期（含 422/409/403/409 各状态码）；原小批 `/runs` 链路照常存活。

## 二、满规模实测（手动脚本，非单测）

### 200 万份精确研究 `scripts/large_scale_demo.py 2000000`
- 段预算 2MB（强制 8 段、多层归并）：峰值 RSS **114.3MB < 256MB**，磁盘产物 118.1MB，总用时约 10s。
- 段预算 16MB（生产默认，`docker-compose.yml`）：峰值 RSS **142.8MB < 256MB**，用时约 9.3s。
- 5 个分位点与 `numpy.quantile(method='linear')` **逐位相等**；均值/标准差 1e-9 容差内一致；
  完整 ECDF 1000 个唯一点、分页拼接逐点一致，末点累计 = 2,000,000、比例 1.0；发布核验 13 项全过。

### 中断恢复实测 `scripts/large_restart_demo.py`（N=500,000，真实 kill 进程）
- 分段完成、归并前 kill；重启后从清单恢复到 verified，
  已落妥的 2 个排序段 mtime 与 SHA256 完全不变（不重做），结果与 numpy 全量参考一致。

## 三、复现命令
```bash
# 自动化测试（backend/ 下）
python -m pytest tests/ -q

# 满规模（200万）与中断恢复
LARGE_SEGMENT_BYTES=2097152 LARGE_MERGE_FANOUT=8 LARGE_READER_BYTES=262144 \
LARGE_ECDF_PAGE_ROWS=500 python -m scripts.large_scale_demo 2000000
python -m scripts.large_restart_demo
```

## 四、已修复的实现缺陷（测试驱动发现）
1. Neumaier 求和的三元表达式运算符优先级错误，污染补偿量（常量批次标准差虚高）——加括号修复。
2. 恢复重生成段时按缓冲计数而非全局索引切分，段边界错位——改为全局索引切分，确定性逐字节复现。
3. 已进入归并阶段后发现 level-0 坏段时被早退逻辑跳过——增加阶段回退与下游产物清理。
4. 方差多除一次自由度、均值漏除 n——按 `np.std(ddof=1)` 同口径修正。
5. Review 表外键只指向小批 runs——改为应用层 `run_kind + run_id` 区分小批/大批量，互不干扰。
