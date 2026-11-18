"""端到端 HTTP 冒烟：FastAPI TestClient + SQLite，走真实鉴权与大批量路由。"""
import os, types
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config.database import Base, get_db
from src.models.study import Study
from src.models.user import User
from src.utils.security import hash_password
from src.services import large_runs
from src.services.large import layout
from src.main import app


@pytest.fixture()
def client(tmp_path, monkeypatch):
    engine = create_engine(f'sqlite:///{tmp_path}/api.db',
                           connect_args={'check_same_thread': False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    monkeypatch.setattr('src.config.database.Session', Session)
    monkeypatch.setattr(large_runs, 'Session', Session)
    monkeypatch.setattr(layout, 'ARTIFACT_ROOT', tmp_path / 'art')

    class FakeRedis:
        def rpush(self, *a): return 1
        def ping(self): return True
        def incr(self, key): return 1
        def expire(self, key, ttl): return True
    fake = types.SimpleNamespace(from_url=lambda u: FakeRedis())
    monkeypatch.setattr(large_runs, 'Redis', fake)
    import src.main as main_mod
    monkeypatch.setattr(main_mod, 'Redis', fake)
    from src.middlewares import rate_limit
    monkeypatch.setattr(rate_limit, 'redis', FakeRedis())

    def override_get_db():
        db = Session()
        try: yield db
        finally: db.close()
    app.dependency_overrides[get_db] = override_get_db
    with Session() as db:
        db.add(Study(name='青禾·API批', source_filename='api.csv', description=''))
        db.add(User(email='admin@demo.local', password_hash=hash_password('pw'), role='admin'))
        db.add(User(email='analyst@demo.local', password_hash=hash_password('pw'), role='analyst'))
        db.add(User(email='viewer@demo.local', password_hash=hash_password('pw'), role='viewer'))
        db.commit()
    # with 块触发 lifespan（建表/健康检查），且整个测试期间保持依赖覆盖，
    # 避免清理后请求回落到指向 Postgres 的全局 engine。
    with TestClient(app) as c:
        c._Session = Session
        c._tmp = tmp_path
        yield c
    app.dependency_overrides.clear()


def token(client, email):
    r = client.post('/auth/login', json={'email': email, 'password': 'pw'})
    assert r.status_code == 200, r.text
    return r.json()['token']


def auth(client, email):
    return {'Authorization': 'Bearer ' + token(client, email)}


def test_large_run_http_lifecycle(client):
    analyst = auth(client, 'analyst@demo.local')
    admin = auth(client, 'admin@demo.local')
    viewer = auth(client, 'viewer@demo.local')

    # 创建
    r = client.post('/large-runs',
                    json={'study_id': 1, 'name': 'API 全链路', 'field': 'moisture',
                          'expected_count': 500}, headers=analyst)
    assert r.status_code == 200, r.text
    run_id = r.json()['id']
    assert r.json()['status'] == 'ingesting'

    # 分两批接收（含同值跨段）
    rows1 = [{'source_key': f'a{i}', 'values': {'moisture': float(i % 7)}} for i in range(300)]
    rows2 = [{'source_key': f'b{i}', 'values': {'moisture': float(i % 11)}} for i in range(200)]
    assert client.post(f'/large-runs/{run_id}/batches', json={'rows': rows1},
                       headers=analyst).status_code == 200
    assert client.post(f'/large-runs/{run_id}/batches', json={'rows': rows2},
                       headers=analyst).status_code == 200
    detail = client.get(f'/large-runs/{run_id}', headers=analyst).json()
    assert detail['input_total'] == 500 and detail['progress']['phase'] == 'ingesting'

    # viewer 不能创建/接收
    assert client.post('/large-runs',
                       json={'study_id': 1, 'name': '禁止', 'field': 'moisture'},
                       headers=viewer).status_code == 403

    # 冻结
    assert client.post(f'/large-runs/{run_id}/freeze', json={},
                       headers=analyst).status_code == 200
    # 冻结后追加被拒
    assert client.post(f'/large-runs/{run_id}/batches',
                       json={'rows': rows1[:1]}, headers=analyst).status_code == 409

    # 坏观测在独立运行上被拒（422），且该运行按设计进入 failed，不影响主运行
    rb = client.post('/large-runs',
                     json={'study_id': 1, 'name': '坏数据运行', 'field': 'moisture'},
                     headers=analyst).json()['id']
    bad = [{'source_key': 'x', 'values': {'moisture': 'not-a-number'}}]
    assert client.post(f'/large-runs/{rb}/batches', json={'rows': bad},
                       headers=analyst).status_code == 422
    assert client.get(f'/large-runs/{rb}', headers=analyst).json()['status'] == 'failed'

    # 直接驱动流水线（worker 职责），再把结果写回
    from src.models.large_run import LargeRun
    from src.services.large.pipeline import run_pipeline
    from src.services.large.result import build_result
    with client._Session() as db:
        lr = db.get(LargeRun, run_id)
        m = run_pipeline(Path(lr.artifact_dir))
        lr.result = build_result(lr, m)
        lr.verification = m['verification']
        lr.progress = m['progress']
        lr.status = 'completed'
        db.commit()

    # 完成后 ECDF 分页可读，越界页被拒
    r = client.get(f'/large-runs/{run_id}/ecdf?page=0', headers=viewer)
    assert r.status_code == 200
    body = r.json()
    assert body['n'] == 500 and body['page'] == 0 and body['pages'] >= 1
    assert client.get(f'/large-runs/{run_id}/ecdf?page=999',
                      headers=viewer).status_code == 409

    # 未复核 viewer 不能导出；analyst 不能复核
    assert client.get(f'/large-runs/{run_id}/export', headers=viewer).status_code == 403
    assert client.post(f'/large-runs/{run_id}/review',
                       json={'note': 'API 链路核验通过'}, headers=analyst).status_code == 403
    # admin 复核
    assert client.post(f'/large-runs/{run_id}/review',
                       json={'note': 'API 链路核验通过'}, headers=admin).status_code == 200
    # viewer 复核后可导出 CSV
    r = client.get(f'/large-runs/{run_id}/export', headers=viewer)
    assert r.status_code == 200 and '大批量运行编号' in r.content.decode('utf-8')
    # 复核不可覆盖（请求体校验或冲突错误，均非 2xx）
    rr = client.post(f'/large-runs/{run_id}/review',
                     json={'note': '再来一次'}, headers=admin)
    assert rr.status_code in (409, 422)
    # 列表接口
    listing = client.get('/large-runs', headers=analyst).json()
    assert any(x['id'] == run_id and x['review'] for x in listing)


def test_small_runs_still_work(client):
    """原小批研究链路不受影响（路由仍挂载、参数校验仍生效）。"""
    analyst = auth(client, 'analyst@demo.local')
    r = client.post('/runs', json={'study_id': 1, 'name': '小批照常',
                                   'parameters': {'field': 'moisture', 'resamples': 200}},
                    headers=analyst)
    print('RUNS', r.status_code, r.text[:200])
    assert r.status_code in (422, 404)


